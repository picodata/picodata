//! Compatibility layer between Tarantool and `raft-rs`.

pub mod error;
pub mod event;
pub mod failover;
mod network;
pub mod node;
pub mod notify;
mod storage;
pub mod topology;

use crate::stringify_debug;
use crate::util::Uppercase;
use ::raft::prelude as raft;
use ::tarantool::tlua::LuaError;
use ::tarantool::tuple::Encode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{Debug, Display};
use uuid::Uuid;

use protobuf::Message as _;

pub use network::ConnectionPool;
pub use storage::Storage;
pub use topology::Topology;

pub type RaftId = u64;
pub type RaftTerm = u64;
pub type RaftIndex = u64;
pub type InstanceId = String;
pub type ReplicasetId = String;

//////////////////////////////////////////////////////////////////////////////////////////
/// Timestamps for raft entries.
///
/// Logical clock provides a cheap and easy way for generating globally unique identifiers.
///
/// - `count` is a simple in-memory counter. It's cheap to increment because it's volatile.
/// - `gen` should be persisted upon LogicalClock initialization to ensure the uniqueness.
/// - `id` corresponds to `raft_id` of the instance (that is already unique across nodes).
#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct LogicalClock {
    id: u64,
    gen: u64,
    count: u64,
}

impl LogicalClock {
    pub fn new(id: u64, gen: u64) -> Self {
        Self { id, gen, count: 0 }
    }

    pub fn inc(&mut self) {
        self.count += 1;
    }
}

impl std::fmt::Display for LogicalClock {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.id, self.gen, self.count)
    }
}

//////////////////////////////////////////////////////////////////////////////////////////
/// The operation on the raft state machine.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "kind")]
pub enum Op {
    /// No operation.
    Nop,
    /// Print the message in tarantool log.
    Info {
        msg: String,
    },
    /// Evaluate the code on every instance in cluster.
    EvalLua(OpEvalLua),
    ///
    ReturnOne(OpReturnOne),
    PersistPeer {
        peer: Peer,
    },

    #[serde(alias = "persist_replication_factor")]
    PersistReplicationFactor {
        replication_factor: u8,
    },
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Nop => f.write_str("Nop"),
            Self::Info { msg } => write!(f, "Info({msg:?})"),
            Self::EvalLua(OpEvalLua { code }) => write!(f, "EvalLua({code:?})"),
            Self::ReturnOne(_) => write!(f, "ReturnOne"),
            Self::PersistPeer { peer } => {
                write!(f, "PersistPeer{}", peer)
            }
            Self::PersistReplicationFactor { replication_factor } => {
                write!(f, "PersistReplicationFactor({replication_factor})")
            }
        }
    }
}

impl Op {
    pub fn on_commit(&self) -> Box<dyn Any> {
        match self {
            Self::Nop => Box::new(()),
            Self::Info { msg } => {
                crate::tlog!(Info, "{msg}");
                Box::new(())
            }
            Self::EvalLua(op) => Box::new(op.result()),
            Self::ReturnOne(op) => Box::new(op.result()),
            Self::PersistPeer { peer } => {
                Storage::persist_peer(peer).unwrap();
                Box::new(peer.clone())
            }
            Self::PersistReplicationFactor { replication_factor } => {
                Storage::persist_replication_factor(*replication_factor).unwrap();
                Box::new(())
            }
        }
    }
}

impl OpResult for Op {
    type Result = ();
    fn result(&self) -> Self::Result {}
}

impl From<OpReturnOne> for Op {
    fn from(op: OpReturnOne) -> Op {
        Op::ReturnOne(op)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct OpReturnOne;

impl OpResult for OpReturnOne {
    type Result = u8;
    fn result(&self) -> Self::Result {
        1
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct OpEvalLua {
    pub code: String,
}

impl OpResult for OpEvalLua {
    type Result = Result<(), LuaError>;
    fn result(&self) -> Self::Result {
        crate::tarantool::eval(&self.code)
    }
}

impl From<OpEvalLua> for Op {
    fn from(op: OpEvalLua) -> Op {
        Op::EvalLua(op)
    }
}

pub trait OpResult {
    type Result: 'static;
    fn result(&self) -> Self::Result;
}

//////////////////////////////////////////////////////////////////////////////////////////
/// Serializable struct representing a member of the raft group.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Peer {
    /// Instances are identified by name.
    pub instance_id: String,
    pub instance_uuid: String,

    /// Used for identifying raft nodes.
    /// Must be unique in the raft group.
    pub raft_id: RaftId,

    /// Inbound address used for communication with the node.
    /// Not to be confused with listen address.
    pub peer_address: String,

    /// Name of a replicaset the instance belongs to.
    pub replicaset_id: String,
    pub replicaset_uuid: String,

    /// Index of the most recent raft log entry that persisted this peer.
    /// `0` means it's not committed yet.
    pub commit_index: RaftIndex,

    /// The state of this instance's activity.
    pub health: Health,

    /// Instance failure domains. Instances with overlapping failure domains
    /// must not be in the same replicaset.
    // TODO: raft_group space is kinda bloated, maybe we should store some data
    // in different spaces/not deserialize the whole tuple every time?
    pub failure_domain: FailureDomain,
}
impl Encode for Peer {}

impl Peer {
    pub fn is_active(&self) -> bool {
        matches!(self.health, Health::Online)
    }
}

impl std::fmt::Display for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "({}, {}, {}, {}, {:?}, {}, {})",
            self.instance_id,
            self.raft_id,
            self.replicaset_id,
            self.peer_address,
            self.health,
            self.commit_index,
            &self.failure_domain,
        )
    }
}

//////////////////////////////////////////////////////////////////////////////////////////
/// Serializable representation of `raft::prelude::Entry`.
///
/// See correspondig definition in `raft-rs`:
/// - <https://github.com/tikv/raft-rs/blob/v0.6.0/proto/proto/eraftpb.proto#L23>
///
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Entry {
    /// See correspondig definition in `raft-rs`:
    /// - <https://github.com/tikv/raft-rs/blob/v0.6.0/proto/proto/eraftpb.proto#L7>
    ///
    /// ```
    /// enum EntryType {
    ///     EntryNormal = 0;
    ///     EntryConfChange = 1;
    ///     EntryConfChangeV2 = 2;
    /// }
    /// ```
    #[serde(with = "entry_type_as_i32")]
    pub entry_type: raft::EntryType,
    pub index: RaftIndex,
    pub term: RaftTerm,

    /// Corresponding `entry.data`. Solely managed by `raft-rs`.
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,

    /// Corresponding `entry.payload`. Managed by the Picodata.
    pub context: Option<EntryContext>,
}

mod entry_type_as_i32 {
    use super::error::CoercionError::UnknownEntryType;
    use ::raft::prelude as raft;
    use protobuf::ProtobufEnum as _;
    use serde::{self, Deserialize, Deserializer, Serializer};

    use serde::de::Error as _;

    pub fn serialize<S>(t: &raft::EntryType, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ser.serialize_i32(t.value())
    }

    pub fn deserialize<'de, D>(de: D) -> Result<raft::EntryType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let t = i32::deserialize(de)?;
        raft::EntryType::from_i32(t).ok_or_else(|| D::Error::custom(UnknownEntryType(t)))
    }
}

/// Raft entry payload specific to the Picodata.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum EntryContext {
    Normal(EntryContextNormal),
    ConfChange(EntryContextConfChange),
}

/// [`EntryContext`] of a normal entry.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EntryContextNormal {
    pub lc: LogicalClock,
    pub op: Op,
}

impl EntryContextNormal {
    #[inline]
    pub fn new(lc: LogicalClock, op: impl Into<Op>) -> Self {
        Self { lc, op: op.into() }
    }
}

/// [`EntryContext`] of a conf change entry, either `EntryConfChange` or `EntryConfChangeV2`
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EntryContextConfChange {
    pub peers: Vec<Peer>,
}

impl Encode for Entry {}
impl ContextCoercion for EntryContextNormal {}
impl ContextCoercion for EntryContextConfChange {}

impl Entry {
    /// Returns the logical clock value if it's an `EntryNormal`.
    pub fn lc(&self) -> Option<&LogicalClock> {
        match &self.context {
            Some(EntryContext::Normal(v)) => Some(&v.lc),
            Some(EntryContext::ConfChange(_)) => None,
            None => None,
        }
    }

    /// Returns the contained `Op` if it's an `EntryNormal`.
    fn op(&self) -> Option<&Op> {
        match &self.context {
            Some(EntryContext::Normal(v)) => Some(&v.op),
            Some(EntryContext::ConfChange(_)) => None,
            None => None,
        }
    }

    pub fn payload(&self) -> EntryPayload {
        match (self.entry_type, &self.context) {
            (raft::EntryType::EntryNormal, None) => {
                debug_assert!(self.data.is_empty());
                EntryPayload::NormalEmpty
            }
            (raft::EntryType::EntryNormal, Some(EntryContext::Normal(ctx))) => {
                debug_assert!(self.data.is_empty());
                EntryPayload::Normal(ctx)
            }
            (raft::EntryType::EntryConfChange, None) => {
                let mut cc = raft::ConfChange::default();
                cc.merge_from_bytes(&self.data).unwrap();
                EntryPayload::ConfChange(cc)
            }
            (raft::EntryType::EntryConfChangeV2, None) => {
                let mut cc = raft::ConfChangeV2::default();
                cc.merge_from_bytes(&self.data).unwrap();
                EntryPayload::ConfChangeV2(cc)
            }
            (e, c) => {
                crate::warn_or_panic!("Unexpected context `{:?}` for entry `{:?}`", c, e);
                EntryPayload::NormalEmpty
            }
        }
    }
}

#[derive(Debug)]
pub enum EntryPayload<'a> {
    NormalEmpty,
    Normal(&'a EntryContextNormal),
    ConfChange(raft::ConfChange),
    ConfChangeV2(raft::ConfChangeV2),
}

impl<'a> std::fmt::Display for EntryPayload<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return match self {
            EntryPayload::NormalEmpty => f.write_str("-"),
            EntryPayload::Normal(norm) => write!(f, "{}", norm.op),
            EntryPayload::ConfChange(cc) => {
                write!(f, "{}({})", change_type(cc.change_type), cc.node_id)
            }
            EntryPayload::ConfChangeV2(ccv2) => {
                write!(f, "{:?}(", ccv2.transition)?;
                let mut iter = ccv2.changes.iter();
                let cc = (&mut iter).next().unwrap();
                write!(f, "{}({})", change_type(cc.change_type), cc.node_id)?;
                for cc in iter.take(ccv2.changes.len() - 1) {
                    write!(f, ", {}({})", change_type(cc.change_type), cc.node_id)?;
                }
                f.write_str(")")?;
                Ok(())
            }
        };

        const fn change_type(ct: raft::ConfChangeType) -> &'static str {
            match ct {
                raft::ConfChangeType::AddNode => "Promote",
                raft::ConfChangeType::AddLearnerNode => "Demote",
                raft::ConfChangeType::RemoveNode => "Remove",
            }
        }
    }
}

impl EntryContext {
    fn from_bytes_normal(bytes: &[u8]) -> Result<Option<Self>, error::CoercionError> {
        match EntryContextNormal::read_from_bytes(bytes)? {
            Some(v) => Ok(Some(Self::Normal(v))),
            None => Ok(None),
        }
    }

    fn from_bytes_conf_change(bytes: &[u8]) -> Result<Option<Self>, error::CoercionError> {
        match EntryContextConfChange::read_from_bytes(bytes)? {
            Some(v) => Ok(Some(Self::ConfChange(v))),
            None => Ok(None),
        }
    }

    fn write_to_bytes(ctx: Option<&Self>) -> Vec<u8> {
        match ctx {
            None => vec![],
            Some(Self::Normal(v)) => v.to_bytes(),
            Some(Self::ConfChange(v)) => v.to_bytes(),
        }
    }
}

impl TryFrom<&raft::Entry> for self::Entry {
    type Error = error::CoercionError;

    fn try_from(e: &raft::Entry) -> Result<Self, Self::Error> {
        let ret = Self {
            entry_type: e.entry_type,
            index: e.index,
            term: e.term,
            data: Vec::from(e.get_data()),
            context: match e.entry_type {
                raft::EntryType::EntryNormal => EntryContext::from_bytes_normal(&e.context)?,
                raft::EntryType::EntryConfChange | raft::EntryType::EntryConfChangeV2 => {
                    EntryContext::from_bytes_conf_change(&e.context)?
                }
            },
        };

        Ok(ret)
    }
}

impl From<self::Entry> for raft::Entry {
    fn from(row: self::Entry) -> raft::Entry {
        raft::Entry {
            entry_type: row.entry_type,
            index: row.index,
            term: row.term,
            data: row.data.into(),
            context: EntryContext::write_to_bytes(row.context.as_ref()).into(),
            ..Default::default()
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
/// A wrapper for `raft::prelude::Message` already serialized with a protobuf.
///
/// This struct is used for passing `raft::prelude::Message`
/// over Tarantool binary protocol (`net_box`).
#[derive(Clone, Deserialize, Serialize)]
struct MessagePb(#[serde(with = "serde_bytes")] Vec<u8>);
impl Encode for MessagePb {}

impl ::std::fmt::Debug for MessagePb {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        f.debug_struct(stringify_debug!(MessagePb))
            .finish_non_exhaustive()
    }
}

impl From<raft::Message> for self::MessagePb {
    fn from(m: raft::Message) -> Self {
        Self(m.write_to_bytes().expect("that's a bug"))
    }
}

impl TryFrom<self::MessagePb> for raft::Message {
    type Error = protobuf::ProtobufError;

    fn try_from(pb: self::MessagePb) -> Result<raft::Message, Self::Error> {
        let mut ret = raft::Message::default();
        ret.merge_from_bytes(&pb.0)?;
        Ok(ret)
    }
}

///////////////////////////////////////////////////////////////////////////////
/// This trait allows converting `EntryContext` to / from `Vec<u8>`.
pub trait ContextCoercion: Serialize + DeserializeOwned {
    fn read_from_bytes(bytes: &[u8]) -> Result<Option<Self>, error::CoercionError> {
        match bytes {
            bytes if bytes.is_empty() => Ok(None),
            bytes => Ok(Some(rmp_serde::from_read_ref(bytes)?)),
        }
    }

    fn write_to_bytes(ctx: Option<&Self>) -> Vec<u8> {
        match ctx {
            None => vec![],
            Some(ctx) => rmp_serde::to_vec_named(ctx).unwrap(),
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        ContextCoercion::write_to_bytes(Some(self))
    }
}

///////////////////////////////////////////////////////////////////////////////
/// Request to change cluster topology.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TopologyRequest {
    Join(JoinRequest),
    UpdatePeer(UpdatePeerRequest),
}

impl From<JoinRequest> for TopologyRequest {
    fn from(j: JoinRequest) -> Self {
        Self::Join(j)
    }
}

impl From<UpdatePeerRequest> for TopologyRequest {
    fn from(a: UpdatePeerRequest) -> Self {
        Self::UpdatePeer(a)
    }
}

///////////////////////////////////////////////////////////////////////////////
/// Request to join the cluster.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinRequest {
    pub cluster_id: String,
    pub instance_id: Option<String>,
    pub replicaset_id: Option<String>,
    pub advertise_address: String,
    pub failure_domain: FailureDomain,
}
impl Encode for JoinRequest {}

///////////////////////////////////////////////////////////////////////////////
/// Response to a JoinRequest
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinResponse {
    pub peer: Peer,
    pub raft_group: Vec<Peer>,
    pub box_replication: Vec<String>,
    // TODO add later:
    // Other parameters necessary for box.cfg()
    // pub read_only: bool,
}
impl Encode for JoinResponse {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExpelRequest {
    pub cluster_id: String,
    pub instance_id: String,
}
impl Encode for ExpelRequest {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExpelResponse {}
impl Encode for ExpelResponse {}

///////////////////////////////////////////////////////////////////////////////
/// Activity state of an instance.
#[derive(PartialEq, Eq, Clone, Debug, Deserialize, Serialize)]
pub enum Health {
    // Instance is active and is handling requests.
    Online,
    // Instance has gracefully shut down.
    Offline,
    // Instance has permanently removed from cluster.
    Expelled,
}

impl Health {
    const fn to_str(&self) -> &str {
        match self {
            Self::Online => "Online",
            Self::Offline => "Offline",
            Self::Expelled => "Expelled",
        }
    }
}

impl Display for Health {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

impl Default for Health {
    fn default() -> Self {
        Self::Offline
    }
}

///////////////////////////////////////////////////////////////////////////////
/// Request to deactivate the instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpdatePeerRequest {
    pub health: Health,
    pub instance_id: String,
    pub cluster_id: String,
    pub failure_domain: Option<FailureDomain>,
}
impl Encode for UpdatePeerRequest {}

impl UpdatePeerRequest {
    #[inline]
    pub fn set_online(instance_id: impl Into<String>, cluster_id: impl Into<String>) -> Self {
        Self {
            health: Health::Online,
            instance_id: instance_id.into(),
            cluster_id: cluster_id.into(),
            failure_domain: None,
        }
    }

    #[inline]
    pub fn set_offline(instance_id: impl Into<String>, cluster_id: impl Into<String>) -> Self {
        Self {
            health: Health::Offline,
            instance_id: instance_id.into(),
            cluster_id: cluster_id.into(),
            failure_domain: None,
        }
    }

    #[inline]
    pub fn set_expelled(instance_id: impl Into<String>, cluster_id: impl Into<String>) -> Self {
        Self {
            health: Health::Expelled,
            instance_id: instance_id.into(),
            cluster_id: cluster_id.into(),
            failure_domain: None,
        }
    }

    #[inline]
    pub fn set_failure_domain(&mut self, failure_domain: FailureDomain) {
        self.failure_domain = Some(failure_domain);
    }
}

///////////////////////////////////////////////////////////////////////////////
/// Response to a [`UpdatePeerRequest`]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpdatePeerResponse {
    // It's empty now, but it may be extended in future
}
impl Encode for UpdatePeerResponse {}

///////////////////////////////////////////////////////////////////////////////
lazy_static::lazy_static! {
    static ref NAMESPACE_INSTANCE_UUID: Uuid =
        Uuid::new_v3(&Uuid::nil(), "INSTANCE_UUID".as_bytes());
    static ref NAMESPACE_REPLICASET_UUID: Uuid =
        Uuid::new_v3(&Uuid::nil(), "REPLICASET_UUID".as_bytes());
}

/// Generate UUID for an instance from `instance_id` (String).
/// Use Version-3 (MD5) UUID.
pub fn instance_uuid(instance_id: &str) -> String {
    let uuid = Uuid::new_v3(&NAMESPACE_INSTANCE_UUID, instance_id.as_bytes());
    uuid.hyphenated().to_string()
}

/// Generate UUID for a replicaset from `replicaset_id` (String).
/// Use Version-3 (MD5) UUID.
pub fn replicaset_uuid(replicaset_id: &str) -> String {
    let uuid = Uuid::new_v3(&NAMESPACE_REPLICASET_UUID, replicaset_id.as_bytes());
    uuid.hyphenated().to_string()
}

////////////////////////////////////////////////////////////////////////////////
/// Failure domains of a given instance.
#[derive(Default, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct FailureDomain {
    #[serde(flatten)]
    data: HashMap<Uppercase, Uppercase>,
}

impl FailureDomain {
    pub fn contains_name(&self, name: &Uppercase) -> bool {
        self.data.contains_key(name)
    }

    pub fn names(&self) -> std::collections::hash_map::Keys<Uppercase, Uppercase> {
        self.data.keys()
    }

    /// Empty `FailureDomain` doesn't intersect with any other `FailureDomain`
    /// even with another empty one.
    pub fn intersects(&self, other: &Self) -> bool {
        for (name, value) in &self.data {
            match other.data.get(name) {
                Some(other_value) if value == other_value => {
                    return true;
                }
                _ => {}
            }
        }
        false
    }
}

impl std::fmt::Display for FailureDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("{")?;
        let mut iter = self.data.iter();
        if let Some((k, v)) = (&mut iter).next() {
            write!(f, "{k}: {v}")?;
            for (k, v) in iter {
                write!(f, ", {k}: {v}")?;
            }
        }
        f.write_str("}")?;
        Ok(())
    }
}

impl std::fmt::Debug for FailureDomain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut ds = f.debug_struct(stringify_debug!(FailureDomain));
        for (name, value) in &self.data {
            ds.field(name, &**value);
        }
        ds.finish()
    }
}

impl<I, K, V> From<I> for FailureDomain
where
    I: IntoIterator<Item = (K, V)>,
    Uppercase: From<K>,
    Uppercase: From<V>,
{
    fn from(data: I) -> Self {
        Self {
            data: data
                .into_iter()
                .map(|(k, v)| (Uppercase::from(k), Uppercase::from(v)))
                .collect(),
        }
    }
}

impl<'a> IntoIterator for &'a FailureDomain {
    type IntoIter = <&'a HashMap<Uppercase, Uppercase> as IntoIterator>::IntoIter;
    type Item = <&'a HashMap<Uppercase, Uppercase> as IntoIterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        self.data.iter()
    }
}
