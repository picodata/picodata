//! Compatibility layer between Tarantool and `raft-rs`.

mod error;
pub mod event;
pub mod failover;
mod network;
pub mod node;
mod storage;
pub mod topology;

use ::raft::prelude as raft;
use ::tarantool::tuple::AsTuple;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::convert::TryFrom;
use std::fmt::Display;
use uuid::Uuid;

use protobuf::Message as _;

pub use network::ConnectionPool;
pub use storage::Storage;
pub use topology::Topology;

pub type RaftId = u64;
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
#[derive(Clone, Debug, Default, Serialize, Deserialize, Hash, PartialEq, Eq)]
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
    EvalLua {
        code: String,
    },
    ///
    ReturnOne(OpReturnOne),
    PersistPeer {
        peer: Peer,
    },
}

impl Op {
    pub fn on_commit(&self) -> Box<dyn Any> {
        match self {
            Self::Nop => Box::new(()),
            Self::Info { msg } => {
                crate::tlog!(Info, "{msg}");
                Box::new(())
            }
            Self::EvalLua { code } => {
                crate::tarantool::eval(code);
                Box::new(())
            }
            Self::ReturnOne(op) => Box::new(op.result()),
            Self::PersistPeer { peer } => {
                Storage::persist_peer(peer).unwrap();
                Box::new(peer.clone())
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
    type Result = u64;
    fn result(&self) -> Self::Result {
        1
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
    pub raft_id: u64,

    /// Inbound address used for communication with the node.
    /// Not to be confused with listen address.
    pub peer_address: String,

    /// Name of a replicaset the instance belongs to.
    pub replicaset_id: String,
    pub replicaset_uuid: String,

    /// Index in the raft log. `0` means it's not committed yet.
    pub commit_index: u64,

    /// The state of this instance's activity.
    pub health: Health,
}
impl AsTuple for Peer {}

impl Peer {
    pub fn is_active(&self) -> bool {
        matches!(self.health, Health::Online)
    }
}

//////////////////////////////////////////////////////////////////////////////////////////
/// Serializable representation of `raft::prelude::Entry`.
///
/// See correspondig definition in `raft-rs`:
/// - <https://github.com/tikv/raft-rs/blob/v0.6.0/proto/proto/eraftpb.proto#L23>
///
#[derive(Clone, Serialize, Deserialize, PartialEq)]
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
    pub index: u64,
    pub term: u64,

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

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("entry_type", &self.entry_type)
            .field("index", &self.index)
            .field("term", &self.term)
            .field("data", &self.data)
            .field("context", &self.context)
            .finish()
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

/// [`EntryContext`] of a conf change entry, either `EntryConfChange` or `EntryConfChangeV2`
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EntryContextConfChange {
    pub peers: Vec<Peer>,
}

impl AsTuple for Entry {}
impl ContextCoercion for EntryContextNormal {}
impl ContextCoercion for EntryContextConfChange {}

impl Entry {
    /// Returns the logical clock value if it's an `EntryNormal`.
    fn lc(&self) -> Option<&LogicalClock> {
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
impl AsTuple for MessagePb {}

impl ::std::fmt::Debug for MessagePb {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        f.debug_struct("MessagePb").finish_non_exhaustive()
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
    SetActive(SetActiveRequest),
}

impl From<JoinRequest> for TopologyRequest {
    fn from(j: JoinRequest) -> Self {
        Self::Join(j)
    }
}

impl From<SetActiveRequest> for TopologyRequest {
    fn from(a: SetActiveRequest) -> Self {
        Self::SetActive(a)
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
    pub voter: bool,
}
impl AsTuple for JoinRequest {}

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
impl AsTuple for JoinResponse {}

///////////////////////////////////////////////////////////////////////////////
/// [`SetActiveRequest`] kind tag
#[derive(PartialEq, Eq, Clone, Debug, Deserialize, Serialize)]
pub enum Health {
    // Instance is active and is handling requests.
    Online,
    // Instance has gracefully shut down.
    Offline,
}

impl Health {
    const fn to_str(&self) -> &str {
        match self {
            Self::Online => "Online",
            Self::Offline => "Offline",
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
pub struct SetActiveRequest {
    pub kind: Health,
    pub instance_id: String,
    pub cluster_id: String,
}
impl AsTuple for SetActiveRequest {}

impl SetActiveRequest {
    #[allow(dead_code)]
    #[inline]
    pub fn activate(instance_id: impl Into<String>, cluster_id: impl Into<String>) -> Self {
        Self {
            kind: Health::Online,
            instance_id: instance_id.into(),
            cluster_id: cluster_id.into(),
        }
    }

    #[inline]
    pub fn deactivate(instance_id: impl Into<String>, cluster_id: impl Into<String>) -> Self {
        Self {
            kind: Health::Offline,
            instance_id: instance_id.into(),
            cluster_id: cluster_id.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
/// Response to a [`SetActiveRequest`]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SetActiveResponse {
    pub peer: Peer,
}
impl AsTuple for SetActiveResponse {}

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
