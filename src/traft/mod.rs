//! Compatibility layer between Tarantool and `raft-rs`.

pub mod error;
pub mod event;
pub mod failover;
pub mod governor;
mod network;
pub mod node;
pub mod notify;
mod raft_storage;
pub mod rpc;
pub mod storage;
pub mod topology;

use crate::stringify_debug;
use crate::util::{AnyWithTypeName, Uppercase};
use ::raft::prelude as raft;
use ::tarantool::error::Error as TntError;
use ::tarantool::tlua;
use ::tarantool::tlua::LuaError;
use ::tarantool::tuple::{Encode, ToTupleBuffer, Tuple, TupleBuffer};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use uuid::Uuid;

use protobuf::Message as _;

pub use network::ConnectionPool;
pub use raft_storage::RaftSpaceAccess;
use storage::ClusterSpace;
pub use storage::Storage;
pub use topology::Topology;

pub type RaftId = u64;
pub type RaftTerm = u64;
pub type RaftIndex = u64;
pub type ReplicasetId = String;

pub const INIT_RAFT_TERM: RaftTerm = 1;

////////////////////////////////////////////////////////////////////////////////
/// Unique id of a cluster instance.
///
/// This is a new-type style wrapper around String, to distinguish it from other
/// strings.
#[rustfmt::skip]
#[derive(Default, Debug, Eq, Clone, Hash)]
#[derive(tlua::LuaRead, tlua::Push, tlua::PushInto)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct InstanceId(pub String);

impl std::fmt::Display for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl From<String> for InstanceId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for InstanceId {
    fn from(s: &str) -> Self {
        Self(s.into())
    }
}

impl From<InstanceId> for String {
    fn from(i: InstanceId) -> Self {
        i.0
    }
}

impl AsRef<str> for InstanceId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::borrow::Borrow<str> for InstanceId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for InstanceId {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl<T> std::cmp::PartialEq<T> for InstanceId
where
    T: ?Sized,
    T: AsRef<str>,
{
    fn eq(&self, rhs: &T) -> bool {
        self.0 == rhs.as_ref()
    }
}

impl std::str::FromStr for InstanceId {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, std::convert::Infallible> {
        Ok(Self(s.into()))
    }
}

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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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
    /// Cluster-wide data modification operation.
    /// Should be used to manipulate the cluster-wide configuration.
    Dml(OpDML),
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return match self {
            Self::Nop => f.write_str("Nop"),
            Self::Info { msg } => write!(f, "Info({msg:?})"),
            Self::EvalLua(OpEvalLua { code }) => write!(f, "EvalLua({code:?})"),
            Self::ReturnOne(_) => write!(f, "ReturnOne"),
            Self::PersistPeer { peer } => {
                write!(f, "PersistPeer{}", peer)
            }
            Self::Dml(OpDML::Insert { space, tuple }) => {
                write!(f, "Insert({space}, {})", DisplayAsJson(tuple))
            }
            Self::Dml(OpDML::Replace { space, tuple }) => {
                write!(f, "Replace({space}, {})", DisplayAsJson(tuple))
            }
            Self::Dml(OpDML::Update { space, key, ops }) => {
                let key = DisplayAsJson(key);
                let ops = DisplayAsJson(&**ops);
                write!(f, "Update({space}, {key}, {ops})")
            }
            Self::Dml(OpDML::Delete { space, key }) => {
                write!(f, "Delete({space}, {})", DisplayAsJson(key))
            }
        };

        struct DisplayAsJson<T>(pub T);

        impl std::fmt::Display for DisplayAsJson<&TupleBuffer> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                if let Some(data) = rmp_serde::from_slice::<serde_json::Value>(self.0.as_ref())
                    .ok()
                    .and_then(|v| serde_json::to_string(&v).ok())
                {
                    return write!(f, "{data}");
                }

                write!(f, "{:?}", self.0)
            }
        }

        impl std::fmt::Display for DisplayAsJson<&[TupleBuffer]> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "[")?;
                if let Some(elem) = self.0.first() {
                    write!(f, "{}", DisplayAsJson(elem))?;
                }
                for elem in self.0.iter().skip(1) {
                    write!(f, ", {}", DisplayAsJson(elem))?;
                }
                write!(f, "]")
            }
        }
    }
}

impl Op {
    pub fn on_commit(&self, peers: &storage::Peers) -> Box<dyn AnyWithTypeName> {
        match self {
            Self::Nop => Box::new(()),
            Self::Info { msg } => {
                crate::tlog!(Info, "{msg}");
                Box::new(())
            }
            Self::EvalLua(op) => Box::new(op.result()),
            Self::ReturnOne(op) => Box::new(op.result()),
            Self::PersistPeer { peer } => {
                peers.put(peer).unwrap();
                Box::new(peer.clone())
            }
            Self::Dml(op) => Box::new(op.result()),
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpReturnOne;

impl OpResult for OpReturnOne {
    type Result = u8;
    fn result(&self) -> Self::Result {
        1
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpEvalLua {
    pub code: String,
}

impl OpResult for OpEvalLua {
    type Result = Result<(), LuaError>;
    fn result(&self) -> Self::Result {
        crate::tarantool::exec(&self.code)
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
// OpDML

/// Cluster-wide data modification operation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum OpDML {
    Insert {
        space: ClusterSpace,
        #[serde(with = "serde_bytes")]
        tuple: TupleBuffer,
    },
    Replace {
        space: ClusterSpace,
        #[serde(with = "serde_bytes")]
        tuple: TupleBuffer,
    },
    Update {
        space: ClusterSpace,
        #[serde(with = "serde_bytes")]
        key: TupleBuffer,
        #[serde(with = "vec_of_raw_byte_buf")]
        ops: Vec<TupleBuffer>,
    },
    Delete {
        space: ClusterSpace,
        #[serde(with = "serde_bytes")]
        key: TupleBuffer,
    },
}

impl OpResult for OpDML {
    type Result = tarantool::Result<Option<Tuple>>;
    fn result(&self) -> Self::Result {
        match self {
            Self::Insert { space, tuple } => space.insert(tuple).map(Some),
            Self::Replace { space, tuple } => space.replace(tuple).map(Some),
            Self::Update { space, key, ops } => space.update(key, ops),
            Self::Delete { space, key } => space.delete(key),
        }
    }
}

impl From<OpDML> for Op {
    fn from(op: OpDML) -> Op {
        Op::Dml(op)
    }
}

impl OpDML {
    /// Serializes `tuple` and returns an [`OpDML::Insert`] in case of success.
    pub fn insert(space: ClusterSpace, tuple: &impl ToTupleBuffer) -> Result<Self, TntError> {
        let res = Self::Insert {
            space,
            tuple: tuple.to_tuple_buffer()?,
        };
        Ok(res)
    }

    /// Serializes `tuple` and returns an [`OpDML::Replace`] in case of success.
    pub fn replace(space: ClusterSpace, tuple: &impl ToTupleBuffer) -> Result<Self, TntError> {
        let res = Self::Replace {
            space,
            tuple: tuple.to_tuple_buffer()?,
        };
        Ok(res)
    }

    /// Serializes `key` and returns an [`OpDML::Update`] in case of success.
    pub fn update(
        space: ClusterSpace,
        key: &impl ToTupleBuffer,
        ops: impl Into<Vec<TupleBuffer>>,
    ) -> Result<Self, TntError> {
        let res = Self::Update {
            space,
            key: key.to_tuple_buffer()?,
            ops: ops.into(),
        };
        Ok(res)
    }

    /// Serializes `key` and returns an [`OpDML::Delete`] in case of success.
    pub fn delete(space: ClusterSpace, key: &impl ToTupleBuffer) -> Result<Self, TntError> {
        let res = Self::Delete {
            space,
            key: key.to_tuple_buffer()?,
        };
        Ok(res)
    }
}

mod vec_of_raw_byte_buf {
    use super::TupleBuffer;
    use ::tarantool::error::Error as TntError;
    use serde::de::Error as _;
    use serde::ser::SerializeSeq;
    use serde::{self, Deserialize, Deserializer, Serializer};
    use serde_bytes::{ByteBuf, Bytes};
    use std::convert::TryFrom;

    pub fn serialize<S>(v: &[TupleBuffer], ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = ser.serialize_seq(Some(v.len()))?;
        for buf in v {
            seq.serialize_element(Bytes::new(buf.as_ref()))?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(de: D) -> Result<Vec<TupleBuffer>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tmp = Vec::<ByteBuf>::deserialize(de)?;
        // FIXME(gmoshkin): redundant copy happens here,
        // because ByteBuf and TupleBuffer are essentially the same struct,
        // but there's no easy foolproof way
        // to convert a Vec<ByteBuf> to Vec<TupleBuffer>
        // because of borrow and drop checkers
        let res: Result<_, TntError> = tmp
            .into_iter()
            .map(|bb| TupleBuffer::try_from(bb.into_vec()))
            .collect();
        res.map_err(D::Error::custom)
    }
}

//////////////////////////////////////////////////////////////////////////////////////////
/// Serializable struct representing a member of the raft group.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Peer {
    /// Instances are identified by name.
    pub instance_id: InstanceId,
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

    /// The cluster's mind about actual state of this instance's activity.
    pub current_grade: CurrentGrade,
    /// The desired state of this instance
    pub target_grade: TargetGrade,

    /// Instance failure domains. Instances with overlapping failure domains
    /// must not be in the same replicaset.
    // TODO: raft_group space is kinda bloated, maybe we should store some data
    // in different spaces/not deserialize the whole tuple every time?
    pub failure_domain: FailureDomain,
}
impl Encode for Peer {}

impl Peer {
    pub fn is_online(&self) -> bool {
        matches!(self.current_grade, CurrentGrade::Online)
    }

    /// Peer has a grade that implies it may cooperate.
    /// Currently this means that target_grade is neither Offline or Expelled.
    pub fn may_respond(&self) -> bool {
        self.target_grade != TargetGrade::Offline && self.target_grade != TargetGrade::Expelled
    }

    pub fn has_grades(&self, current: CurrentGrade, target: TargetGrade) -> bool {
        self.current_grade == current && self.target_grade == target
    }
}

impl std::fmt::Display for Peer {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return write!(f,
            "({}, {}, {}, {}, {}, {}, {})",
            self.instance_id,
            self.raft_id,
            self.replicaset_id,
            self.peer_address,
            GradeTransition { from: self.current_grade, to: self.target_grade },
            self.commit_index,
            &self.failure_domain,
        );

        struct GradeTransition { from: CurrentGrade, to: TargetGrade }
        impl std::fmt::Display for GradeTransition {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let (from, to) = (self.from.as_str(), self.to.as_str());
                if from == to {
                    f.write_str(to)
                } else {
                    write!(f, "{from} -> {to}")
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Replicaset info
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Replicaset {
    /// Primary identifier.
    pub replicaset_id: String,

    /// UUID used to identify replicasets by tarantool's subsystems.
    pub replicaset_uuid: String,

    /// Instance id of the current replication leader.
    pub master_id: InstanceId,

    /// Sharding weight of the replicaset.
    pub weight: rpc::sharding::cfg::Weight,
}
impl Encode for Replicaset {}

impl std::fmt::Display for Replicaset {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "({}, master: {}, weight: {})",
            self.replicaset_id, self.master_id, self.weight,
        )
    }
}

//////////////////////////////////////////////////////////////////////////////////////////
/// Serializable representation of `raft::prelude::Entry`.
///
/// See correspondig definition in `raft-rs`:
/// - <https://github.com/tikv/raft-rs/blob/v0.6.0/proto/proto/eraftpb.proto#L23>
///
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum EntryContext {
    Normal(EntryContextNormal),
    ConfChange(EntryContextConfChange),
}

/// [`EntryContext`] of a normal entry.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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
                if let Some(cc) = iter.next() {
                    write!(f, "{}({})", change_type(cc.change_type), cc.node_id)?;
                    for cc in iter.take(ccv2.changes.len() - 1) {
                        write!(f, ", {}({})", change_type(cc.change_type), cc.node_id)?;
                    }
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
        f.debug_tuple(stringify_debug!(MessagePb))
            .field(&self.0)
            .finish()
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
    pub instance_id: Option<InstanceId>,
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
    pub instance_id: InstanceId,
}
impl Encode for ExpelRequest {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExpelResponse {}
impl Encode for ExpelResponse {}

///////////////////////////////////////////////////////////////////////////////
crate::define_str_enum! {
    /// Activity state of an instance.
    pub enum CurrentGrade {
        // Instance has gracefully shut down or has not been started yet.
        Offline = "Offline",
        // Instance has synced by commit index.
        RaftSynced = "RaftSynced",
        // Instance has configured replication.
        Replicated = "Replicated",
        // Instance has configured sharding.
        ShardingInitialized = "ShardingInitialized",
        // Instance is active and is handling requests.
        Online = "Online",
        // Instance has permanently removed from cluster.
        Expelled = "Expelled",
    }
    FromStr::Err = UnknownGrade;
}

#[derive(thiserror::Error, Debug)]
#[error("unknown grade {0:?}")]
pub struct UnknownGrade(pub String);

impl Default for CurrentGrade {
    fn default() -> Self {
        Self::Offline
    }
}

crate::define_str_enum! {
    pub enum TargetGrade {
        // Instance should be configured up
        Online = "Online",
        // Instance should be gracefully shut down
        Offline = "Offline",
        // Instance should be removed from cluster
        Expelled = "Expelled",
    }
    FromStr::Err = UnknownTargetGrade;
}

#[derive(thiserror::Error, Debug)]
#[error("unknown target grade {0:?}")]
pub struct UnknownTargetGrade(pub String);

impl Default for TargetGrade {
    fn default() -> Self {
        Self::Online
    }
}

///////////////////////////////////////////////////////////////////////////////
/// Request to update the instance in the storage.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpdatePeerRequest {
    pub instance_id: InstanceId,
    pub cluster_id: String,
    pub changes: Vec<PeerChange>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PeerChange {
    CurrentGrade(CurrentGrade),
    TargetGrade(TargetGrade),
    FailureDomain(FailureDomain),
}

impl PeerChange {
    pub fn apply(self, peer: &mut Peer) {
        match self {
            Self::CurrentGrade(value) => peer.current_grade = value,
            Self::TargetGrade(value) => peer.target_grade = value,
            Self::FailureDomain(value) => peer.failure_domain = value,
        }
    }
}

impl Encode for UpdatePeerRequest {}
impl UpdatePeerRequest {
    #[inline]
    pub fn new(instance_id: InstanceId, cluster_id: String) -> Self {
        Self {
            instance_id,
            cluster_id,
            changes: vec![],
        }
    }
    #[inline]
    pub fn with_current_grade(mut self, value: CurrentGrade) -> Self {
        self.changes.push(PeerChange::CurrentGrade(value));
        self
    }
    #[inline]
    pub fn with_target_grade(mut self, value: TargetGrade) -> Self {
        self.changes.push(PeerChange::TargetGrade(value));
        self
    }
    #[inline]
    pub fn with_failure_domain(mut self, value: FailureDomain) -> Self {
        self.changes.push(PeerChange::FailureDomain(value));
        self
    }
}

///////////////////////////////////////////////////////////////////////////////
/// Response to a [`UpdatePeerRequest`]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum UpdatePeerResponse {
    Ok,
    ErrNotALeader,
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
        if let Some((k, v)) = iter.next() {
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
