//! Compatibility layer between Tarantool and `raft-rs`.

pub mod error;
pub mod event;
pub(crate) mod network;
pub mod node;
pub mod notify;
pub mod op;
pub(crate) mod raft_storage;
pub mod rpc;
pub mod topology;

use crate::instance::Instance;
use crate::stringify_debug;
use ::raft::prelude as raft;
use ::tarantool::tuple::Encode;
use error::CoercionError;
use op::Op;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::result::Result as StdResult;
use uuid::Uuid;

use protobuf::Message as _;

pub use network::ConnectionPool;
pub use raft_storage::RaftSpaceAccess;
pub use rpc::{join, update_instance};
pub use topology::Topology;

pub type RaftId = u64;
pub type RaftTerm = u64;
pub type RaftIndex = u64;
pub type Address = String;
pub type Distance = u64;

pub const INIT_RAFT_TERM: RaftTerm = 1;

pub type Result<T> = std::result::Result<T, error::Error>;

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
/// Serializable struct representing an address of a member of raft group
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerAddress {
    /// Used for identifying raft nodes.
    /// Must be unique in the raft group.
    pub raft_id: RaftId,

    /// Inbound address used for communication with the node.
    /// Not to be confused with listen address.
    pub address: Address,
}
impl Encode for PeerAddress {}

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
    ///     EntryNormal = 0,
    ///     EntryConfChange = 1,
    ///     EntryConfChangeV2 = 2,
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
    pub instances: Vec<Instance>,
}

impl Encode for Entry {}
impl ContextCoercion for EntryContextNormal {}
impl ContextCoercion for EntryContextConfChange {}

impl Entry {
    /// Returns the logical clock value if it's an `EntryNormal`.
    pub fn lc(&self) -> Option<LogicalClock> {
        match &self.context {
            Some(EntryContext::Normal(v)) => Some(v.lc),
            Some(EntryContext::ConfChange(_)) => None,
            None => None,
        }
    }

    /// Returns the contained `Op` if it's an `EntryNormal`
    /// consuming `self` by value.
    pub fn into_op(self) -> Option<Op> {
        match self.context {
            Some(EntryContext::Normal(v)) => Some(v.op),
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
                raft::ConfChangeType::AddNode => "AddNode",
                raft::ConfChangeType::AddLearnerNode => "AddLearnerNode",
                raft::ConfChangeType::RemoveNode => "RemoveNode",
            }
        }
    }
}

impl EntryContext {
    pub fn from_raft_entry(e: &raft::Entry) -> StdResult<Option<Self>, CoercionError> {
        if e.context.is_empty() {
            return Ok(None);
        }
        let res = match e.entry_type {
            raft::EntryType::EntryNormal => Self::Normal(ContextCoercion::from_bytes(&e.context)?),
            raft::EntryType::EntryConfChange | raft::EntryType::EntryConfChangeV2 => {
                Self::ConfChange(ContextCoercion::from_bytes(&e.context)?)
            }
        };
        Ok(Some(res))
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

    fn try_from(e: &raft::Entry) -> StdResult<Self, Self::Error> {
        let ret = Self {
            entry_type: e.entry_type,
            index: e.index,
            term: e.term,
            data: Vec::from(e.get_data()),
            context: EntryContext::from_raft_entry(e)?,
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

    fn try_from(pb: self::MessagePb) -> StdResult<raft::Message, Self::Error> {
        let mut ret = raft::Message::default();
        ret.merge_from_bytes(&pb.0)?;
        Ok(ret)
    }
}

///////////////////////////////////////////////////////////////////////////////
/// This trait allows converting `EntryContext` to / from `Vec<u8>`.
pub trait ContextCoercion: Serialize + DeserializeOwned {
    #[inline(always)]
    fn from_bytes(bytes: &[u8]) -> StdResult<Self, error::CoercionError> {
        Ok(rmp_serde::from_slice(bytes)?)
    }

    #[inline(always)]
    fn to_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec_named(self).unwrap()
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Generate UUID for an instance from `instance_id` (String).
/// Use Version-3 (MD5) UUID.
pub fn instance_uuid(instance_id: &str) -> String {
    static mut NAMESPACE_INSTANCE_UUID: Option<Uuid> = None;
    let ns = unsafe { NAMESPACE_INSTANCE_UUID.get_or_insert_with(|| uuid_v3("INSTANCE_UUID")) };
    let uuid = Uuid::new_v3(ns, instance_id.as_bytes());
    uuid.hyphenated().to_string()
}

/// Generate UUID for a replicaset from `replicaset_id` (String).
/// Use Version-3 (MD5) UUID.
pub fn replicaset_uuid(replicaset_id: &str) -> String {
    static mut NAMESPACE_REPLICASET_UUID: Option<Uuid> = None;
    let ns = unsafe { NAMESPACE_REPLICASET_UUID.get_or_insert_with(|| uuid_v3("REPLICASET_UUID")) };
    let uuid = Uuid::new_v3(ns, replicaset_id.as_bytes());
    uuid.hyphenated().to_string()
}

#[inline(always)]
fn uuid_v3(name: &str) -> Uuid {
    Uuid::new_v3(&Uuid::nil(), name.as_bytes())
}

////////////////////////////////////////////////////////////////////////////////
/// Migration
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Migration {
    pub id: u64,
    pub body: String,
}

impl Encode for Migration {}
