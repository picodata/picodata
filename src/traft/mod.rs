//! Compatibility layer between Tarantool and `raft-rs`.

pub mod error;
pub(crate) mod network;
pub mod node;
pub mod op;
pub(crate) mod raft_storage;

use crate::instance::Instance;
use crate::stringify_debug;
use ::raft::prelude as raft;
use ::tarantool::tuple::Encode;
use op::Op;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::result::Result as StdResult;
use uuid::Uuid;

use protobuf::Message as _;

pub use network::ConnectionPool;
pub use raft_storage::RaftSpaceAccess;

pub type RaftId = u64;
pub type RaftTerm = u64;
pub type RaftIndex = u64;
pub type Address = String;
pub type Distance = u64;

pub const INIT_RAFT_TERM: RaftTerm = 1;

pub type Result<T> = std::result::Result<T, error::Error>;

//////////////////////////////////////////////////////////////////////////////////////////
// RaftEntryId
//////////////////////////////////////////////////////////////////////////////////////////

/// A pair of raft entry index and term. Uniquely identifies a raft log entry.
/// Defines a strict lexicographical ordering equivalent to (term, index).
#[rustfmt::skip]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RaftEntryId {
    pub index: RaftIndex,
    pub term: RaftTerm,
}
impl Encode for RaftEntryId {}

impl PartialOrd for RaftEntryId {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RaftEntryId {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.term, self.index).cmp(&(other.term, other.index))
    }
}

impl std::fmt::Display for RaftEntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[term: {}, index: {}]", self.term, self.index)
    }
}

//////////////////////////////////////////////////////////////////////////////////////////
/// Timestamps for raft read states.
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

////////////////////////////////////////////////////////////////////////////////
// ReadStateContext
////////////////////////////////////////////////////////////////////////////////

/// Context of a raft read state request. Is required to distinguish between
/// responses to different read state requests.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadStateContext {
    pub lc: LogicalClock,
}

impl ReadStateContext {
    #[inline(always)]
    fn from_raft_ctx(ctx: &[u8]) -> Result<Self> {
        let res = rmp_serde::from_slice(ctx).map_err(tarantool::error::Error::from)?;
        Ok(res)
    }

    #[inline(always)]
    fn to_raft_ctx(&self) -> Vec<u8> {
        rmp_serde::to_vec_named(self).expect("out of memory")
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
    pub context: EntryContext,
}
impl Encode for Entry {}

impl Entry {
    /// Computes the number of bytes the entry with these fields will take up
    /// when encoded into a tuple. This function is used to catch early the
    /// entries which exceed the max_tuple_size threshold.
    pub fn tuple_size(index: RaftIndex, term: RaftTerm, data: &[u8], context: &[u8]) -> usize {
        // This capacity fits any msgpack value header and then some
        const CAPACITY: usize = 16;
        let mut dummy = [0_u8; CAPACITY];

        // Msgpack array of 5 elements, header needs 1 byte
        let msgpack_header_size = 1;

        // Entry type is an enum with only 3 variants, is encoded as 1 byte
        let entry_type_size = 1;

        // Encoded size of the raft index
        let mut buf = dummy.as_mut_slice();
        rmp::encode::write_uint(&mut buf, index).expect("buffer has enough capacity");
        let index_size = CAPACITY - buf.len();

        // Encoded size of the raft term
        let mut buf = dummy.as_mut_slice();
        rmp::encode::write_uint(&mut buf, term).expect("buffer has enough capacity");
        let term_size = CAPACITY - buf.len();

        // Encoded size of the raft-rs specific data field
        let mut buf = dummy.as_mut_slice();
        rmp::encode::write_bin_len(&mut buf, data.len() as _).expect("buffer has enough capacity");
        let data_header_size = CAPACITY - buf.len();
        let data_size = data_header_size + data.len();

        // Context is already encoded as msgpack value, so we use it's length as is
        let context_size = context.len();

        msgpack_header_size + entry_type_size + index_size + term_size + data_size + context_size
    }
}

mod entry_type_as_i32 {
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
        raft::EntryType::from_i32(t)
            .ok_or_else(|| D::Error::custom(format!("unknown entry type ({t})")))
    }
}

/// Raft entry payload specific to the Picodata.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum EntryContext {
    Op(Op),
    None,
}
impl Encode for EntryContext {}

impl Entry {
    /// Returns the contained `Op` if it's an `EntryNormal`
    /// consuming `self` by value.
    pub fn into_op(self) -> Option<Op> {
        match self.context {
            EntryContext::Op(op) => Some(op),
            EntryContext::None => None,
        }
    }

    pub fn payload(&self) -> EntryPayload {
        match (self.entry_type, &self.context) {
            (raft::EntryType::EntryNormal, EntryContext::None) => {
                debug_assert!(self.data.is_empty());
                EntryPayload::NormalEmpty
            }
            (raft::EntryType::EntryNormal, EntryContext::Op(op)) => {
                debug_assert!(self.data.is_empty());
                EntryPayload::Normal(op)
            }
            (raft::EntryType::EntryConfChange, EntryContext::None) => {
                let mut cc = raft::ConfChange::default();
                cc.merge_from_bytes(&self.data).unwrap();
                EntryPayload::ConfChange(cc)
            }
            (raft::EntryType::EntryConfChangeV2, EntryContext::None) => {
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
    Normal(&'a Op),
    ConfChange(raft::ConfChange),
    ConfChangeV2(raft::ConfChangeV2),
}

impl<'a> std::fmt::Display for EntryPayload<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return match self {
            EntryPayload::NormalEmpty => f.write_str("-"),
            EntryPayload::Normal(op) => write!(f, "{}", op),
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
    #[inline]
    pub fn from_raft_entry(e: &raft::Entry) -> Result<Self> {
        if e.context.is_empty() {
            return Ok(Self::None);
        }
        let res: Self = rmp_serde::from_slice(&e.context).map_err(tarantool::error::Error::from)?;
        Ok(res)
    }

    #[inline]
    fn to_raft_ctx(&self) -> Vec<u8> {
        match self {
            Self::None => vec![],
            Self::Op(op) => rmp_serde::to_vec(op).expect("encoding may only fail due to oom"),
        }
    }

    #[inline(always)]
    fn to_raft_entry(&self) -> raft::Entry {
        let mut res = raft::Entry::new();
        res.entry_type = raft::EntryType::EntryNormal;
        res.context = self.to_raft_ctx().into();
        res
    }
}

impl TryFrom<&raft::Entry> for self::Entry {
    type Error = error::Error;

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
            context: row.context.to_raft_ctx().into(),
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

/// Generate UUID for an instance from `instance_id` (String).
/// Use Version-3 (MD5) UUID.
pub fn instance_uuid(instance_id: &str) -> String {
    static mut NAMESPACE_INSTANCE_UUID: Option<Uuid> = None;
    let ns = unsafe { NAMESPACE_INSTANCE_UUID.get_or_insert_with(|| uuid_v3("INSTANCE_UUID")) };
    let uuid = Uuid::new_v3(ns, instance_id.as_bytes());
    uuid.to_hyphenated().to_string()
}

/// Generate UUID for a replicaset from `replicaset_id` (String).
/// Use Version-3 (MD5) UUID.
pub fn replicaset_uuid(replicaset_id: &str) -> String {
    static mut NAMESPACE_REPLICASET_UUID: Option<Uuid> = None;
    let ns = unsafe { NAMESPACE_REPLICASET_UUID.get_or_insert_with(|| uuid_v3("REPLICASET_UUID")) };
    let uuid = Uuid::new_v3(ns, replicaset_id.as_bytes());
    uuid.to_hyphenated().to_string()
}

#[inline(always)]
fn uuid_v3(name: &str) -> Uuid {
    Uuid::new_v3(&Uuid::nil(), name.as_bytes())
}

#[cfg(test)]
mod test {
    use super::*;
    use tarantool::tuple::ToTupleBuffer;

    #[test]
    fn traft_entry_tuple_size_calculation() {
        let entry = Entry {
            entry_type: raft::EntryType::EntryNormal,
            index: 420,
            term: 69,
            data: vec![4, 8, 15, 16, 23, 42],
            context: EntryContext::Op(
                op::Dml::Replace {
                    table: 69105,
                    tuple: ("foo", 100500, "bar").to_tuple_buffer().unwrap(),
                    initiator: 1337,
                }
                .into(),
            ),
        };

        let entry_tuple = entry.to_tuple_buffer().unwrap();
        #[rustfmt::skip]
        eprintln!("{}", tarantool::util::DisplayAsHexBytes(entry_tuple.as_ref()));

        let encoded_context = entry.context.to_raft_ctx();
        #[rustfmt::skip]
        eprintln!("{}", tarantool::util::DisplayAsHexBytes(&encoded_context));

        let fast_size = Entry::tuple_size(entry.index, entry.term, &entry.data, &encoded_context);
        assert_eq!(entry_tuple.len(), fast_size);
    }
}
