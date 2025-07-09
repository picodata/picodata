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
use error::to_error_other;
use op::Op;
use picodata_plugin::util::msgpack_decode_bin;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::result::Result as StdResult;
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;

use protobuf::Message as _;

pub use network::ConnectionPool;
pub use raft_storage::RaftSpaceAccess;

pub type RaftId = u64;
pub type RaftTerm = u64;
pub type ResRowCount = u64;
pub type RaftIndex = u64;
pub type Address = String;
pub type Distance = u64;

pub const INIT_RAFT_TERM: RaftTerm = 1;

pub type Result<T, E = error::Error> = std::result::Result<T, E>;

::tarantool::define_str_enum! {
    /// An enumeration of all connection types that picodata supports.
    pub enum ConnectionType {
        Iproto = "iproto",
        Pgproto = "pgproto",
    }
}

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

    /// Used for identifying the connection type.
    /// For example "iproto", "pgproto".
    pub connection_type: ConnectionType,
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
    use protobuf::Enum as _;
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

impl std::fmt::Display for EntryPayload<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return match self {
            EntryPayload::NormalEmpty => f.write_str("-"),
            EntryPayload::Normal(op) => write!(f, "{op}"),
            EntryPayload::ConfChange(cc) => {
                write!(f, "{}({})", change_type(cc.change_type()), cc.node_id)
            }
            EntryPayload::ConfChangeV2(ccv2) => {
                write!(f, "{:?}(", ccv2.transition)?;
                let mut iter = ccv2.changes.iter();
                if let Some(cc) = iter.next() {
                    write!(f, "{}({})", change_type(cc.change_type()), cc.node_id)?;
                    for cc in iter.take(ccv2.changes.len() - 1) {
                        write!(f, ", {}({})", change_type(cc.change_type()), cc.node_id)?;
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
        res.set_entry_type(raft::EntryType::EntryNormal);
        res.context = self.to_raft_ctx();
        res
    }
}

impl TryFrom<&raft::Entry> for self::Entry {
    type Error = error::Error;

    fn try_from(e: &raft::Entry) -> StdResult<Self, Self::Error> {
        let ret = Self {
            entry_type: e.entry_type(),
            index: e.index,
            term: e.term,
            data: Vec::from(e.data()),
            context: EntryContext::from_raft_entry(e)?,
        };

        Ok(ret)
    }
}

impl From<self::Entry> for raft::Entry {
    fn from(row: self::Entry) -> raft::Entry {
        raft::Entry {
            entry_type: row.entry_type.into(),
            index: row.index,
            term: row.term,
            data: row.data,
            context: row.context.to_raft_ctx(),
            ..Default::default()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// RaftMessageExt
////////////////////////////////////////////////////////////////////////////////

/// This struct represents arguments for the [`proc_raft_interact`] stored
/// procedure which is used to send messages between cluster instances as part
/// of the raft algorithm.
///
/// Note that in some comments I sometimes call it "request" because it's used
/// as part of RPC requests and also because it has some quirks in how it is
/// encoded.
///
/// Most of the data in this struct comes from the raft-rs library in form of
/// [`raft::Message`]. But we also add extra information which is used only by
/// picodata for various purposes.
///
/// # Encoding format
///
/// At the IPROTO protocol level [`proc_raft_interact`] receives an array of msgpack
/// binary data:
/// - the first blob in the array is a protobuf-encoded [`raft::Message`]
/// - the second blob is the msgpack-encoded array of extra fields
///
/// See [`RaftMessageExt::encode`] and [`RaftMessageExt::decode`] functions for
/// detailed implementation.
///
/// This format is very unusual and confusing, but there's historical reasons
/// for it's existence and we must retain backwards compatibility, so it stays
/// this way at least for now.
///
/// TODO: simplify the format in the next major release <https://git.picodata.io/core/picodata/-/issues/2069>
///
/// [`proc_raft_interact`]: node::proc_raft_interact
#[derive(Default)]
pub struct RaftMessageExt {
    /// Info generated by raft-rs as part of the implementation of the raft algorithm.
    inner: raft::Message,

    /// Numeric version of the request encoding format. Must increase every time
    /// a change is made to the request encoding format.
    version: u64,

    /// Miscellaneous flags
    flags: RaftMessageFlags,

    /// Applied index of the sender.
    applied: RaftIndex,
}

bitflags::bitflags! {
    #[derive(Debug, Default, PartialEq, Eq)]
    struct RaftMessageFlags: u64 {
        /// A magic field required by bitflags! for future compatibility
        const _ = !0;
    }
}

type Flags = RaftMessageFlags;

impl RaftMessageExt {
    const FIRST_ENCODING_VERSION: u64 = 1;
    /// Don't forget to update this if the encoding format changes!
    const LATEST_ENCODING_VERSION: u64 = 2;

    #[inline(always)]
    const fn new(inner: raft::Message, applied: RaftIndex) -> Self {
        Self {
            inner,
            version: Self::LATEST_ENCODING_VERSION,
            flags: RaftMessageFlags::empty(),
            applied,
        }
    }

    fn decode(data: &[u8]) -> Result<Self> {
        let mut res = Self::default();

        let mut iter = match tarantool::msgpack::ValueIter::from_array(data) {
            Ok(v) => v,
            // NOTE: here and later we use an explicit match + return instead
            // of `map_err()?` because we use `#[track_caller]` to capture the
            // source location of the error which in case of `map_err()` will
            // show a random location in raft std library.
            Err(e) => return Err(invalid_msgpack(e).into()),
        };

        // Read the protobuf-encoded raft-rs message
        let Some(first_arg) = iter.next() else {
            return Err(invalid_msgpack("expected at least 1 argument, got none").into());
        };
        let protobuf = match msgpack_decode_bin(first_arg) {
            Ok(v) => v,
            Err(e) => return Err(invalid_msgpack(e).into()),
        };

        res.inner
            .merge_from_bytes(protobuf)
            .map_err(error::Error::other)?;

        let Some(second_arg) = iter.next() else {
            // If we only got a single value in that msgpack array, this means
            // that was the old version of the request encoding. This is needed
            // for backwards compatibility during the rolling upgrade to a newer
            // picodata version.

            res.version = Self::FIRST_ENCODING_VERSION;
            return Ok(res);
        };

        // New version of request encoding - msgpack array with elements:
        // - msgpack bin: protobuf-encoded raft-rs message
        // - msgpack bin: msgpack-encoded array with elements:
        //    - request version number
        //    - raft applied index
        //    - flags
        // NOTE: we put all the new fields into a msgpack bin for backwards
        // compatibility, because proc_raft_interact on older versions of picodata
        // expects an array of msgpack bin

        let extra_args = match msgpack_decode_bin(second_arg) {
            Ok(v) => v,
            Err(e) => return Err(invalid_msgpack(e).into()),
        };

        res.decode_extra_args(extra_args)?;

        // For future compatibility we don't check if there's more elements in
        // the array. That way we can add more arguments in the future and older
        // versions will simply ignore them.

        Ok(res)
    }

    fn decode_extra_args(&mut self, data: &[u8]) -> Result<(), BoxError> {
        let mut iter = match tarantool::msgpack::ValueIter::from_array(data) {
            Ok(v) => v,
            Err(e) => return Err(invalid_msgpack(e)),
        };

        // Read the version number
        let Some(mut first_arg) = iter.next() else {
            return Ok(());
        };
        let version = match rmp::decode::read_int(&mut first_arg) {
            Ok(v) => v,
            Err(e) => return Err(invalid_msgpack(e)),
        };
        self.version = version;

        // Read flags
        let Some(mut second_arg) = iter.next() else {
            return Ok(());
        };
        let bits: u64 = match rmp::decode::read_int(&mut second_arg) {
            Ok(v) => v,
            Err(e) => return Err(invalid_msgpack(e)),
        };
        self.flags = RaftMessageFlags::from_bits_retain(bits as _);

        // Read the applied index
        let Some(mut third_arg) = iter.next() else {
            return Ok(());
        };
        let applied = match rmp::decode::read_int(&mut third_arg) {
            Ok(v) => v,
            Err(e) => return Err(invalid_msgpack(e)),
        };
        self.applied = applied;

        // For future compatibility we don't check if there's more elements in
        // the array. That way we can add more arguments in the future and older
        // versions will simply ignore them.

        Ok(())
    }

    fn encode(&self, buffer: &mut impl std::io::Write) -> Result<(), BoxError> {
        rmp::encode::write_array_len(buffer, 2).map_err(to_error_other)?;

        // NOTE we put the raft-rs message as first value because this gives us
        // backwards compatibility for free. Older version of picodata will
        // decode this first message, handle it successfully. After that it will
        // attempt to decode the next message in the array, but will fail
        // because it's not a message but a version number. This is ok for us
        // because such errors are just logged as warnings and effectively
        // ignored otherwise, so we don't break anything. At the same time the
        // older picodata version successfully decodes the newer request encoding.

        // Write the protobuf-encoded raft-rs message
        let bin_len = self.inner.compute_size();
        rmp::encode::write_bin_len(buffer, bin_len as _).map_err(to_error_other)?;
        self.inner.write_to_writer(buffer).map_err(to_error_other)?;

        self.encode_extra_args(buffer).map_err(to_error_other)?;

        Ok(())
    }

    fn encode_extra_args(&self, buffer: &mut impl std::io::Write) -> Result<(), BoxError> {
        const N_EXTRA_ARGS: usize = 3;

        // Allocate a temporary buffer on the stack with enough capacity:
        // 1 byte for the msgack array header
        // 9 is the maximum number size of encoded integer of which there's currently 3.
        let mut temp = smallvec::SmallVec::<[u8; 1 + 9 * N_EXTRA_ARGS]>::new();

        rmp::encode::write_array_len(&mut temp, N_EXTRA_ARGS as _).map_err(to_error_other)?;

        // Write the version number
        debug_assert_eq!(self.version, Self::LATEST_ENCODING_VERSION);
        rmp::encode::write_uint(&mut temp, self.version).map_err(to_error_other)?;

        // Write flags
        let flags = self.flags.bits();
        rmp::encode::write_uint(&mut temp, flags as _).map_err(to_error_other)?;

        // Write the applied index
        rmp::encode::write_uint(&mut temp, self.applied).map_err(to_error_other)?;

        // Copy from the temporary buffer to the output buffer
        rmp::encode::write_bin(buffer, &temp).map_err(to_error_other)?;

        Ok(())
    }
}

impl tarantool::tuple::ToTupleBuffer for RaftMessageExt {
    #[inline(always)]
    fn write_tuple_data(&self, buffer: &mut impl std::io::Write) -> tarantool::Result<()> {
        self.encode(buffer)?;
        Ok(())
    }
}

#[inline(always)]
#[track_caller]
fn invalid_msgpack(error: impl ToString) -> BoxError {
    BoxError::new(TarantoolErrorCode::InvalidMsgpack, error.to_string())
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
    type Error = protobuf::Error;

    fn try_from(pb: self::MessagePb) -> StdResult<raft::Message, Self::Error> {
        let mut ret = raft::Message::default();
        ret.merge_from_bytes(&pb.0)?;
        Ok(ret)
    }
}

///////////////////////////////////////////////////////////////////////////////

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
