//! Compatibility layer between Tarantool and `raft-rs`.

mod error;
mod network;
pub mod node;
mod storage;

use ::raft::prelude as raft;
use ::tarantool::tuple::AsTuple;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

use protobuf::Message as _;
use protobuf::ProtobufEnum as _;

pub use network::ConnectionPool;
pub use storage::Storage;

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
    Info { msg: String },
    /// Evaluate the code on every instance in cluster.
    EvalLua { code: String },
}

//////////////////////////////////////////////////////////////////////////////////////////
/// Serializable struct representing a member of the raft group.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Peer {
    pub raft_id: u64,
    pub peer_address: String,
    pub voter: bool,
    pub instance_id: String,
    // pub replicaset_id: String,
    // pub instance_uuid: String,
    // pub replicaset_uuid: String,
    /// `0` means it's not committed yet.
    pub commit_index: u64,
}
impl AsTuple for Peer {}

impl Peer {}

//////////////////////////////////////////////////////////////////////////////////////////
/// Serializable representation of `raft::prelude::Entry`.
///
/// See correspondig definition in `raft-rs`:
/// - <https://github.com/tikv/raft-rs/blob/v0.6.0/proto/proto/eraftpb.proto#L23>
///
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Entry {
    /// ```
    /// enum EntryType {
    ///     EntryNormal = 0;
    ///     EntryConfChange = 1;
    ///     EntryConfChangeV2 = 2;
    /// }
    /// ```
    pub entry_type: i32,
    pub index: u64,
    pub term: u64,

    /// Corresponding `entry.data`. Solely managed by `raft-rs`.
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,

    /// Corresponding `entry.payload`. Managed by the Picodata.
    pub context: Option<EntryContext>,
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
    pub lc: LogicalClock,
    pub peers: Vec<Peer>,
}

impl AsTuple for Entry {}
impl ContextCoercion for EntryContextNormal {}
impl ContextCoercion for EntryContextConfChange {}

impl Entry {
    /// Returns the logical clock value (if any)
    /// from both `EntryNormal` and `EntryConfChange`.
    fn lc(&self) -> Option<&LogicalClock> {
        match &self.context {
            None => None,
            Some(EntryContext::Normal(v)) => Some(&v.lc),
            Some(EntryContext::ConfChange(v)) => Some(&v.lc),
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

    /// Returns the iterator over contained `Vec<Peer>` if it's an `EntryConfChange`.
    fn iter_peers(&self) -> std::slice::Iter<'_, Peer> {
        match &self.context {
            Some(EntryContext::ConfChange(v)) => v.peers.iter(),
            _ => (&[]).iter(),
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
            entry_type: e.entry_type.value(),
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

impl TryFrom<self::Entry> for raft::Entry {
    type Error = error::CoercionError;

    fn try_from(row: self::Entry) -> Result<raft::Entry, Self::Error> {
        let ret = raft::Entry {
            entry_type: raft::EntryType::from_i32(row.entry_type)
                .ok_or(Self::Error::UnknownEntryType(row.entry_type))?,
            index: row.index,
            term: row.term,
            data: row.data.into(),
            context: EntryContext::write_to_bytes(row.context.as_ref()).into(),
            ..Default::default()
        };

        Ok(ret)
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
