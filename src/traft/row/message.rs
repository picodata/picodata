use ::raft::prelude as raft;
use serde::Deserialize;
use serde::Serialize;
use std::convert::TryFrom;

use crate::error::CoercionError;
use crate::traft::row;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Message {
    // See also https://pkg.go.dev/go.etcd.io/etcd/raft/v3#hdr-MessageType
    pub msg_type: String,
    pub to: u64,
    pub from: u64,
    pub term: u64,
    pub log_term: u64,
    pub index: u64,
    pub entries: Vec<row::Entry>,
    pub commit: u64,
    pub commit_term: u64,
    // pub snapshot: SingularPtrField<Snapshot>,
    // pub request_snapshot: u64,
    pub reject: bool,
    pub reject_hint: u64,
    // pub context: Bytes,
    pub priority: u64,
}
impl ::tarantool::tuple::AsTuple for Message {}

impl TryFrom<raft::Message> for self::Message {
    type Error = CoercionError;

    fn try_from(m: raft::Message) -> Result<Self, Self::Error> {
        Ok(Self {
            msg_type: format!("{:?}", m.get_msg_type()),
            to: m.get_to(),
            from: m.get_from(),
            term: m.get_term(),
            log_term: m.get_log_term(),
            index: m.get_index(),
            commit: m.get_commit(),
            commit_term: m.get_commit_term(),
            reject: m.get_reject(),
            reject_hint: m.get_reject_hint(),
            priority: m.get_priority(),
            entries: m
                .get_entries()
                .iter()
                .map(|e| row::Entry::try_from(e.clone()))
                .collect::<Result<Vec<row::Entry>, _>>()?,
        })
    }
}

impl TryFrom<self::Message> for raft::Message {
    type Error = CoercionError;

    fn try_from(row: self::Message) -> Result<raft::Message, Self::Error> {
        let mut ret = raft::Message::new();

        let message_type = match row.msg_type {
            t if t == "MsgHup" => raft::MessageType::MsgHup,
            t if t == "MsgBeat" => raft::MessageType::MsgBeat,
            t if t == "MsgPropose" => raft::MessageType::MsgPropose,
            t if t == "MsgAppend" => raft::MessageType::MsgAppend,
            t if t == "MsgAppendResponse" => raft::MessageType::MsgAppendResponse,
            t if t == "MsgRequestVote" => raft::MessageType::MsgRequestVote,
            t if t == "MsgRequestVoteResponse" => raft::MessageType::MsgRequestVoteResponse,
            t if t == "MsgSnapshot" => raft::MessageType::MsgSnapshot,
            t if t == "MsgHeartbeat" => raft::MessageType::MsgHeartbeat,
            t if t == "MsgHeartbeatResponse" => raft::MessageType::MsgHeartbeatResponse,
            t if t == "MsgUnreachable" => raft::MessageType::MsgUnreachable,
            t if t == "MsgSnapStatus" => raft::MessageType::MsgSnapStatus,
            t if t == "MsgCheckQuorum" => raft::MessageType::MsgCheckQuorum,
            t if t == "MsgTransferLeader" => raft::MessageType::MsgTransferLeader,
            t if t == "MsgTimeoutNow" => raft::MessageType::MsgTimeoutNow,
            t if t == "MsgReadIndex" => raft::MessageType::MsgReadIndex,
            t if t == "MsgReadIndexResp" => raft::MessageType::MsgReadIndexResp,
            t if t == "MsgRequestPreVote" => raft::MessageType::MsgRequestPreVote,
            t if t == "MsgRequestPreVoteResponse" => raft::MessageType::MsgRequestPreVoteResponse,
            other => return Err(CoercionError::UnknownMessageType(other)),
        };

        ret.set_entries(
            row.entries
                .iter()
                .map(|e| raft::Entry::try_from(e.clone()))
                .collect::<Result<Vec<raft::Entry>, _>>()?
                .into(),
        );
        ret.set_msg_type(message_type);
        ret.set_to(row.to);
        ret.set_from(row.from);
        ret.set_term(row.term);
        ret.set_log_term(row.log_term);
        ret.set_index(row.index);
        ret.set_commit(row.commit);
        ret.set_commit_term(row.commit_term);
        ret.set_reject(row.reject);
        ret.set_reject_hint(row.reject_hint);
        ret.set_priority(row.priority);

        Ok(ret)
    }
}

impl Default for Message {
    fn default() -> Self {
        Self::try_from(raft::Message::default()).expect("that's a bug")
    }
}

inventory::submit!(crate::InnerTest {
    name: "test_traft_row_Message",
    body: || {
        use ::tarantool::tuple::Tuple;
        use serde_json::json;
        use serde_json::Value as JsonValue;

        fn ser(m: self::Message) -> JsonValue {
            Tuple::from_struct(&m)
                .expect("coercing Tuple from self::Message failed")
                .as_struct()
                .expect("coercing json::Value from Tuple failed")
        }

        // serialize self::Message as json
        ///////////////////////////////////////////////////////////////////////

        let z = 0u64;
        assert_eq!(
            ser(Message::default()),
            json!(["MsgHup", z, z, z, z, z, [], z, z, false, z, z])
        );

        // raft::Message from self::Message
        ///////////////////////////////////////////////////////////////////////

        assert_eq!(
            raft::Message::try_from(Message::default())
                .expect("coercing raft::Message from self::Message failed"),
            raft::Message::default()
        );

        assert_eq!(
            raft::Message::try_from(Message {
                msg_type: "MsgUnknown".into(),
                ..Default::default()
            })
            .map_err(|e| format!("{e}")),
            Err("unknown message type \"MsgUnknown\"".into())
        );

        assert_eq!(
            raft::Message::try_from(Message {
                entries: vec![row::Entry {
                    entry_type: "EntryUnknown".into(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .map_err(|e| format!("{e}")),
            Err("unknown entry type \"EntryUnknown\"".into())
        );
    }
});
