use ::raft::prelude as raft;
use ::raft::StorageError;
use serde::Deserialize;
use serde::Serialize;
use std::convert::TryFrom;
use thiserror::Error;

use crate::Message;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Entry {
    pub entry_type: String,
    pub index: u64,
    pub term: u64,
    pub msg: Message,
}
impl ::tarantool::tuple::AsTuple for Entry {}

impl TryFrom<raft::Entry> for self::Entry {
    type Error = rmp_serde::decode::Error;

    fn try_from(e: raft::Entry) -> Result<Self, Self::Error> {
        Ok(Self {
            entry_type: format!("{:?}", e.get_entry_type()),
            index: e.get_index(),
            term: e.get_term(),
            msg: Message::try_from(e.get_data())?,
        })
    }
}

#[derive(Debug, Error, PartialEq)]
#[error("unknown entry type \"{0}\"")]
pub struct UnknownEntryType(String);
impl From<UnknownEntryType> for StorageError {
    fn from(err: UnknownEntryType) -> StorageError {
        StorageError::Other(Box::new(err))
    }
}

impl TryFrom<self::Entry> for raft::Entry {
    type Error = UnknownEntryType;

    fn try_from(row: self::Entry) -> Result<raft::Entry, Self::Error> {
        let mut ret = raft::Entry::new();

        let entry_type = match row.entry_type {
            t if t == "EntryNormal" => raft::EntryType::EntryNormal,
            t if t == "EntryConfChange" => raft::EntryType::EntryConfChange,
            t if t == "EntryConfChangeV2" => raft::EntryType::EntryConfChangeV2,
            other => return Err(UnknownEntryType(other)),
        };

        ret.set_entry_type(entry_type);
        ret.set_index(row.index);
        ret.set_term(row.term);
        let bytes: Vec<u8> = Vec::from(&row.msg);
        ret.set_data(bytes.into());

        Ok(ret)
    }
}

impl Default for Entry {
    fn default() -> Self {
        Self::try_from(raft::Entry::default()).expect("that's a bug")
    }
}

impl Entry {
    fn new(msg: Message) -> Self {
        Self {
            msg,
            ..Default::default()
        }
    }
}

inventory::submit!(crate::InnerTest {
    name: "test_traft_row_Entry",
    body: || {
        use ::tarantool::tuple::Tuple;
        use serde_json::json;

        fn ser(e: self::Entry) -> serde_json::Value {
            let t = Tuple::from_struct(&e).expect("coercing self::Entry to Tuple failed");
            t.as_struct().expect("coercing Tuple to json::Value failed")
        }

        // Serialize
        ///////////////////////////////////////////////////////////////////////
        assert_eq!(
            ser(Entry::default()),
            json!(["EntryNormal", 0, 0, ["empty"]])
        );

        assert_eq!(
            ser(Entry::new(Message::Info {
                msg: "!".to_owned()
            })),
            json!(["EntryNormal", 0, 0, ["info", "!"]])
        );

        assert_eq!(
            ser(Entry {
                entry_type: "EntryNormal".to_owned(),
                index: 1001,
                term: 1002,
                msg: Message::EvalLua {
                    code: "return nil".to_owned(),
                },
            }),
            json!(["EntryNormal", 1001, 1002, ["eval_lua", "return nil"]])
        );

        let msg = Message::Info {
            msg: "?".to_owned(),
        };
        assert_eq!(
            raft::Entry::try_from(self::Entry {
                entry_type: "EntryConfChangeV2".to_owned(),
                index: 99,
                term: 2,
                msg: msg.clone(),
            })
            .expect("coercing self::Entry to raft::Entry failed"),
            raft::Entry {
                entry_type: raft::EntryType::EntryConfChangeV2,
                index: 99,
                term: 2,
                data: Vec::<u8>::from(&msg).into(),
                ..Default::default()
            }
        );

        // Deserialize
        ///////////////////////////////////////////////////////////////////////
        let row = self::Entry {
            entry_type: "EntryUnknown".to_owned(),
            index: 0u64,
            term: 0u64,
            msg: Message::Empty,
        };
        assert_eq!(
            raft::Entry::try_from(row),
            Err(UnknownEntryType("EntryUnknown".to_owned()))
        );
    }
});
