use raft::prelude::Entry as RaftEntry;
use raft::prelude::EntryType as RaftEntryType;
use serde::Deserialize;
use serde::Serialize;
use std::convert::TryFrom;
use std::convert::TryInto;

use super::Message;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RaftEntryRow {
    pub entry_type: String,
    pub index: u64,
    pub term: u64,
    pub msg: Message,
}
impl ::tarantool::tuple::AsTuple for RaftEntryRow {}

impl TryFrom<&RaftEntry> for RaftEntryRow {
    type Error = rmp_serde::decode::Error;

    fn try_from(e: &RaftEntry) -> Result<Self, Self::Error> {
        Ok(RaftEntryRow {
            entry_type: format!("{:?}", e.get_entry_type()),
            index: e.get_index(),
            term: e.get_term(),
            msg: e.get_data().try_into()?,
        })
    }
}

impl From<RaftEntryRow> for RaftEntry {
    fn from(row: RaftEntryRow) -> RaftEntry {
        let mut ret = RaftEntry::new();

        let entry_type = match row.entry_type.as_ref() {
            "EntryNormal" => RaftEntryType::EntryNormal,
            _ => unreachable!(),
        };

        ret.set_entry_type(entry_type);
        ret.set_index(row.index);
        ret.set_term(row.term);
        let bytes: Vec<u8> = Vec::from(row.msg);
        ret.set_data(bytes.into());

        ret
    }
}

impl Default for RaftEntryRow {
    fn default() -> Self {
        Self::try_from(&RaftEntry::default()).unwrap()
    }
}

impl RaftEntryRow {
    fn new(msg: Message) -> Self {
        Self {
            msg,
            ..Default::default()
        }
    }
}

inventory::submit!(crate::InnerTest {
    name: "test_traft_storage_RaftEntryRow",
    body: || {
        fn ser(e: RaftEntryRow) -> serde_json::Value {
            use ::tarantool::tuple::Tuple;
            let t = Tuple::from_struct(&e).unwrap();
            t.as_struct().unwrap()
        }

        assert_eq!(
            ser(RaftEntryRow::default()),
            serde_json::json!(["EntryNormal", 0, 0, ["empty"]])
        );

        let msg = Message::Info {
            msg: "!".to_owned(),
        };
        assert_eq!(
            ser(RaftEntryRow::new(msg)),
            serde_json::json!(["EntryNormal", 0, 0, ["info", "!"]])
        );

        let msg = Message::EvalLua {
            code: "return nil".to_owned(),
        };
        assert_eq!(
            ser(RaftEntryRow {
                entry_type: "EntryNormal".to_owned(),
                index: 1001,
                term: 1002,
                msg,
            }),
            serde_json::json!(["EntryNormal", 1001, 1002, ["eval_lua", "return nil"]])
        );
    }
});
