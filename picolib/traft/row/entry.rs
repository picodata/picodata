use ::raft::prelude as raft;
use serde::Deserialize;
use serde::Serialize;
use std::convert::TryFrom;
use std::convert::TryInto;

use crate::Message;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Entry {
    pub entry_type: String,
    pub index: u64,
    pub term: u64,
    pub msg: Message,
}
impl ::tarantool::tuple::AsTuple for Entry {}

impl TryFrom<&raft::Entry> for self::Entry {
    type Error = rmp_serde::decode::Error;

    fn try_from(e: &raft::Entry) -> Result<Self, Self::Error> {
        Ok(Self {
            entry_type: format!("{:?}", e.get_entry_type()),
            index: e.get_index(),
            term: e.get_term(),
            msg: e.get_data().try_into()?,
        })
    }
}

impl From<self::Entry> for raft::Entry {
    fn from(row: self::Entry) -> raft::Entry {
        let mut ret = raft::Entry::new();

        let entry_type = match row.entry_type.as_ref() {
            "EntryNormal" => raft::EntryType::EntryNormal,
            _ => unimplemented!(),
        };

        ret.set_entry_type(entry_type);
        ret.set_index(row.index);
        ret.set_term(row.term);
        let bytes: Vec<u8> = Vec::from(row.msg);
        ret.set_data(bytes.into());

        ret
    }
}

impl Default for Entry {
    fn default() -> Self {
        Self::try_from(&raft::Entry::default()).unwrap()
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
        fn ser(e: Entry) -> serde_json::Value {
            use ::tarantool::tuple::Tuple;
            let t = Tuple::from_struct(&e).unwrap();
            t.as_struct().unwrap()
        }

        assert_eq!(
            ser(Entry::default()),
            serde_json::json!(["EntryNormal", 0, 0, ["empty"]])
        );

        let msg = Message::Info {
            msg: "!".to_owned(),
        };
        assert_eq!(
            ser(Entry::new(msg)),
            serde_json::json!(["EntryNormal", 0, 0, ["info", "!"]])
        );

        let msg = Message::EvalLua {
            code: "return nil".to_owned(),
        };
        assert_eq!(
            ser(Entry {
                entry_type: "EntryNormal".to_owned(),
                index: 1001,
                term: 1002,
                msg,
            }),
            serde_json::json!(["EntryNormal", 1001, 1002, ["eval_lua", "return nil"]])
        );
    }
});
