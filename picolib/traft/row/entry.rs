use ::raft::prelude as raft;
use serde::Deserialize;
use serde::Serialize;
use std::convert::TryFrom;

use crate::error::CoercionError;
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
    type Error = CoercionError;

    fn try_from(e: raft::Entry) -> Result<Self, Self::Error> {
        Ok(Self {
            entry_type: format!("{:?}", e.get_entry_type()),
            index: e.get_index(),
            term: e.get_term(),
            msg: Message::try_from(e.get_data())?,
        })
    }
}

impl TryFrom<self::Entry> for raft::Entry {
    type Error = CoercionError;

    fn try_from(row: self::Entry) -> Result<raft::Entry, Self::Error> {
        let mut ret = raft::Entry::new();

        let entry_type = match row.entry_type {
            t if t == "EntryNormal" => raft::EntryType::EntryNormal,
            t if t == "EntryConfChange" => raft::EntryType::EntryConfChange,
            t if t == "EntryConfChangeV2" => raft::EntryType::EntryConfChangeV2,
            other => return Err(CoercionError::UnknownEntryType(other)),
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
        use serde_json::Value as JsonValue;

        fn ser(e: self::Entry) -> JsonValue {
            Tuple::from_struct(&e)
                .expect("coercing Tuple from self::Entry failed")
                .as_struct()
                .expect("coercing json::Value from Tuple failed")
        }

        // serialize self::Entry as json
        ///////////////////////////////////////////////////////////////////////

        assert_eq!(
            ser(Entry::default()),
            json!(["EntryNormal", 0u64, 0u64, ["empty"]])
        );

        assert_eq!(
            ser(Entry::new(Message::Info {
                msg: "!".to_owned()
            })),
            json!(["EntryNormal", 0u64, 0u64, ["info", "!"]])
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
            json!(["EntryNormal", 1001u64, 1002u64, ["eval_lua", "return nil"]])
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
            .expect("coercing raft::Entry from self::Entry failed"),
            raft::Entry {
                entry_type: raft::EntryType::EntryConfChangeV2,
                index: 99,
                term: 2,
                data: Vec::<u8>::from(&msg).into(),
                ..Default::default()
            }
        );

        // raft::Entry from self::Entry
        ///////////////////////////////////////////////////////////////////////

        let row = self::Entry {
            entry_type: "EntryUnknown".to_owned(),
            ..Default::default()
        };
        assert_eq!(
            raft::Entry::try_from(row).map_err(|e| format!("{e}")),
            Err("unknown entry type \"EntryUnknown\"".to_owned())
        );

        assert_eq!(
            raft::Entry::try_from(Entry::default())
                .expect("coercing raft::Entry from self::Entry failed"),
            raft::Entry::default()
        );
    }
});
