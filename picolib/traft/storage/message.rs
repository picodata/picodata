use serde::Deserialize;
use serde::Serialize;
use std::convert::TryFrom;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "kind")]
pub enum Message {
    Empty,
    Info { msg: String },
    EvalLua { code: String },
}

impl Default for Message {
    fn default() -> Self {
        Self::Empty
    }
}

impl TryFrom<&[u8]> for Message {
    type Error = rmp_serde::decode::Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.is_empty() {
            return Ok(Message::Empty);
        }
        rmp_serde::from_read_ref(data)
    }
}

impl From<Message> for Vec<u8> {
    fn from(msg: Message) -> Vec<u8> {
        if matches!(msg, Message::Empty) {
            return Vec::new();
        }
        let mut ser = rmp_serde::Serializer::new(Vec::new());
        msg.serialize(&mut ser).unwrap();
        ser.into_inner()
    }
}

inventory::submit!(crate::InnerTest {
    name: "test_traft_storage_Message",
    body: || {
        use ::tarantool::tuple::AsTuple;
        use ::tarantool::tuple::Tuple;

        // Test to / from tarantool::Tuple

        fn ser<T: AsTuple>(e: T) -> serde_json::Value {
            let t = Tuple::from_struct(&e).unwrap();
            t.as_struct().unwrap()
        }

        assert_eq!(ser((Message::Empty,)), serde_json::json!([["empty"]]));

        let msg = Message::Info {
            msg: "hello, serde!".to_owned(),
        };
        assert_eq!(ser((msg,)), serde_json::json!([["info", "hello, serde!"]]));

        let msg = Message::EvalLua {
            code: "return true".to_owned(),
        };
        assert_eq!(
            ser((msg,)),
            serde_json::json!([["eval_lua", "return true"]])
        );

        // Test from / to Vec<u8>

        let buf: Vec<u8> = vec![];
        assert_eq!(Message::try_from(buf.as_ref()).unwrap(), Message::Empty);

        let buf: Vec<u8> = rmp_serde::to_vec(&("info", "xxx")).unwrap();
        assert_eq!(
            Message::try_from(buf.as_ref()).unwrap(),
            Message::Info {
                msg: "xxx".to_owned()
            }
        );

        let msg = Message::EvalLua {
            code: "os.exit()".to_owned(),
        };
        let buf: Vec<u8> = msg.clone().into();
        assert_eq!(
            ("eval_lua", "os.exit()"),
            rmp_serde::from_read_ref(&buf).unwrap()
        );
        assert_eq!(Message::try_from(buf.as_ref()).unwrap(), msg,);
    }
});
