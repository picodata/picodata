use std::cmp::Ordering;

use tarantool::tlua::{Index, Indexable, Nil};
use tarantool::tuple::{AsTuple, FieldType, KeyDef, KeyDefItem, Tuple, TupleBuffer};
use serde::Serialize;

use crate::common::{S1Record, S2Key, S2Record};

pub fn test_tuple_new_from_struct() {
    let input = S1Record {
        id: 1,
        text: "text".to_string(),
    };
    assert!(Tuple::from_struct(&input).is_ok());
}

pub fn new_tuple_from_flutten_struct() {
    #[derive(Serialize)]
    struct Embedded {
        b: i32,
        c: i32,
    }
    #[derive(Serialize)]
    struct FlattenStruct {
        a: i32,
        #[serde(flatten)]
        emb: Embedded,
    }
    impl AsTuple for FlattenStruct{}

    let input = FlattenStruct {
        a: 1,
        emb: Embedded {
            b: 2,
            c: 3,
        },
    };
    assert_eq!(
        Tuple::from_struct(&input).unwrap_err().to_string(),
        concat![
            "Failed to encode tuple: Invalid msgpack value (epxected array, found Map([",
            r#"(String(Utf8String { s: Ok("a") }), Integer(PosInt(1))), "#,
            r#"(String(Utf8String { s: Ok("b") }), Integer(PosInt(2))), "#,
            r#"(String(Utf8String { s: Ok("c") }), Integer(PosInt(3)))"#,
            "]))"
        ]
    )
}

pub fn tuple_buffer_from_vec_fail() {
    assert_eq!(
        TupleBuffer::try_from_vec(vec![1, 2, 3]).unwrap_err().to_string(),
        "Failed to encode tuple: Invalid msgpack value (epxected array, found Integer(PosInt(1)))"
    )
}

pub fn test_tuple_field_count() {
    // struct -> tuple
    let tuple = Tuple::from_struct(&S2Record {
        id: 1,
        key: "key".to_string(),
        value: "value".to_string(),
        a: 2,
        b: 3,
    })
    .unwrap();
    assert_eq!(tuple.len(), 5);

    // empty tuple
    let tuple = Tuple::from_struct(&()).unwrap();
    assert_eq!(tuple.len(), 0);

    // tuple w/ single field
    let tuple = Tuple::from_struct(&(0,)).unwrap();
    assert_eq!(tuple.len(), 1);

    // very long tuple
    let tuple =
        Tuple::from_struct(&(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)).unwrap();
    assert_eq!(tuple.len(), 16);
}

pub fn test_tuple_size() {
    let tuple = Tuple::from_struct(&S2Record {
        id: 1,
        key: "key".to_string(),
        value: "value".to_string(),
        a: 2,
        b: 3,
    })
    .unwrap();
    assert_eq!(tuple.bsize(), 14);
}

pub fn test_tuple_into_struct() {
    let input = S2Record {
        id: 1,
        key: "key".to_string(),
        value: "value".to_string(),
        a: 2,
        b: 3,
    };

    // 1:1 decode
    let tuple = Tuple::from_struct(&input).unwrap();
    let output: S2Record = tuple.into_struct().unwrap();
    assert_eq!(output, input);

    // partial decode (with trimming trailing fields)
    let tuple = Tuple::from_struct(&input).unwrap();
    let output: S1Record = tuple.into_struct().unwrap();
    assert_eq!(
        output,
        S1Record {
            id: 1,
            text: "key".to_string()
        }
    );
}

#[allow(clippy::redundant_clone)]
pub fn test_tuple_clone() {
    let tuple_2 = Tuple::from_struct(&S1Record {
        id: 1,
        text: "text".to_string(),
    })
    .unwrap();
    let tuple_1 = tuple_2.clone();
    assert!(tuple_1.into_struct::<S1Record>().is_ok());
}

pub fn test_tuple_iterator() {
    let tuple = Tuple::from_struct(&S1Record {
        id: 1,
        text: "text".to_string(),
    })
    .unwrap();
    let mut iterator = tuple.iter().unwrap();

    assert_eq!(iterator.next().unwrap(), Some(1));
    assert_eq!(iterator.next().unwrap(), Some("text".to_string()));
    assert_eq!(iterator.next::<()>().unwrap(), None);
}

pub fn test_tuple_iterator_seek_rewind() {
    let tuple = Tuple::from_struct(&S2Record {
        id: 1,
        key: "key".to_string(),
        value: "value".to_string(),
        a: 1,
        b: 2,
    })
    .unwrap();
    let mut iterator = tuple.iter().unwrap();

    // rewind iterator to first position
    iterator.rewind();
    assert_eq!(iterator.position(), 0);

    // rewind iterator to first position
    assert!(iterator.seek::<i32>(3).unwrap().is_some());
    assert_eq!(iterator.position(), 4);
}

pub fn test_tuple_get_format() {
    let tuple = Tuple::from_struct(&S1Record {
        id: 1,
        text: "text".to_string(),
    })
    .unwrap();
    let _ = tuple.format();
}

pub fn test_tuple_get_field() {
    let tuple = Tuple::from_struct(&S2Record {
        id: 1,
        key: "key".to_string(),
        value: "value".to_string(),
        a: 1,
        b: 2,
    })
    .unwrap();

    assert_eq!(tuple.field::<u32>(0).unwrap(), Some(1));
    assert_eq!(tuple.get(0), Some(1));
    assert_eq!(tuple.get("id"), None::<()>);
    assert_eq!(tuple.field::<String>(1).unwrap(), Some("key".to_string()));
    assert_eq!(tuple.get(1), Some("key".to_string()));
    assert_eq!(tuple.field::<String>(2).unwrap(), Some("value".to_string()));
    assert_eq!(tuple.get(2), Some("value".to_string()));
    assert_eq!(tuple.field::<i32>(3).unwrap(), Some(1));
    assert_eq!(tuple.get(3), Some(1));
    assert_eq!(tuple.field::<i32>(4).unwrap(), Some(2));
    assert_eq!(tuple.get(4), Some(2));
    assert_eq!(tuple.field::<i32>(5).unwrap(), None);
    assert_eq!(tuple.get(5), None::<()>);
    assert_eq!(tuple.get(6), None::<()>);
}

pub fn tuple_get_field_path() {
    let space = tarantool::space::Space::find("test_s2").unwrap();
    let idx_1 = space.index("idx_1").unwrap();
    let tuple = idx_1.get(&("key_16",)).unwrap().unwrap();
    assert_eq!(tuple.get("key"), Some("key_16".to_string()));
    assert_eq!(tuple.get("id"), Some(16));
    assert_eq!(tuple.get("value"), Some("value_16".to_string()));
    assert_eq!(tuple.get("a"), Some(1));
    assert_eq!(tuple.get("b"), Some(3));

    let space = tarantool::space::Space::find("with_array").unwrap();
    let idx = space.index("pk").unwrap();
    let tuple = idx.get(&(1,)).unwrap().unwrap();
    assert_eq!(tuple.get("array[0]"), None::<()>);
    assert_eq!(tuple.get("array[1]"), Some(1));
    assert_eq!(tuple.get("array[2]"), Some(2));
    assert_eq!(tuple.get("array[3]"), Some(3));
    assert_eq!(tuple.get("array[4]"), None::<()>);
    assert_eq!(tuple.get("array"), Some(vec![1, 2, 3]));

    let tuple = idx.get(&(2,)).unwrap().unwrap();
    assert_eq!(tuple.get("array[1]"), Some("foo".to_string()));
    assert_eq!(tuple.get("array[2][1]"), Some("bar".to_string()));
    assert_eq!(tuple.get("array[2][2][1]"), Some(69));
    assert_eq!(tuple.get("array[2][2][2]"), Some(420));
    assert_eq!(tuple.get("array[3]"), Some(3.14));
    assert_eq!(tuple.get("array"), Some(
            ("foo".to_string(), ("bar".to_string(), (69, 420)), 3.14)
    ));

    let lua = tarantool::lua_state();
    lua.set("tuple_get_field_path", &tuple);
    let tuple_in_lua: Indexable<_> = lua.get("tuple_get_field_path").unwrap();
    assert_eq!(tuple_in_lua.get("array[1]"), Some("foo".to_string()));
    assert_eq!(tuple_in_lua.get("array[2][1]"), Some("bar".to_string()));
    assert_eq!(tuple_in_lua.get("array[2][2][1]"), Some(69));
    assert_eq!(tuple_in_lua.get("array[2][2][2]"), Some(420));
    assert_eq!(tuple_in_lua.get("array[3]"), Some(3.14));
    lua.set("tuple_get_field_path", Nil);
}

pub fn test_tuple_compare() {
    let tuple_a = Tuple::from_struct(&S2Record {
        id: 1,
        key: "key".to_string(),
        value: "value".to_string(),
        a: 1,
        b: 2,
    })
    .unwrap();

    let tuple_b = Tuple::from_struct(&S2Record {
        id: 1,
        key: "key".to_string(),
        value: "value".to_string(),
        a: 3,
        b: 4,
    })
    .unwrap();

    let key = KeyDef::new(vec![
        KeyDefItem {
            field_id: 0,
            field_type: FieldType::Unsigned,
        },
        KeyDefItem {
            field_id: 3,
            field_type: FieldType::Integer,
        },
    ]);
    assert_eq!(key.compare(&tuple_a, &tuple_b), Ordering::Less);
}

pub fn test_tuple_compare_with_key() {
    let tuple = Tuple::from_struct(&S2Record {
        id: 1,
        key: "key".to_string(),
        value: "value".to_string(),
        a: 1,
        b: 2,
    })
    .unwrap();

    let key_value = S2Key { id: 1, a: 3, b: 4 };

    let key_def = KeyDef::new(vec![
        KeyDefItem {
            field_id: 0,
            field_type: FieldType::Unsigned,
        },
        KeyDefItem {
            field_id: 3,
            field_type: FieldType::Integer,
        },
        KeyDefItem {
            field_id: 4,
            field_type: FieldType::Integer,
        },
    ]);
    assert_eq!(key_def.compare_with_key(&tuple, &key_value), Ordering::Less);
}

pub fn to_and_from_lua() {
    let tuple = Tuple::from_struct(&S2Record {
        id: 42,
        key: "hello".into(),
        value: "nice".into(),
        a: 420,
        b: 69,
    }).unwrap();

    let lua = tarantool::lua_state();
    lua.set("to_and_from_lua", &tuple);

    let tuple_in_lua: Indexable<_> = lua.get("to_and_from_lua").unwrap();
    assert_eq!(tuple_in_lua.get(1), Some(42));
    assert_eq!(tuple_in_lua.get(2), Some("hello".to_string()));
    assert_eq!(tuple_in_lua.get(3), Some("nice".to_string()));
    assert_eq!(tuple_in_lua.get(4), Some(420));
    assert_eq!(tuple_in_lua.get(5), Some(69));
    drop(tuple_in_lua);

    let tuple: Tuple = lua.get("to_and_from_lua").unwrap();
    let res = tuple.into_struct::<S2Record>().unwrap();
    assert_eq!(res, S2Record {
        id: 42,
        key: "hello".into(),
        value: "nice".into(),
        a: 420,
        b: 69,
    });

    lua.set("to_and_from_lua", Nil);
}

pub fn tuple_debug_fmt() {
    let tuple = Tuple::from_struct(&S2Record {
        id: 42,
        key: "hello".into(),
        value: "nice".into(),
        a: 420,
        b: 69,
    }).unwrap();

    assert_eq!(format!("{:?}", tuple),
        r#"Tuple(Array([Integer(PosInt(42)), String(Utf8String { s: Ok("hello") }), String(Utf8String { s: Ok("nice") }), Integer(PosInt(420)), Integer(PosInt(69))]))"#
    );

    let tuple = Tuple::from_struct(&(1, true, "foo")).unwrap();
    let buf = TupleBuffer::from(tuple);

    assert_eq!(format!("{:?}", buf),
        r#"TupleBuffer::Vector(Tuple(Array([Integer(PosInt(1)), Boolean(true), String(Utf8String { s: Ok("foo") })])))"#
    );
}

