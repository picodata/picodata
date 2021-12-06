use std::cmp::Ordering;

use tarantool::hlua::AsLua;
use tarantool::tuple::{FieldType, KeyDef, KeyDefItem, Tuple};

use crate::common::{S1Record, S2Key, S2Record};

pub fn test_tuple_new_from_struct() {
    let input = S1Record {
        id: 1,
        text: "text".to_string(),
    };
    assert!(Tuple::from_struct(&input).is_ok());
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

    // tuple w/ single field
    let tuple = Tuple::from_struct(&(0,)).unwrap();
    assert_eq!(tuple.len(), 1);

    // empty tuple
    let tuple = Tuple::from_struct::<Vec<()>>(&vec![]).unwrap();
    assert_eq!(tuple.len(), 0);
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

pub fn test_tuple_clone() {
    let tuple_1 = {
        let tuple_2 = Tuple::from_struct(&S1Record {
            id: 1,
            text: "text".to_string(),
        })
        .unwrap();
        tuple_2.clone()
    };
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
    assert_eq!(tuple.field::<String>(1).unwrap(), Some("key".to_string()));
    assert_eq!(tuple.field::<String>(2).unwrap(), Some("value".to_string()));
    assert_eq!(tuple.field::<i32>(3).unwrap(), Some(1));
    assert_eq!(tuple.field::<i32>(4).unwrap(), Some(2));
    assert_eq!(tuple.field::<i32>(5).unwrap(), None);
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

    let lua = tarantool::global_lua();
    let lua = lua.push(tuple);
    let tuple = lua.read::<Tuple>().unwrap();
    let res = tuple.into_struct::<S2Record>().unwrap();
    assert_eq!(res, S2Record {
        id: 42,
        key: "hello".into(),
        value: "nice".into(),
        a: 420,
        b: 69,
    });
}

