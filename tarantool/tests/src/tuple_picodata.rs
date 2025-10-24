#![cfg(feature = "picodata")]

use tarantool::tuple::{FieldType, KeyDef, KeyDefPart, Tuple};

pub fn tuple_hash() {
    let tuple = Tuple::new(&(1, 2, 3)).unwrap();
    let key = KeyDef::new(vec![
        &KeyDefPart {
            field_no: 0,
            field_type: FieldType::Integer,
            ..Default::default()
        },
        &KeyDefPart {
            field_no: 1,
            field_type: FieldType::Integer,
            ..Default::default()
        },
    ])
    .unwrap();
    assert_eq!(key.hash(&tuple), 605624609);

    let tuple = Tuple::new(&(1,)).unwrap();
    let key = KeyDef::new(vec![&KeyDefPart {
        field_no: 0,
        field_type: FieldType::Integer,
        ..Default::default()
    }])
    .unwrap();
    assert_eq!(key.hash(&tuple), 1457374933);

    let tuple = Tuple::new(&(1,)).unwrap();
    let key = KeyDef::new(vec![
        &KeyDefPart {
            field_no: 0,
            field_type: FieldType::Integer,
            ..Default::default()
        },
        &KeyDefPart {
            field_no: 1,
            field_type: FieldType::Integer,
            is_nullable: true,
            ..Default::default()
        },
    ])
    .unwrap();
    assert_eq!(key.hash(&tuple), 766361540);
}
