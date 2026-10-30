#![cfg(feature = "picodata")]

use std::borrow::Cow;
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

pub fn key_hash() {
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
    // The same hash as of the tuple `[1, 2, 3]` in `tuple_hash`.
    assert_eq!(unsafe { key.hash_key_raw(b"\x01\x02") }, 605624609);

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
    // The same hash as of the tuple `[1]` with the nullable field missing in
    // `tuple_hash`: the key holds an explicit nil for that field.
    assert_eq!(unsafe { key.hash_key_raw(b"\x01\xc0") }, 766361540);
}

/// The raw key hashes as `tuple`, from which its fields are extracted.
fn check_key_hash(parts: &[KeyDefPart], tuple: &[u8], key: &[u8]) {
    let key_def = KeyDef::new(parts).unwrap();
    let tuple = Tuple::try_from_slice(tuple).unwrap();
    let hash = unsafe { key_def.hash_key_raw(key) };
    assert_eq!(hash, key_def.hash(&tuple), "key: {key:x?}");
}

pub fn key_hash_matches_tuple_hash() {
    let part = |field_no, field_type| KeyDefPart {
        field_no,
        field_type,
        ..Default::default()
    };

    // A string is hashed without its MsgPack header, so its width doesn't matter.
    let parts = [part(0, FieldType::String)];
    check_key_hash(&parts, b"\x91\xa3abc", b"\xa3abc");
    check_key_hash(&parts, b"\x91\xa3abc", b"\xd9\x03abc");

    // The fields go in the order of the parts, not of the tuple.
    let parts = [part(2, FieldType::Integer), part(0, FieldType::Integer)];
    check_key_hash(&parts, b"\x93\x01\x02\x03", b"\x03\x01");

    // The key holds the field the path points to.
    let parts = [KeyDefPart {
        path: Some(Cow::Borrowed(c"x")),
        ..part(0, FieldType::Integer)
    }];
    check_key_hash(&parts, b"\x91\x81\xa1x\x01", b"\x01");

    // An integral floating point number is hashed as an integer.
    let parts = [part(0, FieldType::Number)];
    check_key_hash(&parts, b"\x91\x02", b"\xcb\x40\x00\x00\x00\x00\x00\x00\x00");
    check_key_hash(&parts, b"\x91\x02", b"\xca\x40\x00\x00\x00");

    // The collation of a part applies to the key.
    let parts = [KeyDefPart {
        collation: Some(Cow::Borrowed(c"unicode_ci")),
        ..part(0, FieldType::String)
    }];
    check_key_hash(&parts, b"\x91\xa3ABC", b"\xa3abc");
}
