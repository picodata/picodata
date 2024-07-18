use rand::Rng;
use std::borrow::Cow;
use std::collections::BTreeMap;

use tarantool::index::{self, IndexOptions, IteratorType};
use tarantool::sequence::Sequence;
use tarantool::space::UpdateOps;
use tarantool::space::{self, Field, Space, SystemSpace};
use tarantool::space::{SpaceCreateOptions, SpaceEngineType, SpaceType};
use tarantool::test::util::on_scope_exit;
use tarantool::tuple::Tuple;
use tarantool::util::Value;
use tarantool::{update, upsert};

use crate::common::{QueryOperation, S1Record, S2Key, S2Record};

pub fn space_get_by_name() {
    assert!(Space::find("test_s1").is_some());
    assert!(Space::find("test_s1_invalid").is_none());
}

pub fn space_get_by_name_cached() {
    assert!(Space::find_cached("test_s1").is_some());
    assert!(Space::find_cached("test_s1").is_some());
    assert!(Space::find_cached("test_s1_invalid").is_none());
}

pub fn space_cache_invalidated() {
    const SPACE_NAME: &str = "test_space_cache_invalidated_space";
    Space::builder(SPACE_NAME).create().unwrap();
    let space = Space::find_cached(SPACE_NAME).unwrap();
    space.drop().unwrap();

    // `space` is invalid due to stale cache
    let space = Space::find_cached(SPACE_NAME).unwrap();
    let msg = space.get(&[1]).unwrap_err().to_string();
    const HEAD: &str = "box error: NoSuchSpace: Space '";
    const TAIL: &str = "' does not exist";
    assert_eq!(&msg[..HEAD.len()], HEAD);
    assert_eq!(&msg[msg.len() - TAIL.len()..], TAIL);

    // refresh the cache
    tarantool::space::clear_cache();
    assert!(Space::find_cached(SPACE_NAME).is_none());
}

pub fn space_get_system() {
    let space: Space = SystemSpace::Space.into();
    assert!(space.len().is_ok());
}

pub fn index_get_by_name() {
    let space = Space::find("test_s2").unwrap();
    assert!(space.index("idx_1").is_some());
    assert!(space.index("idx_1_invalid").is_none());
}

pub fn index_get_by_name_cached() {
    let space = Space::find("test_s2").unwrap();
    assert!(space.index_cached("idx_1").is_some());
    assert!(space.index_cached("idx_1").is_some());
    assert!(space.index_cached("idx_1_invalid").is_none());
}

pub fn index_cache_invalidated() {
    const INDEX_NAME: &str = "test_index_cache_invalidated_index";
    const SPACE_NAME: &str = "test_index_cache_invalidated_space";
    let space = Space::builder(SPACE_NAME).create().unwrap();
    space.index_builder(INDEX_NAME).create().unwrap();
    let index = space.index_cached(INDEX_NAME).unwrap();
    index.drop().unwrap();

    // `index` is invalid due to stale cache
    let index = space.index_cached(INDEX_NAME).unwrap();
    assert_eq!(
        index.get(&[1]).unwrap_err().to_string(),
        format!("box error: NoSuchIndexID: No index #0 is defined in space '{SPACE_NAME}'")
    );

    // refresh the cache
    tarantool::space::clear_cache();
    assert!(space.index_cached(INDEX_NAME).is_none());
}

pub fn get() {
    let space = Space::find("test_s2").unwrap();

    let idx_1 = space.index("idx_1").unwrap();
    let output = idx_1.get(&("key_16".to_string(),)).unwrap();
    assert!(output.is_some());
    assert_eq!(
        output.unwrap().decode::<S2Record>().unwrap(),
        S2Record {
            id: 16,
            key: "key_16".to_string(),
            value: "value_16".to_string(),
            a: 1,
            b: 3
        }
    );

    let idx_2 = space.index("idx_2").unwrap();
    let output = idx_2.get(&S2Key { id: 17, a: 2, b: 3 }).unwrap();
    assert!(output.is_some());
    assert_eq!(
        output.unwrap().decode::<S2Record>().unwrap(),
        S2Record {
            id: 17,
            key: "key_17".to_string(),
            value: "value_17".to_string(),
            a: 2,
            b: 3
        }
    );
}

pub fn select() {
    let space = Space::find("test_s2").unwrap();
    let result: Vec<S2Record> = space
        .primary_key()
        .select(IteratorType::LE, &(5,))
        .unwrap()
        .map(|x| x.decode().unwrap())
        .collect();
    assert_eq!(
        result,
        vec![
            S2Record {
                id: 5,
                key: "key_5".to_string(),
                value: "value_5".to_string(),
                a: 0,
                b: 1
            },
            S2Record {
                id: 4,
                key: "key_4".to_string(),
                value: "value_4".to_string(),
                a: 4,
                b: 0
            },
            S2Record {
                id: 3,
                key: "key_3".to_string(),
                value: "value_3".to_string(),
                a: 3,
                b: 0
            },
            S2Record {
                id: 2,
                key: "key_2".to_string(),
                value: "value_2".to_string(),
                a: 2,
                b: 0
            },
            S2Record {
                id: 1,
                key: "key_1".to_string(),
                value: "value_1".to_string(),
                a: 1,
                b: 0
            }
        ]
    );

    let idx = space.index("idx_3").unwrap();
    let result: Vec<S2Record> = idx
        .select(IteratorType::Eq, &(3,))
        .unwrap()
        .map(|x| x.decode().unwrap())
        .collect();
    assert_eq!(
        result,
        vec![
            S2Record {
                id: 3,
                key: "key_3".to_string(),
                value: "value_3".to_string(),
                a: 3,
                b: 0
            },
            S2Record {
                id: 8,
                key: "key_8".to_string(),
                value: "value_8".to_string(),
                a: 3,
                b: 1
            },
            S2Record {
                id: 13,
                key: "key_13".to_string(),
                value: "value_13".to_string(),
                a: 3,
                b: 2
            },
            S2Record {
                id: 18,
                key: "key_18".to_string(),
                value: "value_18".to_string(),
                a: 3,
                b: 3
            },
        ]
    );
}

pub fn select_composite_key() {
    let space = Space::find("test_s2").unwrap();
    let idx = space.index("idx_2").unwrap();

    let result: Vec<S2Record> = idx
        .select(IteratorType::Eq, &(3, 3, 0))
        .unwrap()
        .map(|x| x.decode().unwrap())
        .collect();
    assert_eq!(
        result,
        vec![S2Record {
            id: 3,
            key: "key_3".to_string(),
            value: "value_3".to_string(),
            a: 3,
            b: 0
        }]
    );
}

pub fn len() {
    let space = Space::find("test_s2").unwrap();
    assert_eq!(space.len().unwrap(), 20_usize);
}

pub fn random() {
    let space = Space::find("test_s2").unwrap();
    let idx = space.primary_key();
    let mut rng = rand::thread_rng();

    let result = idx.random(rng.gen()).unwrap();
    assert!(result.is_some());

    let output = result.unwrap().decode::<S2Record>().unwrap();
    assert_eq!(output.a, (output.id as i32) % 5);
    assert_eq!(output.b, (output.id as i32) / 5);
    assert_eq!(output.key, format!("key_{}", output.id));
    assert_eq!(output.value, format!("value_{}", output.id));
}

pub fn min_max() {
    let space = Space::find("test_s2").unwrap();
    let idx = space.index("idx_3").unwrap();

    let result_min = idx.min(&(3,)).unwrap();
    assert!(result_min.is_some());
    assert_eq!(
        result_min.unwrap().decode::<S2Record>().unwrap(),
        S2Record {
            id: 3,
            key: "key_3".to_string(),
            value: "value_3".to_string(),
            a: 3,
            b: 0
        },
    );

    let result_max = idx.max(&(3,)).unwrap();
    assert!(result_max.is_some());
    assert_eq!(
        result_max.unwrap().decode::<S2Record>().unwrap(),
        S2Record {
            id: 18,
            key: "key_18".to_string(),
            value: "value_18".to_string(),
            a: 3,
            b: 3
        },
    );
}

pub fn count() {
    let space = Space::find("test_s2").unwrap();
    assert_eq!(
        space.primary_key().count(IteratorType::LE, &(7,),).unwrap(),
        7_usize
    );
    assert_eq!(
        space.primary_key().count(IteratorType::GT, &(7,),).unwrap(),
        13_usize
    );
}

#[allow(deprecated)]
pub fn extract_key() {
    let space = Space::find("test_s2").unwrap();
    let idx = space.index("idx_2").unwrap();
    let record = S2Record {
        id: 11,
        key: "key_11".to_string(),
        value: "value_11".to_string(),
        a: 1,
        b: 2,
    };
    let key: Tuple = unsafe { idx.extract_key(Tuple::new(&record).unwrap()) };
    assert_eq!(key.decode::<S2Key>().unwrap(), S2Key { id: 11, a: 1, b: 2 });
    let tuple = idx.get(&key).unwrap().unwrap();
    assert_eq!(tuple.decode::<S2Record>().unwrap(), record);
}

pub fn insert() {
    let space = Space::find("test_s1").unwrap();
    space.truncate().unwrap();

    let input = S1Record {
        id: 1,
        text: "Test".to_string(),
    };
    let insert_result = space.insert(&input).unwrap();
    assert_eq!(insert_result.decode::<S1Record>().unwrap(), input);

    let output = space.get(&(input.id,)).unwrap();
    assert!(output.is_some());
    assert_eq!(output.unwrap().decode::<S1Record>().unwrap(), input);
}

pub fn replace() {
    let space = Space::find("test_s1").unwrap();
    space.truncate().unwrap();

    let original_input = S1Record {
        id: 1,
        text: "Original".to_string(),
    };
    space.insert(&original_input).unwrap();

    let new_input = S1Record {
        id: original_input.id,
        text: "New".to_string(),
    };
    let replace_result = space.replace(&new_input).unwrap();
    assert_eq!(replace_result.decode::<S1Record>().unwrap(), new_input);

    let output = space.get(&(new_input.id,)).unwrap();
    assert!(output.is_some());
    assert_eq!(output.unwrap().decode::<S1Record>().unwrap(), new_input);
}

pub fn delete() {
    let space = Space::find("test_s1").unwrap();
    space.truncate().unwrap();

    // Insert a test tuple into the space.
    let input = S1Record {
        id: 334,
        text: "Test".to_string(),
    };
    space.insert(&input).unwrap();

    // Delete the tuple we just inserted.
    let delete_result = space.delete(&(input.id,)).unwrap();
    assert!(delete_result.is_some());
    assert_eq!(delete_result.unwrap().decode::<S1Record>().unwrap(), input);

    // Tuple is no longer in the space.
    let output = space.get(&(input.id,)).unwrap();
    assert!(output.is_none());

    // If id isn't found in the space, delete returns Ok(None).
    let invalid_id = 0xdead_beef_u32;
    let res = space.delete(&[invalid_id]).unwrap();
    assert!(res.is_none());
}

pub fn update() {
    let space = Space::find("test_s1").unwrap();
    space.truncate().unwrap();

    // Insert a test tuple into the space.
    let input = S1Record {
        id: 361,
        text: "Original".to_string(),
    };
    space.insert(&input).unwrap();

    // Update the tuple we just inserted.
    let update_result = space.update(&[input.id], [("=", 1, "New")]).unwrap();
    assert!(update_result.is_some());
    assert_eq!(
        update_result.unwrap().decode::<S1Record>().unwrap().text,
        "New"
    );

    // Tuple was updated.
    let output = space.get(&(input.id,)).unwrap();
    assert_eq!(output.unwrap().decode::<S1Record>().unwrap().text, "New");

    // If id isn't found in the space, update returns Ok(None).
    let invalid_id = 0xdead_beef_u32;
    let res = space.update(&[invalid_id], [("=", 1, "New")]).unwrap();
    assert!(res.is_none());
}

pub fn update_macro() {
    let space = Space::find("test_s2").unwrap();

    let input = S2Record {
        id: 100,
        key: "Original".to_string(),
        value: "Original".to_string(),
        a: 0,
        b: 0,
    };
    space.put(&input).unwrap();

    let update_result = update!(
        space,
        (input.id,),
        &("=", "key", "New"),
        ("=", "value", "New"),
        ("=", "a", 1),
        &("=", "b", 2),
    )
    .unwrap();
    assert!(update_result.is_some());

    let updated = update_result.unwrap().decode::<S2Record>().unwrap();
    assert_eq!(updated.key, "New");
    assert_eq!(updated.value, "New");
    assert_eq!(updated.a, 1);
    assert_eq!(updated.b, 2);

    let output = space
        .get(&(input.id,))
        .unwrap()
        .unwrap()
        .decode::<S2Record>()
        .unwrap();
    assert_eq!(output.key, "New");
    assert_eq!(output.value, "New");
    assert_eq!(output.a, 1);
    assert_eq!(output.b, 2);
}

pub fn update_index_macro() {
    let space = Space::find("test_s2").unwrap();

    let input = S2Record {
        id: 101,
        key: "Original".to_string(),
        value: "Original".to_string(),
        a: 0,
        b: 0,
    };
    space.put(&input).unwrap();

    let update_result = update!(
        space.index("primary").unwrap(),
        (input.id,),
        ("=", "key", "NewKey"),
        ("=", "a", 1),
    )
    .unwrap();
    assert!(update_result.is_some());
    let update_result = update!(
        space.index("idx_1").unwrap(),
        ("NewKey",),
        ("=", "value", "New"),
        ("=", "b", 2),
    )
    .unwrap();
    assert!(update_result.is_some());

    let updated = update_result.unwrap().decode::<S2Record>().unwrap();
    assert_eq!(updated.key, "NewKey");
    assert_eq!(updated.value, "New");
    assert_eq!(updated.a, 1);
    assert_eq!(updated.b, 2);

    let output = space
        .get(&(input.id,))
        .unwrap()
        .unwrap()
        .decode::<S2Record>()
        .unwrap();
    assert_eq!(output.key, "NewKey");
    assert_eq!(output.value, "New");
    assert_eq!(output.a, 1);
    assert_eq!(output.b, 2);
}

pub fn update_ops() {
    let space = Space::builder("update_ops_test_space").create().unwrap();
    space.index_builder("pk").create().unwrap();

    space.insert(&(1, 0)).unwrap();

    assert_eq!(
        space
            .get(&[1])
            .unwrap()
            .unwrap()
            .decode::<(i32, i32)>()
            .unwrap(),
        (1, 0),
    );

    space
        .update(&[1], UpdateOps::new().add(1, 13).unwrap())
        .unwrap();

    assert_eq!(
        space
            .get(&[1])
            .unwrap()
            .unwrap()
            .decode::<(i32, i32)>()
            .unwrap(),
        (1, 13),
    );

    space
        .update(
            &[1],
            UpdateOps::new().insert(-1, 69).unwrap().sub(1, 8).unwrap(),
        )
        .unwrap();

    assert_eq!(
        space
            .get(&[1])
            .unwrap()
            .unwrap()
            .decode::<(i32, i32, i32)>()
            .unwrap(),
        (1, 5, 69),
    );

    space
        .update(
            &[1],
            UpdateOps::new()
                .insert(-1, "hello")
                .unwrap()
                .insert(-1, "there")
                .unwrap()
                .insert(-1, "pal")
                .unwrap()
                .delete(-2, 2)
                .unwrap()
                .insert(-1, "world")
                .unwrap(),
        )
        .unwrap();

    assert_eq!(
        space
            .get(&[1])
            .unwrap()
            .unwrap()
            .decode::<(i32, i32, i32, String, String)>()
            .unwrap(),
        (1, 5, 69, "hello".to_string(), "world".to_string()),
    );

    space
        .update(
            &[1],
            UpdateOps::new()
                .and(1, 0b100)
                .unwrap()
                .xor(2, 0b101)
                .unwrap()
                .assign(3, 0)
                .unwrap()
                .splice(4, 1, 2, "i")
                .unwrap(),
        )
        .unwrap();

    assert_eq!(
        space
            .get(&[1])
            .unwrap()
            .unwrap()
            .decode::<(i32, i32, i32, i32, String)>()
            .unwrap(),
        (1, 4, 64, 0, "wild".to_string()),
    );

    space
        .update(
            &[1],
            UpdateOps::new()
                .delete(1, 2)
                .unwrap()
                .or(1, 420)
                .unwrap()
                .delete(2, 9999)
                .unwrap(),
        )
        .unwrap();

    assert_eq!(
        space
            .get(&[1])
            .unwrap()
            .unwrap()
            .decode::<(i32, i32)>()
            .unwrap(),
        (1, 420),
    );
}

pub fn upsert() {
    let space = Space::find("test_s1").unwrap();
    space.truncate().unwrap();

    let original_input = S1Record {
        id: 1,
        text: "Original".to_string(),
    };
    space.insert(&original_input).unwrap();

    space
        .upsert(
            &S1Record {
                id: 1,
                text: "New".to_string(),
            },
            [QueryOperation {
                op: "=".to_string(),
                field_id: 1,
                value: "Test 1".into(),
            }],
        )
        .unwrap();

    space
        .upsert(
            &S1Record {
                id: 2,
                text: "New".to_string(),
            },
            [QueryOperation {
                op: "=".to_string(),
                field_id: 1,
                value: "Test 2".into(),
            }],
        )
        .unwrap();

    let output = space.get(&(1,)).unwrap();
    assert_eq!(output.unwrap().decode::<S1Record>().unwrap().text, "Test 1");

    let output = space.get(&(2,)).unwrap();
    assert_eq!(output.unwrap().decode::<S1Record>().unwrap().text, "New");
}

pub fn upsert_macro() {
    let space = Space::find("test_s2").unwrap();

    let original_input = S2Record {
        id: 111,
        key: "test_box_upsert_macro_1".to_string(),
        value: "Original".to_string(),
        a: 0,
        b: 0,
    };
    space.insert(&original_input).unwrap();

    upsert!(
        space,
        &(S2Record {
            id: 111,
            key: "does not matter".to_string(),
            value: "UpsertNew".to_string(),
            a: 2,
            b: 2
        }),
        ("=", "value", "UpsertUpdated"),
        ("=", "a", 1),
    )
    .unwrap();

    upsert!(
        space,
        &S2Record {
            id: 112,
            key: "test_box_upsert_macro_2".to_string(),
            value: "UpsertNew".to_string(),
            a: 2,
            b: 2
        },
        ("=", "key", "UpsertUpdated"),
        ("=", "a", 1),
    )
    .unwrap();

    let output = space
        .get(&(111,))
        .unwrap()
        .unwrap()
        .decode::<S2Record>()
        .unwrap();
    assert_eq!(output.key, "test_box_upsert_macro_1");
    assert_eq!(output.value, "UpsertUpdated");
    assert_eq!(output.a, 1);

    let output = space
        .get(&(112,))
        .unwrap()
        .unwrap()
        .decode::<S2Record>()
        .unwrap();
    assert_eq!(output.key, "test_box_upsert_macro_2");
    assert_eq!(output.value, "UpsertNew");
    assert_eq!(output.a, 2);
}

pub fn truncate() {
    let space = Space::find("test_s1").unwrap();
    space.truncate().unwrap();

    assert_eq!(space.len().unwrap(), 0_usize);
    for i in 0..10 {
        space
            .insert(&S1Record {
                id: i,
                text: "Test".to_string(),
            })
            .unwrap();
    }
    assert_eq!(space.len().unwrap(), 10_usize);
    space.truncate().unwrap();
    assert_eq!(space.len().unwrap(), 0_usize);
}

pub fn sequence_get_by_name() {
    assert!(Sequence::find("test_seq").unwrap().is_some());
    assert!(Sequence::find("test_seq_invalid").unwrap().is_none());
}

pub fn sequence_iterate() {
    let mut seq = Sequence::find("test_seq").unwrap().unwrap();
    seq.reset().unwrap();
    assert_eq!(seq.next().unwrap(), 1);
    assert_eq!(seq.next().unwrap(), 2);
}

pub fn sequence_set() {
    let mut seq = Sequence::find("test_seq").unwrap().unwrap();
    seq.reset().unwrap();
    assert_eq!(seq.next().unwrap(), 1);

    seq.set(99).unwrap();
    assert_eq!(seq.next().unwrap(), 100);
}

pub fn space_create_opt_default() {
    let opts = SpaceCreateOptions::default();

    // Create space with default options.
    let result = Space::create("new_space_1", &opts);
    assert_eq!(result.is_ok(), true);

    drop_space("new_space_1");
}

pub fn space_create_opt_if_not_exists() {
    let mut opts = SpaceCreateOptions::default();
    let _result = Space::create("new_space_2", &opts);

    // Test `SpaceExists` error.
    let result_1 = Space::create("new_space_2", &opts);
    assert_eq!(result_1.is_err(), true);

    // Test `if_not_exists` option.
    opts.if_not_exists = true;
    let result_2 = Space::create("new_space_2", &opts);
    assert_eq!(result_2.is_err(), false);

    drop_space("new_space_2");
}

pub fn space_create_id_increment() {
    let opts = SpaceCreateOptions::default();
    let _result = Space::create("new_space_3", &opts);
    let mut prev_id = Space::find("new_space_3").unwrap().id();
    for i in 302..306 {
        let name = format!("new_space_{}", i);
        let result = Space::create(name.as_str(), &opts);
        let curr_id = result.unwrap().id();
        assert_eq!(prev_id + 1, curr_id);
        prev_id = curr_id;
    }

    drop_space("new_space_3");
    for i in 302..306 {
        let name = format!("new_space_{}", i);
        drop_space(name.as_str());
    }
}

#[allow(clippy::field_reassign_with_default)]
pub fn space_create_opt_user() {
    let mut opts = SpaceCreateOptions::default();

    // Test `user` option.
    opts.user = Some("admin".to_string());
    let result_1 = Space::create("new_space_4", &opts);
    assert_eq!(result_1.is_ok(), true);

    // Test `NoSuchUser` error.
    opts.user = Some("user".to_string());
    let result_2 = Space::create("new_space_5", &opts);
    assert_eq!(result_2.is_err(), true);

    drop_space("new_space_4");
}

pub fn space_create_opt_id() {
    let opts = SpaceCreateOptions {
        id: Some(10000),
        ..Default::default()
    };

    let result_1 = Space::create("new_space_6", &opts);
    let id = result_1.unwrap().id();
    assert_eq!(id, opts.id.unwrap());

    drop_space("new_space_6");
}

pub fn space_drop() {
    let opts = SpaceCreateOptions::default();

    for i in 400..406 {
        // Create space and drop it.
        let name = format!("new_space_{}", i);
        let _create_result = Space::create(name.as_str(), &opts);
        drop_space(name.as_str());
        // Check that space has been poperly removed.
        let find_result = Space::find(name.as_str());
        assert_eq!(find_result.is_none(), true);
    }
}

pub fn index_create_drop() {
    let space_opts = SpaceCreateOptions::default();
    let space = Space::create("new_space_7", &space_opts).unwrap();

    let index_opts = IndexOptions::default();
    let create_result = space.create_index("new_index", &index_opts);

    if create_result.is_err() {
        panic!("{:?}", create_result.err());
    }
    assert_eq!(create_result.is_ok(), true);

    let index_query_1 = space.index("new_index");
    assert_eq!(index_query_1.is_some(), true);

    let drop_result = index_query_1.unwrap().drop();
    assert_eq!(drop_result.is_ok(), true);

    let index_query_2 = space.index("new_index");
    assert_eq!(index_query_2.is_none(), true);

    drop_space("new_space_7");
}

pub fn space_create_is_sync() {
    let opts = SpaceCreateOptions {
        space_type: SpaceType::Synchronous,
        ..Default::default()
    };

    let result = Space::create("new_space_8", &opts);
    assert!(result.is_ok());
    let space = result.unwrap();

    let info = space.meta().unwrap();

    let is_sync_value = info.flags.get("is_sync").unwrap();
    assert!(matches!(is_sync_value, tarantool::util::Value::Bool(_)));
    match is_sync_value {
        tarantool::util::Value::Bool(v) => assert!(v),
        _ => unreachable!("value is bool"),
    };

    drop_space("new_space_8");
}

pub fn space_meta() {
    fn assert_field(
        field: &BTreeMap<Cow<'_, str>, Value>,
        name: &str,
        r#type: &str,
        is_nullable: bool,
    ) {
        assert!(matches!(field.get("is_nullable").unwrap(), Value::Bool(_)));
        match field.get("is_nullable").unwrap() {
            Value::Bool(nullable) => {
                assert_eq!(*nullable, is_nullable);
            }
            _ => unreachable!(),
        }

        assert!(matches!(field.get("name").unwrap(), Value::Str(_)));
        match field.get("name").unwrap() {
            Value::Str(n) => {
                assert_eq!(n.to_string(), name.to_string());
            }
            _ => unreachable!(),
        }

        assert!(matches!(field.get("type").unwrap(), Value::Str(_)));
        match field.get("type").unwrap() {
            Value::Str(t) => {
                assert_eq!(t.to_string(), r#type.to_string());
            }
            _ => unreachable!(),
        }
    }

    let opts = SpaceCreateOptions {
        engine: SpaceEngineType::Memtx,
        space_type: SpaceType::DataLocal,
        format: Some(vec![
            Field::unsigned("f1"),
            Field::boolean("f2"),
            Field {
                name: "f3".to_string(),
                field_type: space::FieldType::String,
                is_nullable: true,
            },
        ]),
        ..Default::default()
    };

    let space = Space::create("new_space_9", &opts).expect("space new_space_9 should exists");
    let meta = space.meta().expect("meta should exists");

    assert_eq!(meta.name, "new_space_9");
    assert_eq!(meta.engine, SpaceEngineType::Memtx);
    assert!(matches!(meta.flags.get("group_id").unwrap(), Value::Num(1)));

    assert_field(meta.format.get(0).unwrap(), "f1", "unsigned", false);
    assert_field(meta.format.get(1).unwrap(), "f2", "boolean", false);
    assert_field(meta.format.get(2).unwrap(), "f3", "string", true);

    let space = Space::builder("new_space_10")
        .space_type(SpaceType::DataTemporary)
        .format([Field::unsigned("f1")])
        .create()
        .unwrap();
    let meta = space.meta().expect("meta should exists");

    assert_eq!(meta.name, "new_space_10");
    assert!(matches!(
        meta.flags.get("temporary").unwrap(),
        Value::Bool(true)
    ));
}

pub fn drop_space(name: &str) {
    let result = Space::find(name).unwrap().drop();
    assert_eq!(result.is_err(), false);
}

pub fn index_parts() {
    let space = Space::builder("index_parts_test").create().unwrap();

    let index = space
        .index_builder("pk")
        .part((1, index::FieldType::Unsigned))
        .part(2)
        .create()
        .unwrap();

    space.insert(&(1, 2, 3)).unwrap();
    space.insert(&(2, "foo")).unwrap();
    space.insert(&(3, 3.14, [3, 2, 1])).unwrap();
    space.insert(&(4,)).unwrap_err();
    space.insert(&("5", 1)).unwrap_err();

    let mut iter = index
        .select(tarantool::index::IteratorType::All, &())
        .unwrap();

    assert_eq!(iter.next().and_then(|t| t.decode().ok()), Some((1, 2, 3)));
    assert_eq!(
        iter.next().and_then(|t| t.decode().ok()),
        Some((2, "foo".to_string()))
    );
    assert_eq!(
        iter.next().and_then(|t| t.decode().ok()),
        Some((3, 3.14, [3, 2, 1]))
    );
    assert!(iter.next().is_none());
}

pub fn fully_temporary_space() {
    let lua = tarantool::lua_state();
    lua.exec("box.cfg { read_only = true }").unwrap();
    let _guard = on_scope_exit(|| {
        tarantool::lua_state()
            .exec("box.cfg { read_only = false }")
            .unwrap();
    });

    // Data-temporary space cannot be created when read_only = true
    let err = Space::builder("data-temporary")
        .space_type(SpaceType::DataTemporary)
        .create()
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "box error: Readonly: Can't modify data on a read-only instance - box.cfg.read_only is true"
    );

    // But fully-temporary space can
    let space = Space::builder("fully-temporary")
        .space_type(SpaceType::Temporary)
        .create()
        .unwrap();

    assert_eq!(
        space.meta().unwrap().flags.get("type"),
        Some(&Value::Str("temporary".into()))
    );

    // Index creation also works
    let index = space.index_builder("pk").create().unwrap();

    // Inserting obviously works, because the space is data-temporary
    space.put(&(1, 2, 3)).unwrap();
    let row = space
        .select(IteratorType::All, &())
        .unwrap()
        .map(|t| t.decode::<(i32, i32, i32)>().unwrap())
        .next()
        .unwrap();
    assert_eq!(row, (1, 2, 3));

    // Truncating also works
    space.truncate().unwrap();
    let count = space.select(IteratorType::All, &()).unwrap().count();
    assert_eq!(count, 0);

    // Drop space and index works
    index.drop().unwrap();
    space.drop().unwrap();

    // The next available id in the fully-temporary range is given out each time
    let space_1 = Space::builder("t1")
        .space_type(SpaceType::Temporary)
        .create()
        .unwrap();
    assert!(dbg!(space_1.id()) > 0x3fff_ffff);

    let space_2 = Space::builder("t2")
        .space_type(SpaceType::Temporary)
        .create()
        .unwrap();
    assert_eq!(space_2.id(), space_1.id() + 1);

    let space_3 = Space::builder("t3")
        .space_type(SpaceType::Temporary)
        .create()
        .unwrap();
    assert_eq!(space_3.id(), space_1.id() + 2);

    // Id of space "t2" is now free but we don't start filling the holes until ids overflow
    space_2.drop().unwrap();
    let space_4 = Space::builder("t4")
        .space_type(SpaceType::Temporary)
        .create()
        .unwrap();
    assert_eq!(space_4.id(), space_1.id() + 3);

    // Take the maximum space id to force ids to start filling the holes
    let space_5 = Space::builder("t5")
        .id(tarantool::space::SPACE_ID_MAX)
        .space_type(SpaceType::Temporary)
        .create()
        .unwrap();
    assert_eq!(space_5.id(), tarantool::space::SPACE_ID_MAX);

    // Now the id of space "t2" is taken
    let space_6 = Space::builder("t6")
        .space_type(SpaceType::Temporary)
        .create()
        .unwrap();
    assert_eq!(space_6.id(), space_1.id() + 1);

    space_1.drop().unwrap();
    space_3.drop().unwrap();
    space_4.drop().unwrap();
    space_5.drop().unwrap();
    space_6.drop().unwrap();
}
