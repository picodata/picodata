use std::borrow::Cow;
use std::collections::BTreeMap;
use rand::Rng;

use tarantool::index::{IndexFieldType, IndexOptions, IteratorType};
use tarantool::sequence::Sequence;
use tarantool::space::{Field, Space, SpaceCreateOptions, SpaceEngineType, SpaceFieldType, SystemSpace};
use tarantool::tuple::Tuple;
use tarantool::{update, upsert};
use tarantool::util::Value;

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

pub fn get() {
    let space = Space::find("test_s2").unwrap();

    let idx_1 = space.index("idx_1").unwrap();
    let output = idx_1.get(&("key_16".to_string(),)).unwrap();
    assert!(output.is_some());
    assert_eq!(
        output.unwrap().into_struct::<S2Record>().unwrap(),
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
        output.unwrap().into_struct::<S2Record>().unwrap(),
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
    let result: Vec<S1Record> = space
        .primary_key()
        .select(IteratorType::LE, &(5,))
        .unwrap()
        .map(|x| x.into_struct().unwrap())
        .collect();
    assert_eq!(
        result,
        vec![
            S1Record {
                id: 5,
                text: "key_5".to_string()
            },
            S1Record {
                id: 4,
                text: "key_4".to_string()
            },
            S1Record {
                id: 3,
                text: "key_3".to_string()
            },
            S1Record {
                id: 2,
                text: "key_2".to_string()
            },
            S1Record {
                id: 1,
                text: "key_1".to_string()
            }
        ]
    );

    let idx = space.index("idx_3").unwrap();
    let result: Vec<S2Record> = idx
        .select(IteratorType::Eq, &(3,))
        .unwrap()
        .map(|x| x.into_struct().unwrap())
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
        .map(|x| x.into_struct().unwrap())
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

    let output = result.unwrap().into_struct::<S2Record>().unwrap();
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
        result_min.unwrap().into_struct::<S2Record>().unwrap(),
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
        result_max.unwrap().into_struct::<S2Record>().unwrap(),
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
    assert_eq!(
        idx.extract_key(Tuple::from_struct(&record).unwrap())
            .into_struct::<S2Key>()
            .unwrap(),
        S2Key { id: 11, a: 1, b: 2 }
    );
}

pub fn insert() {
    let mut space = Space::find("test_s1").unwrap();
    space.truncate().unwrap();

    let input = S1Record {
        id: 1,
        text: "Test".to_string(),
    };
    let insert_result = space.insert(&input).unwrap();
    assert_eq!(
        insert_result.into_struct::<S1Record>().unwrap(),
        input
    );

    let output = space.get(&(input.id,)).unwrap();
    assert!(output.is_some());
    assert_eq!(output.unwrap().into_struct::<S1Record>().unwrap(), input);
}

pub fn replace() {
    let mut space = Space::find("test_s1").unwrap();
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
    assert_eq!(
        replace_result.into_struct::<S1Record>().unwrap(),
        new_input
    );

    let output = space.get(&(new_input.id,)).unwrap();
    assert!(output.is_some());
    assert_eq!(
        output.unwrap().into_struct::<S1Record>().unwrap(),
        new_input
    );
}

pub fn delete() {
    let mut space = Space::find("test_s1").unwrap();
    space.truncate().unwrap();

    let input = S1Record {
        id: 1,
        text: "Test".to_string(),
    };
    space.insert(&input).unwrap();

    let delete_result = space.delete(&(input.id,)).unwrap();
    assert!(delete_result.is_some());
    assert_eq!(
        delete_result.unwrap().into_struct::<S1Record>().unwrap(),
        input
    );

    let output = space.get(&(input.id,)).unwrap();
    assert!(output.is_none());
}

pub fn update() {
    let mut space = Space::find("test_s1").unwrap();
    space.truncate().unwrap();

    let input = S1Record {
        id: 1,
        text: "Original".to_string(),
    };
    space.insert(&input).unwrap();

    let update_result = space
        .update(
            &(input.id,),
            &[QueryOperation {
                op: "=".to_string(),
                field_id: 1,
                value: "New".into(),
            }],
        )
        .unwrap();
    assert!(update_result.is_some());
    assert_eq!(
        update_result
            .unwrap()
            .into_struct::<S1Record>()
            .unwrap()
            .text,
        "New"
    );

    let output = space.get(&(input.id,)).unwrap();
    assert_eq!(
        output.unwrap().into_struct::<S1Record>().unwrap().text,
        "New"
    );
}

pub fn update_macro() {
    let mut space = Space::find("test_s2").unwrap();

    let input = S2Record {
        id: 100,
        key: "Original".to_string(),
        value: "Original".to_string(),
        a: 0,
        b: 0
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

    let updated = update_result
        .unwrap()
        .into_struct::<S2Record>().unwrap();
    assert_eq!(updated.key, "New");
    assert_eq!(updated.value, "New");
    assert_eq!(updated.a, 1);
    assert_eq!(updated.b, 2);

    let output = space.get(&(input.id,))
        .unwrap().unwrap()
        .into_struct::<S2Record>().unwrap();
    assert_eq!(output.key, "New");
    assert_eq!(output.value, "New");
    assert_eq!(output.a, 1);
    assert_eq!(output.b, 2);
}

pub fn update_index_macro() {
    let mut space = Space::find("test_s2").unwrap();

    let input = S2Record {
        id: 101,
        key: "Original".to_string(),
        value: "Original".to_string(),
        a: 0,
        b: 0
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

    let updated = update_result
        .unwrap()
        .into_struct::<S2Record>().unwrap();
    assert_eq!(updated.key, "NewKey");
    assert_eq!(updated.value, "New");
    assert_eq!(updated.a, 1);
    assert_eq!(updated.b, 2);

    let output = space.get(&(input.id,))
        .unwrap().unwrap()
        .into_struct::<S2Record>().unwrap();
    assert_eq!(output.key, "NewKey");
    assert_eq!(output.value, "New");
    assert_eq!(output.a, 1);
    assert_eq!(output.b, 2);
}

pub fn upsert() {
    let mut space = Space::find("test_s1").unwrap();
    space.truncate().unwrap();

    let original_input = S1Record {
        id: 1,
        text: "Original".to_string(),
    };
    space.insert(&original_input).unwrap();

    let () = space
        .upsert(
            &S1Record {
                id: 1,
                text: "New".to_string(),
            },
            &[QueryOperation {
                op: "=".to_string(),
                field_id: 1,
                value: "Test 1".into(),
            }],
        )
        .unwrap();

    let () = space
        .upsert(
            &S1Record {
                id: 2,
                text: "New".to_string(),
            },
            &[QueryOperation {
                op: "=".to_string(),
                field_id: 1,
                value: "Test 2".into(),
            }],
        )
        .unwrap();

    let output = space.get(&(1,)).unwrap();
    assert_eq!(
        output.unwrap().into_struct::<S1Record>().unwrap().text,
        "Test 1"
    );

    let output = space.get(&(2,)).unwrap();
    assert_eq!(
        output.unwrap().into_struct::<S1Record>().unwrap().text,
        "New"
    );
}

pub fn upsert_macro() {
    let mut space = Space::find("test_s2").unwrap();

    let original_input = S2Record {
        id: 111,
        key: "test_box_upsert_macro_1".to_string(),
        value: "Original".to_string(),
        a: 0,
        b: 0,
    };
    space.insert(&original_input).unwrap();

    let () = upsert!(
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

    let () = upsert!(
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
    ).unwrap();

    let output = space.get(&(111, )).unwrap().unwrap().into_struct::<S2Record>().unwrap();
    assert_eq!(output.key, "test_box_upsert_macro_1");
    assert_eq!(output.value, "UpsertUpdated");
    assert_eq!(output.a, 1);

    let output = space.get(&(112, )).unwrap().unwrap().into_struct::<S2Record>().unwrap();
    assert_eq!(output.key, "test_box_upsert_macro_2");
    assert_eq!(output.value, "UpsertNew");
    assert_eq!(output.a, 2);
}

pub fn truncate() {
    let mut space = Space::find("test_s1").unwrap();
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
        .. Default::default()
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
        is_local: false,
        is_sync: true,
        .. Default::default()
    };

    let result = Space::create("new_space_8", &opts);
    assert_eq!(result.is_ok(), true);

    let info = Space::from(SystemSpace::Space)
        .index("name").unwrap()
        .select(IteratorType::Eq, &("new_space_8", )).unwrap()
        .next().expect("space info not found");

    #[derive(serde::Deserialize)]
    pub struct Info {
        _id: u64,
        _owner: u64,
        _name: String,
        _engine: String,
        _field_count: u64,
        flags: BTreeMap<String, bool>,
    }

    let info = info.as_struct::<Info>().unwrap();
    assert_eq!(info.flags.get("is_sync"), Some(&true));

    drop_space("new_space_8");
}

pub fn space_meta() {
    fn assert_field(field: &BTreeMap<Cow<'_, str>, Value>, name: &str, r#type: &str, is_nullable: bool) {
        assert!(matches!(field.get("is_nullable").unwrap(), Value::Bool(_)));
        match field.get("is_nullable").unwrap() {
            Value::Bool(nullable) => {
                assert_eq!(*nullable, is_nullable);
            }
            _ => unreachable!()
        }

        assert!(matches!(field.get("name").unwrap(), Value::Str(_)));
        match field.get("name").unwrap() {
            Value::Str(n) => {
                assert_eq!(n.to_string(), name.to_string());
            }
            _ => unreachable!()
        }

        assert!(matches!(field.get("type").unwrap(), Value::Str(_)));
        match field.get("type").unwrap() {
            Value::Str(t) => {
                assert_eq!(t.to_string(), r#type.to_string());
            }
            _ => unreachable!()
        }
    }

    let opts = SpaceCreateOptions {
        engine: SpaceEngineType::Memtx,
        is_local: true,
        format: Some(vec![
            Field::unsigned("f1"),
            Field::boolean("f2"),
            Field {
                name: "f3".to_string(),
                field_type: SpaceFieldType::String,
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

    let opts = SpaceCreateOptions {
        is_local: false,
        is_temporary: true,
        is_sync: true,
        format: Some(vec![Field::unsigned("f1"), ]),
        ..Default::default()
    };
    let space = Space::create("new_space_10", &opts).expect("space new_space_10 should exists");
    let meta = space.meta().expect("meta should exists");

    assert_eq!(meta.name, "new_space_10");
    assert!(matches!(meta.flags.get("temporary").unwrap(), Value::Bool(true)));
    assert!(matches!(meta.flags.get("is_sync").unwrap(), Value::Bool(true)));
}

pub fn drop_space(name: &str) {
    let result = Space::find(name).unwrap().drop();
    assert_eq!(result.is_err(), false);
}

pub fn index_parts() {
    let mut space = Space::builder("index_parts_test")
        .create().unwrap();

    let index = space.index_builder("pk")
        .part((1, IndexFieldType::Unsigned))
        .part(2)
        .create().unwrap();

    space.insert(&(1, 2, 3)).unwrap();
    space.insert(&(2, "foo")).unwrap();
    space.insert(&(3, 3.14, [3, 2, 1])).unwrap();
    space.insert(&(4,)).unwrap_err();
    space.insert(&("5", 1)).unwrap_err();

    let mut iter = index.select(tarantool::index::IteratorType::All, &())
        .unwrap();

    assert_eq!(iter.next().and_then(|t| t.as_struct().ok()), Some((1, 2, 3)));
    assert_eq!(iter.next().and_then(|t| t.as_struct().ok()), Some((2, "foo".to_string())));
    assert_eq!(iter.next().and_then(|t| t.as_struct().ok()), Some((3, 3.14, [3, 2, 1])));
    assert!(iter.next().is_none());
}
