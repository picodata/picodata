use super::*;
use crate::ir::tests::{column_integer_user_non_null, vcolumn_integer_user_non_null};
use pretty_assertions::assert_eq;
use tarantool::decimal;
use tarantool::decimal::Decimal;
#[cfg(feature = "mock")]
use {
    crate::executor::engine::mock::RouterRuntimeMock,
    crate::ir::transformation::redistribution::{MotionKey, Target},
    std::collections::HashMap,
};

/// Test initialisation of the virtual table.
#[test]
fn virtual_table_1() {
    let mut vtable = VirtualTable::new();

    vtable.add_column(vcolumn_integer_user_non_null());

    vtable.add_tuple(vec![Value::from(1_u64)]);

    vtable.set_alias("test");

    let expected = VirtualTable {
        columns: vec![column_integer_user_non_null(SmolStr::from("COL_1"))],
        tuples: vec![vec![Value::from(1_u64)]],
        name: Some(SmolStr::from("test")),
        primary_key: None,
        bucket_index: VTableIndex::new(),
    };

    assert_eq!(expected, vtable);
}

/// Test resharding.
#[cfg(feature = "mock")]
#[test]
fn virtual_table_2() {
    let mut vtable = VirtualTable::new();
    vtable.add_column(vcolumn_integer_user_non_null());
    vtable.add_column(vcolumn_integer_user_non_null());
    let tuple1 = vec![Value::from(1_u64), Value::from(2_u64)];
    let tuple2 = vec![Value::from(3_u64), Value::from(4_u64)];
    vtable.add_tuple(tuple1.clone());
    vtable.add_tuple(tuple2.clone());
    vtable.set_alias("t");

    let engine = RouterRuntimeMock::new();
    let key = MotionKey {
        targets: vec![Target::Reference(0)],
    };
    vtable.reshard(&key, &engine).unwrap();

    let expected = VirtualTable {
        columns: vec![
            column_integer_user_non_null(SmolStr::from("COL_1")),
            column_integer_user_non_null(SmolStr::from("COL_2")),
        ],
        tuples: vec![tuple1, tuple2],
        name: Some(SmolStr::from("t")),
        primary_key: None,
        bucket_index: VTableIndex {
            value: HashMap::from_iter(vec![(1301, vec![1]), (3940, vec![0])]),
        },
    };

    assert_eq!(expected, vtable);
}

/// Test resharding virtual table with a primary key.
#[cfg(feature = "mock")]
#[test]
fn virtual_table_3() {
    let mut vtable = VirtualTable::new();
    vtable.add_column(vcolumn_integer_user_non_null());
    vtable.add_column(vcolumn_integer_user_non_null());
    let tuple1 = vec![Value::from(1_u64), Value::from(2_u64)];
    let tuple2 = vec![Value::from(3_u64), Value::from(4_u64)];
    vtable.add_tuple(tuple1.clone());
    vtable.add_tuple(tuple2.clone());
    vtable.set_alias("t");
    vtable.set_primary_key(&[1]).unwrap();

    let engine = RouterRuntimeMock::new();
    let key = MotionKey {
        targets: vec![Target::Reference(0)],
    };
    vtable.reshard(&key, &engine).unwrap();

    let expected = VirtualTable {
        columns: vec![
            column_integer_user_non_null(SmolStr::from("COL_1")),
            column_integer_user_non_null(SmolStr::from("COL_2")),
        ],
        tuples: vec![tuple1, tuple2],
        name: Some(SmolStr::from("t")),
        primary_key: Some(vec![1]),
        bucket_index: VTableIndex {
            value: HashMap::from_iter(vec![(1301, vec![1]), (3940, vec![0])]),
        },
    };

    assert_eq!(expected, vtable);
}

#[cfg(feature = "mock")]
#[test]
fn vtable_rearrange_for_update() {
    let mut vtable = VirtualTable::new();

    // t: a (pk) b (shard key)
    let pk_value = Value::from(1_u64);
    let new_sh_key_value = Value::from(1_u64);
    let old_sh_key_value = Value::from(2_u64);
    let tuple = vec![
        pk_value.clone(),
        new_sh_key_value.clone(),
        old_sh_key_value.clone(),
    ];
    vtable.add_column(vcolumn_integer_user_non_null());
    vtable.add_column(vcolumn_integer_user_non_null());
    vtable.set_alias("t");
    vtable.add_tuple(tuple);

    vtable.set_primary_key(&[0]).unwrap();

    let engine = RouterRuntimeMock::new();

    let old_shard_cols_len = 1;
    let new_shard_cols_positions: Vec<usize> = vec![1];
    vtable
        .rearrange_for_update(&engine, old_shard_cols_len, &new_shard_cols_positions)
        .unwrap();

    let mut expected_index = VTableIndex::new();
    let delete_tuple_bucket = engine.determine_bucket_id(&[&old_sh_key_value]).unwrap();
    let insert_tuple_bucket = engine.determine_bucket_id(&[&new_sh_key_value]).unwrap();
    expected_index.add_entry(delete_tuple_bucket, 1);
    expected_index.add_entry(insert_tuple_bucket, 0);

    let expected = VirtualTable {
        columns: vec![
            column_integer_user_non_null(SmolStr::from("COL_1")),
            column_integer_user_non_null(SmolStr::from("COL_2")),
        ],
        tuples: vec![vec![pk_value.clone(), new_sh_key_value], vec![pk_value]],
        name: Some(SmolStr::from("t")),
        primary_key: Some(vec![0]),
        bucket_index: expected_index,
    };

    assert_eq!(expected, vtable);
}

#[test]
fn vtable_add_missing_from1() {
    let mut vtable = VirtualTable::new();

    // t: a (pk)
    vtable.add_column(vcolumn_integer_user_non_null());
    vtable.add_column(vcolumn_integer_user_non_null());
    vtable.set_alias("t");
    vtable.add_tuple(vec![Value::from(1_u64), Value::from(3_u64)]);
    vtable.add_tuple(vec![Value::from(1_u64), Value::from(4_u64)]);
    vtable.add_tuple(vec![Value::from(2_u64), Value::from(5_u64)]);

    vtable.set_primary_key(&[0]).unwrap();

    let mut from_vtable = VirtualTable::new();
    from_vtable.add_column(vcolumn_integer_user_non_null());
    from_vtable.set_alias("s");
    from_vtable.add_tuple(vec![Value::from(1_u64)]);
    from_vtable.add_tuple(vec![Value::from(1_u64)]);
    from_vtable.add_tuple(vec![Value::from(3_u64)]);

    vtable.add_missing_rows(&Rc::new(from_vtable)).unwrap();

    let expected_index = VTableIndex::new();

    let expected = VirtualTable {
        columns: vec![
            column_integer_user_non_null(SmolStr::from("COL_1")),
            column_integer_user_non_null(SmolStr::from("COL_2")),
        ],
        tuples: vec![
            vec![Value::from(1_u64), Value::from(3_u64)],
            vec![Value::from(1_u64), Value::from(4_u64)],
            vec![Value::from(2_u64), Value::from(5_u64)],
            vec![Value::from(3_u64), Value::Null],
        ],
        name: Some(SmolStr::from("t")),
        primary_key: Some(vec![0]),
        bucket_index: expected_index,
    };

    assert_eq!(expected, vtable);
}

#[test]
fn vtable_add_missing_from2() {
    let mut vtable = VirtualTable::new();

    // t: a (pk)
    vtable.add_column(vcolumn_integer_user_non_null());
    vtable.add_column(vcolumn_integer_user_non_null());
    vtable.set_alias("t");
    vtable.add_tuple(vec![Value::from(1_u64), Value::from(3_u64)]);
    vtable.add_tuple(vec![Value::from(2_u64), Value::from(4_u64)]);

    vtable.set_primary_key(&[0]).unwrap();

    let mut from_vtable = VirtualTable::new();
    from_vtable.add_column(vcolumn_integer_user_non_null());
    from_vtable.set_alias("s");
    from_vtable.add_tuple(vec![Value::from(1_u64)]);
    from_vtable.add_tuple(vec![Value::from(2_u64)]);
    from_vtable.add_tuple(vec![Value::from(3_u64)]);
    from_vtable.add_tuple(vec![Value::from(3_u64)]);
    from_vtable.add_tuple(vec![Value::from(3_u64)]);

    vtable.add_missing_rows(&Rc::new(from_vtable)).unwrap();

    let expected_index = VTableIndex::new();

    let expected = VirtualTable {
        columns: vec![
            column_integer_user_non_null(SmolStr::from("COL_1")),
            column_integer_user_non_null(SmolStr::from("COL_2")),
        ],
        tuples: vec![
            vec![Value::from(1_u64), Value::from(3_u64)],
            vec![Value::from(2_u64), Value::from(4_u64)],
            vec![Value::from(3_u64), Value::Null],
            vec![Value::from(3_u64), Value::Null],
            vec![Value::from(3_u64), Value::Null],
        ],
        name: Some(SmolStr::from("t")),
        primary_key: Some(vec![0]),
        bucket_index: expected_index,
    };

    assert_eq!(expected, vtable);
}

#[test]
fn vtable_remove_duplicates1() {
    let mut vtable = VirtualTable::new();

    // t: a (pk)
    vtable.add_column(vcolumn_integer_user_non_null());
    vtable.add_column(vcolumn_integer_user_non_null());
    vtable.set_alias("t");
    vtable.add_tuple(vec![Value::from(1_u64), Value::from(2_u64)]);
    vtable.add_tuple(vec![Value::from(1_u64), Value::from(2_u64)]);

    vtable.set_primary_key(&[0]).unwrap();

    vtable.remove_duplicates();

    let expected_index = VTableIndex::new();

    let expected = VirtualTable {
        columns: vec![
            column_integer_user_non_null(SmolStr::from("COL_1")),
            column_integer_user_non_null(SmolStr::from("COL_2")),
        ],
        tuples: vec![vec![Value::from(1_u64), Value::from(2_u64)]],
        name: Some(SmolStr::from("t")),
        primary_key: Some(vec![0]),
        bucket_index: expected_index,
    };

    assert_eq!(expected, vtable);
}

#[test]
fn vtable_remove_duplicates2() {
    let mut vtable = VirtualTable::new();

    // t: a (pk)
    vtable.add_column(vcolumn_integer_user_non_null());
    vtable.add_column(vcolumn_integer_user_non_null());
    vtable.set_alias("t");

    vtable.set_primary_key(&[0]).unwrap();

    vtable.remove_duplicates();

    let expected_index = VTableIndex::new();

    let expected = VirtualTable {
        columns: vec![
            column_integer_user_non_null(SmolStr::from("COL_1")),
            column_integer_user_non_null(SmolStr::from("COL_2")),
        ],
        tuples: vec![],
        name: Some(SmolStr::from("t")),
        primary_key: Some(vec![0]),
        bucket_index: expected_index,
    };

    assert_eq!(expected, vtable);
}

#[test]
fn vtable_remove_duplicates3() {
    let mut vtable = VirtualTable::new();

    // t: a (pk)
    vtable.add_column(vcolumn_integer_user_non_null());
    vtable.add_column(vcolumn_integer_user_non_null());
    vtable.set_alias("t");
    vtable.add_tuple(vec![Value::from(1_u64), Value::from(2_u64)]);
    vtable.add_tuple(vec![Value::from(1_u64), Value::Null]);
    vtable.add_tuple(vec![Value::from(1_u64), Value::from(2_u64)]);

    vtable.set_primary_key(&[0]).unwrap();

    vtable.remove_duplicates();

    let expected_index = VTableIndex::new();

    let expected = VirtualTable {
        columns: vec![
            column_integer_user_non_null(SmolStr::from("COL_1")),
            column_integer_user_non_null(SmolStr::from("COL_2")),
        ],
        tuples: vec![
            vec![Value::from(1_u64), Value::Null],
            vec![Value::from(1_u64), Value::from(2_u64)],
        ],
        name: Some(SmolStr::from("t")),
        primary_key: Some(vec![0]),
        bucket_index: expected_index,
    };

    assert_eq!(expected, vtable);
}

#[test]
fn vtable_values_types_casting_single_tuple() {
    let mut actual_vtable = VirtualTable::new();

    actual_vtable.add_column(vcolumn_integer_user_non_null());
    actual_vtable.add_tuple(vec![Value::Unsigned(1)]);
    let vtable_types = actual_vtable.get_types();
    let unified_types = calculate_unified_types(&vtable_types).unwrap();
    actual_vtable.cast_values(&unified_types).unwrap();

    let mut expected_vtable = VirtualTable::new();
    expected_vtable.add_column(VTableColumn {
        r#type: DerivedType::new(UnrestrictedType::Integer),
        role: ColumnRole::User,
        is_nullable: false,
    });
    expected_vtable.add_tuple(vec![Value::Integer(1)]);

    assert_eq!(actual_vtable, expected_vtable)
}

#[test]
fn vtable_values_types_casting_two_tuples() {
    let mut actual_vtable = VirtualTable::new();

    actual_vtable.add_column(vcolumn_integer_user_non_null());
    actual_vtable.add_tuple(vec![Value::Unsigned(1)]);
    actual_vtable.add_tuple(vec![Value::Integer(1)]);
    let vtable_types = actual_vtable.get_types();
    let unified_types = calculate_unified_types(&vtable_types).unwrap();
    actual_vtable.cast_values(&unified_types).unwrap();

    let mut expected_vtable = VirtualTable::new();
    expected_vtable.add_column(VTableColumn {
        r#type: DerivedType::new(UnrestrictedType::Integer),
        role: ColumnRole::User,
        is_nullable: false,
    });
    expected_vtable.add_tuple(vec![Value::Integer(1)]);
    expected_vtable.add_tuple(vec![Value::Integer(1)]);

    assert_eq!(actual_vtable, expected_vtable)
}

#[test]
fn vtable_values_types_casting_two_tuples_err() {
    let mut actual_vtable = VirtualTable::new();

    actual_vtable.add_column(vcolumn_integer_user_non_null());
    actual_vtable.add_tuple(vec![Value::Unsigned(1)]);
    actual_vtable.add_tuple(vec![Value::String("name".into())]);
    let vtable_types = actual_vtable.get_types();
    let err = calculate_unified_types(&vtable_types).unwrap_err();
    assert_eq!(
        true,
        err.to_string()
            .contains("Unable to unify inconsistent types: Integer and String.")
    );
}

#[test]
fn vtable_values_types_casting_two_columns() {
    let mut actual_vtable = VirtualTable::new();

    actual_vtable.add_column(vcolumn_integer_user_non_null());
    actual_vtable.add_column(vcolumn_integer_user_non_null());
    actual_vtable.add_tuple(vec![Value::Unsigned(1), Value::Integer(1)]);
    let vtable_types = actual_vtable.get_types();
    let unified_types = calculate_unified_types(&vtable_types).unwrap();
    actual_vtable.cast_values(&unified_types).unwrap();

    let mut expected_vtable = VirtualTable::new();
    expected_vtable.add_column(VTableColumn {
        r#type: DerivedType::new(UnrestrictedType::Integer),
        role: ColumnRole::User,
        is_nullable: false,
    });
    expected_vtable.add_column(VTableColumn {
        r#type: DerivedType::new(UnrestrictedType::Integer),
        role: ColumnRole::User,
        is_nullable: false,
    });
    expected_vtable.add_tuple(vec![Value::Integer(1), Value::Integer(1)]);

    assert_eq!(actual_vtable, expected_vtable)
}

#[test]
fn vtable_values_types_casting_two_columns_two_tuples() {
    let mut actual_vtable = VirtualTable::new();

    actual_vtable.add_column(vcolumn_integer_user_non_null());
    actual_vtable.add_column(vcolumn_integer_user_non_null());
    actual_vtable.add_tuple(vec![Value::Unsigned(1), Value::Integer(1)]);
    actual_vtable.add_tuple(vec![Value::from(Decimal::from(2)), Value::Integer(1)]);
    let vtable_types = actual_vtable.get_types();
    let unified_types = calculate_unified_types(&vtable_types).unwrap();
    actual_vtable.cast_values(&unified_types).unwrap();

    let mut expected_vtable = VirtualTable::new();
    expected_vtable.add_column(VTableColumn {
        r#type: DerivedType::new(UnrestrictedType::Decimal),
        role: ColumnRole::User,
        is_nullable: false,
    });
    expected_vtable.add_column(VTableColumn {
        r#type: DerivedType::new(UnrestrictedType::Integer),
        role: ColumnRole::User,
        is_nullable: false,
    });
    expected_vtable.add_tuple(vec![Value::from(Decimal::from(1)), Value::Integer(1)]);
    expected_vtable.add_tuple(vec![Value::from(Decimal::from(2)), Value::Integer(1)]);

    assert_eq!(actual_vtable, expected_vtable)
}

#[test]
fn vtable_values_types_casting_two_columns_with_nulls() {
    let mut actual_vtable = VirtualTable::new();

    actual_vtable.add_column(vcolumn_integer_user_non_null());
    actual_vtable.add_column(vcolumn_integer_user_non_null());
    actual_vtable.add_tuple(vec![Value::Unsigned(1), Value::Null]);
    actual_vtable.add_tuple(vec![Value::Null, Value::Null]);
    actual_vtable.add_tuple(vec![Value::from(Decimal::from(2)), Value::Null]);
    let vtable_types = actual_vtable.get_types();
    let unified_types = calculate_unified_types(&vtable_types).unwrap();
    actual_vtable.cast_values(&unified_types).unwrap();

    let mut expected_vtable = VirtualTable::new();
    expected_vtable.add_column(VTableColumn {
        r#type: DerivedType::new(UnrestrictedType::Decimal),
        role: ColumnRole::User,
        is_nullable: true,
    });
    expected_vtable.add_column(VTableColumn {
        r#type: DerivedType::unknown(),
        role: ColumnRole::User,
        is_nullable: true,
    });
    expected_vtable.add_tuple(vec![Value::from(Decimal::from(1)), Value::Null]);
    expected_vtable.add_tuple(vec![Value::Null, Value::Null]);
    expected_vtable.add_tuple(vec![Value::from(Decimal::from(2)), Value::Null]);

    assert_eq!(actual_vtable, expected_vtable)
}

#[test]
fn vtable_values_types_casting_two_columns_numerical() {
    let mut actual_vtable = VirtualTable::new();

    actual_vtable.add_column(vcolumn_integer_user_non_null());
    actual_vtable.add_column(vcolumn_integer_user_non_null());
    actual_vtable.add_tuple(vec![Value::Unsigned(1), Value::Integer(1)]);
    actual_vtable.add_tuple(vec![Value::Null, Value::Unsigned(5)]);
    actual_vtable.add_tuple(vec![Value::Integer(1), Value::Null]);
    actual_vtable.add_tuple(vec![
        Value::Double(4.2_f64.into()),
        Value::from(decimal!(5.4)),
    ]);
    actual_vtable.add_tuple(vec![
        Value::from(Decimal::from(2)),
        Value::Double(0.5_f64.into()),
    ]);
    let vtable_types = actual_vtable.get_types();
    let unified_types = calculate_unified_types(&vtable_types).unwrap();
    actual_vtable.cast_values(&unified_types).unwrap();

    let mut expected_vtable = VirtualTable::new();
    expected_vtable.add_column(VTableColumn {
        r#type: DerivedType::new(UnrestrictedType::Decimal),
        role: ColumnRole::User,
        is_nullable: true,
    });
    expected_vtable.add_column(VTableColumn {
        r#type: DerivedType::new(UnrestrictedType::Decimal),
        role: ColumnRole::User,
        is_nullable: true,
    });
    expected_vtable.add_tuple(vec![
        Value::from(Decimal::from(1)),
        Value::from(Decimal::from(1)),
    ]);
    expected_vtable.add_tuple(vec![Value::Null, Value::from(Decimal::from(5))]);
    expected_vtable.add_tuple(vec![Value::from(Decimal::from(1)), Value::Null]);
    expected_vtable.add_tuple(vec![Value::from(decimal!(4.2)), Value::from(decimal!(5.4))]);
    expected_vtable.add_tuple(vec![
        Value::from(Decimal::from(2)),
        Value::from(decimal!(0.5)),
    ]);

    assert_eq!(actual_vtable, expected_vtable)
}
