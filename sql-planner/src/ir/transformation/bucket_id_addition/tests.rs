use crate::collection;
use crate::executor::bucket::Buckets;
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::executor::ExecutingQuery;
use pretty_assertions::assert_eq;

#[test]
fn test_bucket_id_addition1() {
    // Primary key is (a), sharding key is (a).
    let query = r#"SELECT "a" FROM "t5" WHERE "a" = 42"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let buckets = {
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        query.bucket_discovery(top).unwrap()
    };
    query
        .get_mut_exec_plan()
        .get_mut_ir_plan()
        .add_condition_on_bucket_id(&buckets)
        .unwrap();
    let query_explain = query.get_exec_plan().get_ir_plan().as_explain().unwrap();

    insta::assert_snapshot!(query_explain, @r#"
    projection ("t5"."a"::int -> "a")
        selection ("t5"."bucket_id"::int in ROW(5815::int)) and ("t5"."a"::int = 42::int)
            scan "t5"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    assert_eq!(Buckets::Filtered(collection!(5815)), buckets);
}

#[test]
fn test_bucket_id_addition2() {
    // Primary key is (a), sharding key is (a).
    let query = r#"SELECT "a" FROM "t5" WHERE "a" IN (42, 43)"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let buckets = {
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        query.bucket_discovery(top).unwrap()
    };
    query
        .get_mut_exec_plan()
        .get_mut_ir_plan()
        .add_condition_on_bucket_id(&buckets)
        .unwrap();
    let query_explain = query.get_exec_plan().get_ir_plan().as_explain().unwrap();

    insta::assert_snapshot!(query_explain, @r#"
    projection ("t5"."a"::int -> "a")
        selection ("t5"."bucket_id"::int in ROW(5815::int, 7100::int)) and (("t5"."a"::int = 42::int) or ("t5"."a"::int = 43::int))
            scan "t5"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    assert_eq!(Buckets::Filtered(collection!(5815, 7100)), buckets);
}

#[test]
fn test_bucket_id_addition3() {
    // Do not apply the transformation because
    // "bucket_id" is not first field in the table.
    // Primary key is (id).
    let query = r#"SELECT "id" FROM "history" WHERE "id" = 42"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let buckets = {
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        query.bucket_discovery(top).unwrap()
    };
    query
        .get_mut_exec_plan()
        .get_mut_ir_plan()
        .add_condition_on_bucket_id(&buckets)
        .unwrap();
    let query_explain = query.get_exec_plan().get_ir_plan().as_explain().unwrap();

    insta::assert_snapshot!(query_explain, @r#"
    projection ("history"."id"::int -> "id")
        selection "history"."id"::int = 42::int
            scan "history"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    assert_eq!(Buckets::Filtered(collection!(5815)), buckets);
}

#[test]
fn test_bucket_id_addition4() {
    // Primary key is (a, b), sharding key is (a, b).
    let query = r#"SELECT "a" FROM "t6" WHERE "a" = '42' AND "b" IN (24,25)"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let buckets = {
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        query.bucket_discovery(top).unwrap()
    };
    query
        .get_mut_exec_plan()
        .get_mut_ir_plan()
        .add_condition_on_bucket_id(&buckets)
        .unwrap();
    let query_explain = query.get_exec_plan().get_ir_plan().as_explain().unwrap();

    insta::assert_snapshot!(query_explain, @r#"
    projection ("t6"."a"::string -> "a")
        selection ("t6"."bucket_id"::int in ROW(307::int, 518::int)) and ((ROW("t6"."b"::int, "t6"."a"::string) = ROW(24::int, '42'::string)) or (ROW("t6"."b"::int, "t6"."a"::string) = ROW(25::int, '42'::string)))
            scan "t6"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    assert_eq!(Buckets::Filtered(collection!(307, 518)), buckets);
}

#[test]
fn test_bucket_id_addition5() {
    // Primary key is (a, b), sharding key is (a).
    let query = r#"SELECT "a" FROM "t7" WHERE "a" = '42'"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let buckets = {
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        query.bucket_discovery(top).unwrap()
    };
    query
        .get_mut_exec_plan()
        .get_mut_ir_plan()
        .add_condition_on_bucket_id(&buckets)
        .unwrap();
    let query_explain = query.get_exec_plan().get_ir_plan().as_explain().unwrap();

    insta::assert_snapshot!(query_explain, @r#"
    projection ("t7"."a"::string -> "a")
        selection ("t7"."bucket_id"::int in ROW(5815::int)) and ("t7"."a"::string = '42'::string)
            scan "t7"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    assert_eq!(Buckets::Filtered(collection!(5815)), buckets);
}

#[test]
fn test_bucket_id_addition6() {
    // Primary key is (d), sharding key is (c).
    let query = r#"SELECT * FROM "t4" WHERE "c" = '42'"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let buckets = {
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        query.bucket_discovery(top).unwrap()
    };
    query
        .get_mut_exec_plan()
        .get_mut_ir_plan()
        .add_condition_on_bucket_id(&buckets)
        .unwrap();
    let query_explain = query.get_exec_plan().get_ir_plan().as_explain().unwrap();

    insta::assert_snapshot!(query_explain, @r#"
    projection ("t4"."c"::string -> "c", "t4"."d"::int -> "d")
        selection ("t4"."bucket_id"::int in ROW(5815::int)) and ("t4"."c"::string = '42'::string)
            scan "t4"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);

    assert_eq!(Buckets::Filtered(collection!(5815)), buckets);
}
