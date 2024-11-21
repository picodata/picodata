use super::*;

use crate::executor::engine::mock::RouterRuntimeMock;
use pretty_assertions::assert_eq;
use smol_str::format_smolstr;

#[test]
fn front_valid_sql1() {
    // Tables "test_space" and "hash_testing" have the same columns "sys_op" and "bucket_id",
    // that previously caused a "duplicate column" error in the output tuple of the
    // INNER JOIN node. Now the error is fixed.
    let query = r#"SELECT "id", "product_units" FROM "hash_testing"
        INNER JOIN "test_space" as t
        ON "hash_testing"."identification_number" = t."id"
        WHERE "hash_testing"."identification_number" = 5 and "hash_testing"."product_code" = '123'"#;

    let metadata = &RouterRuntimeMock::new();
    Query::new(metadata, query, vec![]).unwrap();
}

#[test]
fn front_invalid_sql2() {
    let query = r#"INSERT INTO "t" ("a", "b", "c") VALUES(1, 2, 3, 4)"#;

    let metadata = &RouterRuntimeMock::new();
    let plan_err = Query::new(metadata, query, vec![]).unwrap_err();

    assert_eq!(
        SbroadError::UnexpectedNumberOfValues(
            r#"invalid number of values: 4. Table t expects 3 column(s)."#.into()
        ),
        plan_err
    );
}

#[test]
fn front_invalid_sql3() {
    let query = r#"INSERT INTO "t" SELECT "b", "d" FROM "t""#;

    let metadata = &RouterRuntimeMock::new();
    let plan_err = Query::new(metadata, query, vec![]).unwrap_err();

    assert_eq!(
        SbroadError::UnexpectedNumberOfValues(
            r#"invalid number of values: 2. Table t expects 4 column(s)."#.into()
        ),
        plan_err
    );
}

#[test]
fn front_invalid_sql4() {
    let query = r#"INSERT INTO "t" VALUES(1, 2)"#;

    let metadata = &RouterRuntimeMock::new();
    let plan_err = Query::new(metadata, query, vec![]).unwrap_err();

    assert_eq!(
        SbroadError::UnexpectedNumberOfValues(
            r#"invalid number of values: 2. Table t expects 4 column(s)."#.into()
        ),
        plan_err
    );
}

#[test]
fn front_explain_select_sql1() {
    let sql = r#"EXPLAIN SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t""#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();

    let expected_explain = SmolStr::from(
        r#"projection ("t"."identification_number"::integer -> "c1", "t"."product_code"::string -> "product_code")
    scan "hash_testing" -> "t"
execution options:
    vdbe_max_steps = 45000
    vtable_max_rows = 5000
buckets = [1-10000]
"#,
    );

    if let Ok(actual_explain) = query.dispatch().unwrap().downcast::<SmolStr>() {
        assert_eq!(expected_explain, *actual_explain);
    } else {
        panic!("Explain must be string")
    }
}

#[test]
fn front_explain_select_sql2() {
    let sql = r#"EXPLAIN SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t"
        UNION ALL
        SELECT "t2"."identification_number", "product_code" FROM "hash_testing_hist" as "t2""#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();

    let expected_explain: SmolStr = format_smolstr!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
        r#"union all"#,
        r#"    projection ("t"."identification_number"::integer -> "c1", "t"."product_code"::string -> "product_code")"#,
        r#"        scan "hash_testing" -> "t""#,
        r#"    projection ("t2"."identification_number"::integer -> "identification_number", "t2"."product_code"::string -> "product_code")"#,
        r#"        scan "hash_testing_hist" -> "t2""#,
        r#"execution options:"#,
        r#"    vdbe_max_steps = 45000"#,
        r#"    vtable_max_rows = 5000"#,
        r#"buckets = [1-10000]"#,
    );

    if let Ok(actual_explain) = query.dispatch().unwrap().downcast::<SmolStr>() {
        assert_eq!(expected_explain, *actual_explain);
    } else {
        panic!("Explain must be string")
    }
}

#[test]
fn front_explain_select_sql3() {
    let sql = r#"explain select "a" from "t3" as "q1"
        inner join (select "t3"."a" as "a2", "t3"."b" as "b2" from "t3") as "q2"
        on "q1"."a" = "q2"."a2""#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();

    let expected_explain: SmolStr = format_smolstr!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
        r#"projection ("q1"."a"::string -> "a")"#,
        r#"    join on ROW("q1"."a"::string) = ROW("q2"."a2"::string)"#,
        r#"        scan "q1""#,
        r#"            projection ("q1"."a"::string -> "a", "q1"."b"::integer -> "b")"#,
        r#"                scan "t3" -> "q1""#,
        r#"        scan "q2""#,
        r#"            projection ("t3"."a"::string -> "a2", "t3"."b"::integer -> "b2")"#,
        r#"                scan "t3""#,
        r#"execution options:"#,
        r#"    vdbe_max_steps = 45000"#,
        r#"    vtable_max_rows = 5000"#,
        r#"buckets = [1-10000]"#,
    );

    if let Ok(actual_explain) = query.dispatch().unwrap().downcast::<SmolStr>() {
        assert_eq!(expected_explain, *actual_explain);
    } else {
        panic!("explain must be string")
    }
}

#[test]
fn front_explain_select_sql4() {
    let sql = r#"explain select "q2"."a" from "t3" as "q1"
        inner join "t3" as "q2"
        on "q1"."a" = "q2"."a""#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();

    let expected_explain: SmolStr = format_smolstr!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
        r#"projection ("q2"."a"::string -> "a")"#,
        r#"    join on ROW("q1"."a"::string) = ROW("q2"."a"::string)"#,
        r#"        scan "q1""#,
        r#"            projection ("q1"."a"::string -> "a", "q1"."b"::integer -> "b")"#,
        r#"                scan "t3" -> "q1""#,
        r#"        scan "q2""#,
        r#"            projection ("q2"."a"::string -> "a", "q2"."b"::integer -> "b")"#,
        r#"                scan "t3" -> "q2""#,
        r#"execution options:"#,
        r#"    vdbe_max_steps = 45000"#,
        r#"    vtable_max_rows = 5000"#,
        r#"buckets = [1-10000]"#,
    );

    if let Ok(actual_explain) = query.dispatch().unwrap().downcast::<SmolStr>() {
        assert_eq!(expected_explain, *actual_explain);
    } else {
        panic!("explain must be string")
    }
}
