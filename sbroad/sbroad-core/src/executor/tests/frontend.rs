use super::*;

use crate::executor::engine::mock::RouterRuntimeMock;
use pretty_assertions::assert_eq;

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
        SbroadError::Invalid(
            Entity::Query,
            Some("INSERT expects 3 columns, got 4".into())
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
        SbroadError::Invalid(
            Entity::Query,
            Some("INSERT expects 4 columns, got 2".into())
        ),
        plan_err
    );
}

#[test]
fn front_explain_select_sql1() {
    let sql = r#"EXPLAIN SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t""#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();

    if let Ok(actual_explain) = query.dispatch().unwrap().downcast::<SmolStr>() {
        insta::assert_snapshot!(*actual_explain, @r#"
        projection ("t"."identification_number"::int -> "c1", "t"."product_code"::string -> "product_code")
            scan "hash_testing" -> "t"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        buckets = [1-10000]
        "#);
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

    if let Ok(actual_explain) = query.dispatch().unwrap().downcast::<SmolStr>() {
        insta::assert_snapshot!(*actual_explain, @r#"
        union all
            projection ("t"."identification_number"::int -> "c1", "t"."product_code"::string -> "product_code")
                scan "hash_testing" -> "t"
            projection ("t2"."identification_number"::int -> "identification_number", "t2"."product_code"::string -> "product_code")
                scan "hash_testing_hist" -> "t2"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        buckets = [1-10000]
        "#);
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

    if let Ok(actual_explain) = query.dispatch().unwrap().downcast::<SmolStr>() {
        insta::assert_snapshot!(*actual_explain, @r#"
        projection ("q1"."a"::string -> "a")
            join on "q1"."a"::string = "q2"."a2"::string
                scan "t3" -> "q1"
                scan "q2"
                    projection ("t3"."a"::string -> "a2", "t3"."b"::int -> "b2")
                        scan "t3"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        buckets = [1-10000]
        "#);
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

    if let Ok(actual_explain) = query.dispatch().unwrap().downcast::<SmolStr>() {
        insta::assert_snapshot!(*actual_explain, @r#"
        projection ("q2"."a"::string -> "a")
            join on "q1"."a"::string = "q2"."a"::string
                scan "t3" -> "q1"
                scan "t3" -> "q2"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        buckets = [1-10000]
        "#);
    } else {
        panic!("explain must be string")
    }
}
