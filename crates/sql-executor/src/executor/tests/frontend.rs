use super::*;
use crate::errors::Entity;
use crate::test_helpers::ExecutingQueryExt;

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
    ExecutingQuery::from_text_and_params(metadata, query, vec![]).unwrap();
}

#[test]
fn front_invalid_sql2() {
    let query = r#"INSERT INTO "t" ("a", "b", "c") VALUES(1, 2, 3, 4)"#;

    let metadata = &RouterRuntimeMock::new();
    let plan_err = ExecutingQuery::from_text_and_params(metadata, query, vec![]).unwrap_err();

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
    let plan_err = ExecutingQuery::from_text_and_params(metadata, query, vec![]).unwrap_err();

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
    let plan_err = ExecutingQuery::from_text_and_params(metadata, query, vec![]).unwrap_err();

    assert_eq!(
        SbroadError::Invalid(
            Entity::Query,
            Some("INSERT expects 4 columns, got 2".into())
        ),
        plan_err
    );
}
