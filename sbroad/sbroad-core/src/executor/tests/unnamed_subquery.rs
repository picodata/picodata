use crate::executor::engine::mock::{DispatchInfo, PortMocked, RouterRuntimeMock};
use crate::ir::relation::{Column, ColumnRole, Table};
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::types::{DerivedType, UnrestrictedType};
use crate::ir::value::Value;
use pretty_assertions::assert_eq;
use rand::random;

use super::*;

#[test]
fn unnamed_subquery1_test() {
    let input = r#"SELECT * FROM (SELECT * FROM t)"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("unnamed_subquery"."a"::int -> "a", "unnamed_subquery"."b"::int -> "b", "unnamed_subquery"."c"::int -> "c", "unnamed_subquery"."d"::int -> "d")
        scan "unnamed_subquery"
            projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
                scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn unnamed_subquery2_test() {
    let input = r#"SELECT * FROM (SELECT * FROM t) join (SELECT * FROM t) on true"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("unnamed_subquery"."a"::int -> "a", "unnamed_subquery"."b"::int -> "b", "unnamed_subquery"."c"::int -> "c", "unnamed_subquery"."d"::int -> "d", "unnamed_subquery_1"."a"::int -> "a", "unnamed_subquery_1"."b"::int -> "b", "unnamed_subquery_1"."c"::int -> "c", "unnamed_subquery_1"."d"::int -> "d")
        join on true::bool
            scan "unnamed_subquery"
                projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
                    scan "t"
            motion [policy: full]
                scan "unnamed_subquery_1"
                    projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn unnamed_subquery_name_conflict1_test() {
    let input =
        r#"SELECT * FROM (SELECT * FROM t) join (SELECT * FROM t) as "unnamed_subquery" on true"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("unnamed_subquery_1"."a"::int -> "a", "unnamed_subquery_1"."b"::int -> "b", "unnamed_subquery_1"."c"::int -> "c", "unnamed_subquery_1"."d"::int -> "d", "unnamed_subquery"."a"::int -> "a", "unnamed_subquery"."b"::int -> "b", "unnamed_subquery"."c"::int -> "c", "unnamed_subquery"."d"::int -> "d")
        join on true::bool
            scan "unnamed_subquery_1"
                projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
                    scan "t"
            motion [policy: full]
                scan "unnamed_subquery"
                    projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn unnamed_subquery_name_conflict2_test() {
    let input = r#"WITH unnamed_subquery as (SELECT * FROM t) SELECT * FROM (SELECT * FROM t) join unnamed_subquery on true"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    insta::assert_snapshot!(plan.as_explain().unwrap(), @r#"
    projection ("unnamed_subquery_1"."a"::int -> "a", "unnamed_subquery_1"."b"::int -> "b", "unnamed_subquery_1"."c"::int -> "c", "unnamed_subquery_1"."d"::int -> "d", "unnamed_subquery"."a"::int -> "a", "unnamed_subquery"."b"::int -> "b", "unnamed_subquery"."c"::int -> "c", "unnamed_subquery"."d"::int -> "d")
        join on true::bool
            scan "unnamed_subquery_1"
                projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
                    scan "t"
            scan cte unnamed_subquery($0)
    subquery $0:
    motion [policy: full]
                    projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
                        scan "t"
    execution options:
        sql_vdbe_opcode_max = 45000
        sql_motion_row_max = 5000
    "#);
}

#[test]
fn unnamed_subquery_name_conflict3_test() {
    let input = r#"SELECT * FROM (SELECT * FROM t) join unnamed_subquery on true"#;

    let mut coordinator = RouterRuntimeMock::new();
    let table = Table::new_global(
        random(),
        "unnamed_subquery",
        vec![Column::new(
            "a",
            DerivedType::new(UnrestrictedType::Integer),
            ColumnRole::User,
            false,
        )],
        &["a"],
    )
    .unwrap();
    coordinator.add_table(table);

    let mut query = ExecutingQuery::from_text_and_params(&coordinator, input, vec![]).unwrap();
    let mut port = PortMocked::new();
    query.dispatch(&mut port).unwrap();
    let info = port.decode();
    assert_eq!(1, info.len());
    let DispatchInfo::All(sql, params) = info.get(0).unwrap() else {
        panic!("Expected a single dispatch on all replicasets");
    };
    assert_eq!(
        sql,
        &format!(
            "{} {} {}",
            "SELECT * FROM",
            r#"(SELECT "t"."a", "t"."b", "t"."c", "t"."d" FROM "t") as "unnamed_subquery_1""#,
            r#"INNER JOIN "unnamed_subquery" ON CAST($1 AS bool)"#,
        ),
    );
    assert_eq!(params, &vec![Value::Boolean(true)]);
}

#[test]
fn unnamed_subquery_try_to_reach_implicitly_1_test() {
    let sql = r#"SELECT "unnamed_subquery"."a" FROM (SELECT * FROM t)"#;
    let coordinator = RouterRuntimeMock::new();

    let result = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]);
    assert!(result.is_err());

    let Err(error) = result else { unreachable!() };

    assert_eq!(
        error.to_string(),
        r#"column with name "a" and scan Some("unnamed_subquery") not found"#
    );
}

#[test]
fn unnamed_subquery_try_to_reach_implicitly_2_test() {
    let sql = r#"SELECT "a", COUNT(*)  FROM (SELECT * FROM t) GROUP BY "unnamed_subquery"."a""#;
    let coordinator = RouterRuntimeMock::new();

    let result = ExecutingQuery::from_text_and_params(&coordinator, sql, vec![]);
    assert!(result.is_err());

    let Err(error) = result else { unreachable!() };

    assert_eq!(
        error.to_string(),
        r#"column with name "a" and scan Some("unnamed_subquery") not found"#
    );
}
