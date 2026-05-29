use crate::ir::node::expression::Expression;
use crate::ir::node::{ArrayLiteral, Cast, IndexExpr, Node32, Node64};
use crate::ir::transformation::helpers::{expect_sql_to_ir_error, sql_to_ir, sql_to_optimized_ir};
use crate::ir::types::{CastType, NestedType, UnrestrictedType};
use crate::ir::value::Value;
use crate::ir::Plan;

/// Whether the plan still contains an explicit array cast.
fn has_array_cast_node(plan: &Plan) -> bool {
    plan.nodes.iter32().any(|node| {
        matches!(
            node,
            Node32::Cast(Cast {
                to: CastType::Array(_),
                ..
            })
        )
    })
}

/// Find the single `ArrayLiteral` node in plan and return its stored `col_type`.
fn array_literal_col_type(plan: &Plan) -> UnrestrictedType {
    let mut found = None;
    for node_id in plan.nodes.iter32() {
        if let Node32::ArrayLiteral(ArrayLiteral { col_type, .. }) = node_id {
            assert!(found.is_none(), "expected exactly one ArrayLiteral");
            found = Some(col_type.get().expect("col_type populated"));
        }
    }
    found.expect("no ArrayLiteral found")
}

fn has_cast_node(plan: &Plan) -> bool {
    plan.nodes
        .iter32()
        .any(|node| matches!(node, Node32::Cast(_)))
}

/// Collect every `IndexExpr` node in the plan.
fn index_nodes(plan: &Plan) -> Vec<IndexExpr> {
    plan.nodes
        .iter64()
        .filter_map(|node| match node {
            Node64::Index(idx) => Some(idx.clone()),
            _ => None,
        })
        .collect()
}

/// Assert the plan holds exactly `count` index-chain nodes, and its child is an array cast.
fn assert_index_over_array_cast(plan: &Plan, count: usize) {
    let indexes = index_nodes(plan);
    assert_eq!(indexes.len(), count, "unexpected number of IndexExpr nodes");
    let base = indexes.first().expect("expected an IndexExpr");
    let child = plan
        .get_expression_node(base.child)
        .expect("index child resolves to an expression");
    assert!(
        matches!(&child, Expression::Cast(cast) if matches!(cast.to, CastType::Array(_))),
        "IndexExpr child should be an array cast, got {child:?}"
    );
}

/// Return the `col_type` of an `ArrayLiteral` in the plan.
fn array_literal_col_type_any(plan: &Plan) -> UnrestrictedType {
    plan.nodes
        .iter32()
        .find_map(|node| {
            if let Node32::ArrayLiteral(ArrayLiteral { col_type, .. }) = node {
                Some(col_type.get().expect("col_type populated"))
            } else {
                None
            }
        })
        .expect("no ArrayLiteral found")
}

#[test]
fn array_literal_homogeneous_int() {
    let sql = r#"SELECT ARRAY[1, 2, 3] FROM "test_space""#;
    let plan = sql_to_ir(sql, vec![]);
    assert_eq!(
        array_literal_col_type(&plan),
        UnrestrictedType::Array(NestedType::Integer)
    );
}

#[test]
fn array_literal_numeric_widening() {
    let sql = r#"SELECT ARRAY[1, 1.5] FROM "test_space""#;
    let plan = sql_to_ir(sql, vec![]);
    assert_eq!(
        array_literal_col_type(&plan),
        UnrestrictedType::Array(NestedType::Numeric)
    );
}

#[test]
fn array_literal_discovery_text_int_mix_rejected() {
    let sql = r#"SELECT ARRAY[1, 'hello'] FROM "test_space""#;
    let err = expect_sql_to_ir_error(sql, &[]);
    let msg = format!("{err}");
    assert!(
        msg.contains("failed to parse"),
        "expected parse failure, got: {msg}"
    );
}

#[test]
fn array_literal_discovery_unmatchable_mix_rejected() {
    let sql = r#"SELECT ARRAY[1, true] FROM "test_space""#;
    let err = expect_sql_to_ir_error(sql, &[]);
    let msg = format!("{err}");
    assert!(
        msg.contains("cannot be matched"),
        "expected a type-mismatch error, got: {msg}"
    );
}

#[test]
fn array_literal_empty() {
    // With no context the element type defaults to text.
    let sql = r#"SELECT ARRAY[] FROM "test_space""#;
    let plan = sql_to_ir(sql, vec![]);
    assert_eq!(
        array_literal_col_type(&plan),
        UnrestrictedType::Array(NestedType::Text)
    );
}

#[test]
fn array_literal_empty_index_plus_arithmetic_infers_int() {
    let sql = r#"SELECT ARRAY[][1] + 100 FROM "test_space""#;
    let plan = sql_to_ir(sql, vec![]);
    assert_eq!(
        array_literal_col_type(&plan),
        UnrestrictedType::Array(NestedType::Integer)
    );
}

#[test]
fn array_literal_nested_rejected() {
    let sql = r#"SELECT ARRAY[ARRAY[1]] FROM "test_space""#;
    let err = expect_sql_to_ir_error(sql, &[]);
    let msg = format!("{err}");
    assert!(
        msg.contains("nested arrays are not supported"),
        "unexpected error: {msg}"
    );
}

#[test]
fn array_literal_param_inferred_from_siblings() {
    let sql = r#"SELECT ARRAY[?, 1] FROM "test_space""#;
    let plan = sql_to_ir(sql, vec![Value::from(7_i64)]);
    assert_eq!(
        array_literal_col_type(&plan),
        UnrestrictedType::Array(NestedType::Integer)
    );
}

#[test]
fn array_literal_insert_into_array_column() {
    let sql = r#"INSERT INTO "arr_t" ("a", "b") VALUES (1, ARRAY[1, 2, 3])"#;
    let plan = sql_to_ir(sql, vec![]);
    assert_eq!(
        array_literal_col_type(&plan),
        UnrestrictedType::Array(NestedType::Integer)
    );
}

#[test]
fn array_literal_insert_text_literal_coerced_to_column_element() {
    let sql = r#"INSERT INTO "arr_t" ("a", "b") VALUES (1, ARRAY[1, 2, '3'])"#;
    let plan = sql_to_ir(sql, vec![]);
    assert_eq!(
        array_literal_col_type(&plan),
        UnrestrictedType::Array(NestedType::Integer)
    );
}

#[test]
fn array_literal_empty_insert_into_typed_column_takes_column_element_type() {
    let sql = r#"INSERT INTO "arr_t" ("a", "b") VALUES (1, ARRAY[])"#;
    let plan = sql_to_ir(sql, vec![]);
    assert_eq!(
        array_literal_col_type(&plan),
        UnrestrictedType::Array(NestedType::Integer)
    );
}

#[test]
fn array_literal_empty_in_insert_with_null_sibling() {
    let sql = r#"INSERT INTO "arr_t" ("a", "b") VALUES (1, ARRAY[]), (2, NULL)"#;
    let plan = sql_to_ir(sql, vec![]);
    assert_eq!(
        array_literal_col_type(&plan),
        UnrestrictedType::Array(NestedType::Integer)
    );
}

#[test]
fn array_literal_insert_decimal_rejected_against_int_column() {
    let sql = r#"INSERT INTO "arr_t" ("a", "b") VALUES (1, ARRAY[1, 2, 3.])"#;
    let err = expect_sql_to_ir_error(sql, &[]);
    let msg = format!("{err}");
    assert!(
        msg.contains("int[]") && msg.contains("numeric[]"),
        "expected element-type mismatch error mentioning int[] and numeric[], got: {msg}"
    );
}

#[test]
fn array_literal_insert_unparseable_text_rejected() {
    let sql = r#"INSERT INTO "arr_t" ("a", "b") VALUES (1, ARRAY[1, 2, 'x'])"#;
    let err = expect_sql_to_ir_error(sql, &[]);
    let msg = format!("{err}");
    assert!(
        msg.contains("failed to parse"),
        "expected parse failure, got: {msg}"
    );
}

#[test]
fn array_literal_explain_preserves_order() {
    let sql = r#"explain (logical) SELECT ARRAY[1, 2, 3] FROM "test_space""#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (ARRAY[1::int, 2::int, 3::int] -> col_1)
      scan test_space
    "#);
}

#[test]
fn array_cast_empty_typed_by_cast() {
    for (ty, expected) in [
        ("int", NestedType::Integer),
        ("double", NestedType::Double),
        ("text", NestedType::Text),
        ("bool", NestedType::Boolean),
        ("json", NestedType::Map),
    ] {
        let sql = format!(r#"SELECT ARRAY[]::{ty}[] FROM "test_space""#);
        let plan = sql_to_ir(&sql, vec![]);
        assert_eq!(
            array_literal_col_type_any(&plan),
            UnrestrictedType::Array(expected),
            "unexpected element type for ::{ty}[]"
        );
        assert!(
            has_array_cast_node(&plan),
            "array cast should survive as a node for ::{ty}[]"
        );
    }
}

#[test]
fn array_cast_literal_coerces_elements() {
    let sql = r#"SELECT ARRAY[1, 2, 3]::double[] FROM "test_space""#;
    let plan = sql_to_ir(sql, vec![]);
    assert_eq!(
        array_literal_col_type_any(&plan),
        UnrestrictedType::Array(NestedType::Double)
    );
    assert!(
        has_array_cast_node(&plan),
        "array cast should survive as a node"
    );
}

#[test]
fn array_cast_via_cast_op_spelling() {
    let sql = r#"SELECT CAST(ARRAY[1, 2] AS int[]) FROM "test_space""#;
    let plan = sql_to_ir(sql, vec![]);
    assert_eq!(
        array_literal_col_type_any(&plan),
        UnrestrictedType::Array(NestedType::Integer)
    );
    assert!(
        has_array_cast_node(&plan),
        "array cast should survive as a node"
    );
}

#[test]
fn array_cast_in_insert_values() {
    let sql = r#"INSERT INTO "arr_t" ("a", "b") VALUES (1, ARRAY[]::int[])"#;
    let plan = sql_to_ir(sql, vec![]);
    assert_eq!(
        array_literal_col_type_any(&plan),
        UnrestrictedType::Array(NestedType::Integer)
    );
    assert!(
        has_array_cast_node(&plan),
        "array cast should survive as a node"
    );
}

#[test]
fn array_cast_on_array_literal_survives() {
    let sql = r#"SELECT ARRAY[1.4, 2.5]::int[] FROM "test_space""#;
    let plan = sql_to_ir(sql, vec![]);
    assert!(
        has_array_cast_node(&plan),
        "array cast should survive as a node"
    );
}

#[test]
fn array_cast_on_array_reference_survives() {
    let sql = r#"SELECT "b"::int[] FROM "arr_t""#;
    let plan = sql_to_ir(sql, vec![]);
    assert!(
        has_array_cast_node(&plan),
        "array cast on an array-typed reference should survive as a node"
    );
}

#[test]
fn array_cast_on_null_folds_to_null() {
    let sql = r#"SELECT NULL::int[] FROM "test_space""#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    assert!(!has_cast_node(&plan), "NULL array cast should be folded");
    let has_array_literal = plan
        .nodes
        .iter32()
        .any(|node| matches!(node, Node32::ArrayLiteral(_)));
    assert!(
        !has_array_literal,
        "NULL array cast must not produce an ArrayLiteral"
    );
}

#[test]
fn array_cast_then_index_parses() {
    let sql = r#"SELECT "b"::int[][1] FROM "arr_t""#;
    let plan = sql_to_ir(sql, vec![]);
    assert_index_over_array_cast(&plan, 1);
}

#[test]
fn array_cast_then_index_matches_parenthesized_spelling() {
    let bare = sql_to_ir(r#"SELECT "b"::int[][1] FROM "arr_t""#, vec![]);
    let parens = sql_to_ir(r#"SELECT ("b"::int[])[1] FROM "arr_t""#, vec![]);
    assert_index_over_array_cast(&bare, 1);
    assert_index_over_array_cast(&parens, 1);
}

#[test]
fn chained_index_folds_into_single_node() {
    let plan = sql_to_ir(r#"SELECT "b"::int[][1][2] FROM "arr_t""#, vec![]);
    let indexes = index_nodes(&plan);
    assert_eq!(indexes.len(), 1, "chain must collapse to a single node");
    assert_eq!(indexes[0].indexes.len(), 2, "node must hold both selectors");
    let child = plan
        .get_expression_node(indexes[0].child)
        .expect("index child resolves to an expression");
    assert!(
        matches!(&child, Expression::Cast(cast) if matches!(cast.to, CastType::Array(_))),
        "chain base should be the array cast, got {child:?}"
    );
}

#[test]
fn chained_index_explain_renders_whole_chain() {
    let sql = r#"explain (logical) SELECT "b"::int[][1][2] FROM "arr_t""#;
    let plan = sql_to_optimized_ir(sql, vec![]);
    let explain = plan.explain_logical().unwrap();
    assert!(
        explain.contains("]["),
        "chained index should render as one chain, got:\n{explain}"
    );
}

#[test]
fn array_multibrace_cast_rejected() {
    // Multidimensional bracket typing is a DDL-only feature; a cast accepts at most one `[]`.
    for sql in [
        r#"SELECT "b"::int[][] FROM "arr_t""#,
        r#"SELECT "b"::int[][][][] FROM "arr_t""#,
    ] {
        let err = expect_sql_to_ir_error(sql, &[]);
        let msg = format!("{err}");
        assert!(
            msg.to_lowercase().contains("parse")
                || msg.contains("rule")
                || msg.contains("expected"),
            "expected a parse error for {sql}, got: {msg}"
        );
    }
}

#[test]
fn array_cast_to_unsigned_parse_error() {
    // `unsigned` is a DDL-only domain type
    let sql = r#"SELECT ARRAY[]::unsigned[] FROM "test_space""#;
    let err = expect_sql_to_ir_error(sql, &[]);
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("parse") || msg.contains("rule") || msg.contains("expected"),
        "expected a parse error, got: {msg}"
    );
}
