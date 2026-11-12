use crate::frontend::sql::transform_into_plan;
use crate::ir::types::UnrestrictedType;
use pretty_assertions::assert_eq;
use sql_executor::executor::engine::mock::RouterConfigurationMock;
use sql_executor::test_helpers::{sql_to_ir, sql_to_optimized_ir};
use sql_explain::explain::explain_logical;

/// The sugar applies to each spelling that names the builtin. A quoted
/// identifier keeps its case, so `"JSON_EXTRACT_PATH"` is a different,
/// unknown function.
#[test]
fn json_extract_path_spellings() {
    let desugared = "projection ('{}'::text::json['a'::text]['b'::text] -> col_1)\n  scan t1";
    let cases = [
        ("json_extract_path", Ok(desugared)),
        ("JSON_EXTRACT_PATH", Ok(desugared)),
        (r#""json_extract_path""#, Ok(desugared)),
        (
            r#""JSON_EXTRACT_PATH""#,
            Err("SQL function JSON_EXTRACT_PATH not found"),
        ),
    ];
    let metadata = &RouterConfigurationMock::new();
    for (name, expected) in cases {
        let input =
            format!("explain (logical) select {name}(cast('{{}}' as json), 'a', 'b') from t1");
        match expected {
            Ok(explain) => {
                let plan = sql_to_optimized_ir(&input, vec![]);
                assert_eq!(explain, explain_logical(&plan).unwrap(), "{name}");
            }
            Err(msg) => {
                let err = transform_into_plan(&input, &[], metadata).unwrap_err();
                assert_eq!(msg, err.to_string(), "{name}");
            }
        }
    }
}

#[test]
fn json_extract_path_over_json_yields_json() {
    let plan = sql_to_ir(
        r#"select json_extract_path(cast('{}' as json), 'a', 'b') from t1"#,
        vec![],
    );
    let top = plan.get_top().unwrap();
    let output = plan.get_relational_output(top).unwrap();
    let columns = plan.get_row_list(output).unwrap();
    let col_type = plan
        .get_expression_node(columns[0])
        .unwrap()
        .calculate_type(&plan)
        .unwrap();
    assert_eq!(col_type.get(), &Some(UnrestrictedType::Json));
}

#[test]
fn json_extract_path_requires_index_argument() {
    let input = r#"select json_extract_path(cast('{}' as json)) from t1"#;
    let metadata = &RouterConfigurationMock::new();
    let err = transform_into_plan(input, &[], metadata).unwrap_err();
    assert_eq!(
        "could not resolve function overload for json_extract_path(json)",
        err.to_string()
    );
}
