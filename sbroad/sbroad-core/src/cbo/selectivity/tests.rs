use crate::cbo::selectivity::calculate_filter_selectivity;
use crate::cbo::TableColumnPair;
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::ir::operator::Bool;
use crate::ir::value::Value;
use smol_str::SmolStr;
use tarantool::decimal;

/// This test is here just to show that the code compiles.
#[test]
fn numeric_filter_eq_selectivity() {
    let runtime = RouterRuntimeMock::new();
    let table_name = SmolStr::from("test_space");
    let column_index = 0;

    let constant = Value::from(4u64);
    let selectivity = calculate_filter_selectivity(
        &runtime,
        &TableColumnPair(table_name, column_index),
        &constant,
        &Bool::Eq,
    )
    .unwrap();
    assert_eq!(selectivity, decimal!(1.0))
}
