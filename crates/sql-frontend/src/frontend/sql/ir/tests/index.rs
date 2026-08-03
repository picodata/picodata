use crate::ir::node::{ddl::Ddl, CreateIndex};
use sql_executor::test_helpers::{expect_sql_to_ir_error, sql_to_ir_without_bind};
use tarantool::index::SortOrder;

#[test]
fn create_index_parsing() {
    let sql = r#"CREATE INDEX idx ON t5 (a)"#;
    let plan = sql_to_ir_without_bind(sql, &[]);
    let Ddl::CreateIndex(CreateIndex { columns, .. }) =
        plan.get_ddl_node(plan.get_top().unwrap()).unwrap()
    else {
        panic!("expected create index")
    };
    assert_eq!(columns, &vec![("a".into(), SortOrder::Asc)]);

    let sql = r#"CREATE INDEX idx ON t5(a)"#;
    let _ = sql_to_ir_without_bind(sql, &[]);

    let sql = r#"CREATE INDEX idx ON t5 using tree (a)"#;
    let _ = sql_to_ir_without_bind(sql, &[]);

    let sql = r#"CREATE INDEX idx ON t5 using tree(a)"#;
    let _ = sql_to_ir_without_bind(sql, &[]);
}

#[test]
fn create_index_sort_order() {
    let plan = sql_to_ir_without_bind("CREATE INDEX idx ON t5 (a, b ASC, c DESC)", &[]);
    let Ddl::CreateIndex(CreateIndex { columns, .. }) =
        plan.get_ddl_node(plan.get_top().unwrap()).unwrap()
    else {
        panic!("expected create index")
    };
    assert_eq!(
        columns,
        &vec![
            ("a".into(), SortOrder::Asc),
            ("b".into(), SortOrder::Asc),
            ("c".into(), SortOrder::Desc),
        ]
    );
}

#[test]
fn create_index_sort_order_rejects_non_tree_indexes() {
    for index_type in ["hash", "rtree", "bitset"] {
        for order in ["asc", "desc"] {
            let sql = format!("CREATE INDEX idx ON t5 USING {index_type} (a {order})");
            let error = expect_sql_to_ir_error(&sql, &[]);
            assert_eq!(
                error.to_string(),
                format!("invalid index: {index_type} index does not support explicit sort order")
            );
        }
    }
}
