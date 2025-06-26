use crate::ir::transformation::helpers::sql_to_ir_without_bind;

#[test]
fn create_index_parsing() {
    let sql = r#"CREATE INDEX idx ON t5 (a)"#;
    let _ = sql_to_ir_without_bind(sql, &[]);

    let sql = r#"CREATE INDEX idx ON t5(a)"#;
    let _ = sql_to_ir_without_bind(sql, &[]);

    let sql = r#"CREATE INDEX idx ON t5 using tree (a)"#;
    let _ = sql_to_ir_without_bind(sql, &[]);

    let sql = r#"CREATE INDEX idx ON t5 using tree(a)"#;
    let _ = sql_to_ir_without_bind(sql, &[]);
}
