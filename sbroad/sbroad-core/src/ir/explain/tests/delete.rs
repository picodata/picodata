use super::*;

#[test]
fn delete1_test() {
    explain_check(
        r#"DELETE FROM "t1""#,
        &format!(
            "{}\n{}\n{}\n{}\n",
            r#"delete "t1""#,
            r#"execution options:"#,
            r#"    vdbe_max_steps = 45000"#,
            r#"    vtable_max_rows = 5000"#,
        ),
    );
}

#[test]
fn delete2_test() {
    explain_check(
        r#"DELETE FROM "t1" where "a" > 3"#,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
            r#"delete "t1""#,
            r#"    motion [policy: local]"#,
            r#"        projection ("t1"."a"::string -> "pk_col_0", "t1"."b"::integer -> "pk_col_1")"#,
            r#"            selection ROW("t1"."a"::string) > ROW(3::unsigned)"#,
            r#"                scan "t1""#,
            r#"execution options:"#,
            r#"    vdbe_max_steps = 45000"#,
            r#"    vtable_max_rows = 5000"#,
        ),
    );
}

#[test]
fn delete3_test() {
    explain_check(
        r#"DELETE FROM "t1" where "a" in (SELECT "b" from "t1")"#,
        &format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
            r#"delete "t1""#,
            r#"    motion [policy: local]"#,
            r#"        projection ("t1"."a"::string -> "pk_col_0", "t1"."b"::integer -> "pk_col_1")"#,
            r#"            selection ROW("t1"."a"::string) in ROW($0)"#,
            r#"                scan "t1""#,
            r#"subquery $0:"#,
            r#"motion [policy: full]"#,
            r#"                    scan"#,
            r#"                        projection ("t1"."b"::integer -> "b")"#,
            r#"                            scan "t1""#,
            r#"execution options:"#,
            r#"    vdbe_max_steps = 45000"#,
            r#"    vtable_max_rows = 5000"#,
        ),
    );
}
