use crate::ir::transformation::helpers::{expect_sql_to_ir_error, sql_to_ir_without_bind};

#[test]
fn anonymous_blocks_parsing() {
    let queries = [
        "DO LANGUAGE SQL $$ BEGIN RETURN QUERY SELECT 1; END $$",
        "DO $$ BEGIN UPDATE t2 SET e = f; END $$",
        "DO $$ BEGIN UPDATE t2 SET e = f; UPDATE t2 SET e = f; END $$",
        "DO $$ BEGIN UPDATE t2 SET e = f;UPDATE t2 SET e = f;END$$",
        "DO $$ BEGIN RETURN QUERY SELECT 1; UPDATE t2 SET e = f; END $$",
        "DO $$ BEGIN RETURN QUERY SELECT 1;UPDATE t2 SET e = f;END$$",
        "DO $$ BEGIN RETURN QUERY VALUES(1);UPDATE t2 SET e = f;END$$",
        " DO $$ BEGIN RETURN QUERY SELECT 1  ; UPDATE t2 SET e = f ; END $$ ",
        " DO $$ BEGIN RETURN QUERY VALUES (1 ) ; UPDATE t2 SET e = f  ; END $$ ",
        "DO $$ BEGIN RETURN QUERY VALUES (1); RETURN QUERY SELECT b FROM t1; END$$",
        "DO $$ BEGIN RETURN QUERY SELECT (VALUES (1)); RETURN QUERY SELECT b FROM t1 AS t; END$$",
        "DO $$ BEGIN RETURN QUERY VALUES (1); RETURN QUERY SELECT b FROM t1 AS t WHERE a::int = b; END$$",
        "DO $$ BEGIN RETURN QUERY VALUES (1); RETURN QUERY SELECT b FROM t1 WHERE a::int = b; UPDATE t2 SET e = f; END$$",
        "DO $$ BEGIN RETURN QUERY VALUES (1); RETURN QUERY SELECT b FROM t1 WHERE a::int = b; UPDATE t2 SET e = f WHERE e <> f; END$$",
        "DO $$ BEGIN RETURN QUERY VALUES (1); RETURN QUERY SELECT b FROM t1 WHERE a::int = b; UPDATE t2 SET e = f WHERE e <> f; END$$",
        "DO $$ BEGIN RETURN QUERY VALUES (1); RETURN QUERY SELECT b FROM t1 WHERE a::int = b; DELETE FROM t2; END$$",
        "DO $$ BEGIN RETURN QUERY VALUES (1); RETURN QUERY SELECT b FROM t1 WHERE a::int = b; DELETE FROM t2 WHERE e <> f; END$$",
        "DO $$ BEGIN  UPDATE t2 SET e = f; UPDATE t2 SET e = f WHERE e = f ;  END $$",
        "DO $$ BEGIN UPDATE t2 SET e = f; END $$ OPTION (SQL_VDBE_OPCODE_MAX = 1)",
        "DO $$ BEGIN UPDATE t2 SET e = f; END $$OPTION(SQL_VDBE_OPCODE_MAX=1)",
        r#"
        DO $$
        BEGIN
            RETURN QUERY SELECT 1;
            RETURN QUERY SELECT 1;
            UPDATE t2 SET e = f;
            UPDATE t2 SET e = f WHERE e = f;
            DELETE FROM t2;
            DELETE FROM t2 WHERE e <> f;
        END$$
        "#,
        r#"
        DO $$
        BEGIN
            RETURN QUERY SELECT 1;
            RETURN QUERY SELECT e FROM t2 t1 WHERE f = f;
            RETURN QUERY SELECT 1 + 2 AS з;
            RETURN QUERY SELECT 1 + 2 FROM t2 t1;
            RETURN QUERY SELECT 1 + 2 + e + f FROM t2 t1 WHERE f = 1;
            UPDATE t2 SET e = f;
            UPDATE t2 SET e = f WHERE e = f;
            DELETE FROM t2;
            DELETE FROM t2 WHERE e <> f;
        END$$
        "#,
    ];

    for query in queries {
        eprintln!("{query}");
        let _ = sql_to_ir_without_bind(query, &[]);
    }

    // Run the same queries after formatting.
    let opts = sqlformat::FormatOptions::default();
    for query in queries {
        let formatted = sqlformat::format(query, &sqlformat::QueryParams::None, &opts);
        eprintln!("SQL: {formatted}");
        let _ = sql_to_ir_without_bind(&formatted, &[]);
    }
}

#[test]
fn anonymous_blocks_parsing_errors() {
    let cases = [
        // No queries.
        ("DO $$ BEGIN END $$", "rule parsing error"),
        // No semicolon at the end.
        ("DO $$ BEGIN SELECT 1 END $$", "rule parsing error"),
        // No spaces in "DO LANGUAGE SQL".
        (
            "DOLANGUAGESQL $$ BEGIN SELECT 1; END $$",
            "rule parsing error",
        ),
        // RETURN QUERY types must be the same
        (
            "DO LANGUAGE SQL $$ BEGIN RETURN QUERY SELECT 1; RETURN QUERY SELECT 1.5; END $$",
            "RETURN QUERY types cannot be matched",
        ),
        // RETURN QUERY types must be the same
        (
            "DO LANGUAGE SQL $$ BEGIN RETURN QUERY SELECT false; RETURN QUERY SELECT true, 1; END $$",
            "RETURN QUERY types cannot be matched",
        ),
        // QUERY statements must follow LET and RETURN QUERY statements
        (
            "DO LANGUAGE SQL $$ BEGIN UPDATE t2 SET e = f; RETURN QUERY SELECT 2; END $$",
            "QUERY statements must follow LET and RETURN QUERY statements",
        ),
        // DDL is not supported in blocks.
        (
            "DO LANGUAGE SQL $$ BEGIN CREATE TABLE t(a INT PRIMARY KEY); END $$",
            // TODO: change it to smth like "DDL queries are not supported in transactions"
            "rule parsing error",
        ),
        // DDL is not supported in blocks.
        (
            "DO LANGUAGE SQL $$ BEGIN CREATE USER u WITH PASSWORD 'Passw0rd'; END $$",
            "rule parsing error",
        ),
        // LET is not supported yet
        (
            "DO LANGUAGE SQL $$ BEGIN LET v = (SELECT 1);  END $$",
            "LET statements are not supported yet",
        ),
        // LET rhs must be SELECT or UPDATE queries
        (
            "DO LANGUAGE SQL $$ BEGIN LET v = (UPDATE t2 SET e = f);  END $$",
            "rule parsing error",
        ),
        // Cannot return UPDATE.
        (
            "DO LANGUAGE SQL $$ RETURN QUERY UPDATE t2 SET e = f; END $$",
            // TODO: change it to smth like "cannot return dml query"
            "rule parsing error",
        ),
        // Cannot parse `;;` at the end (like PG).
        (
            "DO $$ BEGIN UPDATE t2 SET e = f;; END $$",
            "rule parsing error",
        ),
        // Options must be specified only for a block.
        (
            "DO $$ BEGIN RETURN QUERY SELECT 1 OPTION (SQL_VDBE_OPCODE_MAX = 1); END $$",
            "OPTION cannot be specified for individual queries within a transaction; specify it for the entire DO block instead",
        ),
        // Can't set SQL_MOTION_ROW_MAX for a block.
        (
            "DO $$ BEGIN RETURN QUERY SELECT 1; END $$ OPTION (SQL_MOTION_ROW_MAX = 1)",
            "transaction cannot have any motions; SQL_MOTION_ROW_MAX is not applicable to transactions",
        ),
    ];

    for (query, error_pattern) in cases {
        let error = expect_sql_to_ir_error(query, &[]);
        eprintln!("{}: {} vs {}", query, error.to_string(), error_pattern);
        assert!(error.to_string().contains(error_pattern))
    }
}

#[test]
fn parameterized_anonymous_blocks() {
    let ok_cases = [
        // Use parameter in the filter.
        "DO $$ BEGIN UPDATE t2 SET e = f WHERE f = $1; END $$",
        // Use parameter in the filter twice.
        "DO $$ BEGIN UPDATE t2 SET e = f WHERE f = $1; UPDATE t2 SET e = e + 1 WHERE f = $1; END $$",
        // Define parameter type in RETURN QUERY and use it in the following query.
        "DO $$ BEGIN RETURN QUERY SELECT $1::int; UPDATE t2 SET e = f WHERE f = $1; END $$",
        // Define parameter type in RETURN QUERY and use it in the following queries.
        "DO $$ BEGIN RETURN QUERY SELECT $1 + 1; UPDATE t2 SET e = $1 + $1 WHERE f = $1; END $$",
    ];

    let error_cases = [
        // Parameter type is inferred from `$1 = a` to text and then compared with e
        // and f columns of different types in the following query.
        (
            "DO $$ BEGIN RETURN QUERY SELECT a FROM t1 WHERE $1 = a; UPDATE t2 SET e = $1 WHERE f = $1; END $$",
            "could not resolve operator overload for =(int, text)",
        ),
        // Parameter type is inferred from `f = $1` to int and then compared with text expression.
        (
            "DO $$ BEGIN UPDATE t2 SET e = 1 WHERE f = $1; UPDATE t2 SET e = 2 WHERE f::text = $1; END $$",
            "could not resolve operator overload for =(text, int)",
        ),
    ];

    for query in ok_cases {
        eprintln!("{}", query);
        let _ = sql_to_ir_without_bind(query, &[]);
    }

    for (query, error_pattern) in error_cases {
        let error = expect_sql_to_ir_error(query, &[]);
        eprintln!("{} vs {}", error.to_string(), error_pattern);
        assert!(error.to_string().contains(error_pattern));
    }
}

#[test]
fn block_query_has_motions_errors() {
    // Some kind of a list of queries that cannot be executed in a block as they have motions.
    let test_cases = [
        (
            "DO $$ BEGIN RETURN QUERY SELECT (SELECT a FROM t1) FROM t1; END $$",
            "SELECT",
        ),
        (
            "DO $$ BEGIN RETURN QUERY VALUES ((SELECT a FROM t1)); END $$",
            "VALUES",
        ),
        (
            "DO $$ BEGIN RETURN QUERY SELECT * FROM t1 ORDER BY 1; END $$",
            "SELECT",
        ),
        (
            "DO $$ BEGIN RETURN QUERY SELECT * FROM t1 GROUP BY a, b; END $$",
            "SELECT",
        ),
        (
            "DO $$ BEGIN RETURN QUERY SELECT * FROM t1 LIMIT 1; END $$",
            "LIMIT",
        ),
        (
            "DO $$ BEGIN RETURN QUERY SELECT * FROM t1 GROUP BY a, b ORDER BY a; END $$",
            "SELECT",
        ),
        (
            "DO $$ BEGIN RETURN QUERY SELECT * FROM t1 GROUP BY b, a ORDER BY b LIMIT 1; END $$",
            "LIMIT",
        ),
        (
            "DO $$ BEGIN RETURN QUERY SELECT * FROM t1 UNION SELECT * FROM t1; END $$",
            "UNION",
        ),
        (
            "DO $$ BEGIN RETURN QUERY SELECT * FROM t1 JOIN t2 ON true; END $$",
            "SELECT",
        ),
        (
            "DO $$ BEGIN INSERT INTO t1 VALUES ('1',2); END $$",
            "INSERT",
        ),
    ];

    for (block, keyword) in test_cases {
        let plan = sql_to_ir_without_bind(block, &[]);
        let error = plan.optimize_block().unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("{keyword} query has motions which are not allowed in transactions")
        );
    }
}

#[test]
fn delete_in_block_parsing() {
    let ok_cases = [
        "DO $$ BEGIN DELETE FROM t2; END $$",
        "DO $$ BEGIN DELETE FROM t2 WHERE e = f; END $$",
        "DO $$ BEGIN DELETE FROM t2 WHERE e = 1; END $$",
        "DO $$ BEGIN DELETE FROM t2; DELETE FROM t2 WHERE e = f; END $$",
        "DO $$ BEGIN RETURN QUERY SELECT e FROM t2 WHERE e = 1; DELETE FROM t2 WHERE e = f; END $$",
        "DO $$ BEGIN DELETE FROM t2 WHERE f = $1; END $$",
    ];

    for query in ok_cases {
        eprintln!("{query}");
        let _ = sql_to_ir_without_bind(query, &[]);
    }
}

#[test]
fn delete_in_block_optimize() {
    let ok_cases = [
        "DO $$ BEGIN DELETE FROM t2; END $$",
        "DO $$ BEGIN DELETE FROM t2 WHERE e = f; END $$",
        "DO $$ BEGIN DELETE FROM t2 WHERE e = 1; END $$",
        "DO $$ BEGIN DELETE FROM t2; DELETE FROM t2 WHERE e = f; END $$",
        "DO $$ BEGIN RETURN QUERY SELECT e FROM t2 WHERE e = 1; DELETE FROM t2 WHERE e = f; END $$",
    ];

    for query in ok_cases {
        eprintln!("{query}");
        let plan = sql_to_ir_without_bind(query, &[]);
        plan.optimize_block().unwrap();
    }
}

#[test]
fn delete_in_block_errors() {
    let error_cases = [
        (
            "DO $$ BEGIN DELETE FROM t2 WHERE e = (SELECT 1); END $$",
            "DELETE in transaction cannot have subqueries",
        ),
        (
            "DO $$ BEGIN DELETE FROM t2 WHERE e IN (SELECT e FROM t2); END $$",
            "DELETE in transaction cannot have subqueries",
        ),
    ];

    for (query, error_pattern) in error_cases {
        let error = expect_sql_to_ir_error(query, &[]);
        eprintln!("{}: {} vs {}", query, error, error_pattern);
        assert!(error.to_string().contains(error_pattern));
    }
}
