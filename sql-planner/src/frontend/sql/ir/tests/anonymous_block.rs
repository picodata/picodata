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
        // QUERY and IF statements must follow LET and RETURN QUERY statements
        (
            "DO LANGUAGE SQL $$ BEGIN UPDATE t2 SET e = f; RETURN QUERY SELECT 2; END $$",
            "QUERY and IF statements must follow LET and RETURN QUERY statements",
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
        // Unused LET is rejected.
        (
            "DO LANGUAGE SQL $$ BEGIN LET v = (SELECT 1);  END $$",
            "LET variable \"v\" is declared but never used",
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
            "DO $$ BEGIN INSERT INTO t1 SELECT a, b FROM t1; END $$",
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

/// Tests that LET resolution + planning succeed end-to-end (down to
/// `sql_to_ir_without_bind`).
#[test]
fn let_resolution_ok() {
    // The mock metadata gives `t1` (a: string, b: int) and `t2`
    // (e/f/g/h: int). LET RHS types are picked to match the consumer.
    let cases = [
        // Basic LET → bare-identifier reference.
        "DO $$ BEGIN \
            LET v = (SELECT b FROM t1 WHERE a = 'x'); \
            UPDATE t2 SET e = v; \
        END $$",
        // LET reused across multiple later statements.
        "DO $$ BEGIN \
            LET v = (SELECT b FROM t1 WHERE a = 'x'); \
            UPDATE t2 SET e = v; \
            UPDATE t2 SET f = v WHERE e = v; \
        END $$",
        // LET RHS referencing a prior LET.
        "DO $$ BEGIN \
            LET a = (SELECT 1); \
            LET b = (SELECT a + 1); \
            UPDATE t2 SET e = a + b; \
        END $$",
        // LET shadowed by a same-type redeclaration. Both decls must be
        // used to satisfy the unused-LET check: the second LET RHS
        // references the first, and the trailing UPDATE references the
        // second. (DML statements must come after all LET / RETURN QUERY,
        // so no interleaving with DML in between.)
        "DO $$ BEGIN \
            LET v = (SELECT 1); \
            LET v = (SELECT v + 1); \
            UPDATE t2 SET e = v; \
        END $$",
        // RETURN QUERY can also reference a LET.
        "DO $$ BEGIN \
            LET v = (SELECT b FROM t1 WHERE a = 'x'); \
            RETURN QUERY SELECT v; \
        END $$",
        // LET and RETURN QUERY can interleave freely (only DML must come
        // after LET / RETURN QUERY).
        "DO $$ BEGIN \
            LET a = (SELECT 1); \
            RETURN QUERY SELECT a; \
            LET b = (SELECT 2); \
            RETURN QUERY SELECT b; \
        END $$",
    ];

    for query in cases {
        eprintln!("{query}");
        let _ = sql_to_ir_without_bind(query, &[]);
    }
}

/// Tests for LET resolution errors: ambiguity, use-before-declare,
/// multi-column RHS, type mismatch on redeclaration, unused LET.
#[test]
fn let_resolution_errors() {
    let cases = [
        // Use before declaration: `v` is a column of t1 (and we don't have
        // one anyway), and the LET hasn't been pushed into scope yet.
        (
            "DO $$ BEGIN UPDATE t2 SET e = v; LET v = (SELECT 1); END $$",
            "column with name \"v\" not found",
        ),
        // Self-reference inside the LET RHS (no prior `v` exists). The RHS
        // is `(SELECT v + 1)` with no relation in scope, so the bare `v`
        // hits the "Reference … met under Values" path after the LET
        // lookup misses.
        (
            "DO $$ BEGIN LET v = (SELECT v + 1); UPDATE t2 SET e = v; END $$",
            "Reference v met under Values",
        ),
        // Ambiguity: relation `t2` has a column `e`, and a LET also named
        // `e`. A bare `e` inside an UPDATE on `t2` would otherwise be
        // ambiguous.
        (
            "DO $$ BEGIN LET e = (SELECT 1); UPDATE t2 SET e = e + 1; END $$",
            "column reference \"e\" is ambiguous: it could refer to either a LET variable or a table column",
        ),
        // Multi-column LET RHS is rejected at planning time (single-row
        // checking is deferred to runtime per the design).
        (
            "DO $$ BEGIN LET v = (SELECT a, b FROM t1); UPDATE t2 SET e = v; END $$",
            "LET RHS must be a single-column query",
        ),
        // Redeclaration with a different type. (No DML between the two
        // LETs — the ordering rule still applies.)
        (
            "DO $$ BEGIN \
                LET v = (SELECT 1::int); \
                LET v = (SELECT 'x'); \
                UPDATE t2 SET e = v; \
            END $$",
            "cannot be redeclared with a different type",
        ),
        // Unused LET.
        (
            "DO $$ BEGIN LET v = (SELECT 1); RETURN QUERY SELECT 1; END $$",
            "LET variable \"v\" is declared but never used",
        ),
        // A redeclaration creates a fresh binding; the new binding still
        // has to be used for the unused-LET check to pass. Here both
        // declarations of `v` exist but the second is never referenced
        // before the block ends.
        (
            "DO $$ BEGIN \
                LET v = (SELECT 1); \
                RETURN QUERY SELECT v; \
                LET v = (SELECT 2); \
                RETURN QUERY SELECT 3; \
            END $$",
            "LET variable \"v\" is declared but never used",
        ),
    ];

    for (query, error_pattern) in cases {
        let error = expect_sql_to_ir_error(query, &[]);
        eprintln!("{query}: {} vs {error_pattern}", error);
        assert!(error.to_string().contains(error_pattern));
    }
}

/// Tests for IF parsing: bare-expression condition, DML body, interaction
/// with LET, etc.
#[test]
fn if_resolution_ok() {
    let cases = [
        // Plain `IF <bool-expr> THEN UPDATE …; END IF;`. Cond is a literal,
        // body is a single DML statement.
        "DO $$ BEGIN \
            IF true THEN UPDATE t2 SET e = f; END IF; \
        END $$",
        // Cond as a comparison.
        "DO $$ BEGIN \
            IF 1 > 0 THEN UPDATE t2 SET e = f; END IF; \
        END $$",
        // Cond references a LET defined earlier.
        "DO $$ BEGIN \
            LET v = (SELECT b FROM t1 WHERE a = 'x'); \
            IF v > 0 THEN UPDATE t2 SET e = v; END IF; \
        END $$",
        // Multiple DML statements in the body.
        "DO $$ BEGIN \
            IF 1 > 0 THEN \
                UPDATE t2 SET e = f; \
                UPDATE t2 SET f = e WHERE e <> f; \
                DELETE FROM t2 WHERE e = 0; \
            END IF; \
        END $$",
        // DML after IF works.
        "DO $$ BEGIN \
            IF 1 > 0 THEN \
                UPDATE t2 SET e = f; \
                UPDATE t2 SET f = e WHERE e <> f; \
                DELETE FROM t2 WHERE e = 0; \
            END IF; \
            UPDATE t2 SET e = f; \
        END $$",
    ];

    for query in cases {
        eprintln!("{query}");
        let _ = sql_to_ir_without_bind(query, &[]);
    }
}

/// Tests for IF parsing errors: condition shape, body restrictions,
/// ordering rule.
#[test]
fn if_resolution_errors() {
    let cases = [
        // SELECT (DQL) is not allowed in body — `SELECT 1;` parses as a
        // `BlockQueryStatement` but is rejected by the IF body's DML check.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN SELECT 1; END IF; \
            END $$",
            "IF body may only contain DML statements",
        ),
        // LET is not allowed inside IF body.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN LET v = (SELECT 1); END IF; \
            END $$",
            "LET is not allowed inside IF body",
        ),
        // RETURN QUERY is not allowed inside IF body.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN RETURN QUERY SELECT 1; END IF; \
            END $$",
            "RETURN QUERY is not allowed inside IF body",
        ),
        // Nested IF is not allowed.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN \
                    IF 2 > 0 THEN UPDATE t2 SET e = f; END IF; \
                END IF; \
            END $$",
            "nested IF is not allowed",
        ),
        // Can't interleave IF with DQL.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN \
                    UPDATE t2 SET e = f; \
                END IF; \
                RETURN QUERY SELECT 1;
            END $$",
            "QUERY and IF statements must follow LET and RETURN QUERY statements",
        ),
    ];

    for (query, error_pattern) in cases {
        let error = expect_sql_to_ir_error(query, &[]);
        eprintln!("{query}: {} vs {error_pattern}", error);
        assert!(error.to_string().contains(error_pattern));
    }
}
