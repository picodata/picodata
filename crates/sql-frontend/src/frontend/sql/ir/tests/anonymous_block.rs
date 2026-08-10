use sql_executor::test_helpers::{expect_sql_to_ir_error, sql_to_ir_without_bind};

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
        (
            "DO LANGUAGE SQL $$ BEGIN UPDATE t2 SET e = f; RETURN QUERY SELECT 2; END $$",
            "LET and RETURN QUERY statements must precede all DML statements",
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
        // IOCDU update target must not be table-qualified.
        (
            "DO $$ BEGIN INSERT INTO \"t\" VALUES (1, 1, 1, 1) ON CONFLICT (\"a\") DO UPDATE SET t.c = t.c + 1; END $$",
            "ON CONFLICT DO UPDATE SET target column must not be table-qualified",
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
        // Route a typed parameter through LET and use it in an IOCDU RHS.
        r#"DO $$ BEGIN
            LET m = (SELECT $1::int);
            INSERT INTO "t" VALUES (1, 1, 1, 1) ON CONFLICT ("a") DO UPDATE SET "c" = "c" + m;
        END $$"#,
        // Use a parameter in an IOCDU RHS.
        "DO $$ BEGIN INSERT INTO \"t\" VALUES (1, 1, 1, 1) ON CONFLICT (\"a\") DO UPDATE SET \"c\" = \"c\" + $1; END $$",
        // Allow table-qualified self reference in IOCDU RHS.
        "DO $$ BEGIN INSERT INTO \"t\" VALUES (1, 1, 1, 1) ON CONFLICT (\"a\") DO UPDATE SET \"c\" = t.c + 1; END $$",
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
        // Unknown parameter type defaults to text, so the LET variable cannot be added to int.
        (
            r#"DO $$ BEGIN
                LET m = (SELECT $1);
                INSERT INTO "t" VALUES (1, 1, 1, 1) ON CONFLICT ("a") DO UPDATE SET "c" = "c" + m;
            END $$"#,
            "could not resolve operator overload for +(int, text)",
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
    let tail = "cannot run in a transactional block because it requires \
                cross-shard data movement; restrict by the sharding key, \
                or move it outside the block";

    let test_cases = [
        (
            "DO $$ BEGIN RETURN QUERY SELECT (SELECT a FROM t1) FROM t1; END $$",
            "statement 1 (RETURN QUERY)",
        ),
        (
            "DO $$ BEGIN RETURN QUERY VALUES ((SELECT a FROM t1)); END $$",
            "statement 1 (RETURN QUERY)",
        ),
        (
            "DO $$ BEGIN RETURN QUERY SELECT * FROM t1 ORDER BY 1; END $$",
            "statement 1 (RETURN QUERY)",
        ),
        (
            "DO $$ BEGIN RETURN QUERY SELECT * FROM t1 GROUP BY a, b; END $$",
            "statement 1 (RETURN QUERY)",
        ),
        (
            "DO $$ BEGIN RETURN QUERY SELECT * FROM t1 LIMIT 1; END $$",
            "statement 1 (RETURN QUERY)",
        ),
        (
            "DO $$ BEGIN RETURN QUERY SELECT * FROM t1 GROUP BY a, b ORDER BY a; END $$",
            "statement 1 (RETURN QUERY)",
        ),
        (
            "DO $$ BEGIN RETURN QUERY SELECT * FROM t1 GROUP BY b, a ORDER BY b LIMIT 1; END $$",
            "statement 1 (RETURN QUERY)",
        ),
        (
            "DO $$ BEGIN RETURN QUERY SELECT * FROM t1 UNION SELECT * FROM t1; END $$",
            "statement 1 (RETURN QUERY)",
        ),
        (
            "DO $$ BEGIN RETURN QUERY SELECT * FROM t1 JOIN t2 ON true; END $$",
            "statement 1 (RETURN QUERY)",
        ),
        (
            "DO $$ BEGIN INSERT INTO t1 SELECT a, b FROM t1; END $$",
            "statement 1 (DML)",
        ),
        (
            "DO $$ BEGIN \
                IF (SELECT b FROM t1) > 0 THEN UPDATE t2 SET e = f; END IF; \
            END $$",
            "statement 1.1 (IF condition)",
        ),
        (
            "DO $$ BEGIN \
                IF true THEN INSERT INTO t1 SELECT a, b FROM t1; END IF; \
            END $$",
            "statement 1.2 (DML)",
        ),
        (
            "DO $$ BEGIN \
                RETURN QUERY SELECT 1; \
                RETURN QUERY SELECT b FROM t1 ORDER BY 1; \
            END $$",
            "statement 2 (RETURN QUERY)",
        ),
        (
            "DO $$ BEGIN \
                IF true THEN \
                    DELETE FROM t2 WHERE e = 1; \
                    INSERT INTO t1 SELECT a, b FROM t1; \
                END IF; \
            END $$",
            "statement 1.3 (DML)",
        ),
    ];

    for (block, locator) in test_cases {
        let plan = sql_to_ir_without_bind(block, &[]);
        let error = plan.optimize_block().unwrap_err();
        assert_eq!(error.to_string(), format!("{locator}: {tail}"));
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
/// multi-column RHS, type mismatch on redeclaration.
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
        // A delimited identifier can hold anything, but a LET name ends up in
        // the generated SQL as `:{name}`, so it has to stay a single bound
        // parameter token.
        (
            r#"DO $$ BEGIN LET "my var" = (SELECT 1); UPDATE t2 SET e = "my var"; END $$"#,
            r#"LET variable name "my var" is invalid: it contains ' '"#,
        ),
        // A digit-leading name is worse than malformed: `:1` is what a
        // positional parameter looks like, so it would silently read $1.
        (
            r#"DO $$ BEGIN LET "1" = (SELECT 1); UPDATE t2 SET e = "1"; END $$"#,
            r#"LET variable name "1" is invalid: it starts with a digit"#,
        ),
        (
            r#"DO $$ BEGIN LET "1x" = (SELECT 1); UPDATE t2 SET e = "1x"; END $$"#,
            r#"LET variable name "1x" is invalid: it starts with a digit"#,
        ),
        (
            r#"DO $$ BEGIN LET "" = (SELECT 1); END $$"#,
            r#"LET variable name "" is invalid: it is empty"#,
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
        // RETURN QUERY inside the body is the only source of returned rows.
        "DO $$ BEGIN \
            IF 1 > 0 THEN RETURN QUERY SELECT 1; END IF; \
        END $$",
        // RETURN QUERY inside the body may be followed by DML in the same body.
        "DO $$ BEGIN \
            LET v = (SELECT b FROM t1 WHERE a = 'x'); \
            IF v > 0 THEN \
                RETURN QUERY SELECT v; \
                UPDATE t2 SET e = v; \
            END IF; \
        END $$",
        // Top-level and nested RETURN QUERY agree on types.
        "DO $$ BEGIN \
            RETURN QUERY SELECT 1; \
            IF 1 > 0 THEN RETURN QUERY SELECT 2; END IF; \
        END $$",
        // Several IF blocks, each returning rows.
        "DO $$ BEGIN \
            IF 1 > 0 THEN RETURN QUERY SELECT 1; END IF; \
            IF 2 > 0 THEN RETURN QUERY SELECT 2; END IF; \
        END $$",
        // An IF is not a write in itself: one whose body only reads may
        // precede a top-level RETURN QUERY.
        "DO $$ BEGIN \
            IF 1 > 0 THEN RETURN QUERY SELECT 1; END IF; \
            RETURN QUERY SELECT 2; \
        END $$",
        // ... and a LET, which may then feed the DML that follows.
        "DO $$ BEGIN \
            IF 1 > 0 THEN RETURN QUERY SELECT 1; END IF; \
            LET v = (SELECT b FROM t1 WHERE a = 'x'); \
            UPDATE t2 SET e = v; \
        END $$",
        // A LET declared in the body feeds the DML next to it.
        "DO $$ BEGIN \
            IF 1 > 0 THEN \
                LET v = (SELECT b FROM t1 WHERE a = 'x'); \
                UPDATE t2 SET e = v; \
            END IF; \
        END $$",
        // A body LET may be seeded from one declared before the IF.
        "DO $$ BEGIN \
            LET v = (SELECT b FROM t1 WHERE a = 'x'); \
            IF v > 0 THEN \
                LET w = (SELECT v + 1); \
                UPDATE t2 SET e = w; \
            END IF; \
        END $$",
        // Sibling bodies are separate scopes, so each may bind the same name
        // to a variable of its own, of an unrelated type. They share a runtime
        // slot, which is safe: neither body can read the other's value.
        "DO $$ BEGIN \
            IF 1 > 0 THEN LET v = (SELECT 1); END IF; \
            IF 2 > 0 THEN LET v = (SELECT 'x'); END IF; \
        END $$",
        // A body LET may take the name of a column: while it is live the two
        // never meet, since a body-local LET cannot be read from a query over
        // that table (that would be ambiguous, see `let_resolution_errors`).
        // Once END IF frees the name, the column resolves as usual.
        "DO $$ BEGIN \
            IF 1 > 0 THEN LET e = (SELECT 1); END IF; \
            RETURN QUERY SELECT e FROM t2; \
        END $$",
        // Same, in a position where the reference is the only thing that could
        // have named the dead variable.
        "DO $$ BEGIN \
            IF 1 > 0 THEN LET f = (SELECT 1); END IF; \
            UPDATE t2 SET e = f WHERE f > 0; \
        END $$",
        // An empty body is a well-formed no-op -- the condition still runs.
        "DO $$ BEGIN IF 1 > 0 THEN END IF; END $$",
        // ... including as one branch among several, and nested.
        "DO $$ BEGIN \
            IF 1 > 0 THEN END IF; \
            IF 2 > 0 THEN \
                IF 3 > 0 THEN END IF; \
                UPDATE t2 SET e = f; \
            END IF; \
        END $$",
        // A name freed by END IF can also be reused at the top level.
        "DO $$ BEGIN \
            IF 1 > 0 THEN LET v = (SELECT 'x'); END IF; \
            LET v = (SELECT 1); \
            UPDATE t2 SET e = v; \
        END $$",
        // IFs nest, and an inner condition may read a LET of the enclosing
        // body since it is evaluated in that scope.
        "DO $$ BEGIN \
            IF 1 > 0 THEN \
                LET v = (SELECT b FROM t1 WHERE a = 'x'); \
                IF v > 0 THEN \
                    RETURN QUERY SELECT v; \
                    UPDATE t2 SET e = v; \
                END IF; \
            END IF; \
        END $$",
        // Each level keeps its own scope, so sibling inner bodies may bind the
        // same name to variables of their own. (Neither may hold DML: the
        // second body's LET would then be a read after a write.)
        "DO $$ BEGIN \
            IF 1 > 0 THEN \
                IF 2 > 0 THEN LET v = (SELECT 1); END IF; \
                IF 3 > 0 THEN LET v = (SELECT 'x'); END IF; \
            END IF; \
        END $$",
        // Three levels deep, with statements interleaved at every level.
        "DO $$ BEGIN \
            IF 1 > 0 THEN \
                UPDATE t2 SET e = f; \
                IF 2 > 0 THEN \
                    DELETE FROM t2 WHERE e = 0; \
                    IF 3 > 0 THEN UPDATE t2 SET f = e; END IF; \
                END IF; \
                UPDATE t2 SET e = f WHERE e <> f; \
            END IF; \
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
        // A bare SELECT (DQL) is not allowed in body -- `SELECT 1;` parses as a
        // `BlockQueryStatement` but is rejected by the IF body's DML check.
        // Use RETURN QUERY to return rows.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN SELECT 1; END IF; \
            END $$",
            "bare DQL is not allowed, use LET or RETURN QUERY",
        ),
        // A body LET is a read, so it is bound by the ordering rule too.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN \
                    UPDATE t2 SET e = f; \
                    LET v = (SELECT 1); \
                END IF; \
            END $$",
            "LET and RETURN QUERY statements must precede all DML statements",
        ),
        // A body LET keeps the rest of the LET rules: single-column RHS...
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN LET v = (SELECT a, b FROM t1); END IF; \
            END $$",
            "LET RHS must be a single-column query",
        ),
        // A body LET falls out of scope at END IF.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN LET v = (SELECT 1); END IF; \
                RETURN QUERY SELECT v; \
            END $$",
            "LET variable \"v\" is out of scope",
        ),
        // Same, when the reference could otherwise have resolved to a column.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN LET vv = (SELECT 1); END IF; \
                UPDATE t2 SET e = vv; \
            END $$",
            "LET variable \"vv\" is out of scope",
        ),
        // Shadowing an enclosing LET is rejected rather than silently
        // discarded at END IF -- whatever the types are.
        (
            "DO $$ BEGIN \
                LET v = (SELECT 1::int); \
                IF 1 > 0 THEN LET v = (SELECT 'x'); END IF; \
            END $$",
            "is already declared outside IF body",
        ),
        (
            "DO $$ BEGIN \
                LET v = (SELECT 1); \
                IF 1 > 0 THEN LET v = (SELECT 2); END IF; \
                RETURN QUERY SELECT v; \
            END $$",
            "is already declared outside IF body",
        ),
        // ... and neither is a repeated LET within one body.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN \
                    LET v = (SELECT 1::int); \
                    LET v = (SELECT 'x'); \
                    UPDATE t2 SET e = v; \
                END IF; \
            END $$",
            "cannot be redeclared with a different type",
        ),
        // Reads must precede writes inside the IF body as well.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN \
                    UPDATE t2 SET e = f; \
                    RETURN QUERY SELECT 1; \
                END IF; \
            END $$",
            "LET and RETURN QUERY statements must precede all DML statements",
        ),
        // The same rule spans nesting levels: the DML sits at the top level,
        // the read is inside the IF body.
        (
            "DO $$ BEGIN \
                UPDATE t2 SET e = f; \
                IF 1 > 0 THEN RETURN QUERY SELECT 1; END IF; \
            END $$",
            "LET and RETURN QUERY statements must precede all DML statements",
        ),
        // A nested RETURN QUERY takes part in the block's type inference, so it
        // must agree with the other RETURN QUERY statements.
        (
            "DO $$ BEGIN \
                RETURN QUERY SELECT 1; \
                IF 1 > 0 THEN RETURN QUERY SELECT 'x'; END IF; \
            END $$",
            "RETURN QUERY types cannot be matched",
        ),
        // The agreement holds between two nested RETURN QUERY statements too.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN RETURN QUERY SELECT 1; END IF; \
                IF 2 > 0 THEN RETURN QUERY SELECT 1, 2; END IF; \
            END $$",
            "RETURN QUERY types cannot be matched",
        ),
        // Every rule keeps applying however deep the IFs go: a bare DQL...
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN IF 2 > 0 THEN SELECT 1; END IF; END IF; \
            END $$",
            "bare DQL is not allowed, use LET or RETURN QUERY",
        ),
        // ...the reads-before-writes ordering, across nesting levels...
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN \
                    IF 2 > 0 THEN UPDATE t2 SET e = f; END IF; \
                    RETURN QUERY SELECT 1; \
                END IF; \
            END $$",
            "LET and RETURN QUERY statements must precede all DML statements",
        ),
        // ...and the ban on shadowing an enclosing LET.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN \
                    LET v = (SELECT 1); \
                    IF 2 > 0 THEN LET v = (SELECT 2); UPDATE t2 SET e = v; END IF; \
                END IF; \
            END $$",
            "is already declared outside IF body",
        ),
        // An inner LET is gone once its own body ends.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN \
                    IF 2 > 0 THEN LET v = (SELECT 1); END IF; \
                    UPDATE t2 SET e = v; \
                END IF; \
            END $$",
            "LET variable \"v\" is out of scope",
        ),
        // ON CONFLICT DO UPDATE resolves LET names on its own path; it must
        // explain an ended scope the same way, not as an unsupported literal.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN LET incr = (SELECT 1); END IF; \
                INSERT INTO \"t\" VALUES (1, 1, 1, 1) \
                    ON CONFLICT (\"a\") DO UPDATE SET \"c\" = \"c\" + incr; \
            END $$",
            "LET variable \"incr\" is out of scope",
        ),
        // Can't interleave IF with DQL.
        (
            "DO $$ BEGIN \
                IF 1 > 0 THEN \
                    UPDATE t2 SET e = f; \
                END IF; \
                RETURN QUERY SELECT 1;
            END $$",
            "LET and RETURN QUERY statements must precede all DML statements",
        ),
    ];

    for (query, error_pattern) in cases {
        let error = expect_sql_to_ir_error(query, &[]);
        eprintln!("{query}: {} vs {error_pattern}", error);
        assert!(error.to_string().contains(error_pattern));
    }
}

/// The `IF` condition is parsed without a projection of its own, so its window used
/// to survive in the parser state and get attached to the projection of the following
/// `INSERT`.
#[test]
fn if_condition_window_does_not_leak_into_projection() {
    use crate::ir::node::Node96;
    let plan = sql_to_ir_without_bind(
        "DO $$ BEGIN \
            IF count(*) OVER () > 0 THEN DELETE FROM t2; END IF; \
            INSERT INTO t2 SELECT max(e) OVER (), f, g, h FROM t2; \
        END $$",
        &[],
    );
    let windows: Vec<usize> = plan
        .nodes
        .iter96()
        .filter_map(|node| match node {
            Node96::Projection(projection) => Some(projection.windows.len()),
            _ => None,
        })
        .collect();
    assert_eq!(vec![1], windows);
}

/// A `RETURN QUERY` nested in an `IF` body defines the block's output format
/// just like a top-level one. Without this the block claims zero columns while
/// the VDBE still emits rows, and pgproto fails with "Expected 0 columns".
#[test]
fn if_body_return_query_defines_output_format() {
    use crate::ir::node::block::Block;
    use crate::ir::types::{DerivedType, UnrestrictedType};

    let return_columns = |query: &str| {
        let plan = sql_to_ir_without_bind(query, &[]);
        let top_id = plan.get_top().unwrap();
        let Block::Anonymous(block) = plan.get_block_node(top_id).unwrap() else {
            panic!("expected an anonymous block");
        };
        block.return_columns.clone()
    };

    let columns = return_columns(
        "DO $$ BEGIN \
            LET v = (SELECT 1); \
            IF v >= 0 THEN RETURN QUERY SELECT v; END IF; \
        END $$",
    );
    assert_eq!(
        vec![DerivedType::new(UnrestrictedType::Integer)],
        columns.iter().map(|c| c.1).collect::<Vec<_>>()
    );

    // Multi-column bodies are picked up as well: the VDBE used to fall back to
    // a single result column whenever no top-level RETURN QUERY was present.
    let columns = return_columns(
        "DO $$ BEGIN \
            IF 1 > 0 THEN RETURN QUERY SELECT 1, 'x'; END IF; \
        END $$",
    );
    assert_eq!(
        vec![
            DerivedType::new(UnrestrictedType::Integer),
            DerivedType::new(UnrestrictedType::String),
        ],
        columns.iter().map(|c| c.1).collect::<Vec<_>>()
    );
}

/// A LET variable compiles to a runtime slot named after the source
/// identifier, at every nesting depth and however often the name is reused by
/// scopes that do not overlap. Renaming the slot to keep such variables apart
/// would be redundant -- they cannot observe each other's values -- and would
/// leak invented names like `v_2` into EXPLAIN and the generated SQL.
#[test]
fn let_slots_are_named_after_the_source_identifier() {
    use crate::ir::node::block::Block;
    use crate::ir::node::BlockStatement;

    let let_vars = |query: &str| {
        let plan = sql_to_ir_without_bind(query, &[]);
        let top_id = plan.get_top().unwrap();
        let Block::Anonymous(block) = plan.get_block_node(top_id).unwrap() else {
            panic!("expected an anonymous block");
        };
        fn collect(stmts: &[BlockStatement<crate::ir::node::NodeId>], out: &mut Vec<String>) {
            for stmt in stmts {
                match stmt {
                    BlockStatement::Let { var, .. } => out.push(var.to_string()),
                    BlockStatement::If { body, .. } => collect(body, out),
                    _ => {}
                }
            }
        }
        let mut vars = Vec::new();
        collect(&block.statements, &mut vars);
        vars
    };

    // Sibling bodies reusing a name.
    assert_eq!(
        vec![":v", ":v"],
        let_vars(
            "DO $$ BEGIN \
                IF 1 > 0 THEN LET v = (SELECT 1); RETURN QUERY SELECT v; END IF; \
                IF 2 > 0 THEN LET v = (SELECT 2); RETURN QUERY SELECT v; END IF; \
            END $$"
        )
    );

    // A body first, then the top level -- the top-level variable is an ordinary
    // one and must not be renamed on account of the body that preceded it.
    assert_eq!(
        vec![":v", ":v"],
        let_vars(
            "DO $$ BEGIN \
                IF 1 > 0 THEN LET v = (SELECT 1); RETURN QUERY SELECT v; END IF; \
                LET v = (SELECT 2); \
                UPDATE t2 SET e = v; \
            END $$"
        )
    );

    // A redeclaration within one scope is a re-assignment of the same slot.
    assert_eq!(
        vec![":v", ":v"],
        let_vars(
            "DO $$ BEGIN \
                LET v = (SELECT 1); \
                LET v = (SELECT v + 1); \
                UPDATE t2 SET e = v; \
            END $$"
        )
    );
}
