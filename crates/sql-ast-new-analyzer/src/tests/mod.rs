//! Analyzer snapshot tests.
//!
//! The same mechanism as the parser tests, one stage later:
//! parse, analyze against the mocked catalog - the [`MockCatalog`] built by
//! [`catalog`], pinned as DDL by the [`mocked_tables_reference`] snapshot below.
//! Finally render and pin result with an inline `insta` snapshot.
//!
//! The `Analyzed` rendering carries the analysis results — inferred types as
//! `::type` suffixes and implicit coercions as materialized `CAST` calls — so
//! one snapshot pins resolution and typing together, not mere acceptance.
//! Analysis rejections are pinned the same way: the error text is part of the contract.
//!
//! # Module layout
//! This file holds only the harness — the catalog, the two render helpers, and
//! the DDL snapshot that pins the catalog itself. The cases live in one module
//! per analyzer module, so a case sits next to the code that decides it and the
//! two files line up one to one.
//!
//! | module | covers |
//! |---|---|
//! | [`multiset`] | WITH clause, and the `UNION`/`EXCEPT`/`INTERSECT` tree |
//! | [`select`] | WHERE |
//! | [`table_expression`] | FROM clause, table factors, joins |
//! | [`expr`] | column references, expression types |
//! | [`asterisk`] | `*` and `t.*` expansion |
//!
//! A query exercises more than one of them at once, so the module is chosen by
//! what the case *pins*, not by what it mentions: `SELECT t.* FROM cte` is an
//! asterisk case even though it reads a CTE, and `WHERE` over a join is a WHERE
//! case even though the join decides which column it finds.
//!
//! The helpers below stay private. A private item is already in scope for every
//! descendant module, so the submodules reach them through `use super::…` and
//! nothing else in the crate can.

mod asterisk;
mod expr;
mod multiset;
mod select;
mod table_expression;

use sql_ast_new_corpus::MockCatalog;

use sql_ast_new_nodes::rendering::normalize_whitespace;
use sql_ast_new_nodes::AbstractSyntaxTree;
use sql_frontend::frontend::sql::Ast;
use sql_ir::ir::metadata::Metadata;
use sql_ir::ir::relation::{ColumnRole, SpaceEngine};
use sql_ir::ir::types::{DerivedType, UnrestrictedType};

/// The catalog these tests analyze against.
///
/// `MockCatalog` — the builder `sql-ast-new-corpus` registers the corpus schema
/// on — takes tables the way a `CREATE TABLE` would give them, so what a case
/// reads off `t1` is right here and not behind a fixture's own conventions.
/// This suite types every SQL construct it can, and wants a column of each type
/// to do it with; register further test-local tables here and every helper
/// below sees them.
///
/// Note that `text` and `string` are one type (`UnrestrictedType::String`), so
/// the DDL pinned by [`mocked_tables_reference`] renders both spellings as
/// `string`.
fn catalog() -> MockCatalog {
    use UnrestrictedType::{Boolean, Datetime, Decimal, Double, Integer, String};

    let mut catalog = MockCatalog::new();
    catalog.add_sharded(
        "t1",
        &[
            ("a", Integer, true),
            ("b", Integer, false),
            ("c", Double, true),
            ("d", Decimal, true),
            ("e", String, true),
            ("f", String, true),
            ("g", Boolean, true),
        ],
        &["a", "b"],
        &["b"],
    );
    catalog.add_sharded(
        "t2",
        &[
            ("a", Double, true),
            ("b", Decimal, false),
            ("c", Integer, true),
            ("d", String, true),
            ("e", Boolean, true),
            ("f", Datetime, true),
            ("g", String, true),
        ],
        &["c", "d"],
        &["b"],
    );
    catalog.add_sharded(
        "t3",
        &[("a", Integer, true), ("b", String, false)],
        &["a", "b"],
        &["a"],
    );
    catalog.add_sharded(
        "t4",
        &[("a", Integer, true), ("b", String, false)],
        &["a", "b"],
        &["a"],
    );
    catalog.add_sharded(
        "t5",
        &[("c", Integer, true), ("d", String, false)],
        &["c", "d"],
        &["c"],
    );
    // The one table with a composite primary key. Functional dependency is a
    // whole-key rule - grouping by part of a key determines nothing - and a
    // single-column key cannot tell the two apart.
    catalog.add_sharded(
        "t6",
        &[
            ("k1", Integer, false),
            ("k2", Integer, false),
            ("v", Integer, true),
        ],
        &["k1"],
        &["k1", "k2"],
    );
    catalog
}

/// Render one mocked table as the CREATE TABLE statement it stands in for
/// (user columns only; types in their `::type`-suffix spelling).
fn table_ddl(catalog: &MockCatalog, name: &str) -> String {
    let table = catalog.table(name).expect("mocked table must exist");
    let key_columns = |positions: &[usize]| {
        positions
            .iter()
            .map(|&pos| format!("\"{}\"", table.columns[pos].name))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let mut ddl = format!("CREATE TABLE \"{}\" (\n", table.name);
    for column in &table.columns {
        if *column.get_role() != ColumnRole::User {
            continue;
        }
        let ty = column.r#type.get().as_ref().expect("mocked type is known");
        let nullability = if column.is_nullable {
            "NULL"
        } else {
            "NOT NULL"
        };
        ddl.push_str(&format!("    \"{}\" {ty} {nullability},\n", column.name));
    }
    ddl.push_str(&format!(
        "    PRIMARY KEY ({})\n",
        key_columns(&table.primary_key.positions)
    ));
    let engine = match table.engine() {
        SpaceEngine::Memtx => "MEMTX",
        SpaceEngine::Vinyl => "VINYL",
    };
    let sharding_key = key_columns(table.get_sk().expect("mocked table is sharded"));
    ddl.push_str(&format!(
        ") USING {engine} DISTRIBUTED BY ({sharding_key});"
    ));
    ddl
}

/// The mocked tables used throughout these tests, rendered from the catalog
/// itself so this reference cannot go stale.
#[test]
fn mocked_tables_reference() {
    let catalog = catalog();
    let ddl = ["t1", "t2", "t3", "t4", "t5", "t6"]
        .map(|name| table_ddl(&catalog, name))
        .join("\n\n");
    insta::assert_snapshot!(ddl, @r#"
    CREATE TABLE "t1" (
        "a" int NULL,
        "b" int NOT NULL,
        "c" double NULL,
        "d" decimal NULL,
        "e" string NULL,
        "f" string NULL,
        "g" bool NULL,
        PRIMARY KEY ("b")
    ) USING MEMTX DISTRIBUTED BY ("a", "b");

    CREATE TABLE "t2" (
        "a" double NULL,
        "b" decimal NOT NULL,
        "c" int NULL,
        "d" string NULL,
        "e" bool NULL,
        "f" datetime NULL,
        "g" string NULL,
        PRIMARY KEY ("b")
    ) USING MEMTX DISTRIBUTED BY ("c", "d");

    CREATE TABLE "t3" (
        "a" int NULL,
        "b" string NOT NULL,
        PRIMARY KEY ("a")
    ) USING MEMTX DISTRIBUTED BY ("a", "b");

    CREATE TABLE "t4" (
        "a" int NULL,
        "b" string NOT NULL,
        PRIMARY KEY ("a")
    ) USING MEMTX DISTRIBUTED BY ("a", "b");

    CREATE TABLE "t5" (
        "c" int NULL,
        "d" string NOT NULL,
        PRIMARY KEY ("c")
    ) USING MEMTX DISTRIBUTED BY ("c", "d");

    CREATE TABLE "t6" (
        "k1" int NOT NULL,
        "k2" int NOT NULL,
        "v" int NULL,
        PRIMARY KEY ("k1", "k2")
    ) USING MEMTX DISTRIBUTED BY ("k1");
    "#);
}

// -------------------------------- Utils ---------------------------------

/// Parse + analyze a query and render the analyzed AST (with inferred `::type` suffixes).
/// Client-supplied parameter types are positional: the first entry types `$1`.
/// A `DerivedType::unknown` entry stands for a parameter the client
/// left untyped — inferred from context, like an absent one.
fn analyzed(query: &str, param_types: &[DerivedType]) -> String {
    let catalog = catalog();
    let ast = AbstractSyntaxTree::new(query).expect("expected to build AST");
    let analyzed = ast
        .analyze(&catalog, param_types)
        .expect("expected to analyze AST");
    normalize_whitespace(&analyzed.to_string())
}

/// Parse + analyze a query expected to fail analysis and render the error.
fn analyze_error(query: &str, param_types: &[DerivedType]) -> String {
    let catalog = catalog();
    let ast = AbstractSyntaxTree::new(query).expect("expected to build AST");
    ast.analyze(&catalog, param_types)
        .err()
        .expect("expected analyze to fail")
        .to_string()
}
