use sql_ast_new_corpus::MockCatalog;

use sql_ast_new_nodes::AbstractSyntaxTree;

use sql_frontend::frontend::sql::transform_into_plan_old;

use sql_frontend::frontend::sql::Ast;
use sql_ir::ir::metadata::Metadata;
use sql_ir::ir::node::expression::Expression;
use sql_ir::ir::node::relational::Relational;
use sql_ir::ir::node::{Projection, ReferenceAsteriskSource};
use sql_ir::ir::relation::{ColumnRole, SpaceEngine};
use sql_ir::ir::tree::traversal::PostOrder;
use sql_ir::ir::types::{DerivedType, UnrestrictedType};
use sql_ir::ir::Plan;

use super::{AnalyzedAst, AstTranslator};

// -------------------------------- Utils ---------------------------------

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
/// `text`.
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

    // Add indexes
    catalog.add_index("t1_a", "t1", &["a"]);

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
        "e" text NULL,
        "f" text NULL,
        "g" bool NULL,
        PRIMARY KEY ("b")
    ) USING MEMTX DISTRIBUTED BY ("a", "b");

    CREATE TABLE "t2" (
        "a" double NULL,
        "b" decimal NOT NULL,
        "c" int NULL,
        "d" text NULL,
        "e" bool NULL,
        "f" datetime NULL,
        "g" text NULL,
        PRIMARY KEY ("b")
    ) USING MEMTX DISTRIBUTED BY ("c", "d");

    CREATE TABLE "t3" (
        "a" int NULL,
        "b" text NOT NULL,
        PRIMARY KEY ("a")
    ) USING MEMTX DISTRIBUTED BY ("a", "b");

    CREATE TABLE "t4" (
        "a" int NULL,
        "b" text NOT NULL,
        PRIMARY KEY ("a")
    ) USING MEMTX DISTRIBUTED BY ("a", "b");

    CREATE TABLE "t5" (
        "c" int NULL,
        "d" text NOT NULL,
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

fn analyzed<'q>(
    query: &'q str,
    param_types: &'q [DerivedType],
    metadata: &'q impl Metadata,
) -> AnalyzedAst<'q> {
    AbstractSyntaxTree::<'q>::new(query)
        .expect("expected to build AST")
        .analyze(metadata, param_types)
        .expect("expected to analyze AST")
}

#[test]
fn select_without_scan() {
    let meta = catalog();
    let query = "SELECT 1, 2";

    let ir = analyzed(query, &[], &meta)
        .translate()
        .expect("expected to translate AST into IR plan");

    let arena = ir
        .formatted_arena()
        .expect("expected to get formatted arena");

    insta::assert_snapshot!(arena, @"
    ---------------------------------------------
    [id: 432] relation: SelectWithoutScan
    	Children:
    	Output:	[id: 064] expression: Row
    		List:
    		[id: 132] expression: Alias [name = col_1]
    			Child:
    			[id: 032] expression: Constant [value = 1]
    		[id: 332] expression: Alias [name = col_2]
    			Child:
    			[id: 232] expression: Constant [value = 2]
    ---------------------------------------------
    ")
}

#[test]
fn scan_with_indexed_by() {
    let meta = catalog();
    let query = "SELECT 1 + a FROM t1 INDEXED BY t1_a";

    let ir = analyzed(query, &[], &meta)
        .translate()
        .expect("expected to translate AST into IR plan");

    let arena = ir
        .formatted_arena()
        .expect("expected to get formatted arena");

    insta::assert_snapshot!(arena, @"
    ---------------------------------------------
    [id: 096] relation: ScanRelation
    	Relation: t1
    	[No children]
    	Columns: [a: int, b: int, c: double, d: decimal, e: text, f: text, g: bool, bucket_id: int (system)]
    ---------------------------------------------
    ---------------------------------------------
    [id: 296] relation: Projection
    	Children:
    		Child_id = 096
    	Output:	[id: 064] expression: Row
    		List:
    		[id: 232] expression: Alias [name = col_1]
    			Child:
    			[id: 132] expression: Arithmetic: [op: +]
    				Child
    				Left child
    				[id: 032] expression: Constant [value = 1]
    				Right child
    				[id: 196] expression: Reference
    					Alias: a
    					Referenced table name (or alias): t1
    					target_id: 096
    					Position: 0
    					Column type: int
    ---------------------------------------------
    ");

    assert!(ir.indexes.indexes.contains_key("t1_a"));
}

#[test]
fn simple_select_with_cte() {
    let meta = catalog();
    let query = "WITH cte AS (SELECT a FROM t2) SELECT 1 + a FROM t1 INDEXED BY t1_a";

    let ir = analyzed(query, &[], &meta)
        .translate()
        .expect("expected to translate AST into IR plan");

    let arena = ir
        .formatted_arena()
        .expect("expected to get formatted arena");

    insta::assert_snapshot!(arena, @"
    ---------------------------------------------
    [id: 396] relation: ScanRelation
    	Relation: t1
    	[No children]
    	Columns: [a: int, b: int, c: double, d: decimal, e: text, f: text, g: bool, bucket_id: int (system)]
    ---------------------------------------------
    ---------------------------------------------
    [id: 596] relation: Projection
    	Children:
    		Child_id = 396
    	Output:	[id: 164] expression: Row
    		List:
    		[id: 332] expression: Alias [name = col_1]
    			Child:
    			[id: 232] expression: Arithmetic: [op: +]
    				Child
    				Left child
    				[id: 132] expression: Constant [value = 1]
    				Right child
    				[id: 496] expression: Reference
    					Alias: a
    					Referenced table name (or alias): t1
    					target_id: 396
    					Position: 0
    					Column type: int
    ---------------------------------------------
    ");

    assert!(ir.indexes.indexes.contains_key("t1_a"));
}

// --------------------------------- CTE ----------------------------------

/// Plan arena (reachable from the top) built by the new pipeline.
fn new_arena(query: &str, meta: &MockCatalog) -> String {
    analyzed(query, &[], meta)
        .translate()
        .expect("expected to translate AST into IR plan")
        .formatted_arena()
        .expect("expected to get formatted arena for new pipeline IR")
}

/// Plan arena built by the old pipeline.
fn old_arena(query: &str, meta: &MockCatalog) -> String {
    transform_into_plan_old(query, &[], meta)
        .expect("expected to transform query into IR plan with old pipeline")
        .formatted_arena()
        .expect("expected to get formatted arena for old pipeline IR")
}

/// Arena with node ids renumbered by first appearance: the old pipeline adds
/// argument casts and ordinal references in post-passes, so its raw ids
/// differ from an in-order translation of the same plan.
fn normalized(arena: &str) -> String {
    let id =
        regex::Regex::new(r"(id: |Child_id = |target_id: |Subquery_id = |rel_id: )(\d+)").unwrap();
    let mut seen: Vec<String> = Vec::new();
    id.replace_all(arena, |caps: &regex::Captures| {
        let raw = &caps[2];
        let pos = seen.iter().position(|s| s == raw).unwrap_or_else(|| {
            seen.push(raw.to_owned());
            seen.len() - 1
        });
        format!("{}{pos}", &caps[1])
    })
    .into_owned()
}

/// Asterisk marks of every projection's output columns, in post-order:
/// `formatted_arena` does not print them.
fn star_tags(plan: &Plan) -> Vec<Vec<Option<ReferenceAsteriskSource>>> {
    let top = plan.get_top().expect("expected plan top");
    PostOrder::new(&plan.nodes, |node| plan.nodes.rel_iter(node), top)
        .filter_map(|(id, _)| match plan.get_relation_node(id) {
            Ok(Relational::Projection(Projection { output, .. })) => Some(*output),
            _ => None,
        })
        .map(|output| {
            let cols = plan.get_row_list(output).expect("expected output row");
            cols.iter()
                .map(|&col| {
                    let child = plan.get_child_under_alias(col).expect("expected alias");
                    match plan.get_expression_node(child) {
                        Ok(Expression::Reference(r)) => r.asterisk_source.clone(),
                        _ => None,
                    }
                })
                .collect()
        })
        .collect()
}

fn new_star_tags(query: &str) -> Vec<Vec<Option<ReferenceAsteriskSource>>> {
    let meta = catalog();
    let plan = analyzed(query, &[], &meta)
        .translate()
        .expect("expected to translate AST into IR plan");
    star_tags(&plan)
}

/// The new pipeline must build the same plan as the old one.
fn assert_same_as_old(query: &str) {
    let meta = catalog();
    pretty_assertions::assert_eq!(
        normalized(&new_arena(query, &meta)),
        normalized(&old_arena(query, &meta))
    );
    let old = transform_into_plan_old(query, &[], &meta).expect("expected old plan");
    pretty_assertions::assert_eq!(new_star_tags(query), star_tags(&old), "asterisk marks");
}

/// An unaliased item is named after the column it reads, otherwise `col_N`
/// counted over the unnamed items only.
#[test]
fn unaliased_items() {
    for query in [
        "SELECT a FROM t1",
        "SELECT * FROM t1",
        "SELECT t1.* FROM t1",
        "SELECT a, a + 1, b, 2 FROM t1",
        "SELECT 1, 2",
        "SELECT * FROM (SELECT 1, a FROM t1)",
        "SELECT x FROM (SELECT a AS x FROM t1)",
        "SELECT a FROM t1 GROUP BY a",
        "SELECT a FROM t1 GROUP BY 1",
    ] {
        assert_same_as_old(query);
    }
}

/// Columns from `*` / `t.*` are marked like the old pipeline marks them: the
/// storage SQL writes the `*` back, which reads duplicate names by position.
#[test]
fn asterisk_tags() {
    for query in [
        "SELECT * FROM t1",
        "SELECT t1.* FROM t1",
        "SELECT *, * FROM t1",
        "SELECT a, *, t1.* FROM t1",
        "SELECT * FROM t1 JOIN t2 ON true",
        "SELECT t2.* FROM t1 JOIN t2 ON true",
        "SELECT * FROM (SELECT * FROM t1)",
        "WITH c AS (SELECT a FROM t1) SELECT * FROM c",
        "SELECT * FROM t1 GROUP BY 1, 2, 3, 4, 5, 6, 7",
        "SELECT * FROM t1 GROUP BY a, b, c, d, e, f, g",
    ] {
        assert_same_as_old(query);
    }
}

/// A `*` over a USING merge is not the FROM clause's column list, so none of
/// it is marked: one `*` in the storage SQL would bring back the merged-away
/// column. `t.*` is not affected by the merge. Old has no USING.
#[test]
fn asterisk_tags_using() {
    let tags = new_star_tags("SELECT * FROM t1 JOIN t1 AS x USING (a)");
    assert!(tags.iter().flatten().all(Option::is_none), "{tags:?}");

    let tags = new_star_tags("SELECT t1.* FROM t1 JOIN t1 AS x USING (a)");
    let t1 = Some(ReferenceAsteriskSource::new(Some("t1".into()), 0));
    assert_eq!(tags, vec![vec![t1; 7]]);
}

/// PostgreSQL names a call after the function, a cast and CASE after their
/// operand; old names them `col_N`.
#[test]
fn item_names_diverge_from_old() {
    let meta = catalog();
    let alias = regex::Regex::new(r"Alias \[name = (\w+)\]").unwrap();
    let names = |arena: &str| {
        let names: Vec<_> = alias
            .captures_iter(arena)
            .map(|c| c[1].to_owned())
            .collect();
        names.join(" ")
    };
    for (query, new_names, old_names) in [
        ("SELECT count(*) FROM t1", "count", "col_1"),
        ("SELECT CAST(a AS text) FROM t1", "a", "col_1"),
        ("SELECT count(*), 1 FROM t1", "count col_1", "col_1 col_2"),
        // Inner item, then the outer one reading it.
        (
            "SELECT * FROM (SELECT count(*) FROM t1)",
            "count count",
            "col_1 col_1",
        ),
    ] {
        assert_eq!(names(&new_arena(query, &meta)), new_names, "{query}");
        assert_eq!(names(&old_arena(query, &meta)), old_names, "{query}");
    }
}

#[test]
fn cte_reference() {
    assert_same_as_old("WITH c AS (SELECT a AS a FROM t1) SELECT a AS a FROM c");
}

#[test]
fn cte_alias_names_the_scan() {
    assert_same_as_old("WITH c AS (SELECT a AS a FROM t1) SELECT a AS a FROM c AS q");
}

#[test]
fn cte_column_list_renames_by_position() {
    // `a` of `c` is the body's second column, `t1.b`.
    assert_same_as_old("WITH c (b, a) AS (SELECT a AS a, b AS b FROM t1) SELECT a AS a FROM c");
}

#[test]
fn cte_references_earlier_sibling() {
    assert_same_as_old(
        "WITH c1 AS (SELECT a AS a FROM t1), c2 AS (SELECT a AS a FROM c1) SELECT a AS a FROM c2",
    );
}

#[test]
fn cte_shadows_table() {
    assert_same_as_old("WITH t1 AS (SELECT c AS c FROM t2) SELECT c AS c FROM t1");
}

#[test]
fn cte_main_select_without_from() {
    // No FROM: one row, not one row per row of the CTE body.
    assert_same_as_old("WITH c AS (SELECT a AS a FROM t1) SELECT 1");
}

#[test]
fn cte_body_without_from_after_other_cte() {
    assert_same_as_old(
        "WITH c1 AS (SELECT a AS a FROM t1), c2 AS (SELECT 1 AS x) SELECT x AS x FROM c2",
    );
}

#[test]
fn cte_referenced_from_subquery() {
    assert_same_as_old(
        "WITH c AS (SELECT a AS a FROM t1) SELECT a AS a FROM (SELECT a AS a FROM c)",
    );
}

// The old pipeline rejects the cases below; they check which body the scan reads.

#[test]
fn cte_shadowed_by_nested_with() {
    // The old pipeline rejects the redefinition; the inner `c` wins here.
    let arena = new_arena(
        "WITH c AS (SELECT a AS a FROM t1) \
         SELECT b AS b FROM (WITH c AS (SELECT b AS b FROM t2) SELECT b AS b FROM c)",
        &catalog(),
    );
    assert!(arena.contains("Relation: t2"));
    assert!(!arena.contains("Relation: t1"));
}

#[test]
fn cte_body_with_own_with() {
    // The old grammar does not parse a WITH inside a CTE body.
    let arena = new_arena(
        "WITH c AS (WITH d AS (SELECT a AS a FROM t1) SELECT a AS a FROM d) SELECT a AS a FROM c",
        &catalog(),
    );
    assert_eq!(arena.matches("relation: ScanCte").count(), 2);
    assert!(arena.contains("Relation: t1"));
}

// -------------------------------- WHERE ---------------------------------

#[test]
fn where_comparison() {
    assert_same_as_old("SELECT a AS a FROM t1 WHERE b > 1");
}

#[test]
fn where_bucket_id() {
    assert_same_as_old("SELECT bucket_id AS x FROM t1 WHERE bucket_id = 1");
}

#[test]
fn where_each_comparison_op() {
    for op in ["=", "<>", "<", ">", "<=", ">="] {
        assert_same_as_old(&format!("SELECT a AS a FROM t1 WHERE a {op} b"));
    }
}

#[test]
fn where_and_or() {
    assert_same_as_old("SELECT a AS a FROM t1 WHERE b > 1 AND e = 'x' OR g");
}

#[test]
fn where_not() {
    assert_same_as_old("SELECT a AS a FROM t1 WHERE NOT g");
}

#[test]
fn where_and_chain_stays_left_nested() {
    assert_same_as_old("SELECT a AS a FROM t1 WHERE a = 1 AND b = 2 AND g");
}

/// The old pipeline rotates a right-nested AND one level up.
#[test]
fn where_right_nested_and_is_rotated() {
    assert_same_as_old("SELECT a AS a FROM t1 WHERE a = 1 AND (b = 2 AND g)");
    assert_same_as_old("SELECT a AS a FROM t1 WHERE a = 1 AND (b = 2 AND (e = 'x' AND g))");
    assert_same_as_old("SELECT a AS a FROM t1 WHERE a = 1 AND ((b = 2 AND e = 'x') AND g)");
}

#[test]
fn where_over_cte() {
    assert_same_as_old("WITH c AS (SELECT a AS a FROM t1) SELECT a AS a FROM c WHERE a > 1");
}

// ------------------------------- GROUP BY -------------------------------

#[test]
fn group_by_column() {
    assert_same_as_old("SELECT a AS a, count(*) AS c FROM t1 GROUP BY a");
}

#[test]
fn group_by_two_columns() {
    assert_same_as_old("SELECT a AS a, b AS b, sum(c) AS s FROM t1 GROUP BY a, b");
}

#[test]
fn group_by_expression() {
    assert_same_as_old("SELECT a + b AS s, count(*) AS c FROM t1 GROUP BY a + b");
}

#[test]
fn group_by_key_not_in_select_list() {
    assert_same_as_old("SELECT count(*) AS c FROM t1 GROUP BY a");
}

#[test]
fn group_by_ordinal_of_column() {
    assert_same_as_old("SELECT a AS a, count(*) AS c FROM t1 GROUP BY 1");
}

#[test]
fn group_by_alias_of_column() {
    assert_same_as_old("SELECT a AS x, count(*) AS c FROM t1 GROUP BY x");
}

#[test]
fn group_by_alias_of_expression() {
    assert_same_as_old("SELECT a + 1 AS x, count(*) AS c FROM t1 GROUP BY x");
}

#[test]
fn group_by_repeated_keys() {
    assert_same_as_old("SELECT a AS a, count(*) AS c FROM t1 GROUP BY a, a");
    assert_same_as_old("SELECT a AS a, count(*) AS c FROM t1 GROUP BY 1, 1");
}

/// The GroupBy node's section of an arena dump.
fn group_by_section(arena: &str) -> &str {
    let start = arena.find("relation: GroupBy").expect("plan has a GroupBy");
    let end = arena[start..].find("-----").expect("section ends");
    &arena[start..start + end]
}

/// Old pipeline defect: `replace_group_by_ordinals_with_references`
/// (sql-frontend `ir_populator/plan_ext.rs`) rewrites an ordinal only when
/// the select item is a bare column, so here the constant `1` stays the
/// grouping key. The new pipeline groups by the item, as `GROUP BY x` does.
#[test]
fn group_by_ordinal_of_expression_diverges_from_old() {
    let meta = catalog();
    let by_ordinal = "SELECT a + 1 AS x, count(*) AS c FROM t1 GROUP BY 1";
    let by_alias = "SELECT a + 1 AS x, count(*) AS c FROM t1 GROUP BY x";
    let new = new_arena(by_ordinal, &meta);
    pretty_assertions::assert_eq!(new, new_arena(by_alias, &meta));
    assert!(group_by_section(&new).contains("Arithmetic: [op: +]"));

    let old = group_by_section(&old_arena(by_ordinal, &meta)).to_owned();
    assert!(old.contains("Constant [value = 1]"));
    assert!(!old.contains("Arithmetic"));
}

/// Two keys naming one alias group by two copies of the aliased expression.
#[test]
fn group_by_repeated_alias() {
    let meta = catalog();
    for (by_alias, by_expr) in [
        (
            "SELECT a AS x, count(*) AS c FROM t1 GROUP BY x, x",
            "SELECT a AS x, count(*) AS c FROM t1 GROUP BY a, a",
        ),
        (
            "SELECT a + 1 AS x, count(*) AS c FROM t1 GROUP BY x, x",
            "SELECT a + 1 AS x, count(*) AS c FROM t1 GROUP BY a + 1, a + 1",
        ),
    ] {
        pretty_assertions::assert_eq!(
            normalized(&new_arena(by_alias, &meta)),
            normalized(&new_arena(by_expr, &meta))
        );
        assert_same_as_old(by_alias);
    }
}

// ------------------------------ Aggregates ------------------------------

#[test]
fn aggregates_without_group_by() {
    assert_same_as_old(
        "SELECT count(*) AS c, sum(a) AS s, min(b) AS mn, max(c) AS mx, avg(d) AS av, total(a) AS t FROM t1",
    );
}

/// Every aggregate argument is cast to its own type (Tarantool overload resolution).
#[test]
fn aggregate_argument_is_cast() {
    let query = "SELECT count(b) AS c FROM t1";
    assert_same_as_old(query);
    assert!(new_arena(query, &catalog()).contains("Cast [to = int]"));
}

/// An aggregate under a cast keeps its own type: the final stage reads it.
#[test]
fn aggregate_under_cast() {
    use sql_ir::ir::node::{Node64, ScalarFunction};

    let meta = catalog();
    for query in [
        "SELECT CAST(sum(c) AS int) AS x FROM t1",
        "SELECT CAST(count(*) AS text) AS x FROM t1",
        // An implicit cast: the old pipeline leaves `count(*)` uncast.
        "SELECT a AS a FROM t1 GROUP BY a HAVING count(*) > min(c)",
    ] {
        if !query.contains("HAVING") {
            assert_same_as_old(query);
        }
        let mut ir = analyzed(query, &[], &meta).translate().unwrap();
        assert!(ir.nodes.iter64().all(|node| !matches!(
            node,
            Node64::ScalarFunction(ScalarFunction { func_type, .. }) if func_type.get().is_none()
        )));
        ir.optimize().unwrap();
    }
}

#[test]
fn count_distinct() {
    assert_same_as_old("SELECT count(DISTINCT a) AS c FROM t1");
}

#[test]
fn group_concat() {
    assert_same_as_old("SELECT group_concat(e, ',') AS g FROM t1");
    assert_same_as_old("SELECT string_agg(f, ';') AS g FROM t1");
    assert_same_as_old("SELECT group_concat(DISTINCT e) AS g FROM t1");
}

// -------------------------------- HAVING --------------------------------

#[test]
fn having_aggregate() {
    assert_same_as_old("SELECT a AS a, count(*) AS c FROM t1 GROUP BY a HAVING count(*) > 1");
}

/// The select-list copy of an ordinal key must read the Having node, not the GroupBy.
#[test]
fn having_over_ordinal_key() {
    assert_same_as_old("SELECT a AS a, count(*) AS c FROM t1 GROUP BY 1 HAVING count(*) > 1");
}

#[test]
fn having_aggregate_and_key() {
    assert_same_as_old(
        "SELECT a AS a, max(b) AS m FROM t1 GROUP BY a HAVING max(b) > 10 AND a > 1",
    );
}

#[test]
fn having_key_not_in_select_list() {
    assert_same_as_old("SELECT count(*) AS c FROM t1 GROUP BY a HAVING a > 1");
}

#[test]
fn having_without_group_by() {
    assert_same_as_old("SELECT count(*) AS c FROM t1 HAVING count(*) > 0");
}

// ------------------------------- Combined -------------------------------

#[test]
fn where_group_by_having() {
    assert_same_as_old(
        "SELECT a AS a, max(b) AS m FROM t1 WHERE b > 0 GROUP BY a HAVING max(b) > 10",
    );
}

#[test]
fn distinct_with_group_by() {
    assert_same_as_old("SELECT DISTINCT a AS a FROM t1 GROUP BY a");
}

// --------------------------------- JOIN ---------------------------------

#[test]
fn join_on() {
    for query in [
        "SELECT t1.a AS a, t2.c AS c FROM t1 JOIN t2 ON t1.a = t2.c",
        "SELECT t1.a AS a, t2.c AS c FROM t1 LEFT JOIN t2 ON t1.a = t2.c",
        "SELECT t3.a AS a FROM t3 JOIN t4 ON true",
        "SELECT x.a AS a, y.b AS b FROM t3 AS x JOIN t4 AS y ON x.a = y.a",
        // The right-nested AND is rotated the way the old pipeline does it.
        "SELECT t1.a AS a FROM t1 JOIN t2 ON t1.a = t2.c AND (t1.b = t2.c AND t2.d = t1.e)",
    ] {
        assert_same_as_old(query);
    }
}

/// The second ON reads the chain built so far through the first join; WHERE
/// and the select list read the whole chain.
#[test]
fn join_chain() {
    assert_same_as_old(
        "SELECT t1.a AS a, t3.b AS b FROM t1 JOIN t2 ON t1.a = t2.c JOIN t3 ON t3.a = t2.c \
         WHERE t2.c = 1 AND t3.a = 2",
    );
}

/// The old pipeline turns CROSS JOIN into `INNER JOIN ... ON true`.
#[test]
fn join_cross() {
    for query in [
        "SELECT t1.a AS a, t2.c AS c FROM t1 CROSS JOIN t2",
        "SELECT * FROM t1 CROSS JOIN t2",
        "SELECT t1.a AS a FROM t1 CROSS JOIN t2 JOIN t3 ON t3.a = t2.c",
    ] {
        assert_same_as_old(query);
    }
}

#[test]
fn join_cte() {
    assert_same_as_old(
        "WITH c AS (SELECT c AS c FROM t2) SELECT t1.a AS a FROM t1 JOIN c ON t1.a = c.c",
    );
}

#[test]
fn join_grouped() {
    assert_same_as_old(
        "SELECT t1.a AS a, count(*) AS n FROM t1 JOIN t2 ON t1.a = t2.c \
         GROUP BY t1.a HAVING count(*) > 1",
    );
}

/// The old grammar has no USING, so the oracle is the equivalent ON query.
/// A bare reference to the merged column arrives as an identity cast, which
/// translation elides.
#[test]
fn join_using() {
    let meta = catalog();
    for (using, on) in [
        (
            "SELECT a AS a FROM t3 JOIN t4 USING (a)",
            "SELECT t3.a AS a FROM t3 JOIN t4 ON t3.a = t4.a",
        ),
        (
            "SELECT t3.a AS a FROM t3 LEFT JOIN t4 USING (a, b)",
            "SELECT t3.a AS a FROM t3 LEFT JOIN t4 ON t3.a = t4.a AND t3.b = t4.b",
        ),
    ] {
        assert_same_as_old(on);
        pretty_assertions::assert_eq!(
            normalized(&new_arena(using, &meta)),
            normalized(&new_arena(on, &meta))
        );
    }
}

/// USING over differently typed columns waits for Cast translation.
#[test]
fn join_using_mixed_types_unsupported() {
    let meta = catalog();
    assert!(
        analyzed("SELECT 1 AS x FROM t1 JOIN t2 USING (a)", &[], &meta)
            .translate()
            .is_err()
    );
}

/// Old pipeline defect: `build_join_condition_ir` (sql-frontend `ast/mod.rs`)
/// parses ON like any boolean expression, so an aggregate or a window function
/// lands in the join condition. PostgreSQL and the new analyzer reject both.
#[test]
fn join_on_aggregate_or_window_diverges_from_old() {
    let meta = catalog();
    for (query, old_marker) in [
        (
            "SELECT t1.a AS a FROM t1 JOIN t2 ON sum(t1.a) = t2.c",
            "ScalarFunction [name = sum",
        ),
        (
            "SELECT t1.a AS a FROM t1 JOIN t2 ON row_number() OVER () = t2.c",
            "expression: Over",
        ),
    ] {
        let err = AbstractSyntaxTree::new(query)
            .expect("expected to build AST")
            .analyze(&meta, &[])
            .err()
            .expect("expected the analyzer to reject the condition");
        assert!(err.to_string().contains("JOIN conditions"), "{err}");
        assert!(old_arena(query, &meta).contains(old_marker));
    }
}

// ---------------------------- SET OPERATIONS ----------------------------

#[test]
fn set_operations() {
    for query in [
        "SELECT a AS a FROM t1 UNION SELECT a AS a FROM t3",
        "SELECT a AS a FROM t1 UNION ALL SELECT a AS a FROM t3",
        "SELECT a AS a FROM t1 EXCEPT SELECT a AS a FROM t3",
        "SELECT a AS a FROM t1 EXCEPT DISTINCT SELECT a AS a FROM t3",
        // Left-associative: `(t1 UNION ALL t3) EXCEPT t4`.
        "SELECT a AS a FROM t1 UNION ALL SELECT a AS a FROM t3 EXCEPT SELECT a AS a FROM t4",
        "SELECT a AS a FROM t1 WHERE b = 1 UNION SELECT a AS a FROM t3 WHERE a = 2",
    ] {
        assert_same_as_old(query);
    }
}

#[test]
fn set_operation_with_cte() {
    assert_same_as_old(
        "WITH c AS (SELECT a AS a FROM t1) SELECT a AS a FROM c UNION SELECT a AS a FROM c",
    );
    // A non-projection CTE body is wrapped into a subquery to rename its columns.
    assert_same_as_old(
        "WITH c (x) AS (SELECT a AS a FROM t1 UNION SELECT a AS a FROM t3) SELECT x AS x FROM c",
    );
}

#[test]
fn set_operation_unsupported() {
    let meta = catalog();
    for (query, err) in [
        (
            "SELECT a AS a FROM t1 INTERSECT SELECT a AS a FROM t3",
            "INTERSECT is not supported",
        ),
        (
            "SELECT a AS a FROM t1 EXCEPT ALL SELECT a AS a FROM t3",
            "EXCEPT ALL is not supported",
        ),
    ] {
        let res = analyzed(query, &[], &meta).translate();
        assert_eq!(res.err().unwrap().to_string(), err, "{query}");
    }
}

/// The old grammar does not parse a WITH in an operand; the inner `c` wins there,
/// so the plan reaches its body (the only `b` alias).
#[test]
fn set_operation_operand_with_own_with() {
    let arena = new_arena(
        "WITH c AS (SELECT a AS a FROM t1) SELECT a AS a FROM c \
         UNION ALL (WITH c AS (SELECT b AS b FROM t1) SELECT b AS y FROM c)",
        &catalog(),
    );
    assert_eq!(arena.matches("relation: ScanCte").count(), 2);
    assert!(arena.contains("Alias [name = b]"));
}

/// ORDER BY and LIMIT are not translated yet, so they must not be dropped silently.
#[test]
fn order_by_and_limit_unsupported() {
    let meta = catalog();
    for query in [
        "SELECT a AS a FROM t1 LIMIT 1",
        "SELECT a AS a FROM t1 ORDER BY 1",
        "SELECT a AS a FROM t1 UNION SELECT a AS a FROM t3 LIMIT 1",
        "SELECT a AS a FROM t1 UNION SELECT a AS a FROM t3 ORDER BY 1",
        "(SELECT a AS a FROM t1 LIMIT 1) UNION ALL SELECT a AS a FROM t3",
    ] {
        assert!(analyzed(query, &[], &meta).translate().is_err(), "{query}");
    }
}

/// Operands keep their own types: the plan unifies them at the set operation
/// node, above an operand's DISTINCT or GROUP BY.
#[test]
fn set_operation_mixed_types() {
    for query in [
        "SELECT a AS a FROM t1 UNION SELECT c AS c FROM t1",
        "SELECT DISTINCT a AS a FROM t1 UNION ALL SELECT c AS c FROM t1",
        "SELECT a AS a FROM t1 GROUP BY 1 UNION ALL SELECT c AS c FROM t1",
        "SELECT a AS a FROM t1 UNION SELECT a AS a FROM t3 UNION ALL SELECT c AS c FROM t1",
        "SELECT a AS a FROM t1 EXCEPT SELECT d AS d FROM t1",
        "SELECT 1 AS x UNION SELECT c AS x FROM t1",
        // No cast below the operand's DISTINCT, aggregate or GROUP BY.
        "SELECT DISTINCT a + 0 AS x FROM t1 UNION ALL SELECT c AS c FROM t1",
        "SELECT sum(a) AS s FROM t1 UNION ALL SELECT c AS c FROM t1",
        "SELECT a + b AS x FROM t1 GROUP BY a + b UNION ALL SELECT c AS c FROM t1",
        // A reference to the unified column has the unified type.
        "WITH q AS (SELECT a AS x FROM t1 UNION SELECT c AS x FROM t1) SELECT x AS x FROM q",
    ] {
        assert_same_as_old(query);
    }
}

// ------------------------------ SUBQUERIES ------------------------------

#[test]
fn from_subquery() {
    for query in [
        "SELECT x AS x FROM (SELECT a AS x FROM t1) AS s",
        "SELECT s.x AS x FROM (SELECT a AS x FROM t1) AS s WHERE s.x > 1",
        "SELECT a AS a FROM (SELECT a AS a FROM t1)",
        // Unnamed subqueries are named in scan order, inner first.
        "SELECT a AS a FROM (SELECT a AS a FROM (SELECT a AS a FROM t1))",
        "SELECT x AS x FROM (SELECT a AS x FROM t1) JOIN (SELECT a AS y FROM t3) ON x = y",
        "SELECT t1.a AS a FROM t1 JOIN (SELECT a AS a FROM t3) AS s ON t1.a = s.a",
    ] {
        assert_same_as_old(query);
    }
}

/// An unnamed subquery skips names the query uses, wherever they appear.
#[test]
fn unnamed_subquery_skips_used_aliases() {
    for query in [
        "SELECT x AS x FROM (SELECT a AS x FROM t1) AS unnamed_subquery JOIN (SELECT a AS y FROM t3) ON x = y",
        "SELECT x AS x FROM (SELECT a AS x FROM t1) JOIN (SELECT a AS y FROM t3) AS unnamed_subquery ON x = y",
        "SELECT a AS a FROM (SELECT a AS a FROM (SELECT a AS a FROM t1)) AS unnamed_subquery",
        "SELECT x AS x FROM (SELECT a AS x FROM t1) JOIN (SELECT a AS y FROM t3) AS unnamed_subquery_1 ON x = y \
         JOIN (SELECT a AS z FROM t4) ON x = z",
        "SELECT x AS x FROM (SELECT a AS x FROM t1) JOIN t3 AS unnamed_subquery ON x = unnamed_subquery.a",
        "WITH unnamed_subquery AS (SELECT a AS y FROM t3) \
         SELECT x AS x FROM (SELECT a AS x FROM t1) JOIN unnamed_subquery AS u ON x = u.y",
    ] {
        assert_same_as_old(query);
    }

    let mut meta = catalog();
    meta.add_sharded(
        "unnamed_subquery",
        &[("a", UnrestrictedType::Integer, false)],
        &["a"],
        &["a"],
    );
    let query = "SELECT x AS x FROM (SELECT a AS x FROM t1) JOIN unnamed_subquery ON x = unnamed_subquery.a";
    pretty_assertions::assert_eq!(
        normalized(&new_arena(query, &meta)),
        normalized(&old_arena(query, &meta))
    );
}

/// The optimizer names a motion over a join from the same set.
#[test]
fn unnamed_join_skips_used_aliases() {
    let meta = catalog();
    let query =
        "WITH c AS (SELECT a AS a FROM t3) SELECT unnamed_join.a AS a FROM c AS unnamed_join \
                 LEFT JOIN t1 ON unnamed_join.a = t1.a LEFT JOIN c AS d ON unnamed_join.a = d.a";
    let optimized = |mut ir: Plan| ir.optimize().unwrap().formatted_arena().unwrap();
    let new = optimized(analyzed(query, &[], &meta).translate().unwrap());
    let old = optimized(transform_into_plan_old(query, &[], &meta).unwrap());
    assert!(new.contains("alias = unnamed_join_1]"));
    pretty_assertions::assert_eq!(normalized(&new), normalized(&old));
}

#[test]
fn expression_subquery() {
    for query in [
        "SELECT a AS a FROM t1 WHERE a IN (SELECT a AS a FROM t3)",
        "SELECT a AS a FROM t1 WHERE a NOT IN (SELECT a AS a FROM t3)",
        "SELECT a AS a FROM t1 WHERE EXISTS (SELECT a AS a, b AS b FROM t3)",
        "SELECT a AS a FROM t1 WHERE NOT EXISTS (SELECT a AS a FROM t3)",
        "SELECT a AS a FROM t1 WHERE a = (SELECT a AS a FROM t3)",
        "SELECT a AS a FROM t1 WHERE a > (SELECT max(a) AS m FROM t3)",
        // The owner lists subqueries in expression order.
        "SELECT a AS a FROM t1 WHERE a IN (SELECT a AS a FROM t3) AND b IN (SELECT a AS a FROM t4)",
        "SELECT a AS a FROM t1 GROUP BY a, (SELECT a AS a FROM t3)",
        "SELECT a AS a FROM t1 GROUP BY a HAVING a IN (SELECT a AS a FROM t3)",
        "SELECT (SELECT a AS a FROM t3) AS x FROM t1",
        "SELECT (SELECT 1 AS y) AS x",
        "SELECT t1.a AS a FROM t1 JOIN t3 ON t1.a IN (SELECT a AS a FROM t4)",
        "SELECT x AS x FROM (SELECT a AS x FROM t1 WHERE a IN (SELECT a AS a FROM t3))",
        "SELECT a AS a FROM t1 WHERE a IN (SELECT a AS a FROM t3 WHERE a IN (SELECT a AS a FROM t4))",
    ] {
        assert_same_as_old(query);
    }
}

#[test]
fn expression_subquery_unsupported() {
    let meta = catalog();
    for query in [
        // Correlated: the old pipeline rejects it too.
        "SELECT a AS a FROM t1 WHERE EXISTS (SELECT a AS a FROM t3 WHERE t3.a = t1.a)",
        // The analyzer casts the subquery to the common type.
        "SELECT a AS a FROM t1 WHERE c IN (SELECT a AS a FROM t3)",
        // The old pipeline groups by the constant `1` instead.
        "SELECT (SELECT 1 AS y) AS x FROM t1 GROUP BY 1",
    ] {
        assert!(analyzed(query, &[], &meta).translate().is_err(), "{query}");
    }
}

/// The analyzer casts a scalar subquery to the common type; the old pipeline
/// compares as is.
#[test]
fn scalar_subquery_mixed_types_diverges_from_old() {
    let meta = catalog();
    let query = "SELECT a AS a FROM t1 WHERE c = (SELECT a AS a FROM t3)";
    assert!(new_arena(query, &meta).contains("Cast [to = double]"));
    assert!(!old_arena(query, &meta).contains("Cast"));
}
