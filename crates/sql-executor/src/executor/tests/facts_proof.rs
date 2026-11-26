//! Route every stage of the queries below on the mock runtime and check the
//! single-node decisions of AddMotions.
//!
//! What is checked (mock = bucket discovery only, no dispatch or execution):
//! - I: every node AddMotions proved single-node routes to Any or at most one
//!   bucket after bind, in the stage that computes it (bucket discovery
//!   itself fails otherwise);
//! - II: a stage holding such a node runs at most once, by the dispatch rule
//!   of `src/sql/router.rs::bucket_dispatch`: no bucket -> once if the stage
//!   emits a row from empty input, else never; Any -> once; buckets -> once
//!   per replicaset.
//!
//! Inputs of Segment / Full motions are materialized twice: empty, and
//! "spread" (rows 1..=3 in every column, resharded by the motion key). The
//! planner never proves a stage over a Segment motion from the vtable, so
//! a spread table may only show more buckets than real data would.

use std::collections::HashSet;
use std::rc::Rc;

use crate::executor::engine::mock::RouterRuntimeMock;
use crate::executor::engine::Router;
use crate::executor::vtable::VirtualTable;
use crate::executor::ExecutingQuery;
use crate::ir::bucket::{BucketSet, Buckets};
use crate::ir::node::relational::Relational;
use crate::ir::node::{Motion, NodeId, Projection};
use crate::ir::options::Options;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::types::{DerivedType, UnrestrictedType};
use crate::ir::value::Value;
use crate::ir::Plan;
use crate::test_helpers::vcolumn_integer_user_non_null;
use crate::utils::MutexLike;
use sql_frontend::frontend::sql::transform_into_plan;

/// Queries checked as is and inside the wrappers of `wrap`.
const BASES: [&str; 68] = [
    // bucket_id and plain keys
    "SELECT id AS a FROM test_space WHERE bucket_id = 42",
    "SELECT id AS a FROM test_space WHERE bucket_id = 0 AND id = 1",
    "SELECT id AS a FROM test_space WHERE bucket_id = 42 AND id = 1",
    "SELECT id AS a FROM test_space WHERE bucket_id = $1 AND id = $2",
    "SELECT a FROM t WHERE a = $1 AND b = $2",
    "SELECT a FROM t WHERE a = 1 AND b = 2",
    "SELECT a FROM t WHERE (a,b) = ($1,$2)",
    "SELECT c AS a FROM (SELECT count(*) AS c FROM t5 WHERE a = $1) s JOIN global_t ON true",
    // composite keys, OR, types and casts
    "SELECT a FROM t WHERE b = 2 AND c = 3 AND a = 1",
    "SELECT a FROM t WHERE (a = 1 AND b = 2 AND c = 1) OR (a = 1 AND b = 2 AND c = 2)",
    "SELECT a FROM t WHERE a = $1 AND (b = $2 OR (b = $2 AND c = 1))",
    "SELECT a FROM t WHERE a = 1::decimal AND b = 1::decimal",
    "SELECT a FROM dec_t WHERE a = 1",
    "SELECT a FROM dec_t WHERE a = 1.5",
    "SELECT a FROM t5 WHERE a = $1 OR a = $2",
    "SELECT a FROM t5 WHERE a = $1 AND a = $1",
    "SELECT a FROM t5 WHERE CAST(a AS int) = 1",
    "SELECT a FROM t5 WHERE a = int '1'",
    "SELECT a FROM t5 WHERE a = 1 AND b = NULL",
    // `bool_sharded` is sharded by `b`: IS TRUE / IS FALSE pin it
    "SELECT payload AS a FROM bool_sharded WHERE b IS FALSE",
    "SELECT payload AS a FROM bool_sharded WHERE b IS TRUE AND payload = $1",
    "SELECT payload AS a FROM bool_sharded WHERE b IS TRUE AND b IS FALSE",
    // `x IS TRUE` is `x` as a conjunct
    "SELECT id AS a FROM test_space WHERE (id = $1) IS TRUE",
    "SELECT id AS a FROM test_space WHERE (id = 1 AND id = $1) IS TRUE",
    "SELECT payload AS a FROM bool_sharded WHERE (b = false) IS TRUE AND b IS TRUE",
    // bucket_id
    "SELECT id AS a FROM test_space WHERE bucket_id = 100000",
    "SELECT id AS a FROM test_space WHERE bucket_id = 42.5",
    "SELECT id AS a FROM test_space WHERE bucket_id = 42e0",
    "SELECT id AS a FROM test_space WHERE bucket_id = 0.5",
    "SELECT id AS a FROM test_space WHERE bucket_id = $1",
    "SELECT id AS a FROM test_space WHERE bucket_id = 42 OR bucket_id = 43",
    // joins
    "SELECT x.a FROM (SELECT a FROM t5 WHERE a = $1) x JOIN t5 y ON x.a = y.a JOIN t5 z ON y.a = z.a",
    "SELECT x.a FROM (SELECT a FROM t5 WHERE a = 1) x JOIN t5 y ON x.a = y.b JOIN t5 z ON y.a = z.a",
    "SELECT s.a FROM global_t g1 JOIN (SELECT a FROM t5 WHERE a = $1) s ON true JOIN global_t g2 ON g2.a = s.a",
    "SELECT l.a FROM t5 l LEFT JOIN (SELECT a FROM t5 WHERE a = 1) r ON l.a = r.a",
    // an empty input of a global INNER JOIN under a LEFT JOIN
    "SELECT q.x AS a FROM (SELECT s.a AS x FROM (SELECT a FROM t5 WHERE false) s JOIN global_t ON true) q LEFT JOIN t5 ON q.x = t5.a",
    "SELECT s.a FROM global_t JOIN (SELECT a FROM t5 WHERE a = 1 AND a = 2) s ON true LEFT JOIN t5 ON s.a = t5.a",
    "WITH s AS (SELECT a FROM t5 WHERE a = $1) SELECT s.a FROM s JOIN global_t ON true LEFT JOIN t5 AS r ON s.a = r.a",
    "SELECT l.a FROM t5 l LEFT JOIN t5 r ON l.a = r.a WHERE l.a = $1",
    "SELECT l.a FROM t5 l LEFT JOIN t5 r ON l.a = r.a WHERE r.a = 1",
    "SELECT l.a FROM t5 l JOIN t5 r ON l.a = r.a WHERE r.a = $1",
    "SELECT l.a FROM t5 l JOIN t5 r ON l.b = r.b WHERE l.a = 1 AND r.a = 1",
    // conditions of one domain that conflict only together
    "SELECT a FROM (SELECT a, b FROM t5 WHERE b = 1) q WHERE q.b = 2",
    "SELECT l.a FROM (SELECT a FROM t5 WHERE a = 1) l JOIN (SELECT a FROM t5 WHERE a = 2) r ON l.a = r.a",
    "SELECT l.a FROM (SELECT a FROM t5 WHERE a = $1) l JOIN (SELECT a FROM t5 WHERE a = $2) r ON l.a = r.a",
    // unions
    "SELECT a FROM t5 WHERE a = 1 UNION SELECT a FROM t5 WHERE a = 1",
    "SELECT a FROM t5 WHERE a = 1 UNION SELECT a FROM t5 WHERE a = 2",
    "SELECT a FROM t5 WHERE a = $1 UNION ALL SELECT a FROM t5 WHERE a = $1",
    "SELECT a FROM t5 WHERE a = $1 UNION ALL SELECT a FROM (SELECT a FROM t5 WHERE a = $1 UNION SELECT a FROM t5 WHERE a = $1) u",
    "SELECT a FROM global_t UNION ALL SELECT a FROM t5 WHERE a = 1",
    // scalar aggregates
    "SELECT count(*) AS a FROM t5 WHERE a = 1 UNION ALL SELECT count(*) AS a FROM t5 WHERE a = 1",
    "SELECT count(*) AS a FROM t5 WHERE a = 1 UNION ALL SELECT count(*) AS a FROM t5 WHERE a = 2",
    "SELECT count(*) AS a FROM t5 WHERE a = $1 UNION SELECT count(*) AS a FROM t5 WHERE a = $1",
    "SELECT count(*) AS a FROM t5 WHERE a = 1 UNION ALL SELECT count(*) AS a FROM t5 WHERE a = 1 AND a = 2",
    "SELECT count(*) AS a FROM t5 WHERE a = 1 AND a = 2",
    "SELECT s.c AS a FROM (SELECT count(*) AS c FROM t5 WHERE a = 1) s JOIN t5 r ON r.a = 1",
    "SELECT c AS a FROM global_t JOIN (SELECT count(*) AS c FROM t5 WHERE a = $1) s ON true",
    "SELECT u.a FROM (SELECT count(*) AS a FROM t5 WHERE a = $1 UNION ALL SELECT count(*) AS a FROM t5 WHERE a = $1) u JOIN global_t ON true",
    // CTE, subqueries, SELECT without FROM, LIMIT
    "WITH s AS (SELECT a FROM t5 WHERE a = $1) SELECT a FROM s UNION ALL SELECT a FROM s",
    "WITH s AS (SELECT a FROM t5 WHERE a = 1) SELECT s1.a FROM s s1 JOIN s s2 ON s1.a = s2.a",
    "WITH s AS (SELECT count(*) AS a FROM t5 WHERE a = 1) SELECT a FROM s UNION ALL SELECT a FROM s",
    "SELECT a FROM t5 WHERE a = 1 AND b IN (SELECT b FROM t5 WHERE a = 1)",
    "SELECT a FROM t5 WHERE a = $1 AND EXISTS (SELECT 1 FROM t5 WHERE a = $1)",
    "SELECT a FROM t5 WHERE a = 1 AND b IN (SELECT b FROM t5 WHERE a = 2)",
    "SELECT (SELECT a FROM t5 WHERE a = 1) AS a",
    "SELECT (SELECT count(*) FROM t5 WHERE a = $1) AS a",
    "SELECT a FROM (SELECT a FROM t5 WHERE a = $1 ORDER BY a LIMIT 1) s",
    "SELECT a FROM t5 WHERE a = $1 LIMIT 1",
];

fn wrap(base: &str) -> Vec<String> {
    vec![
        base.to_string(),
        format!("SELECT count(*) FROM ({base}) q"),
        format!("SELECT a FROM ({base}) q ORDER BY a LIMIT 1"),
        format!("SELECT count(*) FROM ({base}) q UNION ALL SELECT count(*) FROM t5 WHERE a = $1"),
        format!("SELECT count(*) FROM t5 WHERE a = $1 AND b IN ({base})"),
    ]
}

fn param_count(sql: &str) -> usize {
    (1..=9)
        .filter(|i| sql.contains(&format!("${i}")))
        .max()
        .unwrap_or(0)
}

fn bindings(n: usize) -> Vec<Vec<Value>> {
    let values = [Value::Null, Value::from(1), Value::from(2)];
    let mut result = vec![vec![]];
    for _ in 0..n {
        result = result
            .into_iter()
            .flat_map(|prefix| {
                values.iter().map(move |v| {
                    let mut next = prefix.clone();
                    next.push(v.clone());
                    next
                })
            })
            .collect();
    }
    result
}

/// The nodes a stage computes: from `top` down to the motions it reads.
fn stage_nodes(plan: &Plan, top: NodeId) -> HashSet<NodeId> {
    let mut nodes = HashSet::new();
    let mut stack = vec![top];
    while let Some(id) = stack.pop() {
        if !nodes.insert(id) {
            continue;
        }
        if let Ok(Relational::Motion(Motion { policy, .. })) = plan.get_relation_node(id) {
            if matches!(policy, MotionPolicy::Segment(_) | MotionPolicy::Full) && id != top {
                continue;
            }
        }
        // Local / LocalSegment run inside the stage; children include subqueries.
        stack.extend(plan.nodes.rel_iter(id));
    }
    nodes
}

fn runs(buckets: &Buckets, emits: bool, coordinator: &RouterRuntimeMock) -> usize {
    match buckets {
        Buckets::Filtered(BucketSet::Exact(set)) if set.is_empty() => usize::from(emits),
        Buckets::Any => 1,
        other => coordinator.vshard_mock.group(other).len(),
    }
}

/// The aggregate whose direct input `d` is, so with no motion between them:
/// `Some(true)` for a scalar aggregate, `Some(false)` for a grouped one,
/// `None` for no aggregate.
fn aggregate_over(plan: &Plan, d: NodeId) -> Option<bool> {
    let top = plan.get_top().ok()?;
    let mut stack = vec![top];
    let mut seen = HashSet::new();
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        stack.extend(plan.nodes.rel_iter(id));
        let Ok(Relational::Projection(Projection {
            group_by,
            having,
            output,
            child,
            ..
        })) = plan.get_relation_node(id)
        else {
            continue;
        };
        if having.or(*group_by).or(*child) != Some(d) {
            continue;
        }
        let aggregates = plan.contains_aggregates(*output, true).unwrap_or(false);
        return if group_by.is_some() {
            Some(false)
        } else if having.is_some() || aggregates {
            Some(true)
        } else {
            None
        };
    }
    None
}

fn spread_vtable(columns: usize, spread: bool) -> VirtualTable {
    let mut table = VirtualTable::new();
    for _ in 0..columns {
        table.add_column(vcolumn_integer_user_non_null());
    }
    if spread {
        for v in 1..=3 {
            table.add_tuple(vec![Value::from(v); columns]);
        }
    }
    table
}

/// Route all stages of one bound plan; returns the verdict of each stage.
/// Adds the number of single-node decisions checked to `checked`.
fn route_binding(
    coordinator: &RouterRuntimeMock,
    plan: Plan,
    spread: bool,
    checked: &mut usize,
) -> Result<String, String> {
    let mut query = ExecutingQuery::from_plan(coordinator, plan).map_err(|e| e.to_string())?;
    let slices = query.get_exec_plan().get_ir_plan().slices().clone();
    let mut decisions: Vec<NodeId> = query
        .get_exec_plan()
        .get_ir_plan()
        .facts_proof
        .single_node
        .iter()
        .copied()
        .collect();
    // A stable report order.
    decisions.sort_by_key(|id| format!("{id:?}"));
    let mut stages: Vec<(NodeId, Buckets, bool)> = Vec::new();
    for slice in slices.slices() {
        for motion_id in slice.positions() {
            let plan = query.get_exec_plan().get_ir_plan();
            let Relational::Motion(Motion { policy, child, .. }) = plan
                .get_relation_node(*motion_id)
                .map_err(|e| e.to_string())?
            else {
                continue;
            };
            let policy = policy.clone();
            if matches!(policy, MotionPolicy::Local | MotionPolicy::LocalSegment(_)) {
                continue;
            }
            if child.is_some_and(|c| matches!(plan.get_relation_node(c), Ok(Relational::Values(_))))
            {
                continue;
            }
            let top = plan
                .get_motion_subtree_root(*motion_id)
                .map_err(|e| e.to_string())?;
            let columns = plan
                .get_relational_output(*motion_id)
                .and_then(|out| plan.get_row_list(out).map(|l| l.len()))
                .map_err(|e| e.to_string())?;
            let buckets = query.bucket_discovery(top).map_err(|e| e.to_string())?;
            let emits = query
                .get_exec_plan()
                .subtree_dispatch_flags_at(top)
                .map_err(|e| e.to_string())?
                .emits_row_from_empty_input;
            stages.push((top, buckets, emits));
            let mut vtable = spread_vtable(columns, spread);
            if let MotionPolicy::Segment(key) = &policy {
                vtable
                    .reshard(key, coordinator)
                    .map_err(|e| e.to_string())?;
            }
            let vshard = coordinator
                .get_vshard_object_by_tier(None)
                .map_err(|e| e.to_string())?;
            let exec_plan = query.get_mut_exec_plan();
            exec_plan
                .set_motion_vtable(motion_id, vtable, &vshard)
                .map_err(|e| e.to_string())?;
            exec_plan
                .mark_motion_subtree_unlinked(*motion_id)
                .map_err(|e| e.to_string())?;
        }
    }
    let top = query
        .get_exec_plan()
        .get_ir_plan()
        .get_top()
        .map_err(|e| e.to_string())?;
    let buckets = query.bucket_discovery(top).map_err(|e| e.to_string())?;
    let emits = query
        .get_exec_plan()
        .subtree_dispatch_flags_at(top)
        .map_err(|e| e.to_string())?
        .emits_row_from_empty_input;
    stages.push((top, buckets, emits));

    let plan = query.get_exec_plan().get_ir_plan();
    let mut fields = Vec::new();
    for (stage_top, buckets, emits) in &stages {
        let nodes = stage_nodes(plan, *stage_top);
        let stage_runs = runs(buckets, *emits, coordinator);
        let mut verdict = Vec::new();
        let mut held = 0;
        for d in decisions.iter().filter(|d| nodes.contains(d)) {
            let scalar_aggregate = aggregate_over(plan, *d) == Some(true);
            held += 1;
            *checked += 1;
            let kind = plan
                .get_relation_node(*d)
                .map(|rel| {
                    format!("{rel:?}")
                        .chars()
                        .take_while(|c| c.is_alphanumeric())
                        .collect::<String>()
                })
                .unwrap_or_default();
            let d = &format!("{d:?}/{kind}");
            // A scalar aggregate right over a proven node skipped its gather,
            // so it runs where the stage does: exactly once, even on no input.
            if scalar_aggregate && stage_runs != 1 {
                verdict.push(format!("VIOLATION_II {d:?} runs={stage_runs}"));
            } else if stage_runs > 1 {
                verdict.push(format!("WARN_II {d:?} runs={stage_runs}"));
            }
        }
        fields.push(format!(
            "{stage_top:?}:{buckets}:emits={emits}:runs={stage_runs}:decisions={held}{}",
            if verdict.is_empty() {
                String::new()
            } else {
                format!(":{}", verdict.join(","))
            }
        ));
    }
    Ok(fields.join(" | "))
}

/// Properties I and II on every stage of `BASES`, for every binding
/// of NULL, 1 and 2 and on empty and spread motion inputs.
#[test]
fn facts_proof_keeps_its_promise_on_every_stage() {
    let coordinator = RouterRuntimeMock::new();
    let mut checked = 0;
    let mut failures = Vec::new();
    for sql in BASES.iter().flat_map(|base| wrap(base)) {
        let n = param_count(&sql);
        let types = vec![DerivedType::new(UnrestrictedType::Integer); n];
        let plan = {
            let metadata = coordinator.metadata().lock();
            let mut plan = transform_into_plan(&sql, &types, &*metadata).unwrap();
            plan.optimize().unwrap();
            plan
        };
        for params in bindings(n) {
            for spread in [false, true] {
                let mut bound = plan.clone();
                bound
                    .bind_statement(params.clone(), Options::default())
                    .unwrap();
                let at = format!("{sql} {params:?} spread={spread}");
                match route_binding(&coordinator, bound, spread, &mut checked) {
                    Ok(fields) if fields.contains("VIOLATION") => {
                        failures.push(format!("{at}: {fields}"));
                    }
                    Ok(_) => {}
                    Err(e) => failures.push(format!("{at}: {e}")),
                }
            }
        }
    }
    assert!(checked > 0, "no single-node decision was checked");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A lost witness is found where the tree walk alone does not narrow the
/// route: `x.a = y.b` keeps the pin of `x.a` out of the walk's reach.
#[test]
fn lost_witness_fails_instead_of_running_wider() {
    let sql = "SELECT x.a FROM (SELECT a FROM t5 WHERE a = 1) x \
               JOIN t5 y ON x.a = y.b JOIN t5 z ON y.a = z.a";
    let coordinator = RouterRuntimeMock::new();
    let mut plan = {
        let metadata = coordinator.metadata().lock();
        let mut plan = transform_into_plan(sql, &[], &*metadata).unwrap();
        plan.optimize_statement().unwrap();
        plan
    };
    plan.bind_statement(vec![], Options::default()).unwrap();
    let mut checked = 0;
    route_binding(&coordinator, plan.clone(), true, &mut checked).unwrap();

    assert!(!plan.facts_proof.routes.is_empty());
    Rc::make_mut(&mut plan.facts_proof).routes.clear();
    let error = route_binding(&coordinator, plan, true, &mut checked).unwrap_err();
    assert!(
        error.contains("proven single-node by the planner"),
        "{error}"
    );
}

/// The inputs of a node that AddMotions keeps on one bucket are checked
/// together, not only each on its own: with the witness of one input
/// distorted to the bucket of 2, each still reads one bucket, but not the
/// same one. `b = 1 AND a = b` keeps the pin of `a` out of the walk's reach.
/// EXCEPT is routed by its left input alone, so it is not checked so; a
/// pinned SELECT without FROM has a check of its own. Enrichment writes the
/// pin into the filter, where the walk sees it.
#[cfg(not(feature = "enrich_restrictions"))]
#[test]
fn inputs_split_by_a_distorted_witness_fail() {
    use crate::ir::bucket::facts_route::{FactsRoute, FactsRoutePart};

    const PIN: &str = "SELECT a FROM t5 WHERE b = 1 AND a = b";
    const COUNT: &str = "SELECT count(*) FROM t5 WHERE b = 1 AND a = b";
    let queries = [
        format!("{PIN} UNION {PIN}"),
        format!("{COUNT} UNION ALL {COUNT}"),
        format!("{PIN} UNION {PIN} UNION {PIN}"),
    ];
    let coordinator = RouterRuntimeMock::new();
    for sql in &queries {
        let mut plan = {
            let metadata = coordinator.metadata().lock();
            let mut plan = transform_into_plan(sql, &[], &*metadata).unwrap();
            plan.optimize_statement().unwrap();
            plan
        };
        plan.bind_statement(vec![], Options::default()).unwrap();
        let mut checked = 0;
        route_binding(&coordinator, plan.clone(), true, &mut checked).unwrap();

        let proof = Rc::make_mut(&mut plan.facts_proof);
        let host = *proof.routes.keys().next().unwrap();
        proof.routes.insert(
            host,
            vec![FactsRoute::Key(vec![FactsRoutePart::Value(Value::from(2))])],
        );
        let error = route_binding(&coordinator, plan, true, &mut checked).unwrap_err();
        assert!(
            error.contains("proven single-node by the planner"),
            "{sql}: {error}"
        );
    }
}

/// DML over the read shapes of `BASES`: the stage that reads the rows
/// to write keeps properties I and II. INSERT VALUES has no facts and its
/// Values motion is not materialized here, so it is left out.
const DML: [&str; 30] = [
    "DELETE FROM t5 WHERE a = $1",
    "DELETE FROM t5 WHERE a = 1 AND b = 2",
    "DELETE FROM t5 WHERE (a = 1 AND b = 2) OR (a = 1 AND b = 3)",
    "DELETE FROM t5 WHERE a IN (SELECT a FROM t5 WHERE a = 1)",
    "DELETE FROM t5 WHERE b IN (SELECT b FROM t3_2 WHERE a = $1) AND a = $1",
    "DELETE FROM t5 WHERE a = (SELECT count(*) FROM t5 WHERE a = 1)",
    "DELETE FROM t5 WHERE a IN (SELECT a FROM t5 WHERE a = 1 LIMIT 1)",
    "DELETE FROM t5 WHERE a = 1 AND a IN (SELECT a FROM t3_2 WHERE a = 1)",
    "DELETE FROM t WHERE a = 1 AND b = 2",
    "DELETE FROM test_space WHERE bucket_id = $1",
    "DELETE FROM t5",
    "DELETE FROM global_t WHERE a = 1",
    "UPDATE t5 SET b = 2 WHERE a = $1",
    "UPDATE t5 SET b = (SELECT b FROM t3_2 WHERE a = $1) WHERE a = $1",
    "UPDATE t5 SET b = (SELECT count(*) FROM t5 WHERE a = 1) WHERE a = 1",
    "UPDATE t5 SET b = (SELECT b FROM t5 WHERE a = 2 LIMIT 1) WHERE a = 1",
    "UPDATE t5 SET b = 3 WHERE a = 1 AND b IN (SELECT b FROM t5 WHERE a = 1)",
    "UPDATE t SET a = 5 WHERE a = 1 AND b = 2",
    "UPDATE t SET a = $1 WHERE a = $2 AND b = $3",
    "UPDATE t SET a = 5 WHERE a = 1 AND b = 2 AND c IN (SELECT b FROM t5 WHERE a = 1)",
    "UPDATE global_t SET b = 1 WHERE a IN (SELECT a FROM t5 WHERE a = 1)",
    "INSERT INTO t5 SELECT * FROM t5 WHERE a = $1",
    "INSERT INTO t5 (a, b) SELECT count(*), 1 FROM t5 WHERE a = 1",
    "INSERT INTO t5 (a, b) SELECT a, b FROM t5 WHERE a = 1 ORDER BY b LIMIT 1",
    "INSERT INTO t5 (a, b) SELECT l.a, r.b FROM t5 l JOIN t5 r ON l.a = r.b WHERE l.a = 1",
    "INSERT INTO t5 (a, b) SELECT a, b FROM t5 WHERE a = 1 UNION ALL SELECT a, b FROM t5 WHERE a = 1",
    "INSERT INTO t5 (a, b) SELECT a, count(*) FROM t5 WHERE a = 1 GROUP BY a",
    "INSERT INTO t5 (a, b) SELECT a, b FROM t5 WHERE a = 1 UNION SELECT a, b FROM t5 WHERE a = 2",
    "INSERT INTO t5 (a, b) SELECT l.a, r.b FROM t5 l JOIN t3_2 r ON l.a = r.a WHERE l.a = $1",
    "INSERT INTO global_t SELECT a, b FROM t5 WHERE a = $1",
];

#[test]
fn facts_proof_keeps_its_promise_on_dml() {
    let coordinator = RouterRuntimeMock::new();
    let mut checked = 0;
    let mut failures = Vec::new();
    for sql in DML {
        let n = param_count(sql);
        let types = vec![DerivedType::new(UnrestrictedType::Integer); n];
        let plan = {
            let metadata = coordinator.metadata().lock();
            let mut plan = transform_into_plan(sql, &types, &*metadata).unwrap();
            plan.optimize().unwrap();
            plan
        };
        for params in bindings(n) {
            for spread in [false, true] {
                let mut bound = plan.clone();
                bound
                    .bind_statement(params.clone(), Options::default())
                    .unwrap();
                let at = format!("{sql} {params:?} spread={spread}");
                match route_binding(&coordinator, bound, spread, &mut checked) {
                    Ok(fields) if fields.contains("VIOLATION") => {
                        failures.push(format!("{at}: {fields}"));
                    }
                    Ok(_) => {}
                    Err(e) => failures.push(format!("{at}: {e}")),
                }
            }
        }
    }
    assert!(checked > 0, "no single-node decision was checked");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The read stage of DELETE / UPDATE / INSERT ... SELECT gets the witness of
/// its filter, and a sharded UPDATE pins the old key, not the `SET` value.
#[test]
fn dml_read_gets_the_witness_of_the_old_key() {
    use crate::ir::bucket::facts_route::{FactsRoute, FactsRoutePart};
    let coordinator = RouterRuntimeMock::new();
    let plan_of = |sql: &str, n: usize| {
        let types = vec![DerivedType::new(UnrestrictedType::Integer); n];
        let metadata = coordinator.metadata().lock();
        let mut plan = transform_into_plan(sql, &types, &*metadata).unwrap();
        plan.optimize_statement().unwrap();
        plan
    };
    let one = FactsRoute::Key(vec![FactsRoutePart::Value(Value::from(1))]);
    for sql in [
        "DELETE FROM t5 WHERE a = 1",
        "UPDATE t5 SET b = 2 WHERE a = 1",
        "INSERT INTO t5 SELECT * FROM t5 WHERE a = 1",
    ] {
        let plan = plan_of(sql, 0);
        let routes: Vec<_> = plan.facts_proof.routes.values().collect();
        assert_eq!(routes, [&vec![one.clone()]], "{sql}");
    }

    // `SET a = $1` with $1 = 5 must not move the read to the bucket of (5, 2).
    let sql = "UPDATE t SET a = $1 WHERE a = $2 AND b = $3";
    let mut plan = plan_of(sql, 3);
    plan.bind_statement(
        vec![Value::from(5), Value::from(1), Value::from(2)],
        Options::default(),
    )
    .unwrap();
    let selection = *plan.facts_proof.routes.keys().next().unwrap();
    let read = |plan: Plan| {
        let mut query = ExecutingQuery::from_plan(&coordinator, plan).unwrap();
        query.bucket_discovery(selection).unwrap()
    };
    let with_witness = read(plan.clone());
    Rc::make_mut(&mut plan.facts_proof).routes.clear();
    let walk_only = read(plan);
    assert!(with_witness.is_single_node(), "{with_witness}");
    assert_eq!(with_witness, walk_only, "{sql}");
}

/// The proven nodes of a two-stage DML live in its read stage: a lost
/// witness fails that stage, which runs before the stage that writes.
#[test]
fn lost_witness_of_a_sharded_update_fails_before_the_write() {
    use crate::ir::node::relational::MutRelational;
    use crate::ir::node::Selection;

    let sql = "UPDATE t SET a = 5 WHERE a = 1 AND b = 2";
    let coordinator = RouterRuntimeMock::new();
    let mut plan = {
        let metadata = coordinator.metadata().lock();
        let mut plan = transform_into_plan(sql, &[], &*metadata).unwrap();
        plan.optimize_statement().unwrap();
        plan
    };
    plan.bind_statement(vec![], Options::default()).unwrap();
    let selection = *plan.facts_proof.routes.keys().next().unwrap();
    // `WHERE true`: the tree walk reads every bucket, and nothing narrows it.
    let always = plan.add_const(Value::Boolean(true));
    let MutRelational::Selection(Selection { filter, .. }) =
        plan.get_mut_relation_node(selection).unwrap()
    else {
        unreachable!()
    };
    *filter = always;
    Rc::make_mut(&mut plan.facts_proof).routes.clear();

    let top = plan.get_top().unwrap();
    let mut query = ExecutingQuery::from_plan(&coordinator, plan).unwrap();
    let slices = query.get_exec_plan().get_ir_plan().slices().clone();
    let [read] = slices.slices() else {
        panic!("one read stage below the write, got {slices:?}")
    };
    let [motion] = read.positions() else {
        panic!("one motion in the read stage")
    };
    let plan = query.get_exec_plan().get_ir_plan();
    let read_top = plan.get_motion_subtree_root(*motion).unwrap();
    assert_ne!(read_top, top);
    let error = query.bucket_discovery(read_top).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("proven single-node by the planner"),
        "{error}"
    );
}

/// `$65535` is a valid parameter: it must not stand for `CURRENT_USER` in the
/// facts, or `a` would be pinned to the user name.
#[test]
fn the_last_parameter_is_not_current_user() {
    use crate::executor::engine::Vshard;
    let sql = "SELECT a FROM t1 WHERE current_user = 'admin' AND a = $65535 AND b = 1";
    let n = 65535;
    let coordinator = RouterRuntimeMock::new();
    let mut plan = {
        let types = vec![DerivedType::new(UnrestrictedType::String); n];
        let metadata = coordinator.metadata().lock();
        let mut plan = transform_into_plan(sql, &types, &*metadata).unwrap();
        plan.optimize_statement().unwrap();
        plan
    };
    plan.bind_statement(vec![Value::from("p"); n], Options::default())
        .unwrap();
    let top = plan.get_top().unwrap();
    let mut query = ExecutingQuery::from_plan(&coordinator, plan).unwrap();
    let bucket = coordinator
        .determine_bucket_id(&[&Value::from("p"), &Value::from(1)])
        .unwrap();
    assert_eq!(
        Buckets::new_filtered([bucket].into_iter().collect()),
        query.bucket_discovery(top).unwrap()
    );
}

/// EXCEPT of a global input copies that planned input under its INTERSECT.
/// The copy keeps the facts proof: its one-stage scalar aggregate reads one
/// bucket and runs once, as the original does, not on every replicaset.
#[test]
fn except_copy_of_a_global_input_keeps_the_facts_proof() {
    let sql = "SELECT n FROM (SELECT count(*) AS n FROM (\
               SELECT count(*) AS v FROM t5 WHERE a = b AND b = 1 \
               UNION ALL SELECT id AS v FROM test_space) AS u \
               UNION SELECT a AS n FROM global_t) AS l \
               EXCEPT SELECT id AS n FROM test_space";
    let coordinator = RouterRuntimeMock::new();
    let mut plan = {
        let metadata = coordinator.metadata().lock();
        let mut plan = transform_into_plan(sql, &[], &*metadata).unwrap();
        plan.optimize().unwrap();
        plan
    };
    plan.bind_statement(vec![], Options::default()).unwrap();
    let mut checked = 0;
    let stages = route_binding(&coordinator, plan, true, &mut checked).unwrap();
    let proven: Vec<&str> = stages
        .split(" | ")
        .filter(|stage| stage.contains(":decisions=1"))
        .collect();
    assert_eq!(proven.len(), 2, "{stages}");
    assert!(
        proven.iter().all(|stage| stage.contains(":runs=1")),
        "{stages}"
    );
}
