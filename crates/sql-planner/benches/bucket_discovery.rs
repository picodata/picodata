//! Bucket discovery of a prepared execution plan.
//!
//! Parsing, optimization and the mock motion materialization happen outside
//! the timed region. An iteration runs bucket discovery for the top and for
//! each motion that the router materializes, as the dispatch of a statement
//! does. The virtual tables of the motions are empty. The mock router
//! calculates bucket ids with its own hash, so the cost of the tarantool key
//! hash is not measured.
//!
//! Run: cargo bench -p sql-planner --bench bucket_discovery
//! Compare identical sources on both revisions with --save-baseline/--baseline.
//!
//! An iteration of a tpcb case takes 0.2–1 µs, so CPU frequency and machine
//! load change the time by more than a small change of the code does. To
//! compare two revisions:
//!
//! - Filter to the group that the change is for, for example
//!   `-- bucket_discovery_tpcb`. Run the other groups once at the end to find
//!   regressions.
//! - Build each revision from a commit hash, not from `HEAD`, and run the
//!   revisions in turn: A, B, A, B. Use the lower time of each revision. A
//!   difference smaller than the difference between the two runs of one
//!   revision is noise.
//! - Do not change the power mode between the runs.
//! - Count instructions to see a change of the work, independent of the
//!   frequency. `perf stat -e instructions:u,cycles:u` counts the whole
//!   process, also parsing and the statistics of criterion. Run one case
//!   twice with two measurement times, for example 2 s and 6 s, and divide
//!   the difference of the counters by the difference of the iterations. The
//!   iterations are the sum of `iters` in
//!   `target/criterion/<group>/<case>/new/sample.json`.
//! - Instructions repeat within about 0.1%, cycles do not. Cycles divided by
//!   the time per iteration give the frequency, and instructions divided by
//!   cycles give the IPC. If the time changes and the instructions and the
//!   IPC do not, the cause is the frequency, not the code.
//! - To find where the time goes, record `perf record -e instructions:u
//!   --call-graph dwarf` of the bench binary with `--profile-time`. Samples of
//!   instructions are more stable than samples of cycles.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use sql_ast_new_corpus::{corpus_catalog, corpus_queries};
use sql_executor::executor::engine::mock::{RouterConfigurationMock, RouterRuntimeMock};
use sql_executor::executor::vtable::{VTableColumn, VirtualTable, VirtualTableMap};
use sql_executor::executor::ExecutingQuery;
use sql_frontend::frontend::sql::transform_into_plan;
use sql_ir::errors::SbroadError;
use sql_ir::ir::bucket::Buckets;
use sql_ir::ir::metadata::Metadata;
use sql_ir::ir::node::relational::Relational;
use sql_ir::ir::node::NodeId;
use sql_ir::ir::relation::ColumnRole;
use sql_ir::ir::transformation::redistribution::MotionPolicy;

fn prepare<'r>(
    sql: &str,
    metadata: &impl Metadata,
    runtime: &'r RouterRuntimeMock,
) -> Result<(ExecutingQuery<'r, RouterRuntimeMock>, Vec<NodeId>), SbroadError> {
    let mut ir = transform_into_plan(sql, &[], metadata)?;
    ir.optimize()?;
    let top = ir.get_top()?;
    let motions: Vec<_> = ir
        .slices()
        .slices()
        .iter()
        .flat_map(|slice| slice.positions().iter().copied())
        .collect();
    let mut roots = Vec::new();
    let mut vtables = VirtualTableMap::default();
    for motion_id in motions {
        let Relational::Motion(motion) = ir.get_relation_node(motion_id)? else {
            unreachable!("slices contain motions");
        };
        // Dispatch discovers the buckets of the motions that the router
        // materializes: local motions run with their parent, and VALUES
        // under a segment motion are materialized without discovery.
        let child = ir.get_relation_node(ir.get_motion_child(motion_id)?)?;
        match motion.policy {
            MotionPolicy::Local | MotionPolicy::LocalSegment(_) => {}
            MotionPolicy::Segment(_) if matches!(child, Relational::Values(_)) => {}
            _ => roots.push(ir.get_motion_subtree_root(motion_id)?),
        }
        let mut vtable = VirtualTable::new();
        for &alias in ir.get_row_list(motion.output)? {
            vtable.add_column(VTableColumn {
                r#type: ir.get_expression_node(alias)?.calculate_type(&ir)?,
                role: ColumnRole::User,
                is_nullable: true,
            });
        }
        vtables.insert(motion_id, Rc::new(vtable));
    }
    if !roots.contains(&top) {
        roots.push(top);
    }
    let mut query = ExecutingQuery::from_plan(runtime, ir)?;
    query.get_mut_exec_plan().set_vtables(vtables);
    Ok((query, roots))
}

fn discover_all(
    query: &mut ExecutingQuery<'_, RouterRuntimeMock>,
    roots: &[NodeId],
) -> Result<Vec<Buckets>, SbroadError> {
    roots
        .iter()
        .map(|&root| query.bucket_discovery(root))
        .collect()
}

fn run(c: &mut Criterion, suite: &str, cases: Vec<(String, String)>, metadata: &impl Metadata) {
    let runtime = RouterRuntimeMock::new();
    for (name, sql) in cases {
        let checked = catch_unwind(AssertUnwindSafe(|| {
            let (mut query, roots) = prepare(&sql, metadata, &runtime)?;
            discover_all(&mut query, &roots)?;
            Ok::<_, SbroadError>((query, roots))
        }));
        let (mut query, roots) = match checked {
            Ok(Ok(value)) => value,
            Ok(Err(err)) => {
                eprintln!("SKIP {suite}/{name}: {err}");
                continue;
            }
            Err(_) => {
                eprintln!("SKIP {suite}/{name}: preparation panicked");
                continue;
            }
        };
        c.benchmark_group(format!("bucket_discovery_{suite}"))
            .bench_function(BenchmarkId::from_parameter(&name), |b| {
                b.iter(|| black_box(discover_all(&mut query, black_box(&roots)).unwrap()))
            });
    }
}

fn cases(list: &[(&str, &str)]) -> Vec<(String, String)> {
    list.iter()
        .map(|(name, sql)| ((*name).to_owned(), (*sql).to_owned()))
        .collect()
}

/// The statements of pgbench tpcb on the tables of the mock.
fn tpcb_cases() -> Vec<(String, String)> {
    cases(&[
        (
            "key_select",
            r#"SELECT "FIRST_NAME" FROM "test_space" WHERE "id" = 1"#,
        ),
        (
            "key_update",
            r#"UPDATE "test_space" SET "FIRST_NAME" = 'a' WHERE "id" = 1"#,
        ),
        (
            "values_insert",
            r#"INSERT INTO "test_space" ("id", "FIRST_NAME") VALUES (1, 'a')"#,
        ),
        ("key_delete", r#"DELETE FROM "test_space" WHERE "id" = 1"#),
    ])
}

fn targeted_cases() -> Vec<(String, String)> {
    cases(&[
        (
            "key_or_key",
            r#"SELECT * FROM "test_space" WHERE "id" = 1 OR "id" = 2"#,
        ),
        (
            "composite_key",
            r#"SELECT * FROM "t" WHERE "a" = 1 AND "b" = 2"#,
        ),
        (
            "key_join",
            r#"SELECT * FROM "test_space" AS t1 JOIN "test_space" AS t2
                ON t1."id" = t2."id" WHERE t1."id" = 1"#,
        ),
        (
            "key_subquery",
            r#"SELECT * FROM "test_space" WHERE "id" = 1
                AND "sysFrom" IN (SELECT "a" FROM "global_t")"#,
        ),
        ("no_filter", r#"SELECT * FROM "test_space""#),
        ("global", r#"SELECT * FROM "global_t" WHERE "a" = 1"#),
    ])
}

fn benches(c: &mut Criterion) {
    run(c, "tpcb", tpcb_cases(), &RouterConfigurationMock::new());
    run(
        c,
        "targeted",
        targeted_cases(),
        &RouterConfigurationMock::new(),
    );
    run(
        c,
        "corpus",
        corpus_queries()
            .into_iter()
            .map(|q| {
                let (name, sql) = q.into_parts();
                (name.to_owned(), sql)
            })
            .collect(),
        &corpus_catalog(),
    );
}

criterion_group!(bucket_discovery, benches);
criterion_main!(bucket_discovery);
