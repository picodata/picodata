//! SQL generation from a prepared execution plan.
//!
//! `ir_to_sql` measures SyntaxPlan construction, syntax-node ordering and SQL
//! rendering. `render_sql` measures only generate_sql on precomputed nodes.
//! Both render every local subtree of the query, including the final stage.
//! Parsing, optimization, parameter collection and mock motion materialization
//! happen outside the timed region. The plan is reused without cloning; returned
//! SQL strings are dropped outside the timed region.
//!
//! Run: cargo bench -p sql-planner --bench generate_sql
//! Compare identical sources on both revisions with --save-baseline/--baseline.
//! SQL_BENCH_DUMP_DIR optionally records the generated SQL for comparison.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use sql_ast_new_corpus::{corpus_catalog, corpus_queries};
use sql_executor::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use sql_executor::executor::engine::helpers::table_name;
use sql_executor::executor::engine::mock::RouterConfigurationMock;
use sql_executor::executor::ir::ExecutionPlan;
use sql_executor::executor::vtable::{VTableColumn, VirtualTable};
use sql_frontend::frontend::sql::transform_into_plan;
use sql_ir::errors::SbroadError;
use sql_ir::ir::metadata::Metadata;
use sql_ir::ir::node::relational::Relational;
use sql_ir::ir::node::NodeId;
use sql_ir::ir::relation::ColumnRole;
use sql_ir::ir::tree::Snapshot;

struct Prepared {
    plan: ExecutionPlan,
    roots: Vec<NodeId>,
    constants: Vec<Vec<NodeId>>,
}

fn prepare(sql: &str, metadata: &impl Metadata) -> Result<Prepared, SbroadError> {
    let ir = transform_into_plan(sql, &[], metadata)?.optimize()?;
    let top = ir.get_top()?;
    let motions: Vec<_> = ir
        .slices()
        .slices()
        .iter()
        .flat_map(|slice| slice.positions().iter().copied())
        .collect();
    let mut roots = Vec::new();
    let mut vtables = std::collections::HashMap::new();
    for motion_id in motions {
        let Relational::Motion(motion) = ir.get_relation_node(motion_id)? else {
            unreachable!("slices contain motions");
        };
        roots.push(ir.get_motion_subtree_root(motion_id)?);
        let mut vtable = VirtualTable::new();
        // Motion outputs are explicit on both sides of the relational-output
        // refactor, so the exact same benchmark can compare both revisions.
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
    let mut plan = ExecutionPlan::new(ir);
    plan.set_vtables(vtables);
    let constants = roots
        .iter()
        .map(|&root| {
            plan.local_sql_params(root, Snapshot::Oldest)
                .map(|params| params.constant_ids().to_vec())
        })
        .collect::<Result<_, _>>()?;
    Ok(Prepared {
        plan,
        roots,
        constants,
    })
}

fn render_all(prepared: &Prepared) -> Result<Vec<String>, SbroadError> {
    prepared
        .roots
        .iter()
        .zip(&prepared.constants)
        .map(|(&root, constants)| {
            let syntax = SyntaxPlan::new(&prepared.plan, root, Snapshot::Oldest)?;
            let ordered = OrderedSyntaxNodes::try_from(syntax)?;
            let nodes = ordered.to_syntax_data()?;
            prepared
                .plan
                .generate_sql(&nodes, 0, table_name, Some(constants))
        })
        .collect()
}

fn run(c: &mut Criterion, suite: &str, cases: Vec<(String, String)>, metadata: &impl Metadata) {
    for (name, sql) in cases {
        let checked = catch_unwind(AssertUnwindSafe(|| {
            let prepared = prepare(&sql, metadata)?;
            let expected = render_all(&prepared)?;
            Ok::<_, SbroadError>((prepared, expected))
        }));
        let (prepared, expected) = match checked {
            Ok(Ok(value)) => value,
            other => {
                match other {
                    Ok(Err(err)) => eprintln!("SKIP {suite}/{name}: {err}"),
                    Err(_) => eprintln!("SKIP {suite}/{name}: preparation panicked"),
                    _ => unreachable!(),
                }
                continue;
            }
        };
        if let Some(dir) = std::env::var_os("SQL_BENCH_DUMP_DIR") {
            let dir = std::path::PathBuf::from(dir).join(suite);
            std::fs::create_dir_all(&dir).unwrap();
            for (index, sql) in expected.iter().enumerate() {
                std::fs::write(dir.join(format!("{name}-{index}.sql")), sql).unwrap();
            }
        }

        c.benchmark_group(format!("ir_to_sql_{suite}"))
            .bench_function(BenchmarkId::from_parameter(&name), |b| {
                b.iter_batched(
                    || (),
                    |()| black_box(render_all(black_box(&prepared)).unwrap()),
                    BatchSize::PerIteration,
                )
            });

        let ordered: Vec<_> = prepared
            .roots
            .iter()
            .map(|&root| {
                let syntax = SyntaxPlan::new(&prepared.plan, root, Snapshot::Oldest).unwrap();
                OrderedSyntaxNodes::try_from(syntax).unwrap()
            })
            .collect();
        let nodes: Vec<_> = ordered
            .iter()
            .map(|o| o.to_syntax_data().unwrap())
            .collect();
        let render = || -> Vec<String> {
            nodes
                .iter()
                .zip(&prepared.constants)
                .map(|(nodes, constants)| {
                    prepared
                        .plan
                        .generate_sql(black_box(nodes), 0, table_name, Some(constants))
                        .unwrap()
                })
                .collect()
        };
        assert_eq!(
            expected,
            render(),
            "the two modes must produce identical SQL: {name}"
        );
        c.benchmark_group(format!("render_sql_{suite}"))
            .bench_function(BenchmarkId::from_parameter(&name), |b| {
                b.iter_batched(|| (), |()| black_box(render()), BatchSize::PerIteration)
            });
    }
}

fn targeted_cases() -> Vec<(String, String)> {
    let mut cases = vec![
        ("simple".into(), "SELECT a FROM t WHERE a = 1".into()),
        (
            "motion_groupby".into(),
            "SELECT b, count(*) FROM t GROUP BY b".into(),
        ),
        (
            "motion_subquery".into(),
            "SELECT a FROM t WHERE a IN (SELECT b FROM t)".into(),
        ),
        (
            "union_all".into(),
            "SELECT a FROM t UNION ALL SELECT b FROM t".into(),
        ),
    ];
    for width in [32, 256, 1024] {
        let projection = (0..width)
            .map(|i| format!("a AS c{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        cases.push((
            format!("wide_{width}"),
            format!("SELECT {projection} FROM t WHERE b > 0"),
        ));
    }
    let mut joined = "t AS t0".to_owned();
    for i in 1..8 {
        joined.push_str(&format!(" JOIN t AS t{i} ON t{}.a = t{i}.a", i - 1));
    }
    let projection = (0..256)
        .map(|i| format!("t{}.b AS c{i}", i % 8))
        .collect::<Vec<_>>()
        .join(", ");
    cases.push((
        "join_8_wide_256".into(),
        format!("SELECT {projection} FROM {joined}"),
    ));
    let mut nested = "SELECT a, b FROM t WHERE b > 0".to_owned();
    for i in 0..32 {
        nested = format!("SELECT a, b FROM ({nested}) AS s{i} WHERE a > {i}");
    }
    cases.push(("nested_32".into(), nested));
    let ctes = (0..32)
        .map(|i| {
            if i == 0 {
                "s0 AS (SELECT a, b FROM t)".into()
            } else {
                format!("s{i} AS (SELECT a, b FROM s{} WHERE b > {i})", i - 1)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    cases.push(("cte_32".into(), format!("WITH {ctes} SELECT a, b FROM s31")));
    cases
}

fn benches(c: &mut Criterion) {
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

criterion_group!(sql_generation, benches);
criterion_main!(sql_generation);
