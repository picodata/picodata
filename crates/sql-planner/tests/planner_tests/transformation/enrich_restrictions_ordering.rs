use std::process::Command;

use sql::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use sql::executor::engine::helpers::table_name;
use sql::executor::ir::ExecutionPlan;
use sql::explain::ir::LogicalExplain;
use sql::ir::options::Options;
use sql::ir::tree::Snapshot;
use sql::ir::types::UnrestrictedType;
use sql::ir::value::Value;
use sql_ast_new_corpus::MockCatalog;
use sql_frontend::frontend::sql::transform_into_plan;

// AHash seeds vary between processes. Rebuilding in one test thread can reuse
// the same seed and miss the unstable order inherited from equality classes.
#[test]
fn predicates_and_plan_ids_are_stable_across_processes() {
    let mut expected = None;
    for _ in 0..16 {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "transformation::enrich_restrictions_ordering::ordering_probe",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let stdout = String::from_utf8(output.stdout).unwrap();
        let results: Vec<_> = stdout
            .lines()
            .filter_map(|line| line.strip_prefix("ORDERING_RESULT "))
            .map(str::to_owned)
            .collect();
        assert_eq!(results.len(), 5, "{stdout}");
        if let Some(expected) = &expected {
            pretty_assertions::assert_eq!(expected, &results);
        } else {
            expected = Some(results);
        }
    }
}

#[test]
#[ignore = "subprocess helper for predicates_and_plan_ids_are_stable_across_processes"]
fn ordering_probe() {
    let mut catalog = MockCatalog::new();
    catalog.add_global(
        "t",
        &[
            ("a", UnrestrictedType::Integer, false),
            ("b", UnrestrictedType::Integer, false),
        ],
        &["a"],
    );
    let cases = [
        ("SELECT a FROM t WHERE a = 1 AND b = 2", vec![]),
        (
            "SELECT l.a FROM t l JOIN t r ON l.a = r.a \
             WHERE l.a = 1 AND l.b = 2 AND r.b = 3",
            vec![],
        ),
        (
            "SELECT a FROM t WHERE a = $1 AND a = 1 AND b = $2 AND b = 2",
            vec![Value::Integer(1), Value::Integer(2)],
        ),
        (
            "SELECT a FROM (SELECT a FROM t WHERE a = $1 AND a = 1 \
             UNION ALL SELECT a FROM t WHERE a = $1 AND a = 2) s",
            vec![Value::Integer(1)],
        ),
        (
            "SELECT a FROM t WHERE a = $1 AND b = $2",
            vec![Value::Integer(1), Value::Integer(2)],
        ),
    ];
    for (query, values) in cases {
        let types: Vec<_> = values.iter().map(Value::get_type).collect();
        let plan = transform_into_plan(query, &types, &catalog)
            .unwrap()
            .optimize()
            .unwrap();
        let top = plan.get_top().unwrap();
        let explain = LogicalExplain::new(&plan, top).unwrap().to_string();
        let plan = plan.bind_statement(values, Options::default()).unwrap();
        let ex_plan = ExecutionPlan::new(plan);
        let top = ex_plan.get_ir_plan().get_top().unwrap();
        let plan_id = ex_plan.calculate_plan_id(top).unwrap();
        let mut sql = Vec::new();
        for snapshot in [Snapshot::Latest, Snapshot::Oldest] {
            let params = ex_plan.local_sql_params(top, snapshot).unwrap();
            let syntax = SyntaxPlan::new(&ex_plan, top, snapshot).unwrap();
            let ordered = OrderedSyntaxNodes::try_from(syntax).unwrap();
            let nodes = ordered.to_syntax_data().unwrap();
            sql.push(
                ex_plan
                    .generate_sql(&nodes, 0, table_name, Some(params.constant_ids().to_vec()))
                    .unwrap(),
            );
        }
        println!("\nORDERING_RESULT {:?}", (explain, sql, plan_id));
    }
}
