//! Shared benchmark/profiling cases for `ast_new`.
//!
//! Pure `String`-building, no criterion dependency: the same file is compiled
//! into the criterion bench (`ast_fill.rs`) and, via a `#[path]` include, into
//! the dhat example (`examples/ast_fill_alloc.rs`), so timings and allocation
//! counts are always measured over identical inputs.
//!
//! Two case sets, deliberately opposite in character:
//!   * [`synthetic_benchmark_cases`] — generated, pathologically large ASTs
//!     (a 14000-column projection, an IN list of 100000 values, deep subquery
//!     nesting, ...). Each case stresses one node kind at a scale real queries
//!     never reach, so a per-node cost regression is amplified past noise and
//!     named by the case that catches it.
//!   * [`corpus_benchmark_cases`] — the anonymized real-world DQL statements
//!     from `sql-ast-new-corpus`, giving the realistic mix the synthetic set
//!     abstracts away.
use sql_ast_new_corpus::{corpus_queries, CorpusQuery};
use sql_executor::executor::engine::mock::{
    VEHICLE_ACTUAL_TABLE as VEHICLE_TABLE, VEHICLE_COLUMNS, VEHICLE_HISTORY_TABLE,
};

/// A named SQL statement to feed through an AST-fill.
pub struct BenchmarkCase {
    pub name: &'static str,
    pub sql: String,
}

impl BenchmarkCase {
    pub fn new(name: &'static str, sql: impl Into<String>) -> Self {
        Self {
            name,
            sql: sql.into(),
        }
    }
}

impl From<CorpusQuery> for BenchmarkCase {
    fn from(corpus_q: CorpusQuery) -> Self {
        let (name, sql) = corpus_q.into_parts();
        Self::new(name, sql)
    }
}

fn quoted(name: &str) -> String {
    format!("\"{name}\"")
}

fn vehicle_column(idx: usize) -> &'static str {
    VEHICLE_COLUMNS[idx % VEHICLE_COLUMNS.len()]
}

fn qualified(alias: &str, column: &str) -> String {
    format!("{alias}.{}", quoted(column))
}

fn complex_projection_expr(idx: usize, alias: &str) -> String {
    let col1 = qualified(alias, vehicle_column(idx));
    let col2 = qualified(alias, vehicle_column(idx + 1));
    let col3 = qualified(alias, vehicle_column(idx + 2));
    let output = quoted(&format!("expr_{idx}"));

    match idx % 6 {
        0 => format!("{col1} AS {output}"),
        1 => format!("({col1} + {col2} - {}) AS {output}", idx + 1),
        2 => {
            format!(
                "CASE WHEN {col1} = {} THEN {col2} ELSE {col3} END AS {output}",
                idx + 1
            )
        }
        3 => format!("COALESCE({col1}, {col2}, {}) AS {output}", idx + 1),
        4 => format!("CAST(({col1} + {col2}) AS integer) AS {output}"),
        _ => format!("ABS({col1} - {col2}) AS {output}"),
    }
}

fn long_arithmetic_expr(expression_count: usize) -> String {
    let exprs = (0..expression_count)
        .map(|_| "0")
        .collect::<Vec<_>>()
        .join("+");

    format!("SELECT {exprs}")
}

fn wide_projection(expression_count: usize) -> String {
    let projection = (0..expression_count)
        .map(|idx| complex_projection_expr(idx, "v"))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "SELECT {projection} FROM {table} AS v WHERE v.{reestrid} > 0",
        table = quoted(VEHICLE_TABLE),
        reestrid = quoted("reestrid"),
    )
}

fn predicate_condition(idx: usize) -> String {
    let col = qualified("v", vehicle_column(idx));

    match idx % 4 {
        0 => format!("{col} = {}", idx + 1),
        1 => format!("{col} > {}", idx + 1),
        2 => format!("{col} <= {}", idx + 1),
        _ => format!("{col} <> {}", idx + 1),
    }
}

fn balanced_predicate_expr(start: usize, count: usize) -> String {
    if count == 1 {
        return predicate_condition(start);
    }

    let left_count = count / 2;
    let right_count = count - left_count;
    let op = if start % 2 == 0 { "OR" } else { "AND" };
    let left = balanced_predicate_expr(start, left_count);
    let right = balanced_predicate_expr(start + left_count, right_count);

    format!("({left} {op} {right})")
}

fn balanced_predicate(predicate_count: usize) -> String {
    let predicate = balanced_predicate_expr(0, predicate_count);

    format!(
        "SELECT v.{reestrid} FROM {table} AS v WHERE {predicate}",
        reestrid = quoted("reestrid"),
        table = quoted(VEHICLE_TABLE),
    )
}

fn wide_vehicle_subquery(alias: &str, table: &str, projection_width: usize) -> String {
    let mut columns = VEHICLE_COLUMNS
        .iter()
        .map(|column| quoted(column))
        .collect::<Vec<_>>();

    for idx in VEHICLE_COLUMNS.len()..projection_width {
        columns.push(format!(
            "{} AS {}",
            quoted(vehicle_column(idx)),
            quoted(&format!("{alias}_extra_{idx}")),
        ));
    }

    let columns = columns.join(", ");

    format!(
        "(SELECT {columns} FROM {table} WHERE {reestrid} >= 0) AS {alias}",
        table = quoted(table),
        reestrid = quoted("reestrid"),
    )
}

fn join_chain(join_count: usize, projection_width: usize) -> String {
    let mut sql = format!(
        "SELECT v0.{reestrid} FROM {}",
        wide_vehicle_subquery("v0", VEHICLE_TABLE, projection_width),
        reestrid = quoted("reestrid"),
    );

    for idx in 1..=join_count {
        let table = if idx % 2 == 0 {
            VEHICLE_TABLE
        } else {
            VEHICLE_HISTORY_TABLE
        };
        let prev = idx - 1;
        sql.push_str(&format!(
            " INNER JOIN {} ON v{prev}.{reestrid} = v{idx}.{reestrid} \
             AND v{idx}.{vehicleguid} >= {} \
             AND v{idx}.{sys_from} <= v{prev}.{sys_to}",
            wide_vehicle_subquery(&format!("v{idx}"), table, projection_width),
            idx + 1,
            reestrid = quoted("reestrid"),
            vehicleguid = quoted("vehicleguid"),
            sys_from = quoted("sys_from"),
            sys_to = quoted("sys_to"),
        ));
    }

    sql
}

fn union_all(branch_count: usize, projection_width: usize) -> String {
    let columns = VEHICLE_COLUMNS
        .iter()
        .take(projection_width)
        .map(|column| quoted(column))
        .collect::<Vec<_>>()
        .join(", ");

    let branches = (0..branch_count)
        .map(|idx| {
            format!(
                "SELECT {columns} FROM {table} WHERE {reestrid} = {value} \
                 OR {vehicleguid} = {value}",
                table = if idx % 2 == 0 {
                    quoted(VEHICLE_TABLE)
                } else {
                    quoted(VEHICLE_HISTORY_TABLE)
                },
                reestrid = quoted("reestrid"),
                vehicleguid = quoted("vehicleguid"),
                value = idx + 1,
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");

    format!(
        "SELECT * FROM ({branches}) AS u WHERE u.{reestrid} > 0",
        reestrid = quoted("reestrid"),
    )
}

fn nested_layer_predicate(layer: usize, predicate_count: usize) -> String {
    (0..predicate_count)
        .map(|idx| {
            let column = if idx % 2 == 0 {
                quoted("reestrid")
            } else {
                quoted("vehicleguid")
            };
            let value = layer * predicate_count + idx + 1;
            let condition = match idx % 4 {
                0 => format!("{column} = {value}"),
                1 => format!("{column} > {value}"),
                2 => format!("{column} <= {value}"),
                _ => format!("{column} <> {value}"),
            };

            if idx == 0 {
                condition
            } else if idx % 2 == 0 {
                format!("OR ({condition})")
            } else {
                format!("AND ({condition})")
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn nested_subquery(depth: usize, predicates_per_level: usize) -> String {
    let mut inner = format!(
        "SELECT {reestrid}, {vehicleguid} FROM {table}",
        reestrid = quoted("reestrid"),
        vehicleguid = quoted("vehicleguid"),
        table = quoted(VEHICLE_TABLE),
    );

    for idx in 0..depth {
        let predicate = nested_layer_predicate(idx, predicates_per_level);
        inner = format!(
            "SELECT {reestrid} AS {reestrid}, \
                    {vehicleguid} AS {vehicleguid} \
             FROM ({inner}) AS sq{idx} \
             WHERE {reestrid} IN \
                 (SELECT {reestrid} FROM {history} WHERE {vehicleguid} >= {value}) \
               AND EXISTS \
                 (SELECT 1 FROM {actual} AS e{idx} \
                  WHERE e{idx}.{reestrid} = {reestrid}) \
               AND ({predicate})",
            reestrid = quoted("reestrid"),
            vehicleguid = quoted("vehicleguid"),
            history = quoted(VEHICLE_HISTORY_TABLE),
            actual = quoted(VEHICLE_TABLE),
            value = idx + 1,
        );
    }

    inner
}

fn case_expr(branch_count: usize) -> String {
    let branches = (0..branch_count)
        .map(|idx| {
            format!(
                "WHEN v.{reestrid} = {value} THEN v.{column}",
                reestrid = quoted("reestrid"),
                value = idx + 1,
                column = quoted(vehicle_column(idx)),
            )
        })
        .collect::<Vec<_>>()
        .join(" ");

    format!(
        "SELECT CASE {branches} ELSE v.{fallback} END AS {case_value} FROM {table} AS v",
        fallback = quoted("reestrid"),
        case_value = quoted("case_value"),
        table = quoted(VEHICLE_TABLE),
    )
}

fn in_list(value_count: usize) -> String {
    let values = (1..=value_count)
        .map(|idx| idx.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "SELECT v.{reestrid} FROM {table} AS v WHERE v.{reestrid} IN ({values})",
        reestrid = quoted("reestrid"),
        table = quoted(VEHICLE_TABLE),
    )
}

fn aggregate_having(aggregate_count: usize, having_count: usize) -> String {
    let aggregates = (0..aggregate_count)
        .map(|idx| {
            let col = qualified("v", vehicle_column(idx));
            let output = quoted(&format!("agg_{idx}"));

            match idx % 4 {
                0 => format!("SUM({col}) AS {output}"),
                1 => format!("COUNT({col}) AS {output}"),
                2 => format!("MAX({col}) AS {output}"),
                _ => format!("MIN({col}) AS {output}"),
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    let having = (0..having_count)
        .map(|idx| {
            let col = qualified("v", vehicle_column(idx));
            let condition = match idx % 4 {
                0 => format!("SUM({col}) > {}", idx + 1),
                1 => format!("COUNT({col}) >= {}", idx + 1),
                2 => format!("MAX({col}) <> {}", idx + 1),
                _ => format!("MIN({col}) <= {}", idx + 1),
            };

            if idx == 0 {
                condition
            } else if idx % 2 == 0 {
                format!("OR ({condition})")
            } else {
                format!("AND ({condition})")
            }
        })
        .collect::<Vec<_>>()
        .join(" ");

    format!(
        "SELECT v.{reestrid}, {aggregates} FROM {table} AS v \
         GROUP BY v.{reestrid} HAVING {having} ORDER BY {first_agg} DESC",
        reestrid = quoted("reestrid"),
        table = quoted(VEHICLE_TABLE),
        first_agg = quoted("agg_0"),
    )
}

fn cte_chain(cte_count: usize) -> String {
    let ctes = (0..cte_count)
        .map(|idx| {
            let source = if idx == 0 {
                quoted(VEHICLE_TABLE)
            } else {
                format!("cte{}", idx - 1)
            };

            format!(
                "cte{idx}({reestrid}, {vehicleguid}) AS (\
                 SELECT {reestrid}, {vehicleguid} FROM {source} \
                 WHERE {reestrid} >= {value})",
                reestrid = quoted("reestrid"),
                vehicleguid = quoted("vehicleguid"),
                value = idx + 1,
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "WITH {ctes} SELECT c.{reestrid}, c.{vehicleguid} FROM cte{last} AS c \
         WHERE c.{reestrid} IN (SELECT {reestrid} FROM {history})",
        reestrid = quoted("reestrid"),
        vehicleguid = quoted("vehicleguid"),
        last = cte_count - 1,
        history = quoted(VEHICLE_HISTORY_TABLE),
    )
}

/// Generated, pathologically large ASTs that stress AST-fill throughput.
pub fn synthetic_benchmark_cases() -> Vec<BenchmarkCase> {
    vec![
        BenchmarkCase::new(
            "long_arithmetic_expr_chain_1000_operands",
            long_arithmetic_expr(1000),
        ),
        BenchmarkCase::new("wide_projection_14000_exprs", wide_projection(14000)),
        BenchmarkCase::new(
            "balanced_boolean_predicate_30000_terms",
            balanced_predicate(30000),
        ),
        BenchmarkCase::new(
            "balanced_boolean_predicate_34000_terms",
            balanced_predicate(34000),
        ),
        BenchmarkCase::new("join_chain_8_wide_subqueries", join_chain(8, 7000)),
        BenchmarkCase::new("union_all_1000_branches_64_cols", union_all(1000, 64)),
        BenchmarkCase::new(
            "nested_subquery_120_levels_280_terms",
            nested_subquery(120, 280),
        ),
        BenchmarkCase::new("case_expr_20000_branches", case_expr(20000)),
        BenchmarkCase::new("in_list_100000_values", in_list(100000)),
        BenchmarkCase::new("aggregate_having_13500_terms", aggregate_having(9000, 4500)),
        BenchmarkCase::new("cte_chain_8000_steps", cte_chain(8000)),
    ]
}

pub fn corpus_benchmark_cases() -> Vec<BenchmarkCase> {
    corpus_queries()
        .into_iter()
        .map(BenchmarkCase::from)
        .collect()
}
