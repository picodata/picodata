use sqlparser::ast::{Expr, Query, SetExpr, SetOperator, SetQuantifier, Value, Visit, Visitor};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

use sbroad::ir::value::Value as SbroadValue;

#[derive(Debug, Clone)]
pub enum WellKnownQuery {
    ListOfTables(String),
}

impl WellKnownQuery {
    pub fn sql(&self) -> String {
        match self {
            WellKnownQuery::ListOfTables(..) => {
                // TODO: filter _pico names like PostgreSQL filters pg_ names
                r#"
                    select
                        "name" as "relname",
                        NULL::text as "text"
                    from "_pico_table"
                    where substr("name", 1, $1::int) = $2::text
                    order by "relname"
                "#
                .into()
            }
        }
    }

    pub fn parameters(&self) -> Vec<SbroadValue> {
        match self {
            WellKnownQuery::ListOfTables(pattern) => {
                vec![
                    SbroadValue::from(pattern.len() as u64),
                    SbroadValue::from(pattern.to_string()),
                ]
            }
        }
    }
}

/// Visitor that parses a query that psql sends on tab-complete for table name.
///
/// For example, this is what psql would send for this query `select * from t<TAB>`:
/// ```sql
/// SELECT c.relname,
///        NULL::pg_catalog.text
/// FROM pg_catalog.pg_class c
/// WHERE c.relkind IN ('r',
///                     'S',
///                     'v',
///                     'm',
///                     'f',
///                     'p')
///   AND (c.relname) LIKE '%'
///   AND pg_catalog.pg_table_is_visible(c.oid)
///   AND c.relnamespace <>
///     (SELECT oid
///      FROM pg_catalog.pg_namespace
///      WHERE nspname = 'pg_catalog')
/// UNION ALL
/// SELECT NULL::pg_catalog.text,
///        n.nspname
/// FROM pg_catalog.pg_namespace n
/// WHERE n.nspname LIKE '%'
///   AND n.nspname NOT LIKE E'pg\\_%'
/// LIMIT 1000
/// ```
///
/// NOTE: It doesn't parse the whole query, it only checks that we read from pg_catalog.pg_class,
/// and compare c.relname with something using LIKE, so collisions can happen.
#[derive(Default, Debug)]
struct ListOfTablesQueryParser {
    found_select_from_pg_class: bool,
    table_name_pattern: Option<String>,
}

impl Visitor for ListOfTablesQueryParser {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        let Expr::Like { expr, pattern, .. } = expr else {
            return ControlFlow::Continue(());
        };

        // (c.relname) LIKE ...
        let Expr::Nested(expr) = expr.as_ref() else {
            return ControlFlow::Continue(());
        };

        // c.relname
        let Expr::CompoundIdentifier(ident) = expr.as_ref() else {
            return ControlFlow::Continue(());
        };
        if ident.len() != 2
            || ident[0].value.to_lowercase() != "c"
            || ident[1].value.to_lowercase() != "relname"
        {
            return ControlFlow::Continue(());
        }

        // ... LIKE <pattern>
        let Expr::Value(Value::SingleQuotedString(like_pattern)) = &**pattern else {
            return ControlFlow::Continue(());
        };
        self.table_name_pattern = Some(like_pattern.clone());
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        let SetExpr::SetOperation {
            op,
            left,
            right,
            set_quantifier,
        } = query.body.as_ref()
        else {
            return ControlFlow::Continue(());
        };

        if *op != SetOperator::Union || *set_quantifier != SetQuantifier::All {
            return ControlFlow::Continue(());
        }
        if is_select_from_pg_class(left) || is_select_from_pg_class(right) {
            self.found_select_from_pg_class = true;
        }
        ControlFlow::Continue(())
    }
}

fn is_select_from_pg_class(expr: &SetExpr) -> bool {
    let SetExpr::Select(select) = expr else {
        return false;
    };

    for table_with_joins in &select.from {
        if let sqlparser::ast::TableFactor::Table { name, .. } = &table_with_joins.relation {
            if name.to_string().to_lowercase() == "pg_catalog.pg_class" {
                return true;
            }
        }
    }
    false
}

fn prepare_like_pattern(pattern: &str) -> Option<String> {
    // Note: PostgreSQL escapes like pattern in 2 steps:
    // 1) add '\' before '_' and '%' characters;
    // 2) escape the resulting string.
    // So after that 2 '\' appear before '_'.
    //
    // See [make_like_pattern](https://github.com/postgres/postgres/blob/05506510de6ae24ba6de00cef2f458920c8a72ea/src/bin/psql/tab-complete.c#L5953) for details.
    //
    // We remove these backslashes because they prevent matching for names containing '_'.
    // For instance, `make_like_pattern("_pico")` returns "\\_pico" that doesn't match names
    // staring with "_pico".
    let pattern = pattern.replace("\\\\_", "_");

    // remove % from the end of the pattern
    pattern.strip_suffix('%').map(|x| x.to_string())
}

// Parse list of tables query and return table name pattern on success.
fn parse_list_of_tables_query(query: &str) -> Option<String> {
    let dialect = PostgreSqlDialect {};
    let ast = Parser::parse_sql(&dialect, query).ok()?;
    let mut visitor = ListOfTablesQueryParser::default();
    ast.visit(&mut visitor);

    if !visitor.found_select_from_pg_class {
        return None;
    }

    if let Some(pattern) = visitor.table_name_pattern {
        return prepare_like_pattern(&pattern);
    }

    None
}

pub fn parse(sql: &str) -> Option<WellKnownQuery> {
    if let Some(table_name_pattern) = parse_list_of_tables_query(sql) {
        return Some(WellKnownQuery::ListOfTables(table_name_pattern));
    }
    None
}
