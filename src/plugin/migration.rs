use crate::cas::Range;
use crate::plugin::{do_plugin_cas, PLUGIN_DIR};
use crate::schema::{PluginDef, ADMIN_ID};
use crate::storage::ClusterwideTable;
use crate::traft::node;
use crate::traft::op::{Dml, Op};
use crate::{error_injection, sql, tlog, traft};
use sbroad::backend::sql::ir::PatternWithParams;
use std::io;
use std::io::ErrorKind;
use tarantool::fiber;
use tarantool::space::UpdateOps;
use tarantool::time::Instant;

const MIGRATION_FILE_EXT: &'static str = "db";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("File `{0}` invalid extension, `.db` expected")]
    Extension(String),

    #[error("Error while open migration file `{0}`: {1}")]
    File(String, io::Error),

    #[error("Invalid migration file format: {0}")]
    InvalidMigrationFormat(String),

    #[error("Error while apply UP command `{0}`: {1}")]
    Up(String, String),

    #[error("Update migration progress: {0}")]
    UpdateProgress(String),
}

fn extract_comment(line: &str) -> Option<&str> {
    let (prefix, comment) = line.trim().split_once("--")?;
    if !prefix.is_empty() {
        return None;
    };
    Some(comment.trim())
}

#[derive(Debug)]
struct MigrationQueries {
    up: Vec<String>,
    down: Vec<String>,
}

/// Reads and parses `filename`, returns a pair of arrays: one
/// for "UP" queries and one for "DOWN" queries.
#[inline]
fn read_migration_queries_from_file(filename: &str) -> Result<MigrationQueries, Error> {
    let source = std::fs::read_to_string(filename).map_err(|e| Error::File(filename.into(), e))?;
    parse_migration_queries(&source, filename)
}

/// Parses the migration queries from `source`, returns a pair of arrays: one
/// for "UP" queries and one for "DOWN" queries.
///
/// `filename` is only used for reporting errors to the user.
fn parse_migration_queries(source: &str, filename: &str) -> Result<MigrationQueries, Error> {
    let mut up_lines = vec![];
    let mut down_lines = vec![];

    let mut state = State::Initial;

    for (line, lineno) in source.lines().zip(1..) {
        if line.trim().is_empty() {
            continue;
        }

        match extract_comment(line) {
            Some("pico.UP") => {
                if !up_lines.is_empty() {
                    return Err(Error::InvalidMigrationFormat(format!(
                        "{filename}:{lineno}: duplicate `pico.UP` annotation found"
                    )));
                }
                state = State::ParsingUp;
                continue;
            }
            Some("pico.DOWN") => {
                if !down_lines.is_empty() {
                    return Err(Error::InvalidMigrationFormat(format!(
                        "{filename}:{lineno}: duplicate `pico.DOWN` annotation found"
                    )));
                }
                state = State::ParsingDown;
                continue;
            }
            Some(comment) if comment.starts_with("pico.") => {
                return Err(Error::InvalidMigrationFormat(
                    format!("{filename}:{lineno}: unsupported annotation `{comment}`, expected one of `pico.UP`, `pico.DOWN`"),
                ));
            }
            Some(_) => {
                // Ignore other comments
                continue;
            }
            None => {}
        }

        // A query line found
        match state {
            State::Initial => {
                return Err(Error::InvalidMigrationFormat(format!(
                    "{filename}: no pico.UP annotation found at start of file"
                )));
            }
            State::ParsingUp => up_lines.push(line),
            State::ParsingDown => down_lines.push(line),
        }
    }

    enum State {
        Initial,
        ParsingUp,
        ParsingDown,
    }

    let all_up_queries = up_lines.join("\n");
    let all_down_queries = down_lines.join("\n");

    Ok(MigrationQueries {
        up: split_sql_queries(&all_up_queries),
        down: split_sql_queries(&all_down_queries),
    })
}

fn split_sql_queries(sql: &str) -> Vec<String> {
    let mut queries = Vec::new();

    let mut lexer = crate::util::Lexer::new(sql);
    lexer.set_quote_escaping_style(crate::util::QuoteEscapingStyle::DoubleSingleQuote);

    let mut paren_depth = 0;
    let mut current_query_start = 0;
    while let Some(token) = lexer.next_token() {
        match token.text {
            "(" => {
                paren_depth += 1;
            }
            ")" => {
                paren_depth -= 1;
            }
            ";" => {
                if paren_depth != 0 {
                    // Ignore semicolons in nested queries
                } else {
                    let query = &sql[current_query_start..token.end];
                    queries.push(query.trim().to_owned());
                    current_query_start = token.end;
                }
            }
            _ => {}
        }
    }

    let tail = sql[current_query_start..].trim();
    if !tail.is_empty() {
        // no semicolon at the end of last query
        queries.push(tail.to_owned());
    }

    queries
}

/// Apply sql from migration file onto cluster.
trait SqlApplier {
    fn apply(&self, sql: &str, deadline: Option<Instant>) -> traft::Result<()>;
}

/// By default, sql applied with SBroad.
struct SBroadApplier;

impl SqlApplier for SBroadApplier {
    fn apply(&self, sql: &str, deadline: Option<Instant>) -> traft::Result<()> {
        // Should sbroad accept a timeout parameter?
        if let Some(deadline) = deadline {
            if fiber::clock() > deadline {
                return Err(traft::error::Error::Timeout);
            }
        }

        let mut params = PatternWithParams::new(sql.to_string(), vec![]);
        params.tracer = Some("stat".to_string());
        sql::dispatch_sql_query(params.into()).map(|_| ())
    }
}

fn up_single_file(
    queries: &MigrationQueries,
    applier: &impl SqlApplier,
    deadline: Instant,
) -> Result<(), Error> {
    for sql in &queries.up {
        if let Err(e) = applier.apply(sql, Some(deadline)) {
            return Err(Error::Up(sql.clone(), e.to_string()));
        }
    }

    Ok(())
}

fn down_single_file(queries: &MigrationQueries, applier: &impl SqlApplier) {
    for sql in &queries.down {
        if let Err(e) = applier.apply(sql, None) {
            tlog!(Error, "Error while apply DOWN command `{sql}`: {e}");
        }
    }
}

/// Apply UP part from migration files. If one of migration files migrated with errors,
/// then rollback happens: for file that triggered error and all previously migrated files
/// DOWN part is called.
///
/// # Arguments
///
/// * `plugin_name`: name of plugin for which migrations belong to
/// * `migrations`: list of migration file names
/// * `deadline`: applying deadline
pub fn up(
    plugin_name: &str,
    migrations: &[String],
    deadline: Instant,
) -> crate::plugin::Result<()> {
    // checking the existence of migration files
    let mut migration_files = vec![];
    let plugin_dir = PLUGIN_DIR.with(|dir| dir.lock().clone()).join(plugin_name);
    for file in migrations {
        let migration_path = plugin_dir.join(file);

        if error_injection::is_enabled("PLUGIN_MIGRATION_FIRST_FILE_INVALID_EXT") {
            return Err(Error::Extension(file.to_string()).into());
        }
        if migration_path
            .extension()
            .and_then(|os_str| os_str.to_str())
            != Some(MIGRATION_FILE_EXT)
        {
            return Err(Error::Extension(file.to_string()).into());
        }

        if !migration_path.exists() {
            return Err(Error::File(
                file.to_string(),
                io::Error::new(ErrorKind::NotFound, "file not found"),
            )
            .into());
        }

        migration_files.push(migration_path.display().to_string());
    }

    fn handle_err(to_revert: &[MigrationQueries]) {
        let it = to_revert.iter().rev();
        for queries in it {
            down_single_file(queries, &SBroadApplier);
        }
    }

    let mut seen_queries = Vec::with_capacity(migration_files.len());

    let node = node::global().expect("node must be already initialized");
    for (num, db_file) in migration_files.iter().enumerate() {
        let res = read_migration_queries_from_file(db_file);
        let queries = crate::unwrap_ok_or!(res,
            Err(e) => {
                handle_err(&seen_queries);
                return Err(e.into());
            }
        );
        seen_queries.push(queries);
        let queries = seen_queries.last().expect("just inserted");

        if num == 1 && error_injection::is_enabled("PLUGIN_MIGRATION_SECOND_FILE_APPLY_ERROR") {
            handle_err(&seen_queries);
            return Err(Error::Up("".to_string(), "".to_string()).into());
        }

        if let Err(e) = up_single_file(queries, &SBroadApplier, deadline) {
            handle_err(&seen_queries);
            return Err(e.into());
        }

        let mut enable_ops = UpdateOps::new();
        enable_ops
            .assign(PluginDef::FIELD_MIGRATION_PROGRESS, num)
            .expect("serialization cannot fail");
        let update_dml = Dml::update(
            ClusterwideTable::Plugin,
            &[plugin_name],
            enable_ops,
            ADMIN_ID,
        )?;
        let ranges = vec![Range::new(ClusterwideTable::Plugin).eq([plugin_name])];

        if let Err(e) = do_plugin_cas(node, Op::Dml(update_dml), ranges, None, deadline) {
            handle_err(&seen_queries);
            return Err(Error::UpdateProgress(e.to_string()).into());
        }
    }

    Ok(())
}

/// Apply DOWN part from migration files.
///
/// # Arguments
///
/// * `plugin_name`: name of plugin for which migrations belong to
/// * `migrations`: list of migration file names
pub fn down(plugin_name: &str, migrations: &[String]) {
    let migrations = migrations.iter().rev();
    for db_file in migrations {
        let plugin_dir = PLUGIN_DIR.with(|dir| dir.lock().clone()).join(plugin_name);
        let migration_path = plugin_dir.join(db_file);
        let filename = migration_path.display().to_string();

        let res = read_migration_queries_from_file(&filename);
        let queries = crate::unwrap_ok_or!(res,
            Err(e) => {
                tlog!(Error, "Rollback DOWN migration error: {e}");
                continue;
            }
        );

        down_single_file(&queries, &SBroadApplier);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::cell::RefCell;

    #[test]
    fn test_parse_migration_queries() {
        let queries = r#"
-- pico.UP
sql_command1
        "#;
        let queries = parse_migration_queries(queries, "test.db").unwrap();
        assert_eq!(queries.up, &["sql_command1"],);
        assert!(queries.down.is_empty());

        let queries = r#"
-- test comment

-- pico.UP
multiline
sql
command;
another command;
nested (
    multiline;
    command;
);

command with 'semicolon ; in quotes';

-- test comment

no semicolon after last command
-- pico.DOWN

sql_command1;
sql_command2;

-- test comment

        "#;
        let queries = parse_migration_queries(queries, "test.db").unwrap();
        assert_eq!(
            queries.up,
            &[
                "multiline
sql
command;",
                "another command;",
                "nested (\n    multiline;\n    command;\n);",
                "command with 'semicolon ; in quotes';",
                "no semicolon after last command",
            ],
        );
        assert_eq!(queries.down, &["sql_command1;", "sql_command2;",],);

        //
        // Errors
        //
        let queries = r#"
sql_command1
        "#;
        let e = parse_migration_queries(queries, "test.db").unwrap_err();
        assert_eq!(
            e.to_string(),
            "Invalid migration file format: test.db: no pico.UP annotation found at start of file",
        );

        let queries = r#"
-- pico.up
        "#;
        let e = parse_migration_queries(queries, "test.db").unwrap_err();
        assert_eq!(
            e.to_string(),
            "Invalid migration file format: test.db:2: unsupported annotation `pico.up`, expected one of `pico.UP`, `pico.DOWN`",
        );

        let queries = r#"
-- pico.UP
command;
-- pico.UP
        "#;
        let e = parse_migration_queries(queries, "test.db").unwrap_err();
        assert_eq!(
            e.to_string(),
            "Invalid migration file format: test.db:4: duplicate `pico.UP` annotation found",
        );

        let queries = r#"
-- pico.DOWN
command_1;
-- pico.DOWN
command_2
        "#;
        let e = parse_migration_queries(queries, "test.db").unwrap_err();
        assert_eq!(
            e.to_string(),
            "Invalid migration file format: test.db:4: duplicate `pico.DOWN` annotation found",
        );
    }

    struct BufApplier {
        poison_query: Option<&'static str>,
        buf: RefCell<Vec<String>>,
    }

    impl SqlApplier for BufApplier {
        fn apply(&self, sql: &str, _deadline: Option<Instant>) -> crate::traft::Result<()> {
            if let Some(p) = self.poison_query {
                if p == sql {
                    return Err(crate::traft::error::Error::Other("test error".into()));
                }
            }
            self.buf.borrow_mut().push(sql.to_string());
            Ok(())
        }
    }

    #[test]
    fn test_migration_up() {
        let no_deadline = Instant::now().saturating_add(tarantool::clock::INFINITY);

        let source = r#"
-- pico.UP
sql_command_1;
sql_command_2;
sql_command_3;
"#;
        let queries = parse_migration_queries(source, "test.db").unwrap();
        let applier = BufApplier {
            buf: RefCell::new(vec![]),
            poison_query: None,
        };
        up_single_file(&queries, &applier, no_deadline).unwrap();

        #[rustfmt::skip]
        assert_eq!(
            applier.buf.borrow().iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            &["sql_command_1;", "sql_command_2;", "sql_command_3;"],
        );

        //
        let source = r#"
-- pico.UP
sql_command_1;
sql_command_2;
sql_command_3;
"#;
        let queries = parse_migration_queries(source, "test.db").unwrap();
        let applier = BufApplier {
            buf: RefCell::new(vec![]),
            poison_query: Some("sql_command_2;"),
        };
        up_single_file(&queries, &applier, no_deadline).unwrap_err();

        #[rustfmt::skip]
        assert_eq!(
            applier.buf.borrow().iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            &["sql_command_1;"],
        );
    }

    #[test]
    fn test_migration_down() {
        let source = r#"
-- pico.UP
sql_command_1
-- pico.DOWN
sql_command_2;
sql_command_3;
"#;
        let queries = parse_migration_queries(&source, "test.db").unwrap();
        let applier = BufApplier {
            buf: RefCell::new(vec![]),
            poison_query: None,
        };
        down_single_file(&queries, &applier);
        #[rustfmt::skip]
        assert_eq!(
            applier.buf.borrow().iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            &["sql_command_2;", "sql_command_3;"],
        );

        //
        let source = r#"
-- pico.UP
-- pico.DOWN
sql_command_1;
sql_command_2;
sql_command_3;
"#;
        let queries = parse_migration_queries(&source, "test.db").unwrap();
        let applier = BufApplier {
            buf: RefCell::new(vec![]),
            poison_query: Some("sql_command_2;"),
        };
        down_single_file(&queries, &applier);
        #[rustfmt::skip]
        assert_eq!(
            applier.buf.borrow().iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            &["sql_command_1;", "sql_command_3;"],
        );
    }
}
