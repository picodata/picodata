use crate::cas::Range;
use crate::plugin::{do_plugin_cas, PLUGIN_DIR};
use crate::schema::{PluginDef, ADMIN_ID};
use crate::storage::ClusterwideTable;
use crate::traft::node;
use crate::traft::op::{Dml, Op};
use crate::{error_injection, sql, tlog, traft};
use sbroad::backend::sql::ir::PatternWithParams;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, ErrorKind};
use std::path::{Path, PathBuf};
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
    InvalidMigrationFormat(&'static str),

    #[error("Error while apply UP command `{0}`: {1}")]
    Up(String, String),

    #[error("Update migration progress: {0}")]
    UpdateProgress(String),
}

#[derive(Debug, PartialEq)]
enum Annotation {
    Up,
    Down,
}

#[derive(Debug, PartialEq)]
enum MigrationLine {
    Comment(String),
    Annotation(Annotation),
    String(String),
}

/// Parse migration data line by line.
struct MigrationLineParser<B: BufRead> {
    inner: io::Lines<B>,
}

impl<B: BufRead> MigrationLineParser<B> {
    fn new(lines: io::Lines<B>) -> Self {
        Self { inner: lines }
    }
}

impl MigrationLineParser<BufReader<File>> {
    /// Construct parser from .db file.
    fn from_file<P: AsRef<Path>>(filename: P) -> Result<Self, Error> {
        let file_path = filename.as_ref();
        let file = File::open(file_path)
            .map_err(|e| Error::File(file_path.to_string_lossy().to_string(), e))?;
        let lines = io::BufReader::new(file).lines();
        Ok(Self::new(lines))
    }
}

impl<B: BufRead> Iterator for MigrationLineParser<B> {
    type Item = MigrationLine;

    fn next(&mut self) -> Option<Self::Item> {
        fn extract_comment(line: &str) -> Option<&str> {
            let (prefix, comment) = line.trim().split_once("--")?;
            if !prefix.is_empty() {
                return None;
            };
            Some(comment.trim())
        }

        for line in self.inner.by_ref() {
            let line = match line {
                Err(_) => {
                    // ignore non-utf8 lines
                    continue;
                }
                Ok(line) => {
                    if line.is_empty() {
                        continue;
                    }

                    let maybe_comment = extract_comment(&line);

                    if let Some(comment) = maybe_comment {
                        match comment {
                            "pico.UP" => MigrationLine::Annotation(Annotation::Up),
                            "pico.DOWN" => MigrationLine::Annotation(Annotation::Down),
                            _ => MigrationLine::Comment(comment.to_string()),
                        }
                    } else {
                        MigrationLine::String(line)
                    }
                }
            };
            return Some(line);
        }

        None
    }
}

/// Apply sql from migration file onto cluster.
trait SqlApplier {
    fn apply(&self, sql: &str) -> traft::Result<()>;
}

/// By default, sql applied with SBroad.
struct SBroadApplier;

impl SqlApplier for SBroadApplier {
    fn apply(&self, sql: &str) -> traft::Result<()> {
        let mut params = PatternWithParams::new(sql.to_string(), vec![]);
        params.tracer = Some("stat".to_string());
        sql::dispatch_sql_query(params.into()).map(|_| ())
    }
}

fn up_single_file<B: BufRead>(
    mut migration_iter: MigrationLineParser<B>,
    applier: impl SqlApplier,
) -> Result<(), Error> {
    for line in migration_iter.by_ref() {
        match line {
            MigrationLine::Comment(_) => continue,
            MigrationLine::Annotation(Annotation::Up) => break,
            _ => {
                return Err(Error::InvalidMigrationFormat(
                    "no pico.UP annotation found at start of file",
                ))
            }
        }
    }

    for line in migration_iter {
        match line {
            MigrationLine::Comment(_) => continue,
            MigrationLine::Annotation(Annotation::Down) => return Ok(()),
            MigrationLine::Annotation(Annotation::Up) => {
                return Err(Error::InvalidMigrationFormat(
                    "only single pico.UP annotation allowed",
                ))
            }
            MigrationLine::String(sql) => {
                if let Err(e) = applier.apply(&sql) {
                    return Err(Error::Up(sql, e.to_string()));
                }
            }
        }
    }

    Ok(())
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

        migration_files.push(migration_path.clone());
    }

    fn handle_err(to_revert: &[PathBuf]) {
        let it = to_revert.iter().rev();
        for f in it {
            let iter = match MigrationLineParser::from_file(f) {
                Ok(mi) => mi,
                Err(e) => {
                    tlog!(Error, "Rollback DOWN migration error: {e}");
                    continue;
                }
            };

            down_single_file(iter, SBroadApplier);
        }
    }

    let node = node::global().expect("node must be already initialized");
    for (num, db_file) in migration_files.iter().enumerate() {
        if num == 1 && error_injection::is_enabled("PLUGIN_MIGRATION_SECOND_FILE_APPLY_ERROR") {
            handle_err(&migration_files[..num + 1]);
            return Err(Error::Up("".to_string(), "".to_string()).into());
        }

        let migration_iter = MigrationLineParser::from_file(db_file)?;
        if let Err(e) = up_single_file(migration_iter, SBroadApplier) {
            handle_err(&migration_files[..num + 1]);
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
            handle_err(&migration_files[..num + 1]);
            return Err(Error::UpdateProgress(e.to_string()).into());
        }
    }

    Ok(())
}

fn down_single_file<B: BufRead>(migration_iter: MigrationLineParser<B>, applier: impl SqlApplier) {
    // skip all while pico.DOWN is not reached
    let migration_iter = migration_iter
        .skip_while(|line| !matches!(line, MigrationLine::Annotation(Annotation::Down)))
        .skip(1);

    for line in migration_iter {
        match line {
            MigrationLine::Comment(_) => continue,
            MigrationLine::Annotation(_) => {
                let e = Error::InvalidMigrationFormat(
                    "only single pico.UP/pico.DOWN annotation allowed",
                );
                tlog!(Error, "Error while apply DOWN command: {e}");
            }
            MigrationLine::String(sql) => {
                if let Err(e) = applier.apply(&sql) {
                    tlog!(Error, "Error while apply DOWN command `{sql}`: {e}");
                }
            }
        }
    }
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
        let migration_iter = match MigrationLineParser::from_file(&migration_path) {
            Ok(mi) => mi,
            Err(e) => {
                tlog!(Error, "Rollback DOWN migration error: {e}");
                continue;
            }
        };

        down_single_file(migration_iter, SBroadApplier);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::io::{BufRead, BufReader};
    use std::rc::Rc;

    #[test]
    fn test_migration_line_parser() {
        struct TestCase {
            migration_data: &'static str,
            expected_lines: Vec<MigrationLine>,
        }
        let test_cases = vec![
            TestCase {
                migration_data: r#"
-- pico.UP
sql_command_1
"#,
                expected_lines: vec![
                    MigrationLine::Annotation(Annotation::Up),
                    MigrationLine::String("sql_command_1".to_string()),
                ],
            },
            TestCase {
                migration_data: r#"
-- test comment

-- pico.UP
-- pico.DOWN

sql_command_1
-- test comment
"#,
                expected_lines: vec![
                    MigrationLine::Comment("test comment".to_string()),
                    MigrationLine::Annotation(Annotation::Up),
                    MigrationLine::Annotation(Annotation::Down),
                    MigrationLine::String("sql_command_1".to_string()),
                    MigrationLine::Comment("test comment".to_string()),
                ],
            },
        ];

        for tc in test_cases {
            let lines = BufReader::new(tc.migration_data.as_bytes()).lines();
            let parser = MigrationLineParser::new(lines);
            let parsing_res = parser.collect::<Vec<_>>();

            assert_eq!(parsing_res, tc.expected_lines);
        }
    }

    struct BufApplier {
        poison_line: Option<&'static str>,
        buf: Rc<RefCell<Vec<String>>>,
    }

    impl SqlApplier for BufApplier {
        fn apply(&self, sql: &str) -> crate::traft::Result<()> {
            if let Some(p) = self.poison_line {
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
        struct TestCase {
            migration_data: &'static str,
            poison_line: Option<&'static str>,
            expected_applied_commands: Vec<&'static str>,
            error: bool,
        }
        let test_cases = vec![
            TestCase {
                migration_data: r#"
-- pico.UP
sql_command_1
sql_command_2
sql_command_3
"#,
                poison_line: None,
                expected_applied_commands: vec!["sql_command_1", "sql_command_2", "sql_command_3"],
                error: false,
            },
            TestCase {
                migration_data: r#"
-- pico.UP
sql_command_1
sql_command_2
sql_command_3
"#,
                poison_line: Some("sql_command_2"),
                expected_applied_commands: vec!["sql_command_1"],
                error: true,
            },
            TestCase {
                migration_data: r#"
-- pico.U
sql_command_1
"#,
                poison_line: None,
                expected_applied_commands: vec![],
                error: true,
            },
            TestCase {
                migration_data: r#"
-- pico.UP
sql_command_1
-- pico.UP
sql_command_2
"#,
                poison_line: None,
                expected_applied_commands: vec!["sql_command_1"],
                error: true,
            },
        ];

        for tc in test_cases {
            let lines = BufReader::new(tc.migration_data.as_bytes()).lines();
            let iter = MigrationLineParser::new(lines);
            let buf = Rc::new(RefCell::new(vec![]));
            let applier = BufApplier {
                buf: buf.clone(),
                poison_line: tc.poison_line,
            };
            let result = up_single_file(iter, applier);

            assert_eq!(tc.error, result.is_err());
            assert_eq!(
                buf.borrow().iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                tc.expected_applied_commands
            );
        }
    }

    #[test]
    fn test_migration_down() {
        struct TestCase {
            migration_data: &'static str,
            poison_line: Option<&'static str>,
            expected_applied_commands: Vec<&'static str>,
        }
        let test_cases = vec![
            TestCase {
                migration_data: r#"
-- pico.UP
sql_command_1
-- pico.DOWN
sql_command_2
sql_command_3
"#,
                poison_line: None,
                expected_applied_commands: vec!["sql_command_2", "sql_command_3"],
            },
            TestCase {
                migration_data: r#"
-- pico.UP
-- pico.DOWN
sql_command_1
sql_command_2
sql_command_3
"#,
                poison_line: Some("sql_command_2"),
                expected_applied_commands: vec!["sql_command_1", "sql_command_3"],
            },
            TestCase {
                migration_data: r#"
-- pico.DOWN
sql_command_1
-- pico.DOWN
sql_command_2
"#,
                poison_line: None,
                expected_applied_commands: vec!["sql_command_1", "sql_command_2"],
            },
        ];

        for tc in test_cases {
            let lines = BufReader::new(tc.migration_data.as_bytes()).lines();
            let iter = MigrationLineParser::new(lines);
            let buf = Rc::new(RefCell::new(vec![]));
            let applier = BufApplier {
                buf: buf.clone(),
                poison_line: tc.poison_line,
            };
            down_single_file(iter, applier);
            assert_eq!(
                buf.borrow().iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                tc.expected_applied_commands
            );
        }
    }
}
