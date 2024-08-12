use crate::cbus::ENDPOINT_NAME;
use crate::plugin::reenterable_plugin_cas_request;
use crate::plugin::PreconditionCheckResult;
use crate::plugin::{PluginIdentifier, PLUGIN_DIR};
use crate::schema::ADMIN_ID;
use crate::storage::ClusterwideTable;
use crate::traft::node;
use crate::traft::op::{Dml, Op};
use crate::util::Lexer;
use crate::util::QuoteEscapingStyle;
use crate::{error_injection, sql, tlog, traft};
use std::fs::File;
use std::io;
use std::io::{ErrorKind, Read};
use std::time::Duration;
use tarantool::cbus;
use tarantool::fiber;
use tarantool::time::Instant;

const MIGRATION_FILE_EXT: &'static str = "db";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("File `{0}` invalid extension, `.db` expected")]
    Extension(String),

    #[error("Error while open migration file `{0}`: {1}")]
    File(String, io::Error),

    #[error("Failed spawning a migrations parsing thread: {0}")]
    ThreadDead(String),

    #[error("Invalid migration file format: {0}")]
    InvalidMigrationFormat(String),

    #[error("Failed to apply `UP` command (file: {filename}) `{}`: {error}", DisplayTruncated(.command))]
    Up {
        filename: String,
        command: String,
        error: String,
    },

    #[error("Update migration progress: {0}")]
    UpdateProgress(String),

    #[error("inconsistent with previous version migration list, reason: {0}")]
    InconsistentMigrationList(String),
}

const MAX_COMMAND_LENGTH_TO_SHOW: usize = 256;
struct DisplayTruncated<'a>(&'a str);

impl std::fmt::Display for DisplayTruncated<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let command = self.0;

        if command.len() < MAX_COMMAND_LENGTH_TO_SHOW {
            return f.write_str(command);
        }

        let mut lexer = Lexer::new(command);
        lexer.set_quote_escaping_style(QuoteEscapingStyle::DoubleSingleQuote);

        let Some(first_token) = lexer.next_token() else {
            return f.write_str(command);
        };

        if first_token.end > MAX_COMMAND_LENGTH_TO_SHOW {
            // First token is too big, just truncate it
            f.write_str(&command[0..MAX_COMMAND_LENGTH_TO_SHOW])?;
            f.write_str("...")?;
            return Ok(());
        }

        let mut current_end = first_token.end;
        while let Some(token) = lexer.next_token() {
            if token.end > MAX_COMMAND_LENGTH_TO_SHOW {
                break;
            }
            current_end = token.end;
        }

        f.write_str(&command[0..current_end])?;
        f.write_str("...")?;
        Ok(())
    }
}

/// Sends a task to a separate thread to calculate the checksum of the migrations file
/// and blocks the current fiber until the result is ready.
pub fn calculate_migration_hash_async(migration: &MigrationInfo) -> Result<md5::Digest, Error> {
    let shortname = &migration.filename_from_manifest;
    let fullpath = &migration.full_filepath;

    let (sender, receiver) = cbus::oneshot::channel(ENDPOINT_NAME);

    fn calculate_migration_hash_from_file(filename: &str) -> Result<md5::Digest, io::Error> {
        const BUF_SIZE: usize = 4096;

        let mut f = File::open(filename)?;
        let mut context = md5::Context::new();
        let mut buffer = [0; BUF_SIZE];

        loop {
            let n = f.read(&mut buffer)?;
            if n == 0 {
                break;
            }

            context.consume(&buffer[..n]);
        }

        let digest = context.compute();
        Ok(digest)
    }

    tlog!(Info, "hashing migrations file '{shortname}'");
    let t0 = Instant::now_accurate();

    std::thread::scope(|s| -> std::io::Result<_> {
        std::thread::Builder::new()
            .name("migrations_parser".into())
            .spawn_scoped(s, move || {
                tlog!(Debug, "hashing a migrations file '{fullpath}'");
                let res = calculate_migration_hash_from_file(fullpath)
                    .map_err(|e| Error::File(fullpath.to_string(), e));
                if let Err(e) = &res {
                    tlog!(Debug, "failed hashing migrations file '{fullpath}': {e}");
                }

                sender.send(res)
            })?;
        Ok(())
    })
    .map_err(|e| Error::ThreadDead(e.to_string()))?;

    let res = receiver.receive().map_err(|e| {
        #[rustfmt::skip]
        tlog!(Error, "failed receiving migrations hash from file '{shortname}': {e}");
        Error::ThreadDead(e.to_string())
    });

    let elapsed = t0.elapsed();
    #[rustfmt::skip]
    tlog!(Info, "done hashing migrations file '{shortname}', elapsed time: {elapsed:?}");

    res?
}

/// Stores info about a migration file which is being processed.
#[derive(Debug, Default, Clone)]
pub struct MigrationInfo {
    /// This is the filepath specified in the plugin manifest. This value is stored
    /// in the system tables and is used for displaying diagnostics to the user.
    ///
    /// This should be a postfix of [`Self::full_filepath`].
    filename_from_manifest: String,

    /// This is the full path to the file with the migration queries.
    /// It is used for accessing the file system.
    full_filepath: String,

    is_parsed: bool,
    up: Vec<String>,
    down: Vec<String>,
}

impl MigrationInfo {
    /// Initializes the struct, doesn't read the file yet.
    pub fn new_unparsed(plugin_ident: &PluginIdentifier, filename: String) -> Self {
        let plugin_dir = PLUGIN_DIR
            .with(|dir| dir.lock().clone())
            .join(&plugin_ident.name)
            .join(&plugin_ident.version);
        let fullpath = plugin_dir.join(&filename);

        MigrationInfo {
            filename_from_manifest: filename,
            full_filepath: fullpath.to_string_lossy().into_owned(),
            is_parsed: false,
            up: vec![],
            down: vec![],
        }
    }

    /// Returns the full path to the migration file.
    #[inline(always)]
    pub fn path(&self) -> &std::path::Path {
        std::path::Path::new(&self.full_filepath)
    }

    /// Returns the filename from the plugin's manifest file.
    #[inline(always)]
    pub fn shortname(&self) -> &str {
        &self.filename_from_manifest
    }
}

/// Sends a task to a separate thread to parse the migrations file and blocks
/// the current fiber until the result is ready.
fn read_migration_queries_from_file_async(migration: &mut MigrationInfo) -> Result<(), Error> {
    let (sender, receiver) = cbus::oneshot::channel(ENDPOINT_NAME);

    let shortname = &migration.filename_from_manifest;
    let fullpath = &migration.full_filepath;
    let mut migration_for_other_thread = migration.clone();

    tlog!(Info, "parsing migrations file '{shortname}'");
    let t0 = Instant::now_accurate();

    std::thread::scope(|s| -> std::io::Result<_> {
        std::thread::Builder::new()
            .name("migrations_parser".into())
            .spawn_scoped(s, move || {
                let fullpath = &migration_for_other_thread.full_filepath;
                tlog!(Debug, "parsing a migrations file '{fullpath}'");
                let res = read_migration_queries_from_file(&mut migration_for_other_thread);
                if let Err(e) = &res {
                    let fullpath = &migration_for_other_thread.full_filepath;
                    tlog!(Debug, "failed parsing migrations file '{fullpath}': {e}");
                }

                sender.send(res.map(|()| migration_for_other_thread))
            })?;
        Ok(())
    })
    .map_err(|e| Error::ThreadDead(e.to_string()))?;

    // FIXME: add receive_timeout/receive_deadline
    let res = receiver.receive().map_err(|e| {
        #[rustfmt::skip]
        tlog!(Error, "failed receiving migrations parsed from file '{fullpath}': {e}");
        Error::ThreadDead(e.to_string())
    });

    let elapsed = t0.elapsed();
    #[rustfmt::skip]
    tlog!(Info, "done parsing migrations file '{shortname}', elapsed time: {elapsed:?}");

    let migration_from_other_thread = res??;
    #[rustfmt::skip]
    debug_assert_eq!(migration.full_filepath, migration_from_other_thread.full_filepath);
    #[rustfmt::skip]
    debug_assert_eq!(migration.filename_from_manifest, migration_from_other_thread.filename_from_manifest);

    *migration = migration_from_other_thread;
    Ok(())
}

/// Reads and parses migrations file desribed by `migration`.
#[inline]
fn read_migration_queries_from_file(migration: &mut MigrationInfo) -> Result<(), Error> {
    let fullpath = &migration.full_filepath;
    let source = std::fs::read_to_string(fullpath).map_err(|e| Error::File(fullpath.into(), e))?;
    parse_migration_queries(&source, migration)
}

/// Parses the migration queries from `source`, returns updates `migration` with
/// "UP" and "DOWN" queries from the file.
fn parse_migration_queries(source: &str, migration: &mut MigrationInfo) -> Result<(), Error> {
    let mut up_lines = vec![];
    let mut down_lines = vec![];

    let filename = &migration.filename_from_manifest;

    let mut state = State::Initial;

    for (line, lineno) in source.lines().zip(1..) {
        let line_trimmed = line.trim();
        if line_trimmed.is_empty() {
            continue;
        }

        let temp = line_trimmed
            .split_once("--")
            .map(|(l, r)| (l.trim_end(), r.trim_start()));
        match temp {
            Some(("", "pico.UP")) => {
                if !up_lines.is_empty() {
                    return Err(Error::InvalidMigrationFormat(format!(
                        "{filename}:{lineno}: duplicate `pico.UP` annotation found"
                    )));
                }
                state = State::ParsingUp;
                continue;
            }
            Some(("", "pico.DOWN")) => {
                if !down_lines.is_empty() {
                    return Err(Error::InvalidMigrationFormat(format!(
                        "{filename}:{lineno}: duplicate `pico.DOWN` annotation found"
                    )));
                }
                state = State::ParsingDown;
                continue;
            }
            Some(("", comment)) if comment.starts_with("pico.") => {
                return Err(Error::InvalidMigrationFormat(
                    format!("{filename}:{lineno}: unsupported annotation `{comment}`, expected one of `pico.UP`, `pico.DOWN`"),
                ));
            }
            Some((code, comment)) if comment.contains("pico.UP") => {
                debug_assert!(!code.is_empty());
                return Err(Error::InvalidMigrationFormat(format!(
                    "{filename}:{lineno}: unexpected `pico.UP` annotation, it must be at the start of the line"
                )));
            }
            Some((code, comment)) if comment.contains("pico.DOWN") => {
                debug_assert!(!code.is_empty());
                return Err(Error::InvalidMigrationFormat(format!(
                    "{filename}:{lineno}: unexpected `pico.DOWN` annotation, it must be at the start of the line"
                )));
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

    migration.is_parsed = true;
    migration.up = split_sql_queries(&up_lines);
    migration.down = split_sql_queries(&down_lines);
    Ok(())
}

fn split_sql_queries(lines: &[&str]) -> Vec<String> {
    let mut queries = Vec::new();

    let mut current_query_start = 0;
    let mut current_query_length = 0;
    for (line, i) in lines.iter().copied().zip(0..) {
        // `+ 1` for an extra '\n'
        current_query_length += line.len() + 1;

        let mut found_query_end = false;
        if let Some((code, _comment)) = line.split_once("--") {
            if code.trim_end().ends_with(';') {
                found_query_end = true;
            }
        } else if line.trim_end().ends_with(';') {
            found_query_end = true;
        }

        let is_last_line = i == lines.len() - 1;
        if found_query_end || is_last_line {
            let mut query = String::with_capacity(current_query_length);
            for line in &lines[current_query_start..i + 1] {
                query.push_str(line);
                // Add the original line breaks because the query may be
                // shown to the user for debugging.
                query.push('\n');
            }

            // And immediately remove one trailing newline because OCD
            let trailing_newline = query.pop();
            debug_assert_eq!(trailing_newline, Some('\n'));

            queries.push(query);
            current_query_start = i + 1;
            current_query_length = 0;
        }
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

        sql::sql_dispatch(sql, vec![], None, None).map(|_| ())
    }
}

fn up_single_file(
    queries: &MigrationInfo,
    applier: &impl SqlApplier,
    deadline: Instant,
) -> Result<(), Error> {
    debug_assert!(queries.is_parsed);
    let filename = &queries.filename_from_manifest;

    for (sql, i) in queries.up.iter().zip(1..) {
        #[rustfmt::skip]
        tlog!(Debug, "applying `UP` migration query {filename} #{i}/{} `{}`", queries.up.len(), DisplayTruncated(sql));
        if let Err(e) = applier.apply(sql, Some(deadline)) {
            #[rustfmt::skip]
            tlog!(Error, "failed applying `UP` migration query (file: {filename}) `{}`", DisplayTruncated(sql));
            return Err(Error::Up {
                filename: filename.into(),
                command: sql.clone(),
                error: e.to_string(),
            });
        }
    }

    Ok(())
}

fn down_single_file(queries: &MigrationInfo, applier: &impl SqlApplier) {
    debug_assert!(queries.is_parsed);
    let filename = &queries.filename_from_manifest;

    for (sql, i) in queries.down.iter().zip(1..) {
        #[rustfmt::skip]
        tlog!(Debug, "applying `DOWN` migration query {filename} #{i}/{} `{}`", queries.down.len(), DisplayTruncated(sql));
        if let Err(e) = applier.apply(sql, None) {
            #[rustfmt::skip]
            tlog!(Error, "Error while apply DOWN query (file: {filename}) `{}`: {e}", DisplayTruncated(sql));
        }
    }
}

fn down_single_file_with_commit(
    plugin_name: &str,
    queries: &MigrationInfo,
    applier: &impl SqlApplier,
    deadline: Instant,
) {
    let node = node::global().expect("node must be already initialized");

    down_single_file(queries, applier);

    let make_op = || {
        let dml = Dml::delete(
            ClusterwideTable::PluginMigration,
            &[plugin_name, &queries.filename_from_manifest],
            ADMIN_ID,
        )?;
        Ok(PreconditionCheckResult::DoOp(Op::Dml(dml)))
    };

    tlog!(Debug, "updating global storage with DOWN migration");
    if let Err(e) = reenterable_plugin_cas_request(node, make_op, vec![], deadline) {
        tlog!(
            Debug,
            "failed: updating global storage with regular DOWN migration progress: {e}"
        );
    }
}

/// Apply UP part from migration files. If one of migration files migrated with errors,
/// then rollback happens: for file that triggered error and all previously migrated files
/// DOWN part is called.
///
/// # Arguments
///
/// * `plugin_ident`: plugin for which migrations belong to
/// * `migrations`: list of migration file names
/// * `deadline`: applying deadline
pub fn apply_up_migrations(
    plugin_ident: &PluginIdentifier,
    migrations: &[String],
    deadline: Instant,
    rollback_timeout: Duration,
) -> crate::plugin::Result<()> {
    // checking the existence of migration files
    let mut migration_files = vec![];
    for file in migrations {
        let migration = MigrationInfo::new_unparsed(plugin_ident, file.clone());

        if error_injection::is_enabled("PLUGIN_MIGRATION_FIRST_FILE_INVALID_EXT") {
            return Err(Error::Extension(file.to_string()).into());
        }
        if migration
            .path()
            .extension()
            .and_then(|os_str| os_str.to_str())
            != Some(MIGRATION_FILE_EXT)
        {
            return Err(Error::Extension(file.to_string()).into());
        }

        if !migration.path().exists() {
            return Err(Error::File(
                file.to_string(),
                io::Error::new(ErrorKind::NotFound, "file not found"),
            )
            .into());
        }

        migration_files.push(migration);
    }

    let handle_err = |to_revert: &[MigrationInfo]| {
        let deadline = fiber::clock().saturating_add(rollback_timeout);
        let it = to_revert.iter().rev();
        for queries in it {
            down_single_file_with_commit(&plugin_ident.name, queries, &SBroadApplier, deadline);
        }
    };

    let migrations_count = migration_files.len();
    let mut seen_queries = Vec::with_capacity(migrations_count);

    let node = node::global().expect("node must be already initialized");
    for (num, mut migration) in migration_files.into_iter().enumerate() {
        #[rustfmt::skip]
        tlog!(Info, "applying `UP` migrations, progress: {num}/{migrations_count}");

        let res = read_migration_queries_from_file_async(&mut migration);
        if let Err(e) = res {
            handle_err(&seen_queries);
            return Err(e.into());
        }
        seen_queries.push(migration);
        let migration = seen_queries.last().expect("just inserted");

        if num == 1 && error_injection::is_enabled("PLUGIN_MIGRATION_SECOND_FILE_APPLY_ERROR") {
            handle_err(&seen_queries);
            return Err(Error::Up {
                filename: migration.filename_from_manifest.clone(),
                command: "<no-command>".into(),
                error: "injected error".into(),
            }
            .into());
        }

        if let Err(e) = up_single_file(migration, &SBroadApplier, deadline) {
            handle_err(&seen_queries);
            return Err(e.into());
        }

        let hash = match calculate_migration_hash_async(migration) {
            Ok(h) => h,
            Err(e) => {
                handle_err(&seen_queries);
                return Err(e.into());
            }
        };

        let make_op = || {
            let dml = Dml::replace(
                ClusterwideTable::PluginMigration,
                &(
                    &plugin_ident.name,
                    &migration.filename_from_manifest,
                    &format!("{:x}", hash),
                ),
                ADMIN_ID,
            )?;
            Ok(PreconditionCheckResult::DoOp(Op::Dml(dml)))
        };

        #[rustfmt::skip]
        tlog!(Debug, "updating global storage with migrations progress {num}/{migrations_count}");
        // FIXME: currently it possible that migrations will be initiated from
        // 2 different instances simultaneously which can break some invariants.
        // What we should do is to introduce a global lock in _pico_property
        // such that the client first checks if the lock is acquired, then does
        // a CaS request to acquire the lock and only after that starts doing
        // the migrations, and releases the lock at the end.
        if let Err(e) = reenterable_plugin_cas_request(node, make_op, vec![], deadline) {
            #[rustfmt::skip]
            tlog!(Error, "failed: updating global storage with migrations progress: {e}");

            handle_err(&seen_queries);
            return Err(Error::UpdateProgress(e.to_string()).into());
        }
    }
    #[rustfmt::skip]
    tlog!(Info, "applying `UP` migrations, progress: {0}/{0}", migrations_count);

    Ok(())
}

/// Apply DOWN part from migration files.
///
/// # Arguments
///
/// * `plugin_identity`: plugin for which migrations belong to
/// * `migrations`: list of migration file names
pub fn apply_down_migrations(
    plugin_identity: &PluginIdentifier,
    migrations: &[String],
    deadline: Instant,
) {
    let iter = migrations.iter().rev().zip(0..);
    for (filename, num) in iter {
        #[rustfmt::skip]
        tlog!(Info, "applying `DOWN` migrations, progress: {num}/{}", migrations.len());

        let mut migration = MigrationInfo::new_unparsed(plugin_identity, filename.clone());
        let res = read_migration_queries_from_file_async(&mut migration);
        if let Err(e) = res {
            tlog!(Error, "Rollback DOWN migration error: {e}");
            continue;
        }

        down_single_file_with_commit(&plugin_identity.name, &migration, &SBroadApplier, deadline);
    }
    #[rustfmt::skip]
    tlog!(Info, "applying `DOWN` migrations, progress: {0}/{0}", migrations.len());
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::cell::RefCell;

    #[track_caller]
    fn parse_migration_queries_for_tests(sql: &str) -> Result<MigrationInfo, Error> {
        let mut migration = MigrationInfo {
            full_filepath: "not used".into(),
            filename_from_manifest: "test.db".into(),
            ..Default::default()
        };
        parse_migration_queries(sql, &mut migration)?;
        Ok(migration)
    }

    #[test]
    fn test_parse_migration_queries() {
        let queries = r#"
-- pico.UP
sql_command1
        "#;
        let queries = parse_migration_queries_for_tests(queries).unwrap();
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
    multiline
    command
);

command with 'semicolon ; in quotes';

comment after; -- command

-- test comment

no semicolon after last command
-- pico.DOWN

sql_command1;
sql_command2;

-- test comment

        "#;
        let queries = parse_migration_queries_for_tests(queries).unwrap();
        assert_eq!(
            queries.up,
            &[
                "multiline\nsql\ncommand;",
                "another command;",
                "nested (\n    multiline\n    command\n);",
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
        let e = parse_migration_queries_for_tests(queries).unwrap_err();
        assert_eq!(
            e.to_string(),
            "Invalid migration file format: test.db: no pico.UP annotation found at start of file",
        );

        let queries = r#"
-- pico.up
        "#;
        let e = parse_migration_queries_for_tests(queries).unwrap_err();
        assert_eq!(
            e.to_string(),
            "Invalid migration file format: test.db:2: unsupported annotation `pico.up`, expected one of `pico.UP`, `pico.DOWN`",
        );

        let queries = r#"
-- pico.UP
command;
-- pico.UP
        "#;
        let e = parse_migration_queries_for_tests(queries).unwrap_err();
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
        let e = parse_migration_queries_for_tests(queries).unwrap_err();
        assert_eq!(
            e.to_string(),
            "Invalid migration file format: test.db:4: duplicate `pico.DOWN` annotation found",
        );

        let queries = r#"
 -- pico.UP
command;
command_2; -- pico.DOWN
command_3;
        "#;
        let e = parse_migration_queries_for_tests(queries).unwrap_err();
        assert_eq!(
            e.to_string(),
            "Invalid migration file format: test.db:4: unexpected `pico.DOWN` annotation, it must be at the start of the line",
        );

        let queries = r#"
-- pico.UP
command; -- pico.UP
        "#;
        let e = parse_migration_queries_for_tests(queries).unwrap_err();
        assert_eq!(
            e.to_string(),
            "Invalid migration file format: test.db:3: unexpected `pico.UP` annotation, it must be at the start of the line",
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
        let no_deadline = Instant::now_accurate().saturating_add(tarantool::clock::INFINITY);

        let source = r#"
-- pico.UP
sql_command_1;
sql_command_2;
sql_command_3;
"#;
        let queries = parse_migration_queries_for_tests(source).unwrap();
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
        let queries = parse_migration_queries_for_tests(source).unwrap();
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
        let queries = parse_migration_queries_for_tests(&source).unwrap();
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
        let queries = parse_migration_queries_for_tests(&source).unwrap();
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
