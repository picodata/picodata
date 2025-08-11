use crate::cas;
use crate::cbus::ENDPOINT_NAME;
use crate::config::PicodataConfig;
use crate::error_code::ErrorCode;
use crate::plugin::PluginIdentifier;
use crate::plugin::PreconditionCheckResult;
use crate::plugin::{lock, reenterable_plugin_cas_request};
use crate::schema::ADMIN_ID;
use crate::storage::{self, Catalog, SystemTable};
use crate::traft::node;
use crate::traft::op::{Dml, Op};
use crate::util::Lexer;
use crate::util::QuoteEscapingStyle;
use crate::{sql, tlog, traft};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::time::Duration;
use std::{io, panic};
use tarantool::cbus;
use tarantool::error::BoxError;
use tarantool::error::IntoBoxError;
use tarantool::fiber;
use tarantool::time::Instant;

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

/// Function executes provided closure on a separate thread
/// blocking the current fiber until the result is ready.
pub fn blocking<F, R>(f: F) -> traft::Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (sender, receiver) = cbus::oneshot::channel(ENDPOINT_NAME);

    let jh = std::thread::Builder::new()
        .name("blocking".to_owned())
        .spawn(|| {
            sender.send(f());
        })?;

    if let Ok(r) = receiver.receive() {
        return Ok(r);
    };

    // Sender is dropped, which means provided function panicked.
    // Attempt to get proper error by joining the thread.
    // In theory this can block for insignificant amount of time.
    match jh.join() {
        Ok(_) => Err(BoxError::new(
            ErrorCode::Other,
            "BUG: thread returned without sending result to the channel",
        )
        .into()),
        Err(e) => panic::resume_unwind(e),
    }
}

/// Sends a task to a separate thread to calculate the checksum of the migrations file
/// and blocks the current fiber until the result is ready.
pub fn calculate_migration_hash_async(migration: &MigrationInfo) -> traft::Result<md5::Digest> {
    let shortname = &migration.filename_from_manifest;
    let fullpath = migration.full_filepath.clone();

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

    let res = blocking(move || {
        tlog!(Debug, "hashing a migrations file '{fullpath}'");
        let res = calculate_migration_hash_from_file(&fullpath);
        if let Err(e) = &res {
            tlog!(Debug, "failed hashing migrations file '{fullpath}': {e}");
        }
        res
    })?
    .inspect_err(|e| {
        #[rustfmt::skip]
        tlog!(Error, "failed receiving migrations hash from file '{shortname}': {e}");
    })?;

    let elapsed = t0.elapsed();
    #[rustfmt::skip]
    tlog!(Info, "done hashing migrations file '{shortname}', elapsed time: {elapsed:?}");

    Ok(res)
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
        let share_dir = PicodataConfig::get().instance.share_dir();
        let plugin_dir = share_dir
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
fn read_migration_queries_from_file_async(
    mut migration: MigrationInfo,
    plugin_ident: &PluginIdentifier,
    storage: &Catalog,
) -> traft::Result<MigrationInfo> {
    tlog!(Info, "parsing migrations file '{}'", migration.shortname());
    let t0 = Instant::now_accurate();

    let substitutions = storage
        .plugin_config
        .get_by_entity(plugin_ident, CONTEXT_ENTITY)?;

    let migration = blocking(move || {
        let fullpath = &migration.full_filepath;
        tlog!(Debug, "parsing a migrations file '{fullpath}'");
        let res = read_migration_queries_from_file(&mut migration, &substitutions);
        if let Err(e) = &res {
            let fullpath = &migration.full_filepath;
            tlog!(Debug, "failed parsing migrations file '{fullpath}': {e}");
        }

        res.map(|_| migration)
    })??;

    let elapsed = t0.elapsed();
    tlog!(
        Info,
        "done parsing migrations file '{}', elapsed time: {elapsed:?}",
        migration.shortname()
    );

    Ok(migration)
}

/// Reads and parses migrations file desribed by `migration`.
#[inline]
fn read_migration_queries_from_file(
    migration: &mut MigrationInfo,
    substitutions: &HashMap<String, rmpv::Value>,
) -> Result<(), BoxError> {
    let fullpath = &migration.full_filepath;
    let source = match std::fs::read_to_string(fullpath) {
        Ok(v) => v,
        Err(e) => {
            #[rustfmt::skip]
            return Err(BoxError::new(ErrorCode::Other, format!("failed reading file {fullpath}: {e}")));
        }
    };
    parse_migration_queries(&source, migration, substitutions)
}

/// Parses the migration queries from `source`, returns updates `migration` with
/// "UP" and "DOWN" queries from the file.
fn parse_migration_queries(
    source: &str,
    migration: &mut MigrationInfo,
    substitutions: &HashMap<String, rmpv::Value>,
) -> Result<(), BoxError> {
    let mut up_lines = vec![];
    let mut down_lines = vec![];

    let filename = &migration.filename_from_manifest;

    let mut state = State::Initial;

    for (mut line, lineno) in source.lines().zip(1..) {
        line = line.trim_end();
        if line.is_empty() {
            continue;
        }

        let temp = line
            .split_once("--")
            .map(|(l, r)| (l.trim_end(), r.trim_start()));
        match temp {
            Some(("", "pico.UP")) => {
                if !up_lines.is_empty() {
                    return Err(duplicate_annotation(filename, lineno, "pico.UP"));
                }
                state = State::ParsingUp;
                continue;
            }
            Some(("", "pico.DOWN")) => {
                if !down_lines.is_empty() {
                    return Err(duplicate_annotation(filename, lineno, "pico.DOWN"));
                }
                state = State::ParsingDown;
                continue;
            }
            Some(("", comment)) if comment.starts_with("pico.") => {
                return Err(BoxError::new(ErrorCode::PluginError,
                    format!("{filename}:{lineno}: unsupported annotation `{comment}`, expected one of `pico.UP`, `pico.DOWN`"),
                ));
            }
            Some((code, comment)) if comment.contains("pico.UP") => {
                debug_assert!(!code.is_empty());
                return Err(misplaced_annotation(filename, lineno, "pico.UP"));
            }
            Some((code, comment)) if comment.contains("pico.DOWN") => {
                debug_assert!(!code.is_empty());
                return Err(misplaced_annotation(filename, lineno, "pico.DOWN"));
            }
            Some((code, _)) => {
                if code.is_empty() {
                    // Ignore other comments
                    continue;
                }

                // Remove the trailling comment
                line = code;
            }
            None => {}
        }

        // A query line found
        match state {
            State::Initial => {
                return Err(BoxError::new(
                    ErrorCode::PluginError,
                    format!("{filename}: no pico.UP annotation found at start of file"),
                ));
            }
            State::ParsingUp => {
                up_lines.push(substitute_config_placeholders(
                    line,
                    filename,
                    lineno,
                    substitutions,
                )?);
            }
            State::ParsingDown => down_lines.push(substitute_config_placeholders(
                line,
                filename,
                lineno,
                substitutions,
            )?),
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

#[track_caller]
fn duplicate_annotation(filename: &str, lineno: usize, annotation: &str) -> BoxError {
    BoxError::new(
        ErrorCode::PluginError,
        format!("{filename}:{lineno}: duplicate `{annotation}` annotation found"),
    )
}

#[track_caller]
fn misplaced_annotation(filename: &str, lineno: usize, annotation: &str) -> BoxError {
    BoxError::new(
        ErrorCode::PluginError,
        format!("{filename}:{lineno}: unexpected `{annotation}` annotation, it must be at the start of the line"),
    )
}

fn split_sql_queries(lines: &[Cow<'_, str>]) -> Vec<String> {
    let mut queries = Vec::new();

    let mut current_query_start = 0;
    let mut current_query_length = 0;
    for (i, line) in lines.iter().enumerate() {
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

pub const CONTEXT_ENTITY: &str = "migration_context";

const MARKER: &str = "@_plugin_config.";

/// Finds all placeholders in provided query string and replaces with value taken from provided map.
/// Function returns query string with all substitutions applied if any
fn substitute_config_placeholders<'a>(
    query_string: &'a str,
    filename: &str,
    lineno: usize,
    substitutions: &HashMap<String, rmpv::Value>,
) -> Result<Cow<'a, str>, BoxError> {
    let mut query_string = Cow::from(query_string);

    // Note that it is wrong to search for all placeholders first and then replace them, because
    // after each replace position of the following placeholder in a string might change.
    while let Some(placeholder) = find_placeholder(query_string.as_ref()) {
        let key = &placeholder.variable;
        let Some(value) = substitutions.get(key) else {
            return Err(not_found(filename, lineno, key));
        };

        let Some(value) = value.as_str() else {
            return Err(bad_type(filename, lineno, key, value));
        };

        query_string
            .to_mut()
            .replace_range(placeholder.start..placeholder.end, value);
    }

    Ok(query_string)
}

#[track_caller]
fn not_found(filename: &str, lineno: usize, key: &str) -> BoxError {
    BoxError::new(
        ErrorCode::PluginError,
        format!("{filename}:{lineno}: no key named {key} found in migration context"),
    )
}

#[track_caller]
fn bad_type(filename: &str, lineno: usize, key: &str, value: &rmpv::Value) -> BoxError {
    BoxError::new(
        ErrorCode::PluginError,
        format!("{filename}:{lineno}: only strings are supported as placeholder targets, {key} is not a string but {value}")
    )
}

#[derive(Debug, PartialEq, Eq)]
struct Placeholder {
    variable: String,
    start: usize,
    end: usize,
}

fn find_placeholder(query_string: &str) -> Option<Placeholder> {
    let start = query_string.find(MARKER)?;

    // we either find next symbol that is not part of the name
    // or assume placeholder lasts till the end of the string
    let end = query_string[start + MARKER.len()..]
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(query_string.len() - start - MARKER.len());

    let variable = &query_string[start + MARKER.len()..start + MARKER.len() + end];

    Some(Placeholder {
        variable: variable.to_owned(),
        start,
        end: start + MARKER.len() + end,
    })
}

/// Apply sql from migration file onto cluster.
trait SqlApplier {
    fn apply(&self, sql: &str, deadline: Instant) -> traft::Result<()>;
}

/// By default, sql applied with SBroad.
struct SBroadApplier;

impl SqlApplier for SBroadApplier {
    #[track_caller]
    fn apply(&self, sql: &str, deadline: Instant) -> traft::Result<()> {
        // check that lock is still actual
        lock::lock_is_acquired_by_us()?;

        if fiber::clock() > deadline {
            return Err(traft::error::Error::timeout());
        }

        sql::parse_and_dispatch(sql, vec![], Some(deadline), None).map(|_| ())
    }
}

fn up_single_file(
    queries: &MigrationInfo,
    applier: &impl SqlApplier,
    deadline: Instant,
) -> traft::Result<()> {
    debug_assert!(queries.is_parsed);
    let filename = &queries.filename_from_manifest;

    for (sql, i) in queries.up.iter().zip(1..) {
        #[rustfmt::skip]
        tlog!(Debug, "applying `UP` migration query {filename} #{i}/{} `{}`", queries.up.len(), DisplayTruncated(sql));
        if let Err(e) = applier.apply(sql, deadline) {
            #[rustfmt::skip]
            tlog!(Error, "failed applying `UP` migration query (file: {filename}) `{}`: {e}", DisplayTruncated(sql));
            let cause = e.into_box_error();
            let message = format!(
                "Failed to apply `UP` command (file: {filename}) `{}`: {cause}",
                DisplayTruncated(sql)
            );
            if let Some((file, line)) = cause.file().zip(cause.line()) {
                return Err(
                    BoxError::with_location(cause.error_code(), message, file, line).into(),
                );
            } else {
                return Err(BoxError::new(cause.error_code(), message).into());
            }
        }
    }

    Ok(())
}

fn down_single_file(queries: &MigrationInfo, applier: &impl SqlApplier, deadline: Instant) {
    debug_assert!(queries.is_parsed);
    let filename = &queries.filename_from_manifest;

    for (sql, i) in queries.down.iter().zip(1..) {
        #[rustfmt::skip]
        tlog!(Debug, "applying `DOWN` migration query {filename} #{i}/{} `{}`", queries.down.len(), DisplayTruncated(sql));
        if let Err(e) = applier.apply(sql, deadline) {
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

    down_single_file(queries, applier, deadline);

    let make_op = || {
        lock::lock_is_acquired_by_us()?;

        let dml = Dml::delete(
            storage::PluginMigrations::TABLE_ID,
            &[plugin_name, &queries.filename_from_manifest],
            ADMIN_ID,
        )?;
        let ranges = vec![cas::Range::for_dml(&dml)?];
        Ok(PreconditionCheckResult::DoOp((Op::Dml(dml), ranges)))
    };

    tlog!(Debug, "updating global storage with DOWN migration");
    if let Err(e) = reenterable_plugin_cas_request(node, make_op, deadline) {
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
) -> traft::Result<()> {
    crate::error_injection!(block "PLUGIN_MIGRATION_LONG_MIGRATION");

    // checking the existence of migration files
    let mut migration_files = vec![];
    for file in migrations {
        let migration = MigrationInfo::new_unparsed(plugin_ident, file.clone());

        if !migration.path().exists() {
            return Err(
                io::Error::new(ErrorKind::NotFound, format!("file '{file}' not found")).into(),
            );
        }

        migration_files.push(migration);
    }

    let handle_err = |to_revert: &[MigrationInfo]| {
        let deadline = fiber::clock().saturating_add(rollback_timeout);
        let it = to_revert.iter().rev();
        for migration in it {
            down_single_file_with_commit(&plugin_ident.name, migration, &SBroadApplier, deadline);
        }
    };

    let migrations_count = migration_files.len();
    let mut seen_queries = Vec::with_capacity(migrations_count);

    let node = node::global().expect("node must be already initialized");
    for (num, migration) in migration_files.into_iter().enumerate() {
        #[rustfmt::skip]
        tlog!(Info, "applying `UP` migrations, progress: {num}/{migrations_count}");

        let migration =
            read_migration_queries_from_file_async(migration, plugin_ident, &node.storage)
                .inspect_err(|_| handle_err(&seen_queries))?;
        seen_queries.push(migration);
        let migration = seen_queries.last().expect("just inserted");

        if let Err(e) = up_single_file(migration, &SBroadApplier, deadline) {
            handle_err(&seen_queries);
            return Err(e);
        }

        let hash = match calculate_migration_hash_async(migration) {
            Ok(h) => h,
            Err(e) => {
                handle_err(&seen_queries);
                return Err(e);
            }
        };

        let make_op = || {
            lock::lock_is_acquired_by_us()?;

            let dml = Dml::replace(
                storage::PluginMigrations::TABLE_ID,
                &(
                    &plugin_ident.name,
                    &migration.filename_from_manifest,
                    &format!("{hash:x}"),
                ),
                ADMIN_ID,
            )?;
            let ranges = vec![cas::Range::for_dml(&dml)?];
            Ok(PreconditionCheckResult::DoOp((Op::Dml(dml), ranges)))
        };

        #[rustfmt::skip]
        tlog!(Debug, "updating global storage with migrations progress {num}/{migrations_count}");
        if let Err(e) = reenterable_plugin_cas_request(node, make_op, deadline) {
            #[rustfmt::skip]
            tlog!(Error, "failed: updating global storage with migrations progress: {e}");

            handle_err(&seen_queries);
            return Err(e);
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
    plugin_ident: &PluginIdentifier,
    migrations: &[String],
    deadline: Instant,
    storage: &Catalog,
) {
    let iter = migrations.iter().rev().zip(0..);
    for (filename, num) in iter {
        #[rustfmt::skip]
        tlog!(Info, "applying `DOWN` migrations, progress: {num}/{}", migrations.len());

        let migration = MigrationInfo::new_unparsed(plugin_ident, filename.clone());
        let migration =
            match read_migration_queries_from_file_async(migration, plugin_ident, storage) {
                Ok(migration) => migration,
                Err(e) => {
                    tlog!(Error, "Rollback DOWN migration error: {e}");
                    continue;
                }
            };

        down_single_file_with_commit(&plugin_ident.name, &migration, &SBroadApplier, deadline);
    }
    #[rustfmt::skip]
    tlog!(Info, "applying `DOWN` migrations, progress: {0}/{0}", migrations.len());
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use rmpv::Utf8String;
    use std::cell::RefCell;

    #[track_caller]
    fn parse_migration_queries_for_tests(sql: &str) -> Result<MigrationInfo, BoxError> {
        let mut migration = MigrationInfo {
            full_filepath: "not used".into(),
            filename_from_manifest: "test.db".into(),
            ..Default::default()
        };
        parse_migration_queries(sql, &mut migration, &HashMap::new())?;
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

   comment after;   -- command

-- test comment

no semicolon after last command
  -- pico.DOWN

sql_command1;
sql_command2; -- another comment --

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
                // Indentation is preserved, but trailing whitespace is removed
                "   comment after;",
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
            e.message(),
            "test.db: no pico.UP annotation found at start of file",
        );

        let queries = r#"
-- pico.up
        "#;
        let e = parse_migration_queries_for_tests(queries).unwrap_err();
        assert_eq!(
            e.message(),
            "test.db:2: unsupported annotation `pico.up`, expected one of `pico.UP`, `pico.DOWN`",
        );

        let queries = r#"
-- pico.UP
command;
-- pico.UP
        "#;
        let e = parse_migration_queries_for_tests(queries).unwrap_err();
        assert_eq!(
            e.message(),
            "test.db:4: duplicate `pico.UP` annotation found",
        );

        let queries = r#"
-- pico.DOWN
command_1;
-- pico.DOWN
command_2
        "#;
        let e = parse_migration_queries_for_tests(queries).unwrap_err();
        assert_eq!(
            e.message(),
            "test.db:4: duplicate `pico.DOWN` annotation found",
        );

        let queries = r#"
 -- pico.UP
command;
command_2; -- pico.DOWN
command_3;
        "#;
        let e = parse_migration_queries_for_tests(queries).unwrap_err();
        assert_eq!(
            e.message(),
            "test.db:4: unexpected `pico.DOWN` annotation, it must be at the start of the line",
        );

        let queries = r#"
-- pico.UP
command; -- pico.UP
        "#;
        let e = parse_migration_queries_for_tests(queries).unwrap_err();
        assert_eq!(
            e.message(),
            "test.db:3: unexpected `pico.UP` annotation, it must be at the start of the line",
        );
    }

    struct BufApplier {
        poison_query: Option<&'static str>,
        buf: RefCell<Vec<String>>,
    }

    impl SqlApplier for BufApplier {
        fn apply(&self, sql: &str, _deadline: Instant) -> crate::traft::Result<()> {
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
        let deadline = Instant::now_accurate().saturating_add(tarantool::time::INFINITY);
        down_single_file(&queries, &applier, deadline);
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
        let deadline = Instant::now_accurate().saturating_add(tarantool::time::INFINITY);
        down_single_file(&queries, &applier, deadline);
        #[rustfmt::skip]
        assert_eq!(
            applier.buf.borrow().iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            &["sql_command_1;", "sql_command_3;"],
        );
    }

    #[test]
    fn test_find_placeholder() {
        let query_string = "SELECT @_plugin_config.kek FROM bubba".to_owned();
        let placeholder = find_placeholder(&query_string).expect("placeholder must be found");
        assert_eq!(
            placeholder,
            Placeholder {
                variable: "kek".to_owned(),
                start: 7,
                end: 26,
            }
        );

        // check with underscore and such
        let query_string = "SELECT @_plugin_config.kek_91 FROM bubba".to_owned();
        let placeholder = find_placeholder(&query_string).expect("placeholder must be found");

        assert_eq!(
            placeholder,
            Placeholder {
                variable: "kek_91".to_owned(),
                start: 7,
                end: 29,
            }
        );

        let query_string = "SELECT @_plugin_config.kek.with.dot FROM bubba".to_owned();
        let placeholder_with_service =
            find_placeholder(&query_string).expect("placeholder must be found");
        assert_eq!(
            placeholder_with_service,
            Placeholder {
                variable: "kek".to_owned(),
                start: 7,
                end: 26,
            }
        );

        // check case at the end of the query
        let query_string = "foo bar @_plugin_config.kek".to_owned();
        let placeholder = find_placeholder(&query_string).expect("placeholder must be found");

        assert_eq!(
            placeholder,
            Placeholder {
                variable: "kek".to_owned(),
                start: 8,
                end: 27,
            }
        );

        // at the end with special symbols
        let query_string = "foo bar @_plugin_config.kek_".to_owned();
        let placeholder = find_placeholder(&query_string).expect("placeholder must be found");

        assert_eq!(
            placeholder,
            Placeholder {
                variable: "kek_".to_owned(),
                start: 8,
                end: 28,
            }
        );

        let query_string = "SELECT 1";
        assert!(find_placeholder(&query_string).is_none());
    }

    fn migration_context() -> HashMap<String, rmpv::Value> {
        let mut substitutions = HashMap::new();
        substitutions.insert(
            String::from("var_a"),
            rmpv::Value::String(Utf8String::from("value_123")),
        );

        substitutions.insert(
            String::from("var_b_longer_name"),
            rmpv::Value::String(Utf8String::from("value_123_also_long")),
        );

        substitutions
    }

    #[test]
    fn test_substitute_config_placeholder_one() {
        let mut query_string = "SELECT @_plugin_config.var_a".to_owned();

        let substitutions = migration_context();

        assert_eq!(
            substitute_config_placeholders(&mut query_string, "test", 1, &substitutions).unwrap(),
            "SELECT value_123"
        )
    }

    #[test]
    fn test_substitute_config_placeholders_many() {
        let mut query_string =
            "SELECT @_plugin_config.var_a @@_plugin_config.var_b_longer_name".to_owned();

        let substitutions = migration_context();

        assert_eq!(
            substitute_config_placeholders(&mut query_string, "test", 1, &substitutions).unwrap(),
            "SELECT value_123 @value_123_also_long"
        )
    }
}

mod tests_internal {
    use std::panic;

    use crate::cbus::init_cbus_endpoint;
    use crate::plugin::migration::blocking;

    #[tarantool::test]
    fn test_blocking_ok() {
        init_cbus_endpoint();

        let res = blocking(|| 42).unwrap();
        assert_eq!(res, 42)
    }

    #[tarantool::test]
    fn test_blocking_panics() {
        init_cbus_endpoint();

        // Restore the default panic hook in case it's been replaced.
        // We don't have to set it again afterwards because each test that's
        // defined using `tarantool::test` is run in a separate process.
        let _ = panic::take_hook();

        let res = panic::catch_unwind(|| blocking(|| panic!("uh oh")).unwrap());
        assert_eq!(
            res.unwrap_err().downcast_ref::<&'static str>().unwrap(),
            &"uh oh"
        );
    }
}
