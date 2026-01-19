use crate::address::IprotoAddress;
use crate::cli;
use crate::cli::args;
use crate::config::DEFAULT_USERNAME;
use crate::schema::PICO_SERVICE_USER_NAME;
use crate::traft;
use crate::traft::error::Error;

use std::fmt::Display;
use std::fs;
use std::io::{BufRead as _, BufReader, Write as _};
use std::path::Path;
use std::time::Duration;

use comfy_table::{ContentArrangement, Table};
use nix::sys::termios::{tcgetattr, tcsetattr, LocalFlags, SetArg::TCSADRAIN};
use serde::{Deserialize, Serialize};
use serde_json;
use tarantool::auth::AuthMethod;
use tarantool::network::{client::tls, AsClient, Client, Config};

/// Output format for admin console results.
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    /// Default ASCII table with headers and row count.
    Table,
    /// Values only, no headers or row count, with configurable separator (default: tab).
    TuplesOnly { separator: char },
    /// JSON array output.
    Json,
    /// CSV with configurable separator (default: comma).
    Csv { separator: char },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ColumnDesc {
    pub name: String,
    #[serde(rename = "type")]
    ty: String,
}

impl Display for ColumnDesc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RowSet {
    pub metadata: Vec<ColumnDesc>,
    pub rows: Vec<Vec<rmpv::Value>>,
}

impl RowSet {
    /// Format the row set according to the specified output format.
    pub fn format(&self, fmt: OutputFormat) -> String {
        match fmt {
            OutputFormat::Table => self.to_string(),
            OutputFormat::TuplesOnly { separator } => self.format_tuples_only(separator),
            OutputFormat::Json => self.format_json(),
            OutputFormat::Csv { separator } => self.format_csv(separator),
        }
    }

    /// Format a single MessagePack value as a string.
    fn format_value(v: &rmpv::Value) -> String {
        if let rmpv::Value::String(s) = v {
            s.as_str().unwrap_or("").to_string()
        } else if let rmpv::Value::Nil = v {
            String::new()
        } else {
            serde_json::to_string(v).unwrap_or_else(|_| v.to_string())
        }
    }

    /// Format as tuples only (no headers, no row count).
    fn format_tuples_only(&self, separator: char) -> String {
        let sep = separator.to_string();
        self.rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(Self::format_value)
                    .collect::<Vec<_>>()
                    .join(&sep)
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Format as JSON array of objects.
    fn format_json(&self) -> String {
        let objects: Vec<serde_json::Value> = self
            .rows
            .iter()
            .map(|row| {
                let obj: serde_json::Map<String, serde_json::Value> = self
                    .metadata
                    .iter()
                    .zip(row.iter())
                    .filter_map(|(col, val)| {
                        serde_json::to_value(val)
                            .ok()
                            .map(|v| (col.name.clone(), v))
                    })
                    .collect();
                serde_json::Value::Object(obj)
            })
            .collect();
        serde_json::to_string_pretty(&objects).unwrap_or_else(|_| "[]".to_string())
    }

    /// Escape a value for CSV output.
    fn escape_csv(value: &str, separator: char) -> String {
        if value.contains(separator) || value.contains('"') || value.contains('\n') {
            format!("\"{}\"", value.replace('"', "\"\""))
        } else {
            value.to_string()
        }
    }

    /// Format as CSV with the specified separator.
    fn format_csv(&self, separator: char) -> String {
        let sep = separator.to_string();
        let mut lines = vec![self
            .metadata
            .iter()
            .map(|c| Self::escape_csv(&c.name, separator))
            .collect::<Vec<_>>()
            .join(&sep)];

        let mut rows = self
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| Self::escape_csv(&Self::format_value(v), separator))
                    .collect::<Vec<_>>()
                    .join(&sep)
            })
            .collect::<Vec<_>>();

        lines.append(&mut rows);
        lines.join("\n")
    }
}

impl Display for RowSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut table = Table::new();
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(self.metadata.iter());

        for row in &self.rows {
            let formatted_row: Vec<String> = row
                .iter()
                .map(|v| {
                    // if cell is Utf8String, then we format it as plain string with no quotes
                    if let rmpv::Value::String(s) = v {
                        s.as_str().unwrap().to_string()
                    } else {
                        v.to_string()
                    }
                })
                .collect();

            table.add_row(formatted_row);
        }

        f.write_fmt(format_args!("{table}\n"))?;
        f.write_fmt(format_args!("({} rows)", self.rows.len()))
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RowCount {
    row_count: usize,
}

impl Display for RowCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.row_count))
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(transparent)]
pub struct ExplainResult {
    explain_result: Vec<Option<String>>,
}

impl Display for ExplainResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for i in self.explain_result.iter() {
            match i {
                Some(line) => writeln!(f, "{line}")?,
                None => writeln!(f)?,
            };
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum ResultSet {
    Explain(Vec<ExplainResult>),
    RowSet(Vec<RowSet>),
    RowCount(Vec<RowCount>),
    // Option<()> here is for ignoring tarantool redundant "null"
    // Example:
    // ---
    // - null   <-----
    // - "sbroad: rule parsing error: ...
    Error(Option<()>, String),
}

impl ResultSet {
    /// Format the result set according to the specified output format.
    pub fn format(&self, fmt: OutputFormat) -> String {
        match self {
            ResultSet::RowSet(s) => s
                .first()
                .expect(
                    "RowSet is represented as a Vec<Vec<Rows>> where outer vec always has lenghth equal to 1"
                )
                .format(fmt),
            ResultSet::Error(_, message) => message.clone(),
            ResultSet::RowCount(c) => c
                .first()
                .expect("RowCount response always consists of a Vec containing the only entry")
                .to_string(),
            ResultSet::Explain(e) => e
                .first()
                .expect(
                    "Explain is represented as a Vec<Vec<String>> where outer vec always has lenghth equal to 1"
                )
                .to_string(),
        }
    }
}

impl Display for ResultSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.format(OutputFormat::Table))
    }
}

//////////////////////////////////////////////////////////////////////
// Credentials
//////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    username: String,
    // TODO: <https://git.picodata.io/core/picodata/-/issues/1263>.
    password: String,
    method: Option<AuthMethod>,
}

impl Credentials {
    fn new(username: String, password: String, method: Option<AuthMethod>) -> Self {
        Self {
            username,
            password,
            method,
        }
    }

    pub fn connect(
        &self,
        address: &IprotoAddress,
        tls_args: &args::IprotoTlsArgs,
        timeout: Option<Duration>,
    ) -> cli::Result<Client> {
        let host = address.host.as_str();
        let port = address
            .port
            .parse::<u16>()
            .map_err(|e| format!("ERROR: parsing port '{}' failed: {e}", address.port))?;

        let any_connection_error = |e| {
            Error::other(format!(
                "ERROR: connection failure for address '{host}:{port}': {e}",
            ))
        };
        let connection_error = |e| {
            Error::other(format!(
                "ERROR: connection failure for address '{host}:{port}': {e}",
            ))
        };

        let mut config = Config::default();

        // NOTE: Cloning here is inevitable because connection config requires
        // an owned string. This might be fixed at some point in future.
        let authority = (self.username.clone(), self.password.clone());
        config.creds = Some(authority);
        config.connect_timeout = timeout;

        let tls_connector = if tls_args.cert.is_some() {
            let tls_config = tls_args.to_tls_config();
            Some(tls::TlsConnector::new(tls_config)?)
        } else {
            None
        };

        if let Some(method) = self.method {
            config.auth_method = method;
        } else {
            let connection_client = try_determine_auth_method(host, port, config, tls_connector)
                .map_err(any_connection_error)?;
            return Ok(connection_client);
        }

        let potential_client = ::tarantool::fiber::block_on(Client::connect_with_config_and_tls(
            host,
            port,
            config,
            tls_connector,
        ))
        .map_err(connection_error)?;

        // Check if the connection is valid. We need to do it because connect
        // is lazy and we want to check whether authentication have succeeded
        // or not.
        ::tarantool::fiber::block_on(potential_client.ping()).map_err(connection_error)?;

        Ok(potential_client)
    }
}

impl TryFrom<&args::Expel> for Credentials {
    type Error = traft::error::Error;

    fn try_from(value: &args::Expel) -> Result<Self, Self::Error> {
        let username = value
            .peer_address
            .user
            .clone()
            .unwrap_or_else(|| PICO_SERVICE_USER_NAME.to_owned());

        let password = match value.password_file.clone() {
            Some(path) => read_password_from_file(&path)?,
            None if !value.tls.cert_auth => {
                let prompt_message = format!("Enter password for {username}: ");
                prompt_password(&prompt_message)
                    .map_err(|e| Error::other(format!("Failed to prompt for a password: {e}")))?
            }
            _ => String::new(),
        };

        Ok(Credentials::new(username, password, value.auth_method))
    }
}

impl TryFrom<&args::Connect> for Credentials {
    type Error = traft::error::Error;

    fn try_from(value: &args::Connect) -> Result<Self, Self::Error> {
        let username = value
            .address
            .user
            .clone()
            .unwrap_or_else(|| value.user.clone());

        let password = if username == DEFAULT_USERNAME {
            String::new()
        } else {
            match value.password_file.clone() {
                Some(path) => read_password_from_file(&path)?,
                None if !value.tls.cert_auth => {
                    let prompt_message = format!("Enter password for {username}: ");
                    prompt_password(&prompt_message).map_err(|e| {
                        Error::other(format!("Failed to prompt for a password: {e}"))
                    })?
                }
                _ => String::new(),
            }
        };

        Ok(Credentials::new(username, password, value.auth_method))
    }
}

impl TryFrom<&args::Status> for Credentials {
    type Error = traft::error::Error;

    fn try_from(value: &args::Status) -> Result<Self, Self::Error> {
        let username = value
            .peer_address
            .user
            .clone()
            .unwrap_or_else(|| PICO_SERVICE_USER_NAME.to_owned());

        let password = match value.password_file.clone() {
            Some(path) => read_password_from_file(&path)?,
            None if !value.tls.cert_auth => {
                let prompt_message = format!("Enter password for {username}: ");
                prompt_password(&prompt_message)
                    .map_err(|e| Error::other(format!("Failed to prompt for a password: {e}")))?
            }
            _ => String::new(),
        };

        let method = if username == PICO_SERVICE_USER_NAME {
            Some(AuthMethod::ChapSha1)
        } else {
            None
        };

        Ok(Credentials::new(username, password, method))
    }
}

impl TryFrom<&args::ServiceConfigUpdate> for Credentials {
    type Error = traft::error::Error;

    fn try_from(value: &args::ServiceConfigUpdate) -> Result<Self, Self::Error> {
        let username = value
            .peer_address
            .user
            .clone()
            .unwrap_or_else(|| PICO_SERVICE_USER_NAME.to_owned());

        let password = match value.password_file.clone() {
            Some(path) => read_password_from_file(&path)?,
            None => String::new(),
        };

        Ok(Credentials::new(username, password, None))
    }
}

fn try_determine_auth_method(
    url: &str,
    port: u16,
    config: Config,
    tls_connector: Option<tls::TlsConnector>,
) -> cli::Result<Client> {
    for auth_method in AuthMethod::VARIANTS {
        // NOTE: This cloning is needed because authentication requires owned data.
        let mut config = config.clone();
        config.auth_method = *auth_method;

        let connection_client = ::tarantool::fiber::block_on(Client::connect_with_config_and_tls(
            url,
            port,
            config,
            tls_connector.clone(),
        ))?;

        // Even if we get a "successful" client after connection request, it does not mean it
        // is valid (i.e., in our case, it does not mean that appropriate auth method was used).
        // We have to make a testing request from a client to determine whether it is valid or not.
        let connection_validity = ::tarantool::fiber::block_on(connection_client.ping());

        if connection_validity.is_ok() {
            return Ok(connection_client);
        }
    }

    Err(String::from("User not found or supplied credentials are invalid").into())
}

fn read_password_from_file(path: &Path) -> traft::Result<String> {
    let read_error = |error| {
        let error_message = format!("ERROR: bad password file at '{path:?}': {error}");
        traft::error::Error::other(error_message)
    };

    let content = fs::read_to_string(path).map_err(read_error)?;
    let line = content
        .lines()
        .next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "empty file"))
        .map_err(read_error)?;

    // NOTE: Calling `to_owned` is fine here since passwords are typically
    // short strings, this function is called infrequently, and the clarity of
    // returning an owned string outweighs the minor allocation cost.
    let password = line.trim().to_owned();
    Ok(password)
}

/// # Description
///
/// Prompts a password from a terminal.
///
/// # Internals
///
/// This function bypasses `stdin` redirection (like `cat script.lua |
/// picodata connect`) and always prompts a password from a TTY.
fn prompt_password(prompt: &str) -> std::io::Result<String> {
    // See also: <https://man7.org/linux/man-pages/man3/termios.3.html>.
    let mut tty = std::fs::File::options()
        .read(true)
        .write(true)
        .open("/dev/tty")?;

    let tcattr_old = tcgetattr(&tty)?;

    // Disable echo while prompting a password
    let mut tcattr_new = tcattr_old.clone();
    tcattr_new.local_flags.set(LocalFlags::ECHO, false);
    tcattr_new.local_flags.set(LocalFlags::ECHONL, true);
    tcsetattr(&tty, TCSADRAIN, &tcattr_new)?;

    let do_prompt_password = |tty: &mut std::fs::File| {
        // Print the prompt
        tty.write_all(prompt.as_bytes())?;
        tty.flush()?;

        // Read the password
        let mut password = String::new();
        BufReader::new(tty).read_line(&mut password)?;

        if !password.ends_with('\n') {
            // Preliminary EOF, a user didn't hit enter
            return Err(std::io::Error::from(std::io::ErrorKind::Interrupted));
        }

        let crlf = |c| matches!(c, '\r' | '\n');
        Ok(password.trim_end_matches(crlf).to_owned())
    };

    // Try reading the password, then restore old terminal settings.
    let result = do_prompt_password(&mut tty);
    let _ = tcsetattr(&tty, TCSADRAIN, &tcattr_old);

    result
}

pub fn is_broken_pipe(error: &traft::error::Error) -> bool {
    if let traft::error::Error::Tarantool(tarantool::error::Error::IO(error)) = error {
        if error.kind() == std::io::ErrorKind::BrokenPipe {
            return true;
        }
    }
    false
}

/// Sets up a handler to terminate the current process when its parent process exits.
///
/// On **Linux**, this uses `prctl(PR_SET_PDEATHSIG, SIGKILL)` to automatically
/// send `SIGKILL` to the process if the parent dies.
///
/// On **macOS**, a background thread is spawned to periodically check if the
/// parent process has been reparented to Launchd (PID 1). If so, the process
/// exits with code 137 (parent death), emulating Linux's parent-death behavior.
pub fn set_parent_death_handler() {
    #[cfg(target_os = "linux")]
    {
        nix::sys::prctl::set_pdeathsig(nix::sys::signal::SIGKILL).expect("should not fail");
    }

    // macOS does not provide prctl or signalfd, so we cannot handle parent
    // death in the same way as Linux. Alternative approaches (pipes, signals)
    // caused issues due to restrictions in the Pike environment. To work
    // around this, we monitor whether the child process has been reparented
    // to Launchd (macOS init daemon process with PID 1), which indicates that
    // the original parent has exited. This emulates Linux's parent-death signal
    // behavior for `picodata demo` subcommand purposes.
    #[cfg(target_os = "macos")]
    {
        use nix::unistd::Pid;

        const LAUNCHD_PID: Pid = Pid::from_raw(1);

        std::thread::Builder::new()
            .name("macos-parent-death-monitor".into())
            .spawn(move || loop {
                if nix::unistd::getppid() == LAUNCHD_PID {
                    std::process::exit(137 /* parent death exit code */);
                }

                // NOTE: avoid busy waiting by sleeping.
                std::thread::sleep(Duration::from_millis(500));
            })
            .expect("should not fail");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_rowset() -> RowSet {
        RowSet {
            metadata: vec![
                ColumnDesc {
                    name: "id".to_string(),
                    ty: "integer".to_string(),
                },
                ColumnDesc {
                    name: "name".to_string(),
                    ty: "string".to_string(),
                },
            ],
            rows: vec![
                vec![
                    rmpv::Value::Integer(1.into()),
                    rmpv::Value::String("Alice".into()),
                ],
                vec![
                    rmpv::Value::Integer(2.into()),
                    rmpv::Value::String("Bob".into()),
                ],
            ],
        }
    }

    fn empty_rowset() -> RowSet {
        RowSet {
            metadata: vec![ColumnDesc {
                name: "id".to_string(),
                ty: "integer".to_string(),
            }],
            rows: vec![],
        }
    }

    #[test]
    fn test_format_tuples_only() {
        let rowset = sample_rowset();
        let output = rowset.format(OutputFormat::TuplesOnly { separator: '\t' });
        assert_eq!(output, "1\tAlice\n2\tBob");
    }

    #[test]
    fn test_format_tuples_only_custom_separator() {
        let rowset = sample_rowset();
        let output = rowset.format(OutputFormat::TuplesOnly { separator: '|' });
        assert_eq!(output, "1|Alice\n2|Bob");
    }

    #[test]
    fn test_format_tuples_only_empty() {
        let rowset = empty_rowset();
        let output = rowset.format(OutputFormat::TuplesOnly { separator: '\t' });
        assert_eq!(output, "");
    }

    #[test]
    fn test_format_json() {
        let rowset = sample_rowset();
        let output = rowset.format(OutputFormat::Json);
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["id"], 1);
        assert_eq!(arr[0]["name"], "Alice");
        assert_eq!(arr[1]["id"], 2);
        assert_eq!(arr[1]["name"], "Bob");
    }

    #[test]
    fn test_format_json_empty() {
        let rowset = empty_rowset();
        let output = rowset.format(OutputFormat::Json);
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 0);
    }

    #[test]
    fn test_format_csv() {
        let rowset = sample_rowset();
        let output = rowset.format(OutputFormat::Csv { separator: ',' });
        assert_eq!(output, "id,name\n1,Alice\n2,Bob");
    }

    #[test]
    fn test_format_csv_custom_separator() {
        let rowset = sample_rowset();
        let output = rowset.format(OutputFormat::Csv { separator: ';' });
        assert_eq!(output, "id;name\n1;Alice\n2;Bob");
    }

    #[test]
    fn test_format_csv_empty() {
        let rowset = empty_rowset();
        let output = rowset.format(OutputFormat::Csv { separator: ',' });
        assert_eq!(output, "id");
    }

    #[test]
    fn test_format_csv_escaping() {
        let rowset = RowSet {
            metadata: vec![ColumnDesc {
                name: "value".to_string(),
                ty: "string".to_string(),
            }],
            rows: vec![
                vec![rmpv::Value::String("hello,world".into())],
                vec![rmpv::Value::String("with\"quote".into())],
                vec![rmpv::Value::String("line\nbreak".into())],
            ],
        };
        let output = rowset.format(OutputFormat::Csv { separator: ',' });
        assert_eq!(
            output,
            "value\n\"hello,world\"\n\"with\"\"quote\"\n\"line\nbreak\""
        );
    }

    #[test]
    fn test_format_table_includes_row_count() {
        let rowset = sample_rowset();
        let output = rowset.format(OutputFormat::Table);
        assert!(output.contains("(2 rows)"));
        assert!(output.contains("id"));
        assert!(output.contains("name"));
    }

    #[test]
    fn test_format_value_nil() {
        let val = rmpv::Value::Nil;
        assert_eq!(RowSet::format_value(&val), "");
    }

    #[test]
    fn test_format_value_types() {
        assert_eq!(RowSet::format_value(&rmpv::Value::Boolean(true)), "true");
        assert_eq!(RowSet::format_value(&rmpv::Value::Integer(42.into())), "42");
        assert_eq!(
            RowSet::format_value(&rmpv::Value::String("test".into())),
            "test"
        );
        // Arrays and maps are serialized as compact JSON without whitespace
        let array = rmpv::Value::Array(vec![
            rmpv::Value::Integer(1.into()),
            rmpv::Value::Integer(2.into()),
            rmpv::Value::Integer(3.into()),
        ]);
        assert_eq!(RowSet::format_value(&array), "[1,2,3]");
    }

    #[test]
    fn test_result_set_format_delegates_to_rowset() {
        let rowset = sample_rowset();
        let result_set = ResultSet::RowSet(vec![rowset]);
        let output = result_set.format(OutputFormat::TuplesOnly { separator: '\t' });
        assert_eq!(output, "1\tAlice\n2\tBob");
    }

    #[test]
    fn test_result_set_error_ignores_format() {
        let result_set = ResultSet::Error(None, "some error".to_string());
        assert_eq!(result_set.format(OutputFormat::Json), "some error");
        assert_eq!(
            result_set.format(OutputFormat::Csv { separator: ',' }),
            "some error"
        );
    }
}
