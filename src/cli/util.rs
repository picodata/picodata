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
use tarantool::auth::AuthMethod;
use tarantool::network::{client::tls, AsClient, Client, Config};

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

impl Display for ResultSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResultSet::RowSet(s) => f.write_fmt(format_args!(
                "{}",
                s.first().expect(
                    "RowSet is represented as a Vec<Vec<Rows>> where outer vec always has lenghth equal to 1"
                )
            )),
            ResultSet::Error(_, message) => f.write_str(message),
            ResultSet::RowCount(c) => f.write_fmt(format_args!("{}", c.first().expect(
                "RowCount response always consists of a Vec containing the only entry"
            ))),
            ResultSet::Explain(e) => f.write_fmt(format_args!("{}", e.first().expect(
                "Explain is represented as a Vec<Vec<String>> where outer vec always has lenghth equal to 1"
            ))),
        }
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
            None => {
                let prompt_message = format!("Enter password for {username}: ");
                prompt_password(&prompt_message)
                    .map_err(|e| Error::other(format!("Failed to prompt for a password: {e}")))?
            }
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
                None => {
                    let prompt_message = format!("Enter password for {username}: ");
                    prompt_password(&prompt_message).map_err(|e| {
                        Error::other(format!("Failed to prompt for a password: {e}"))
                    })?
                }
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
            None => {
                let prompt_message = format!("Enter password for {username}: ");
                prompt_password(&prompt_message)
                    .map_err(|e| Error::other(format!("Failed to prompt for a password: {e}")))?
            }
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

    let username = config.creds.expect("credentials should be set").0;
    let error = crate::auth::Error::UserNotFoundOrInvalidCreds(username);
    Err(Box::new(error))
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
pub fn prompt_password(prompt: &str) -> std::io::Result<String> {
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
