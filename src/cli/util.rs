use crate::address::IprotoAddress;
use crate::cli::console::ReplError;
use crate::config::DEFAULT_USERNAME;
use crate::traft;
use crate::traft::error::Error;
use crate::util::prompt_password;

use std::fmt::Display;
use std::time::Duration;

use comfy_table::{ContentArrangement, Table};
use serde::{Deserialize, Serialize};
use tarantool::auth::AuthMethod;
use tarantool::network::{Client, Config};

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
    explain_result: Vec<String>,
}

impl Display for ExplainResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for i in self.explain_result.iter() {
            f.write_fmt(format_args!("{}\n", i))?
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

fn get_password_from_file(path: &str) -> traft::Result<String> {
    let content = std::fs::read_to_string(path).map_err(|e| {
        Error::other(format!(
            r#"can't read password from password file by "{path}", reason: {e}"#
        ))
    })?;

    let password = content
        .lines()
        .next()
        .ok_or_else(|| Error::other("Empty password file"))?
        .trim();

    if password.is_empty() {
        return Ok(String::new());
    }

    Ok(password.into())
}

/// Determines the username and password from the provided arguments and uses
/// these credentials to establish a connection to a remote instance at given `address`.
///
/// The user for the connection is determined in the following way:
/// - if `address.user` is not `None`, it is used, else
/// - if `user` is not `None`, it is used, else
/// - [`DEFAULT_USERNAME`] is used.
///
/// The password for the connection is determined in the following way:
/// - if resulting user is `DEFAULT_USERNAME`, the password is empty (!?!??), else
/// - if `password_file` is not `None`, it is used to read the password from, else
/// - prompts the user for the password on the tty.
///
/// On success returns the connection object and the chosen username.
pub fn determine_credentials_and_connect(
    address: &IprotoAddress,
    user: Option<&str>,
    password_file: Option<&str>,
    auth_method: AuthMethod,
    timeout: Duration,
) -> traft::Result<(Client, String)> {
    let user = if let Some(user) = &address.user {
        user
    } else if let Some(user) = user {
        user
    } else {
        DEFAULT_USERNAME
    };

    let password = if user == DEFAULT_USERNAME {
        String::new()
    } else if let Some(path) = password_file {
        get_password_from_file(path)?
    } else {
        let prompt = format!("Enter password for {user}: ");
        prompt_password(&prompt)
            .map_err(|err| Error::other(format!("Failed to prompt for a password: {err}")))?
    };

    let mut config = Config::default();
    config.creds = Some((user.into(), password));
    config.auth_method = auth_method;
    config.connect_timeout = Some(timeout);

    let port = match address.port.parse::<u16>() {
        Ok(port) => port,
        Err(err) => {
            return Err(Error::other(ReplError::Other(format!(
                "Error while parsing instance port '{}': {err}",
                address.port
            ))))
        }
    };

    let client =
        ::tarantool::fiber::block_on(Client::connect_with_config(&address.host, port, config))?;

    Ok((client, user.into()))
}
