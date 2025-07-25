use std::fmt::{Debug, Display};
use std::time::Duration;

use tarantool::auth::AuthMethod;
use tarantool::network::{AsClient, Client, Config};

use crate::address::IprotoAddress;
use crate::config::DEFAULT_USERNAME;
use crate::traft::error::Error;
use crate::util::prompt_password;
use nix::unistd::isatty;

use super::args;
use super::console::{Command, Console, ReplError, SpecialCommand};
use comfy_table::{ContentArrangement, Table};
use serde::{Deserialize, Serialize};

fn get_password_from_file(path: &str) -> Result<String, Error> {
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
            f.write_fmt(format_args!("{i}\n"))?
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
) -> Result<(Client, String), Error> {
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

fn sql_repl(args: args::Connect) -> Result<(), ReplError> {
    let (client, user) = determine_credentials_and_connect(
        &args.address,
        Some(&args.user),
        args.password_file.as_deref(),
        args.auth_method,
        Duration::from_secs(args.timeout),
    )
    .map_err(|err| {
        ReplError::Other(format!(
            "Connection Error (address {}). Try to reconnect: {}",
            args.address, err
        ))
    })?;

    // Check if connection is valid. We need to do it because connect is lazy
    // and we want to check whether authentication have succeeded or not
    if let Err(err) = ::tarantool::fiber::block_on(client.ping()) {
        return Err(ReplError::Other(format!(
            "Connection Error (address {}). Try to reconnect: {}",
            args.address, err
        )));
    }

    let mut console = Console::new()?;

    console.greet(&format!(
        "Connected to interactive console by address \"{}:{}\" under \"{}\" user",
        args.address.host, args.address.port, user
    ));

    const HELP_MESSAGE: &'static str = "
    Available backslash commands:
        \\e                              Open the editor specified by the EDITOR environment variable
        \\help                           Show this screen
        \\set delimiter shiny-delimiter  Set console delimiter to 'shiny-delimiter'
        \\set delimiter default          Reset console delimiter to default (;)
        \\set delimiter enter            Reset console delimiter to enter

    Available hotkeys:
        Enter                           Submit the request
        Alt  + Enter                    Insert a newline character
        Ctrl + C                        Discard current input
        Ctrl + D                        Quit interactive console";

    while let Some(command) = console.read()? {
        ::tarantool::fiber::block_on(client.ping())
            .map_err(|e| ReplError::LostConnectionToServer(e.into()))?;

        match command {
            Command::Control(command) => {
                match command {
                    SpecialCommand::PrintHelp => console.write(HELP_MESSAGE),
                    SpecialCommand::SwitchLanguage(_) => {
                        // picodata connect doesn't know about language switching
                        console.write("Unknown special sequence")
                    }
                }
            }
            Command::Expression(line) => {
                let response = ::tarantool::fiber::block_on(
                    client.call(".proc_sql_dispatch", &(line, Vec::<()>::new())),
                );

                let res = match response {
                    Ok(tuple) => {
                        let res = tuple.decode::<Vec<ResultSet>>().map_err(|err| {
                            ReplError::Other(format!(
                                "Error occurred while decoding response: {err}",
                            ))
                        })?;

                        // There should always be exactly one element in the outer tuple
                        let Some(res) = res.first() else {
                            return Err(ReplError::Other("Invalid form of response".to_string()));
                        };

                        res.to_string()
                    }

                    Err(err) => match err {
                        tarantool::network::ClientError::ErrorResponse(err) => {
                            let is_terminal = isatty(0).unwrap_or(false);
                            if !is_terminal {
                                return Err(ReplError::Other(err.to_string()));
                            }

                            err.to_string()
                        }
                        tarantool::network::ClientError::ConnectionClosed(err) => {
                            return Err(ReplError::LostConnectionToServer(err.into()));
                        }
                        e => return Err(e.into()),
                    },
                };

                console.write(&res);
            }
        };
    }

    Ok(())
}

pub fn main(args: args::Connect) -> ! {
    let tt_args = args.tt_args().unwrap();
    super::tarantool::main_cb(&tt_args, || -> Result<(), ReplError> {
        if let Err(error) = sql_repl(args) {
            eprintln!("{error}");
            std::process::exit(1);
        }
        std::process::exit(0)
    })
}
