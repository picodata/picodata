use crate::cli::args;
use crate::cli::console::{Command, Console, ReplError, SpecialCommand};
use crate::cli::util::Credentials;

use std::fmt::{Debug, Display};
use std::time::Duration;

use comfy_table::{ContentArrangement, Table};
use nix::unistd::isatty;
use serde::{Deserialize, Serialize};
use tarantool::network::AsClient;

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
                Some(line) => writeln!(f, "{}", line)?,
                None => writeln!(f)?,
            }
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

fn sql_repl(args: args::Connect) -> Result<(), ReplError> {
    // setup credentials and options for the connection
    let credentials = Credentials::try_from(&args).map_err(ReplError::other)?;
    let timeout = Some(Duration::from_secs(args.timeout));
    let client = credentials
        .connect(&args.address, timeout)
        .map_err(ReplError::other)?;

    let mut console = Console::new()?;
    let username = args.address.user.unwrap_or(args.user);
    console.greet(&format!(
        "Connected to interactive console by address \"{}:{}\" under \"{}\" user",
        args.address.host, args.address.port, username,
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
