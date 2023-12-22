use std::fmt::Display;
use std::ops::ControlFlow;
use std::path::Path;
use std::str::FromStr;
use std::{env, fs, io, process};

use comfy_table::{ContentArrangement, Table};

use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use tarantool::network::{client, AsClient, Client, Config};

use crate::tarantool_main;
use crate::util::unwrap_or_terminate;

use super::args::{self, Address, DEFAULT_USERNAME};

pub(crate) fn get_password_from_file(path: &str) -> Result<String, String> {
    let content = std::fs::read_to_string(path).map_err(|e| {
        format!(r#"can't read password from password file by "{path}", reason: {e}"#)
    })?;

    let password = content
        .lines()
        .next()
        .ok_or("Empty password file".to_string())?
        .trim();

    if password.is_empty() {
        return Ok(String::new());
    }

    Ok(password.into())
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct ColDesc {
    name: String,
    #[serde(rename = "type")]
    ty: String,
}

impl Display for ColDesc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct RowSet {
    metadata: Vec<ColDesc>,
    rows: Vec<Vec<rmpv::Value>>,
}

impl Display for RowSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut table = Table::new();
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(self.metadata.iter());

        for row in &self.rows {
            table.add_row(row);
        }

        f.write_fmt(format_args!("{table}\n"))?;
        f.write_fmt(format_args!("({} rows)", self.rows.len()))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct RowCount {
    row_count: usize,
}

impl Display for RowCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.row_count))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(untagged)]
enum ResultSet {
    RowSet(Vec<RowSet>),
    RowCount(Vec<RowCount>),
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
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum SqlReplError {
    #[error("{0}")]
    Client(#[from] client::Error),

    #[error("Failed to prompt for a password: {0}")]
    Prompt(io::Error),

    #[error("{0}")]
    Io(io::Error),
}

const HISTORY_FILE_NAME: &str = ".picodata_history";

async fn sql_repl_main(args: args::Connect) {
    unwrap_or_terminate(sql_repl(args).await);
    std::process::exit(0)
}

// Ideally we should have an enum for all commands. For now we have only two options, usual line
// and only one special command. To not overengineer things at this point just handle this as ifs.
// When the set of commands grows it makes total sense to transform this to clear parse/execute pipeline
// and separate enum variants for each command variant.
fn handle_special_sequence(line: &str) -> Result<ControlFlow<String>, SqlReplError> {
    if line != "\\e" {
        eprintln!("Unknown special sequence");
        return Ok(ControlFlow::Continue(()));
    }

    let editor = match env::var_os("EDITOR") {
        Some(e) => e,
        None => {
            eprintln!("EDITOR environment variable is not set");
            return Ok(ControlFlow::Continue(()));
        }
    };

    let temp = tempfile::Builder::new()
        .suffix(".sql")
        .tempfile()
        .map_err(SqlReplError::Io)?;
    let status = process::Command::new(&editor)
        .arg(temp.path())
        .status()
        .map_err(SqlReplError::Io)?;

    if !status.success() {
        eprintln!("{:?} returned non zero exit status: {}", editor, status);
        return Ok(ControlFlow::Continue(()));
    }

    let line = fs::read_to_string(temp.path()).map_err(SqlReplError::Io)?;
    Ok(ControlFlow::Break(line))
}

async fn sql_repl(args: args::Connect) -> Result<(), SqlReplError> {
    let address = unwrap_or_terminate(Address::from_str(&args.address));
    let user = address.user.as_ref().unwrap_or(&args.user).clone();

    let password = if user == DEFAULT_USERNAME {
        String::new()
    } else if let Some(path) = args.password_file {
        unwrap_or_terminate(get_password_from_file(&path))
    } else {
        let prompt = format!("Enter password for {user}: ");
        match crate::util::prompt_password(&prompt) {
            Ok(password) => password,
            Err(e) => {
                return Err(SqlReplError::Prompt(e));
            }
        }
    };

    let client = Client::connect_with_config(
        &address.host,
        address.port.parse().unwrap(),
        Config {
            creds: Some((user, password)),
        },
    )
    .await?;

    // Check if connection is valid. We need to do it because connect is lazy
    // and we want to check whether authentication have succeeded or not
    client.call("box.schema.user.info", &()).await?;

    // It is deprecated because of unexpected behavior on windows.
    // We're ok with that.
    #[allow(deprecated)]
    let history_file = env::home_dir()
        .unwrap_or_default()
        .join(Path::new(HISTORY_FILE_NAME));

    let mut rl = DefaultEditor::new().unwrap();
    rl.load_history(&history_file).ok();

    loop {
        let readline = rl.readline("picosql :) ");
        match readline {
            Ok(line) => {
                let line = {
                    if line.starts_with('\\') {
                        match handle_special_sequence(&line)? {
                            ControlFlow::Continue(_) => continue,
                            ControlFlow::Break(line) => line,
                        }
                    } else {
                        line
                    }
                };

                if line.is_empty() {
                    continue;
                }

                let response = client.call("pico.sql", &(&line,)).await?;
                let res: ResultSet = response
                    .decode()
                    .expect("Response must have the shape of ResultSet structure");
                println!("{res}");

                rl.add_history_entry(line.as_str()).ok();
                rl.save_history(&history_file).ok();
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}

pub fn main(args: args::Connect) -> ! {
    let rc = tarantool_main!(
        args.tt_args().unwrap(),
        callback_data: (args,),
        callback_data_type: (args::Connect,),
        callback_body: {
            ::tarantool::fiber::block_on(sql_repl_main(args))
        }
    );
    std::process::exit(rc);
}
