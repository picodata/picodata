use std::fmt::Display;
use std::str::FromStr;

use tarantool::network::{AsClient, Client, Config};

use crate::tarantool_main;
use crate::util::{prompt_password, unwrap_or_terminate};

use super::args::{self, Address, DEFAULT_USERNAME};
use super::console::{Console, ReplError};
use comfy_table::{ContentArrangement, Table};
use serde::{Deserialize, Serialize};

fn get_password_from_file(path: &str) -> Result<String, String> {
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

#[derive(Serialize, Deserialize, Debug)]
struct ColumnDesc {
    name: String,
    #[serde(rename = "type")]
    ty: String,
}

impl Display for ColumnDesc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct RowSet {
    metadata: Vec<ColumnDesc>,
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

#[derive(Serialize, Deserialize, Debug)]
struct RowCount {
    row_count: usize,
}

impl Display for RowCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.row_count))
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum ResultSet {
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
        }
    }
}

fn sql_repl(args: args::Connect) -> Result<(), ReplError> {
    let address = Address::from_str(&args.address).map_err(ReplError::Other)?;

    let user = address.user.as_ref().unwrap_or(&args.user).clone();

    let password = if user == DEFAULT_USERNAME {
        String::new()
    } else if let Some(path) = args.password_file {
        get_password_from_file(&path).map_err(ReplError::Other)?
    } else {
        let prompt = format!("Enter password for {user}: ");
        prompt_password(&prompt)
            .map_err(|err| ReplError::Other(format!("Failed to prompt for a password: {err}")))?
    };

    let mut config = Config::default();
    config.creds = Some((user, password));
    config.auth_method = args.auth_method;

    let client = ::tarantool::fiber::block_on(Client::connect_with_config(
        &address.host,
        address.port.parse().unwrap(),
        config,
    ))?;

    // Check if connection is valid. We need to do it because connect is lazy
    // and we want to check whether authentication have succeeded or not
    ::tarantool::fiber::block_on(client.call("box.schema.user.info", &()))?;

    let mut console = Console::new("picosql :) ")?;

    while let Some(line) = console.read()? {
        let response = ::tarantool::fiber::block_on(client.call("pico.sql", &(line,)))?;

        let res: ResultSet = response.decode().map_err(|err| {
            ReplError::Other(format!("error occured while processing output: {}", err))
        })?;

        console.write(&res.to_string());
    }

    Ok(())
}

pub fn main(args: args::Connect) -> ! {
    let rc = tarantool_main!(
        args.tt_args().unwrap(),
        callback_data: (args,),
        callback_data_type: (args::Connect,),
        callback_body: {
            unwrap_or_terminate(sql_repl(args));
            std::process::exit(0)
        }
    );
    std::process::exit(rc);
}
