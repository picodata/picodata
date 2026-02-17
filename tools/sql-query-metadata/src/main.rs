use std::{
    io::{self, Read},
    str::FromStr,
};

use clap::Parser;
use postgres::{
    Config, NoTls, Statement,
    error::DbError,
    types::{Oid, Type},
};

/// Print parameter and distribution key metadata
#[derive(Parser)]
struct Args {
    /// Postgres connection string
    connection_string: String,

    /// If specified, types are printed as raw oids
    #[arg(short = 'r', long = "raw")]
    raw_oids: bool,
}

fn create_notice_callback(raw_oids: bool) -> impl Fn(DbError) {
    #[derive(serde::Deserialize)]
    #[serde(tag = "type")]
    #[allow(unused)]
    struct PreparedStatementMetadata {
        query: String,
        tier: Option<String>,
        dk_meta: Vec<(u16, Oid)>,
    }

    move |notice: DbError| {
        let metadata: PreparedStatementMetadata =
            serde_json::from_str(notice.message()).expect("JSON must be correct");

        let param_vec: Vec<_> = if raw_oids {
            metadata
                .dk_meta
                .into_iter()
                .map(|(idx, oid)| format!("${idx}::{oid}"))
                .collect()
        } else {
            metadata
                .dk_meta
                .into_iter()
                .map(|(idx, oid)| format!("${idx}::{}", Type::from_oid(oid).expect("unknown type")))
                .collect()
        };

        println!("DK_META:\n[{}]\n", param_vec.join(", "));
    }
}

fn read_input_query() -> Result<String, Box<dyn std::error::Error>> {
    println!("Enter the query and press CTRL+D:");

    let mut buf = String::new();
    io::stdin().read_to_string(&mut buf)?;

    Ok(buf)
}

fn collect_parameter_info(stmt: &Statement, raw_oids: bool) -> String {
    let mut pt_meta = Vec::new();

    if raw_oids {
        for (index, param_type) in stmt.params().iter().enumerate() {
            pt_meta.push(format!("${}::{}", index + 1, param_type.oid()));
        }
    } else {
        for (index, param_type) in stmt.params().iter().enumerate() {
            pt_meta.push(format!("${}::{}", index + 1, param_type));
        }
    }

    pt_meta.join(", ")
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let notice_callback = create_notice_callback(args.raw_oids);
    let mut client = Config::from_str(&args.connection_string)?
        .notice_callback(notice_callback)
        .connect(NoTls)?;

    let query = read_input_query()?;
    println!("\nQUERY:\n{}\n", query.trim());

    let stmt = client.prepare(&query)?;
    let pt_meta = collect_parameter_info(&stmt, args.raw_oids);

    println!("PT_META:\n[{pt_meta}]\n");

    Ok(())
}
