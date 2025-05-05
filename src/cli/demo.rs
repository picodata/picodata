use crate::cli::args::Demo;
use crate::config::DEFAULT_LISTEN_HOST;

use std::collections::BTreeMap;
use std::env;
use std::io::{Error as IoError, Write};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};

use pike::cluster as builder;
use pike::cluster::{PicodataInstance, PicodataInstanceProperties};

pub(crate) const DEFAULT_ADMIN_NAME: &'static str = "admin";
pub(crate) const DEFAULT_ADMIN_PASS: &'static str = "T0psecret";
pub(crate) const DEFAULT_DIRECTORY: &'static str = ".pico";

const BASIC_WELCOME_MESSAGE: &'static str = r#"

    *************     *********
    ************    *************
    **********    ****************
    *********    ******************
               ***
    *************    **************
    ***********    ****************
    **********   ******************
                ***
    *************     *************
    ************    **************
    **********    ***************
    ********     **************
               ***
    *************      Welcome to Picodata from PostgreSQL interactive console!
    ***********        Try to `SELECT` something from the `warehouse` table :)
    **********

"#;

fn fmt_io_err(err: IoError) -> String {
    err.to_string()
}

fn start_cluster() -> Result<Vec<PicodataInstance>, String> {
    let mut tiers = BTreeMap::new();
    tiers.insert(
        "default".to_owned(),
        builder::Tier {
            replicasets: 2,
            replication_factor: 2,
        },
    );
    let topology = builder::Topology {
        tiers,
        ..Default::default()
    };

    let data_directory = env::current_dir()
        .map_err(fmt_io_err)?
        .join(DEFAULT_DIRECTORY);
    // TODO: even though we clear data directory from previous
    // run, we ignore the fact that previous run didn't kill
    // or stopped the whole cluster. We need to kill all ran
    // instances after the user exited from `psql` console.
    if data_directory.exists() {
        std::fs::remove_dir_all(&data_directory).map_err(fmt_io_err)?;
    }

    let parameters = builder::RunParamsBuilder::default()
        .topology(topology)
        .data_dir(data_directory)
        .build()
        .expect("topology and data directory should be initialized");

    env::set_var("PICODATA_ADMIN_PASSWORD", DEFAULT_ADMIN_PASS);

    Ok(builder::run(&parameters)
        .expect("running a new cluster with correct parameters should be fine"))
}

fn fill_with_data(instance_properties: &PicodataInstanceProperties) -> Result<(), String> {
    let mut command = Command::new("psql");

    let argument = format!(
        "user={} host={} port={} password={} sslmode=disable",
        DEFAULT_ADMIN_NAME, DEFAULT_LISTEN_HOST, instance_properties.pg_port, DEFAULT_ADMIN_PASS
    );
    command.arg(argument);

    let queries = r#"
        CREATE TABLE warehouse (
            id INTEGER NOT NULL,
            item TEXT NOT NULL,
            type TEXT NOT NULL,
            PRIMARY KEY (id))
        USING memtx DISTRIBUTED BY (id)
        OPTION (TIMEOUT = 3.0);

        INSERT INTO warehouse VALUES
            (1, 'bricks', 'heavy'),
            (2, 'bars', 'light'),
            (3, 'blocks', 'heavy'),
            (4, 'piles', 'light'),
            (5, 'panels', 'light');
    "#
    .as_bytes();

    let child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
    match child {
        Ok(mut child) => {
            child
                .stdin
                .as_mut()
                .expect("`stdin` should be piped on command creation")
                .write_all(queries)
                .expect("writing queries to `psql` console should be fine");
            Ok(())
        }
        Err(error) => Err(format!("failed to execute `psql`: {error}")),
    }
}

fn start_psql(cluster: &[PicodataInstance]) -> Result<(), String> {
    let instance = cluster
        .first()
        .expect("deployed cluster should have at least one instance");
    let properties = instance.properties();

    fill_with_data(&properties)?;

    let mut command = Command::new("psql");
    let argument = format!(
        "user={} host={} port={} password={} sslmode=disable",
        DEFAULT_ADMIN_NAME, DEFAULT_LISTEN_HOST, properties.pg_port, DEFAULT_ADMIN_PASS
    );
    command.arg(argument);

    println!("{BASIC_WELCOME_MESSAGE}");

    let error = command.exec();
    Err(format!("failed to execute `psql`: {error}"))
}

/////////////////////////////////////////////////////////////////////
// main
/////////////////////////////////////////////////////////////////////

pub fn main(args: Demo) -> ! {
    if let Err(error) = main_impl(args) {
        eprintln!("{error}");
        std::process::exit(1)
    }

    std::process::exit(0)
}

fn main_impl(args: Demo) -> Result<(), Box<dyn std::error::Error>> {
    match args {
        Demo::Simple => {
            let cluster = start_cluster()?;
            start_psql(&cluster)?;
        }
    }

    Ok(())
}
