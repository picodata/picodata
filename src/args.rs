use clap::Parser;
use std::{
    borrow::Cow,
    ffi::{CStr, CString},
};
use tarantool::log::SayLevel;
use tarantool::tlua;
use thiserror::Error;

use crate::failure_domain::FailureDomain;
use crate::instance::InstanceId;
use crate::replicaset::ReplicasetId;
use crate::util::Uppercase;

#[derive(Debug, Parser)]
#[clap(name = "picodata", version = "23.06.0")]
pub enum Picodata {
    Run(Run),
    Tarantool(Tarantool),
    Expel(Expel),
    Test(Test),
    Connect(Connect),
}

////////////////////////////////////////////////////////////////////////////////
// Run
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser, tlua::Push, PartialEq)]
#[clap(about = "Run the picodata instance")]
pub struct Run {
    #[clap(
        long,
        value_name = "name",
        default_value = "demo",
        env = "PICODATA_CLUSTER_ID"
    )]
    /// Name of the cluster. The instance will refuse
    /// to join a cluster with a different name.
    pub cluster_id: String,

    #[clap(
        long,
        value_name = "path",
        default_value = ".",
        env = "PICODATA_DATA_DIR",
        parse(try_from_str = try_parse_path)
    )]
    /// Here the instance persists all of its data
    pub data_dir: String,

    #[clap(long, value_name = "name", env = "PICODATA_INSTANCE_ID")]
    /// Name of the instance.
    /// If not defined, it'll be generated automatically.
    pub instance_id: Option<InstanceId>,

    #[clap(
        long = "advertise",
        value_name = "[host][:port]",
        env = "PICODATA_ADVERTISE",
        parse(try_from_str = try_parse_address)
    )]
    /// Address the other instances should use to connect to this instance.
    /// Defaults to `--listen` value.
    pub advertise_address: Option<String>,

    #[clap(
        short = 'l',
        long = "listen",
        value_name = "[host][:port]",
        parse(try_from_str = try_parse_address),
        default_value = "localhost:3301",
        env = "PICODATA_LISTEN"
    )]
    /// Socket bind address
    pub listen: String,

    #[clap(
        long = "peer",
        value_name = "[host][:port]",
        require_value_delimiter = true,
        use_value_delimiter = true,
        parse(try_from_str = try_parse_address),
        default_value = "localhost:3301",
        env = "PICODATA_PEER"
    )]
    /// Address(es) of other instance(s)
    pub peers: Vec<String>,

    #[clap(
        long = "failure-domain",
        value_name = "key=value",
        require_value_delimiter = true,
        use_value_delimiter = true,
        parse(try_from_str = try_parse_kv_uppercase),
        env = "PICODATA_FAILURE_DOMAIN"
    )]
    /// Comma-separated list describing physical location of the server.
    /// Each domain is a key-value pair.
    /// Picodata will avoid putting two instances into the same
    /// replicaset if at least one key of their failure domains has the
    /// same value. Instead, new replicasets will be created.
    /// Replicasets will be populated with instances from different
    /// failure domains until the desired replication factor is reached.
    pub failure_domain: Vec<(Uppercase, Uppercase)>,

    #[clap(long, value_name = "name", env = "PICODATA_REPLICASET_ID")]
    /// Name of the replicaset
    pub replicaset_id: Option<ReplicasetId>,

    #[clap(long, arg_enum, default_value = "info", env = "PICODATA_LOG_LEVEL")]
    /// Log level
    log_level: LogLevel,

    #[clap(long, default_value = "1", env = "PICODATA_INIT_REPLICATION_FACTOR")]
    /// Total number of replicas (copies of data) for each replicaset in
    /// the cluster. It's only accounted upon the cluster initialization
    /// (when the first instance bootstraps), and ignored aftwerwards.
    pub init_replication_factor: u8,

    #[clap(
        long,
        value_name = "path",
        env = "PICODATA_SCRIPT",
        parse(try_from_str = try_parse_path)
    )]
    /// A path to a lua script that will be executed at postjoin stage
    pub script: Option<String>,

    #[clap(
        long,
        value_name = "[host][:port]",
        env = "PICODATA_HTTP_LISTEN",
        parse(try_from_str = try_parse_address)
    )]
    /// Address to start the HTTP server on. The routing API is exposed
    /// in Lua as `_G.pico.httpd` variable. If not specified, it won't
    /// be initialized.
    pub http_listen: Option<String>,
}

// Copy enum because clap:ArgEnum can't be derived for the foreign SayLevel.
#[derive(Debug, Copy, Clone, tlua::Push, PartialEq, clap::ArgEnum)]
#[clap(rename_all = "lower")]
enum LogLevel {
    Fatal,
    System,
    Error,
    Crit,
    Warn,
    Info,
    Verbose,
    Debug,
}

impl From<LogLevel> for SayLevel {
    fn from(l: LogLevel) -> SayLevel {
        match l {
            LogLevel::Fatal => SayLevel::Fatal,
            LogLevel::System => SayLevel::System,
            LogLevel::Error => SayLevel::Error,
            LogLevel::Crit => SayLevel::Crit,
            LogLevel::Warn => SayLevel::Warn,
            LogLevel::Info => SayLevel::Info,
            LogLevel::Verbose => SayLevel::Verbose,
            LogLevel::Debug => SayLevel::Debug,
        }
    }
}

impl Run {
    /// Get the arguments that will be passed to `tarantool_main`
    pub fn tt_args(&self) -> Result<Vec<CString>, String> {
        Ok(vec![current_exe()?])
    }

    pub fn advertise_address(&self) -> String {
        match &self.advertise_address {
            Some(v) => v.clone(),
            None => self.listen.clone(),
        }
    }

    pub fn log_level(&self) -> SayLevel {
        self.log_level.into()
    }

    pub fn failure_domain(&self) -> FailureDomain {
        FailureDomain::from(
            self.failure_domain
                .iter()
                .map(|(k, v)| (k.clone(), v.clone())),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tarantool
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser, tlua::Push)]
#[clap(about = "Run tarantool")]
pub struct Tarantool {
    #[clap(raw = true, parse(try_from_str = CString::new))]
    pub args: Vec<CString>,
}

impl Tarantool {
    /// Get the arguments that will be passed to `tarantool_main`
    pub fn tt_args(&self) -> Result<Vec<Cow<CStr>>, String> {
        Ok(std::iter::once(current_exe()?.into())
            .chain(self.args.iter().map(AsRef::as_ref).map(Cow::from))
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Expel
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser, tlua::Push)]
#[clap(about = "Expel node from cluster")]
pub struct Expel {
    #[clap(long, value_name = "name", default_value = "demo")]
    /// Name of the cluster from instance should be expelled.
    pub cluster_id: String,

    #[clap(long, value_name = "name", default_value = "")]
    /// Name of the instance to expel.
    pub instance_id: InstanceId,

    #[clap(
        long = "peer",
        value_name = "[host][:port]",
        parse(try_from_str = try_parse_address),
        default_value = "localhost:3301",
    )]
    /// Address of any instance from the cluster.
    pub peer_address: String,
}

impl Expel {
    // Get the arguments that will be passed to `tarantool_main`
    pub fn tt_args(&self) -> Result<Vec<CString>, String> {
        Ok(vec![current_exe()?])
    }
}

////////////////////////////////////////////////////////////////////////////////
// Test
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser, tlua::Push)]
#[clap(about = "Run picodata integration tests")]
pub struct Test {
    #[clap(env = "PICODATA_TEST_FILTER")]
    /// Only run tests matching the filter.
    pub filter: Option<String>,

    #[clap(long = "nocapture", env = "PICODATA_TEST_NOCAPTURE")]
    /// Do not capture test output.
    pub nocapture: bool,
}

impl Test {
    pub fn tt_args(&self) -> Result<Vec<CString>, String> {
        Ok(vec![current_exe()?])
    }
}

////////////////////////////////////////////////////////////////////////////////
// fns
////////////////////////////////////////////////////////////////////////////////

fn current_exe() -> Result<CString, String> {
    CString::new(
        std::env::current_exe()
            .map_err(|e| format!("Failed getting current executable path: {e}"))?
            .display()
            .to_string(),
    )
    .map_err(|e| format!("Current executable path contains nul bytes: {e}"))
}

#[derive(Error, Debug)]
pub enum ParseAddressError {
    #[error("bad address: {0}")]
    BadAddress(&'static str),

    #[error("bad port: {0}")]
    BadPort(#[from] std::num::ParseIntError),
}

fn try_parse_address(text: &str) -> Result<String, ParseAddressError> {
    let (host, port) = match text.rsplit_once(':') {
        Some((mut host, port)) => {
            if host.is_empty() {
                host = "localhost";
            }
            if host.contains(':') {
                return Err(ParseAddressError::BadAddress(
                    r#"contains more than 1 ":" symbol"#,
                ));
            }
            (host, port.parse::<u16>()?)
        }
        None => (text, 3301),
    };
    Ok(format!("{host}:{port}"))
}

#[derive(Error, Debug)]
pub enum ParsePathError {
    #[error("empty path is not supported, use \".\" for current dir")]
    EmptyPath,
}

fn try_parse_path(text: &str) -> Result<String, ParsePathError> {
    match text {
        "" => Err(ParsePathError::EmptyPath),
        _ => Ok(text.to_string()),
    }
}

/// Parses a '=' sepparated string of key and value and converts both to
/// uppercase.
fn try_parse_kv_uppercase(s: &str) -> Result<(Uppercase, Uppercase), String> {
    let (key, value) = s
        .split_once('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{}`", s))?;
    Ok((key.into(), value.into()))
}

////////////////////////////////////////////////////////////////////////////////
// Picodata cli
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser)]
#[clap(about = "Сonnect a Picodata instance and start interactive Lua console")]
pub struct Connect {
    #[clap(
        short = 'u',
        long = "user",
        value_name = "user",
        default_value = "guest",
        env = "PICODATA_USER"
    )]
    /// The username to connect with
    pub user: String,

    /// Picodata instance address
    #[clap(parse(try_from_str = try_parse_address),)]
    pub address: String,
}

impl Connect {
    /// Get the arguments that will be passed to `tarantool_main`
    pub fn tt_args(&self) -> Result<Vec<CString>, String> {
        Ok(vec![current_exe()?])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_parse_address() {
        assert_eq!(try_parse_address("1234").unwrap(), "1234:3301");
        assert_eq!(try_parse_address(":1234").unwrap(), "localhost:1234");
        assert_eq!(try_parse_address("example").unwrap(), "example:3301");
        assert_eq!(
            try_parse_address("localhost:1234").unwrap(),
            "localhost:1234"
        );
        assert_eq!(try_parse_address("1.2.3.4:1234").unwrap(), "1.2.3.4:1234");
        assert_eq!(try_parse_address("example:1234").unwrap(), "example:1234");
        assert!(try_parse_address("example:123456").is_err());
        assert!(try_parse_address("example::1234").is_err());
    }

    #[test]
    fn test_parse_path() {
        assert!(try_parse_path("").is_err());
    }

    macro_rules! parse {
        ($subcmd:ty, $($arg:literal),*) => {{
            let args = vec![stringify!($subcmd), $($arg),*];
            <$subcmd>::try_parse_from(args).unwrap()
        }}
    }

    struct EnvDump(Vec<(String, String)>);
    impl EnvDump {
        fn new() -> Self {
            let dump: Vec<(String, String)> = std::env::vars()
                .filter(|(k, _)| k.starts_with("PICODATA_"))
                .collect();
            for (k, _) in &dump {
                std::env::remove_var(k);
            }
            Self(dump)
        }
    }

    impl Drop for EnvDump {
        fn drop(&mut self) {
            for (k, v) in self.0.drain(..) {
                std::env::set_var(k, v);
            }
        }
    }

    #[test]
    fn test_parse() {
        let _env_dump = EnvDump::new();

        std::env::set_var("PICODATA_INSTANCE_ID", "instance-id-from-env");
        {
            let parsed = parse![Run,];
            assert_eq!(parsed.instance_id, Some("instance-id-from-env".into()));
            assert_eq!(parsed.peers.as_ref(), vec!["localhost:3301"]);
            assert_eq!(parsed.listen, "localhost:3301"); // default
            assert_eq!(parsed.advertise_address(), "localhost:3301"); // default
            assert_eq!(parsed.log_level(), SayLevel::Info); // default
            assert_eq!(parsed.failure_domain(), FailureDomain::default()); // default

            let parsed = parse![Run, "--instance-id", "instance-id-from-args"];
            assert_eq!(parsed.instance_id, Some("instance-id-from-args".into()));

            let parsed = parse![Run, "--instance-id", ""];
            assert_eq!(parsed.instance_id, Some("".into()));
        }

        std::env::set_var("PICODATA_PEER", "peer-from-env");
        {
            let parsed = parse![Run,];
            assert_eq!(parsed.peers.as_ref(), vec!["peer-from-env:3301"]);

            let parsed = parse![Run, "--peer", "peer-from-args"];
            assert_eq!(parsed.peers.as_ref(), vec!["peer-from-args:3301"]);

            let parsed = parse![Run, "--peer", ":3302"];
            assert_eq!(parsed.peers.as_ref(), vec!["localhost:3302"]);

            let parsed = parse![Run, "--peer", "p1", "--peer", "p2,p3"];
            assert_eq!(parsed.peers.as_ref(), vec!["p1:3301", "p2:3301", "p3:3301"]);
        }

        std::env::set_var("PICODATA_INSTANCE_ID", "");
        {
            let parsed = parse![Run,];
            assert_eq!(parsed.instance_id, Some("".into()));
        }

        std::env::remove_var("PICODATA_INSTANCE_ID");
        {
            let parsed = parse![Run,];
            assert_eq!(parsed.instance_id, None);
        }

        std::env::set_var("PICODATA_LISTEN", "listen-from-env");
        {
            let parsed = parse![Run,];
            assert_eq!(parsed.listen, "listen-from-env:3301");
            assert_eq!(parsed.advertise_address(), "listen-from-env:3301");

            let parsed = parse![Run, "-l", "listen-from-args"];
            assert_eq!(parsed.listen, "listen-from-args:3301");
            assert_eq!(parsed.advertise_address(), "listen-from-args:3301");
        }

        std::env::set_var("PICODATA_ADVERTISE", "advertise-from-env");
        {
            let parsed = parse![Run,];
            assert_eq!(parsed.listen, "listen-from-env:3301");
            assert_eq!(parsed.advertise_address(), "advertise-from-env:3301");

            let parsed = parse![Run, "-l", "listen-from-args"];
            assert_eq!(parsed.listen, "listen-from-args:3301");
            assert_eq!(parsed.advertise_address(), "advertise-from-env:3301");

            let parsed = parse![Run, "--advertise", "advertise-from-args"];
            assert_eq!(parsed.listen, "listen-from-env:3301");
            assert_eq!(parsed.advertise_address(), "advertise-from-args:3301");
        }

        std::env::set_var("PICODATA_LOG_LEVEL", "verbose");
        {
            let parsed = parse![Run,];
            assert_eq!(parsed.log_level(), SayLevel::Verbose);

            let parsed = parse![Run, "--log-level", "warn"];
            assert_eq!(parsed.log_level(), SayLevel::Warn);
        }

        std::env::set_var("PICODATA_FAILURE_DOMAIN", "k1=env1,k2=env2");
        {
            let parsed = parse![Run,];
            assert_eq!(
                parsed.failure_domain(),
                FailureDomain::from([("K1", "ENV1"), ("K2", "ENV2")])
            );

            let parsed = parse![Run, "--failure-domain", "k1=arg1,k1=arg1-again"];
            assert_eq!(
                parsed.failure_domain(),
                FailureDomain::from([("K1", "ARG1-AGAIN")])
            );

            let parsed = parse![
                Run,
                "--failure-domain",
                "k2=arg2",
                "--failure-domain",
                "k3=arg3,k4=arg4"
            ];
            assert_eq!(
                parsed.failure_domain(),
                FailureDomain::from([("K2", "ARG2"), ("K3", "ARG3"), ("K4", "ARG4")])
            );
        }

        {
            let parsed = parse![Run,];
            assert_eq!(parsed.init_replication_factor, 1);

            let parsed = parse![Run, "--init-replication-factor", "7"];
            assert_eq!(parsed.init_replication_factor, 7);

            std::env::set_var("PICODATA_INIT_REPLICATION_FACTOR", "9");
            let parsed = parse![Run,];
            assert_eq!(parsed.init_replication_factor, 9);
        }

        {
            let parsed = parse![Connect, "somewhere:3301"];
            assert_eq!(parsed.address, "somewhere:3301");
            assert_eq!(parsed.user, "guest");

            let parsed = parse![Connect, "somewhere:3301", "--user", "superman"];
            assert_eq!(parsed.user, "superman");

            std::env::set_var("PICODATA_USER", "batman");
            let parsed = parse!(Connect, "somewhere:3301");
            assert_eq!(parsed.user, "batman");
        }
    }
}
