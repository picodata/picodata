use clap::Parser;
use std::{
    borrow::Cow,
    ffi::{CStr, CString},
};
use tarantool::log::SayLevel;
use tarantool::tlua::{self, c_str};
use thiserror::Error;

#[derive(Debug, Parser)]
#[clap(name = "picodata", version = env!("CARGO_PKG_VERSION"))]
pub enum Picodata {
    Run(Run),
    Tarantool(Tarantool),
    Test(Test),
}

////////////////////////////////////////////////////////////////////////////////
// Run
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser, tlua::Push, PartialEq)]
#[clap(about = "Run the picodata instance")]
pub struct Run {
    #[clap(long, value_name = "name", env = "PICODATA_CLUSTER_ID")]
    /// Name of the cluster
    pub cluster_id: Option<String>,

    #[clap(
        long,
        value_name = "path",
        default_value = ".",
        env = "PICODATA_DATA_DIR"
    )]
    /// Here the instance persists all of its data
    pub data_dir: String,

    #[clap(
        short = 'e',
        long = "tarantool-exec",
        value_name = "expr",
        parse(try_from_str = CString::new),
    )]
    /// Execute tarantool (Lua) script
    pub tarantool_exec: Option<CString>,

    #[clap(long, value_name = "name", env = "PICODATA_INSTANCE_ID")]
    /// Name of the instance
    pub instance_id: String,

    #[clap(
        long,
        value_name = "[host][:port]",
        env = "PICODATA_ADVERTISE_ADDRESS",
        parse(try_from_str = try_parse_address)
    )]
    /// Address the other instances should use to connect to this instance
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
        required = true,
        env = "PICODATA_PEER"
    )]
    /// Address of other instance(s)
    pub peers: Vec<String>,

    #[clap(long, value_name = "name", env = "PICODATA_REPLICASET_ID")]
    /// Name of the replicaset
    pub replicaset_id: Option<String>,

    #[clap(long, arg_enum, default_value = "info", env = "PICODATA_LOG_LEVEL")]
    /// Log level
    log_level: LogLevel,
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
        let mut res = vec![current_exe()?];

        if let Some(script) = &self.tarantool_exec {
            res.extend([c_str!("-e").into(), script.clone()]);
        }

        Ok(res)
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
// Test
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser, tlua::Push)]
#[clap(about = "Run picodata integration tests")]
pub struct Test {
    #[clap(flatten)]
    pub run: Run,
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
        std::env::set_var("PICODATA_PEER", "peer-from-env");
        {
            let parsed = parse![Run,];
            assert_eq!(parsed.instance_id, "instance-id-from-env");
            assert_eq!(parsed.peers.as_ref(), vec!["peer-from-env:3301"]);
            assert_eq!(parsed.listen, "localhost:3301"); // default
            assert_eq!(parsed.advertise_address(), "localhost:3301"); // default
            assert_eq!(parsed.log_level(), SayLevel::Info); // default

            let parsed = parse![Run, "--instance-id", "instance-id-from-args"];
            assert_eq!(parsed.instance_id, "instance-id-from-args");

            let parsed = parse![Run, "--peer", "peer-from-args"];
            assert_eq!(parsed.peers.as_ref(), vec!["peer-from-args:3301"]);
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

        std::env::set_var("PICODATA_ADVERTISE_ADDRESS", "advertise-from-env");
        {
            let parsed = parse![Run,];
            assert_eq!(parsed.listen, "listen-from-env:3301");
            assert_eq!(parsed.advertise_address(), "advertise-from-env:3301");

            let parsed = parse![Run, "-l", "listen-from-args"];
            assert_eq!(parsed.listen, "listen-from-args:3301");
            assert_eq!(parsed.advertise_address(), "advertise-from-env:3301");

            let parsed = parse![Run, "--advertise-address", "advertise-from-args"];
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
    }
}
