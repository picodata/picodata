use std::{
    borrow::Cow,
    error::Error,
    ffi::{CStr, CString},
    fmt::Display,
};
use structopt::StructOpt;
use tarantool::tlua::{self, c_str};

#[derive(Debug, StructOpt)]
#[structopt(name = "picodata", version = env!("CARGO_PKG_VERSION"))]
pub enum Picodata {
    Run(Run),
    Tarantool(Tarantool),
    Test(Test),
}

////////////////////////////////////////////////////////////////////////////////
// Run
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, StructOpt, tlua::Push)]
#[structopt(about = "Run the picodata instance")]
pub struct Run {
    #[structopt(long, value_name = "name", env = "PICODATA_CLUSTER_ID")]
    /// Name of the cluster
    pub cluster_id: Option<String>,

    #[structopt(
        long,
        value_name = "path",
        default_value = ".",
        env = "PICODATA_DATA_DIR"
    )]
    /// Here the instance persists all of its data
    pub data_dir: String,

    #[structopt(
        short = "e",
        long = "tarantool-exec",
        value_name = "expr",
        parse(try_from_str = CString::new),
    )]
    /// Execute tarantool (Lua) script
    pub tarantool_exec: Option<CString>,

    #[structopt(long, value_name = "name", env = "PICODATA_INSTANCE_ID")]
    /// Name of the instance
    pub instance_id: String,

    #[structopt(
        long,
        value_name = "host[:port]",
        env = "PICODATA_ADVERTISE_ADDRESS",
        parse(try_from_str = try_parse_address)
    )]
    /// Address the other instances should use to connect to this instance
    pub advertise_address: Option<String>,

    #[structopt(
        short = "l",
        long = "listen",
        value_name = "host[:port]",
        parse(try_from_str = try_parse_address),
        default_value = "localhost:3301",
        env = "PICODATA_LISTEN"
    )]
    /// Socket bind address
    pub listen: String,

    #[structopt(
        long = "peer",
        value_name = "host[:port]",
        require_delimiter = true,
        required = true,
        env = "PICODATA_PEER"
    )]
    /// Address of other instance(s)
    pub peers: Vec<String>,

    #[structopt(hidden = true, env = "PICODATA_RAFT_ID")]
    pub raft_id: Option<u64>,

    #[structopt(long, value_name = "name", env = "PICODATA_REPLICASET_ID")]
    /// Name of the replicaset
    pub replicaset_id: Option<String>,

    #[structopt(
        hidden = true,
        long = "picolib-autorun",
        env = "PICOLIB_AUTORUN",
        parse(from_str = parse_flag),
        default_value = "true",
    )]
    /// Only used for testing. Should probably be removed at this point
    pub autorun: bool,
}

impl Run {
    #[allow(dead_code)]
    /// Returns argument matches as if the `argv` is empty
    pub fn from_env() -> Self {
        let matches = Self::clap().get_matches_from::<_, String>([]);
        Self::from_clap(&matches)
    }

    /// Get the arguments that will be passed to `tarantool_main`
    pub fn tt_args(&self) -> Result<Vec<CString>, String> {
        let mut res = vec![current_exe()?];

        if let Some(script) = &self.tarantool_exec {
            res.extend([c_str!("-e").into(), script.clone()]);
        }

        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tarantool
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, StructOpt, tlua::Push)]
#[structopt(about = "Run tarantool")]
pub struct Tarantool {
    #[structopt(raw = true, parse(try_from_str = CString::new))]
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

#[derive(Debug, StructOpt, tlua::Push)]
#[structopt(about = "Run picodata integration tests")]
pub struct Test {
    #[structopt(flatten)]
    pub run: Run,
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

fn parse_flag(text: &str) -> bool {
    let text = text.to_lowercase();
    matches!(text.as_str(), "on" | "true" | "yes") || text.parse::<i64>().unwrap_or(0) != 0
}

#[derive(Debug)]
pub struct BadAddress(String);

impl Error for BadAddress {}

impl Display for BadAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bad address: {}", self.0)
    }
}

fn try_parse_address(text: &str) -> Result<String, Box<dyn Error>> {
    let (host, port) = match text.rsplit_once(":") {
        Some((mut host, port)) => {
            if host.is_empty() {
                host = "localhost";
            }
            if host.contains(':') {
                return Err(Box::new(BadAddress(
                    r#"contains more than 1 ":" symbol"#.into(),
                )));
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
}
