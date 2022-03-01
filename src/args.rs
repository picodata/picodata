use crate::traft::row::Peer;
use std::{
    borrow::Cow,
    ffi::{CStr, CString},
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
    pub instance_id: Option<String>,

    #[structopt(
        short = "l",
        long = "listen",
        value_name = "[host:]port",
        default_value = "3301",
        env = "PICODATA_LISTEN"
    )]
    /// Socket bind address
    pub listen: String,

    #[structopt(
        long = "peer",
        value_name = "host:port",
        parse(from_str = parse_peer),
        default_value = "127.0.0.1:3301",
        require_delimiter = true,
        env = "PICODATA_PEER",
    )]
    /// Address of other instance(s)
    pub peers: Vec<Peer>,

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

fn parse_peer(text: &str) -> Peer {
    static mut CURRENT_RAFT_ID: u64 = 0;

    unsafe { CURRENT_RAFT_ID += 1 }

    Peer {
        raft_id: unsafe { CURRENT_RAFT_ID },
        uri: text.into(),
    }
}

fn parse_flag(text: &str) -> bool {
    let text = text.to_lowercase();
    matches!(text.as_str(), "on" | "true" | "yes") || text.parse::<i64>().unwrap_or(0) != 0
}
