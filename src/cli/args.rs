use crate::address::Address;
use crate::config::DEFAULT_USERNAME;
use crate::instance::InstanceId;
use crate::util::Uppercase;
use clap::Parser;
use std::borrow::Cow;
use std::ffi::{CStr, CString};
use tarantool::auth::AuthMethod;
use tarantool::log::SayLevel;
use tarantool::tlua;

#[derive(Debug, Parser)]
#[clap(name = "picodata", version = env!("GIT_DESCRIBE"))]
pub enum Picodata {
    Run(Box<Run>),
    #[clap(hide = true)]
    Tarantool(Tarantool),
    Expel(Expel),
    Test(Test),
    Connect(Connect),
    Admin(Admin),
}

////////////////////////////////////////////////////////////////////////////////
// Run
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser, tlua::Push, PartialEq)]
#[clap(about = "Run the picodata instance")]
pub struct Run {
    #[clap(long, value_name = "NAME", env = "PICODATA_CLUSTER_ID")]
    /// Name of the cluster. The instance will refuse
    /// to join a cluster with a different name.
    ///
    /// By default this will be "demo".
    pub cluster_id: Option<String>,

    #[clap(long, value_name = "PATH", env = "PICODATA_DATA_DIR")]
    /// Here the instance persists all of its data.
    ///
    /// By default this is the current working directory (".").
    pub data_dir: Option<String>,

    #[clap(long, value_name = "PATH", env = "PICODATA_CONFIG_FILE")]
    /// Path to configuration file in yaml format.
    ///
    /// By default the "config.yaml" in the data directory is used.
    pub config: Option<String>,

    #[clap(long, value_name = "NAME", env = "PICODATA_INSTANCE_ID")]
    /// Name of the instance.
    /// If not defined, it'll be generated automatically.
    pub instance_id: Option<String>,

    #[clap(
        long = "advertise",
        value_name = "[HOST][:PORT]",
        env = "PICODATA_ADVERTISE"
    )]
    /// Address the other instances should use to connect to this instance.
    /// Defaults to `--listen` value.
    pub advertise_address: Option<Address>,

    #[clap(
        short = 'l',
        long = "listen",
        value_name = "[HOST][:PORT]",
        env = "PICODATA_LISTEN"
    )]
    /// Socket bind address.
    ///
    /// By default "localhost:3301" is used.
    pub listen: Option<Address>,

    #[clap(
        long = "peer",
        value_name = "[HOST][:PORT]",
        require_value_delimiter = true,
        use_value_delimiter = true,
        env = "PICODATA_PEER"
    )]
    /// Address(es) of other instance(s)
    ///
    /// By default "localhost:3301" is used.
    pub peers: Vec<Address>,

    #[clap(
        long = "failure-domain",
        value_name = "KEY=VALUE",
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

    #[clap(long, value_name = "NAME", env = "PICODATA_REPLICASET_ID")]
    /// Name of the replicaset.
    ///
    /// If not specified, a replicaset will be automatically chosen based on the
    /// failure domain settings.
    pub replicaset_id: Option<String>,

    #[clap(long, arg_enum, env = "PICODATA_LOG_LEVEL")]
    /// Log level.
    ///
    /// By default "info" is used.
    pub log_level: Option<LogLevel>,

    #[clap(long, env = "PICODATA_INIT_REPLICATION_FACTOR", group = "init_cfg")]
    /// Total number of replicas (copies of data) for each replicaset.
    /// It makes sense only when starting cluster without --init-cfg option.
    ///
    /// By default 1 is used.
    pub init_replication_factor: Option<u8>,

    #[clap(long, value_name = "PATH", env = "PICODATA_SCRIPT")]
    /// A path to a lua script that will be executed at postjoin stage.
    /// At the moment the script is executed, the local storage is
    /// already initialized and HTTP server is running (if specified).
    /// But the raft node is uninitialized yet.
    pub script: Option<String>,

    #[clap(long, value_name = "[HOST][:PORT]", env = "PICODATA_HTTP_LISTEN")]
    /// Address to start the HTTP server on. The routing API is exposed
    /// in Lua as `_G.pico.httpd` variable. If not specified, it won't
    /// be initialized.
    pub http_listen: Option<Address>,

    #[clap(short = 'i', long = "interactive", env = "PICODATA_INTERACTIVE_MODE")]
    /// Enable interactive console. Deprecated in 24.1.
    pub interactive_mode: bool,

    #[clap(long, value_name = "PATH", env = "PICODATA_ADMIN_SOCK")]
    /// Unix socket for the interactive console to connect using
    /// `picodata admin`. Unlike connecting via `picodata connect`
    /// console communication occurs in plain text
    /// and always operates under the admin account.
    ///
    /// By default the "admin.sock" in the data directory is used.
    // TODO: rename to admin_socket
    pub admin_sock: Option<String>,

    #[clap(
        long,
        value_name = "PATH",
        env = "PICODATA_PLUGINS",
        require_value_delimiter = true,
        use_value_delimiter = true
    )]
    /// Path to `some_plugin_name.so`
    pub plugins: Vec<String>,

    #[clap(long = "tier", value_name = "TIER", env = "PICODATA_INSTANCE_TIER")]
    /// Name of the tier to which the instance will belong.
    ///
    /// By default "default" is used.
    pub tier: Option<String>,

    /// Filepath to configuration file in yaml format.
    #[clap(
        long = "init-cfg",
        value_name = "PATH",
        env = "PICODATA_INIT_CFG",
        group = "init_cfg"
    )]
    pub init_cfg: Option<String>,

    #[clap(long = "audit", value_name = "PATH", env = "PICODATA_AUDIT_LOG")]
    /// Configuration for the audit log.
    /// Valid options:
    ///
    /// 1. `file:<file>` or simply `<file>` — write to a file, e.g:
    ///
    ///    picodata run --audit '/tmp/audit.log'
    ///
    /// 2. `pipe:<command>` or `| <command>` — redirect to a subprocess, e.g:
    ///
    ///    picodata run --audit '| /bin/capture-from-stdin'
    ///
    /// 3. `syslog:` — write to the syslog, e.g:
    ///
    ///    picodata run --audit 'syslog:'
    ///
    pub audit: Option<String>,

    #[clap(long = "shredding", env = "PICODATA_SHREDDING")]
    /// Shred (not only delete) .xlog and .snap files on rotation
    /// for the security reasons.
    pub shredding: bool,

    #[clap(long = "log", value_name = "PATH", env = "PICODATA_LOG")]
    /// Configuration for the picodata diagnostic log.
    /// Valid options:
    ///
    /// 1. `file:<file>` or simply `<file>` — write to a file, e.g.:
    ///
    ///    picodata run --log '/tmp/picodata.log'
    ///
    /// 2. `pipe:<command>` or `| <command>` — redirect to a subprocess, e.g:
    ///
    ///    picodata run --log '| /dev/capture-from-stdin'
    ///
    /// 3. `syslog:` — write to the syslog, e.g:
    ///
    ///    picodata run --log 'syslog:'
    ///
    pub log: Option<String>,

    #[clap(long = "memtx-memory", env = "PICODATA_MEMTX_MEMORY")]
    /// The amount of memory in bytes to allocate for the database engine.
    ///
    /// By default 67'108'864 is used.
    pub memtx_memory: Option<u64>,

    #[clap(
        long = "service-password-file",
        value_name = "PATH",
        env = "PICODATA_SERVICE_PASSWORD_FILE"
    )]
    /// Path to a plain-text file with a password for the system user "pico_service".
    /// This password will be used for internal communication among instances of
    /// picodata, so it must be the same on all instances.
    pub service_password_file: Option<String>,
}

// Copy enum because clap:ArgEnum can't be derived for the foreign SayLevel.
tarantool::define_str_enum! {
    #[derive(clap::ArgEnum)]
    #[clap(rename_all = "lower")]
    pub enum LogLevel {
        Fatal = "fatal",
        System = "system",
        Error = "error",
        Crit = "crit",
        Warn = "warn",
        Info = "info",
        Verbose = "verbose",
        Debug = "debug",
    }
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
        let mut args = vec![
            current_exe()?,
            CString::new(r"-e").unwrap(),
            CString::new(r#" "#).unwrap(),
        ];

        if self.interactive_mode {
            args.push(CString::new("-i").unwrap());
        }

        Ok(args)
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
    #[clap(long, value_name = "NAME", default_value = "demo")]
    /// Name of the cluster from instance should be expelled.
    pub cluster_id: String,

    #[clap(long, value_name = "NAME", default_value = "")]
    /// Name of the instance to expel.
    pub instance_id: InstanceId,

    #[clap(
        short = 'u',
        long = "user",
        value_name = "USER",
        default_value = DEFAULT_USERNAME,
        env = "PICODATA_USER"
    )]
    /// The username to connect with. Ignored if provided in `ADDRESS`.
    pub user: String,

    #[clap(
        short = 'a',
        long = "auth-type",
        value_name = "METHOD",
        default_value = AuthMethod::ChapSha1.as_str(),
    )]
    /// The preferred authentication method.
    pub auth_method: AuthMethod,

    #[clap(long = "peer", value_name = "[USER@][HOST][:PORT]")]
    /// Address of any picodata instance of the given cluster. It will be used
    /// to redirect the request to the current raft-leader to execute the actual
    /// request.
    pub peer_address: Address,

    #[clap(long, env = "PICODATA_PASSWORD_FILE")]
    /// Path to a plain-text file with a password.
    /// If this option isn't provided, the password is prompted from the terminal.
    pub password_file: Option<String>,
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
#[clap(about = "Connect to the Distributed SQL console")]
#[clap(after_help = "SPECIAL COMMANDS:
    \\e            Open the editor specified by the EDITOR environment variable
    \\help         Show this screen

HOTKEYS:
    Enter         Submit the request
    Alt  + Enter  Insert a newline character
    Ctrl + C      Discard current input
    Ctrl + D      Quit interactive console
")]
pub struct Connect {
    #[clap(
        short = 'u',
        long = "user",
        value_name = "USER",
        default_value = DEFAULT_USERNAME,
        env = "PICODATA_USER"
    )]
    /// The username to connect with. Ignored if provided in `ADDRESS`.
    pub user: String,

    #[clap(
        short = 'a',
        long = "auth-type",
        value_name = "METHOD",
        default_value = AuthMethod::ChapSha1.as_str(),
    )]
    /// The preferred authentication method.
    pub auth_method: AuthMethod,

    #[clap(value_name = "ADDRESS")]
    /// Picodata instance address to connect. Format:
    /// `[user@][host][:port]`.
    pub address: Address,

    #[clap(long, env = "PICODATA_PASSWORD_FILE")]
    /// Path to a plain-text file with a password.
    /// If this option isn't provided, the password is prompted from the terminal.
    pub password_file: Option<String>,
}

impl Connect {
    /// Get the arguments that will be passed to `tarantool_main`
    pub fn tt_args(&self) -> Result<Vec<CString>, String> {
        Ok(vec![current_exe()?])
    }
}

#[derive(Debug, Parser)]
#[clap(about = "Connect to the Admin console of a Picodata instance")]
#[clap(after_help = "SPECIAL COMMANDS:
    \\e            Open the editor specified by the EDITOR environment variable
    \\help         Show this screen
    \\sql          Switch console language to SQL (default)
    \\lua          Switch console language to Lua (deprecated)

HOTKEYS:
    Enter         Submit the request
    Alt  + Enter  Insert a newline character
    Ctrl + C      Discard current input
    Ctrl + D      Quit interactive console
")]
pub struct Admin {
    #[clap(value_name = "PATH")]
    /// Unix socket path to connect.
    pub socket_path: String,
}

impl Admin {
    /// Get the arguments that will be passed to `tarantool_main`
    pub fn tt_args(&self) -> Result<Vec<CString>, String> {
        Ok(vec![current_exe()?])
    }
}
