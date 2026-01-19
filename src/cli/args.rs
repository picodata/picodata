use crate::address::{HttpAddress, IprotoAddress, PgprotoAddress};
use crate::config::{ByteSize, DEFAULT_USERNAME};
use crate::info::version_for_help;
use crate::util::Uppercase;

use std::borrow::Cow;
use std::ffi::{CStr, CString};
use std::path::PathBuf;

use clap::Parser;
use tarantool::auth::AuthMethod;
use tarantool::log::SayLevel;
use tarantool::network::client::tls;
use tarantool::tlua;

#[derive(Debug, Parser)]
#[clap(name = "picodata", version = version_for_help())]
pub enum Picodata {
    Restore(Restore),
    Run(Box<Run>),
    #[clap(hide = true)]
    Tarantool(Tarantool),
    Expel(Expel),
    Test(Test),
    Connect(Connect),
    Admin(Admin),
    Status(Status),
    #[clap(subcommand)]
    Config(Config),
    #[clap(subcommand)]
    Plugin(Plugin),
    Demo(Demo),
}

pub const CONFIG_PARAMETERS_ENV: &'static str = "PICODATA_CONFIG_PARAMETERS";

////////////////////////////////////////////////////////////////////////////////
// Restore
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser, PartialEq)]
#[clap(about = "Restore the picodata instance from given backup path")]
pub struct Restore {
    #[clap(short = 'p', long = "path")]
    /// Full path to the directory with backup files.
    pub backup_path: String,

    #[clap(short = 'c', long = "config")]
    /// Name of the config file under backup
    /// directory which should be used for restore.
    pub config_name: Option<String>,
}

impl Restore {
    /// Get the arguments that will be passed to `tarantool_main`
    pub fn tt_args(&self) -> Result<Vec<CString>, String> {
        Ok(vec![current_exe()?])
    }
}

////////////////////////////////////////////////////////////////////////////////
// Run
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser, PartialEq)]
#[clap(about = "Run the picodata instance")]
pub struct Run {
    #[clap(long, value_name = "NAME", env = "PICODATA_CLUSTER_NAME")]
    /// Name of the cluster. The instance will refuse
    /// to join a cluster with a different name.
    ///
    /// By default this will be "demo".
    pub cluster_name: Option<String>,

    #[clap(long, value_name = "PATH", env = "PICODATA_INSTANCE_DIR")]
    /// Here the instance persists all of its data.
    ///
    /// By default this is the current working directory (".").
    pub instance_dir: Option<PathBuf>,

    #[clap(long, value_name = "PATH", env = "PICODATA_BACKUP_DIR")]
    /// Here the instance persists all of its **backup** data.
    ///
    /// By default this is "{instance_dir}/backup".
    pub backup_dir: Option<PathBuf>,

    #[clap(long, value_name = "PATH", env = "PICODATA_CONFIG_FILE")]
    /// Path to configuration file in yaml format.
    ///
    /// By default "./picodata.yaml" is used if it exists.
    pub config: Option<PathBuf>,

    #[clap(
        short = 'c',
        long,
        value_name = "PARAMETER=VALUE",
        use_value_delimiter = false
    )]
    /// A list of key-value pairs specifying configuration parameters.
    ///
    /// These will override both parameters provided in the picodata.yaml file,
    /// the command-line parameters and the environment variables.
    ///
    /// Key is a `.` separated path to a configuration parameter.
    /// The data in the `VALUE` is interpreted as YAML.
    ///
    /// For example: `-c instance.log.level=debug -c instance.instance_dir=/path/to/dir`
    ///
    /// Can also be provided via PICODATA_CONFIG_PARAMETERS environment variable.
    pub config_parameter: Vec<String>,

    #[clap(long, value_name = "NAME", env = "PICODATA_INSTANCE_NAME")]
    /// Name of the instance.
    /// If not defined, it'll be generated automatically.
    pub instance_name: Option<String>,

    #[clap(
        long = "advertise",
        value_name = "HOST:PORT",
        env = "PICODATA_ADVERTISE",
        hide = true,
        group = "advertise_arguments"
    )]
    /// DEPRECATED option
    ///
    /// Public network address of the instance. It is announced to the
    /// cluster during the instance start. Later it's used by other
    /// instances for connecting to this one.
    ///
    /// Defaults to `--iproto-listen` value which is enough in most cases. But,
    /// for example, in case of `--iproto-listen 0.0.0.0` it should be
    /// specified explicitly:
    ///
    /// picodata run --iproto-listen 0.0.0.0:3301 --iproto-advertise 192.168.0.1:3301
    pub advertise_address: Option<IprotoAddress>,

    #[clap(
        long = "iproto-advertise",
        value_name = "HOST:PORT",
        env = "PICODATA_IPROTO_ADVERTISE",
        group = "advertise_arguments"
    )]
    /// Public network address of the instance. It is announced to the
    /// cluster during the instance start. Later it's used by other
    /// instances for connecting to this one.
    ///
    /// Defaults to `--iproto-listen` value which is enough in most cases. But,
    /// for example, in case of `--iproto-listen 0.0.0.0` it should be
    /// specified explicitly:
    ///
    /// picodata run --iproto-listen 0.0.0.0:3301 --iproto-advertise 192.168.0.1:3301
    pub iproto_advertise: Option<IprotoAddress>,

    #[clap(
        short = 'l',
        long = "listen",
        value_name = "HOST:PORT",
        env = "PICODATA_LISTEN",
        hide = true,
        group = "listen_arguments"
    )]
    /// DEPRECATED option
    ///
    /// Instance network address.
    ///
    /// By default "127.0.0.1:3301" is used.
    pub listen: Option<IprotoAddress>,

    #[clap(
        long = "iproto-listen",
        value_name = "HOST:PORT",
        env = "PICODATA_IPROTO_LISTEN",
        group = "listen_arguments"
    )]
    /// Instance network address.
    ///
    /// By default "127.0.0.1:3301" is used.
    pub iproto_listen: Option<IprotoAddress>,

    #[clap(
        long,
        value_name = "HOST:PORT",
        env = "PICODATA_PG_ADVERTISE",
        group = "advertise_arguments"
    )]
    /// Public network address of the pgproto server.
    /// It is announced to the cluster during the instance start.
    ///
    /// Defaults to `--pg-listen` value which is enough in most cases. But,
    /// for example, in case of `--pg-listen 0.0.0.0:5432` it should be
    /// specified explicitly, .e.g.:
    ///
    /// `picodata run --pg-listen 0.0.0.0:5432 --pg-advertise 192.168.0.1:5432`
    pub pg_advertise: Option<PgprotoAddress>,

    /// Pgproto server address.
    #[clap(long, value_name = "HOST:PORT", env = "PICODATA_PG_LISTEN")]
    pub pg_listen: Option<PgprotoAddress>,

    #[clap(
        long = "peer",
        value_name = "HOST:PORT",
        value_delimiter = ',',
        env = "PICODATA_PEER"
    )]
    /// A comma-separated list of network addresses of other instances.
    /// Used during cluster initialization
    /// and joining an instance to an existing cluster.
    ///
    /// For example: `--peer server-1.picodata.int:13301,server-2.picodata.int:13301`
    ///
    /// Defaults to `--advertise` value which results in creating a new
    /// cluster
    pub peers: Vec<IprotoAddress>,

    #[clap(
        long = "failure-domain",
        value_name = "KEY=VALUE",
        value_delimiter = ',',
        value_parser = try_parse_kv_uppercase,
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

    #[clap(long, value_name = "NAME", env = "PICODATA_REPLICASET_NAME")]
    /// Name of the replicaset.
    /// Used during cluster initialization
    /// and joining an instance to an existing cluster.
    ///
    /// If not specified, a replicaset will be automatically chosen based on the
    /// failure domain settings.
    pub replicaset_name: Option<String>,

    #[clap(long, value_enum, env = "PICODATA_LOG_LEVEL")]
    /// Log level.
    ///
    /// By default "info" is used.
    pub log_level: Option<LogLevel>,

    #[clap(long, env = "PICODATA_INIT_REPLICATION_FACTOR", group = "init_cfg")]
    /// Total number of replicas (copies of data) for each replicaset.
    ///
    /// By default 1 is used.
    pub init_replication_factor: Option<u8>,

    #[clap(long, value_name = "PATH", env = "PICODATA_SCRIPT")]
    /// A path to a lua script that will be executed at postjoin stage.
    /// At the moment the script is executed, the local storage is
    /// already initialized and HTTP server is running (if specified).
    /// But the raft node is uninitialized yet.
    pub script: Option<PathBuf>,

    #[clap(long, value_name = "HOST:PORT", env = "PICODATA_HTTP_LISTEN")]
    /// HTTP server address.
    pub http_listen: Option<HttpAddress>,

    #[clap(short = 'i', long = "interactive", env = "PICODATA_INTERACTIVE_MODE")]
    /// Enable interactive console.
    pub interactive_mode: bool,

    #[clap(long, value_name = "PATH", env = "PICODATA_ADMIN_SOCK")]
    /// Unix socket for the interactive console to connect using
    /// `picodata admin`. Unlike connecting via `picodata connect`
    /// console communication occurs in plain text
    /// and always operates under the admin account.
    ///
    /// By default the "admin.sock" in the instance directory is used.
    pub admin_sock: Option<PathBuf>,

    #[clap(long, value_name = "PATH", env = "PICODATA_SHARE_DIR")]
    /// Path to directory with plugin installations.
    pub share_dir: Option<PathBuf>,

    #[clap(long, value_name = "PATH", env = "PICODATA_PLUGIN_DIR", hide = true)]
    /// Deprecated. Use --share-dir instead.
    pub plugin_dir: Option<PathBuf>,

    #[clap(long = "tier", value_name = "TIER", env = "PICODATA_INSTANCE_TIER")]
    /// Name of the tier to which the instance will belong.
    /// Used during cluster initialization
    /// and joining an instance to an existing cluster.
    ///
    /// By default "default" is used.
    pub tier: Option<String>,

    /// Filepath to configuration file in yaml format.
    #[clap(
        hide = true,
        long = "init-cfg",
        value_name = "PATH",
        env = "PICODATA_INIT_CFG",
        group = "init_cfg"
    )]
    pub init_config: Option<String>,

    #[clap(long = "audit", value_name = "PATH", env = "PICODATA_AUDIT_LOG")]
    // As it's not always a path the value type is left as `String`.
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
    // As it's not always a path the value type is left as `String`.
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
    /// By default, the diagnostic log is output to stderr.
    pub log: Option<String>,

    #[clap(long = "memtx-memory", env = "PICODATA_MEMTX_MEMORY")]
    /// The amount of memory in bytes to allocate for the database engine.
    ///
    /// By default, 64 MiB is used.
    pub memtx_memory: Option<ByteSize>,

    #[clap(long = "memtx-system-memory", env = "PICODATA_MEMTX_SYSTEM_MEMORY")]
    /// The amount of memory in bytes to allocate for system spaces.
    ///
    /// By default, 256 MiB is used.
    pub memtx_system_memory: Option<ByteSize>,

    #[clap(long = "memtx-max-tuple-size", env = "PICODATA_MEMTX_MAX_TUPLE_SIZE")]
    /// Size of the largest allocation unit, for the memtx storage engine
    ///
    /// By default, 1 MiB is used.
    pub memtx_max_tuple_size: Option<ByteSize>,

    #[clap(hide = true, long = "entrypoint-fd")]
    /// A pipe file descriptor from which picodata reads the entrypoint info
    /// when doing a rebootstrap during the cluster initialization.
    ///
    /// This option is for internal use only hence it's marked hidden.
    pub entrypoint_fd: Option<u32>,
}

// Copy enum because clap:ArgEnum can't be derived for the foreign SayLevel.
tarantool::define_str_enum! {
    #[derive(clap::ValueEnum)]
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
    #[clap(
        raw = true,
        // to understand why this is a closure, see:
        // <https://github.com/rust-lang/rust/issues/119045>
        value_parser = |a: &str| CString::new(a)
    )]
    pub args: Vec<CString>,
}

impl Tarantool {
    /// Get the arguments that will be passed to `tarantool_main`
    pub fn tt_args(&self) -> Result<Vec<Cow<'_, CStr>>, String> {
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
    #[clap(long, value_name = "NAME")]
    /// Deprecated. The cluster_name parameter is no longer used and will be removed in the future major release (version 26).
    pub cluster_name: Option<String>,

    #[clap(value_name = "INSTANCE_UUID")]
    /// UUID of the instance to expel.
    pub instance_uuid: String,

    #[clap(
        long = "peer",
        value_name = "[USER@]HOST:PORT",
        env = "PICODATA_PEER",
        default_value = "127.0.0.1:3301"
    )]
    /// Address of any picodata instance of the given cluster.
    pub peer_address: IprotoAddress,

    #[clap(long, env = "PICODATA_PASSWORD_FILE")]
    /// Path to a plain-text file with a password for the
    /// specified user (or system user "pico_service" if no
    /// user was specified). If password doesn't match, error
    /// message is printed to a user. If the password isn't
    /// provided, it will be prompted from the terminal.
    pub password_file: Option<PathBuf>,

    #[clap(short = 'a', long = "auth-type", value_name = "METHOD")]
    /// The preferred authentication method.
    pub auth_method: Option<AuthMethod>,

    #[clap(short = 'f', long = "force")]
    /// Expel instance even if it is currently online.
    pub force: bool,

    #[clap(
        short = 't',
        long = "timeout",
        value_name = "TIMEOUT",
        default_value = "60"
    )]
    /// Time to wait for the operation to complete.
    pub timeout: u64,

    #[clap(flatten)]
    pub tls: IprotoTlsArgs,
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
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((key.into(), value.into()))
}

////////////////////////////////////////////////////////////////////////////////
// Connect
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser)]
#[clap(about = "Connect to the Distributed SQL console")]
#[clap(hide(true))]
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

    #[clap(short = 'a', long = "auth-type", value_name = "METHOD")]
    /// The preferred authentication method.
    pub auth_method: Option<AuthMethod>,

    #[clap(value_name = "ADDRESS")]
    /// Picodata instance address to connect. Format:
    /// `[user@]host:port`.
    pub address: IprotoAddress,

    #[clap(long, env = "PICODATA_PASSWORD_FILE")]
    /// Path to a plain-text file with a password.
    /// If this option isn't provided, the password is prompted from the terminal.
    pub password_file: Option<PathBuf>,

    #[clap(
        short = 't',
        long = "timeout",
        value_name = "TIMEOUT",
        default_value = "20",
        env = "PICODATA_CONNECT_TIMEOUT"
    )]
    /// Connection timeout in seconds.
    pub timeout: u64,

    #[clap(flatten)]
    pub tls: IprotoTlsArgs,
}

impl Connect {
    /// Get the arguments that will be passed to `tarantool_main`
    pub fn tt_args(&self) -> Result<Vec<CString>, String> {
        Ok(vec![current_exe()?])
    }
}

////////////////////////////////////////////////////////////////////////////////
// Admin
////////////////////////////////////////////////////////////////////////////////

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

    #[clap(long = "ignore-errors")]
    /// Flag to continue execution despite invalid queries being sent in non-interactive mode.
    pub ignore_errors: bool,

    #[clap(short = 't', long = "tuples-only", conflicts_with_all = ["csv", "json"])]
    /// Print only tuples without headers or row count.
    pub tuples_only: bool,

    #[clap(short = 'j', long = "json", conflicts_with_all = ["csv", "tuples_only"])]
    /// Output results in JSON format.
    pub json: bool,

    #[clap(short = 'c', long = "csv", conflicts_with_all = ["json", "tuples_only"])]
    /// Output results in CSV format.
    pub csv: bool,

    #[clap(short = 'F', long = "field-separator", conflicts_with = "json")]
    /// Field separator for CSV (default: ',') and tuples-only (default: tab) output.
    pub field_separator: Option<char>,

    #[clap(
        short = 'p',
        long = "prompts",
        conflicts_with_all = ["json", "csv", "tuples_only"]
    )]
    /// Show prompts and decorative messages even when input is not a terminal.
    pub prompts: bool,
}

impl Admin {
    /// Get the arguments that will be passed to `tarantool_main`
    pub fn tt_args(&self) -> Result<Vec<CString>, String> {
        Ok(vec![current_exe()?])
    }
}

////////////////////////////////////////////////////////////////////////////////
// Status
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser)]
#[clap(about = "Display the status of all instances in the cluster")]
pub struct Status {
    #[clap(
        long = "peer",
        value_name = "[USER@]HOST:PORT",
        env = "PICODATA_PEER",
        default_value = "127.0.0.1:3301"
    )]
    /// Address of a picodata instance in the cluster.
    ///
    /// The address should be in the format `[USER@]HOST:PORT` where:
    ///
    /// - `USER` is optional (defaults to "pico_service" if omitted)
    ///
    /// - `HOST` is the hostname or IP address
    ///
    /// - `PORT` is the network port
    ///
    /// Consider the following examples:
    ///
    /// - `picodata status --peer admin@127.0.0.1:3301`
    ///
    /// - `picodata status --peer localhost:3301`
    pub peer_address: IprotoAddress,

    #[clap(
        long = "service-password-file",
        value_name = "PATH",
        env = "PICODATA_SERVICE_PASSWORD_FILE"
    )]
    /// Path to a plain-text file with a password for the
    /// specified user (or system user "pico_service" if no
    /// user was specified). If password doesn't match, error
    /// message is printed to a user. If the password isn't
    /// provided, it will be prompted from the terminal.
    pub password_file: Option<PathBuf>,

    #[clap(
        short = 't',
        long = "timeout",
        value_name = "TIMEOUT",
        default_value = "20",
        env = "PICODATA_CONNECT_TIMEOUT"
    )]
    /// Connection timeout in seconds.
    pub timeout: u64,

    #[clap(flatten)]
    pub tls: IprotoTlsArgs,
}

impl Status {
    /// Get the arguments that will be passed to `tarantool_main`
    pub fn tt_args(&self) -> Result<Vec<CString>, String> {
        Ok(vec![current_exe()?])
    }
}

////////////////////////////////////////////////////////////////////////////////
// Config
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, clap::Subcommand)]
#[clap(about = "Subcommands related to working with the configuration file")]
pub enum Config {
    /// Generate a picodata configuration file with default values.
    Default(ConfigDefault),
}

////////////////////////////////////////////////////////////////////////////////
// ConfigDefault
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser)]
pub struct ConfigDefault {
    #[clap(short = 'o', long = "output-file", value_name = "FILENAME")]
    /// File name for the generated configuration to be written to.
    /// If this option is omitted or the value of "-" is specified,
    /// the file contents are written to the standard output.
    pub output_file: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// Plugin
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, clap::Subcommand)]
#[clap(about = "Subcommand related to plugin management")]
pub enum Plugin {
    Configure(ServiceConfigUpdate),
}

impl Plugin {
    /// Get the arguments that will be passed to `tarantool_main`
    pub fn tt_args(&self) -> Result<Vec<CString>, String> {
        Ok(vec![current_exe()?])
    }
}

////////////////////////////////////////////////////////////////////////////////
// PluginUpdate
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser)]
#[clap(about = "Update plugin's service configuration")]
pub struct ServiceConfigUpdate {
    #[clap(
        long = "peer",
        value_name = "[USER@]HOST:PORT",
        env = "PICODATA_PEER",
        default_value = "127.0.0.1:3301"
    )]
    /// Address of a picodata instance in the cluster.
    ///
    /// The address should be in the format `[USER@]HOST:PORT` where:
    ///
    /// - `USER` is optional (defaults to "pico_service" if omitted)
    ///
    /// - `HOST` is the hostname or IP address
    ///
    /// - `PORT` is the network port
    ///
    /// Consider the following examples:
    ///
    /// - `picodata status --peer admin@127.0.0.1:3301`
    ///
    /// - `picodata status --peer localhost:3301`
    pub peer_address: IprotoAddress,

    #[clap(value_name = "PLUGIN_NAME")]
    /// Name of a plugin that has a service
    /// we want to change config of.
    pub plugin_name: String,

    #[clap(value_name = "PLUGIN_VERSION")]
    /// Version of a plugin that has a service
    /// we want to change config of.
    pub plugin_version: String,

    #[clap(value_name = "PLUGIN_CONFIG")]
    /// Path to a config file in YAML format
    /// that describes a new configuration of a service.
    /// It is not necessary to add all existing
    /// config options to change successfully, you
    /// may want to include only needed.
    pub config_file: PathBuf,

    #[clap(
        long = "service-password-file",
        value_name = "PATH",
        env = "PICODATA_SERVICE_PASSWORD_FILE"
    )]
    /// Path to a plain-text file with a password for the
    /// specified user (or system user "pico_service" if no
    /// user was specified). If password doesn't match, error
    /// message is printed to a user. If the password isn't
    /// provided, it will be prompted from the terminal.
    pub password_file: Option<PathBuf>,

    #[clap(
        long = "timeout",
        value_name = "TIMEOUT",
        env = "PICODATA_CONNECT_TIMEOUT",
        default_value = "20"
    )]
    /// Client connection timeout in seconds.
    pub timeout: u64,

    #[clap(
        long = "service-names",
        value_name = "SERVICE_NAMES",
        use_value_delimiter = true
    )]
    /// A comma-separated list of services names that we
    /// want to change configuration of. Single value
    /// without comma is allowed.
    ///
    /// Example: `--service-names service_1,service_2`
    ///
    /// If no matching service were found, error message
    /// is printed to a user.
    pub service_names: Option<Vec<String>>,

    #[clap(flatten)]
    pub tls: IprotoTlsArgs,
}

#[derive(Debug, PartialEq, clap::Args, tlua::Push)]
#[group(requires_all = ["cert", "key", "ca"])]
pub struct IprotoTlsArgs {
    #[clap(
        long = "tls-cert",
        value_name = "CERT_FILE",
        env = "PICODATA_IPROTO_TLS_CERT"
    )]
    /// Path to certificate file to enable mTLS.
    pub cert: Option<PathBuf>,
    #[clap(
        long = "tls-key",
        value_name = "KEY_FILE",
        env = "PICODATA_IPROTO_TLS_KEY"
    )]
    /// Path to private key file to enable mTLS.
    pub key: Option<PathBuf>,
    #[clap(
        long = "tls-ca",
        value_name = "CA_FILE",
        env = "PICODATA_IPROTO_TLS_CA"
    )]
    /// Path to trusted CA file to enable mTLS.
    pub ca: Option<PathBuf>,
    #[clap(long = "tls-auth", env = "PICODATA_IPROTO_TLS_AUTH")]
    /// Enables TLS certificate authentication.
    pub cert_auth: bool,
}

impl IprotoTlsArgs {
    pub fn to_tls_config(&self) -> tls::TlsConfig<'_> {
        tls::TlsConfig {
            cert_file: self
                .cert
                .as_ref()
                .expect("'tls-cert' argument should be already checked"),
            key_file: self
                .key
                .as_ref()
                .expect("'tls-key' argument should be already checked"),
            ca_file: self.ca.as_ref(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Demo
////////////////////////////////////////////////////////////////////////////////

/// See implementation in [`crate::cli::demo::main`].
/// Used for quick technical validations and live feature demonstrations.
#[derive(Debug, clap::Parser)]
#[clap(about = "Run interactive Picodata demonstration scenario")]
pub struct Demo {
    /// Path to a working directory for the scenario.
    ///
    /// Used as base path for all cluster working directories and miscellaneous things.
    ///
    /// DEFAULT: $CWD/picodata_demo/
    #[clap(long = "working-directory", value_name = "PATH")]
    pub working_directory: Option<PathBuf>,

    /// Number of replicasets to create in the demonstration cluster.
    ///
    /// Each replicaset is an independent fault-tolerance domain.
    ///
    /// DEFAULT: 2
    ///
    /// RESTRICTION: >0
    #[clap(
        long = "replicaset-count",
        value_name = "NUMBER",
        default_value = "2",
        hide_default_value = true
    )]
    pub replicaset_count: u8,

    /// Replication factor for each replicaset.
    ///
    /// The number of instances that store the same data set.
    ///
    /// DEFAULT: 2
    ///
    /// RESTRICTION: >0
    #[clap(
        long = "replication-factor",
        value_name = "NUMBER",
        default_value = "2",
        hide_default_value = true
    )]
    pub replication_factor: u8,

    /// Path to the Picodata executable used to spawn cluster instances.
    ///
    /// If omitted, the currently running executable is reused.
    ///
    /// DEFAULT: executable which you have used to get this help message
    #[clap(long = "picodata-executable", value_name = "PATH")]
    pub picodata_executable: Option<PathBuf>,

    /// Remove the working directory after the demonstration exits.
    ///
    /// When disabled, all instance data directories and cluster artifacts
    /// are deleted after shutdown. Enable this to inspect state on disk.
    ///
    /// DEFAULT: false
    #[clap(
        long = "clean-data",
        default_value = "false",
        default_missing_value = "true"
    )]
    pub clean_data: bool,
}
