use crate::failure_domain::FailureDomain;
use crate::instance::InstanceId;
use crate::replicaset::ReplicasetId;
use crate::tier::{Tier, DEFAULT_TIER};
use crate::util::Uppercase;
use clap::Parser;
use std::borrow::Cow;
use std::collections::HashSet;
use std::ffi::{CStr, CString};
use std::str::FromStr;
use tarantool::auth::AuthMethod;
use tarantool::log::SayLevel;
use tarantool::tlua;

#[derive(Debug, Parser)]
#[clap(name = "picodata", version = "23.06.0")]
pub enum Picodata {
    Run(Box<Run>),
    Tarantool(Tarantool),
    Expel(Expel),
    Test(Test),
    Connect(Connect),
    Sql(ConnectSql),
}

////////////////////////////////////////////////////////////////////////////////
// Run
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Parser, tlua::Push, PartialEq)]
#[clap(about = "Run the picodata instance")]
pub struct Run {
    #[clap(
        long,
        value_name = "NAME",
        default_value = "demo",
        env = "PICODATA_CLUSTER_ID"
    )]
    /// Name of the cluster. The instance will refuse
    /// to join a cluster with a different name.
    pub cluster_id: String,

    #[clap(
        long,
        value_name = "PATH",
        default_value = ".",
        env = "PICODATA_DATA_DIR"
    )]
    /// Here the instance persists all of its data
    pub data_dir: String,

    #[clap(long, value_name = "NAME", env = "PICODATA_INSTANCE_ID")]
    /// Name of the instance.
    /// If not defined, it'll be generated automatically.
    pub instance_id: Option<InstanceId>,

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
        default_value = "localhost:3301",
        env = "PICODATA_LISTEN"
    )]
    /// Socket bind address
    pub listen: Address,

    #[clap(
        long = "peer",
        value_name = "[HOST][:PORT]",
        require_value_delimiter = true,
        use_value_delimiter = true,
        default_value = "localhost:3301",
        env = "PICODATA_PEER"
    )]
    /// Address(es) of other instance(s)
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
    /// Name of the replicaset
    pub replicaset_id: Option<ReplicasetId>,

    #[clap(long, arg_enum, default_value = "info", env = "PICODATA_LOG_LEVEL")]
    /// Log level
    log_level: LogLevel,

    #[clap(
        long,
        default_value = "1",
        env = "PICODATA_INIT_REPLICATION_FACTOR",
        group = "init_cfg"
    )]
    /// Total number of replicas (copies of data) for each replicaset.
    /// It makes sense only when starting cluster without --init-cfg option.
    pub init_replication_factor: u8,

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
    /// Enable interactive console
    pub interactive_mode: bool,

    #[clap(long, value_name = "PATH", env = "PICODATA_CONSOLE_SOCK")]
    /// Unix socket for the interactive console to connect using
    /// `picodata connect --unix`. Unlike connecting to a
    /// `--listen` address, console communication occurs in plain text
    /// and always operates under the admin account.
    pub console_sock: Option<String>,

    #[clap(
        long,
        value_name = "PATH",
        env = "PICODATA_PLUGINS",
        require_value_delimiter = true,
        use_value_delimiter = true
    )]
    /// Path to `some_plugin_name.so`
    pub plugins: Vec<String>,

    /// Name of the tier to which the instance will belong.
    #[clap(long = "tier", value_name = "TIER", default_value = DEFAULT_TIER, env = "PICODATA_INSTANCE_TIER")]
    pub tier: String,

    /// Filepath to configuration file in yaml format.
    #[clap(long = "init-cfg", value_name = "PATH", parse(try_from_str = try_parse_yaml_to_init_cfg), env = "PICODATA_INIT_CFG", group = "init_cfg")]
    pub init_cfg: Option<InitCfg>,
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

    pub fn advertise_address(&self) -> String {
        let Address { host, port, .. } = self.advertise_address.as_ref().unwrap_or(&self.listen);
        format!("{host}:{port}")
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
    #[clap(long, value_name = "NAME", default_value = "demo")]
    /// Name of the cluster from instance should be expelled.
    pub cluster_id: String,

    #[clap(long, value_name = "NAME", default_value = "")]
    /// Name of the instance to expel.
    pub instance_id: InstanceId,

    #[clap(
        long = "peer",
        value_name = "[HOST][:PORT]",
        default_value = "localhost:3301"
    )]
    /// Address of any instance from the cluster.
    pub peer_address: Address,
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
#[clap(about = "Connect a Picodata instance and start interactive Lua console")]
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

    #[clap(long = "unix")]
    /// Treat ADDRESS as a unix socket path.
    pub address_as_socket: bool,

    #[clap(value_name = "ADDRESS")]
    /// Picodata instance address to connect. Format:
    /// `[user@][host][:port]` or `--unix <PATH>`.
    pub address: String,

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
#[clap(about = "Connect a Picodata instance and start interactive SQL console")]
#[clap(
    long_about = "Connect a Picodata instance and start interactive SQL console

In addition to running sql queries picodata sql supports simple meta commands.

Anything you enter in picodata sql that begins with an unquoted backslash is a
meta-command that is processed by the cli itself. These commands make cli more
useful for administration or scripting.

Currently there is only one such command, but other ones are expected to appear.

\\e (edit)
    Opens a temporary file and passes it to binary specified in EDITOR environment
    variable. When the editor is closed if the exit code is zero then the file
    content is treated as a SQL query and attempted to be executed.
"
)]
pub struct ConnectSql {
    #[clap(
        short = 'u',
        long = "user",
        value_name = "USER",
        default_value = "guest",
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
    /// Picodata instance address. Format: `[user@][host][:port]`
    pub address: Address,

    #[clap(long, env = "PICODATA_PASSWORD_FILE")]
    /// Path to a plain-text file with a password.
    /// If this option isn't provided, the password is prompted from the terminal.
    pub password_file: Option<String>,
}

impl ConnectSql {
    /// Get the arguments that will be passed to `tarantool_main`
    pub fn tt_args(&self) -> Result<Vec<CString>, String> {
        Ok(vec![current_exe()?])
    }
}

pub const DEFAULT_USERNAME: &str = "guest";
const DEFAULT_HOST: &str = "localhost";
const DEFAULT_PORT: &str = "3301";

#[derive(Debug, Clone, PartialEq, Eq, tlua::Push)]
pub struct Address {
    pub user: Option<String>,
    pub host: String,
    pub port: String,
}

impl FromStr for Address {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let err = Err("valid format: [user@][host][:port]".to_string());
        let (user, host_port) = match text.rsplit_once('@') {
            Some((user, host_port)) => {
                if user.contains(':') || user.contains('@') {
                    return err;
                }
                if user.is_empty() {
                    return err;
                }
                (Some(user), host_port)
            }
            None => (None, text),
        };
        let (host, port) = match host_port.rsplit_once(':') {
            Some((host, port)) => {
                if host.contains(':') {
                    return err;
                }
                if port.is_empty() {
                    return err;
                }
                let host = if host.is_empty() { None } else { Some(host) };
                (host, Some(port))
            }
            None => (Some(host_port), None),
        };
        Ok(Self {
            user: user.map(Into::into),
            host: host.unwrap_or(DEFAULT_HOST).into(),
            port: port.unwrap_or(DEFAULT_PORT).into(),
        })
    }
}

#[derive(Debug, serde::Deserialize, PartialEq, tlua::Push, Clone)]
pub struct InitCfg {
    pub tiers: Vec<Tier>,
}

fn try_parse_yaml_to_init_cfg(path: &str) -> Result<InitCfg, String> {
    let content =
        std::fs::read_to_string(path).map_err(|e| format!("can't read from {path}, error: {e}"))?;

    let cfg: InitCfg = serde_yaml::from_str(&content)
        .map_err(|e| format!("error while parsing {path}, error: {e}"))?;
    let mut tiers_names = HashSet::new();
    for tier in &cfg.tiers {
        if tiers_names.contains(&tier.name) {
            return Err(format!(
                "found tiers with the same name - \"{}\" in config by path \"{path}\"",
                &tier.name
            ));
        }

        tiers_names.insert(tier.name.clone());
    }

    Ok(cfg)
}

impl Default for InitCfg {
    fn default() -> Self {
        InitCfg {
            tiers: vec![Tier::default()],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_parse_address() {
        assert_eq!(
            "1234".parse(),
            Ok(Address {
                user: None,
                host: "1234".into(),
                port: "3301".into()
            })
        );
        assert_eq!(
            ":1234".parse(),
            Ok(Address {
                user: None,
                host: "localhost".into(),
                port: "1234".into()
            })
        );
        assert_eq!(
            "example".parse(),
            Ok(Address {
                user: None,
                host: "example".into(),
                port: "3301".into()
            })
        );
        assert_eq!(
            "localhost:1234".parse(),
            Ok(Address {
                user: None,
                host: "localhost".into(),
                port: "1234".into()
            })
        );
        assert_eq!(
            "1.2.3.4:1234".parse(),
            Ok(Address {
                user: None,
                host: "1.2.3.4".into(),
                port: "1234".into()
            })
        );
        assert_eq!(
            "example:1234".parse(),
            Ok(Address {
                user: None,
                host: "example".into(),
                port: "1234".into()
            })
        );

        assert_eq!(
            "user@host:port".parse(),
            Ok(Address {
                user: Some("user".into()),
                host: "host".into(),
                port: "port".into()
            })
        );

        assert_eq!(
            "user@:port".parse(),
            Ok(Address {
                user: Some("user".into()),
                host: "localhost".into(),
                port: "port".into()
            })
        );

        assert_eq!(
            "user@host".parse(),
            Ok(Address {
                user: Some("user".into()),
                host: "host".into(),
                port: "3301".into()
            })
        );

        assert!("example::1234".parse::<Address>().is_err());
        assert!("user@@example".parse::<Address>().is_err());
        assert!("user:pass@host:port".parse::<Address>().is_err());
        assert!("a:b@c".parse::<Address>().is_err());
        assert!("user:pass@host".parse::<Address>().is_err());
        assert!("@host".parse::<Address>().is_err());
        assert!("host:".parse::<Address>().is_err());
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
            assert_eq!(
                parsed.peers.as_ref(),
                vec![Address {
                    user: None,
                    host: "localhost".into(),
                    port: "3301".into()
                }]
            );
            assert_eq!(
                parsed.listen,
                Address {
                    user: None,
                    host: "localhost".into(),
                    port: "3301".into()
                }
            ); // default
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
            assert_eq!(
                parsed.peers.as_ref(),
                vec![Address {
                    user: None,
                    host: "peer-from-env".into(),
                    port: "3301".into()
                }]
            );

            let parsed = parse![Run, "--peer", "peer-from-args"];
            assert_eq!(
                parsed.peers.as_ref(),
                vec![Address {
                    user: None,
                    host: "peer-from-args".into(),
                    port: "3301".into()
                }]
            );

            let parsed = parse![Run, "--peer", ":3302"];
            assert_eq!(
                parsed.peers.as_ref(),
                vec![Address {
                    user: None,
                    host: "localhost".into(),
                    port: "3302".into()
                }]
            );

            let parsed = parse![Run, "--peer", "p1", "--peer", "p2,p3"];
            assert_eq!(
                parsed.peers.as_ref(),
                vec![
                    Address {
                        user: None,
                        host: "p1".into(),
                        port: "3301".into()
                    },
                    Address {
                        user: None,
                        host: "p2".into(),
                        port: "3301".into()
                    },
                    Address {
                        user: None,
                        host: "p3".into(),
                        port: "3301".into()
                    }
                ]
            );
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
            assert_eq!(
                parsed.listen,
                Address {
                    user: None,
                    host: "listen-from-env".into(),
                    port: "3301".into()
                }
            );
            assert_eq!(parsed.advertise_address(), "listen-from-env:3301");

            let parsed = parse![Run, "-l", "listen-from-args"];
            assert_eq!(
                parsed.listen,
                Address {
                    user: None,
                    host: "listen-from-args".into(),
                    port: "3301".into()
                }
            );
            assert_eq!(parsed.advertise_address(), "listen-from-args:3301");
        }

        std::env::set_var("PICODATA_ADVERTISE", "advertise-from-env");
        {
            let parsed = parse![Run,];
            assert_eq!(
                parsed.listen,
                Address {
                    user: None,
                    host: "listen-from-env".into(),
                    port: "3301".into()
                }
            );
            assert_eq!(parsed.advertise_address(), "advertise-from-env:3301");

            let parsed = parse![Run, "-l", "listen-from-args"];
            assert_eq!(
                parsed.listen,
                Address {
                    user: None,
                    host: "listen-from-args".into(),
                    port: "3301".into()
                }
            );
            assert_eq!(parsed.advertise_address(), "advertise-from-env:3301");

            let parsed = parse![Run, "--advertise", "advertise-from-args"];
            assert_eq!(
                parsed.listen,
                Address {
                    user: None,
                    host: "listen-from-env".into(),
                    port: "3301".into()
                }
            );
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
            let parsed = parse![Run, "-i"];
            assert_eq!(parsed.interactive_mode, true);

            let parsed = parse![Run, "--interactive"];
            assert_eq!(parsed.interactive_mode, true);

            let parsed = parse![Run,];
            assert_eq!(parsed.interactive_mode, false);
        }

        {
            std::env::set_var("PICODATA_USER", "batman");
            let parsed = parse!(Connect, "somewhere:3301");
            assert_eq!(parsed.user, "batman");
        }
    }
}
