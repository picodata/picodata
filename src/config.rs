use crate::address::Address;
use crate::cli::args;
use crate::cli::args::CONFIG_PARAMETERS_ENV;
use crate::failure_domain::FailureDomain;
use crate::instance::InstanceId;
use crate::introspection::FieldInfo;
use crate::introspection::Introspection;
use crate::replicaset::ReplicasetId;
use crate::storage;
use crate::tier::Tier;
use crate::tier::TierConfig;
use crate::tier::DEFAULT_TIER;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::RaftSpaceAccess;
use crate::util::edit_distance;
use crate::util::file_exists;
use crate::yaml_value::YamlValue;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use tarantool::log::SayLevel;
use tarantool::tlua;

pub const DEFAULT_USERNAME: &str = "guest";
pub const DEFAULT_LISTEN_HOST: &str = "localhost";
pub const DEFAULT_IPROTO_PORT: &str = "3301";
pub const DEFAULT_CONFIG_FILE_NAME: &str = "config.yaml";

////////////////////////////////////////////////////////////////////////////////
// PicodataConfig
////////////////////////////////////////////////////////////////////////////////

#[derive(
    PartialEq,
    Default,
    Debug,
    Clone,
    serde::Deserialize,
    serde::Serialize,
    tlua::Push,
    tlua::PushInto,
    Introspection,
)]
pub struct PicodataConfig {
    // TODO: add a flag for each parameter specifying where it came from, i.e.:
    // - default value
    // - configuration file
    // - command line arguments
    // - environment variable
    // - persisted storage (because some of the values are read from the storage on restart)
    #[serde(default)]
    #[introspection(nested)]
    pub cluster: ClusterConfig,

    #[serde(default)]
    #[introspection(nested)]
    pub instance: InstanceConfig,

    #[serde(flatten)]
    #[introspection(ignore)]
    pub unknown_sections: HashMap<String, YamlValue>,
}

fn validate_args(args: &args::Run) -> Result<(), Error> {
    if args.init_cfg.is_some() {
        return Err(Error::other(
            "error: option `--init-cfg` is removed, use `--config` instead",
        ));
    }

    if args.init_replication_factor.is_some() && args.config.is_some() {
        return Err(Error::other("error: option `--init-replication-factor` cannot be used with `--config` simultaneously"));
    }

    Ok(())
}

impl PicodataConfig {
    // TODO:
    // fn default() -> Self
    // which returns an instance of config with all the default parameters.
    // Also add a command to generate a default config from command line.
    pub fn init(args: args::Run) -> Result<Self, Error> {
        validate_args(&args)?;

        let cwd = std::env::current_dir();
        let cwd = cwd.as_deref().unwrap_or_else(|_| Path::new(".")).display();
        let default_path = format!("{cwd}/{DEFAULT_CONFIG_FILE_NAME}");

        let mut config = None;
        match (&args.config, file_exists(&default_path)) {
            (Some(args_path), true) => {
                if args_path != &default_path {
                    #[rustfmt::skip]
                    tlog!(Warning, "A path to configuration file '{args_path}' was provided explicitly,
but a '{DEFAULT_CONFIG_FILE_NAME}' file in the current working directory '{cwd}' also exists.
Using configuration file '{args_path}'.");
                }

                config = Some((Self::read_yaml_file(args_path)?, args_path));
            }
            (Some(args_path), false) => {
                config = Some((Self::read_yaml_file(args_path)?, args_path));
            }
            (None, true) => {
                #[rustfmt::skip]
                tlog!(Info, "Reading configuration file '{DEFAULT_CONFIG_FILE_NAME}' in the current working directory '{cwd}'.");
                config = Some((Self::read_yaml_file(&default_path)?, &default_path));
            }
            (None, false) => {}
        }

        let mut config = if let Some((mut config, path)) = config {
            config.validate_from_file()?;
            config.instance.config_file = Some(path.clone());
            config
        } else {
            Default::default()
        };

        config.set_from_args(args)?;

        config.validate_common()?;

        Ok(config)
    }

    #[inline]
    pub fn read_yaml_file(path: &str) -> Result<Self, Error> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| Error::other(format!("can't read from '{path}': {e}")))?;
        Self::read_yaml_contents(&contents)
    }

    #[inline]
    pub fn read_yaml_contents(contents: &str) -> Result<Self, Error> {
        let config: Self = serde_yaml::from_str(contents).map_err(Error::invalid_configuration)?;
        Ok(config)
    }

    pub fn set_from_args(&mut self, args: args::Run) -> Result<(), Error> {
        // TODO: add forbid_conflicts_with_args so that it's considered an error
        // if a parameter is specified both in the config and in the command line
        // arguments

        // We have to parse the environment variable explicitly, because clap
        // will ignore it for us in case the -c option was also provided.
        //
        // Also we do this before copying other fields from `args`, so that
        // parameters provided on command line have higher priority. But there's
        // a problem here, because as a result PICODATA_CONFIG_PARAMETERS env
        // var has lower priority then other PICODATA_* env vars, while
        // --config-parameters parameter has higher priority then other cli args.
        if let Ok(env) = std::env::var(CONFIG_PARAMETERS_ENV) {
            for item in env.split(';') {
                let item = item.trim();
                if item.is_empty() {
                    continue;
                }
                let Some((path, yaml)) = item.split_once('=') else {
                    return Err(Error::InvalidConfiguration(format!(
                        "error when parsing {CONFIG_PARAMETERS_ENV} environment variable: expected '=' after '{item}'",
                    )));
                };

                self.set_field_from_yaml(path.trim(), yaml.trim())
                    .map_err(Error::invalid_configuration)?;
            }
        }

        if let Some(data_dir) = args.data_dir {
            self.instance.data_dir = Some(data_dir);
        }

        if let Some(service_password_file) = args.service_password_file {
            self.instance.service_password_file = Some(service_password_file);
        }

        if let Some(cluster_id) = args.cluster_id {
            self.cluster.cluster_id = Some(cluster_id.clone());
            self.instance.cluster_id = Some(cluster_id);
        }

        if let Some(instance_id) = args.instance_id {
            self.instance.instance_id = Some(instance_id);
        }

        if let Some(replicaset_id) = args.replicaset_id {
            self.instance.replicaset_id = Some(replicaset_id);
        }

        if let Some(tier) = args.tier {
            self.instance.tier = Some(tier);
        }

        if let Some(init_replication_factor) = args.init_replication_factor {
            self.cluster.default_replication_factor = Some(init_replication_factor);
        }

        if !args.failure_domain.is_empty() {
            let map = args.failure_domain.into_iter();
            self.instance.failure_domain = Some(FailureDomain::from(map));
        }

        if let Some(address) = args.advertise_address {
            self.instance.advertise_address = Some(address);
        }

        if let Some(listen) = args.listen {
            self.instance.listen = Some(listen);
        }

        if let Some(http_listen) = args.http_listen {
            self.instance.http_listen = Some(http_listen);
        }

        if !args.peers.is_empty() {
            self.instance.peers = Some(args.peers);
        }

        if let Some(log_level) = args.log_level {
            self.instance.log.level = Some(log_level);
        }

        if let Some(log_destination) = args.log {
            self.instance.log.destination = Some(log_destination);
        }

        if let Some(audit_destination) = args.audit {
            self.instance.audit = Some(audit_destination);
        }

        self.instance.plugin_dir = args.plugin_dir;

        if let Some(admin_socket) = args.admin_sock {
            self.instance.admin_socket = Some(admin_socket);
        }

        if let Some(script) = args.script {
            self.instance.deprecated_script = Some(script);
        }

        if args.shredding {
            self.instance.shredding = Some(true);
        }

        if let Some(memtx_memory) = args.memtx_memory {
            self.instance.memtx.memory = Some(memtx_memory);
        }

        // --config-parameter has higher priority than other command line
        // arguments as a side effect of the fact that clap doesn't tell us
        // where the value came from: cli or env. Because we want
        // --config-parameter to have higher priority then other ENV
        // parameters, it must also be higher priority then other cli args.
        // Very sad...
        for item in args.config_parameter {
            let Some((path, yaml)) = item.split_once('=') else {
                return Err(Error::InvalidConfiguration(format!(
                    "failed parsing --config-parameter value: expected '=' after '{item}'"
                )));
            };
            self.set_field_from_yaml(path.trim(), yaml.trim())
                .map_err(Error::invalid_configuration)?;
        }

        Ok(())
    }

    /// Does checks which are applicable to configuration loaded from a file.
    fn validate_from_file(&self) -> Result<(), Error> {
        // XXX: This is kind of a hack. If a config file is provided we require
        // it to define the list of initial tiers. However if config file wasn't
        // specified, there's no way to define tiers, so we just ignore that
        // case and create a dummy "default" tier.
        if self.cluster.tiers.is_empty() {
            return Err(Error::invalid_configuration(
                "empty `cluster.tiers` section which is required to define the initial tiers",
            ));
        }

        match (&self.cluster.cluster_id, &self.instance.cluster_id) {
            (Some(from_cluster), Some(from_instance)) if from_cluster != from_instance => {
                return Err(Error::InvalidConfiguration(format!(
                    "`cluster.cluster_id` ({from_cluster}) conflicts with `instance.cluster_id` ({from_instance})",
                )));
            }
            (None, None) => {
                return Err(Error::invalid_configuration(
                    "either `cluster.cluster_id` or `instance.cluster_id` must be specified",
                ));
            }
            _ => {}
        }

        let mut unknown_parameters = vec![];
        if !self.unknown_sections.is_empty() {
            report_unknown_fields(
                "",
                &self.unknown_sections,
                PicodataConfig::FIELD_INFOS,
                &mut unknown_parameters,
            )
        }

        // TODO: for future compatibility this is a very bad approach. Image if
        // you will this scenario, picodata 69.3 has added a new parameter
        // "maximum_security" and now we want to add it to the config file.
        // But we have a config.yaml file for picodata 42.8 which is already deployed,
        // we add the new parameter to that config and then we try to start the
        // old picodata with the new config, and what we get? Error. Now you
        // have to go and copy-paste all of your config files and have at least
        // one for each picodata version. You probably want to this anyway, but
        // it's going to be incredibly annoying to deal with this when you're
        // just trying to tinker with stuff. For that reason we're gonna have a
        // parameter "strictness" or preferrably a better named one.
        if !self.cluster.unknown_parameters.is_empty() {
            report_unknown_fields(
                "cluster.",
                &self.cluster.unknown_parameters,
                ClusterConfig::FIELD_INFOS,
                &mut unknown_parameters,
            );
        }

        if !self.instance.unknown_parameters.is_empty() {
            report_unknown_fields(
                "instance.",
                &self.instance.unknown_parameters,
                InstanceConfig::FIELD_INFOS,
                &mut unknown_parameters,
            );
        }

        if !unknown_parameters.is_empty() {
            let mut buffer = Vec::with_capacity(512);
            _ = write!(&mut buffer, "unknown parameters: ");
            for (param, i) in unknown_parameters.iter().zip(1..) {
                _ = write!(&mut buffer, "`{}{}`", param.prefix, param.name);
                if let Some(best_match) = param.best_match {
                    _ = write!(&mut buffer, " (did you mean `{best_match}`?)");
                }
                if i != unknown_parameters.len() {
                    _ = write!(&mut buffer, ", ");
                }
            }
            let msg = String::from_utf8_lossy(&buffer);
            return Err(Error::InvalidConfiguration(msg.into()));
        }

        Ok(())
    }

    /// Does basic config validation. This function checks constraints
    /// applicable for all configuration updates.
    ///
    /// Does *NOT* set default values, that is the responsibility of getter
    /// methods.
    ///
    /// Checks specific to reloading a config file on an initialized istance are
    /// done in [`Self::validate_reload`].
    fn validate_common(&self) -> Result<(), Error> {
        for (name, info) in &self.cluster.tiers {
            if let Some(explicit_name) = &info.name {
                return Err(Error::InvalidConfiguration(format!(
                    "tier '{name}' has an explicit name field '{explicit_name}', which is not allowed. Tier name is always derived from the outer dictionary's key"
                )));
            }
        }

        Ok(())
    }

    #[allow(unused)]
    fn validate_reload(&self) -> Result<(), Error> {
        todo!()
    }

    /// Does validation of configuration parameters which are persisted in the
    /// storage.
    pub fn validate_storage(
        &self,
        storage: &storage::Clusterwide,
        raft_storage: &RaftSpaceAccess,
    ) -> Result<(), Error> {
        // Cluster id
        match (raft_storage.cluster_id()?, self.cluster_id()) {
            (from_storage, from_config) if from_storage != from_config => {
                return Err(Error::InvalidConfiguration(format!(
                    "instance restarted with a different `cluster_id`, which is not allowed, was: '{from_storage}' became: '{from_config}'"
                )));
            }
            _ => {}
        }

        // Instance id
        let mut instance_id = None;
        match (raft_storage.instance_id()?, &self.instance.instance_id) {
            (Some(from_storage), Some(from_config)) if from_storage != from_config => {
                return Err(Error::InvalidConfiguration(format!(
                    "instance restarted with a different `instance_id`, which is not allowed, was: '{from_storage}' became: '{from_config}'"
                )));
            }
            (Some(from_storage), _) => {
                instance_id = Some(from_storage);
            }
            _ => {}
        }

        // Replicaset id
        if let Some(instance_id) = &instance_id {
            if let Ok(instance_info) = storage.instances.get(instance_id) {
                match (&instance_info.replicaset_id, &self.instance.replicaset_id) {
                    (from_storage, Some(from_config)) if from_storage != from_config => {
                        return Err(Error::InvalidConfiguration(format!(
                            "instance restarted with a different `replicaset_id`, which is not allowed, was: '{from_storage}' became: '{from_config}'"
                        )));
                    }
                    _ => {}
                }
            }
        }

        // Tier
        match (&raft_storage.tier()?, &self.instance.tier) {
            (Some(from_storage), Some(from_config)) if from_storage != from_config => {
                return Err(Error::InvalidConfiguration(format!(
                    "instance restarted with a different `tier`, which is not allowed, was: '{from_storage}' became: '{from_config}'"
                )));
            }
            _ => {}
        }

        // Advertise address
        if let Some(raft_id) = raft_storage.raft_id()? {
            match (
                storage.peer_addresses.get(raft_id)?,
                &self.instance.advertise_address,
            ) {
                (Some(from_storage), Some(from_config))
                    if from_storage != from_config.to_host_port() =>
                {
                    return Err(Error::InvalidConfiguration(format!(
                        "instance restarted with a different `advertise_address`, which is not allowed, was: '{from_storage}' became: '{from_config}'"
                    )));
                }
                _ => {}
            }
        }

        Ok(())
    }

    #[inline]
    pub fn cluster_id(&self) -> &str {
        match (&self.instance.cluster_id, &self.cluster.cluster_id) {
            (Some(instance_cluster_id), Some(cluster_cluster_id)) => {
                assert_eq!(instance_cluster_id, cluster_cluster_id);
                instance_cluster_id
            }
            (Some(instance_cluster_id), None) => instance_cluster_id,
            (None, Some(cluster_cluster_id)) => cluster_cluster_id,
            (None, None) => "demo",
        }
    }
}

struct UnknownFieldInfo<'a> {
    name: &'a str,
    prefix: &'static str,
    best_match: Option<&'static str>,
}

fn report_unknown_fields<'a>(
    prefix: &'static str,
    unknown_fields: &'a HashMap<String, YamlValue>,
    known_fields: &[FieldInfo],
    report: &mut Vec<UnknownFieldInfo<'a>>,
) {
    for name in unknown_fields.keys() {
        debug_assert!(!known_fields.is_empty());
        let mut min_distance = usize::MAX;
        let mut maybe_best_match = None;
        for known_field in known_fields {
            let distance = edit_distance(name, known_field.name);
            if distance < min_distance {
                maybe_best_match = Some(known_field.name);
                min_distance = distance;
            }
        }

        let mut msg_postfix = String::new();
        if let Some(best_match) = maybe_best_match {
            let name_len = name.chars().count();
            let match_len = best_match.chars().count();
            if (min_distance as f64) / (usize::max(name_len, match_len) as f64) < 0.5 {
                msg_postfix = format!(", did you mean `{best_match}`?");
            } else {
                maybe_best_match = None;
            }
        }

        #[rustfmt::skip]
        tlog!(Warning, "Ignoring unknown parameter `{prefix}{name}`{msg_postfix}");
        report.push(UnknownFieldInfo {
            name,
            prefix,
            best_match: maybe_best_match,
        });
    }
}

////////////////////////////////////////////////////////////////////////////////
// ClusterConfig
////////////////////////////////////////////////////////////////////////////////

#[derive(
    PartialEq,
    Default,
    Debug,
    Clone,
    serde::Deserialize,
    serde::Serialize,
    tlua::Push,
    tlua::PushInto,
    Introspection,
)]
pub struct ClusterConfig {
    pub cluster_id: Option<String>,

    #[serde(deserialize_with = "deserialize_map_forbid_duplicate_keys")]
    pub tiers: HashMap<String, TierConfig>,

    /// Replication factor which is used for tiers which didn't specify one
    /// explicitly. For default value see [`Self::default_replication_factor()`].
    pub default_replication_factor: Option<u8>,

    #[serde(flatten)]
    #[introspection(ignore)]
    pub unknown_parameters: HashMap<String, YamlValue>,
}

impl ClusterConfig {
    pub fn tiers(&self) -> HashMap<String, Tier> {
        if self.tiers.is_empty() {
            return HashMap::from([(
                DEFAULT_TIER.to_string(),
                Tier {
                    name: DEFAULT_TIER.into(),
                    replication_factor: self.default_replication_factor(),
                },
            )]);
        }

        let mut tier_defs = HashMap::with_capacity(self.tiers.len());
        for (name, info) in &self.tiers {
            let replication_factor = info
                .replication_factor
                .unwrap_or_else(|| self.default_replication_factor());

            let tier_def = Tier {
                name: name.into(),
                replication_factor,
                // TODO: support other fields
            };
            tier_defs.insert(name.into(), tier_def);
        }
        tier_defs
    }

    #[inline]
    pub fn default_replication_factor(&self) -> u8 {
        self.default_replication_factor.unwrap_or(1)
    }
}

////////////////////////////////////////////////////////////////////////////////
// InstanceConfig
////////////////////////////////////////////////////////////////////////////////

#[derive(
    PartialEq,
    Default,
    Debug,
    Clone,
    serde::Deserialize,
    serde::Serialize,
    tlua::Push,
    tlua::PushInto,
    Introspection,
)]
pub struct InstanceConfig {
    pub data_dir: Option<String>,
    pub service_password_file: Option<String>,
    pub config_file: Option<String>,

    pub cluster_id: Option<String>,
    pub instance_id: Option<String>,
    pub replicaset_id: Option<String>,
    pub tier: Option<String>,
    pub failure_domain: Option<FailureDomain>,

    pub peers: Option<Vec<Address>>,
    pub advertise_address: Option<Address>,
    pub listen: Option<Address>,
    pub http_listen: Option<Address>,
    pub admin_socket: Option<String>,

    // TODO:
    // - sepparate config file for common parameters
    pub plugin_dir: Option<String>,
    pub deprecated_script: Option<String>,

    pub audit: Option<String>,

    // box.cfg.background currently doesn't work with our supervisor/child implementation
    // pub background: Option<bool>,

    //
    /// Determines wether the .xlog & .snap files should be shredded when
    /// deleting.
    pub shredding: Option<bool>,

    #[serde(default)]
    #[introspection(nested)]
    pub log: LogSection,

    #[serde(default)]
    #[introspection(nested)]
    pub memtx: MemtxSection,

    #[serde(default)]
    #[introspection(nested)]
    pub vinyl: VinylSection,

    #[serde(default)]
    #[introspection(nested)]
    pub iproto: IprotoSection,

    /// Special catch-all field which will be filled by serde with all unknown
    /// fields from the yaml file.
    #[serde(flatten)]
    #[introspection(ignore)]
    pub unknown_parameters: HashMap<String, YamlValue>,
}

impl InstanceConfig {
    #[inline]
    pub fn data_dir(&self) -> String {
        self.data_dir.clone().unwrap_or_else(|| ".".to_owned())
    }

    #[inline]
    pub fn instance_id(&self) -> Option<InstanceId> {
        self.instance_id.as_deref().map(InstanceId::from)
    }

    #[inline]
    pub fn replicaset_id(&self) -> Option<ReplicasetId> {
        self.replicaset_id.as_deref().map(ReplicasetId::from)
    }

    #[inline]
    pub fn tier(&self) -> &str {
        self.tier.as_deref().unwrap_or("default")
    }

    #[inline]
    pub fn failure_domain(&self) -> FailureDomain {
        self.failure_domain.clone().unwrap_or_default()
    }

    #[inline]
    pub fn peers(&self) -> Vec<Address> {
        match &self.peers {
            Some(peers) if !peers.is_empty() => peers.clone(),
            _ => vec![Address {
                user: None,
                host: DEFAULT_LISTEN_HOST.into(),
                port: DEFAULT_IPROTO_PORT.into(),
            }],
        }
    }

    #[inline]
    pub fn advertise_address(&self) -> Address {
        if let Some(advertise_address) = &self.advertise_address {
            advertise_address.clone()
        } else {
            self.listen()
        }
    }

    #[inline]
    pub fn listen(&self) -> Address {
        if let Some(listen) = &self.listen {
            listen.clone()
        } else {
            Address {
                user: None,
                host: DEFAULT_LISTEN_HOST.into(),
                port: DEFAULT_IPROTO_PORT.into(),
            }
        }
    }

    #[inline]
    pub fn admin_socket(&self) -> String {
        if let Some(admin_socket) = &self.admin_socket {
            admin_socket.to_owned()
        } else {
            format!("{}/admin.sock", self.data_dir())
        }
    }

    #[inline]
    pub fn log_level(&self) -> SayLevel {
        if let Some(level) = self.log.level {
            level.into()
        } else {
            SayLevel::Info
        }
    }

    #[inline]
    pub fn shredding(&self) -> bool {
        self.shredding.unwrap_or(false)
    }

    #[inline]
    pub fn memtx_memory(&self) -> u64 {
        self.memtx.memory.unwrap_or(64 * 1024 * 1024)
    }
}

////////////////////////////////////////////////////////////////////////////////
// MemtxSection
////////////////////////////////////////////////////////////////////////////////

#[derive(
    PartialEq,
    Default,
    Debug,
    Clone,
    serde::Deserialize,
    serde::Serialize,
    tlua::Push,
    tlua::PushInto,
    Introspection,
)]
#[serde(deny_unknown_fields)]
pub struct MemtxSection {
    /// How much memory is allocated to store tuples. When the limit is
    /// reached, INSERT or UPDATE requests begin failing with error
    /// ER_MEMORY_ISSUE. The server does not go beyond the memtx_memory limit to
    /// allocate tuples, but there is additional memory used to store indexes
    /// and connection information.
    ///
    /// Minimum is 32MB (32 * 1024 * 1024).
    ///
    /// Corresponds to `box.cfg.memtx_memory`.
    pub memory: Option<u64>,

    /// The maximum number of snapshots that are stored in the memtx_dir
    /// directory. If the number of snapshots after creating a new one exceeds
    /// this value, the Tarantool garbage collector deletes old snapshots. If
    /// the option is set to zero, the garbage collector does not delete old
    /// snapshots.
    ///
    /// Corresponds to `box.cfg.checkpoint_count`.
    pub checkpoint_count: Option<u64>,

    /// The interval in seconds between actions by the checkpoint daemon. If the
    /// option is set to a value greater than zero, and there is activity that
    /// causes change to a database, then the checkpoint daemon calls
    /// box.snapshot() every checkpoint_interval seconds, creating a new
    /// snapshot file each time. If the option is set to zero, the checkpoint
    /// daemon is disabled.
    ///
    /// Corresponds to `box.cfg.checkpoint_interval`.
    pub checkpoint_interval: Option<f64>,
}

tarantool::define_str_enum! {
    #[derive(Default)]
    pub enum MemtxAllocator {
        #[default]
        Small = "small",
        System = "system",
    }
}

////////////////////////////////////////////////////////////////////////////////
// VinylSection
////////////////////////////////////////////////////////////////////////////////

#[derive(
    PartialEq,
    Default,
    Debug,
    Clone,
    serde::Deserialize,
    serde::Serialize,
    tlua::Push,
    tlua::PushInto,
    Introspection,
)]
#[serde(deny_unknown_fields)]
pub struct VinylSection {
    /// The maximum number of in-memory bytes that vinyl uses.
    ///
    /// Corresponds to `box.cfg.vinyl_memory`
    pub memory: Option<u64>,

    /// The cache size for the vinyl storage engine.
    ///
    /// Corresponds to `box.cfg.vinyl_cache`
    pub cache: Option<u64>,
}

////////////////////////////////////////////////////////////////////////////////
// IprotoSection
////////////////////////////////////////////////////////////////////////////////

#[derive(
    PartialEq,
    Default,
    Debug,
    Clone,
    serde::Deserialize,
    serde::Serialize,
    tlua::Push,
    tlua::PushInto,
    Introspection,
)]
#[serde(deny_unknown_fields)]
pub struct IprotoSection {
    /// To handle messages, Tarantool allocates fibers. To prevent fiber
    /// overhead from affecting the whole system, Tarantool restricts how many
    /// messages the fibers handle, so that some pending requests are blocked.
    ///
    /// On powerful systems, increase net_msg_max and the scheduler will
    /// immediately start processing pending requests.
    ///
    /// On weaker systems, decrease net_msg_max and the overhead may decrease
    /// although this may take some time because the scheduler must wait until
    /// already-running requests finish.
    ///
    /// When net_msg_max is reached, Tarantool suspends processing of incoming
    /// packages until it has processed earlier messages. This is not a direct
    /// restriction of the number of fibers that handle network messages, rather
    /// it is a system-wide restriction of channel bandwidth. This in turn
    /// causes restriction of the number of incoming network messages that the
    /// transaction processor thread handles, and therefore indirectly affects
    /// the fibers that handle network messages. (The number of fibers is
    /// smaller than the number of messages because messages can be released as
    /// soon as they are delivered, while incoming requests might not be
    /// processed until some time after delivery.)
    ///
    /// On typical systems, the default value (768) is correct.
    ///
    /// Corresponds to `box.cfg.net_msg_max`
    pub max_concurrent_messages: Option<u64>,
}

////////////////////////////////////////////////////////////////////////////////
// LogSection
////////////////////////////////////////////////////////////////////////////////

#[derive(
    PartialEq,
    Default,
    Debug,
    Clone,
    serde::Deserialize,
    serde::Serialize,
    tlua::Push,
    tlua::PushInto,
    Introspection,
)]
#[serde(deny_unknown_fields)]
pub struct LogSection {
    pub level: Option<args::LogLevel>,

    /// By default, Picodata sends the log to the standard error stream
    /// (stderr). If `log.destination` is specified, Picodata can send the log to a:
    /// - file
    /// - pipe
    /// - system logger
    pub destination: Option<String>,

    pub format: Option<LogFormat>,
}

tarantool::define_str_enum! {
    #[derive(Default)]
    pub enum LogFormat {
        #[default]
        Plain = "plain",
        Json = "json",
    }
}

////////////////////////////////////////////////////////////////////////////////
// deserialize_map_forbid_duplicate_keys
////////////////////////////////////////////////////////////////////////////////

pub fn deserialize_map_forbid_duplicate_keys<'de, D, K, V>(
    des: D,
) -> Result<HashMap<K, V>, D::Error>
where
    D: serde::Deserializer<'de>,
    K: serde::Deserialize<'de> + std::hash::Hash + Eq + std::fmt::Display,
    V: serde::Deserialize<'de>,
{
    use std::collections::hash_map::Entry;
    use std::marker::PhantomData;
    struct Visitor<K, V>(PhantomData<(K, V)>);

    impl<'de, K, V> serde::de::Visitor<'de> for Visitor<K, V>
    where
        K: serde::Deserialize<'de> + std::hash::Hash + Eq + std::fmt::Display,
        V: serde::Deserialize<'de>,
    {
        type Value = HashMap<K, V>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a map with unique keys")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            let mut res = HashMap::with_capacity(map.size_hint().unwrap_or(16));
            while let Some((key, value)) = map.next_entry::<K, V>()? {
                match res.entry(key) {
                    Entry::Occupied(e) => {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate key `{}` found",
                            e.key()
                        )));
                    }
                    Entry::Vacant(e) => {
                        e.insert(value);
                    }
                }
            }

            Ok(res)
        }
    }

    des.deserialize_map(Visitor::<K, V>(PhantomData))
}

////////////////////////////////////////////////////////////////////////////////
// ElectionMode
////////////////////////////////////////////////////////////////////////////////

tarantool::define_str_enum! {
    #[derive(Default)]
    pub enum ElectionMode {
        #[default]
        Off = "off",
        Voter = "voter",
        Candidate = "candidate",
        Manual = "manual",
    }
}

tarantool::define_str_enum! {
    #[derive(Default)]
    pub enum BootstrapStrategy {
        #[default]
        Auto = "auto",
        Legacy = "legacy",
    }
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::on_scope_exit;
    use clap::Parser as _;

    #[test]
    fn config_from_yaml() {
        //
        // Empty config is while useless still valid
        //
        let yaml = r###"
"###;
        let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
        // By default it deserializes as Value::Mapping for some reason
        config.unknown_sections = Default::default();
        assert_eq!(config, PicodataConfig::default());

        let yaml = r###"
cluster:
    cluster_id: foobar

    tiers:
        voter:
            can_vote: true

        storage:
            replication_factor: 3
            can_vote: false

        search-index:
            replication_factor: 2
            can_vote: false

instance:
    instance_id: voter1

goo-goo: ga-ga
"###;

        _ = PicodataConfig::read_yaml_contents(&yaml).unwrap();
    }

    #[test]
    fn duplicate_tiers_is_error() {
        let yaml = r###"
cluster:
    tiers:
        voter:
            can_vote: true

        voter:
            can_vote: false

"###;

        let err = PicodataConfig::read_yaml_contents(&yaml.trim_start()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid configuration: cluster.tiers: duplicate key `voter` found at line 3 column 9"
        );
    }

    #[test]
    fn cluster_id_is_required() {
        let yaml = r###"
cluster:
    tiers:
        default:
instance:
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap();
        let err = config.validate_from_file().unwrap_err();
        assert_eq!(err.to_string(), "invalid configuration: either `cluster.cluster_id` or `instance.cluster_id` must be specified");

        let yaml = r###"
cluster:
    cluster_id: foo
    tiers:
        default:
instance:
    cluster_id: bar
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap();
        let err = config.validate_from_file().unwrap_err();
        assert_eq!(err.to_string(), "invalid configuration: `cluster.cluster_id` (foo) conflicts with `instance.cluster_id` (bar)");
    }

    #[test]
    fn missing_tiers_is_error() {
        let yaml = r###"
cluster:
    cluster_id: test
"###;
        let err = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid configuration: cluster: missing field `tiers` at line 2 column 5"
        );

        let yaml = r###"
cluster:
    cluster_id: test
    tiers:
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap();
        let err = config.validate_from_file().unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid configuration: empty `cluster.tiers` section which is required to define the initial tiers"
        );

        let yaml = r###"
cluster:
    cluster_id: test
    tiers:
        default:
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap();
        config.validate_from_file().unwrap();
    }

    #[test]
    fn spaces_in_addresses() {
        let yaml = r###"
instance:
    listen:  kevin:  <- spacey
"###;
        let err = PicodataConfig::read_yaml_contents(&yaml.trim_start()).unwrap_err();
        #[rustfmt::skip]
        assert_eq!(err.to_string(), "invalid configuration: mapping values are not allowed in this context at line 2 column 19");

        let yaml = r###"
instance:
    listen:  kevin->  :spacey   # <- some more trailing space
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim_start()).unwrap();
        let listen = config.instance.listen.unwrap();
        assert_eq!(listen.host, "kevin->  ");
        assert_eq!(listen.port, "spacey");

        let yaml = r###"
instance:
    listen:  kevin->  <-spacey
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim_start()).unwrap();
        let listen = config.instance.listen.unwrap();
        assert_eq!(listen.host, "kevin->  <-spacey");
        assert_eq!(listen.port, "3301");
    }

    #[test]
    fn default_http_port() {
        let yaml = r###"
instance:
    http_listen: localhost
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim_start()).unwrap();
        let listen = config.instance.http_listen.unwrap();
        assert_eq!(listen.host, "localhost");
        assert_eq!(listen.port, "3301"); // FIXME: This is wrong! The default
                                         // http port should be something different
    }

    #[rustfmt::skip]
    #[test]
    fn parameter_source_precedence() {
        // Save environment before test, to avoid breaking something unrelated
        let env_before: Vec<_> = std::env::vars()
            .filter(|(k, _)| k.starts_with("PICODATA_"))
            .collect();
        for (k, _) in &env_before {
            std::env::remove_var(k);
        }
        let _guard = on_scope_exit(|| {
            for (k, v) in env_before {
                std::env::set_var(k, v);
            }
        });

        //
        // Defaults
        //
        {
            let mut config = PicodataConfig::default();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args).unwrap();

            assert_eq!(
                config.instance.peers(),
                vec![Address {
                    user: None,
                    host: "localhost".into(),
                    port: "3301".into(),
                }]
            );
            assert_eq!(config.instance.instance_id(), None);
            assert_eq!(config.instance.listen().to_host_port(), "localhost:3301");
            assert_eq!(config.instance.advertise_address().to_host_port(), "localhost:3301");
            assert_eq!(config.instance.log_level(), SayLevel::Info);
            assert!(config.instance.failure_domain().data.is_empty());
        }

        //
        // Precedence: command line > env > config
        //
        {
            let yaml = r###"
instance:
    instance_id: I-CONFIG
"###;

            // only config
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args).unwrap();

            assert_eq!(config.instance.instance_id().unwrap(), "I-CONFIG");

            // PICODATA_CONFIG_PARAMETERS > config
            std::env::set_var("PICODATA_CONFIG_PARAMETERS", "instance.instance_id=I-ENV-CONF-PARAM");
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args).unwrap();

            assert_eq!(config.instance.instance_id().unwrap(), "I-ENV-CONF-PARAM");

            // other env > PICODATA_CONFIG_PARAMETERS
            std::env::set_var("PICODATA_INSTANCE_ID", "I-ENVIRON");
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args).unwrap();

            assert_eq!(config.instance.instance_id().unwrap(), "I-ENVIRON");

            // command line > env
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run", "--instance-id=I-COMMANDLINE"]).unwrap();
            config.set_from_args(args).unwrap();

            assert_eq!(config.instance.instance_id().unwrap(), "I-COMMANDLINE");

            // -c PARAMETER=VALUE > other command line
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run", "-c", "instance.instance_id=I-CLI-CONF-PARAM", "--instance-id=I-COMMANDLINE"]).unwrap();
            config.set_from_args(args).unwrap();

            assert_eq!(config.instance.instance_id().unwrap(), "I-CLI-CONF-PARAM");
        }

        //
        // peers parsing
        //
        {
            // empty config means default
            let yaml = r###"
instance:
    peers:
"###;
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args).unwrap();

            assert_eq!(
                config.instance.peers(),
                vec![
                    Address {
                        user: None,
                        host: "localhost".into(),
                        port: "3301".into(),
                    }
                ]
            );

            // only config
            let yaml = r###"
instance:
    peers:
        - bobbert:420
        - tomathan:69
"###;
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args).unwrap();

            assert_eq!(
                config.instance.peers(),
                vec![
                    Address {
                        user: None,
                        host: "bobbert".into(),
                        port: "420".into(),
                    },
                    Address {
                        user: None,
                        host: "tomathan".into(),
                        port: "69".into(),
                    }
                ]
            );

            // env > config
            std::env::set_var("PICODATA_PEER", "oops there's a space over here -> <-:13,             maybe we should at least strip these:37");
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args).unwrap();

            assert_eq!(
                config.instance.peers(),
                vec![
                    Address {
                        user: None,
                        host: "oops there's a space over here -> <-".into(),
                        port: "13".into(),
                    },
                    Address {
                        user: None,
                        host: "             maybe we should at least strip these".into(),
                        port: "37".into(),
                    }
                ]
            );

            // command line > env
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run",
                "--peer", "one:1",
                "--peer", "two:2,    <- same problem here,:3,4"
            ]).unwrap();
            config.set_from_args(args).unwrap();

            assert_eq!(
                config.instance.peers(),
                vec![
                    Address {
                        user: None,
                        host: "one".into(),
                        port: "1".into(),
                    },
                    Address {
                        user: None,
                        host: "two".into(),
                        port: "2".into(),
                    },
                    Address {
                        user: None,
                        host: "    <- same problem here".into(),
                        port: "3301".into(),
                    },
                    Address {
                        user: None,
                        host: "localhost".into(),
                        port: "3".into(),
                    },
                    Address {
                        user: None,
                        host: "4".into(),
                        port: "3301".into(),
                    }
                ]
            );

            // --config-parameter > --peer
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run",
                "-c", "instance.peers=[  host:123  , ghost :321, гост:666]",
            ]).unwrap();
            config.set_from_args(args).unwrap();

            assert_eq!(
                config.instance.peers(),
                vec![
                    Address {
                        user: None,
                        host: "host".into(),
                        port: "123".into(),
                    },
                    Address {
                        user: None,
                        host: "ghost ".into(),
                        port: "321".into(),
                    },
                    Address {
                        user: None,
                        host: "гост".into(),
                        port: "666".into(),
                    },
                ]
            );
        }

        //
        // Advertise = listen unless specified explicitly
        //
        {
            std::env::set_var("PICODATA_LISTEN", "L-ENVIRON");
            let mut config = PicodataConfig::read_yaml_contents("").unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args).unwrap();

            assert_eq!(config.instance.listen().to_host_port(), "L-ENVIRON:3301");
            assert_eq!(config.instance.advertise_address().to_host_port(), "L-ENVIRON:3301");

            let yaml = r###"
instance:
    advertise_address: A-CONFIG
"###;
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args).unwrap();

            assert_eq!(config.instance.listen().to_host_port(), "L-ENVIRON:3301");
            assert_eq!(config.instance.advertise_address().to_host_port(), "A-CONFIG:3301");

            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run", "-l", "L-COMMANDLINE"]).unwrap();
            config.set_from_args(args).unwrap();

            assert_eq!(config.instance.listen().to_host_port(), "L-COMMANDLINE:3301");
            assert_eq!(config.instance.advertise_address().to_host_port(), "A-CONFIG:3301");
        }

        //
        // Failure domain is parsed correctly
        //
        {
            // FIXME: duplicate keys in failure-domain should be a hard error
            // config
            let yaml = r###"
instance:
    failure_domain:
        kconf1: vconf1
        kconf2: vconf2
        kconf2: vconf2-replaced
"###;
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args).unwrap();
            assert_eq!(
                config.instance.failure_domain(),
                FailureDomain::from([("KCONF1", "VCONF1"), ("KCONF2", "VCONF2-REPLACED")])
            );

            // environment
            std::env::set_var("PICODATA_FAILURE_DOMAIN", "kenv1=venv1,kenv2=venv2,kenv2=venv2-replaced");
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args).unwrap();
            assert_eq!(
                config.instance.failure_domain(),
                FailureDomain::from([("KENV1", "VENV1"), ("KENV2", "VENV2-REPLACED")])
            );

            // command line
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run", "--failure-domain", "karg1=varg1,karg1=varg1-replaced"]).unwrap();
            config.set_from_args(args).unwrap();
            assert_eq!(
                config.instance.failure_domain(),
                FailureDomain::from([("KARG1", "VARG1-REPLACED")])
            );

            // command line more complex
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run",
                "--failure-domain", "foo=1",
                "--failure-domain", "bar=2,baz=3"
            ]).unwrap();
            config.set_from_args(args).unwrap();
            assert_eq!(
                config.instance.failure_domain(),
                FailureDomain::from([
                    ("FOO", "1"),
                    ("BAR", "2"),
                    ("BAZ", "3"),
                ])
            );

            // --config-parameter
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run",
                "-c", "instance.failure_domain={foo: '11', bar: '22'}"
            ]).unwrap();
            config.set_from_args(args).unwrap();
            assert_eq!(
                config.instance.failure_domain(),
                FailureDomain::from([
                    ("FOO", "11"),
                    ("BAR", "22"),
                ])
            );
        }

        // --config-parameter + PICODATA_CONFIG_PARAMETERS
        {
            let yaml = r###"
instance:
    tier: this-will-be-overloaded
"###;
            std::env::set_var(
                "PICODATA_CONFIG_PARAMETERS",
                "  instance.tier = ABC;cluster.cluster_id=DEF  ;
                instance.audit=audit.txt ;;
                ; instance.data_dir=. ;"
            );
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run",
                "-c", "  instance.log .level =debug  ",
                "--config-parameter", "instance. memtx . memory=  0xdeadbeef",
            ]).unwrap();
            config.set_from_args(args).unwrap();
            assert_eq!(config.instance.tier.unwrap(), "ABC");
            assert_eq!(config.cluster.cluster_id.unwrap(), "DEF");
            assert_eq!(config.instance.log.level.unwrap(), args::LogLevel::Debug);
            assert_eq!(config.instance.memtx.memory.unwrap(), 0xdead_beef);
            assert_eq!(config.instance.audit.unwrap(), "audit.txt");
            assert_eq!(config.instance.data_dir.unwrap(), ".");

            //
            // Errors
            //
            let yaml = r###"
"###;
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run",
                "-c", "  instance.hoobabooba = malabar  ",
            ]).unwrap();
            let e = config.set_from_args(args).unwrap_err();
            assert!(dbg!(e.to_string()).starts_with("invalid configuration: instance: unknown field `hoobabooba`, expected one of"));

            let yaml = r###"
"###;
            std::env::set_var(
                "PICODATA_CONFIG_PARAMETERS",
                "  cluster.cluster_id=DEF  ;
                  cluster.asdfasdfbasdfbasd = "
            );
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            let e = config.set_from_args(args).unwrap_err();
            assert!(dbg!(e.to_string()).starts_with("invalid configuration: cluster: unknown field `asdfasdfbasdfbasd`, expected one of"));
        }
    }
}
