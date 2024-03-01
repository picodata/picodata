use crate::address::Address;
use crate::cli::args;
use crate::failure_domain::FailureDomain;
use crate::instance::InstanceId;
use crate::replicaset::ReplicasetId;
use crate::storage;
use crate::tier::Tier;
use crate::tier::TierConfig;
use crate::tier::DEFAULT_TIER;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::RaftSpaceAccess;
use crate::util::file_exists;
use std::collections::HashMap;
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
)]
#[serde(deny_unknown_fields)]
pub struct PicodataConfig {
    // TODO: add a flag for each parameter specifying where it came from, i.e.:
    // - default value
    // - configuration file
    // - command line arguments
    // - environment variable
    // - persisted storage (because some of the values are read from the storage on restart)
    #[serde(default)]
    pub cluster: ClusterConfig,

    #[serde(default)]
    pub instance: InstanceConfig,
    // TODO: currently this doesn't compile, because serde_json::Value doesn't implement tlua::Push
    // But if we had this, we could report a warning if unknown fields were specified
    // #[serde(flatten)]
    // pub unknown_sections: HashMap<String, serde_json::Value>,
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

                config = Some(Self::read_yaml_file(args_path)?);
            }
            (Some(args_path), false) => {
                config = Some(Self::read_yaml_file(args_path)?);
            }
            (None, true) => {
                #[rustfmt::skip]
                tlog!(Info, "Reading configuration file '{DEFAULT_CONFIG_FILE_NAME}' in the current working directory '{cwd}'.");
                config = Some(Self::read_yaml_file(&default_path)?);
            }
            (None, false) => {}
        }

        if let Some(config) = &config {
            config.validate_from_file()?;
        }
        let mut config = config.unwrap_or_default();

        config.set_from_args(args);

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

    pub fn set_from_args(&mut self, args: args::Run) {
        // TODO: add forbid_conflicts_with_args so that it's considered an error
        // if a parameter is specified both in the config and in the command line
        // arguments

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
            self.instance.log_level = Some(log_level);
        }

        if let Some(log_destination) = args.log {
            self.instance.log = Some(log_destination);
        }

        if let Some(audit_destination) = args.audit {
            self.instance.audit = Some(audit_destination);
        }

        if !args.plugins.is_empty() {
            self.instance.plugins = Some(args.plugins);
        }

        if let Some(script) = args.script {
            self.instance.deprecated_script = Some(script);
        }

        if args.shredding {
            self.instance.shredding = Some(true);
        }

        if let Some(memtx_memory) = args.memtx_memory {
            self.instance.memtx_memory = Some(memtx_memory);
        }

        // TODO: the rest
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
)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub cluster_id: Option<String>,

    #[serde(deserialize_with = "deserialize_map_forbid_duplicate_keys")]
    pub tiers: HashMap<String, TierConfig>,

    /// Replication factor which is used for tiers which didn't specify one
    /// explicitly. For default value see [`Self::default_replication_factor()`].
    pub default_replication_factor: Option<u8>,
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
)]
#[serde(deny_unknown_fields)]
pub struct InstanceConfig {
    pub data_dir: Option<String>,
    pub service_password_file: Option<String>,

    pub cluster_id: Option<String>,
    pub instance_id: Option<String>,
    pub replicaset_id: Option<String>,
    pub tier: Option<String>,
    pub failure_domain: Option<FailureDomain>,

    // TODO: should this be in `cluster` section?
    pub peers: Option<Vec<Address>>,
    pub advertise_address: Option<Address>,
    pub listen: Option<Address>,
    pub http_listen: Option<Address>,
    pub admin_socket: Option<String>,

    // TODO:
    // - more nested sections!
    // - dynamic parameters should be marked dynamic!
    // - sepparate config file for common parameters
    pub plugins: Option<Vec<String>>,
    pub deprecated_script: Option<String>,

    pub audit: Option<String>,
    pub log_level: Option<args::LogLevel>,
    pub log: Option<String>,
    pub log_format: Option<String>,

    // pub background: Option<bool>,
    // pub pid_file: Option<String>,

    //
    pub shredding: Option<bool>,
    // pub core_strip: Option<bool>,
    // pub core_dump: Option<bool>,

    // pub force_recovery: Option<bool>,
    // pub too_long_threshold: Option<f64>,
    // pub hot_standby: Option<bool>,
    // pub wal_queue_max_size: Option<u64>,
    // pub wal_max_size: Option<u64>,
    // pub wal_mode: Option<String>,
    // pub checkpoint_wal_threshold: Option<f64>,
    // pub wal_cleanup_delay: Option<f64>,
    // pub wal_dir_rescan_delay: Option<f64>,

    //
    pub memtx_memory: Option<u64>,
    // pub memtx_min_tuple_size: Option<u64>,
    // pub memtx_use_mvcc_engine: Option<bool>,
    // pub memtx_allocator: Option<String>,
    // pub memtx_checkpoint_count: Option<u64>,
    // pub memtx_checkpoint_interval: Option<f64>,

    // pub slab_alloc_factor: Option<u64>,
    // pub slab_alloc_granularity: Option<u8>,

    // pub vinyl_cache: Option<u64>,
    // pub vinyl_write_threads: Option<u8>,
    // pub vinyl_defer_deletes: Option<bool>,
    // pub vinyl_run_count_per_level: Option<u64>,
    // pub vinyl_run_size_ratio: Option<f64>,
    // pub vinyl_memory: Option<u64>,
    // pub vinyl_read_threads: Option<u8>,
    // pub vinyl_page_size: Option<u64>,
    // pub vinyl_bloom_fpr: Option<f64>,
    // pub vinyl_timeout: Option<f64>,

    // pub txn_isolation: Option<String>,
    // pub txn_timeout: Option<f64>,

    // pub auth_type: Option<String>,
    // pub iproto_msg_max: Option<u64>,
    // pub iproto_readahead: Option<u64>,
    // pub iproto_threads: Option<u8>,

    // Not configurable currently
    // pub read_only: Option<bool>,
    // pub bootstrap_strategy: Option<String>,
    // pub replication_anon: Option<bool>,
    // pub replication_connect_timeout: Option<f64>,
    // pub replication_skip_conflict: Option<bool>,
    // pub replication_sync_lag: Option<f64>,
    // pub replication_sync_timeout: Option<f64>,
    // pub replication_synchro_quorum: Option<String>,
    // pub replication_synchro_timeout: Option<f64>,
    // pub replication_timeout: Option<f64>,
    // pub replication_threads: Option<u8>,
    // pub election_fencing_mode: Option<String>,
    // pub election_mode: Option<ElectionMode>,
    // pub election_timeout: Option<f64>,

    // TODO: correct type
    // pub metrics: Option<()>,

    // pub feedback_enabled: Option<bool>,
    // pub feedback_interval: Option<f64>,
    // pub feedback_host: Option<String>,
    // pub feedback_send_metrics: Option<bool>,
    // pub feedback_metrics_collect_interval: Option<f64>,
    // pub feedback_metrics_limit: Option<u64>,
    // pub feedback_crashinfo: Option<bool>,

    // pub sql_cache_size: Option<u64>,

    // pub worker_pool_threads: Option<u8>,
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
        if let Some(level) = self.log_level {
            level.into()
        } else {
            SayLevel::Info
        }
    }

    // TODO: give a default value for audit
    // pub fn audit(&self) -> String {}

    #[inline]
    pub fn shredding(&self) -> bool {
        self.shredding.unwrap_or(false)
    }

    // TODO
    // pub core_strip: Option<bool>,
    // pub core_dump: Option<bool>,
    // pub force_recovery: Option<bool>,

    // pub net_msg_max: Option<u64>,
    // pub too_long_threshold: Option<f64>,

    #[inline]
    pub fn memtx_memory(&self) -> u64 {
        self.memtx_memory.unwrap_or(64 * 1024 * 1024)
    }

    // TODO
    // pub memtx_min_tuple_size: Option<u64>,
    // pub memtx_use_mvcc_engine: Option<bool>,
    // pub memtx_allocator: Option<String>,
    // pub memtx_checkpoint_count: Option<u64>,
    // pub memtx_checkpoint_interval: Option<f64>,

    // pub slab_alloc_factor: Option<u64>,

    // pub vinyl_cache: Option<u64>,
    // pub vinyl_write_threads: Option<u8>,
    // pub vinyl_defer_deletes: Option<bool>,
    // pub vinyl_run_count_per_level: Option<u64>,
    // pub vinyl_run_size_ratio: Option<f64>,
    // pub vinyl_memory: Option<u64>,
    // pub vinyl_read_threads: Option<u8>,
    // pub vinyl_page_size: Option<u64>,
    // pub vinyl_bloom_fpr: Option<f64>,

    // pub replication_skip_conflict: Option<bool>,
    // pub wal_queue_max_size: Option<u64>,
    // pub replication_anon: Option<bool>,
    // pub replication_sync_lag: Option<f64>,
    // pub wal_max_size: Option<u64>,
    // pub background: Option<bool>,
    // pub feedback_send_metrics: Option<bool>,
    // pub txn_isolation: Option<String>,
    // pub replication_synchro_quorum: Option<String>,
    // pub wal_mode: Option<String>,
    // pub checkpoint_wal_threshold: Option<u64>,
    // pub replication_sync_timeout: Option<f64>,
    // pub readahead: Option<u64>,

    // pub feedback_host: Option<String>,
    // pub feedback_metrics_collect_interval: Option<f64>,
    // // FIXME TODO:
    // pub metrics: Option<()>,
    // pub feedback_crashinfo: Option<bool>,
    // pub feedback_enabled: Option<bool>,
    // pub feedback_interval: Option<f64>,
    // pub feedback_metrics_limit: Option<u64>,

    // pub replication_connect_timeout: Option<f64>,
    // pub replication_timeout: Option<f64>,
    // pub auth_type: Option<String>,
    // pub election_timeout: Option<f64>,
    // pub election_fencing_mode: Option<String>,
    // pub election_mode: Option<String>,
    // pub wal_cleanup_delay: Option<f64>,
    // pub vinyl_timeout: Option<f64>,
    // pub bootstrap_strategy: Option<String>,
    // pub worker_pool_threads: Option<u8>,
    // pub txn_timeout: Option<f64>,
    // pub slab_alloc_granularity: Option<u8>,
    // pub replication_synchro_timeout: Option<f64>,
    // pub hot_standby: Option<bool>,
    // pub read_only: Option<bool>,
    // pub replication_threads: Option<u8>,
    // pub iproto_threads: Option<u8>,
    // pub wal_dir_rescan_delay: Option<f64>,
    // pub sql_cache_size: Option<u64>,
}

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
        let config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
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
            config.set_from_args(args);

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
            config.set_from_args(args);

            assert_eq!(config.instance.instance_id().unwrap(), "I-CONFIG");

            // env > config
            std::env::set_var("PICODATA_INSTANCE_ID", "I-ENVIRON");
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args);

            assert_eq!(config.instance.instance_id().unwrap(), "I-ENVIRON");

            // command line > env
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run", "--instance-id=I-COMMANDLINE"]).unwrap();
            config.set_from_args(args);

            assert_eq!(config.instance.instance_id().unwrap(), "I-COMMANDLINE");
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
            config.set_from_args(args);

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
            config.set_from_args(args);

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
            config.set_from_args(args);

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
            config.set_from_args(args);

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
        }

        //
        // Advertise = listen unless specified explicitly
        //
        {
            std::env::set_var("PICODATA_LISTEN", "L-ENVIRON");
            let mut config = PicodataConfig::read_yaml_contents("").unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args);

            assert_eq!(config.instance.listen().to_host_port(), "L-ENVIRON:3301");
            assert_eq!(config.instance.advertise_address().to_host_port(), "L-ENVIRON:3301");

            let yaml = r###"
instance:
    advertise_address: A-CONFIG
"###;
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args);

            assert_eq!(config.instance.listen().to_host_port(), "L-ENVIRON:3301");
            assert_eq!(config.instance.advertise_address().to_host_port(), "A-CONFIG:3301");

            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run", "-l", "L-COMMANDLINE"]).unwrap();
            config.set_from_args(args);

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
            config.set_from_args(args);
            assert_eq!(
                config.instance.failure_domain(),
                FailureDomain::from([("KCONF1", "VCONF1"), ("KCONF2", "VCONF2-REPLACED")])
            );

            // environment
            std::env::set_var("PICODATA_FAILURE_DOMAIN", "kenv1=venv1,kenv2=venv2,kenv2=venv2-replaced");
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run"]).unwrap();
            config.set_from_args(args);
            assert_eq!(
                config.instance.failure_domain(),
                FailureDomain::from([("KENV1", "VENV1"), ("KENV2", "VENV2-REPLACED")])
            );

            // command line
            let mut config = PicodataConfig::read_yaml_contents(&yaml).unwrap();
            let args = args::Run::try_parse_from(["run", "--failure-domain", "karg1=varg1,karg1=varg1-replaced"]).unwrap();
            config.set_from_args(args);
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
            config.set_from_args(args);
            assert_eq!(
                config.instance.failure_domain(),
                FailureDomain::from([
                    ("FOO", "1"),
                    ("BAR", "2"),
                    ("BAZ", "3"),
                ])
            );
        }
    }
}
