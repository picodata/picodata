use crate::address::Address;
use crate::cli::args;
use crate::cli::args::CONFIG_PARAMETERS_ENV;
use crate::failure_domain::FailureDomain;
use crate::instance::InstanceId;
use crate::introspection::leaf_field_paths;
use crate::introspection::FieldInfo;
use crate::introspection::Introspection;
use crate::pgproto;
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
use serde_yaml::Value as YamlValue;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use tarantool::log::SayLevel;
use tarantool::tlua;

pub const DEFAULT_USERNAME: &str = "guest";
pub const DEFAULT_LISTEN_HOST: &str = "localhost";
pub const DEFAULT_IPROTO_PORT: &str = "3301";
pub const DEFAULT_CONFIG_FILE_NAME: &str = "config.yaml";

////////////////////////////////////////////////////////////////////////////////
// PicodataConfig
////////////////////////////////////////////////////////////////////////////////

#[derive(PartialEq, Default, Debug, Clone, serde::Deserialize, serde::Serialize, Introspection)]
pub struct PicodataConfig {
    #[serde(default)]
    #[introspection(nested)]
    pub cluster: ClusterConfig,

    #[serde(default)]
    #[introspection(nested)]
    pub instance: InstanceConfig,

    #[serde(flatten)]
    #[introspection(ignore)]
    pub unknown_sections: HashMap<String, YamlValue>,

    #[serde(skip)]
    #[introspection(ignore)]
    pub parameter_sources: ParameterSourcesMap,
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

static mut GLOBAL_CONFIG: Option<Box<PicodataConfig>> = None;

impl PicodataConfig {
    // TODO:
    // fn default() -> Self
    // which returns an instance of config with all the default parameters.
    // Also add a command to generate a default config from command line.
    pub fn init(args: args::Run) -> Result<&'static Self, Error> {
        validate_args(&args)?;

        let cwd = std::env::current_dir();
        let cwd = cwd.as_deref().unwrap_or_else(|_| Path::new("."));
        let default_path = cwd.join(DEFAULT_CONFIG_FILE_NAME);
        let args_path = args.config.clone().map(|path| {
            if path.is_absolute() {
                path
            } else {
                cwd.join(path)
            }
        });

        let mut config_from_file = None;
        match (&args_path, file_exists(&default_path)) {
            (Some(args_path), true) => {
                if args_path != &default_path {
                    let cwd = cwd.display();
                    let args_path = args_path.display();
                    #[rustfmt::skip]
                    tlog!(Warning, "A path to configuration file '{args_path}' was provided explicitly,
but a '{DEFAULT_CONFIG_FILE_NAME}' file in the current working directory '{cwd}' also exists.
Using configuration file '{args_path}'.");
                }

                config_from_file = Some((Self::read_yaml_file(args_path)?, args_path));
            }
            (Some(args_path), false) => {
                config_from_file = Some((Self::read_yaml_file(args_path)?, args_path));
            }
            (None, true) => {
                #[rustfmt::skip]
                tlog!(Info, "Reading configuration file '{DEFAULT_CONFIG_FILE_NAME}' in the current working directory '{}'.", cwd.display());
                config_from_file = Some((Self::read_yaml_file(&default_path)?, &default_path));
            }
            (None, false) => {}
        }

        let mut parameter_sources = ParameterSourcesMap::default();
        let mut config = if let Some((mut config, path)) = config_from_file {
            config.validate_from_file()?;
            #[rustfmt::skip]
            mark_non_none_field_sources(&mut parameter_sources, &config, ParameterSource::ConfigFile);

            if args.config.is_some() {
                #[rustfmt::skip]
                parameter_sources.insert("instance.config_file".into(), ParameterSource::CommandlineOrEnvironment);
            }

            config.instance.config_file = Some(path.clone());
            config
        } else {
            Default::default()
        };

        config.set_from_args_and_env(args, &mut parameter_sources)?;

        config.set_defaults_explicitly(&parameter_sources);

        config.validate_common()?;

        config.parameter_sources = parameter_sources;

        // Safe, because we only initialize config once in a single thread.
        let config_ref = unsafe {
            assert!(GLOBAL_CONFIG.is_none());
            GLOBAL_CONFIG.insert(config)
        };

        Ok(config_ref)
    }

    #[inline(always)]
    pub fn get() -> &'static Self {
        // Safe, because we only mutate GLOBAL_CONFIG once and
        // strictly before anybody calls this function.
        unsafe {
            GLOBAL_CONFIG
                .as_ref()
                .expect("shouldn't be called before config is initialized")
        }
    }

    /// Generates the configuration with parameters set to the default values
    /// where applicable.
    ///
    /// Note that this is different from [`Self::default`], which returns an
    /// "empty" instance of `Self`, because we need to distinguish between
    /// paramteres being unspecified and parameters being set to the default
    /// value explicitly.
    pub fn with_defaults() -> Self {
        let mut config = Self::default();
        config.set_defaults_explicitly(&Default::default());

        // This isn't set by set_defaults_explicitly, because we expect the
        // tiers to be specified explicitly when the config file is provided.
        config.cluster.tier = Some(HashMap::from([(
            DEFAULT_TIER.into(),
            TierConfig {
                replication_factor: Some(1),
                can_vote: true,
                ..Default::default()
            },
        )]));

        // Same story as with `cluster.tier`, the cluster_id must be provided
        // in the config file.
        config.cluster.cluster_id = Some("demo".into());

        config
    }

    #[inline]
    pub fn read_yaml_file(path: impl AsRef<Path>) -> Result<Box<Self>, Error> {
        let contents = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            Error::other(format!(
                "can't read from '{}': {e}",
                path.as_ref().display()
            ))
        })?;
        Self::read_yaml_contents(&contents)
    }

    #[inline]
    pub fn read_yaml_contents(contents: &str) -> Result<Box<Self>, Error> {
        let config = serde_yaml::from_str(contents).map_err(Error::invalid_configuration)?;
        Ok(config)
    }

    /// Sets parameters of `self` to values from `args` as well as current
    /// environment variables. Updates `parameter_sources` specifying the source
    /// for each parameter which is updated by this function.
    pub fn set_from_args_and_env(
        &mut self,
        args: args::Run,
        parameter_sources: &mut ParameterSourcesMap,
    ) -> Result<(), Error> {
        let mut config_from_args = Self::default();

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

                config_from_args
                    .set_field_from_yaml(path.trim(), yaml.trim())
                    .map_err(Error::invalid_configuration)?;
            }
        }

        if let Some(data_dir) = args.data_dir {
            config_from_args.instance.data_dir = Some(data_dir);
        }

        if let Some(service_password_file) = args.service_password_file {
            config_from_args.instance.service_password_file = Some(service_password_file);
        }

        if let Some(cluster_id) = args.cluster_id {
            config_from_args.cluster.cluster_id = Some(cluster_id.clone());
            config_from_args.instance.cluster_id = Some(cluster_id);
        }

        if let Some(instance_id) = args.instance_id {
            config_from_args.instance.instance_id = Some(instance_id);
        }

        if let Some(replicaset_id) = args.replicaset_id {
            config_from_args.instance.replicaset_id = Some(replicaset_id);
        }

        if let Some(tier) = args.tier {
            config_from_args.instance.tier = Some(tier);
        }

        if let Some(init_replication_factor) = args.init_replication_factor {
            config_from_args.cluster.default_replication_factor = Some(init_replication_factor);
        }

        if !args.failure_domain.is_empty() {
            let map = args.failure_domain.into_iter();
            config_from_args.instance.failure_domain = Some(FailureDomain::from(map));
        }

        if let Some(address) = args.advertise_address {
            config_from_args.instance.advertise_address = Some(address);
        }

        if let Some(listen) = args.listen {
            config_from_args.instance.listen = Some(listen);
        }

        if let Some(http_listen) = args.http_listen {
            config_from_args.instance.http_listen = Some(http_listen);
        }

        if let Some(pg_listen) = args.pg_listen {
            config_from_args.instance.pg.listen = Some(pg_listen);
        }

        if !args.peers.is_empty() {
            config_from_args.instance.peer = Some(args.peers);
        }

        if let Some(log_level) = args.log_level {
            config_from_args.instance.log.level = Some(log_level);
        }

        if let Some(log_destination) = args.log {
            config_from_args.instance.log.destination = Some(log_destination);
        }

        if let Some(audit_destination) = args.audit {
            config_from_args.instance.audit = Some(audit_destination);
        }

        config_from_args.instance.plugin_dir = args.plugin_dir;

        if let Some(admin_socket) = args.admin_sock {
            config_from_args.instance.admin_socket = Some(admin_socket);
        }

        if let Some(script) = args.script {
            config_from_args.instance.deprecated_script = Some(script);
        }

        if args.shredding {
            config_from_args.instance.shredding = Some(true);
        }

        if let Some(memtx_memory) = args.memtx_memory {
            config_from_args.instance.memtx.memory = Some(memtx_memory);
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
            config_from_args
                .set_field_from_yaml(path.trim(), yaml.trim())
                .map_err(Error::invalid_configuration)?;
        }

        // All of the parameters set above came from CommandlineOrEnvironment
        #[rustfmt::skip]
        mark_non_none_field_sources(parameter_sources, &config_from_args, ParameterSource::CommandlineOrEnvironment);

        // Only now after sources are marked we can move the actual values into
        // our resulting config.
        self.move_non_none_fields_from(config_from_args);

        Ok(())
    }

    fn set_defaults_explicitly(&mut self, parameter_sources: &HashMap<String, ParameterSource>) {
        for path in &leaf_field_paths::<Self>() {
            if !matches!(
                parameter_sources.get(path),
                None | Some(ParameterSource::Default)
            ) {
                // Parameter value was already set from a config file, or arugments, etc.
                continue;
            }

            let Some(default) = self
                // This function will use the already specified parameters for
                // generating defaults for other dependent parameters. However
                // it doesn't do any sort of topological sorting, so it may
                // result in a panic if `#[introspection(config_default = ...)]`
                // expressions contain any unwraps. So we must sort the
                // parameters manually now
                .get_field_default_value_as_rmpv(path)
                .expect("paths are correct")
            else {
                continue;
            };

            self.set_field_from_rmpv(path, &default)
                .expect("same type, path is correct");
            // Could do this, but if parameter source is not set, it's implied to be ParameterSource::Default, so...
            // mark_non_none_field_sources(&mut parameter_sources, &config, ParameterSource::Default);
        }
    }

    /// Does checks which are applicable to configuration loaded from a file.
    fn validate_from_file(&self) -> Result<(), Error> {
        // XXX: This is kind of a hack. If a config file is provided we require
        // it to define the list of initial tiers. However if config file wasn't
        // specified, there's no way to define tiers, so we just ignore that
        // case and create a dummy "default" tier.
        if let Some(tiers) = &self.cluster.tier {
            if tiers.is_empty() {
                return Err(Error::invalid_configuration(
                    "empty `cluster.tier` section which is required to define the initial tiers",
                ));
            }
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
        let Some(tiers) = &self.cluster.tier else {
            return Ok(());
        };

        for (name, info) in tiers {
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

    // TODO: just derive a move_non_default_fields function,
    // it should be much simpler
    fn move_non_none_fields_from(&mut self, donor: Self) {
        for path in &leaf_field_paths::<PicodataConfig>() {
            let value = donor
                .get_field_as_rmpv(path)
                .expect("this should be tested thoroughly");

            if !value_is_specified(path, &value) {
                continue;
            }

            self.set_field_from_rmpv(path, &value)
                .expect("these fields have the same type")
        }
    }

    pub fn parameters_with_sources_as_rmpv(&self) -> rmpv::Value {
        return recursive_helper(self, "", Self::FIELD_INFOS);

        fn recursive_helper(
            config: &PicodataConfig,
            prefix: &str,
            nested_fields: &[FieldInfo],
        ) -> rmpv::Value {
            use rmpv::Value;
            assert!(!nested_fields.is_empty());

            let mut fields = Vec::with_capacity(nested_fields.len());
            for field in nested_fields {
                let name = field.name;

                let node;
                if !field.nested_fields.is_empty() {
                    let sub_section =
                        recursive_helper(config, &format!("{prefix}{name}."), field.nested_fields);
                    if matches!(&sub_section, Value::Map(m) if m.is_empty()) {
                        // Hide empty sub sections
                        continue;
                    }
                    node = sub_section;
                } else {
                    let path = format!("{prefix}{name}");

                    let value = config
                        .get_field_as_rmpv(&path)
                        .expect("nested fields should have valid field names");
                    if value.is_nil() {
                        // TODO: show default values
                        continue;
                    }

                    #[rustfmt::skip]
                    let source = config.parameter_sources.get(&path).copied().unwrap_or_default();

                    node = Value::Map(vec![
                        (Value::from("value"), value),
                        (Value::from("source"), Value::from(source.as_str())),
                    ]);
                };

                fields.push((Value::from(name), node));
            }

            Value::Map(fields)
        }
    }

    #[inline]
    pub fn max_tuple_size() -> usize {
        // This is value is not configurable at the moment, but this may change
        // in the future. At that point this function will probably also want to
        // accept a `&self` parameter, but for now it's not necessary.
        1048576
    }

    pub fn log_config_params(&self) {
        for path in &leaf_field_paths::<PicodataConfig>() {
            let value = self
                .get_field_as_rmpv(path)
                .expect("this should be tested thoroughly");

            if value_is_specified(path, &value) {
                tlog!(Info, "'{}': {}", path, value);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ParameterSource
////////////////////////////////////////////////////////////////////////////////

type ParameterSourcesMap = HashMap<String, ParameterSource>;

tarantool::define_str_enum! {
    #[derive(Default)]
    pub enum ParameterSource {
        #[default]
        Default = "default",
        ConfigFile = "config_file",
        // TODO: split into 2 sepparate variants
        CommandlineOrEnvironment = "commandline_or_environment",
    }
}

fn mark_non_none_field_sources(
    parameter_sources: &mut ParameterSourcesMap,
    config: &PicodataConfig,
    source: ParameterSource,
) {
    for path in leaf_field_paths::<PicodataConfig>() {
        let value = config
            .get_field_as_rmpv(&path)
            .expect("this should be tested thoroughly");

        if !value_is_specified(&path, &value) {
            continue;
        }

        parameter_sources.insert(path, source);
    }
}

#[inline]
fn value_is_specified(path: &str, value: &rmpv::Value) -> bool {
    if value.is_nil() {
        // Not specified
        return false;
    }

    if path == "cluster.tier" && matches!(&value, rmpv::Value::Map(m) if m.is_empty()) {
        // Special case: config.tier has type HashMap
        // and it must be set explicitly in the config file
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// UnknownFieldInfo
////////////////////////////////////////////////////////////////////////////////

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

#[derive(PartialEq, Default, Debug, Clone, serde::Deserialize, serde::Serialize, Introspection)]
pub struct ClusterConfig {
    pub cluster_id: Option<String>,

    // Option is needed to distinguish between the following cases:
    // when the config was specified and tier section was not
    // AND
    // when configuration file was not specified and tiers map is empty
    #[serde(
        deserialize_with = "deserialize_map_forbid_duplicate_keys",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub tier: Option<HashMap<String, TierConfig>>,

    /// Replication factor which is used for tiers which didn't specify one
    /// explicitly. For default value see [`Self::default_replication_factor()`].
    #[introspection(config_default = 1)]
    pub default_replication_factor: Option<u8>,

    #[serde(flatten)]
    #[introspection(ignore)]
    pub unknown_parameters: HashMap<String, YamlValue>,
}

impl ClusterConfig {
    // this method used only from bootstraping to persist tiers information
    pub fn tiers(&self) -> HashMap<String, Tier> {
        let Some(tiers) = &self.tier else {
            return HashMap::from([(
                DEFAULT_TIER.to_string(),
                Tier {
                    name: DEFAULT_TIER.into(),
                    replication_factor: self.default_replication_factor(),
                    can_vote: true,
                },
            )]);
        };

        let mut tier_defs = HashMap::with_capacity(tiers.len());
        for (name, info) in tiers {
            let replication_factor = info
                .replication_factor
                .unwrap_or_else(|| self.default_replication_factor());

            let tier_def = Tier {
                name: name.into(),
                replication_factor,
                can_vote: info.can_vote,
            };
            tier_defs.insert(name.into(), tier_def);
        }
        tier_defs
    }

    #[inline]
    pub fn default_replication_factor(&self) -> u8 {
        self.default_replication_factor
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }
}

////////////////////////////////////////////////////////////////////////////////
// InstanceConfig
////////////////////////////////////////////////////////////////////////////////

#[derive(PartialEq, Default, Debug, Clone, serde::Deserialize, serde::Serialize, Introspection)]
pub struct InstanceConfig {
    #[introspection(config_default = PathBuf::from("."))]
    pub data_dir: Option<PathBuf>,
    pub service_password_file: Option<PathBuf>,

    // Skip serializing, so that default config doesn't contain this option,
    // which isn't allowed to be set from file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_file: Option<PathBuf>,

    // Skip serializing, so that default config doesn't contain duplicate
    // cluster_id fields.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,

    pub instance_id: Option<String>,
    pub replicaset_id: Option<String>,

    #[introspection(config_default = "default")]
    pub tier: Option<String>,

    #[introspection(config_default = FailureDomain::default())]
    pub failure_domain: Option<FailureDomain>,

    #[introspection(
        config_default = vec![Address {
            user: None,
            host: DEFAULT_LISTEN_HOST.into(),
            port: DEFAULT_IPROTO_PORT.into(),
        }]
    )]
    pub peer: Option<Vec<Address>>,

    #[introspection(
        config_default = Address {
            user: None,
            host: DEFAULT_LISTEN_HOST.into(),
            port: DEFAULT_IPROTO_PORT.into(),
        }
    )]
    pub listen: Option<Address>,

    #[introspection(config_default = self.listen())]
    pub advertise_address: Option<Address>,

    pub http_listen: Option<Address>,

    #[introspection(config_default = self.data_dir.as_ref().map(|dir| dir.join("admin.sock")))]
    pub admin_socket: Option<PathBuf>,

    // TODO:
    // - sepparate config file for common parameters
    pub plugin_dir: Option<PathBuf>,

    // Skip serializing, so that default config doesn't contain this option,
    // because it's deprecated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deprecated_script: Option<PathBuf>,

    pub audit: Option<String>,

    // box.cfg.background currently doesn't work with our supervisor/child implementation
    // pub background: Option<bool>,

    //
    /// Determines wether the .xlog & .snap files should be shredded when
    /// deleting.
    #[introspection(config_default = false)]
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

    #[serde(default)]
    #[introspection(nested)]
    pub pg: pgproto::Config,

    /// Special catch-all field which will be filled by serde with all unknown
    /// fields from the yaml file.
    #[serde(flatten)]
    #[introspection(ignore)]
    pub unknown_parameters: HashMap<String, YamlValue>,
}

// TODO: remove all of the .clone() calls from these methods
impl InstanceConfig {
    #[inline]
    pub fn data_dir(&self) -> PathBuf {
        self.data_dir
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
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
        self.tier
            .as_deref()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    #[inline]
    pub fn failure_domain(&self) -> FailureDomain {
        self.failure_domain
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    #[inline]
    pub fn peers(&self) -> Vec<Address> {
        self.peer
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    #[inline]
    pub fn advertise_address(&self) -> Address {
        self.advertise_address
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    #[inline]
    pub fn listen(&self) -> Address {
        self.listen
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    #[inline]
    pub fn admin_socket(&self) -> PathBuf {
        self.admin_socket
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    #[inline]
    pub fn log_level(&self) -> SayLevel {
        self.log
            .level
            .expect("is set in PicodataConfig::set_defaults_explicitly")
            .into()
    }

    #[inline]
    pub fn shredding(&self) -> bool {
        self.shredding
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    #[inline]
    pub fn memtx_memory(&self) -> u64 {
        self.memtx
            .memory
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }
}

////////////////////////////////////////////////////////////////////////////////
// MemtxSection
////////////////////////////////////////////////////////////////////////////////

#[derive(PartialEq, Default, Debug, Clone, serde::Deserialize, serde::Serialize, Introspection)]
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
    #[introspection(config_default = 64 * 1024 * 1024)]
    pub memory: Option<u64>,

    /// The maximum number of snapshots that are stored in the memtx_dir
    /// directory. If the number of snapshots after creating a new one exceeds
    /// this value, the Tarantool garbage collector deletes old snapshots. If
    /// the option is set to zero, the garbage collector does not delete old
    /// snapshots.
    ///
    /// Corresponds to `box.cfg.checkpoint_count`.
    #[introspection(config_default = 2)]
    pub checkpoint_count: Option<u64>,

    /// The interval in seconds between actions by the checkpoint daemon. If the
    /// option is set to a value greater than zero, and there is activity that
    /// causes change to a database, then the checkpoint daemon calls
    /// box.snapshot() every checkpoint_interval seconds, creating a new
    /// snapshot file each time. If the option is set to zero, the checkpoint
    /// daemon is disabled.
    ///
    /// Corresponds to `box.cfg.checkpoint_interval`.
    #[introspection(config_default = 3600.0)]
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

#[derive(PartialEq, Default, Debug, Clone, serde::Deserialize, serde::Serialize, Introspection)]
#[serde(deny_unknown_fields)]
pub struct VinylSection {
    /// The maximum number of in-memory bytes that vinyl uses.
    ///
    /// Corresponds to `box.cfg.vinyl_memory`
    #[introspection(config_default = 128 * 1024 * 1024)]
    pub memory: Option<u64>,

    /// The cache size for the vinyl storage engine.
    ///
    /// Corresponds to `box.cfg.vinyl_cache`
    #[introspection(config_default = 128 * 1024 * 1024)]
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
    #[introspection(config_default = 0x300)]
    pub max_concurrent_messages: Option<u64>,
}

////////////////////////////////////////////////////////////////////////////////
// LogSection
////////////////////////////////////////////////////////////////////////////////

#[derive(PartialEq, Default, Debug, Clone, serde::Deserialize, serde::Serialize, Introspection)]
#[serde(deny_unknown_fields)]
pub struct LogSection {
    #[introspection(config_default = args::LogLevel::Info)]
    pub level: Option<args::LogLevel>,

    /// By default, Picodata sends the log to the standard error stream
    /// (stderr). If `log.destination` is specified, Picodata can send the log to a:
    /// - file
    /// - pipe
    /// - system logger
    pub destination: Option<String>,

    #[introspection(config_default = LogFormat::Plain)]
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
) -> Result<Option<HashMap<K, V>>, D::Error>
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
        type Value = Option<HashMap<K, V>>;

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

            Ok(Some(res))
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
    use crate::util::{on_scope_exit, ScopeGuard};
    use clap::Parser as _;
    use pretty_assertions::assert_eq;
    use std::{str::FromStr, sync::Mutex};

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
        assert_eq!(config, Default::default());

        let yaml = r###"
cluster:
    cluster_id: foobar

    tier:
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
    tier:
        voter:
            can_vote: true

        voter:
            can_vote: false

"###;

        let err = PicodataConfig::read_yaml_contents(&yaml.trim_start()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid configuration: cluster.tier: duplicate key `voter` found at line 3 column 9"
        );
    }

    #[test]
    fn cluster_id_is_required() {
        let yaml = r###"
cluster:
    tier:
        default:
instance:
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap();
        let err = config.validate_from_file().unwrap_err();
        assert_eq!(err.to_string(), "invalid configuration: either `cluster.cluster_id` or `instance.cluster_id` must be specified");

        let yaml = r###"
cluster:
    cluster_id: foo
    tier:
        default:
instance:
    cluster_id: bar
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap();
        let err = config.validate_from_file().unwrap_err();
        assert_eq!(err.to_string(), "invalid configuration: `cluster.cluster_id` (foo) conflicts with `instance.cluster_id` (bar)");
    }

    #[test]
    fn missing_tiers_is_not_error() {
        let yaml = r###"
cluster:
    cluster_id: test
"###;
        let cfg = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap();

        cfg.validate_from_file()
            .expect("absence `tier` section is ok");

        let yaml = r###"
cluster:
    cluster_id: test
    tier:
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap();
        let err = config.validate_from_file().unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid configuration: empty `cluster.tier` section which is required to define the initial tiers"
        );

        let yaml = r###"
cluster:
    cluster_id: test
    tier:
        default:
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap();
        config.validate_from_file().unwrap();

        let yaml = r###"
cluster:
    default_replication_factor: 3
    cluster_id: test
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap();
        config.validate_from_file().expect("");

        let tiers = config.cluster.tiers();
        let default_tier = tiers
            .get("default")
            .expect("default replication factor should applied to default tier configuration");
        assert_eq!(default_tier.replication_factor, 3);
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

    #[track_caller]
    fn setup_for_tests(yaml: Option<&str>, args: &[&str]) -> Result<Box<PicodataConfig>, Error> {
        let mut config = if let Some(yaml) = yaml {
            PicodataConfig::read_yaml_contents(yaml).unwrap()
        } else {
            Default::default()
        };
        let args = args::Run::try_parse_from(args).unwrap();
        let mut parameter_sources = Default::default();
        mark_non_none_field_sources(&mut parameter_sources, &config, ParameterSource::ConfigFile);
        config.set_from_args_and_env(args, &mut parameter_sources)?;
        config.set_defaults_explicitly(&parameter_sources);
        config.parameter_sources = parameter_sources;
        Ok(config)
    }

    // This is a helper for tests that modify environment variables to test how
    // we're parsing them. But rust unit-tests are ran in parallel, so we
    // must be careful: we can't allow other test which also read env to
    // run concurrently hence the lock.
    // Note: make sure to not assign the guard to `_` variable to avoid
    // premature lock release
    #[must_use]
    fn protect_env() -> ScopeGuard<impl FnOnce() -> ()> {
        static ENV_LOCK: Mutex<()> = Mutex::new(());
        let locked = ENV_LOCK.lock().unwrap();

        // Save environment before test, to avoid breaking something unrelated
        let env_before: Vec<_> = std::env::vars()
            .filter(|(k, _)| k.starts_with("PICODATA_"))
            .collect();
        for (k, _) in &env_before {
            std::env::remove_var(k);
        }
        on_scope_exit(|| {
            for (k, v) in env_before {
                std::env::set_var(k, v);
            }
            std::env::remove_var("PICODATA_CONFIG_PARAMETERS");

            drop(locked)
        })
    }

    #[rustfmt::skip]
    #[test]
    fn parameter_source_precedence() {
        let _guard = protect_env();

        //
        // Defaults
        //
        {
            let config = setup_for_tests(None, &["run"]).unwrap();

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
            let config = setup_for_tests(Some(yaml), &["run"]).unwrap();

            assert_eq!(config.instance.instance_id().unwrap(), "I-CONFIG");

            // PICODATA_CONFIG_PARAMETERS > config
            std::env::set_var("PICODATA_CONFIG_PARAMETERS", "instance.instance_id=I-ENV-CONF-PARAM");
            let config = setup_for_tests(Some(yaml), &["run"]).unwrap();

            assert_eq!(config.instance.instance_id().unwrap(), "I-ENV-CONF-PARAM");

            // other env > PICODATA_CONFIG_PARAMETERS
            std::env::set_var("PICODATA_INSTANCE_ID", "I-ENVIRON");
            let config = setup_for_tests(Some(yaml), &["run"]).unwrap();

            assert_eq!(config.instance.instance_id().unwrap(), "I-ENVIRON");

            // command line > env
            let config = setup_for_tests(Some(yaml), &["run", "--instance-id=I-COMMANDLINE"]).unwrap();

            assert_eq!(config.instance.instance_id().unwrap(), "I-COMMANDLINE");

            // -c PARAMETER=VALUE > other command line
            let config = setup_for_tests(Some(yaml), &["run", "-c", "instance.instance_id=I-CLI-CONF-PARAM", "--instance-id=I-COMMANDLINE"]).unwrap();

            assert_eq!(config.instance.instance_id().unwrap(), "I-CLI-CONF-PARAM");
        }

        //
        // peer parsing
        //
        {
            // empty config means default
            let yaml = r###"
instance:
    peer:
"###;
            let config = setup_for_tests(Some(yaml), &["run"]).unwrap();

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
    peer:
        - bobbert:420
        - tomathan:69
"###;
            let config = setup_for_tests(Some(yaml), &["run"]).unwrap();

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
            let config = setup_for_tests(Some(yaml), &["run"]).unwrap();

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
            let config = setup_for_tests(Some(yaml), &["run",
                "--peer", "one:1",
                "--peer", "two:2,    <- same problem here,:3,4"
            ]).unwrap();

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
            let config = setup_for_tests(Some(yaml), &["run",
                "-c", "instance.peer=[  host:123  , ghost :321, гост:666]",
            ]).unwrap();

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
            let config = setup_for_tests(Some(""), &["run"]).unwrap();

            assert_eq!(config.instance.listen().to_host_port(), "L-ENVIRON:3301");
            assert_eq!(config.instance.advertise_address().to_host_port(), "L-ENVIRON:3301");

            let yaml = r###"
instance:
    advertise_address: A-CONFIG
"###;
            let config = setup_for_tests(Some(yaml), &["run"]).unwrap();

            assert_eq!(config.instance.listen().to_host_port(), "L-ENVIRON:3301");
            assert_eq!(config.instance.advertise_address().to_host_port(), "A-CONFIG:3301");

            let config = setup_for_tests(Some(yaml), &["run", "-l", "L-COMMANDLINE"]).unwrap();

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
            let config = setup_for_tests(Some(yaml), &["run"]).unwrap();
            assert_eq!(
                config.instance.failure_domain(),
                FailureDomain::from([("KCONF1", "VCONF1"), ("KCONF2", "VCONF2-REPLACED")])
            );

            // environment
            std::env::set_var("PICODATA_FAILURE_DOMAIN", "kenv1=venv1,kenv2=venv2,kenv2=venv2-replaced");
            let config = setup_for_tests(Some(yaml), &["run"]).unwrap();
            assert_eq!(
                config.instance.failure_domain(),
                FailureDomain::from([("KENV1", "VENV1"), ("KENV2", "VENV2-REPLACED")])
            );

            // command line
            let config = setup_for_tests(Some(yaml), &["run", "--failure-domain", "karg1=varg1,karg1=varg1-replaced"]).unwrap();
            assert_eq!(
                config.instance.failure_domain(),
                FailureDomain::from([("KARG1", "VARG1-REPLACED")])
            );

            // command line more complex
            let config = setup_for_tests(Some(yaml), &["run",
                "--failure-domain", "foo=1",
                "--failure-domain", "bar=2,baz=3"
            ]).unwrap();
            assert_eq!(
                config.instance.failure_domain(),
                FailureDomain::from([
                    ("FOO", "1"),
                    ("BAR", "2"),
                    ("BAZ", "3"),
                ])
            );

            // --config-parameter
            let config = setup_for_tests(Some(yaml), &["run",
                "-c", "instance.failure_domain={foo: '11', bar: '22'}"
            ]).unwrap();
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
            let config = setup_for_tests(Some(yaml), &["run",
                "-c", "  instance.log .level =debug  ",
                "--config-parameter", "instance. memtx . memory=  0xdeadbeef",
            ]).unwrap();
            assert_eq!(config.instance.tier.unwrap(), "ABC");
            assert_eq!(config.cluster.cluster_id.unwrap(), "DEF");
            assert_eq!(config.instance.log.level.unwrap(), args::LogLevel::Debug);
            assert_eq!(config.instance.memtx.memory.unwrap(), 0xdead_beef);
            assert_eq!(config.instance.audit.unwrap(), "audit.txt");
            assert_eq!(config.instance.data_dir.unwrap(), PathBuf::from("."));

            //
            // Errors
            //
            let yaml = r###"
"###;
            let e = setup_for_tests(Some(yaml), &["run",
                "-c", "  instance.hoobabooba = malabar  ",
            ]).unwrap_err();
            assert!(dbg!(e.to_string()).starts_with("invalid configuration: instance: unknown field `hoobabooba`, expected one of"));

            let yaml = r###"
"###;
            std::env::set_var(
                "PICODATA_CONFIG_PARAMETERS",
                "  cluster.cluster_id=DEF  ;
                  cluster.asdfasdfbasdfbasd = "
            );
            let e = setup_for_tests(Some(yaml), &["run"]).unwrap_err();
            assert!(dbg!(e.to_string()).starts_with("invalid configuration: cluster: unknown field `asdfasdfbasdfbasd`, expected one of"));
        }
    }

    #[test]
    fn pg_config() {
        let _guard = protect_env();

        let yaml = r###"
instance:
"###;
        let config = setup_for_tests(Some(yaml), &["run"]).unwrap();
        // pg section wasn't specified so it shouldn't be enabled
        assert!(!config.instance.pg.enabled());

        let yaml = r###"
instance:
    pg:
        listen: "localhost:5432"
        ssl: true
"###;
        let config = setup_for_tests(Some(yaml), &["run"]).unwrap();
        let pg = config.instance.pg;
        assert!(pg.enabled());
        assert_eq!(&pg.listen().to_host_port(), "localhost:5432");
        assert!(pg.ssl());

        // test defaults
        let yaml = r###"
instance:
    pg:
        listen: "localhost:5432"
"###;
        let config = setup_for_tests(Some(yaml), &["run"]).unwrap();
        let pg = config.instance.pg;
        assert!(pg.enabled());
        assert_eq!(pg.listen(), Address::from_str("localhost:5432").unwrap());
        assert!(!pg.ssl());

        // test config with -c option
        let config =
            setup_for_tests(None, &["run", "-c", "instance.pg.listen=localhost:5432"]).unwrap();
        let pg = config.instance.pg;
        assert!(pg.enabled());
        assert_eq!(pg.listen(), Address::from_str("localhost:5432").unwrap());
        assert!(!pg.ssl());

        // test config from run args
        let config = setup_for_tests(None, &["run", "--pg-listen", "localhost:5432"]).unwrap();
        let pg = config.instance.pg;
        assert!(pg.enabled());
        assert_eq!(pg.listen(), Address::from_str("localhost:5432").unwrap());
        assert!(!pg.ssl());

        // test config from env
        std::env::set_var("PICODATA_PG_LISTEN", "localhost:1234");
        let config = setup_for_tests(None, &["run"]).unwrap();
        let pg = config.instance.pg;
        assert!(pg.enabled());
        assert_eq!(pg.listen(), Address::from_str("localhost:1234").unwrap());
        assert!(!pg.ssl());
    }
}
