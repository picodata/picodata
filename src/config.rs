use crate::address::{HttpAddress, IprotoAddress};
use crate::cli::args;
use crate::cli::args::CONFIG_PARAMETERS_ENV;
use crate::config_parameter_path;
use crate::failure_domain::FailureDomain;
use crate::instance::InstanceName;
use crate::introspection::leaf_field_paths;
use crate::introspection::FieldInfo;
use crate::introspection::Introspection;
use crate::pgproto;
use crate::replicaset::ReplicasetName;
use crate::sql::value_type_str;
use crate::storage;
use crate::system_parameter_name;
use crate::tarantool::set_cfg_field;
use crate::tier::Tier;
use crate::tier::TierConfig;
use crate::tier::DEFAULT_TIER;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::RaftSpaceAccess;
use crate::util::edit_distance;
use crate::util::file_exists;
use sbroad::ir::value::{EncodedValue, Value};
use serde_yaml::Value as YamlValue;
use std::collections::HashMap;
use std::convert::{From, Into};
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use tarantool::log::SayLevel;
use tarantool::tuple::Tuple;

/// This reexport is used in the derive macro for Introspection.
pub use sbroad::ir::relation::Type as SbroadType;

pub use crate::address::{
    DEFAULT_IPROTO_PORT, DEFAULT_LISTEN_HOST, DEFAULT_PGPROTO_PORT, DEFAULT_USERNAME,
};
pub const DEFAULT_CONFIG_FILE_NAME: &str = "picodata.yaml";

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

        let mut config_path = None;
        let mut config_from_file = None;
        if let Some(args_path) = &args_path {
            config_path = Some(args_path);
            config_from_file = Some(Self::read_yaml_file(args_path)?);
        } else if file_exists(&default_path) {
            #[rustfmt::skip]
            tlog!(Info, "Reading configuration file '{DEFAULT_CONFIG_FILE_NAME}' in the current working directory '{}'.", cwd.display());
            config_path = Some(&default_path);
            config_from_file = Some(Self::read_yaml_file(&default_path)?);
        }

        let mut parameter_sources = ParameterSourcesMap::default();
        // Initialize the logger as soon as possible so that as many messages as possible
        // go into the destination expected by user.
        init_core_logger_from_args_env_or_config(
            &args,
            config_from_file.as_deref(),
            &mut parameter_sources,
        )?;

        if let Some(args_path) = &args_path {
            if args_path != &default_path && file_exists(&default_path) {
                let cwd = cwd.display();
                let args_path = args_path.display();
                #[rustfmt::skip]
                tlog!(Warning, "A path to configuration file '{args_path}' was provided explicitly,
but a '{DEFAULT_CONFIG_FILE_NAME}' file in the current working directory '{cwd}' also exists.
Using configuration file '{args_path}'.");
            }
        }

        let mut config = if let Some(mut config) = config_from_file {
            config.validate_from_file()?;
            #[rustfmt::skip]
            mark_non_none_field_sources(&mut parameter_sources, &config, ParameterSource::ConfigFile);

            if args.config.is_some() {
                #[rustfmt::skip]
                parameter_sources.insert("instance.config_file".into(), ParameterSource::CommandlineOrEnvironment);
            }

            assert!(config_path.is_some());
            config.instance.config_file = config_path.cloned();
            config
        } else {
            Default::default()
        };

        config.set_from_args_and_env(args, &mut parameter_sources)?;

        config.handle_deprecated_parameters(&mut parameter_sources)?;

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
    /// parameters being unspecified and parameters being set to the default
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

        // Same story as with `cluster.tier`, the cluster name must be provided
        // in the config file.
        config.cluster.name = Some("demo".into());

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
            for (path, yaml) in parse_picodata_config_parameters_env(&env)? {
                config_from_args
                    .set_field_from_yaml(path, yaml)
                    .map_err(Error::invalid_configuration)?;
            }
        }

        if let Some(instance_dir) = args.instance_dir {
            config_from_args.instance.instance_dir = Some(instance_dir);
        }

        if let Some(service_password_file) = args.service_password_file {
            config_from_args.instance.service_password_file = Some(service_password_file);
        }

        if let Some(cluster_name) = args.cluster_name {
            config_from_args.cluster.name = Some(cluster_name.clone());
            config_from_args.instance.cluster_name = Some(cluster_name);
        }

        if let Some(instance_name) = args.instance_name {
            config_from_args.instance.name = Some(instance_name);
        }

        if let Some(replicaset_name) = args.replicaset_name {
            config_from_args.instance.replicaset_name = Some(replicaset_name);
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

        if let Some(address) = args.iproto_advertise {
            config_from_args.instance.iproto_advertise = Some(address);
        }

        #[allow(deprecated)]
        if let Some(address) = args.advertise_address {
            config_from_args.instance.advertise_address = Some(address);
        }

        if let Some(iproto_listen) = args.iproto_listen {
            config_from_args.instance.iproto_listen = Some(iproto_listen);
        }

        #[allow(deprecated)]
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

        config_from_args.instance.share_dir = args.share_dir;

        #[allow(deprecated)]
        {
            config_from_args.instance.plugin_dir = args.plugin_dir;
        }

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

    fn set_defaults_explicitly(&mut self, parameter_sources: &ParameterSourcesMap) {
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

        match (&self.cluster.name, &self.instance.cluster_name) {
            (Some(from_cluster), Some(from_instance)) if from_cluster != from_instance => {
                return Err(Error::InvalidConfiguration(format!(
                    "`cluster.name` ({from_cluster}) conflicts with `instance.cluster_name` ({from_instance})",
                )));
            }
            (None, None) => {
                return Err(Error::invalid_configuration(
                    "either `cluster.name` or `instance.cluster_name` must be specified",
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
        // But we have a picodata.yaml file for picodata 42.8 which is already deployed,
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

                // TODO: this is a temporary help message implied only for 24.6 version
                if param.name == "instance_id"
                    || param.name == "cluster_id"
                    || param.name == "replicased_id"
                {
                    _ = write!(&mut buffer, " (did you mean `name`?)");
                    continue;
                } else if let Some(best_match) = param.best_match {
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

    /// This function handles the usage of deprecated parameters:
    ///
    /// - Logs a warning message if deprecated parameter is used instead of replacement
    ///
    /// - Returns error in case deprecated parameter and it's replacement are
    ///   specified at the same time
    ///
    /// - Moves the value of the deprecated parameter to it's replacement field
    ///
    /// Only stuff related to deprecated parameters goes here.
    ///
    /// Must be called before [`Self::set_defaults_explicitly`]!.
    #[allow(deprecated)]
    fn handle_deprecated_parameters(
        &mut self,
        parameter_sources: &mut ParameterSourcesMap,
    ) -> Result<(), Error> {
        // In the future when renaming configuration file parameters just
        // add a pair of names into this array.
        let renamed_parameters = &[
            // (deprecated, use_instead)
            (
                config_parameter_path!(instance.plugin_dir),
                config_parameter_path!(instance.share_dir),
            ),
            (
                config_parameter_path!(instance.listen),
                config_parameter_path!(instance.iproto_listen),
            ),
            (
                config_parameter_path!(instance.advertise_address),
                config_parameter_path!(instance.iproto_advertise),
            ),
        ];

        // Handle renamed parameters
        for &(deprecated, use_instead) in renamed_parameters {
            let value = self
                .get_field_as_rmpv(deprecated)
                .expect("this should be tested thoroughly");

            if !value_is_specified(deprecated, &value) {
                continue;
            }

            let deprecated_source = parameter_sources.get(deprecated);
            let deprecated_source =
                *deprecated_source.expect("the parameter was specified, so source must be set");

            let conflicting_value = self
                .get_field_as_rmpv(use_instead)
                .expect("this should be tested thoroughly");

            if value_is_specified(use_instead, &conflicting_value) {
                let conflicting_source = parameter_sources.get(use_instead);
                let conflicting_source = *conflicting_source
                    .expect("the parameter was specified, so source must be set");
                #[rustfmt::skip]
                tlog!(Info, "{deprecated} is set to {value} via {deprecated_source}");
                #[rustfmt::skip]
                tlog!(Info, "{use_instead} is set to {conflicting_value} via {conflicting_source}");

                #[rustfmt::skip]
                return Err(Error::invalid_configuration(format!("{deprecated} is deprecated, use {use_instead} instead (cannot use both at the same time)")));
            }

            #[rustfmt::skip]
            tlog!(Warning, "{deprecated} is deprecated, use {use_instead} instead");

            // Copy the value from the deprecated parameter to the one that should be used instead
            self.set_field_from_rmpv(use_instead, &value)
                .expect("these fields have the same type");

            // Copy the parameter source info as well, so that other parts of the system work correclty.
            // For example set_defaults_explicitly relies on this.
            parameter_sources.insert(use_instead.into(), deprecated_source);
        }

        // TODO: we will likely have more complicated cases of deprecation, not
        // just renamings of parameters. For example some parameter may be
        // deprecated because the feature is replaced with a different feature,
        // for example we don't want users to specify the instance_name directly
        // or something like this. In this case we would just need to log a
        // warning if the parameter is specified. Or even more complex cases can
        // happen like one parameter is replaced with 2 or even more. These
        // kinds of cases are not covered by the above code unfortunately.

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
        // Cluster name
        match (raft_storage.cluster_name()?, self.cluster_name()) {
            (from_storage, from_config) if from_storage != from_config => {
                return Err(Error::InvalidConfiguration(format!(
                    "instance restarted with a different `cluster_name`, which is not allowed, was: '{from_storage}' became: '{from_config}'"
                )));
            }
            _ => {}
        }

        // Instance id
        let mut instance_name = None;
        match (raft_storage.instance_name()?, &self.instance.name) {
            (Some(from_storage), Some(from_config)) if from_storage != from_config => {
                return Err(Error::InvalidConfiguration(format!(
                    "instance restarted with a different `instance_name`, which is not allowed, was: '{from_storage}' became: '{from_config}'"
                )));
            }
            (Some(from_storage), _) => {
                instance_name = Some(from_storage);
            }
            _ => {}
        }

        // Replicaset id
        if let Some(instance_name) = &instance_name {
            if let Ok(instance_info) = storage.instances.get(instance_name) {
                match (
                    &instance_info.replicaset_name,
                    &self.instance.replicaset_name,
                ) {
                    (from_storage, Some(from_config)) if from_storage != from_config => {
                        return Err(Error::InvalidConfiguration(format!(
                            "instance restarted with a different `replicaset_name`, which is not allowed, was: '{from_storage}' became: '{from_config}'"
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
                &self.instance.iproto_advertise,
            ) {
                (Some(from_storage), Some(from_config))
                    if from_storage != from_config.to_host_port() =>
                {
                    return Err(Error::InvalidConfiguration(format!(
                        "instance restarted with a different `iproto_advertise`, which is not allowed, was: '{from_storage}' became: '{from_config}'"
                    )));
                }
                _ => {}
            }
        }

        Ok(())
    }

    #[inline]
    pub fn cluster_name(&self) -> &str {
        match (&self.instance.cluster_name, &self.cluster.name) {
            (Some(instance_cluster_name), Some(cluster_name)) => {
                assert_eq!(instance_cluster_name, cluster_name);
                instance_cluster_name
            }
            (Some(instance_cluster_name), None) => instance_cluster_name,
            (None, Some(cluster_name)) => cluster_name,
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

    #[inline]
    pub fn total_bucket_count() -> u64 {
        // This is value is not configurable at the moment, but this may change
        // in the future. At that point this function will probably also want to
        // accept a `&self` parameter, but for now it's not necessary.
        3000
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

    // For the tests
    pub(crate) fn init_for_tests() {
        let config = Box::new(Self::with_defaults());
        // Safe, because we only initialize config once in a single thread.
        unsafe {
            assert!(GLOBAL_CONFIG.is_none());
            let _ = GLOBAL_CONFIG.insert(config);
        };
    }
}

/// Gets the logging configuration from all possible sources and initializes the core logger.
///
/// Note that this function is an exception to the rule of how we do parameter
/// handling. This is needed because we the logger must be set up as soon as
/// possible so that logging messages during handling of other parameters go
/// into the destination expected by user. All other parameter handling is done
/// elsewhere in [`PicodataConfig::set_from_args_and_env`].
fn init_core_logger_from_args_env_or_config(
    args: &args::Run,
    config: Option<&PicodataConfig>,
    sources: &mut ParameterSourcesMap,
) -> Result<(), Error> {
    let mut destination = None;
    let mut level = DEFAULT_LOG_LEVEL;
    let mut format = DEFAULT_LOG_FORMAT;

    let mut destination_source = ParameterSource::Default;
    let mut level_source = ParameterSource::Default;
    let mut format_source = ParameterSource::Default;

    // Config has the lowest priority
    if let Some(config) = config {
        if let Some(config_destination) = &config.instance.log.destination {
            destination_source = ParameterSource::ConfigFile;
            destination = Some(&**config_destination);
        }

        if let Some(config_level) = config.instance.log.level {
            level_source = ParameterSource::ConfigFile;
            level = config_level;
        }

        if let Some(config_format) = config.instance.log.format {
            format_source = ParameterSource::ConfigFile;
            format = config_format;
        }
    }

    // Env is middle priority
    let env = std::env::var(CONFIG_PARAMETERS_ENV);
    if let Ok(env) = &env {
        for (path, value) in parse_picodata_config_parameters_env(env)? {
            if path == config_parameter_path!(instance.log.destination) {
                destination_source = ParameterSource::CommandlineOrEnvironment;
                destination =
                    Some(serde_yaml::from_str(value).map_err(Error::invalid_configuration)?)
            } else if path == config_parameter_path!(instance.log.level) {
                level_source = ParameterSource::CommandlineOrEnvironment;
                level = serde_yaml::from_str(value).map_err(Error::invalid_configuration)?
            } else if path == config_parameter_path!(instance.log.format) {
                format_source = ParameterSource::CommandlineOrEnvironment;
                format = serde_yaml::from_str(value).map_err(Error::invalid_configuration)?
            } else {
                // Skip for now, it will be checked in `set_from_args_and_env`
                // when everything else other than logger configuration is processed.
            }
        }
    }

    // Arguments have higher priority than env.
    if let Some(args_destination) = &args.log {
        destination_source = ParameterSource::CommandlineOrEnvironment;
        destination = Some(args_destination);
    }

    if let Some(args_level) = args.log_level {
        level_source = ParameterSource::CommandlineOrEnvironment;
        level = args_level;
    }

    // Format is not configurable from commandline options

    tlog::init_core_logger(destination, level.into(), format);

    let path = config_parameter_path!(instance.log.destination);
    sources.insert(path.into(), destination_source);

    let path = config_parameter_path!(instance.log.level);
    sources.insert(path.into(), level_source);

    let path = config_parameter_path!(instance.log.format);
    sources.insert(path.into(), format_source);

    Ok(())
}

fn parse_picodata_config_parameters_env(value: &str) -> Result<Vec<(&str, &str)>, Error> {
    let mut result = Vec::new();
    for item in value.split(';') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let Some((path, yaml)) = item.split_once('=') else {
            return Err(Error::InvalidConfiguration(format!(
                "error when parsing {CONFIG_PARAMETERS_ENV} environment variable: expected '=' after '{item}'",
            )));
        };
        result.push((path.trim(), yaml.trim()));
    }
    Ok(result)
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
    pub name: Option<String>,

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
                    ..Default::default()
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
                ..Default::default()
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
    pub instance_dir: Option<PathBuf>,
    pub service_password_file: Option<PathBuf>,

    // Skip serializing, so that default config doesn't contain this option,
    // which isn't allowed to be set from file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_file: Option<PathBuf>,

    // Skip serializing, so that default config doesn't contain duplicate
    // cluster_name fields.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_name: Option<String>,

    pub name: Option<String>,
    pub replicaset_name: Option<String>,

    #[introspection(config_default = "default")]
    pub tier: Option<String>,

    #[introspection(config_default = FailureDomain::default())]
    pub failure_domain: Option<FailureDomain>,

    #[deprecated = "use iproto_listen instead"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen: Option<IprotoAddress>,

    #[introspection(config_default = IprotoAddress::default())]
    pub iproto_listen: Option<IprotoAddress>,

    #[deprecated = "use iproto_advertise instead"]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub advertise_address: Option<IprotoAddress>,

    #[introspection(config_default = self.iproto_listen())]
    pub iproto_advertise: Option<IprotoAddress>,

    #[introspection(config_default = vec![self.iproto_advertise()])]
    pub peer: Option<Vec<IprotoAddress>>,

    pub http_listen: Option<HttpAddress>,

    #[introspection(config_default = self.instance_dir.as_ref().map(|dir| dir.join("admin.sock")))]
    pub admin_socket: Option<PathBuf>,

    #[deprecated = "use share_dir instead"]
    pub plugin_dir: Option<PathBuf>,

    #[introspection(config_default = "/usr/share/picodata/")]
    pub share_dir: Option<PathBuf>,

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
    pub fn instance_dir(&self) -> PathBuf {
        self.instance_dir
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    #[inline]
    pub fn name(&self) -> Option<InstanceName> {
        self.name.as_deref().map(InstanceName::from)
    }

    #[inline]
    pub fn replicaset_name(&self) -> Option<ReplicasetName> {
        self.replicaset_name.as_deref().map(ReplicasetName::from)
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
    pub fn peers(&self) -> Vec<IprotoAddress> {
        self.peer
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    #[inline]
    pub fn iproto_advertise(&self) -> IprotoAddress {
        self.iproto_advertise
            .clone()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }

    #[inline]
    pub fn iproto_listen(&self) -> IprotoAddress {
        self.iproto_listen
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
    pub fn share_dir(&self) -> &Path {
        self.share_dir
            .as_deref()
            .expect("is set in PicodataConfig::set_defaults_explicitly")
    }
}

////////////////////////////////////////////////////////////////////////////////
// ByteSize
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize, Introspection)]
#[serde(try_from = "String", into = "String")]
pub struct ByteSize {
    amount: u64,
    scale: Scale,
}

impl ByteSize {
    fn as_u64(&self) -> Option<u64> {
        const UNIT: u64 = 1024;
        let multiplier = match self.scale {
            Scale::Bytes => 1,
            Scale::Kilobytes => UNIT,
            Scale::Megabytes => UNIT.pow(2),
            Scale::Gigabytes => UNIT.pow(3),
            Scale::Terabytes => UNIT.pow(4),
        };

        self.amount.checked_mul(multiplier)
    }
}

tarantool::define_str_enum! {
    #[derive(Default)]
   pub enum Scale {
        #[default]
        Bytes = "B",
        Kilobytes = "K",
        Megabytes = "M",
        Gigabytes = "G",
        Terabytes = "T",
   }
}

impl Scale {
    fn from_char(value: char) -> Option<Self> {
        value.to_string().parse().ok()
    }
}

impl TryFrom<String> for ByteSize {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl FromStr for ByteSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (s, scale) = if let Some(scale) = s.chars().last().and_then(Scale::from_char) {
            (&s[..s.len() - 1], scale)
        } else {
            (s, Scale::Bytes)
        };

        let size = ByteSize {
            amount: s
                .parse::<u64>()
                .map_err(|err: std::num::ParseIntError| err.to_string())?,
            scale,
        };

        if size.as_u64().is_none() {
            return Err("Value is too large".to_string());
        }

        Ok(size)
    }
}

impl From<&ByteSize> for u64 {
    fn from(memory: &ByteSize) -> Self {
        memory.as_u64().expect("should be valid ByteSize")
    }
}

impl From<ByteSize> for String {
    fn from(memory: ByteSize) -> Self {
        memory.to_string()
    }
}

impl Display for ByteSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.amount, self.scale)
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
    #[introspection(config_default = "64M")]
    pub memory: Option<ByteSize>,
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
    #[introspection(config_default = "128M")]
    pub memory: Option<ByteSize>,

    /// The cache size for the vinyl storage engine.
    ///
    /// Corresponds to `box.cfg.vinyl_cache`
    #[introspection(config_default = "128M")]
    pub cache: Option<ByteSize>,

    /// Bloom filter false positive rate.
    ///
    /// Corresponds to `box.cfg.vinyl_bloom_fpr`
    #[introspection(config_default = 0.05)]
    pub bloom_fpr: Option<f32>,

    /// Size of the largest allocation unit.
    ///
    /// Corresponds to `box.cfg.vinyl_max_tuple_size`
    #[introspection(config_default = "1M")]
    pub max_tuple_size: Option<ByteSize>,

    /// Read and write unit size for disk operations.
    ///
    /// Corresponds to `box.cfg.vinyl_page_size`
    #[introspection(config_default = "8K")]
    pub page_size: Option<ByteSize>,

    /// The default maximum range size for an index.
    ///
    /// Corresponds to `box.cfg.vinyl_range_size`
    #[introspection(config_default = "1G")]
    pub range_size: Option<ByteSize>,

    /// The maximal number of runs per level in LSM tree.
    ///
    /// Corresponds to `box.cfg.vinyl_run_count_per_level`
    #[introspection(config_default = 2)]
    pub run_count_per_level: Option<i32>,

    /// Ratio between the sizes of different levels in the LSM tree.
    ///
    /// Corresponds to `box.cfg.vinyl_run_size_ratio`
    #[introspection(config_default = 3.5)]
    pub run_size_ratio: Option<f32>,

    /// The maximum number of read threads can be used for concurrent operations.
    ///
    /// Corresponds to `box.cfg.vinyl_read_threads`
    #[introspection(config_default = 1)]
    pub read_threads: Option<i32>,

    /// The maximum number of write threads can be used for concurrent operations.
    ///
    /// Corresponds to `box.cfg.vinyl_write_threads`
    #[introspection(config_default = 4)]
    pub write_threads: Option<i32>,

    /// Timeout for queries in the compression scheduler when available memory is low.
    ///
    /// Corresponds to `box.cfg.vinyl_timeout`
    #[introspection(config_default = 60.)]
    pub timeout: Option<f32>,
}

////////////////////////////////////////////////////////////////////////////////
// LogSection
////////////////////////////////////////////////////////////////////////////////

const DEFAULT_LOG_LEVEL: args::LogLevel = args::LogLevel::Info;
const DEFAULT_LOG_FORMAT: LogFormat = LogFormat::Plain;

#[derive(PartialEq, Default, Debug, Clone, serde::Deserialize, serde::Serialize, Introspection)]
#[serde(deny_unknown_fields)]
pub struct LogSection {
    #[introspection(config_default = DEFAULT_LOG_LEVEL)]
    pub level: Option<args::LogLevel>,

    /// By default, Picodata sends the log to the standard error stream
    /// (stderr). If `log.destination` is specified, Picodata can send the log to a:
    /// - file
    /// - pipe
    /// - system logger
    pub destination: Option<String>,

    #[introspection(config_default = DEFAULT_LOG_FORMAT)]
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
// AlterSystemParameters
////////////////////////////////////////////////////////////////////////////////

/// A struct which represents an enumeration of system parameters which can be
/// configured via ALTER SYSTEM.
///
/// This struct uses the `Introspection` trait to associate with each paramater
/// a `SbroadType` and a default value.
///
/// At the moment this struct is never actually used as a struct, but only as
/// a container for the metadata described above.
#[derive(PartialEq, Default, Debug, Clone, serde::Deserialize, serde::Serialize, Introspection)]
#[serde(deny_unknown_fields)]
pub struct AlterSystemParameters {
    /// Password should contain at least this many characters
    #[introspection(sbroad_type = SbroadType::Unsigned)]
    #[introspection(config_default = 8)]
    pub auth_password_length_min: u64,

    /// Password should contain at least one uppercase letter
    #[introspection(sbroad_type = SbroadType::Boolean)]
    #[introspection(config_default = true)]
    pub auth_password_enforce_uppercase: bool,

    /// Password should contain at least one lowercase letter
    #[introspection(sbroad_type = SbroadType::Boolean)]
    #[introspection(config_default = true)]
    pub auth_password_enforce_lowercase: bool,

    /// Password should contain at least one digit
    #[introspection(sbroad_type = SbroadType::Boolean)]
    #[introspection(config_default = true)]
    pub auth_password_enforce_digits: bool,

    /// Password should contain at least one special symbol.
    /// Special symbols - &, |, ?, !, $, @
    #[introspection(sbroad_type = SbroadType::Boolean)]
    #[introspection(config_default = false)]
    pub auth_password_enforce_specialchars: bool,

    /// Maximum number of login attempts through `picodata connect`.
    /// Each failed login attempt increases a local per user counter of failed attempts.
    /// When the counter reaches the value of this property any subsequent logins
    /// of this user will be denied.
    /// Local counter for a user is reset on successful login.
    #[introspection(sbroad_type = SbroadType::Unsigned)]
    #[introspection(config_default = 4)]
    pub auth_login_attempt_max: u64,

    /// PG statement storage size.
    #[introspection(sbroad_type = SbroadType::Unsigned)]
    #[introspection(config_default = 1024)]
    pub pg_statement_max: u64,

    /// PG portal storage size.
    #[introspection(sbroad_type = SbroadType::Unsigned)]
    #[introspection(config_default = 1024)]
    pub pg_portal_max: u64,

    /// Raft snapshot will be sent out in chunks not bigger than this threshold.
    /// Note: actual snapshot size may exceed this threshold. In most cases
    /// it will just add a couple of dozen metadata bytes. But in extreme
    /// cases if there's a tuple larger than this threshold, it will be sent
    /// in one piece whatever size it has. Please don't store tuples of size
    /// greater than this.
    #[introspection(sbroad_type = SbroadType::Unsigned)]
    #[introspection(config_default = 16 * 1024 * 1024)]
    pub raft_snapshot_chunk_size_max: u64,

    /// Snapshot read views with live reference counts will be forcefully
    /// closed after this number of seconds. This is necessary if followers
    /// do not properly finalize the snapshot application.
    // NOTE: maybe we should instead track the instance/raft ids of
    // followers which requested the snapshots and automatically close the
    // read views if the corresponding instances are *deteremined* to not
    // need them anymore. Or maybe timeouts is the better way..
    #[introspection(sbroad_type = SbroadType::Double)]
    #[introspection(config_default = (24 * 3600))]
    pub raft_snapshot_read_view_close_timeout: f64,

    /// Maximum size in bytes `_raft_log` system space is allowed to grow to
    /// before it gets automatically compacted.
    ///
    /// NOTE: The size is computed from the tuple storage only. Some memory will
    /// also be allocated for the index, and there's no way to control this.
    /// The option only controls the memory allocated for tuples.
    #[introspection(sbroad_type = SbroadType::Unsigned)]
    #[introspection(config_default = 64 * 1024 * 1024)]
    pub raft_wal_size_max: u64,

    /// Maximum number of tuples `_raft_log` system space is allowed to grow to
    /// before it gets automatically compacted.
    #[introspection(sbroad_type = SbroadType::Unsigned)]
    #[introspection(config_default = 64)]
    pub raft_wal_count_max: u64,

    /// Number of seconds to wait before automatically changing an
    /// unresponsive instance's state to Offline.
    #[introspection(sbroad_type = SbroadType::Double)]
    #[introspection(config_default = 30.0)]
    pub governor_auto_offline_timeout: f64,

    /// Governor proposes operations to raft log and waits for them to be
    /// applied to the local raft machine.
    /// Governor will only wait for this many seconds before going to the new loop iteration.
    #[introspection(sbroad_type = SbroadType::Double)]
    #[introspection(config_default = 3.0)]
    pub governor_raft_op_timeout: f64,

    /// Governor sets up replication and configuration in the cluster as well as
    /// applying global schema changes. It is doing so by making RPC requests to
    /// other instances.
    /// Governor will only wait for the RPC responses for this many seconds before going to the new loop iteration.
    #[introspection(sbroad_type = SbroadType::Double)]
    #[introspection(config_default = 3.0)]
    pub governor_common_rpc_timeout: f64,

    /// Governor sets up the plugin system by making RPC requests to other instances.
    /// Governor will only wait for the RPC responses for this many seconds before going to the new loop iteration.
    #[introspection(sbroad_type = SbroadType::Double)]
    #[introspection(config_default = 10.0)]
    pub governor_plugin_rpc_timeout: f64,

    #[introspection(sbroad_type = SbroadType::Unsigned)]
    #[introspection(config_default = 45000)]
    pub sql_vdbe_opcode_max: u64,

    #[introspection(sbroad_type = SbroadType::Unsigned)]
    #[introspection(config_default = 5000)]
    pub sql_motion_row_max: u64,

    /// The maximum number of snapshots that are stored in the memtx_dir
    /// directory. If the number of snapshots after creating a new one exceeds
    /// this value, the Tarantool garbage collector deletes old snapshots. If
    /// the option is set to zero, the garbage collector does not delete old
    /// snapshots.
    ///
    /// Corresponds to `box.cfg.checkpoint_count`.
    #[introspection(sbroad_type = SbroadType::Unsigned)]
    #[introspection(config_default = 2)]
    pub memtx_checkpoint_count: u64,

    /// The interval in seconds between actions by the checkpoint daemon. If the
    /// option is set to a value greater than zero, and there is activity that
    /// causes change to a database, then the checkpoint daemon calls
    /// box.snapshot() every checkpoint_interval seconds, creating a new
    /// snapshot file each time. If the option is set to zero, the checkpoint
    /// daemon is disabled.
    ///
    /// Corresponds to `box.cfg.checkpoint_interval`.
    #[introspection(config_default = 3600)]
    #[introspection(sbroad_type = SbroadType::Unsigned)]
    pub memtx_checkpoint_interval: u64,

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
    #[introspection(sbroad_type = SbroadType::Unsigned)]
    #[introspection(config_default = 0x300)]
    pub iproto_net_msg_max: u64,
}

/// A special macro helper for referring to alter system parameters thoroughout
/// the codebase. It makes sure the parameter with this name exists and returns
/// it's name.
///
/// You also get goto definition and other LSP goodness for the parameter with this macro.
#[macro_export]
macro_rules! system_parameter_name {
    ($name:ident) => {{
        #[allow(dead_code)]
        /// A helper which makes sure that the struct has a field with the given name.
        /// BTW we use the macro from `tarantool` and not from `std::mem`
        /// because rust-analyzer works with our macro but not with the builtin one ¯\_(ツ)_/¯.
        const DUMMY: usize = ::tarantool::offset_of!($crate::config::AlterSystemParameters, $name);
        ::std::stringify!($name)
    }};
}

/// Cached value of "pg_max_statements" option from "_pico_property".
/// 0 means that the value must be read from the table.
pub static MAX_PG_STATEMENTS: AtomicUsize = AtomicUsize::new(0);

/// Cached value of "pg_max_portals" option from "_pico_property".
/// 0 means that the value must be read from the table.
pub static MAX_PG_PORTALS: AtomicUsize = AtomicUsize::new(0);

pub fn validate_alter_system_parameter_value<'v>(
    name: &str,
    value: &'v Value,
) -> Result<EncodedValue<'v>, Error> {
    let Some(expected_type) = get_type_of_alter_system_parameter(name) else {
        return Err(Error::other(format!("unknown parameter: '{name}'")));
    };

    let Ok(casted_value) = value.cast_and_encode(&expected_type) else {
        let actual_type = value_type_str(value);
        return Err(Error::other(format!(
            "invalid value for '{name}' expected {expected_type}, got {actual_type}",
        )));
    };

    // Not sure how I feel about this...
    if name.ends_with("_timeout") {
        let value = casted_value
            .double()
            .expect("unreachable, already casted to double");

        if value < 0.0 {
            return Err(Error::other("timeout value cannot be negative"));
        }
    }

    Ok(casted_value)
}

/// Returns `None` if there's no such parameter.
pub fn get_type_of_alter_system_parameter(name: &str) -> Option<SbroadType> {
    let Ok(typ) = AlterSystemParameters::get_sbroad_type_of_field(name) else {
        return None;
    };

    let typ = typ.expect("types must be specified for all parameters");
    Some(typ)
}

/// Returns `None` if there's no such parameter.
pub fn get_default_value_of_alter_system_parameter(name: &str) -> Option<rmpv::Value> {
    // NOTE: we need an instance of this struct because of how `Introspection::get_field_default_value_as_rmpv`
    // works. We allow default values to refer to other fields of the struct
    // which were actuall provided (see `InstanceConfig::advertise_address` for example).
    // But for alter system parameters it doesn't make sense because these are not
    // stored in the struct itself but in a system table.
    // Alter system parameters currently don't delegate their defaults to other
    // fields, so we just construct a default value of the struct ad hoc when
    // needed.
    //
    // In the future maybe we want to maintain the values of the system
    // parameters in an actual struct and update those every time the values in
    // the system table change.
    let parameters = AlterSystemParameters::default();

    let Ok(default) = parameters.get_field_default_value_as_rmpv(name) else {
        return None;
    };

    let default = default.expect("default must be specified explicitly for all parameters");
    Some(default)
}

/// Returns an array of pairs (parameter name, default value).
pub fn get_defaults_for_all_alter_system_parameters() -> Vec<(String, rmpv::Value)> {
    // NOTE: we need an instance of this struct because of how `Introspection::get_field_default_value_as_rmpv`
    // works. We allow default values to refer to other fields of the struct
    // which were actuall provided (see `InstanceConfig::advertise_address` for example).
    // But for alter system parameters it doesn't make sense because these are not
    // stored in the struct itself but in a system table.
    // Alter system parameters currently don't delegate their defaults to other
    // fields, so we just construct a default value of the struct ad hoc when
    // needed.
    //
    // In the future maybe we want to maintain the values of the system
    // parameters in an actual struct and update those every time the values in
    // the system table change.
    let parameters = AlterSystemParameters::default();

    let mut result = Vec::with_capacity(AlterSystemParameters::FIELD_INFOS.len());
    for name in &leaf_field_paths::<AlterSystemParameters>() {
        let default = parameters
            .get_field_default_value_as_rmpv(name)
            .expect("paths are correct");
        let default = default.expect("default must be specified explicitly for all parameters");
        result.push((name.clone(), default));
    }
    result
}

/// Non-persistent apply of parameter from _pico_db_config
/// represented by key-value tuple.
///
/// In case of dynamic parameter, apply parameter via box.cfg.
///
/// Panic in following cases:
///   - tuple is not key-value with predefined schema
///   - while applying via box.cfg
pub fn apply_parameter(tuple: Tuple) {
    let name = tuple
        .field::<&str>(0)
        .expect("there is always 2 fields in _pico_db_config tuple")
        .expect("key is always present and it's type string");

    // set dynamic parameters
    if name == system_parameter_name!(memtx_checkpoint_count) {
        let value = tuple
            .field::<u64>(1)
            .expect("there is always 2 fields in _pico_db_config tuple")
            .expect("type already checked");

        set_cfg_field("checkpoint_count", value).expect("changing checkpoint_count shouldn't fail");
    } else if name == system_parameter_name!(memtx_checkpoint_interval) {
        let value = tuple
            .field::<f64>(1)
            .expect("there is always 2 fields in _pico_db_config tuple")
            .expect("type already checked");

        set_cfg_field("checkpoint_interval", value)
            .expect("changing checkpoint_interval shouldn't fail");
    } else if name == system_parameter_name!(iproto_net_msg_max) {
        let value = tuple
            .field::<u64>(1)
            .expect("there is always 2 fields in _pico_db_config tuple")
            .expect("type already checked");

        set_cfg_field("net_msg_max", value).expect("changing net_msg_max shouldn't fail");
    } else if name == system_parameter_name!(pg_portal_max) {
        let value = tuple
            .field::<usize>(1)
            .expect("there is always 2 fields in _pico_db_config tuple")
            .expect("type already checked");
        // Cache the value.
        MAX_PG_PORTALS.store(value, Ordering::Relaxed);
    } else if name == system_parameter_name!(pg_statement_max) {
        let value = tuple
            .field::<usize>(1)
            .expect("there is always 2 fields in _pico_db_config tuple")
            .expect("type already checked");
        // Cache the value.
        MAX_PG_STATEMENTS.store(value, Ordering::Relaxed);
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
// config_parameter_path!
////////////////////////////////////////////////////////////////////////////////

/// A helper macro for getting path of a configuration parameter as a
/// `&'static str` in a type safe fashion. Basically you provide a path to a
/// sub-field of [`PicodataConfig`] struct and the macro checks that such field
/// exists and returns a stringified version.
///
/// BTW rust-analyzer will also work with this macro, so you can do
/// go-to-definition and other stuff like that on the path you put into the macro.
#[macro_export]
macro_rules! config_parameter_path {
    ($head:ident $(. $tail:ident)*) => {{
        /// This computation is only needed to make sure that the provided path
        /// corresponds to some sub-field of `PicodataConfig` struct.
        const _: () = unsafe {
            let dummy = std::mem::MaybeUninit::<$crate::config::PicodataConfig>::uninit();
            let dummy_ptr = dummy.as_ptr();
            let _field_ptr = std::ptr::addr_of!((*dummy_ptr).$head $(.$tail)*);
        };
        concat!(stringify!($head), $(".", stringify!($tail),)*)
    }}
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address::PgprotoAddress;
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
    name: foobar

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
    instance_name: voter1

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
    fn cluster_name_is_required() {
        let yaml = r###"
cluster:
    tier:
        default:
instance:
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap();
        let err = config.validate_from_file().unwrap_err();
        assert_eq!(err.to_string(), "invalid configuration: either `cluster.name` or `instance.cluster_name` must be specified");

        let yaml = r###"
cluster:
    name: foo
    tier:
        default:
instance:
    cluster_name: bar
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap();
        let err = config.validate_from_file().unwrap_err();
        assert_eq!(err.to_string(), "invalid configuration: `cluster.name` (foo) conflicts with `instance.cluster_name` (bar)");
    }

    #[test]
    fn missing_tiers_is_not_error() {
        let yaml = r###"
cluster:
    name: test
"###;
        let cfg = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap();

        cfg.validate_from_file()
            .expect("absence `tier` section is ok");

        let yaml = r###"
cluster:
    name: test
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
    name: test
    tier:
        default:
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim()).unwrap();
        config.validate_from_file().unwrap();

        let yaml = r###"
cluster:
    default_replication_factor: 3
    name: test
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
    iproto_listen:  kevin:  <- spacey
"###;
        let err = PicodataConfig::read_yaml_contents(&yaml.trim_start()).unwrap_err();
        #[rustfmt::skip]
        assert_eq!(err.to_string(), "invalid configuration: mapping values are not allowed in this context at line 2 column 26");

        let yaml = r###"
instance:
    iproto_listen:  kevin->  :spacey   # <- some more trailing space
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim_start()).unwrap();
        let listen = config.instance.iproto_listen.unwrap();
        assert_eq!(listen.host, "kevin->  ");
        assert_eq!(listen.port, "spacey");

        let yaml = r###"
instance:
    iproto_listen:  kevin->  <-spacey
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim_start()).unwrap();
        let listen = config.instance.iproto_listen.unwrap();
        assert_eq!(listen.host, "kevin->  <-spacey");
        assert_eq!(listen.port, "3301");
    }

    #[test]
    fn default_http_port() {
        let yaml = r###"
instance:
    http_listen: 127.0.0.1
"###;
        let config = PicodataConfig::read_yaml_contents(&yaml.trim_start()).unwrap();
        let listen = config.instance.http_listen.unwrap();
        assert_eq!(listen.host, "127.0.0.1");
        assert_eq!(listen.port, "8080");
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
        config.handle_deprecated_parameters(&mut parameter_sources)?;
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
                vec![IprotoAddress::default()]
            );
            assert_eq!(config.instance.name(), None);
            assert_eq!(config.instance.iproto_listen().to_host_port(), IprotoAddress::default_host_port());
            assert_eq!(config.instance.iproto_advertise().to_host_port(), IprotoAddress::default_host_port());
            assert_eq!(config.instance.log_level(), SayLevel::Info);
            assert!(config.instance.failure_domain().data.is_empty());
        }

        //
        // Precedence: command line > env > config
        //
        {
            let yaml = r###"
instance:
    name: I-CONFIG
"###;

            // only config
            let config = setup_for_tests(Some(yaml), &["run"]).unwrap();

            assert_eq!(config.instance.name().unwrap(), "I-CONFIG");

            // PICODATA_CONFIG_PARAMETERS > config
            std::env::set_var("PICODATA_CONFIG_PARAMETERS", "instance.name=I-ENV-CONF-PARAM");
            let config = setup_for_tests(Some(yaml), &["run"]).unwrap();

            assert_eq!(config.instance.name().unwrap(), "I-ENV-CONF-PARAM");

            // other env > PICODATA_CONFIG_PARAMETERS
            std::env::set_var("PICODATA_INSTANCE_NAME", "I-ENVIRON");
            let config = setup_for_tests(Some(yaml), &["run"]).unwrap();

            assert_eq!(config.instance.name().unwrap(), "I-ENVIRON");

            // command line > env
            let config = setup_for_tests(Some(yaml), &["run", "--instance-name=I-COMMANDLINE"]).unwrap();

            assert_eq!(config.instance.name().unwrap(), "I-COMMANDLINE");

            // -c PARAMETER=VALUE > other command line
            let config = setup_for_tests(Some(yaml), &["run", "-c", "instance.name=I-CLI-CONF-PARAM", "--instance-name=I-COMMANDLINE"]).unwrap();

            assert_eq!(config.instance.name().unwrap(), "I-CLI-CONF-PARAM");
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
                vec![IprotoAddress::default()]
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
                    IprotoAddress {
                        user: None,
                        host: "bobbert".into(),
                        port: "420".into(),
                    },
                    IprotoAddress {
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
                    IprotoAddress {
                        user: None,
                        host: "oops there's a space over here -> <-".into(),
                        port: "13".into(),
                    },
                    IprotoAddress {
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
                    IprotoAddress {
                        user: None,
                        host: "one".into(),
                        port: "1".into(),
                    },
                    IprotoAddress {
                        user: None,
                        host: "two".into(),
                        port: "2".into(),
                    },
                    IprotoAddress {
                        user: None,
                        host: "    <- same problem here".into(),
                        port: "3301".into(),
                    },
                    IprotoAddress {
                        user: None,
                        host: "127.0.0.1".into(),
                        port: "3".into(),
                    },
                    IprotoAddress {
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
                    IprotoAddress {
                        user: None,
                        host: "host".into(),
                        port: "123".into(),
                    },
                    IprotoAddress {
                        user: None,
                        host: "ghost ".into(),
                        port: "321".into(),
                    },
                    IprotoAddress {
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

            assert_eq!(config.instance.iproto_listen().to_host_port(), "L-ENVIRON:3301");
            assert_eq!(config.instance.iproto_advertise().to_host_port(), "L-ENVIRON:3301");

            let yaml = r###"
instance:
    iproto_advertise: A-CONFIG
"###;
            let config = setup_for_tests(Some(yaml), &["run"]).unwrap();

            assert_eq!(config.instance.iproto_listen().to_host_port(), "L-ENVIRON:3301");
            assert_eq!(config.instance.iproto_advertise().to_host_port(), "A-CONFIG:3301");

            let config = setup_for_tests(Some(yaml), &["run", "-l", "L-COMMANDLINE"]).unwrap();

            assert_eq!(config.instance.iproto_listen().to_host_port(), "L-COMMANDLINE:3301");
            assert_eq!(config.instance.iproto_advertise().to_host_port(), "A-CONFIG:3301");
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
                "  instance.tier = ABC;cluster.name=DEF  ;
                instance.audit=audit.txt ;;
                ; instance.instance_dir=. ;"
            );
            let config = setup_for_tests(Some(yaml), &["run",
                "-c", "  instance.log .level =debug  ",
                "--config-parameter", "instance. memtx . memory=  999",
            ]).unwrap();
            assert_eq!(config.instance.tier.unwrap(), "ABC");
            assert_eq!(config.cluster.name.unwrap(), "DEF");
            assert_eq!(config.instance.log.level.unwrap(), args::LogLevel::Debug);
            assert_eq!(config.instance.memtx.memory.unwrap().to_string(), String::from("999B"));
            assert_eq!(config.instance.audit.unwrap(), "audit.txt");
            assert_eq!(config.instance.instance_dir.unwrap(), PathBuf::from("."));

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
                "  cluster.name=DEF  ;
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
        let pg = config.instance.pg;
        // pg section wasn't specified, but it should be enabled by default
        assert_eq!(
            pg.listen().to_host_port(),
            PgprotoAddress::default_host_port()
        );

        let yaml = r###"
instance:
    pg:
        listen: "127.0.0.1:5432"
        ssl: true
"###;
        let config = setup_for_tests(Some(yaml), &["run"]).unwrap();
        let pg = config.instance.pg;
        assert_eq!(&pg.listen().to_host_port(), "127.0.0.1:5432");
        assert!(pg.ssl());

        // test defaults
        let yaml = r###"
instance:
    pg:
        listen: "127.0.0.1:5432"
"###;
        let config = setup_for_tests(Some(yaml), &["run"]).unwrap();
        let pg = config.instance.pg;
        assert_eq!(
            pg.listen(),
            PgprotoAddress::from_str("127.0.0.1:5432").unwrap()
        );
        assert!(!pg.ssl());

        // test config with -c option
        let config =
            setup_for_tests(None, &["run", "-c", "instance.pg.listen=127.0.0.1:5432"]).unwrap();
        let pg = config.instance.pg;
        assert_eq!(
            pg.listen(),
            PgprotoAddress::from_str("127.0.0.1:5432").unwrap()
        );
        assert!(!pg.ssl());

        // test config from run args
        let config = setup_for_tests(None, &["run", "--pg-listen", "127.0.0.1:5432"]).unwrap();
        let pg = config.instance.pg;
        assert_eq!(
            pg.listen(),
            PgprotoAddress::from_str("127.0.0.1:5432").unwrap()
        );
        assert!(!pg.ssl());

        // test config from env
        std::env::set_var("PICODATA_PG_LISTEN", "127.0.0.1:1234");
        let config = setup_for_tests(None, &["run"]).unwrap();
        let pg = config.instance.pg;
        assert_eq!(
            pg.listen(),
            PgprotoAddress::from_str("127.0.0.1:1234").unwrap()
        );
        assert!(!pg.ssl());
    }

    #[test]
    fn test_bytesize_from_str() {
        let res = |s| ByteSize::from_str(s).unwrap();
        assert_eq!(res("1024").to_string(), "1024B");
        assert_eq!(res("1024").as_u64().unwrap(), 1024);

        assert_eq!(res("256B").to_string(), "256B");
        assert_eq!(res("256B").as_u64().unwrap(), 256);

        assert_eq!(res("64K").to_string(), "64K");
        assert_eq!(res("64K").as_u64().unwrap(), 65_536);

        assert_eq!(res("949M").to_string(), "949M");
        assert_eq!(res("949M").as_u64().unwrap(), 995_098_624);

        assert_eq!(res("1G").to_string(), "1G");
        assert_eq!(res("1G").as_u64().unwrap(), 1_073_741_824);

        assert_eq!(res("10T").to_string(), "10T");
        assert_eq!(res("10T").as_u64().unwrap(), 10_995_116_277_760);

        assert_eq!(res("0M").to_string(), "0M");
        assert_eq!(res("0M").as_u64().unwrap(), 0);

        assert_eq!(res("185T").to_string(), "185T");
        assert_eq!(res("185T").as_u64().unwrap(), 203_409_651_138_560);

        let e = |s| ByteSize::from_str(s).unwrap_err();
        assert_eq!(e(""), "cannot parse integer from empty string");
        assert_eq!(e("M"), "cannot parse integer from empty string");
        assert_eq!(e("X"), "invalid digit found in string");
        assert_eq!(e("errstr"), "invalid digit found in string");
        assert_eq!(e(" "), "invalid digit found in string");
        assert_eq!(e("1Z"), "invalid digit found in string");
        assert_eq!(e("1 K"), "invalid digit found in string");
        assert_eq!(e("1_000"), "invalid digit found in string");
        assert_eq!(e("1 000"), "invalid digit found in string");
        assert_eq!(e("1 000 X"), "invalid digit found in string");
        assert_eq!(e("17000000T"), "Value is too large");
    }

    #[test]
    fn test_iproto_listen_listen_interaction() {
        let _guard = protect_env();

        // iproto_listen should be equal to listen
        let yaml = r###"
instance:
        listen: localhost:3301
"###;
        let config = setup_for_tests(Some(yaml), &["run"]).unwrap();
        assert_eq!(
            config.instance.iproto_listen().to_host_port(),
            "localhost:3301"
        );

        // can't specify both
        let yaml = r###"
instance:
        listen: localhost:3302
        iproto_listen: localhost:3301
"###;
        let config = setup_for_tests(Some(yaml), &["run"]);

        assert_eq!(config.unwrap_err().to_string(), "invalid configuration: instance.listen is deprecated, use instance.iproto_listen instead (cannot use both at the same time)");

        let yaml = r###"
instance:
        iproto_listen: localhost:3302
"###;
        let config = setup_for_tests(Some(yaml), &["run", "--listen", "localhost:3303"]);

        assert_eq!(config.unwrap_err().to_string(), "invalid configuration: instance.listen is deprecated, use instance.iproto_listen instead (cannot use both at the same time)");
    }

    #[test]
    fn test_iproto_advertise_advertise_interaction() {
        let _guard = protect_env();

        // iproto_advertise should be equal to listen
        let yaml = r###"
instance:
        advertise_address: localhost:3301
"###;
        let config = setup_for_tests(Some(yaml), &["run"]).unwrap();
        assert_eq!(
            config.instance.iproto_advertise().to_host_port(),
            "localhost:3301"
        );

        // can't use both options
        let yaml = r###"
instance:
        advertise_address: localhost:3302
        iproto_advertise: localhost:3301
"###;
        let config = setup_for_tests(Some(yaml), &["run"]);
        assert_eq!(config.unwrap_err().to_string(), "invalid configuration: instance.advertise_address is deprecated, use instance.iproto_advertise instead (cannot use both at the same time)");

        // can't use both options
        let yaml = r###"
instance:
        iproto_advertise: localhost:3302
"###;
        let config = setup_for_tests(Some(yaml), &["run", "--advertise", "localhost:3303"]);
        assert_eq!(config.unwrap_err().to_string(), "invalid configuration: instance.advertise_address is deprecated, use instance.iproto_advertise instead (cannot use both at the same time)");
    }
}
