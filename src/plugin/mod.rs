mod ffi;
pub mod manager;
pub mod migration;
pub mod topology;

use crate::schema::{PluginDef, ServiceDef, ServiceRouteItem, ServiceRouteKey, ADMIN_ID};
use libloading::Library;
use once_cell::unsync;
use picoplugin::plugin::interface::ServiceBox;
use serde::de::Error;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::fiber;
use tarantool::time::Instant;

use crate::cas::{compare_and_swap, Range};
use crate::info::InstanceInfo;
use crate::plugin::PluginError::{PluginNotFound, RemoveOfEnabledPlugin};
use crate::storage::{ClusterwideTable, PropertyName};
use crate::traft::node::Node;
use crate::traft::op::{Dml, Op};
use crate::traft::{node, RaftIndex};
use crate::util::effective_user_id;
use crate::{cas, error_injection, tlog, traft};

const DEFAULT_PLUGIN_DIR: &'static str = "/usr/share/picodata/";

thread_local! {
    // TODO this will be removed and replaced with the config (in future)
    static PLUGIN_DIR: unsync::Lazy<fiber::Mutex<PathBuf>> =
        unsync::Lazy::new(|| fiber::Mutex::new(PathBuf::from(DEFAULT_PLUGIN_DIR)));
}

/// Set the new plugin directory.
/// Search of manifest and shared objects will take place in this directory.
pub fn set_plugin_dir(path: &Path) {
    PLUGIN_DIR.with(|dir| *dir.lock() = path.to_path_buf());
}

#[derive(thiserror::Error, Debug)]
pub enum PluginError {
    #[error("Error while install the plugin")]
    InstallationAborted,
    #[error("Error while enable the plugin")]
    EnablingAborted,
    #[error("Error while update plugin topology")]
    TopologyUpdateAborted,
    #[error("Error while discovering manifest for plugin `{0}`: {1}")]
    ManifestNotFound(String, io::Error),
    #[error("Error while parsing manifest `{0}`, reason: {1}")]
    InvalidManifest(String, serde_yaml::Error),
    #[error("`{0}` service defenition not found")]
    ServiceDefenitionNotFound(String),
    #[error("Read plugin_dir: {0}")]
    ReadPluginDir(#[from] io::Error),
    #[error("Invalid shared object file: {0}")]
    InvalidSharedObject(#[from] libloading::Error),
    #[error("Plugin partial load (some of services not found)")]
    PartialLoad,
    #[error("Callback: {0}")]
    Callback(#[from] PluginCallbackError),
    #[error("Attempt to call a disabled plugin")]
    PluginDisabled,
    #[error(transparent)]
    Tarantool(#[from] tarantool::error::Error),
    #[error("Plugin async events queue is full")]
    AsyncEventQueueFull,
    #[error("Plugin `{0}` not found at instance")]
    PluginNotFound(String),
    #[error("Service `{0}` for plugin `{1}` not found at instance")]
    ServiceNotFound(String, String),
    #[error("Remote call error: {0}")]
    RemoteError(String),
    #[error("Remove of enabled plugin is forbidden")]
    RemoveOfEnabledPlugin,
    #[error("Topology: {0}")]
    TopologyError(String),
    #[error("Found more than one service factory for `{0}` ver. `{1}`")]
    ServiceCollision(String, String),
    #[error(transparent)]
    Migration(#[from] migration::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum PluginCallbackError {
    #[error("on_start: {0}")]
    OnStart(BoxError),
    #[error("New configuration validation error: {0}")]
    InvalidConfiguration(BoxError),
}

type Result<T> = std::result::Result<T, PluginError>;

pub struct Service {
    inner: ServiceBox,
    pub name: String,
    pub plugin_name: String,
    _lib: Rc<Library>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
struct ServiceManifest {
    /// Service name
    name: String,
    /// Service description
    description: String,
    /// Service default configuration (this configuration will be sent to `on_start` callback)
    default_configuration: rmpv::Value,
}

impl Eq for ServiceManifest {}

/// Plugin manifest, contains all information needed to load plugin into picodata.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    /// Plugin name
    pub name: String,
    /// Plugin description
    description: String,
    /// Plugin version (TODO use SemVer instead of any string)
    pub version: String,
    /// Plugin services
    services: Vec<ServiceManifest>,
    /// Plugin migration list.
    #[serde(default)]
    pub migration: Vec<String>,
}

impl Manifest {
    /// Load manifest from file `{plugin_dir}/{plugin_name}/manifest.yml`.
    pub fn load(plugin_name: &str) -> Result<Self> {
        // TODO move this into config (in future)
        let plugin_dir = PLUGIN_DIR.with(|dir| dir.lock().clone());
        let manifest_path = plugin_dir.join(format!("{plugin_name}/manifest.yaml"));
        // TODO non-blocking needed?
        let file = File::open(&manifest_path).map_err(|e| {
            PluginError::ManifestNotFound(manifest_path.to_string_lossy().to_string(), e)
        })?;

        let manifest: Manifest = serde_yaml::from_reader(file).map_err(|e| {
            PluginError::InvalidManifest(manifest_path.to_string_lossy().to_string(), e)
        })?;
        if manifest.name != plugin_name {
            return Err(PluginError::InvalidManifest(
                manifest_path.to_string_lossy().to_string(),
                serde_yaml::Error::custom("plugin name should be equal to manifest name"),
            ));
        }

        Ok(manifest)
    }

    /// Return plugin defenition built from manifest.
    pub fn plugin_def(&self) -> PluginDef {
        PluginDef {
            name: self.name.to_string(),
            enabled: false,
            services: self
                .services
                .iter()
                .map(|srv| srv.name.to_string())
                .collect(),
            version: self.version.to_string(),
            description: self.description.to_string(),
            migration_list: self.migration.clone(),
            migration_progress: -1,
        }
    }

    /// Return plugin service definitions built from manifest.
    pub fn service_defs(&self) -> Vec<ServiceDef> {
        self.services
            .iter()
            .map(|srv| ServiceDef {
                plugin_name: self.name.to_string(),
                name: srv.name.to_string(),
                tiers: vec![],
                configuration: srv.default_configuration.clone(),
                version: self.version.to_string(),
                description: srv.description.to_string(),
            })
            .collect()
    }
}

/// Events that may be fired at picodata
/// and which plugins should respond to.
#[derive(Clone, PartialEq, Debug)]
pub enum PluginEvent<'a> {
    /// Picodata instance goes online.
    InstanceOnline,
    /// Picodata instance shutdown (shutdown trigger is called).
    InstanceShutdown,
    /// New plugin load at instance.
    PluginLoad {
        name: &'a str,
        service_defs: &'a [ServiceDef],
    },
    /// Error occurred while the plugin loaded.
    PluginLoadError { name: &'a str },
    /// Request for update service configuration received.
    BeforeServiceConfigurationUpdated {
        plugin: &'a str,
        service: &'a str,
        new_raw: &'a [u8],
    },
    /// Instance demote.
    InstanceDemote,
    /// Instance promote as a replicaset leader.
    InstancePromote,
    /// Plugin service enabled at instance.
    ServiceEnabled { plugin: &'a str, service: &'a str },
    /// Plugin service disabled at instance.
    ServiceDisabled { plugin: &'a str, service: &'a str },
}

/// Events that may be fired at picodata
/// and which plugins should respond to *asynchronously*.
///
/// Asynchronous events needed when fired side can't yield while event handled by plugins,
/// for example, if event fired in transaction.
pub enum PluginAsyncEvent {
    /// Plugin service configuration is updated.
    ServiceConfigurationUpdated {
        plugin: String,
        service: String,
        old_raw: Vec<u8>,
        new_raw: Vec<u8>,
    },
    /// Plugin removed at instance.
    PluginDisabled { name: String },
}

/// Perform clusterwide CAS operation related to plugin routing table.
///
/// # Arguments
///
/// * `dml_ops`: list of dml operations
/// * `timeout`: timeout of whole operation
/// * `ranges`: CAS ranges
fn do_routing_table_cas(
    dml_ops: Vec<Dml>,
    ranges: Vec<Range>,
    timeout: Duration,
) -> traft::Result<()> {
    let node = node::global()?;
    let raft_storage = &node.raft_storage;

    let deadline = fiber::clock().saturating_add(timeout);
    loop {
        let op = Op::BatchDml {
            ops: dml_ops.clone(),
        };

        let req = crate::cas::Request::new(
            op,
            cas::Predicate {
                index: raft_storage.applied()?,
                term: raft_storage.term()?,
                ranges: ranges.clone(),
            },
            ADMIN_ID,
        )?;
        let res = cas::compare_and_swap(&req, deadline.duration_since(fiber::clock()));
        match res {
            Ok((index, term)) => {
                node.wait_index(index, deadline.duration_since(fiber::clock()))?;
                if term != raft::Storage::term(raft_storage, index)? {
                    // leader switched - retry
                    node.wait_status();
                    continue;
                }
            }
            Err(err) => {
                if err.is_cas_err() || err.is_term_mismatch_err() {
                    // cas error - retry
                    fiber::sleep(Duration::from_millis(500));
                    continue;
                } else {
                    return Err(err);
                }
            }
        }
        return Ok(());
    }
}

/// Replace routes in plugins routing table.
pub fn replace_routes(items: &[ServiceRouteItem], timeout: Duration) -> traft::Result<()> {
    if items.is_empty() {
        // dont need to do parasite batch request
        return Ok(());
    }

    let ops = items
        .iter()
        .map(|routing_item| {
            Dml::replace(ClusterwideTable::ServiceRouteTable, &routing_item, ADMIN_ID)
                .expect("encoding should not fail")
        })
        .collect();

    // assert that instances update only self-owned information
    debug_assert!({
        let node = node::global()?;
        let i = InstanceInfo::try_get(node, None)?;
        items.iter().all(|route| route.instance_id == i.instance_id)
    });
    // use empty ranges cause all instances update only self-owned information
    let ranges = vec![];

    do_routing_table_cas(ops, ranges, timeout)
}

/// Remove routes from the plugin routing table.
pub fn remove_routes(keys: &[ServiceRouteKey], timeout: Duration) -> traft::Result<()> {
    if keys.is_empty() {
        // dont need to do parasite batch request
        return Ok(());
    }

    let ops = keys
        .iter()
        .map(|routing_key| {
            Dml::delete(ClusterwideTable::ServiceRouteTable, &routing_key, ADMIN_ID)
                .expect("encoding should not fail")
        })
        .collect();

    // assert that instances update only self-owned information
    debug_assert!({
        let node = node::global()?;
        let i = InstanceInfo::try_get(node, None)?;
        keys.iter().all(|key| key.instance_id == &i.instance_id)
    });
    // use empty ranges cause all instances update only self-owned information
    let ranges = vec![];

    do_routing_table_cas(ops, ranges, timeout)
}

/// Perform clusterwide CAS operation related to plugin system.
///
/// # Arguments
///
/// * `node`: instance node
/// * `op`: CAS operation
/// * `ranges`: CAS ranges
/// * `try_again_condition`: callback, if true - then perform CAS later
/// * `deadline`: deadline of whole operation
fn do_plugin_cas(
    node: &Node,
    op: Op,
    ranges: Vec<Range>,
    try_again_condition: Option<fn(&Node) -> traft::Result<bool>>,
    deadline: Instant,
) -> traft::Result<RaftIndex> {
    let raft_storage = &node.raft_storage;

    loop {
        let index = node.read_index(deadline.duration_since(Instant::now()))?;
        if let Some(try_again_condition) = try_again_condition {
            if try_again_condition(node)? {
                node.wait_index(index + 1, deadline.duration_since(Instant::now()))?;
                continue;
            }
        }

        let req = crate::cas::Request::new(
            op.clone(),
            cas::Predicate {
                index: raft_storage.applied()?,
                term: raft_storage.term()?,
                ranges: ranges.clone(),
            },
            // FIXME: access rules will be implemented in future release
            effective_user_id(),
        )?;
        let cas_result = compare_and_swap(&req, deadline.duration_since(Instant::now()));
        match cas_result {
            Ok((index, term)) => {
                node.wait_index(index, deadline.duration_since(Instant::now()))?;
                if term != raft::Storage::term(raft_storage, index)? {
                    // leader switched - retry
                    node.wait_status();
                    continue;
                }
            }
            Err(err) => {
                if err.is_cas_err() | err.is_term_mismatch_err() {
                    // cas error - retry
                    fiber::sleep(Duration::from_millis(500));
                    continue;
                } else {
                    return Err(err);
                }
            }
        }

        return Ok(index);
    }
}

////////////////////////////////////////////////////////////////////////////////
// External plugin interface
////////////////////////////////////////////////////////////////////////////////

/// Install plugin:
/// 1) check that plugin is ready for run at all instances
/// 2) fill `_pico_service`, `_pico_plugin` and set `_pico_plugin.ready` to `false`
pub fn install_plugin(name: &str, timeout: Duration) -> traft::Result<()> {
    let deadline = fiber::clock().saturating_add(timeout);
    let node = node::global()?;
    let manifest = Manifest::load(name)?;

    let dml = Dml::replace(
        ClusterwideTable::Property,
        &(&PropertyName::PluginInstall, &manifest),
        effective_user_id(),
    )?;

    let mut index = do_plugin_cas(
        node,
        Op::Dml(dml),
        vec![Range::new(ClusterwideTable::Property).eq([PropertyName::PluginInstall])],
        Some(|node| Ok(node.storage.properties.plugin_install()?.is_some())),
        deadline,
    )?;

    while node.storage.properties.plugin_install()?.is_some() {
        node.wait_index(index + 1, deadline.duration_since(Instant::now()))?;
        index = node.read_index(deadline.duration_since(Instant::now()))?;
    }

    let Some(plugin) = node.storage.plugin.get(name)? else {
        return Err(PluginError::InstallationAborted.into());
    };

    if error_injection::is_enabled("PLUGIN_MIGRATION_CLIENT_DOWN") {
        return Ok(());
    }

    let migration_result =
        migration::apply_up_migrations(&plugin.name, &manifest.migration, deadline);
    if let Err(e) = migration_result {
        if let Err(err) = remove_plugin(&plugin.name, Duration::from_secs(2), true) {
            tlog!(Error, "rollback plugin installation error: {err}");
        }
        return Err(e.into());
    }

    Ok(())
}

/// Enable plugin:
/// 1) call `on_start` at all instances (and `on_stop` if something happened wrong)
/// 2) set `_pico_plugin.enable` to `true`
/// 3) update routes in `_pico_service_route`
pub fn enable_plugin(
    name: &str,
    on_start_timeout: Duration,
    timeout: Duration,
) -> traft::Result<()> {
    let deadline = Instant::now().saturating_add(timeout);

    let node = node::global()?;

    let op = Op::PluginEnable {
        plugin_name: name.to_string(),
        on_start_timeout,
    };

    let mut index = do_plugin_cas(
        node,
        op,
        vec![Range::new(ClusterwideTable::Property).eq([PropertyName::PendingPluginEnable])],
        Some(|node| Ok(node.storage.properties.pending_plugin_enable()?.is_some())),
        deadline,
    )?;

    while node.storage.properties.pending_plugin_enable()?.is_some() {
        node.wait_index(index + 1, deadline.duration_since(Instant::now()))?;
        index = node.read_index(deadline.duration_since(Instant::now()))?;
    }

    let plugin = node
        .storage
        .plugin
        .get(name)?
        .ok_or(PluginError::EnablingAborted)?;

    if !plugin.enabled {
        return Err(PluginError::EnablingAborted.into());
    }

    Ok(())
}

/// Update plugin service configuration.
pub fn update_plugin_service_configuration(
    plugin_name: &str,
    service_name: &str,
    new_cfg_raw: &[u8],
    timeout: Duration,
) -> traft::Result<()> {
    let deadline = Instant::now().saturating_add(timeout);

    let node = node::global()?;
    node.plugin_manager
        .handle_event_sync(PluginEvent::BeforeServiceConfigurationUpdated {
            plugin: plugin_name,
            service: service_name,
            new_raw: new_cfg_raw,
        })?;

    let new_cfg: rmpv::Value = rmp_serde::from_slice(new_cfg_raw).expect("out of memory");
    let op = Op::PluginConfigUpdate {
        plugin_name: plugin_name.to_string(),
        service_name: service_name.to_string(),
        config: new_cfg,
    };

    do_plugin_cas(
        node,
        op,
        vec![Range::new(ClusterwideTable::Service).eq((plugin_name, service_name))],
        None,
        deadline,
    )
    .map(|_| ())
}

/// Disable plugin:
/// 1) call `on_stop` for each service in plugin
/// 2) update routes in `_pico_service_route`
/// 3) set `_pico_plugin.enable` to `false`
pub fn disable_plugin(plugin_name: &str, timeout: Duration) -> traft::Result<()> {
    let deadline = Instant::now().saturating_add(timeout);
    let node = node::global()?;
    let op = Op::PluginDisable {
        name: plugin_name.to_string(),
    };

    // it is ok to return error here based on local state,
    // we expect that in small count of cases
    // when "plugin does not exist in local state but exist on leader"
    // user will retry disable manually
    if !node.storage.plugin.contains(plugin_name)? {
        return Err(PluginNotFound(plugin_name.to_string()).into());
    }

    let mut index = do_plugin_cas(
        node,
        op,
        vec![Range::new(ClusterwideTable::Plugin).eq([plugin_name])],
        Some(|node| Ok(node.storage.properties.pending_plugin_disable()?.is_some())),
        deadline,
    )?;

    while node.storage.properties.pending_plugin_disable()?.is_some() {
        node.wait_index(index + 1, deadline.duration_since(Instant::now()))?;
        index = node.read_index(deadline.duration_since(Instant::now()))?;
    }

    Ok(())
}

/// Remove plugin: clear records from `_pico_plugin` and `_pico_service` system tables.
///
/// # Arguments
///
/// * `plugin_name`: plugin name to remove
/// * `timeout`: operation timeout
/// * `force`: whether true if plugin should be removed without DOWN migration, false elsewhere
pub fn remove_plugin(plugin_name: &str, timeout: Duration, force: bool) -> traft::Result<()> {
    let deadline = Instant::now().saturating_add(timeout);

    let node = node::global()?;
    let maybe_plugin = node.storage.plugin.get(plugin_name)?;
    // we check this condition on any instance, this will allow
    // to return an error in most situations, but there are still
    // situations when instance is a follower and has not yet received up-to-date
    // information from the leader - in this case,
    // the error will not be returned to client and raft op
    // must be applied on instances correctly (op should ignore removing if
    // plugin exists and enabled)
    let plugin_exists_and_enabled = maybe_plugin.as_ref().map(|p| p.enabled) == Some(true);
    if plugin_exists_and_enabled && !error_injection::is_enabled("PLUGIN_EXIST_AND_ENABLED") {
        return Err(RemoveOfEnabledPlugin.into());
    }
    let op = Op::PluginRemove {
        name: plugin_name.to_string(),
    };

    do_plugin_cas(
        node,
        op,
        vec![
            Range::new(ClusterwideTable::Plugin).eq([plugin_name]),
            Range::new(ClusterwideTable::Service).eq([plugin_name]),
        ],
        None,
        deadline,
    )?;

    if !force {
        if let Some(plugin) = maybe_plugin {
            migration::apply_down_migrations(&plugin.name, &plugin.migration_list);
        }
    }

    Ok(())
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum TopologyUpdateOp {
    /// Append service to a new tier.
    Append {
        plugin_name: String,
        service_name: String,
        tier: String,
    },
    /// Remove service from a tier.
    Remove {
        plugin_name: String,
        service_name: String,
        tier: String,
    },
}

impl TopologyUpdateOp {
    #[inline(always)]
    pub fn plugin_name(&self) -> &str {
        match self {
            TopologyUpdateOp::Append { plugin_name, .. } => plugin_name,
            TopologyUpdateOp::Remove { plugin_name, .. } => plugin_name,
        }
    }

    #[inline(always)]
    pub fn service_name(&self) -> &str {
        match self {
            TopologyUpdateOp::Append { service_name, .. } => service_name,
            TopologyUpdateOp::Remove { service_name, .. } => service_name,
        }
    }

    #[inline(always)]
    pub fn tier(&self) -> &str {
        match self {
            TopologyUpdateOp::Append { tier, .. } => tier,
            TopologyUpdateOp::Remove { tier, .. } => tier,
        }
    }
}

fn update_tier(upd_op: TopologyUpdateOp, timeout: Duration) -> traft::Result<()> {
    let deadline = Instant::now().saturating_add(timeout);
    let node = node::global()?;

    let mb_service = node
        .storage
        .service
        .get_any_version(upd_op.plugin_name(), upd_op.service_name())?;

    if mb_service.is_none() {
        return Err(PluginError::ServiceNotFound(
            upd_op.service_name().to_string(),
            upd_op.plugin_name().to_string(),
        )
        .into());
    }

    let ranges = vec![
        Range::new(ClusterwideTable::Plugin).eq([upd_op.plugin_name()]),
        Range::new(ClusterwideTable::Service).eq([upd_op.plugin_name(), upd_op.service_name()]),
    ];
    let op = Op::PluginUpdateTopology { op: upd_op.clone() };

    let mut index = do_plugin_cas(
        node,
        op,
        ranges,
        Some(|node| {
            Ok(node
                .storage
                .properties
                .pending_plugin_topology_update()?
                .is_some())
        }),
        deadline,
    )?;

    while node
        .storage
        .properties
        .pending_plugin_topology_update()?
        .is_some()
    {
        node.wait_index(index + 1, deadline.duration_since(Instant::now()))?;
        index = node.read_index(deadline.duration_since(Instant::now()))?;
    }

    let service = node
        .storage
        .service
        .get_any_version(upd_op.plugin_name(), upd_op.service_name())?
        .ok_or(PluginError::TopologyUpdateAborted)?;

    let contains = service.tiers.iter().any(|t| t == upd_op.tier());
    if matches!(upd_op, TopologyUpdateOp::Append { .. }) {
        if !contains {
            return Err(PluginError::TopologyUpdateAborted.into());
        }
    } else if contains {
        return Err(PluginError::TopologyUpdateAborted.into());
    }

    Ok(())
}

/// Enable service on a new tier.
pub fn append_tier(
    plugin_name: &str,
    service_name: &str,
    tier: &str,
    timeout: Duration,
) -> traft::Result<()> {
    update_tier(
        TopologyUpdateOp::Append {
            plugin_name: plugin_name.to_string(),
            service_name: service_name.to_string(),
            tier: tier.to_string(),
        },
        timeout,
    )
}

/// Disable service on a new tier.
pub fn remove_tier(
    plugin_name: &str,
    service_name: &str,
    tier: &str,
    timeout: Duration,
) -> traft::Result<()> {
    update_tier(
        TopologyUpdateOp::Remove {
            plugin_name: plugin_name.to_string(),
            service_name: service_name.to_string(),
            tier: tier.to_string(),
        },
        timeout,
    )
}
