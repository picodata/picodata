pub mod manager;

use crate::schema::{PluginDef, ServiceDef, ServiceRouteItem, ServiceRouteKey, ADMIN_ID};
use libloading::Library;
use once_cell::unsync;
use picoplugin::interface::ServiceBox;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;
use tarantool::fiber;
use tarantool::time::Instant;

use crate::cas::{compare_and_swap, Range};
use crate::info::InstanceInfo;
use crate::storage::{ClusterwideTable, PropertyName};
use crate::traft::node::Node;
use crate::traft::op::{Dml, Op};
use crate::traft::{node, RaftIndex};
use crate::util::effective_user_id;
use crate::{cas, traft};

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
    #[error("Error while load plugin")]
    LoadAborted,
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
    #[error("Attempt to call non active plugin")]
    PluginInactive,
    #[error(transparent)]
    Tarantool(#[from] tarantool::error::Error),
    #[error("Plugin async events queue is full")]
    AsyncEventQueueFull,
    #[error("Plugin `{0}` not found at instance")]
    PluginNotFound(String),
    #[error("Service {0} for plugin `{1}` not found at instance")]
    ServiceNotFound(String, String),
    #[error("Remote call error: {0}")]
    RemoteError(String),
}

#[derive(thiserror::Error, Debug)]
pub enum PluginCallbackError {
    #[error("on_start: {0}")]
    OnStart(Box<dyn std::error::Error>),
    #[error("New configuration validation error: {0}")]
    InvalidConfiguration(Box<dyn std::error::Error>),
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
}

impl Manifest {
    /// Load manifest from file `{plugin_name}.yml`.
    pub fn load(plugin_name: &str) -> Result<Self> {
        // TODO move this into config (in future)
        let plugin_dir = PLUGIN_DIR.with(|dir| dir.lock().clone());
        let manifest_path = plugin_dir.join(format!("{plugin_name}.yaml"));
        // TODO non-blocking needed?
        let file = File::open(&manifest_path).map_err(|e| {
            PluginError::ManifestNotFound(manifest_path.to_string_lossy().to_string(), e)
        })?;

        let manifest = serde_yaml::from_reader(file).map_err(|e| {
            PluginError::InvalidManifest(manifest_path.to_string_lossy().to_string(), e)
        })?;
        Ok(manifest)
    }

    /// Return plugin defenition built from manifest.
    pub fn plugin_def(&self) -> PluginDef {
        PluginDef {
            name: self.name.to_string(),
            running: false,
            services: self
                .services
                .iter()
                .map(|srv| srv.name.to_string())
                .collect(),
            version: self.version.to_string(),
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
                schema_version: 0,
            })
            .collect()
    }
}

/// Events that may be fired at picodata
/// and which plugins should respond to.
#[derive(Clone, PartialEq, Debug)]
pub enum PluginEvent<'a> {
    /// New picodata instance started.
    InstanceStart { join: bool },
    /// Picodata instance shutdown (shutdown trigger is called).
    InstanceShutdown,
    /// New plugin load at instance.
    PluginLoad {
        name: &'a str,
        service_defs: &'a [ServiceDef],
    },
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
}

/// Events that may be fired at picodata
/// and which plugins should respond to *asynchronously*.
///
/// Asynchronous events needed when fired side can't yield while event handled by plugins,
/// for example, if event fired in transaction.
pub enum PluginAsyncEvent {
    /// Error occurred while the plugin loaded.
    PluginLoadError { name: String },
    /// Plugin service configuration is updated.
    ServiceConfigurationUpdated {
        plugin: String,
        service: String,
        old_raw: Vec<u8>,
        new_raw: Vec<u8>,
    },
    /// Plugin removed at instance.
    PluginRemoved { name: String },
}

/// Wait until [`Op::PluginLoadCommit`] or [`Op::PluginLoadAbort`] is coming.
fn wait_for_plugin_commit(
    node: &Node,
    prepare_commit: RaftIndex,
    timeout: Duration,
) -> traft::Result<()> {
    let raft_storage = &node.raft_storage;
    let deadline = fiber::clock().saturating_add(timeout);
    let last_seen = prepare_commit;
    loop {
        let cur_applied = node.get_index();
        let new_entries = raft_storage.entries(last_seen + 1, cur_applied + 1, None)?;
        for entry in new_entries {
            if entry.entry_type != raft::prelude::EntryType::EntryNormal {
                continue;
            }

            match entry.into_op() {
                Some(Op::PluginLoadCommit) => return Ok(()),
                Some(Op::PluginLoadAbort) => return Err(PluginError::LoadAborted.into()),
                _ => (),
            }
        }

        node.wait_index(cur_applied + 1, deadline.duration_since(fiber::clock()))?;
    }
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

        let res = cas::compare_and_swap(
            op,
            cas::Predicate {
                index: raft_storage.applied()?,
                term: raft_storage.term()?,
                ranges: ranges.clone(),
            },
            ADMIN_ID,
            deadline.duration_since(fiber::clock()),
        );
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

        let cas_result = compare_and_swap(
            op.clone(),
            cas::Predicate {
                index: raft_storage.applied()?,
                term: raft_storage.term()?,
                ranges: ranges.clone(),
            },
            // FIXME: access rules will be implemented in future release
            effective_user_id(),
            deadline.duration_since(Instant::now()),
        );
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

/// Create a new plugin.
// FIXME: in future there is two operation instead this one: install_plugin and enable_plugin
pub fn create_plugin(
    name: &str,
    on_start_timeout: Duration,
    timeout: Duration,
) -> traft::Result<()> {
    let deadline = Instant::now().saturating_add(timeout);

    let node = node::global()?;
    let manifest = Manifest::load(name)?;
    let op = Op::PluginLoadPrepare {
        manifest,
        on_start_timeout,
    };

    let index = do_plugin_cas(
        node,
        op,
        vec![Range::new(ClusterwideTable::Property).eq([PropertyName::PendingPluginLoad])],
        Some(|node| Ok(node.storage.properties.pending_plugin_load()?.is_some())),
        deadline,
    )?;

    wait_for_plugin_commit(node, index, deadline.duration_since(Instant::now()))
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

/// Remove plugin.
// FIXME: in future there is two operation instead this one: disable_plugin and remove_plugin
pub fn remove_plugin(plugin_name: &str, timeout: Duration) -> traft::Result<()> {
    let deadline = Instant::now().saturating_add(timeout);
    let node = node::global()?;
    let op = Op::PluginDisable {
        name: plugin_name.to_string(),
    };

    do_plugin_cas(
        node,
        op,
        vec![Range::new(ClusterwideTable::Plugin).eq([plugin_name])],
        None,
        deadline,
    )
    .map(|_| ())
}
