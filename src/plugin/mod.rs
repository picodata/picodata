mod ffi;
pub mod manager;
pub mod migration;
pub mod rpc;
pub mod topology;

use once_cell::unsync;
use picoplugin::background::ServiceId;
use picoplugin::plugin::interface::ServiceBox;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::fiber;
use tarantool::time::Instant;

use crate::cas::Range;
use crate::info::InstanceInfo;
use crate::plugin::migration::MigrationInfo;
use crate::plugin::PluginError::{PluginNotFound, RemoveOfEnabledPlugin};
use crate::schema::{PluginDef, ServiceDef, ServiceRouteItem, ServiceRouteKey, ADMIN_ID};
use crate::storage::{ClusterwideTable, PropertyName};
use crate::traft::error::Error;
use crate::traft::node::Node;
use crate::traft::op::PluginRaftOp;
use crate::traft::op::{Dml, Op};
use crate::traft::{node, RaftIndex};
use crate::unwrap_ok_or;
use crate::util::effective_user_id;
use crate::{cas, tlog, traft};

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
    #[error("Plugin `{0}` already exists")]
    AlreadyExist(PluginIdentifier),
    #[error("Error while install the plugin")]
    InstallationAborted,
    #[error("Error while enable the plugin")]
    EnablingAborted,
    #[error("Error while update plugin topology")]
    TopologyUpdateAborted,
    #[error("Error while discovering manifest for plugin `{0}`: {1}")]
    ManifestNotFound(String, io::Error),
    #[error("Error while parsing manifest `{0}`, reason: {1}")]
    InvalidManifest(String, Box<dyn std::error::Error>),
    #[error("`{0}` service defenition not found")]
    ServiceDefenitionNotFound(String),
    #[error("Read plugin_dir: {0}")]
    ReadPluginDir(#[from] io::Error),
    #[error("Invalid shared object file: {0}")]
    InvalidSharedObject(#[from] libloading::Error),
    #[error("Plugin partial load (some of services not found: {0:?})")]
    PartialLoad(Vec<String>),
    #[error("Callback: {0}")]
    Callback(#[from] PluginCallbackError),
    #[error("Attempt to call a disabled plugin")]
    PluginDisabled,
    #[error(transparent)]
    Tarantool(#[from] tarantool::error::Error),
    #[error("Plugin async events queue is full")]
    AsyncEventQueueFull,
    #[error("Plugin `{0}` not found at instance")]
    PluginNotFound(PluginIdentifier),
    #[error("Service `{0}` for plugin `{1}` not found at instance")]
    ServiceNotFound(String, PluginIdentifier),
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
    #[error(
        "Cannot specify install candidate (there should be only one directory in plugin main dir)"
    )]
    AmbiguousInstallCandidate,
    #[error("Cannot specify enable candidate (there should be only one installed plugin version)")]
    AmbiguousEnableCandidate,
}

#[derive(thiserror::Error, Debug)]
pub enum PluginCallbackError {
    #[error("on_start: {0}")]
    OnStart(BoxError),
    #[error("New configuration validation error: {0}")]
    InvalidConfiguration(BoxError),
}

type Result<T, E = PluginError> = std::result::Result<T, E>;

pub struct Service {
    inner: ServiceBox,
    pub name: String,
    pub version: String,
    pub plugin_name: String,
    pub id: ServiceId,
    _lib: Rc<LibraryWrapper>,
}

pub struct LibraryWrapper {
    pub inner: libloading::Library,
    pub filename: std::path::PathBuf,
}

impl LibraryWrapper {
    /// Find and load a dynamic library.
    ///
    /// The `filename` argument may be either:
    ///
    /// * A library filename;
    /// * The absolute path to the library;
    /// * A relative (to the current working directory) path to the library.
    ///
    /// # Safety
    ///
    /// When a library is loaded, initialisation routines contained within it are executed.
    /// For the purposes of safety, the execution of these routines is conceptually the same calling an
    /// unknown foreign function and may impose arbitrary requirements on the caller for the call
    /// to be sound.
    ///
    /// Additionally, the callers of this function must also ensure that execution of the
    /// termination routines contained within the library is safe as well. These routines may be
    /// executed when the library is unloaded.
    ///
    /// For more infomation see [`libloading::Library::new`].
    #[inline]
    pub unsafe fn new(filename: std::path::PathBuf) -> Result<Self, libloading::Error> {
        let inner = libloading::Library::new(&filename)?;
        tlog!(Debug, "opened library '{}'", filename.display());
        Ok(Self { inner, filename })
    }

    /// Get a pointer to a function or static variable by symbol name.
    ///
    /// The `symbol` may not contain any null bytes, with the exception of the last byte. Providing a
    /// null-terminated `symbol` may help to avoid an allocation.
    ///
    /// The symbol is interpreted as-is; no mangling is done. This means that symbols like `x::y` are
    /// most likely invalid.
    ///
    /// # Safety
    ///
    /// Users of this API must specify the correct type of the function or variable loaded.
    ///
    /// # Platform-specific behaviour
    ///
    /// The implementation of thread-local variables is extremely platform specific and uses of such
    /// variables that work on e.g. Linux may have unintended behaviour on other targets.
    ///
    /// On POSIX implementations where the `dlerror` function is not confirmed to be MT-safe (such
    /// as FreeBSD), this function will unconditionally return an error when the underlying `dlsym`
    /// call returns a null pointer. There are rare situations where `dlsym` returns a genuine null
    /// pointer without it being an error. If loading a null pointer is something you care about,
    /// consider using the [`os::unix::Library::get_singlethreaded`] call.
    ///
    /// [`os::unix::Library::get_singlethreaded`]: libloading::os::unix::Library::get_singlethreaded
    #[inline(always)]
    pub unsafe fn get<'a, T>(
        &'a self,
        symbol: &str,
    ) -> Result<libloading::Symbol<'a, T>, libloading::Error> {
        self.inner.get(symbol.as_bytes())
    }
}

impl Drop for LibraryWrapper {
    #[inline(always)]
    fn drop(&mut self) {
        tlog!(Debug, "closing library '{}'", self.filename.display());
    }
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
    /// Load manifest from file `{plugin_dir}/{plugin_name}/{version}/manifest.yml`.
    pub fn load(ident: &PluginIdentifier) -> Result<Self> {
        let plugin_name = ident.name.as_str();
        let version = ident.version.as_str();

        // TODO move this into config (in future)
        let plugin_dir = PLUGIN_DIR.with(|dir| dir.lock().clone());
        let manifest_path = plugin_dir.join(format!("{plugin_name}/{version}/manifest.yaml"));
        // TODO non-blocking needed?
        let file = File::open(&manifest_path).map_err(|e| {
            PluginError::ManifestNotFound(manifest_path.to_string_lossy().to_string(), e)
        })?;

        let manifest: Manifest = serde_yaml::from_reader(file).map_err(|e| {
            PluginError::InvalidManifest(manifest_path.to_string_lossy().to_string(), e.into())
        })?;
        if manifest.name != plugin_name || manifest.version != version {
            return Err(PluginError::InvalidManifest(
                manifest_path.to_string_lossy().to_string(),
                "plugin name or version should be equal to manifest one".into(),
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
        }
    }

    #[inline(always)]
    pub fn plugin_identifier(&self) -> PluginIdentifier {
        PluginIdentifier::new(self.name.clone(), self.version.clone())
    }

    /// Return plugin service definitions built from manifest.
    pub fn service_defs(&self) -> Vec<ServiceDef> {
        self.services
            .iter()
            .map(|srv| ServiceDef {
                plugin_name: self.name.to_string(),
                name: srv.name.to_string(),
                tiers: vec![],
                version: self.version.to_string(),
                description: srv.description.to_string(),
            })
            .collect()
    }

    /// Return default configuration for a service.
    pub fn get_default_config(&self, svc: &str) -> Option<&rmpv::Value> {
        let config = self.services.iter().find_map(|service| {
            if service.name == svc {
                return Some(&service.default_configuration);
            }
            None
        });
        config
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
        ident: &'a PluginIdentifier,
        service_defs: &'a [ServiceDef],
    },
    /// Error occurred while the plugin loaded.
    PluginLoadError { name: &'a str },
    /// Request for update service configuration received.
    BeforeServiceConfigurationUpdated {
        ident: &'a PluginIdentifier,
        service: &'a str,
        new_raw: &'a [u8],
    },
    /// Instance demote.
    InstanceDemote,
    /// Instance promote as a replicaset leader.
    InstancePromote,
    /// Plugin service enabled at instance.
    ServiceEnabled {
        ident: &'a PluginIdentifier,
        service: &'a str,
    },
    /// Plugin service disabled at instance.
    ServiceDisabled {
        ident: &'a PluginIdentifier,
        service: &'a str,
    },
}

/// Events that may be fired at picodata
/// and which plugins should respond to *asynchronously*.
///
/// Asynchronous events needed when fired side can't yield while event handled by plugins,
/// for example, if event fired in transaction.
pub enum PluginAsyncEvent {
    /// Plugin service configuration is updated.
    ServiceConfigurationUpdated {
        ident: PluginIdentifier,
        service: String,
        old_raw: Vec<u8>,
        new_raw: Vec<u8>,
    },
    /// Plugin removed at instance.
    PluginDisabled { name: String },
}

/// Unique plugin identifier in the system.
#[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq, Hash)]
pub struct PluginIdentifier {
    /// Plugin name.
    pub name: String,
    /// Plugin version.
    pub version: String,
}

impl PluginIdentifier {
    pub fn new(name: String, version: String) -> Self {
        Self { name, version }
    }
}

impl Display for PluginIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}:{}", self.name, self.version))
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

        let predicate = cas::Predicate {
            index: raft_storage.applied()?,
            term: raft_storage.term()?,
            ranges: ranges.clone(),
        };
        let req = crate::cas::Request::new(op.clone(), predicate, ADMIN_ID)?;
        let res = cas::compare_and_swap(&req, deadline.duration_since(Instant::now_fiber()));
        let (index, term) = unwrap_ok_or!(res,
            Err(e) => {
                if e.is_retriable() {
                    continue;
                } else {
                    return Err(e);
                }
            }
        );

        node.wait_index(index, deadline.duration_since(Instant::now_fiber()))?;

        if term != raft::Storage::term(&node.raft_storage, index)? {
            // Leader has changed and the entry got rolled back, retry.
            continue;
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

////////////////////////////////////////////////////////////////////////////////
// reenterable_plugin_cas_request
////////////////////////////////////////////////////////////////////////////////

/// Performs a reenterable plugin change request. Goes into a loop where on each
/// iteration first the preconditions for the operation are checked and the
/// [`Op`] is constructed. Then the CaS request is sent to the leader. The loop
/// continues until either the request is successfully accepted, the deadline is
/// exceeded or a non-retriable error is detected.
///
/// This algorithm must be equivalent to [`crate::sql::reenterable_schema_change_request`].
/// Also please don't refactor it to extract common code using generics or
/// function pointers, this will only make it harder to understand.
fn reenterable_plugin_cas_request(
    node: &Node,
    check_operation_preconditions_and_make_op_for_cas: impl Fn()
        -> Result<PreconditionCheckResult, Error>,
    ranges: Vec<Range>,
    deadline: Instant,
) -> traft::Result<RaftIndex> {
    loop {
        let index = node.read_index(deadline.duration_since(Instant::now_fiber()))?;

        let res = check_operation_preconditions_and_make_op_for_cas()?;
        use PreconditionCheckResult::*;
        let op = match res {
            DoOp(op) => op,
            WaitIndexAndRetry => {
                node.wait_index(index + 1, deadline.duration_since(Instant::now_fiber()))?;
                continue;
            }
            AlreadyApplied => return Ok(index),
        };

        let term = raft::Storage::term(&node.raft_storage, index)?;
        let predicate = cas::Predicate {
            index,
            term,
            ranges: ranges.clone(),
        };
        // FIXME: access rules will be implemented in future release
        let current_user = effective_user_id();
        let req = crate::cas::Request::new(op.clone(), predicate, current_user)?;
        let res = cas::compare_and_swap(&req, deadline.duration_since(Instant::now_fiber()));
        let (index, term) = unwrap_ok_or!(res,
            Err(e) => {
                if e.is_retriable() {
                    continue;
                } else {
                    return Err(e);
                }
            }
        );

        node.wait_index(index, deadline.duration_since(Instant::now_fiber()))?;

        if term != raft::Storage::term(&node.raft_storage, index)? {
            // Leader has changed and the entry got rolled back, retry.
            continue;
        }

        return Ok(index);
    }
}

enum PreconditionCheckResult {
    AlreadyApplied,
    WaitIndexAndRetry,
    DoOp(Op),
}

////////////////////////////////////////////////////////////////////////////////
// External plugin interface
////////////////////////////////////////////////////////////////////////////////

/// Install plugin:
/// 1) check that plugin is ready for run at all instances
/// 2) fill `_pico_service`, `_pico_plugin` and set `_pico_plugin.ready` to `false`
pub fn install_plugin(
    ident: PluginIdentifier,
    timeout: Duration,
    if_not_exists: bool,
) -> traft::Result<()> {
    let deadline = fiber::clock().saturating_add(timeout);
    let node = node::global()?;

    let manifest = Manifest::load(&ident)?;

    let check_and_make_op = || {
        if node.storage.properties.pending_plugin_op()?.is_some() {
            return Ok(PreconditionCheckResult::WaitIndexAndRetry);
        }

        let ident = manifest.plugin_identifier();
        let plugin_already_exists = node.storage.plugin.contains(&ident)?;
        if plugin_already_exists {
            if if_not_exists {
                return Ok(PreconditionCheckResult::AlreadyApplied);
            } else {
                return Err(PluginError::AlreadyExist(ident).into());
            }
        }

        let op = PluginOp::InstallPlugin {
            manifest: manifest.clone(),
        };
        let dml = Dml::replace(
            ClusterwideTable::Property,
            &(&PropertyName::PendingPluginOperation, &op),
            effective_user_id(),
        )?;
        Ok(PreconditionCheckResult::DoOp(Op::Dml(dml)))
    };

    let ranges = vec![
        // Fail if someone proposes another plugin operation
        Range::new(ClusterwideTable::Property).eq([PropertyName::PendingPluginOperation]),
    ];
    let mut index = reenterable_plugin_cas_request(node, check_and_make_op, ranges, deadline)?;

    while node.storage.properties.pending_plugin_op()?.is_some() {
        index = node.wait_index(index + 1, deadline.duration_since(Instant::now_fiber()))?;
    }

    if node.storage.plugin.get(&ident)?.is_none() {
        return Err(PluginError::InstallationAborted.into());
    }

    Ok(())
}

pub fn migration_up(
    ident: &PluginIdentifier,
    timeout: Duration,
    rollback_timeout: Duration,
) -> traft::Result<()> {
    let deadline = fiber::clock().saturating_add(timeout);
    let node = node::global()?;

    // plugin must be already installed
    let installed = node.storage.plugin.contains(ident)?;
    if !installed {
        return Err(PluginError::PluginNotFound(ident.clone()).into());
    }
    // get manifest for loading of migration files
    let manifest = Manifest::load(ident)?;

    let already_applied_migrations = node
        .storage
        .plugin_migration
        .get_files_by_plugin(&ident.name)?;
    if already_applied_migrations.len() > manifest.migration.len() {
        return Err(
            PluginError::Migration(migration::Error::InconsistentMigrationList(
                "more migrations have already been applied than are in the manifest".to_string(),
            ))
            .into(),
        );
    }

    if manifest.migration.is_empty() {
        tlog!(Info, "plugin has no migrations");
        return Ok(());
    }

    let mut migration_delta = manifest.migration;
    for (i, migration_file) in migration_delta
        .drain(..already_applied_migrations.len())
        .enumerate()
    {
        if migration_file != already_applied_migrations[i].migration_file {
            return Err(
                PluginError::Migration(migration::Error::InconsistentMigrationList(format!(
                    "unknown migration files found in manifest migrations ({migration_file})"
                )))
                .into(),
            );
        }

        let migration = MigrationInfo::new_unparsed(ident, migration_file);
        let hash = migration::calculate_migration_hash_async(&migration)
            .map_err(PluginError::Migration)?;
        let hash_string = format!("{:x}", hash);

        if hash_string != already_applied_migrations[i].hash() {
            let shortname = migration.shortname();
            return Err(
                PluginError::Migration(migration::Error::InconsistentMigrationList(
                    format!("unknown migration files found in manifest migrations (mismatched hash checksum for {shortname})")
                ))
                .into(),
            );
        }
    }

    if migration_delta.is_empty() {
        tlog!(Info, "`UP` migrations are up to date");
        return Ok(());
    }

    migration::apply_up_migrations(ident, &migration_delta, deadline, rollback_timeout)?;
    Ok(())
}

pub fn migration_down(ident: PluginIdentifier, timeout: Duration) -> traft::Result<()> {
    let deadline = fiber::clock().saturating_add(timeout);
    let node = node::global()?;

    // plugin must be already installed
    let installed = node.storage.plugin.contains(&ident)?;
    if !installed {
        return Err(PluginError::PluginNotFound(ident).into());
    }

    let migration_list = node
        .storage
        .plugin_migration
        .get_files_by_plugin(&ident.name)?
        .into_iter()
        .map(|rec| rec.migration_file)
        .collect::<Vec<_>>();
    if migration_list.is_empty() {
        tlog!(Info, "`DOWN` migrations are up to date");
    }

    migration::apply_down_migrations(&ident, &migration_list, deadline);
    Ok(())
}

/// Enable plugin:
/// 1) call `on_start` at all instances (and `on_stop` if something happened wrong)
/// 2) set `_pico_plugin.enable` to `true`
/// 3) update routes in `_pico_service_route`
pub fn enable_plugin(
    plugin: &PluginIdentifier,
    on_start_timeout: Duration,
    timeout: Duration,
) -> traft::Result<()> {
    let deadline = Instant::now_fiber().saturating_add(timeout);
    let node = node::global()?;

    let check_and_make_op = || {
        if node.storage.properties.pending_plugin_op()?.is_some() {
            return Ok(PreconditionCheckResult::WaitIndexAndRetry);
        }

        // TODO: check if plugin already enabled

        let services = node.storage.service.get_by_plugin(plugin)?;

        let op = PluginOp::EnablePlugin {
            plugin: plugin.clone(),
            // FIXME: we shouldn't need to send this list, it's already available on
            // the governor, what is going on?
            services,
            timeout: on_start_timeout,
        };
        let dml = Dml::replace(
            ClusterwideTable::Property,
            &(&PropertyName::PendingPluginOperation, &op),
            effective_user_id(),
        )?;
        Ok(PreconditionCheckResult::DoOp(Op::Dml(dml)))
    };

    let ranges = vec![
        // Fail if someone proposes another plugin operation
        Range::new(ClusterwideTable::Property).eq([PropertyName::PendingPluginOperation]),
        // Fail if someone updates this plugin record
        Range::new(ClusterwideTable::Plugin).eq([&plugin.name]),
    ];
    let mut index = reenterable_plugin_cas_request(node, check_and_make_op, ranges, deadline)?;

    while node.storage.properties.pending_plugin_op()?.is_some() {
        index = node.wait_index(index + 1, deadline.duration_since(Instant::now_fiber()))?;
    }

    let plugin = node
        .storage
        .plugin
        .get(plugin)?
        .ok_or(PluginError::EnablingAborted)?;

    if !plugin.enabled {
        return Err(PluginError::EnablingAborted.into());
    }

    Ok(())
}

/// Update plugin service configuration.
pub fn update_plugin_service_configuration(
    ident: &PluginIdentifier,
    service_name: &str,
    new_cfg_raw: &[u8],
    timeout: Duration,
) -> traft::Result<()> {
    let deadline = Instant::now_fiber().saturating_add(timeout);
    let node = node::global()?;

    node.plugin_manager
        .handle_event_sync(PluginEvent::BeforeServiceConfigurationUpdated {
            ident,
            service: service_name,
            new_raw: new_cfg_raw,
        })?;

    let new_cfg: rmpv::Value = rmp_serde::from_slice(new_cfg_raw).expect("out of memory");

    let check_and_make_op = || {
        let op = PluginRaftOp::UpdatePluginConfig {
            ident: ident.clone(),
            service_name: service_name.to_string(),
            config: new_cfg.clone(),
        };
        Ok(PreconditionCheckResult::DoOp(Op::Plugin(op)))
    };

    let ranges = vec![
        // Fail if someone updates this service record
        Range::new(ClusterwideTable::Service).eq((&ident.name, service_name, &ident.version)),
    ];
    reenterable_plugin_cas_request(node, check_and_make_op, ranges, deadline)?;
    Ok(())
}

/// Disable plugin:
/// 1) call `on_stop` for each service in plugin
/// 2) update routes in `_pico_service_route`
/// 3) set `_pico_plugin.enable` to `false`
pub fn disable_plugin(ident: &PluginIdentifier, timeout: Duration) -> traft::Result<()> {
    let deadline = Instant::now_fiber().saturating_add(timeout);
    let node = node::global()?;

    let check_and_make_op = || {
        if node.storage.properties.pending_plugin_op()?.is_some() {
            return Ok(PreconditionCheckResult::WaitIndexAndRetry);
        }

        // TODO: support if_exists option
        if !node.storage.plugin.contains(ident)? {
            return Err(PluginNotFound(ident.clone()).into());
        }

        let op = PluginRaftOp::DisablePlugin {
            ident: ident.clone(),
        };
        Ok(PreconditionCheckResult::DoOp(Op::Plugin(op)))
    };

    let ranges = vec![
        // Fail if someone updates this plugin record
        Range::new(ClusterwideTable::Plugin).eq([&ident.name]),
    ];
    let mut index = reenterable_plugin_cas_request(node, check_and_make_op, ranges, deadline)?;

    while node.storage.properties.pending_plugin_op()?.is_some() {
        index = node.wait_index(index + 1, deadline.duration_since(Instant::now_fiber()))?;
    }

    Ok(())
}

/// Remove plugin: clear records from `_pico_plugin` and `_pico_service` system tables.
///
/// # Arguments
///
/// * `ident`: identity of plugin to remove
/// * `timeout`: operation timeout
pub fn remove_plugin(ident: &PluginIdentifier, timeout: Duration) -> traft::Result<()> {
    let deadline = Instant::now_fiber().saturating_add(timeout);
    let node = node::global()?;

    let check_and_make_op = || {
        if node.storage.properties.pending_plugin_op()?.is_some() {
            return Ok(PreconditionCheckResult::WaitIndexAndRetry);
        }

        let Some(plugin) = node.storage.plugin.get(ident)? else {
            // TODO: support if_exists option
            #[rustfmt::skip]
            return Err(traft::error::Error::other(format!("no such plugin `{ident}`")));
        };

        if plugin.enabled {
            return Err(RemoveOfEnabledPlugin.into());
        }

        let migration_list = node
            .storage
            .plugin_migration
            .get_files_by_plugin(&ident.name)?
            .into_iter()
            .map(|rec| rec.migration_file)
            .collect::<Vec<_>>();

        if !migration_list.is_empty() {
            #[rustfmt::skip]
            return Err(traft::error::Error::other("attempt to remove plugin with applied `UP` migrations"));
        }

        let op = PluginRaftOp::RemovePlugin {
            ident: ident.clone(),
        };
        Ok(PreconditionCheckResult::DoOp(Op::Plugin(op)))
    };

    let ranges = vec![
        // Fail if someone updates this plugin record
        Range::new(ClusterwideTable::Plugin).eq([&ident.name]),
        // Fail if someone updates any service record of this plugin
        Range::new(ClusterwideTable::Service).eq([&ident.name]),
    ];
    reenterable_plugin_cas_request(node, check_and_make_op, ranges, deadline)?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// PluginOp
////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum PluginOp {
    InstallPlugin {
        manifest: Manifest,
    },
    EnablePlugin {
        plugin: PluginIdentifier,
        services: Vec<ServiceDef>,
        timeout: Duration,
    },
    /// Operation to change on which tiers the given services should be deployed.
    UpdateTopology(TopologyUpdateOp),
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum TopologyUpdateOp {
    /// Append service to a new tier.
    Append {
        plugin_identity: PluginIdentifier,
        service_name: String,
        tier: String,
    },
    /// Remove service from a tier.
    Remove {
        plugin_identity: PluginIdentifier,
        service_name: String,
        tier: String,
    },
}

impl TopologyUpdateOp {
    pub fn plugin_identity(&self) -> &PluginIdentifier {
        match self {
            TopologyUpdateOp::Append {
                plugin_identity, ..
            } => plugin_identity,
            TopologyUpdateOp::Remove {
                plugin_identity, ..
            } => plugin_identity,
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
    let deadline = Instant::now_fiber().saturating_add(timeout);
    let node = node::global()?;

    let check_and_make_op = || {
        if node.storage.properties.pending_plugin_op()?.is_some() {
            return Ok(PreconditionCheckResult::WaitIndexAndRetry);
        }

        let service = node
            .storage
            .service
            .get(upd_op.plugin_identity(), upd_op.service_name())?;
        if service.is_none() {
            return Err(PluginError::ServiceNotFound(
                upd_op.service_name().to_string(),
                upd_op.plugin_identity().clone(),
            )
            .into());
        }

        let op = PluginOp::UpdateTopology(upd_op.clone());
        let dml = Dml::replace(
            ClusterwideTable::Property,
            &(&PropertyName::PendingPluginOperation, &op),
            effective_user_id(),
        )?;
        Ok(PreconditionCheckResult::DoOp(Op::Dml(dml)))
    };

    let ident = upd_op.plugin_identity();
    let ranges = vec![
        // Fail if someone updates this plugin record
        Range::new(ClusterwideTable::Plugin).eq([&ident.name, &ident.version]),
        // Fail if someone updates this service record
        Range::new(ClusterwideTable::Service).eq([
            &ident.name,
            upd_op.service_name(),
            &ident.version,
        ]),
        // Fail if someone proposes another plugin operation
        Range::new(ClusterwideTable::Property).eq([PropertyName::PendingPluginOperation]),
    ];
    let mut index = reenterable_plugin_cas_request(node, check_and_make_op, ranges, deadline)?;

    while node.storage.properties.pending_plugin_op()?.is_some() {
        index = node.wait_index(index + 1, deadline.duration_since(Instant::now_fiber()))?;
    }

    let service = node
        .storage
        .service
        .get(ident, upd_op.service_name())?
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
    identity: &PluginIdentifier,
    service_name: &str,
    tier: &str,
    timeout: Duration,
) -> traft::Result<()> {
    update_tier(
        TopologyUpdateOp::Append {
            plugin_identity: identity.clone(),
            service_name: service_name.to_string(),
            tier: tier.to_string(),
        },
        timeout,
    )
}

/// Disable service on a new tier.
pub fn remove_tier(
    identity: &PluginIdentifier,
    service_name: &str,
    tier: &str,
    timeout: Duration,
) -> traft::Result<()> {
    update_tier(
        TopologyUpdateOp::Remove {
            plugin_identity: identity.clone(),
            service_name: service_name.to_string(),
            tier: tier.to_string(),
        },
        timeout,
    )
}
