pub mod background;
mod ffi;
pub mod lock;
pub mod manager;
pub mod metrics;
pub mod migration;
pub mod rpc;
pub mod topology;

use crate::cas::Range;
use crate::config::PicodataConfig;
use crate::info::PICODATA_VERSION;
use crate::plugin::lock::PicoPropertyLock;
use crate::plugin::migration::MigrationInfo;
use crate::plugin::PluginError::PluginNotFound;
use crate::schema::{PluginDef, ServiceDef, ServiceRouteItem, ADMIN_ID};
use crate::storage::{self, PropertyName, SystemTable};
use crate::traft::error::Error;
use crate::traft::error::ErrorInfo;
use crate::traft::node::Node;
use crate::traft::op::PluginRaftOp;
use crate::traft::op::{Dml, Op};
use crate::traft::{node, RaftIndex};
use crate::util::effective_user_id;
use crate::{cas, tlog, traft};
use picodata_plugin::error_code::ErrorCode;
#[allow(unused_imports)]
use picodata_plugin::plugin::interface;
pub use picodata_plugin::plugin::interface::ServiceId;
use picodata_plugin::plugin::interface::{ServiceBox, ValidatorBox};
use rmpv::Value;
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io;
use std::rc::Rc;
use std::time::Duration;
use tarantool::error::{BoxError, IntoBoxError};
use tarantool::fiber;
use tarantool::session;
use tarantool::time::Instant;

pub const LOCK_RELEASE_MINIMUM_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(thiserror::Error, Debug)]
pub enum PluginError {
    #[error("Plugin `{0}` already exists")]
    AlreadyExist(PluginIdentifier),
    #[error("Failed to install plugin `{0}`: {}", DisplaySomeOrDefault(.1, "unknown reason"))]
    InstallationAborted(PluginIdentifier, Option<ErrorInfo>),
    #[error("Failed to enable plugin `{0}`: {}", DisplaySomeOrDefault(.1, "unknown reason"))]
    EnablingAborted(PluginIdentifier, Option<ErrorInfo>),
    #[error("Failed to update topology for plugin `{0}`: {}", DisplaySomeOrDefault(.1, "unknown reason"))]
    TopologyUpdateAborted(PluginIdentifier, Option<ErrorInfo>),
    #[error("Error while discovering manifest for plugin `{0}`: {1}")]
    ManifestNotFound(String, io::Error),
    #[error("Error while parsing manifest `{0}`, reason: {1}")]
    InvalidManifest(String, Box<dyn std::error::Error>),
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
    #[error("Topology: {0}")]
    TopologyError(String),
    #[error("Found more than one service factory for `{0}` ver. `{1}`")]
    ServiceCollision(String, String),
    #[error(
        "Cannot specify install candidate (there should be only one directory in plugin main dir)"
    )]
    AmbiguousInstallCandidate,
    #[error("Cannot specify enable candidate (there should be only one installed plugin version)")]
    AmbiguousEnableCandidate,
    #[error("Some of configuration keys are unknown ({0})")]
    InvalidConfigurationKey(String),
    #[error("Trying to update an empty configuration")]
    UpdateEmptyConfig,
    #[error("Unexpected invalid configuration")]
    InvalidConfiguration,
    #[error("Invalid configuration value (should be a json string): {0}")]
    ConfigDecode(serde_json::Error),
    #[error(
        "Picoplugin version {0} used to build a plugin is incompatible with picodata version {}",
        PICODATA_VERSION
    )]
    IncompatiblePicopluginVersion(String),
}

struct DisplaySomeOrDefault<'a>(&'a Option<ErrorInfo>, &'a str);
impl std::fmt::Display for DisplaySomeOrDefault<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(cause) = self.0 {
            cause.fmt(f)
        } else {
            f.write_str(self.1)
        }
    }
}

impl IntoBoxError for PluginError {
    #[inline(always)]
    fn error_code(&self) -> u32 {
        match self {
            Self::ServiceNotFound { .. } => ErrorCode::NoSuchService as _,
            _ => ErrorCode::Other as _,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PluginCallbackError {
    #[error("on_start: {0}")]
    OnStart(BoxError),
    #[error("New configuration validation error: {0}")]
    InvalidConfiguration(BoxError),
}

type Result<T, E = PluginError> = std::result::Result<T, E>;

/// State of the loaded service.
pub struct ServiceState {
    pub id: ServiceId,

    background_job_shutdown_timeout: Cell<Option<Duration>>,

    /// The dynamic state of the service. It is guarded by a fiber mutex,
    /// because we don't want to enter the plugin callbacks from concurrent fibers.
    volatile_state: fiber::Mutex<ServiceStateVolatile>,
}

impl ServiceState {
    #[inline]
    fn new(
        id: ServiceId,
        inner: ServiceBox,
        config_validator: ValidatorBox,
        lib: Rc<LibraryWrapper>,
    ) -> Self {
        Self {
            id,
            background_job_shutdown_timeout: Cell::new(None),
            volatile_state: fiber::Mutex::new(ServiceStateVolatile {
                inner,
                config_validator,
                _lib: lib,
            }),
        }
    }
}

/// This struct stores volatile state of the service. For example function
/// pointers into the dynamically loaded library. It must be accessed only via
/// a fiber mutex, so that we don't enter into the plugin callbacks from
/// concurrent fibers.
pub struct ServiceStateVolatile {
    /// The implementation of [`interface::Service`] trait which was loaded from
    /// the dynamic library [`Self::_lib`].
    inner: ServiceBox,

    /// The implementation of [`interface::Validator`] trait which was loaded from
    /// the dynamic library [`Self::_lib`].
    config_validator: ValidatorBox,

    /// A handle to the dynamic library from which the service callbacks are loaded.
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

        let share_dir = PicodataConfig::get().instance.share_dir();
        let plugin_dir = share_dir.join(plugin_name);
        let plugin_dir = plugin_dir.join(version);
        let manifest_path = plugin_dir.join("manifest.yaml");
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
            .map(|svc| ServiceDef {
                plugin_name: self.name.to_string(),
                name: svc.name.to_string(),
                tiers: vec![],
                version: self.version.to_string(),
                description: svc.description.to_string(),
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

pub fn build_service_routes_replace_dml(items: &[ServiceRouteItem]) -> Op {
    let ops = items
        .iter()
        .map(|routing_item| {
            Dml::replace(
                storage::ServiceRouteTable::TABLE_ID,
                &routing_item,
                effective_user_id(),
            )
            .expect("encoding should not fail")
        })
        .collect();
    // assert that instances update only self-owned information
    debug_assert!({
        let node = node::global().expect("node must be already initialized");
        let instance_name = node.topology_cache.my_instance_name();
        items.iter().all(|key| key.instance_name == instance_name)
    });
    Op::BatchDml { ops }
}

/// Remove routes from the plugin routing table.
pub fn remove_routes(timeout: Duration) -> traft::Result<()> {
    let deadline = fiber::clock().saturating_add(timeout);
    let node = node::global()?;
    let check_and_make_op = || {
        let instance_name = node.topology_cache.my_instance_name();

        // clear all routes handled by this instances
        let instance_routes = node
            .storage
            .service_route_table
            .get_by_instance(&instance_name.into())
            .expect("storage should not fail");
        let routing_keys: Vec<_> = instance_routes.iter().map(|r| r.key()).collect();
        if routing_keys.is_empty() {
            return Ok(PreconditionCheckResult::AlreadyApplied);
        }
        let ops = routing_keys
            .iter()
            .map(|routing_key| {
                Dml::delete(
                    storage::ServiceRouteTable::TABLE_ID,
                    &routing_key,
                    effective_user_id(),
                )
                .expect("encoding should not fail")
            })
            .collect();
        let batch_dml = Op::BatchDml { ops };

        Ok(PreconditionCheckResult::DoOp((batch_dml, vec![])))
    };

    let _su = session::su(ADMIN_ID).expect("cant fail because admin should always have session");
    reenterable_plugin_cas_request(node, check_and_make_op, deadline)?;

    Ok(())
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
    deadline: Instant,
) -> traft::Result<RaftIndex> {
    loop {
        let index = node.read_index(deadline.duration_since(Instant::now_fiber()))?;

        let res = check_operation_preconditions_and_make_op_for_cas()?;
        use PreconditionCheckResult::*;
        let (op, ranges) = match res {
            DoOp(v) => v,
            WaitIndexAndRetry => {
                node.wait_index(index + 1, deadline.duration_since(Instant::now_fiber()))?;
                continue;
            }
            AlreadyApplied => return Ok(index),
        };

        let predicate = cas::Predicate::new(index, ranges);
        // FIXME: access rules will be implemented in future release
        let current_user = effective_user_id();
        let req = crate::cas::Request::new(op.clone(), predicate, current_user)?;
        let res = cas::compare_and_swap_and_wait(&req, deadline)?;
        if res.is_retriable_error() {
            continue;
        }

        return Ok(index);
    }
}

enum PreconditionCheckResult {
    AlreadyApplied,
    WaitIndexAndRetry,
    DoOp((Op, Vec<cas::Range>)),
}

////////////////////////////////////////////////////////////////////////////////
// External plugin interface
////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct InheritOpts {
    pub config: bool,
    pub topology: bool,
}

/// This function implements config inheritance logic during config upgrades.
///
/// The inheritance strategy is as follows:
/// - For keys that present in both configs, use the value from the current one
/// - Keep new keys from the new config
/// - Add keys that exist only in the current config to the new one
fn inherit_config_inplace(new: &mut Value, cur: Value) {
    match (new, cur) {
        (Value::Map(new_kvs), Value::Map(mut cur_kvs)) => {
            // Inherit current config values for common keys
            for (k, v) in new_kvs.iter_mut() {
                let cur_kv = cur_kvs
                    .iter_mut()
                    .find(|(k2, _)| k == k2)
                    .map(|kv| std::mem::replace(kv, (Value::Nil, Value::Nil)));
                if let Some((_, cur_v)) = cur_kv {
                    inherit_config_inplace(v, cur_v);
                }
            }

            // Inherit keys that are missed in the new confing
            let cur_kvs = cur_kvs.into_iter().filter(|kv| !kv.0.is_nil());
            new_kvs.extend(cur_kvs);
        }
        (new, cur) => *new = cur,
    }
}

/// Create plugin:
/// 1) check that plugin is ready for run at all instances
/// 2) fill `_pico_service`, `_pico_plugin` and set `_pico_plugin.ready` to `false`
pub fn create_plugin(
    ident: PluginIdentifier,
    timeout: Duration,
    if_not_exists: bool,
    inherit_opts: InheritOpts,
) -> traft::Result<()> {
    let deadline = fiber::clock().saturating_add(timeout);
    let node = node::global()?;

    let manifest = Manifest::load(&ident)?;

    let check_and_make_op = || {
        if node.storage.properties.pending_plugin_op()?.is_some() {
            return Ok(PreconditionCheckResult::WaitIndexAndRetry);
        }

        let ident = manifest.plugin_identifier();
        let plugin_already_exists = node.storage.plugins.contains(&ident)?;
        if plugin_already_exists {
            return if if_not_exists {
                Ok(PreconditionCheckResult::AlreadyApplied)
            } else {
                Err(PluginError::AlreadyExist(ident).into())
            };
        }

        // find plugins with the same name
        let mut existing_plugins = node.storage.plugins.get_all_versions(&ident.name)?;
        debug_assert!(
            existing_plugins.len() <= 2,
            "Only two or less plugins with same name may be in the system in the same time"
        );

        if existing_plugins.len() >= 2 {
            let plugin_name = ident.name;
            return Err(BoxError::new(ErrorCode::PluginError,
                format!("too many versions of plugin '{plugin_name}', only 2 versions of the same plugin may exist at the same time")).into());
        }

        let mut inherit_topology = HashMap::new();
        let mut manifest = manifest.clone();
        let mut inherit_entities = HashMap::new();
        if (inherit_opts.topology || inherit_opts.config) && !existing_plugins.is_empty() {
            let existed_plugin = existing_plugins.pop().expect("infallible");
            let existed_identity = existed_plugin.into_identifier();

            if inherit_opts.config {
                let mut entities = node.storage.plugin_config.all_entities(&existed_identity)?;

                manifest.services.iter_mut().for_each(|svc_manifest| {
                    if let Some(cfg) = entities.remove(&svc_manifest.name) {
                        inherit_config_inplace(&mut svc_manifest.default_configuration, cfg);
                    }
                });
                inherit_entities.extend(entities);
            }
            if inherit_opts.topology {
                node.storage
                    .services
                    .get_by_plugin(&existed_identity)?
                    .into_iter()
                    .for_each(|svc| {
                        inherit_topology.insert(svc.name, svc.tiers);
                    });
            }
        }

        let op = PluginOp::CreatePlugin {
            manifest,
            inherit_entities,
            inherit_topology,
        };
        let dml = Dml::replace(
            storage::Properties::TABLE_ID,
            &(&PropertyName::PendingPluginOperation, &op),
            effective_user_id(),
        )?;
        let ranges = vec![
            // Fail if someone proposes another plugin operation
            Range::new(storage::Properties::TABLE_ID).eq([PropertyName::PendingPluginOperation]),
            // Fail if someone updates this plugin record
            Range::new(storage::Plugins::TABLE_ID).eq([&ident.name]),
            Range::new(storage::PluginConfig::TABLE_ID).eq([&ident.name]),
            Range::new(storage::Services::TABLE_ID).eq([&ident.name]),
        ];
        Ok(PreconditionCheckResult::DoOp((Op::Dml(dml), ranges)))
    };

    let index_of_prepare = reenterable_plugin_cas_request(node, check_and_make_op, deadline)?;

    let mut index = index_of_prepare;
    while node.storage.properties.pending_plugin_op()?.is_some() {
        index = node.wait_index(index + 1, deadline.duration_since(Instant::now_fiber()))?;
    }

    if node.storage.plugins.get(&ident)?.is_none() {
        let cause = lookup_abort_cause(node, index_of_prepare)?;
        return Err(PluginError::InstallationAborted(ident.clone(), cause).into());
    }

    Ok(())
}

fn lookup_abort_cause(
    node: &Node,
    index_of_prepare: RaftIndex,
) -> traft::Result<Option<ErrorInfo>> {
    let log_tail = node
        .raft_storage
        .entries(index_of_prepare + 1, u64::MAX, None)?;
    for entry in log_tail {
        let Some(Op::Plugin(PluginRaftOp::Abort { cause })) = entry.into_op() else {
            continue;
        };

        return Ok(Some(cause));
    }

    Ok(None)
}

pub fn migration_up(
    ident: &PluginIdentifier,
    timeout: Duration,
    rollback_timeout: Duration,
) -> traft::Result<()> {
    let deadline = fiber::clock().saturating_add(timeout);
    let node = node::global()?;

    // plugin must be already installed
    let installed = node.storage.plugins.contains(ident)?;
    if !installed {
        return Err(PluginError::PluginNotFound(ident.clone()).into());
    }
    // get manifest for loading of migration files
    let manifest = Manifest::load(ident)?;

    let already_applied_migrations = node.storage.plugin_migrations.get_by_plugin(&ident.name)?;
    let applied_count = already_applied_migrations.len();
    let manifest_count = manifest.migration.len();
    if applied_count > manifest_count {
        return Err(missing_migration_files(applied_count, manifest_count).into());
    }

    if manifest.migration.is_empty() {
        tlog!(Info, "plugin has no migrations");
        return Ok(());
    }

    let mut migration_delta = manifest.migration;
    for (i, migration_file) in migration_delta.drain(..applied_count).enumerate() {
        if migration_file != already_applied_migrations[i].migration_file {
            return Err(unknown_migration_file(migration_file).into());
        }

        let migration = MigrationInfo::new_unparsed(ident, migration_file);
        let hash = migration::calculate_migration_hash_async(&migration)?;
        let hash_string = format!("{hash:x}");

        if hash_string != already_applied_migrations[i].hash() {
            let details = format!("mismatched hash checksum for {}", migration.shortname());
            return Err(unknown_migration_file(details).into());
        }
    }

    if migration_delta.is_empty() {
        tlog!(Info, "`UP` migrations are up to date");
        return Ok(());
    }

    lock::try_acquire(deadline)?;
    let error = migration::apply_up_migrations(ident, &migration_delta, deadline, rollback_timeout);
    let release_timeout = rollback_timeout.max(LOCK_RELEASE_MINIMUM_TIMEOUT);
    lock::release(fiber::clock().saturating_add(release_timeout))?;

    error
}

#[track_caller]
fn missing_migration_files(applied_count: usize, manifest_count: usize) -> BoxError {
    BoxError::new(
        ErrorCode::PluginError,
        format!("more migrations have already been applied ({applied_count}) than are in the manifest ({manifest_count})"),
    )
}

#[track_caller]
fn unknown_migration_file(details: String) -> BoxError {
    BoxError::new(
        ErrorCode::PluginError,
        format!("unknown migration files found in manifest migrations ({details})"),
    )
}

pub fn migration_down(ident: PluginIdentifier, timeout: Duration) -> traft::Result<()> {
    let deadline = fiber::clock().saturating_add(timeout);
    let node = node::global()?;

    // plugin must be already installed
    let installed = node.storage.plugins.contains(&ident)?;
    if !installed {
        return Err(PluginError::PluginNotFound(ident).into());
    }

    let migration_list = node
        .storage
        .plugin_migrations
        .get_by_plugin(&ident.name)?
        .into_iter()
        .map(|rec| rec.migration_file)
        .collect::<Vec<_>>();
    if migration_list.is_empty() {
        tlog!(Info, "`DOWN` migrations are up to date");
    }

    lock::try_acquire(deadline)?;
    migration::apply_down_migrations(&ident, &migration_list, deadline, &node.storage);
    let release_timeout = timeout.max(LOCK_RELEASE_MINIMUM_TIMEOUT);
    lock::release(fiber::clock().saturating_add(release_timeout))?;

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

        // Check if plugin with a different version is already enabled
        let mut plugin_def = None;
        let installed_plugins = node.storage.plugins.get_all_versions(&plugin.name)?;
        for installed_plugin_def in installed_plugins {
            if installed_plugin_def.version == plugin.version {
                plugin_def = Some(installed_plugin_def);
                break;
            }

            if installed_plugin_def.enabled {
                let version = installed_plugin_def.version;
                #[rustfmt::skip]
                return Err(Error::other(format!("plugin `{plugin}` is already enabled with a different version {version}")));
            }
        }

        // Check if plugin is installed
        let Some(plugin_def) = plugin_def else {
            return Err(PluginError::PluginNotFound(plugin.clone()).into());
        };

        // Check if plugin is already enabled
        if plugin_def.enabled {
            // TODO: support if_not_enabled option
            #[rustfmt::skip]
            return Err(Error::other(format!("plugin `{plugin}` is already enabled")));
        }

        // Check migration consistency
        let applied_migrations = node.storage.plugin_migrations.get_by_plugin(&plugin.name)?;
        let applied_files: Vec<_> = applied_migrations
            .into_iter()
            .map(|m| m.migration_file)
            .collect();
        let expected_files = &plugin_def.migration_list;

        let applied = applied_files.len();
        let total = plugin_def.migration_list.len();
        let min_count = usize::min(applied, total);
        if applied_files[..min_count] != expected_files[..min_count] {
            let message = format!("detected incosistency in migrations for plugin `{plugin}`: _pico_plugin: {expected_files:?} vs _pico_plugin_migration: {applied_files:?}");
            // Note this should never happen during normal operation hence warn_or_panic
            crate::warn_or_panic!("{message}");
            #[rustfmt::skip]
            return Err(Error::other(message));
        }

        // Check if migrations are up to date
        if applied < total {
            #[rustfmt::skip]
            return Err(Error::other(format!("cannot enable plugin `{plugin}`: need to apply migrations first (applied {applied}/{total})")));
        }

        let op = PluginOp::EnablePlugin {
            plugin: plugin.clone(),
            timeout: on_start_timeout,
        };
        let dml = Dml::replace(
            storage::Properties::TABLE_ID,
            &(&PropertyName::PendingPluginOperation, &op),
            effective_user_id(),
        )?;
        let ranges = vec![
            // Fail if someone proposes another plugin operation
            Range::new(storage::Properties::TABLE_ID).eq([PropertyName::PendingPluginOperation]),
            // Fail if someone updates this plugin record
            Range::new(storage::Plugins::TABLE_ID).eq([&plugin.name]),
            // Fail if someone updates this plugin's service records
            Range::new(storage::Services::TABLE_ID).eq([&plugin.name]),
            // Fail if someone updates this plugin's migration records
            Range::new(storage::PluginMigrations::TABLE_ID).eq([&plugin.name]),
            // Fail if someone updates this plugin's service route table
            Range::new(storage::ServiceRouteTable::TABLE_ID).eq([&plugin.name]),
        ];
        Ok(PreconditionCheckResult::DoOp((Op::Dml(dml), ranges)))
    };

    let index_of_prepare = reenterable_plugin_cas_request(node, check_and_make_op, deadline)?;

    let mut index = index_of_prepare;
    while node.storage.properties.pending_plugin_op()?.is_some() {
        index = node.wait_index(index + 1, deadline.duration_since(Instant::now_fiber()))?;
    }

    let Some(plugin_def) = node.storage.plugins.get(plugin)? else {
        #[rustfmt::skip]
        return Err(Error::other(format!("plugin `{plugin}` has been dropped by a concurrent request")));
    };

    if !plugin_def.enabled {
        let log_tail = node
            .raft_storage
            .entries(index_of_prepare + 1, u64::MAX, None)?;
        for entry in log_tail {
            let Some(Op::Plugin(PluginRaftOp::DisablePlugin { ident, cause })) = entry.into_op()
            else {
                continue;
            };

            if &ident != plugin {
                continue;
            }

            debug_assert!(cause.is_some());
            return Err(PluginError::EnablingAborted(plugin.clone(), cause).into());
        }
        return Err(PluginError::EnablingAborted(plugin.clone(), None).into());
    }

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
        if !node.storage.plugins.contains(ident)? {
            return Err(PluginNotFound(ident.clone()).into());
        }

        let op = PluginRaftOp::DisablePlugin {
            ident: ident.clone(),
            cause: None,
        };
        let ranges = vec![
            // Fail if someone updates this plugin record
            Range::new(storage::Plugins::TABLE_ID).eq([&ident.name]),
        ];
        Ok(PreconditionCheckResult::DoOp((Op::Plugin(op), ranges)))
    };

    let mut index = reenterable_plugin_cas_request(node, check_and_make_op, deadline)?;

    while node.storage.properties.pending_plugin_op()?.is_some() {
        index = node.wait_index(index + 1, deadline.duration_since(Instant::now_fiber()))?;
    }

    Ok(())
}

/// Drop plugin: clear records from `_pico_plugin` and `_pico_service` system tables.
///
/// # Arguments
///
/// * `ident`: identity of plugin to remove
/// * `drop_data`: whether true if plugin should be removed with DOWN migration, false elsewhere
/// * `if_exists`: if true then no errors acquired if plugin not exists
/// * `timeout`: operation timeout
pub fn drop_plugin(
    ident: &PluginIdentifier,
    drop_data: bool,
    if_exists: bool,
    timeout: Duration,
) -> traft::Result<()> {
    let deadline = Instant::now_fiber().saturating_add(timeout);
    let node = node::global()?;

    let check_and_make_op = || {
        if node.storage.properties.pending_plugin_op()?.is_some() {
            return Ok(PreconditionCheckResult::WaitIndexAndRetry);
        }

        let Some(plugin) = node.storage.plugins.get(ident)? else {
            if if_exists {
                return Ok(PreconditionCheckResult::AlreadyApplied);
            }

            #[rustfmt::skip]
            return Err(BoxError::new(ErrorCode::PluginError, format!("no such plugin `{ident}`")).into());
        };
        let plugin_name = &plugin.name;

        if plugin.enabled {
            #[rustfmt::skip]
            return Err(BoxError::new(ErrorCode::PluginError, format!("attempt to drop an enabled plugin '{plugin_name}'")).into());
        }

        let migration_list = node
            .storage
            .plugin_migrations
            .get_by_plugin(&ident.name)?
            .into_iter()
            .map(|rec| rec.migration_file)
            .collect::<Vec<_>>();
        let have_data = !migration_list.is_empty();

        if drop_data {
            if !have_data {
                tlog!(Info, "plugin has no data (`DOWN` migrations not needed)");
            } else {
                // XXX: inside this call we'll be reenterring reenterable_plugin_cas_request,
                // which is kinda messy, but should work fine.
                // We need to do this because otherwise there's a possibility of
                // race condition when migrations are applied in the middle of
                // DROP PLUGIN WITH DATA which would result in data not being dropped.
                lock::try_acquire(deadline)?;
                migration::apply_down_migrations(ident, &migration_list, deadline, &node.storage);
                let release_timeout = timeout.max(LOCK_RELEASE_MINIMUM_TIMEOUT);
                lock::release(fiber::clock().saturating_add(release_timeout))?;
            }
        }

        if !drop_data && have_data {
            #[rustfmt::skip]
            tlog!(Warning, "removing plugin '{plugin_name}' with applied `UP` migrations");
        }

        let op = PluginRaftOp::DropPlugin {
            ident: ident.clone(),
        };
        let ranges = vec![
            // Fail if any plugin migration in process
            Range::new(storage::Properties::TABLE_ID).eq([PropertyName::PendingPluginOperation]),
            // Fail if someone updates this plugin record
            Range::new(storage::Plugins::TABLE_ID).eq([&plugin_name]),
            // Fail if someone updates any service record of this plugin
            Range::new(storage::Services::TABLE_ID).eq([&plugin_name]),
        ];
        Ok(PreconditionCheckResult::DoOp((Op::Plugin(op), ranges)))
    };

    reenterable_plugin_cas_request(node, check_and_make_op, deadline)?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// PluginOp
////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum PluginOp {
    CreatePlugin {
        manifest: Manifest,
        inherit_entities: HashMap<String, Value>,
        inherit_topology: HashMap<String, Vec<String>>,
    },
    EnablePlugin {
        plugin: PluginIdentifier,
        timeout: Duration,
    },
    /// Operation to change on which tiers the given service should be deployed.
    AlterServiceTiers {
        plugin: PluginIdentifier,
        service: String,
        tier: String,
        kind: TopologyUpdateOpKind,
    },
    MigrationLock(PicoPropertyLock),
}

impl Display for PluginOp {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::CreatePlugin { manifest, .. } => {
                write!(f, "CREATE PLUGIN {}", manifest.name)?;
            }
            Self::EnablePlugin { plugin, .. } => {
                write!(f, "ALTER PLUGIN {} ENABLE", plugin.name)?;
            }
            Self::AlterServiceTiers { plugin, .. } => {
                write!(f, "ALTER PLUGIN {} CHANGE SERVICE TIER", plugin.name)?;
            }
            Self::MigrationLock { .. } => {
                write!(f, "ALTER PLUGIN MIGRATE")?;
            }
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug, Copy)]
pub enum TopologyUpdateOpKind {
    Add,
    Remove,
}

pub fn update_service_tiers(
    plugin: &PluginIdentifier,
    service: &str,
    tier: &str,
    kind: TopologyUpdateOpKind,
    timeout: Duration,
) -> traft::Result<()> {
    let deadline = Instant::now_fiber().saturating_add(timeout);
    let node = node::global()?;

    let check_and_make_op = || {
        if node.storage.properties.pending_plugin_op()?.is_some() {
            return Ok(PreconditionCheckResult::WaitIndexAndRetry);
        }

        let plugin_def = node.storage.plugins.get(plugin)?;
        if plugin_def.is_none() {
            return Err(PluginError::PluginNotFound(plugin.clone()).into());
        }

        let service_def = node.storage.services.get(plugin, service)?;
        if service_def.is_none() {
            return Err(PluginError::ServiceNotFound(service.into(), plugin.clone()).into());
        }

        let op = PluginOp::AlterServiceTiers {
            plugin: plugin.clone(),
            service: service.into(),
            tier: tier.into(),
            kind,
        };
        let dml = Dml::replace(
            storage::Properties::TABLE_ID,
            &(&PropertyName::PendingPluginOperation, &op),
            effective_user_id(),
        )?;
        let ranges = vec![
            // Fail if someone updates this plugin record
            Range::new(storage::Plugins::TABLE_ID).eq([&plugin.name, &plugin.version]),
            // Fail if someone updates this service record
            Range::new(storage::Services::TABLE_ID).eq([&plugin.name, service, &plugin.version]),
            // Fail if someone proposes another plugin operation
            Range::new(storage::Properties::TABLE_ID).eq([PropertyName::PendingPluginOperation]),
        ];
        Ok(PreconditionCheckResult::DoOp((Op::Dml(dml), ranges)))
    };

    let index_of_prepare = reenterable_plugin_cas_request(node, check_and_make_op, deadline)?;

    let mut index = index_of_prepare;
    while node.storage.properties.pending_plugin_op()?.is_some() {
        index = node.wait_index(index + 1, deadline.duration_since(Instant::now_fiber()))?;
    }

    let Some(service) = node.storage.services.get(plugin, service)? else {
        return Err(PluginError::ServiceNotFound(service.into(), plugin.clone()).into());
    };

    let contains = service.tiers.iter().any(|t| t == tier);
    let op_failed = match kind {
        TopologyUpdateOpKind::Add => !contains,
        TopologyUpdateOpKind::Remove => contains,
    };
    if op_failed {
        let cause = lookup_abort_cause(node, index_of_prepare)?;
        return Err(PluginError::TopologyUpdateAborted(plugin.clone(), cause).into());
    }

    Ok(())
}

pub fn change_config_atom(
    ident: &PluginIdentifier,
    kv: &[(&str, Vec<(&str, &str)>)],
    timeout: Duration,
) -> traft::Result<()> {
    let deadline = Instant::now_fiber().saturating_add(timeout);
    let node = node::global()?;
    let make_op = || {
        let plugin_def = node.storage.plugins.get(ident)?;
        if plugin_def.is_none() {
            #[rustfmt::skip]
            return Err(BoxError::new(ErrorCode::PluginError, format!("no such plugin `{ident}`")).into());
        }

        let mut service_config_part = Vec::with_capacity(kv.len());
        let mut ranges = Vec::with_capacity(kv.len());
        for (service, kv) in kv {
            ranges.push(Range::new(storage::Services::TABLE_ID).eq((
                &ident.name,
                service,
                &ident.version,
            )));
            ranges.push(Range::new(storage::PluginConfig::TABLE_ID).eq((
                &ident.name,
                &ident.version,
                service,
            )));

            let kv: Vec<(String, rmpv::Value)> = kv
                .iter()
                .map(|(k, v)| {
                    let json = serde_json::from_str(v);
                    let value = json.unwrap_or_else(|_| rmpv::Value::from(*v));
                    (k.to_string(), value)
                })
                .collect();

            // when migration context is changed we do not perform validation
            // since we dont have anything to validate against
            if service == &migration::CONTEXT_ENTITY {
                service_config_part.push((service.to_string(), kv));
                continue;
            }

            let service_def = node.storage.services.get(ident, service)?;
            if service_def.is_none() {
                let service_id = ServiceId::new(&ident.name, *service, &ident.version);
                #[rustfmt::skip]
                return Err(BoxError::new(ErrorCode::NoSuchService, format!("no such service `{service_id}`")).into());
            }

            let current_cfg = node
                .storage
                .plugin_config
                .get_by_entity_as_mp(ident, service)?;
            let mut current_cfg = match current_cfg {
                Value::Nil => return Err(PluginError::UpdateEmptyConfig.into()),
                Value::Map(cfg) => cfg,
                _ => return Err(PluginError::InvalidConfiguration.into()),
            };

            for (key, value) in kv.clone() {
                let Some((_, value_to_replace)) = current_cfg
                    .iter_mut()
                    .find(|(current_key, _)| current_key.as_str() == Some(key.as_str()))
                else {
                    return Err(PluginError::InvalidConfigurationKey(key).into());
                };
                *value_to_replace = value;
            }

            let new_cfg_raw =
                rmp_serde::to_vec_named(&Value::Map(current_cfg)).expect("out of memory");

            node.plugin_manager
                .handle_before_service_reconfigured(ident, service, &new_cfg_raw)?;

            service_config_part.push((service.to_string(), kv));
        }

        Ok(PreconditionCheckResult::DoOp((
            Op::Plugin(PluginRaftOp::PluginConfigPartialUpdate {
                ident: ident.clone(),
                updates: service_config_part,
            }),
            ranges.clone(),
        )))
    };

    reenterable_plugin_cas_request(node, make_op, deadline).map(|_| ())
}

#[cfg(test)]
mod tests {
    use crate::plugin::inherit_config_inplace;
    use rmpv::Value;

    fn map<'a>(entries: impl IntoIterator<Item = (&'a str, Value)>) -> Value {
        Value::Map(
            entries
                .into_iter()
                .map(|(k, v)| (Value::from(k), v))
                .collect(),
        )
    }

    #[test]
    fn config_inhertance_simple() {
        let mut new = map([("port", Value::from(9090))]);
        let cur = map([
            ("host", Value::from("localhost")),
            ("port", Value::from(8080)),
        ]);

        inherit_config_inplace(&mut new, cur);

        let expected = map([
            ("port", Value::from(8080)),
            ("host", Value::from("localhost")),
        ]);
        assert_eq!(new, expected);
    }

    #[test]
    fn config_inhertance_nested() {
        let mut new = map([("features", map([("b", Value::from(true))]))]);
        let cur = map([(
            "features",
            map([("a", Value::from(false)), ("b", Value::from(false))]),
        )]);

        inherit_config_inplace(&mut new, cur);

        let expected = map([(
            "features",
            map([("b", Value::from(false)), ("a", Value::from(false))]),
        )]);
        assert_eq!(new, expected);
    }

    #[test]
    fn config_inhertance_radix_gl_1874() {
        let mut new = map(vec![
            ("addr", Value::from("0.0.0.0:7379")),
            (
                "clients",
                map([
                    ("max_clients", Value::from(10000)),
                    ("max_input_buffer_size", Value::from(1073741824)),
                    ("max_output_buffer_size", Value::from(1073741824)),
                ]),
            ),
            ("cluster_mode", Value::from(true)),
            (
                "redis_compatibility",
                map([("enabled_deprecated_commands", Value::Array(vec![]))]),
            ),
        ]);

        let cur = map(vec![
            ("addr", Value::from("0.0.0.0:7379")),
            (
                "clients",
                map([
                    ("max_clients", Value::from(10000)),
                    ("max_input_buffer_size", Value::from(1073741824)),
                    ("max_output_buffer_size", Value::from(1073741824)),
                ]),
            ),
            ("cluster_mode", Value::from(true)),
        ]);

        let expected = new.clone();

        inherit_config_inplace(&mut new, cur);

        assert_eq!(new, expected);
    }
}
