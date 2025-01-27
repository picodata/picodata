use crate::config::PicodataConfig;
use crate::info::PICODATA_VERSION;
use crate::plugin::rpc;
use crate::plugin::LibraryWrapper;
use crate::plugin::PluginError::{PluginNotFound, ServiceCollision};
use crate::plugin::ServiceState;
use crate::plugin::{
    remove_routes, replace_routes, topology, Manifest, PluginAsyncEvent, PluginCallbackError,
    PluginError, PluginEvent, PluginIdentifier, Result,
};
use crate::schema::{PluginDef, ServiceDef, ServiceRouteItem, ServiceRouteKey};
use crate::storage::Clusterwide;
use crate::traft::network::ConnectionPool;
use crate::traft::network::WorkerOptions;
use crate::traft::node;
use crate::traft::node::Node;
use crate::version::Version;
use crate::{tlog, traft, warn_or_panic};
use abi_stable::derive_macro_reexports::{RErr, RResult, RSlice};
use abi_stable::std_types::RStr;
use picodata_plugin::background::{Error, InternalGlobalWorkerManager};
use picodata_plugin::error_code::ErrorCode::PluginError as PluginErrorCode;
use picodata_plugin::plugin::interface::FnServiceRegistrar;
use picodata_plugin::plugin::interface::ServiceId;
use picodata_plugin::plugin::interface::{PicoContext, ServiceRegistry};
use picodata_plugin::util::DisplayErrorLocation;
use std::collections::HashMap;
use std::fs;
use std::fs::ReadDir;
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::fiber;
use tarantool::util::IntoClones;

fn context_from_node(node: &Node) -> PicoContext {
    PicoContext::new(!node.is_readonly())
}

fn context_set_service_info(context: &mut PicoContext, service: &ServiceState) {
    context.plugin_name = service.id.plugin.as_str().into();
    context.service_name = service.id.service.as_str().into();
    context.plugin_version = service.id.version.as_str().into();
}

fn plugin_compatibility_check_enabled() -> bool {
    // For the sake of simplicity, we decided to provide this option only through env.
    // However, since it is part of the public API, it must be synced with the documentation.
    std::env::var("PICODATA_UNSAFE_DISABLE_PLUGIN_COMPATIBILITY_CHECK").is_err()
}

/// Check if picodata version matches picodata_plugin version.
fn ensure_picodata_version_compatible(picoplugin_version: &str) -> Result<()> {
    let picodata = Version::try_from(PICODATA_VERSION).expect("correct picodata version");
    let picoplugin = Version::try_from(picoplugin_version).expect("correct picoplugin version");

    picodata
        .cmp_up_to_patch(&picoplugin)
        .is_eq()
        .then_some(())
        .ok_or(PluginError::IncompatiblePicopluginVersion(
            picoplugin_version.to_string(),
        ))
}

type PluginServices = Vec<Rc<ServiceState>>;

/// Represent loaded part of a plugin - services and flag of plugin activation.
struct PluginState {
    /// List of running services.
    services: PluginServices,
    /// Plugin version
    version: String,
}

impl PluginState {
    fn new(version: String) -> Self {
        Self {
            version,
            services: vec![],
        }
    }

    fn remove_service(&mut self, service: &str) -> Option<Rc<ServiceState>> {
        let index = self
            .services
            .iter()
            .position(|svc| svc.id.service == service)?;
        let service = self.services.swap_remove(index);
        Some(service)
    }
}

pub struct PluginManager {
    /// List of pairs (plugin name -> plugin state).
    ///
    /// There are two mutex here to avoid the situation when one long service callback
    /// will block other services.
    plugins: Rc<fiber::Mutex<HashMap<String, PluginState>>>,

    /// A connection pool for plugin RPC needs.
    pub(crate) pool: ConnectionPool,

    /// Queue of async events.
    events_queue: Option<fiber::channel::Channel<PluginAsyncEvent>>,
    /// Fiber for handle async events, those handlers need it for avoided yield's.
    _loop: Option<fiber::JoinHandle<'static, ()>>,
}

impl PluginManager {
    /// Create a new plugin manager.
    pub fn new(storage: Clusterwide) -> Self {
        let (rx, tx) = fiber::channel::Channel::new(1000).into_clones();
        let plugins: Rc<fiber::Mutex<HashMap<String, PluginState>>> =
            Rc::new(fiber::Mutex::default());

        let options = WorkerOptions::default();
        let pool = ConnectionPool::new(storage, options);

        let r#loop = Loop::new(plugins.clone());
        let defer_events_fiber = fiber::Builder::new()
            .name("plugin_manager_loop")
            .func(move || r#loop.run(tx))
            .defer()
            .expect("Plugin manager fiber should not fail");

        Self {
            plugins,
            pool,
            events_queue: Some(rx),
            _loop: defer_events_fiber.into(),
        }
    }

    const AVAILABLE_EXT: &'static [&'static str] = &["so", "dylib"];

    fn load_plugin_dir(&self, ident: &PluginIdentifier) -> Result<ReadDir> {
        let share_dir = PicodataConfig::get().instance.share_dir();
        let plugin_dir = share_dir.join(&ident.name);
        let plugin_dir = plugin_dir.join(&ident.version);
        Ok(fs::read_dir(plugin_dir)?)
    }

    fn load_so(path: &Path) -> Option<Rc<LibraryWrapper>> {
        // filter files by its extension
        let ext = path.extension()?;
        if !Self::AVAILABLE_EXT.contains(&ext.to_string_lossy().as_ref()) {
            return None;
        }

        // trying to load a dynamic library
        let lib = unsafe { LibraryWrapper::new(path.to_path_buf()) }
            .inspect_err(|e| tlog!(Warning, "error while open plugin candidate: {e}"))
            .ok()?;

        Some(Rc::new(lib))
    }

    /// Load plugin services into instance using plugin and service definitions.
    ///
    /// Support `dry_run` -
    /// means that plugin is not being loaded but the possibility of loading is checked.
    fn try_load_inner(
        &self,
        plugin_def: &PluginDef,
        service_defs: &[ServiceDef],
        dry_run: bool,
    ) -> Result<Vec<ServiceState>> {
        let mut loaded_services = Vec::with_capacity(service_defs.len());

        let mut service_defs_to_load = service_defs.to_vec();
        let ident = plugin_def.identifier();

        let entries = self.load_plugin_dir(&ident)?;
        for dir_entry in entries {
            let path = dir_entry?.path();

            let Some(lib) = Self::load_so(&path) else {
                continue;
            };

            // check compatibility
            let picodata_plugin_version =
                unsafe { lib.get::<&RStr<'static>>("PICOPLUGIN_VERSION")? };
            if plugin_compatibility_check_enabled() {
                ensure_picodata_version_compatible(picodata_plugin_version.as_str())?;
            } else {
                tlog!(
                    Warning,
                    "Loading a possibly incompatible plugin built using picodata_plugin {}",
                    picodata_plugin_version.as_str(),
                )
            }

            // fill registry with factories
            let mut registry = ServiceRegistry::default();
            let registrar = unsafe { lib.get::<FnServiceRegistrar>("pico_service_registrar")? };
            registrar(&mut registry);

            tlog!(
                Info,
                "Plugin registry content from file {path:?}: {:?}",
                registry.dump()
            );

            // validate all services to possible factory collisions
            service_defs.iter().try_for_each(|svc| {
                registry
                    .contains(&svc.name, &svc.version)
                    .map_err(|_| ServiceCollision(svc.name.clone(), svc.version.clone()))
                    .map(|_| ())
            })?;

            // trying to load still unloaded services
            service_defs_to_load.retain(|service_def| {
                if dry_run {
                    return !registry
                        .contains(&service_def.name, &plugin_def.version)
                        .expect("checked at previous step");
                }

                let maybe_service = registry
                    .make(&service_def.name, &plugin_def.version)
                    .expect("checked at previous step");
                if let Some(service_inner) = maybe_service {
                    let plugin_name = &*plugin_def.name;
                    let service_name = &*service_def.name;
                    let plugin_version = &*service_def.version;
                    let service_id = ServiceId::new(plugin_name, service_name, plugin_version);
                    let config_validator = registry
                        .remove_config_validator(service_name, plugin_version)
                        .expect("infallible, at least default validator should exists");

                    let service =
                        ServiceState::new(service_id, service_inner, config_validator, lib.clone());
                    loaded_services.push(service);
                    return false;
                }
                true
            });
        }

        if !service_defs_to_load.is_empty() {
            return Err(PluginError::PartialLoad(
                service_defs_to_load
                    .into_iter()
                    .map(|def| def.name)
                    .collect(),
            ));
        }

        if dry_run {
            return Ok(vec![]);
        }

        debug_assert!(loaded_services.len() == service_defs.len());
        Ok(loaded_services)
    }

    /// Load plugin into instance.
    ///
    /// How plugins are loaded:
    /// 1) Extract list of services to load from a `_pico_service` system table.
    /// 2) Filter services by plugin topology.
    /// 3) Read all `.so` and `.dylib` files from `plugin_dir/plugin_name`.
    /// 4) From each file try to load services from load-list.
    ///    If service can be loaded - remove it from load-list.
    ///    If there is more than one factory for one service - throw error.
    /// 5) After scanning all files - checks that load-list is empty - plugin fully loaded now.
    /// 6) Send and handle `PluginLoad` event - `on_start` callbacks will be called.
    /// 7) Return error if any of the previous steps failed with error.
    ///
    /// # Arguments
    ///
    /// * `ident`: plugin identity
    pub fn try_load(&self, ident: &PluginIdentifier) -> Result<()> {
        let node = node::global().expect("node must be already initialized");
        let plugin_def = node
            .storage
            .plugins
            .get(ident)
            .expect("storage should not fail")
            .ok_or_else(|| PluginNotFound(ident.clone()))?;

        let service_defs = node
            .storage
            .services
            .get_by_plugin(ident)
            .expect("storage should not fail");

        // filter services according to topology
        let topology_ctx = topology::TopologyContext::current()?;
        let service_defs = service_defs
            .into_iter()
            .filter(|svc_def| topology::probe_service(&topology_ctx, svc_def))
            .collect::<Vec<_>>();

        let loaded_services = self.try_load_inner(&plugin_def, &service_defs, false)?;

        self.plugins.lock().insert(
            plugin_def.name.clone(),
            PluginState {
                services: loaded_services.into_iter().map(Rc::new).collect(),
                version: ident.version.to_string(),
            },
        );

        self.handle_event_sync(PluginEvent::PluginLoad {
            ident,
            service_defs: &service_defs,
        })
    }

    /// Check the possibility of loading plugin into instance.
    /// Doesn't create an instances of plugin services and doesn't call any events.
    ///
    /// Return error if services from plugin manifest can't be loaded from shared libraries in
    /// `plugin_dir`.
    pub fn try_load_dry_run(&self, manifest: &Manifest) -> Result<()> {
        let plugin_def = manifest.plugin_def();
        let service_defs = manifest.service_defs();
        self.try_load_inner(&plugin_def, &service_defs, true)
            .map(|_| ())
    }

    /// Load and start all enabled plugins and services that must be loaded.
    fn handle_instance_online(&self) -> Result<()> {
        let node = node::global().expect("node must be already initialized");

        let instance_name = node
            .raft_storage
            .instance_name()
            .expect("storage should never fail")
            .expect("should be persisted before Node is initialized");

        // first, clear all routes handled by this instances
        let instance_routes = node
            .storage
            .service_route_table
            .get_by_instance(&instance_name)
            .expect("storage should not fail");
        let routing_keys: Vec<_> = instance_routes.iter().map(|r| r.key()).collect();
        // FIXME: this is wrong, use reenterable_plugin_cas_request with correct cas predicate
        remove_routes(&routing_keys, Duration::from_secs(10))
            .map_err(|traft_err| PluginError::RemoteError(traft_err.to_string()))?;

        // now plugins can be enabled and routing table filled again
        let enabled_plugins = node.storage.plugins.all_enabled()?;

        for plugin in enabled_plugins {
            let ident = plugin.into_identifier();
            // no need to load already loaded plugins
            if self.plugins.lock().get(&ident.name).is_none() {
                self.try_load(&ident)?;
            }

            let items: Vec<_> = self.plugins.lock()[&ident.name]
                .services
                .iter()
                .map(|svc| {
                    ServiceRouteItem::new_healthy(instance_name.clone(), &ident, &svc.id.service)
                })
                .collect();
            // FIXME: this is wrong, use reenterable_plugin_cas_request with correct cas predicate
            replace_routes(&items, Duration::from_secs(10))
                .map_err(|traft_err| PluginError::RemoteError(traft_err.to_string()))?;
        }

        Ok(())
    }

    /// Call `on_stop` callback at services and remove plugin from managed.
    fn handle_plugin_disabled(
        plugins: Rc<fiber::Mutex<HashMap<String, PluginState>>>,
        plugin_name: &str,
    ) -> traft::Result<()> {
        let node = node::global().expect("node must be already initialized");
        let ctx = context_from_node(node);

        let plugin = {
            let mut lock = plugins.lock();
            lock.remove(plugin_name)
        };

        if let Some(plugin_state) = plugin {
            // stop all background jobs and remove metrics first
            stop_background_jobs(&plugin_state.services);
            remove_metrics(&plugin_state.services);

            for service in plugin_state.services.iter() {
                stop_service(service, &ctx);
            }
        }

        Ok(())
    }

    /// Stop all plugin services.
    fn handle_instance_shutdown(&self) {
        let node = node::global().expect("node must be already initialized");
        let ctx = context_from_node(node);

        let plugins = self.plugins.lock();
        let services_to_stop = plugins.values().flat_map(|state| &state.services);

        // stop all background jobs and remove metrics first
        stop_background_jobs(services_to_stop.clone());
        remove_metrics(services_to_stop.clone());

        for service in services_to_stop {
            stop_service(service, &ctx);
        }
    }

    /// Call `on_config_change` at services. Poison services if error at callbacks happens.
    fn handle_config_updated(
        plugins: Rc<fiber::Mutex<HashMap<String, PluginState>>>,
        plugin_identity: &PluginIdentifier,
        service: &str,
        old_cfg_raw: &[u8],
        new_cfg_raw: &[u8],
    ) -> traft::Result<()> {
        let node = node::global()?;
        let mut ctx = context_from_node(node);
        let storage = &node.storage;

        let maybe_service = {
            let lock = plugins.lock();
            lock.get(&plugin_identity.name)
                .and_then(|plugin_state| {
                    plugin_state
                        .services
                        .iter()
                        .find(|svc| svc.id.service == service)
                })
                .cloned()
        };

        let Some(service) = maybe_service else {
            // service is not enabled yet, so we don't need to start a callback
            return Ok(());
        };
        let id = &service.id;
        context_set_service_info(&mut ctx, &service);

        #[rustfmt::skip]
        tlog!(Debug, "calling {id}.on_config_change");

        let mut guard = service.volatile_state.lock();
        let change_config_result = guard.inner.on_config_change(
            &ctx,
            RSlice::from(new_cfg_raw),
            RSlice::from(old_cfg_raw),
        );
        // Release the lock
        drop(guard);

        let instance_name = node.topology_cache.my_instance_name();
        match change_config_result {
            RResult::ROk(_) => {
                let current_instance_poisoned = storage
                    .service_route_table
                    .get(&ServiceRouteKey::new(instance_name, id))?
                    .map(|route| route.poison);

                if current_instance_poisoned == Some(true) {
                    // now the route is healthy
                    let route = ServiceRouteItem::new_healthy(
                        instance_name.into(),
                        plugin_identity,
                        &id.service,
                    );
                    // FIXME: this is wrong, use reenterable_plugin_cas_request with correct cas predicate
                    replace_routes(&[route], Duration::from_secs(10))?;
                }
            }
            RErr(_) => {
                let error = BoxError::last();
                let loc = DisplayErrorLocation(&error);
                tlog!(
                    Warning,
                    "service poisoned, {id}.on_config_change error: {loc}{error}"
                );
                // now the route is poison
                let route = ServiceRouteItem::new_poison(
                    instance_name.into(),
                    plugin_identity,
                    &id.service,
                );
                // FIXME: this is wrong, use reenterable_plugin_cas_request with correct cas predicate
                replace_routes(&[route], Duration::from_secs(10))?;
            }
        }

        Ok(())
    }

    /// Call `on_leader_change` at services. Poison services if error at callbacks happens.
    fn handle_rs_leader_change(&self) -> traft::Result<()> {
        let node = node::global()?;
        let mut ctx = context_from_node(node);
        let storage = &node.storage;
        let instance_name = node.topology_cache.my_instance_name();
        let mut routes_to_replace = vec![];

        for (plugin_name, plugin_state) in self.plugins.lock().iter() {
            let plugin_defs = node.storage.plugins.get_all_versions(plugin_name)?;
            let mut enabled_plugins: Vec<_> =
                plugin_defs.into_iter().filter(|p| p.enabled).collect();
            let plugin_def = match enabled_plugins.len() {
                0 => continue,
                1 => enabled_plugins.pop().expect("infallible"),
                _ => {
                    warn_or_panic!("only one plugin should be enabled at a single moment");
                    enabled_plugins.pop().expect("infallible")
                }
            };
            let plugin_identity = plugin_def.into_identifier();

            for service in plugin_state.services.iter() {
                let id = &service.id;
                context_set_service_info(&mut ctx, service);

                #[rustfmt::skip]
                tlog!(Debug, "calling {id}.on_leader_change");

                let mut guard = service.volatile_state.lock();
                let result = guard.inner.on_leader_change(&ctx);
                // Release the lock
                drop(guard);

                match result {
                    RResult::ROk(_) => {
                        let current_instance_poisoned = storage
                            .service_route_table
                            .get(&ServiceRouteKey::new(instance_name, id))?
                            .map(|route| route.poison);

                        if current_instance_poisoned == Some(true) {
                            // now the route is healthy
                            routes_to_replace.push(ServiceRouteItem::new_healthy(
                                instance_name.into(),
                                &plugin_identity,
                                &id.service,
                            ));
                        }
                    }
                    RErr(_) => {
                        let error = BoxError::last();
                        let loc = DisplayErrorLocation(&error);
                        tlog!(
                            Warning,
                            "service poisoned, {id}.on_leader_change error: {loc}{error}"
                        );
                        // now the route is poison
                        routes_to_replace.push(ServiceRouteItem::new_poison(
                            instance_name.into(),
                            &plugin_identity,
                            &id.service,
                        ));
                    }
                }
            }
        }

        // FIXME: this is wrong, use reenterable_plugin_cas_request with correct cas predicate
        replace_routes(&routes_to_replace, Duration::from_secs(10))?;

        Ok(())
    }

    pub fn handle_service_enabled(
        &self,
        plugin_ident: &PluginIdentifier,
        service: &str,
    ) -> Result<()> {
        let node = node::global().expect("node must be already initialized");
        let storage = &node.storage;

        let Some(plugin_def) = storage.plugins.get(plugin_ident)? else {
            return Err(PluginNotFound(plugin_ident.clone()));
        };
        let Some(service_def) = storage.services.get(plugin_ident, service)? else {
            return Err(PluginError::ServiceNotFound(
                service.to_string(),
                plugin_ident.clone(),
            ));
        };

        let service_defs = [service_def];

        let mut loaded_services = self.try_load_inner(&plugin_def, &service_defs, false)?;
        debug_assert!(loaded_services.len() == 1);
        let Some(new_service) = loaded_services.pop() else {
            return Err(PluginError::ServiceNotFound(
                service.to_string(),
                plugin_ident.clone(),
            ));
        };

        let service = Rc::new(new_service);
        let id = &service.id;

        // call `on_start` callback
        let mut ctx = context_from_node(node);
        let cfg = node
            .storage
            .plugin_config
            .get_by_entity_as_mp(plugin_ident, &service_defs[0].name)?;
        let cfg_raw = rmp_serde::encode::to_vec_named(&cfg).expect("out of memory");

        // add the service to the storage, because the `on_start` may attempt to
        // indirectly reference it through there
        let mut plugins = self.plugins.lock();
        let plugin = plugins
            .entry(plugin_def.name)
            .or_insert_with(|| PluginState::new(plugin_ident.version.to_string()));
        plugin.services.push(service.clone());

        #[rustfmt::skip]
        tlog!(Debug, "calling {id}.on_start");

        context_set_service_info(&mut ctx, &service);

        let mut guard = service.volatile_state.lock();
        let res = guard.inner.on_start(&ctx, RSlice::from(cfg_raw.as_slice()));
        // Release the lock
        drop(guard);

        if res.is_err() {
            let error = BoxError::last();
            let loc = DisplayErrorLocation(&error);
            #[rustfmt::skip]
            tlog!(Error, "plugin callback {id}.on_start error: {loc}{error}");

            // Remove the service which we just added, because it failed to enable
            plugin.remove_service(&id.service);

            return Err(PluginError::Callback(PluginCallbackError::OnStart(error)));
        }

        Ok(())
    }

    fn handle_service_disabled(&self, plugin_ident: &PluginIdentifier, service: &str) {
        let node = node::global().expect("node must be already initialized");
        let ctx = context_from_node(node);

        // remove service from runtime
        let mut plugins = self.plugins.lock();
        let Some(state) = plugins.get_mut(&plugin_ident.name) else {
            return;
        };
        if state.version != plugin_ident.version {
            return;
        }

        let Some(service_to_del) = state.remove_service(service) else {
            return;
        };
        drop(plugins);

        // stop all background jobs and remove metrics first
        stop_background_jobs(&[service_to_del.clone()]);
        remove_metrics(&[service_to_del.clone()]);

        // call `on_stop` callback and drop service
        stop_service(&service_to_del, &ctx);
    }

    fn get_plugin_services(&self, plugin_ident: &PluginIdentifier) -> Result<PluginServices> {
        let plugins = self.plugins.lock();
        let Some(state) = plugins.get(&plugin_ident.name) else {
            return Err(PluginError::PluginNotFound(plugin_ident.clone()));
        };
        Ok(state.services.clone())
    }

    /// Call `on_start` for all plugin services.
    fn handle_plugin_load(
        &self,
        plugin_ident: &PluginIdentifier,
        service_defs: &[ServiceDef],
    ) -> Result<()> {
        let services = self.get_plugin_services(plugin_ident)?;

        let node = node::global().expect("must be initialized");
        let mut ctx = context_from_node(node);

        for service in services.iter() {
            let id = &service.id;
            let def = service_defs
                .iter()
                .find(|def| def.plugin_name == id.plugin && def.name == id.service)
                .expect("definition must exists");

            let cfg = node
                .storage
                .plugin_config
                .get_by_entity_as_mp(plugin_ident, &def.name)?;
            let cfg_raw = rmp_serde::encode::to_vec_named(&cfg).expect("out of memory");

            #[rustfmt::skip]
            tlog!(Debug, "calling {id}.on_start");

            context_set_service_info(&mut ctx, service);

            let mut guard = service.volatile_state.lock();
            let res = guard.inner.on_start(&ctx, RSlice::from(cfg_raw.as_slice()));
            // Release the lock
            drop(guard);

            if res.is_err() {
                let error = BoxError::last();
                let loc = DisplayErrorLocation(&error);
                tlog!(Error, "plugin callback {id}.on_start error: {loc}{error}");

                return Err(PluginError::Callback(PluginCallbackError::OnStart(error)));
            }
        }

        Ok(())
    }

    fn handle_plugin_load_error(&self, plugin: &str) -> Result<()> {
        let node = node::global().expect("must be initialized");
        let ctx = context_from_node(node);

        let maybe_plugin_state = {
            let mut lock = self.plugins.lock();
            lock.remove(plugin)
        };

        if let Some(plugin_state) = maybe_plugin_state {
            // stop all background jobs and remove metrics first
            stop_background_jobs(&plugin_state.services);
            remove_metrics(&plugin_state.services);

            for service in plugin_state.services.iter() {
                stop_service(&service, &ctx);
            }
        }
        Ok(())
    }

    /// Call user defined service configuration validation.
    fn handle_before_service_reconfigured(
        &self,
        plugin_ident: &PluginIdentifier,
        service_name: &str,
        new_cfg_raw: &[u8],
    ) -> Result<()> {
        // fast path - service already in memory
        if let Ok(services) = self.get_plugin_services(plugin_ident) {
            for service in services.iter() {
                if service.id.service != service_name {
                    continue;
                }
                let id = &service.id;

                #[rustfmt::skip]
                tlog!(Debug, "calling {id}.config.validate");

                let service = service.volatile_state.lock();
                let res = service.config_validator.validate(RSlice::from(new_cfg_raw));

                if res.is_err() {
                    let error = BoxError::last();
                    let loc = DisplayErrorLocation(&error);
                    tlog!(
                        Error,
                        "plugin callback {id}.config.validate error: {loc}{error}"
                    );

                    return Err(PluginError::Callback(
                        PluginCallbackError::InvalidConfiguration(error),
                    ));
                }

                return Ok(());
            }
        }

        // slow path, service not in memory, validator should be loaded from .so file
        let entries = self.load_plugin_dir(plugin_ident)?;
        for dir_entry in entries {
            let path = dir_entry?.path();

            let Some(lib) = Self::load_so(&path) else {
                continue;
            };

            let mut registry = ServiceRegistry::default();
            let registrar = unsafe { lib.get::<FnServiceRegistrar>("pico_service_registrar")? };
            registrar(&mut registry);

            if registry
                .contains(service_name, &plugin_ident.version)
                .unwrap_or(true)
            {
                let validator = registry
                    .remove_config_validator(service_name, &plugin_ident.version)
                    .expect("infallible, at least default validator should exists");
                return if let RErr(_) = validator.validate(RSlice::from(new_cfg_raw)) {
                    let error = BoxError::last();
                    Err(PluginError::Callback(
                        PluginCallbackError::InvalidConfiguration(error),
                    ))
                } else {
                    Ok(())
                };
            }
        }

        tlog!(
            Warning,
            "Configuration validator for service {service_name} not found, configuration forced invalid"
        );
        Err(PluginError::Callback(
            PluginCallbackError::InvalidConfiguration(BoxError::new(
                PluginErrorCode,
                "Configuration validator for service not found",
            )),
        ))
    }

    /// Handle picodata event by plugin system.
    /// Any event may be handled by any count of plugins that are interested in this event.
    /// Return error if any of service callbacks return error.
    ///
    /// # Arguments
    ///
    /// * `event`: upcoming event
    pub fn handle_event_sync(&self, event: PluginEvent) -> Result<()> {
        match event {
            PluginEvent::InstanceOnline => {
                self.handle_instance_online()?;
            }
            PluginEvent::InstanceShutdown => {
                self.handle_instance_shutdown();
            }
            PluginEvent::PluginLoad {
                ident,
                service_defs,
            } => {
                self.handle_plugin_load(ident, service_defs)?;
            }
            PluginEvent::PluginLoadError { name } => {
                self.handle_plugin_load_error(name)?;
            }
            PluginEvent::BeforeServiceConfigurationUpdated {
                ident,
                service,
                new_raw,
            } => {
                self.handle_before_service_reconfigured(ident, service, new_raw)?;
            }
            PluginEvent::InstanceDemote | PluginEvent::InstancePromote => {
                if let Err(e) = self.handle_rs_leader_change() {
                    tlog!(Error, "on_leader_change error: {e}");
                }
            }
            PluginEvent::ServiceEnabled { ident, service } => {
                self.handle_service_enabled(ident, service)?;
            }
            PluginEvent::ServiceDisabled { ident, service } => {
                self.handle_service_disabled(ident, service);
            }
        }

        Ok(())
    }

    /// Queue event for deferred execution.
    /// May be called from transactions because never yields.
    ///
    /// # Arguments
    ///
    /// * `event`: queued event
    pub fn handle_event_async(&self, event: PluginAsyncEvent) -> Result<()> {
        self.events_queue
            .as_ref()
            .expect("infallible")
            .try_send(event)
            .map_err(|_| PluginError::AsyncEventQueueFull)
    }

    pub fn get_service_state(&self, id: &ServiceId) -> Option<Rc<ServiceState>> {
        let plugins = self.plugins.lock();
        let plugin = plugins.get(id.plugin())?;
        for service in &plugin.services {
            if &service.id == id {
                return Some(service.clone());
            }
        }

        None
    }
}

impl Drop for PluginManager {
    fn drop(&mut self) {
        let event_queue = self.events_queue.take();
        event_queue.unwrap().close();

        if let Some(r#loop) = self._loop.take() {
            r#loop.join();
        }
    }
}

#[track_caller]
fn stop_service(service: &ServiceState, context: &PicoContext) {
    let service_id = &service.id;

    // SAFETY: It's always safe to clone the context on picodata's side.
    let mut context = unsafe { context.clone() };
    context_set_service_info(&mut context, service);

    #[rustfmt::skip]
    tlog!(Debug, "calling {service_id}.on_stop");

    let mut guard = service.volatile_state.lock();
    let res = guard.inner.on_stop(&context);
    // Release the lock
    drop(guard);

    if res.is_err() {
        let error = BoxError::last();
        let loc = DisplayErrorLocation(&error);
        #[rustfmt::skip]
        tlog!(Error, "plugin callback {service_id}.on_stop error: {loc}{error}");
    }

    rpc::server::unregister_all_rpc_handlers(
        &service_id.plugin,
        &service_id.service,
        &service_id.version,
    );
}

/// Plugin manager inner loop, using for handle async events (must be run in a separate fiber).
struct Loop {
    /// List of pairs (plugin name -> plugin state)
    ///
    /// There are two mutex here to avoid the situation when one long service callback
    /// will block other services.
    plugins: Rc<fiber::Mutex<HashMap<String, PluginState>>>,
}

impl Loop {
    pub fn new(plugins: Rc<fiber::Mutex<HashMap<String, PluginState>>>) -> Self {
        Self { plugins }
    }

    fn handle_event(&self, event: PluginAsyncEvent) -> traft::Result<()> {
        match event {
            PluginAsyncEvent::ServiceConfigurationUpdated {
                ident,
                service,
                old_raw,
                new_raw,
            } => {
                let plugins = self.plugins.clone();
                if let Err(e) = PluginManager::handle_config_updated(
                    plugins, &ident, &service, &old_raw, &new_raw,
                ) {
                    tlog!(Error, "plugin {ident} service {service}, apply new plugin configuration error: {e}");
                }
            }
            PluginAsyncEvent::PluginDisabled { name } => {
                let plugins = self.plugins.clone();
                if let Err(e) = PluginManager::handle_plugin_disabled(plugins, &name) {
                    tlog!(Error, "plugin {name} remove error: {e}");
                }
            }
        }

        Ok(())
    }

    fn run(&self, event_chan: fiber::channel::Channel<PluginAsyncEvent>) {
        while let Some(event) = event_chan.recv() {
            if let Err(e) = self.handle_event(event) {
                tlog!(Error, "plugin async event handler error: {e}");
            }
        }
    }
}

fn stop_background_jobs<'a>(services: impl IntoIterator<Item = &'a Rc<ServiceState>>) {
    const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

    let mut service_to_unregister = vec![];
    for service in services {
        let mut timeout = DEFAULT_SHUTDOWN_TIMEOUT;
        if let Some(user_specified) = service.background_job_shutdown_timeout.get() {
            timeout = user_specified;
        }
        service_to_unregister.push((&service.id, timeout));
    }

    let mut fibers = vec![];
    for (svc_id, timeout) in service_to_unregister {
        fibers.push(fiber::start(move || {
            if let Err(Error::PartialCompleted(expected, completed)) =
                InternalGlobalWorkerManager::instance().unregister_service(&svc_id, timeout)
            {
                tlog!(Warning, "Not all jobs for service {svc_id} was completed on time, expected: {expected}, completed: {completed}");
            }
        }));
    }
    for fiber in fibers {
        fiber.join();
    }
}

fn remove_metrics<'a>(plugins: impl IntoIterator<Item = &'a Rc<ServiceState>>) {
    for service in plugins {
        crate::plugin::metrics::unregister_metrics_handler(&service.id)
    }
}
