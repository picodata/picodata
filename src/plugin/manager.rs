use crate::info::InstanceInfo;
use crate::plugin::rpc;
use crate::plugin::LibraryWrapper;
use crate::plugin::PluginError::{PluginNotFound, ServiceCollision};
use crate::plugin::{
    remove_routes, replace_routes, topology, Manifest, PluginAsyncEvent, PluginCallbackError,
    PluginError, PluginEvent, PluginIdentifier, Result, Service, PLUGIN_DIR,
};
use crate::schema::{PluginDef, ServiceDef, ServiceRouteItem, ServiceRouteKey};
use crate::storage::Clusterwide;
use crate::traft::network::ConnectionPool;
use crate::traft::network::WorkerOptions;
use crate::traft::node;
use crate::traft::node::Node;
use crate::{tlog, traft, warn_or_panic};
use abi_stable::derive_macro_reexports::{RErr, RResult, RSlice};
use picoplugin::background::{Error, InternalGlobalWorkerManager, ServiceId};
use picoplugin::metrics::InternalGlobalMetricsCollection;
use picoplugin::plugin::interface::{PicoContext, ServiceRegistry};
use picoplugin::util::DisplayErrorLocation;
use std::collections::HashMap;
use std::fs;
use std::rc::Rc;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::fiber;
use tarantool::util::IntoClones;

fn context_from_node(node: &Node) -> PicoContext {
    PicoContext::new(!node.is_readonly())
}

fn context_set_service_info(context: &mut PicoContext, service: &Service) {
    context.plugin_name = service.plugin_name.as_str().into();
    context.service_name = service.name.as_str().into();
    context.plugin_version = service.version.as_str().into();
}

type PluginServices = Vec<Rc<fiber::Mutex<Service>>>;

/// Represent loaded part of a plugin - services and flag of plugin activation.
struct PluginState {
    /// List of running services.
    services: PluginServices,
    /// Plugin version
    version: String,
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

    /// Load plugin services into instance using plugin and service definitions.
    ///
    /// Support `dry_run` -
    /// means that plugin is not being loaded but the possibility of loading is checked.
    fn try_load_inner(
        plugin_def: &PluginDef,
        service_defs: &[ServiceDef],
        dry_run: bool,
    ) -> Result<Vec<Service>> {
        let mut loaded_services = Vec::with_capacity(service_defs.len());

        let mut service_defs_to_load = service_defs.to_vec();

        let plugin_dir = PLUGIN_DIR.with(|dir| dir.lock().clone());
        let plugin_dir = plugin_dir.join(&plugin_def.name);
        let plugin_dir = plugin_dir.join(&plugin_def.version);
        let entries = fs::read_dir(plugin_dir)?;
        for dir_entry in entries {
            let path = dir_entry?.path();

            // filter files by its extension
            let Some(ext) = path.extension() else {
                continue;
            };
            if !Self::AVAILABLE_EXT.contains(&ext.to_string_lossy().as_ref()) {
                continue;
            }

            // trying to load a dynamic library
            let lib = match unsafe { LibraryWrapper::new(path.clone()) } {
                Ok(lib) => lib,
                Err(e) => {
                    tlog!(Warning, "error while open plugin candidate: {e}");
                    continue;
                }
            };
            let lib = Rc::new(lib);

            // fill registry with factories
            let mut registry = ServiceRegistry::default();
            type FnRegistrars =
                fn() -> RSlice<'static, extern "C" fn(registry: &mut ServiceRegistry)>;
            let make_registrars = unsafe { lib.get::<FnRegistrars>("registrars")? };
            let service_registrars = make_registrars();
            service_registrars.iter().for_each(|registrar| {
                registrar(&mut registry);
            });

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
                    let plugin_name = plugin_def.name.clone();
                    let service_name = service_def.name.clone();
                    let plugin_version = service_def.version.clone();
                    let service_id = ServiceId::new(&plugin_name, &service_name, &plugin_version);

                    let service = Service {
                        _lib: lib.clone(),
                        name: service_name,
                        version: plugin_version,
                        inner: service_inner,
                        plugin_name,
                        id: service_id,
                    };
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
            .plugin
            .get(ident)
            .expect("storage should not fail")
            .ok_or_else(|| PluginNotFound(ident.clone()))?;

        let service_defs = node
            .storage
            .service
            .get_by_plugin(ident)
            .expect("storage should not fail");

        // filter services according to topology
        let topology_ctx = topology::TopologyContext::current()?;
        let service_defs = service_defs
            .into_iter()
            .filter(|svc_def| topology::probe_service(&topology_ctx, svc_def))
            .collect::<Vec<_>>();

        let loaded_services = Self::try_load_inner(&plugin_def, &service_defs, false)?;

        self.plugins.lock().insert(
            plugin_def.name.clone(),
            PluginState {
                services: loaded_services
                    .into_iter()
                    .map(|svc| Rc::new(fiber::Mutex::new(svc)))
                    .collect(),
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
        Self::try_load_inner(&plugin_def, &service_defs, true).map(|_| ())
    }

    /// Load and start all enabled plugins and services that must be loaded.
    fn handle_instance_online(&self) -> Result<()> {
        let node = node::global().expect("node must be already initialized");

        let instance_id = node
            .raft_storage
            .instance_id()
            .expect("storage should never fail")
            .expect("should be persisted before Node is initialized");

        // first, clear all routes handled by this instances
        let instance_routes = node
            .storage
            .service_route_table
            .get_by_instance(&instance_id)
            .expect("storage should not fail");
        let routing_keys: Vec<_> = instance_routes.iter().map(|r| r.key()).collect();
        remove_routes(&routing_keys, Duration::from_secs(10))
            .map_err(|traft_err| PluginError::RemoteError(traft_err.to_string()))?;

        // now plugins can be enabled and routing table filled again
        let enabled_plugins = node.storage.plugin.all_enabled()?;

        for plugin in enabled_plugins {
            let ident = plugin.into_identity();
            // no need to load already loaded plugins
            if self.plugins.lock().get(&ident.name).is_none() {
                self.try_load(&ident)?;
            }

            let items: Vec<_> = self.plugins.lock()[&ident.name]
                .services
                .iter()
                .map(|svc| {
                    ServiceRouteItem::new_healthy(instance_id.clone(), &ident, &svc.lock().name)
                })
                .collect();
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
        let mut ctx = context_from_node(node);

        let plugin = {
            let mut lock = plugins.lock();
            lock.remove(plugin_name)
        };

        if let Some(plugin_state) = plugin {
            // stop all background jobs and remove metrics first
            stop_background_jobs(std::iter::once(&plugin_state.services));
            remove_metrics(std::iter::once(&plugin_state.services));

            for service in plugin_state.services.iter() {
                let mut service = service.lock();
                context_set_service_info(&mut ctx, &service);
                stop_service(&mut service, &ctx);
            }
        }

        Ok(())
    }

    /// Stop all plugin services.
    fn handle_instance_shutdown(&self) {
        let node = node::global().expect("node must be already initialized");
        let mut ctx = context_from_node(node);

        let plugins = self.plugins.lock();
        let services_to_stop = plugins.values().map(|state| &state.services);

        // stop all background jobs and remove metrics first
        stop_background_jobs(services_to_stop.clone());
        remove_metrics(services_to_stop.clone());

        for services in services_to_stop {
            for service in services.iter() {
                let mut service = service.lock();
                context_set_service_info(&mut ctx, &service);
                stop_service(&mut service, &ctx);
            }
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
                        .find(|svc| svc.lock().name == service)
                })
                .cloned()
        };

        let Some(service) = maybe_service else {
            return Err(
                PluginError::ServiceNotFound(service.to_string(), plugin_identity.clone()).into(),
            );
        };
        let mut service = service.lock();
        context_set_service_info(&mut ctx, &service);

        #[rustfmt::skip]
        tlog!(Debug, "calling {}.{}:{}.on_config_change", service.plugin_name, service.name, service.version);

        let change_config_result = service.inner.on_config_change(
            &ctx,
            RSlice::from(new_cfg_raw),
            RSlice::from(old_cfg_raw),
        );

        let instance_info = InstanceInfo::try_get(node, None)?;
        let service_name = service.name.clone();
        let plugin_name = service.plugin_name.clone();
        drop(service);

        match change_config_result {
            RResult::ROk(_) => {
                let current_instance_poisoned = storage
                    .service_route_table
                    .get(&ServiceRouteKey {
                        instance_id: &instance_info.instance_id,
                        plugin_name: &plugin_name,
                        plugin_version: &plugin_identity.version,
                        service_name: &service_name,
                    })?
                    .map(|route| route.poison);

                if current_instance_poisoned == Some(true) {
                    // now the route is healthy
                    let route = ServiceRouteItem::new_healthy(
                        instance_info.instance_id,
                        plugin_identity,
                        service_name,
                    );
                    replace_routes(&[route], Duration::from_secs(10))?;
                }
            }
            RErr(_) => {
                let error = BoxError::last();
                tlog!(
                    Warning,
                    "service poisoned, `on_config_change` error: {error}"
                );
                // now the route is poison
                let route = ServiceRouteItem::new_poison(
                    instance_info.instance_id,
                    plugin_identity,
                    service_name,
                );
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
        let instance_info = InstanceInfo::try_get(node, None)?;
        let mut routes_to_replace = vec![];

        for (plugin_name, plugin_state) in self.plugins.lock().iter() {
            let plugin_defs = node.storage.plugin.get_all_versions(plugin_name)?;
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
            let plugin_identity = plugin_def.into_identity();

            for service in plugin_state.services.iter() {
                let mut service = service.lock();
                let service_name = service.name.clone();
                context_set_service_info(&mut ctx, &service);

                #[rustfmt::skip]
                tlog!(Debug, "calling {}.{}:{}.on_leader_change", service.plugin_name, service.name, service.version);

                let result = service.inner.on_leader_change(&ctx);
                // Release the lock
                drop(service);

                match result {
                    RResult::ROk(_) => {
                        let current_instance_poisoned = storage
                            .service_route_table
                            .get(&ServiceRouteKey {
                                instance_id: &instance_info.instance_id,
                                plugin_name: &plugin_identity.name,
                                plugin_version: &plugin_identity.version,
                                service_name: &service_name,
                            })?
                            .map(|route| route.poison);

                        if current_instance_poisoned == Some(true) {
                            // now the route is healthy
                            routes_to_replace.push(ServiceRouteItem::new_healthy(
                                instance_info.instance_id.clone(),
                                &plugin_identity,
                                service_name,
                            ));
                        }
                    }
                    RErr(_) => {
                        let error = BoxError::last();
                        tlog!(
                            Warning,
                            "service poisoned, `on_leader_change` error: {error}"
                        );
                        // now the route is poison
                        routes_to_replace.push(ServiceRouteItem::new_poison(
                            instance_info.instance_id.clone(),
                            &plugin_identity,
                            service_name,
                        ));
                    }
                }
            }
        }

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

        let Some(plugin_def) = storage.plugin.get(plugin_ident)? else {
            return Err(PluginNotFound(plugin_ident.clone()));
        };
        let Some(service_def) = storage.service.get(plugin_ident, service)? else {
            return Err(PluginError::ServiceNotFound(
                service.to_string(),
                plugin_ident.clone(),
            ));
        };

        let service_defs = [service_def];

        let mut loaded_services = Self::try_load_inner(&plugin_def, &service_defs, false)?;
        debug_assert!(loaded_services.len() == 1);
        let Some(mut new_service) = loaded_services.pop() else {
            return Err(PluginError::ServiceNotFound(
                service.to_string(),
                plugin_ident.clone(),
            ));
        };

        // call `on_start` callback
        let mut ctx = context_from_node(node);
        let cfg = node
            .storage
            .plugin_config
            .get_by_entity(plugin_ident, &service_defs[0].name)?;
        let cfg_raw = rmp_serde::encode::to_vec_named(&cfg).expect("out of memory");
        context_set_service_info(&mut ctx, &new_service);

        #[rustfmt::skip]
        tlog!(Debug, "calling {}.{}:{}.on_start", new_service.plugin_name, new_service.name, new_service.version);

        if let RErr(_) = new_service
            .inner
            .on_start(&ctx, RSlice::from(cfg_raw.as_slice()))
        {
            let error = BoxError::last();
            return Err(PluginError::Callback(PluginCallbackError::OnStart(error)));
        }

        // append new service to a plugin
        let mut plugins = self.plugins.lock();
        let entry = plugins.entry(plugin_def.name).or_insert(PluginState {
            services: vec![],
            version: plugin_ident.version.to_string(),
        });
        entry.services.push(Rc::new(fiber::Mutex::new(new_service)));

        Ok(())
    }

    fn handle_service_disabled(&self, plugin_ident: &PluginIdentifier, service: &str) {
        let node = node::global().expect("node must be already initialized");
        let mut ctx = context_from_node(node);

        // remove service from runtime
        let mut plugins = self.plugins.lock();
        let Some(state) = plugins.get_mut(&plugin_ident.name) else {
            return;
        };
        if state.version != plugin_ident.version {
            return;
        }

        let Some(svc_idx) = state
            .services
            .iter()
            .position(|svc| svc.lock().name == service)
        else {
            return;
        };
        let service_to_del = state.services.swap_remove(svc_idx);
        drop(plugins);

        // stop all background jobs and remove metrics first
        stop_background_jobs(std::iter::once(&vec![service_to_del.clone()]));
        remove_metrics(std::iter::once(&vec![service_to_del.clone()]));

        // call `on_stop` callback and drop service
        let mut service = service_to_del.lock();
        context_set_service_info(&mut ctx, &service);
        stop_service(&mut service, &ctx);
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
            let mut service = service.lock();
            let def = service_defs
                .iter()
                .find(|def| def.plugin_name == service.plugin_name && def.name == service.name)
                .expect("definition must exists");

            let cfg = node
                .storage
                .plugin_config
                .get_by_entity(plugin_ident, &def.name)?;
            let cfg_raw = rmp_serde::encode::to_vec_named(&cfg).expect("out of memory");

            #[rustfmt::skip]
            tlog!(Debug, "calling {}.{}:{}.on_start", service.plugin_name, service.name, service.version);

            context_set_service_info(&mut ctx, &service);
            if let RErr(_) = service
                .inner
                .on_start(&ctx, RSlice::from(cfg_raw.as_slice()))
            {
                let error = BoxError::last();
                return Err(PluginError::Callback(PluginCallbackError::OnStart(error)));
            }
        }

        Ok(())
    }

    fn handle_plugin_load_error(&self, plugin: &str) -> Result<()> {
        let node = node::global().expect("must be initialized");
        let mut ctx = context_from_node(node);

        let maybe_plugin_state = {
            let mut lock = self.plugins.lock();
            lock.remove(plugin)
        };

        if let Some(plugin_state) = maybe_plugin_state {
            // stop all background jobs and remove metrics first
            stop_background_jobs(std::iter::once(&plugin_state.services));
            remove_metrics(std::iter::once(&plugin_state.services));

            for service in plugin_state.services.iter() {
                let mut service = service.lock();
                context_set_service_info(&mut ctx, &service);
                stop_service(&mut service, &ctx);
            }
        }
        Ok(())
    }

    fn get_enabled_plugin_services(
        &self,
        plugin_ident: &PluginIdentifier,
    ) -> Result<PluginServices> {
        let node = node::global().expect("must be initialized");
        let plugin_def = node.storage.plugin.get(plugin_ident)?;
        if plugin_def.map(|def| def.enabled) != Some(true) {
            return Err(PluginError::PluginDisabled);
        }

        let plugins = self.plugins.lock();
        let Some(state) = plugins.get(&plugin_ident.name) else {
            return Err(PluginError::PluginNotFound(plugin_ident.clone()));
        };
        Ok(state.services.clone())
    }

    /// Call user defined service configuration validation.
    fn handle_before_service_reconfigured(
        &self,
        plugin_ident: &PluginIdentifier,
        service_name: &str,
        new_cfg_raw: &[u8],
    ) -> Result<()> {
        let services = self.get_enabled_plugin_services(plugin_ident)?;

        for service in services.iter() {
            let service = service.lock();
            if service.name == service_name {
                #[rustfmt::skip]
                tlog!(Debug, "calling {}.{}:{}.on_cfg_validate", service.plugin_name, service.name, service.version);

                return if let RErr(_) = service.inner.on_cfg_validate(RSlice::from(new_cfg_raw)) {
                    let error = BoxError::last();
                    Err(PluginError::Callback(
                        PluginCallbackError::InvalidConfiguration(error),
                    ))
                } else {
                    Ok(())
                };
            }
        }

        Ok(())
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
fn stop_service(service: &mut Service, context: &PicoContext) {
    // SAFETY: It's always safe to clone the context on picodata's side.
    let mut context = unsafe { context.clone() };
    context_set_service_info(&mut context, service);

    #[rustfmt::skip]
    tlog!(Debug, "calling {}.{}:{}.on_stop", service.plugin_name, service.name, service.version);

    if service.inner.on_stop(&context).is_err() {
        let error = BoxError::last();
        tlog!(
            Error,
            "plugin {} service {} `on_stop` error: {}{error}",
            service.plugin_name,
            service.name,
            DisplayErrorLocation(&error),
        );
    }

    rpc::server::unregister_all_rpc_handlers(service);
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

fn stop_background_jobs<'a>(plugins: impl Iterator<Item = &'a PluginServices>) {
    const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

    let mut service_to_unregister = vec![];
    let mut max_shutdown_timeout = DEFAULT_SHUTDOWN_TIMEOUT;
    for services in plugins {
        for service in services.iter() {
            let lock = service.lock();
            let svc_id = ServiceId::new(&lock.plugin_name, &lock.name, &lock.version);
            drop(lock);

            if let Some(timeout) =
                InternalGlobalWorkerManager::instance().get_shutdown_timeout(&svc_id)
            {
                if max_shutdown_timeout < timeout {
                    max_shutdown_timeout = timeout;
                }
            }
            service_to_unregister.push(svc_id);
        }
    }

    let mut fibers = vec![];
    for svc_id in service_to_unregister {
        fibers.push(fiber::start(move || {
            if let Err(Error::PartialCompleted(expected, completed)) =
                InternalGlobalWorkerManager::instance().unregister_service(&svc_id, max_shutdown_timeout)
            {
                tlog!(Warning, "Not all jobs for service {svc_id} was completed on time, expected: {expected}, completed: {completed}");
            }
        }));
    }
    for fiber in fibers {
        fiber.join();
    }
}

fn remove_metrics<'a>(plugins: impl Iterator<Item = &'a PluginServices>) {
    for services in plugins {
        for service in services.iter() {
            let lock = service.lock();
            let svc_id = ServiceId::new(&lock.plugin_name, &lock.name, &lock.version);
            drop(lock);

            InternalGlobalMetricsCollection::instance().remove(&svc_id)
        }
    }
}
