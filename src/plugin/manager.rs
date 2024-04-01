use crate::info::InstanceInfo;
use crate::plugin::{
    remove_routes, replace_routes, Manifest, PluginAsyncEvent, PluginCallbackError, PluginError,
    PluginEvent, Result, Service, ServiceManifest, PLUGIN_DIR,
};
use crate::schema::{ServiceDef, ServiceRouteItem, ServiceRouteKey};
use crate::traft::node;
use crate::traft::node::Node;
use crate::{tlog, traft};
use abi_stable::derive_macro_reexports::{RErr, RResult, RSlice};
use libloading::{Library, Symbol};
use picoplugin::interface::{PicoContext, ServiceRegistry};
use std::collections::HashMap;
use std::fs;
use std::rc::Rc;
use std::time::Duration;
use tarantool::fiber;
use tarantool::util::IntoClones;

fn context_from_node(node: &Node) -> PicoContext {
    PicoContext::new(!node.is_readonly())
}

type PluginServices = Box<[Rc<fiber::Mutex<Service>>]>;

/// Represent loaded part of a plugin - services and flag of plugin activation.
struct PluginState {
    /// Plugin activated and ready to handle all events.
    active: bool,
    /// List of running services.
    services: PluginServices,
}

pub struct PluginManager {
    /// List of pairs (plugin name -> plugin state).
    ///
    /// There are two mutex here to avoid the situation when one long service callback
    /// will block other services.
    plugins: Rc<fiber::Mutex<HashMap<String, PluginState>>>,
    /// Queue of async events.
    events_queue: Option<fiber::channel::Channel<PluginAsyncEvent>>,
    /// Fiber for handle async events, those handlers need it for avoided yield's.
    _loop: Option<fiber::JoinHandle<'static, ()>>,
}

impl Default for PluginManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginManager {
    /// Create a new plugin manager.
    pub fn new() -> Self {
        let (rx, tx) = fiber::channel::Channel::new(1000).into_clones();
        let plugins: Rc<fiber::Mutex<HashMap<String, PluginState>>> =
            Rc::new(fiber::Mutex::default());

        let r#loop = Loop::new(plugins.clone());
        let defer_events_fiber = fiber::Builder::new()
            .name("plugin manager loop")
            .func(move || r#loop.run(tx))
            .defer()
            .expect("Plugin manager fiber should not fail");

        Self {
            plugins,
            events_queue: Some(rx),
            _loop: defer_events_fiber.into(),
        }
    }

    /// Load plugin into instance.
    /// How plugins are loaded:
    /// 1) Extract from a manifest list of services that must be loaded (load-list).
    /// 2) Read all files from `plugin_dir`.
    /// 3) From each .so file try to load services from load-list.
    /// If service can be loaded - remove it from load-list.
    /// 4) After scanning all files - checks that load-list is empty - plugin fully load now.
    /// 5) Send and handle `PluginLoad` event - `on_start` callbacks will be called.
    /// 6) Return error if any of the previous steps failed with error.
    ///
    /// After loading plugin is not-active,
    /// this means that plugin can't handle a database events.
    /// Plugin state must be set to active manually.
    ///
    /// # Arguments
    ///
    /// * `manifest`: plugin manifest
    pub fn try_load(&self, manifest: &Manifest) -> Result<()> {
        let mut loaded_services = Vec::with_capacity(manifest.services.len());

        unsafe fn get_sym<'a, T>(
            l: &'a Library,
            symbol: &str,
        ) -> std::result::Result<Symbol<'a, T>, libloading::Error> {
            l.get(symbol.as_bytes())
        }

        let service_defs = manifest.service_defs();
        let mut service_defs_to_load = service_defs.clone();

        let plugin_dir = PLUGIN_DIR.with(|dir| dir.lock().clone());
        let entries = fs::read_dir(plugin_dir)?;
        for dir_entry in entries {
            let path = dir_entry?.path();
            let Ok(lib) = (unsafe { Library::new(path) }) else {
                continue;
            };
            let lib = Rc::new(lib);

            let mut registry = ServiceRegistry::default();

            let make_registrars = unsafe {
                get_sym::<fn() -> RSlice<'static, extern "C" fn(registry: &mut ServiceRegistry)>>(
                    &lib,
                    "registrars",
                )?
            };
            let service_registrars = make_registrars();
            service_registrars.iter().for_each(|registrar| {
                registrar(&mut registry);
            });

            service_defs_to_load.retain(|service_def| {
                if let Some(service_inner) = registry.make(&service_def.name, &manifest.version) {
                    let service = Service {
                        _lib: lib.clone(),
                        name: service_def.name.to_string(),
                        inner: service_inner,
                        plugin_name: manifest.name.to_string(),
                    };
                    loaded_services.push(Rc::new(fiber::Mutex::new(service)));
                    return false;
                }
                true
            });
        }

        if loaded_services.len() != manifest.services.len() {
            return Err(PluginError::PartialLoad);
        }

        self.plugins.lock().insert(
            manifest.name.clone(),
            PluginState {
                active: false,
                services: loaded_services.into_boxed_slice(),
            },
        );

        self.handle_event_sync(PluginEvent::PluginLoad {
            name: &manifest.name,
            service_defs: &service_defs,
        })?;

        Ok(())
    }

    /// Activate selected plugin, now it can handle all events.
    ///
    /// # Arguments
    ///
    /// * `plugin`: plugin name
    pub fn activate(&self, plugin: &str) {
        if let Some(state) = self.plugins.lock().get_mut(plugin) {
            state.active = true;
        }
    }

    /// Load and start all plugins and services that must be loaded.
    fn handle_instance_start(&self, update_routing_table: bool) -> Result<()> {
        let node = node::global().expect("node must be already initialized");
        let plugins = node.storage.plugin.all()?;

        let instance_id = node
            .raft_storage
            .instance_id()
            .expect("storage should never fail")
            .expect("should be persisted before Node is initialized");

        for plugin in plugins {
            let services = node
                .storage
                .service
                .get_by_plugin_and_version(&plugin.name, &plugin.version)?;

            // build manifest using information form system tables _pico_plugin and _pico_service
            let manifest_services = services
                .iter()
                .map(|svc| ServiceManifest {
                    name: svc.name.clone(),
                    description: String::default(),
                    default_configuration: svc.configuration.clone(),
                })
                .collect();

            let manifest = Manifest {
                name: plugin.name.clone(),
                description: String::default(),
                version: plugin.version,
                services: manifest_services,
            };

            self.try_load(&manifest)?;
            // plugin can be activated immediately because it was defined in system spaces
            self.activate(&plugin.name);

            if update_routing_table {
                let items = services
                    .iter()
                    .map(|svc| {
                        ServiceRouteItem::new_healthy(
                            instance_id.clone(),
                            &svc.plugin_name,
                            &svc.name,
                        )
                    })
                    .collect::<Vec<_>>();

                replace_routes(&items, Duration::from_secs(10))
                    .map_err(|traft_err| PluginError::RemoteError(traft_err.to_string()))?;
            }
        }

        Ok(())
    }

    /// Call `on_stop` callback at services and remove plugin from managed.
    fn handle_plugin_removed(
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
            for service in plugin_state.services.iter() {
                let mut service = service.lock();
                if let RErr(e) = service.inner.on_stop(&ctx) {
                    tlog!(
                        Error,
                        "plugin {} service {} `on_stop` error: {e}",
                        service.plugin_name,
                        service.name
                    );
                }
            }

            // remove all services in instance information
            let instance_id = node
                .raft_storage
                .instance_id()
                .expect("storage should never fail")
                .expect("should be persisted before Node is initialized");

            let routing_keys_to_del = plugin_state
                .services
                .iter()
                .map(|svc| {
                    let service = svc.lock();
                    service.name.clone()
                })
                .collect::<Vec<_>>();
            let routing_keys_to_del = routing_keys_to_del
                .iter()
                .map(|service_name| ServiceRouteKey {
                    instance_id: &instance_id,
                    plugin_name,
                    service_name,
                })
                .collect::<Vec<_>>();

            remove_routes(&routing_keys_to_del, Duration::from_secs(10))?;
        }

        Ok(())
    }

    /// Stop all plugin services.
    fn handle_instance_shutdown(&self) {
        let node = node::global().expect("node must be already initialized");
        let ctx = context_from_node(node);

        let plugins = self.plugins.lock();
        let services_to_stop = plugins.values().filter_map(|plugin_state| {
            if plugin_state.active {
                Some(&plugin_state.services)
            } else {
                None
            }
        });

        for services in services_to_stop {
            for service in services.iter() {
                let mut service = service.lock();
                if let RErr(e) = service.inner.on_stop(&ctx) {
                    tlog!(
                        Error,
                        "plugin {} service {} `on_stop` error: {e}",
                        service.plugin_name,
                        service.name
                    );
                }
            }
        }
    }

    /// Call `on_config_change` at services. Poison services if error at callbacks happens.
    fn handle_config_updated(
        plugins: Rc<fiber::Mutex<HashMap<String, PluginState>>>,
        plugin: &str,
        service: &str,
        old_cfg_raw: &[u8],
        new_cfg_raw: &[u8],
    ) -> traft::Result<()> {
        let node = node::global()?;
        let ctx = context_from_node(node);
        let storage = &node.storage;

        let mb_service = {
            let lock = plugins.lock();
            lock.get(plugin)
                .and_then(|plugin_state| {
                    // plugin must be active,
                    // but this already checks at a validation step
                    debug_assert!(plugin_state.active);

                    plugin_state
                        .services
                        .iter()
                        .find(|svc| svc.lock().name == service)
                })
                .cloned()
        };

        let service = mb_service
            .ok_or_else(|| PluginError::ServiceNotFound(service.to_string(), plugin.to_string()))?;

        let mut service = service.lock();

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
                        service_name: &service_name,
                    })?
                    .map(|route| route.poison);

                if current_instance_poisoned == Some(true) {
                    // now the route is healthy
                    let route = ServiceRouteItem::new_healthy(
                        instance_info.instance_id,
                        plugin_name,
                        service_name,
                    );
                    replace_routes(&[route], Duration::from_secs(10))?;
                }
            }
            RErr(e) => {
                tlog!(Warning, "service poisoned, `on_config_change` error: {e}");
                // now the route is poison
                let route = ServiceRouteItem::new_poison(
                    instance_info.instance_id,
                    plugin_name,
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
        let ctx = context_from_node(node);
        let storage = &node.storage;
        let instance_info = InstanceInfo::try_get(node, None)?;
        let mut routes_to_replace = vec![];

        for plugin_state in self.plugins.lock().values() {
            if !plugin_state.active {
                continue;
            }

            for service in plugin_state.services.iter() {
                let service_name = service.lock().name.clone();
                let plugin_name = service.lock().plugin_name.clone();
                let result = service.lock().inner.on_leader_change(&ctx);

                match result {
                    RResult::ROk(_) => {
                        let current_instance_poisoned = storage
                            .service_route_table
                            .get(&ServiceRouteKey {
                                instance_id: &instance_info.instance_id,
                                plugin_name: &plugin_name,
                                service_name: &service_name,
                            })?
                            .map(|route| route.poison);

                        if current_instance_poisoned == Some(true) {
                            // now the route is healthy
                            routes_to_replace.push(ServiceRouteItem::new_healthy(
                                instance_info.instance_id.clone(),
                                plugin_name,
                                service_name,
                            ));
                        }
                    }
                    RErr(e) => {
                        tlog!(Warning, "service poisoned, `on_leader_change` error: {e}");
                        // now the route is poison
                        routes_to_replace.push(ServiceRouteItem::new_poison(
                            instance_info.instance_id.clone(),
                            plugin_name,
                            service_name,
                        ));
                    }
                }
            }
        }

        replace_routes(&routes_to_replace, Duration::from_secs(10))?;

        Ok(())
    }

    fn get_plugin_services(&self, plugin: &str) -> Result<PluginServices> {
        let plugins = self.plugins.lock();
        let state = plugins
            .get(plugin)
            .ok_or_else(|| PluginError::PluginNotFound(plugin.to_string()))?;
        Ok(state.services.clone())
    }

    /// Call `on_start` for all plugin services.
    fn handle_plugin_load(&self, plugin: &str, service_defs: &[ServiceDef]) -> Result<()> {
        let services = self.get_plugin_services(plugin)?;

        let node = node::global().expect("must be initialized");
        let ctx = context_from_node(node);

        for service in services.iter() {
            let mut service = service.lock();
            let def = service_defs
                .iter()
                .find(|def| def.plugin_name == service.plugin_name && def.name == service.name)
                .expect("definition must exists");

            let cfg_raw =
                rmp_serde::encode::to_vec_named(&def.configuration).expect("out of memory");

            if let RErr(e) = service
                .inner
                .on_start(&ctx, RSlice::from(cfg_raw.as_slice()))
            {
                return Err(PluginError::Callback(PluginCallbackError::OnStart(
                    e.into(),
                )));
            }
        }

        Ok(())
    }

    fn get_active_plugin_services(&self, plugin: &str) -> Result<PluginServices> {
        let plugins = self.plugins.lock();
        let state = plugins
            .get(plugin)
            .ok_or_else(|| PluginError::PluginNotFound(plugin.to_string()))?;
        if !state.active {
            return Err(PluginError::PluginInactive);
        }
        Ok(state.services.clone())
    }

    /// Call user defined service configuration validation.
    fn handle_before_service_reconfigured(
        &self,
        plugin: &str,
        service_name: &str,
        new_cfg_raw: &[u8],
    ) -> Result<()> {
        let services = self.get_active_plugin_services(plugin)?;

        for service in services.iter() {
            let service = service.lock();
            if service.name == service_name {
                return if let RErr(e) = service.inner.on_cfg_validate(RSlice::from(new_cfg_raw)) {
                    Err(PluginError::Callback(
                        PluginCallbackError::InvalidConfiguration(e.into_box()),
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
            PluginEvent::InstanceStart { join } => {
                self.handle_instance_start(join)?;
            }
            PluginEvent::InstanceShutdown => {
                self.handle_instance_shutdown();
            }
            PluginEvent::PluginLoad { name, service_defs } => {
                self.handle_plugin_load(name, service_defs)?;
            }
            PluginEvent::BeforeServiceConfigurationUpdated {
                plugin,
                service,
                new_raw,
            } => {
                self.handle_before_service_reconfigured(plugin, service, new_raw)?;
            }
            PluginEvent::InstanceDemote | PluginEvent::InstancePromote => {
                if let Err(e) = self.handle_rs_leader_change() {
                    tlog!(Error, "on_leader_change error: {e}");
                }
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
        let node = node::global().expect("must be initialized");
        let ctx = context_from_node(node);

        match event {
            PluginAsyncEvent::PluginLoadError {
                name: plugin_to_stop,
            } => {
                let mb_plugin_state = {
                    let mut lock = self.plugins.lock();
                    lock.remove(&plugin_to_stop)
                };

                if let Some(plugin_state) = mb_plugin_state {
                    for service in plugin_state.services.iter() {
                        let mut service = service.lock();
                        service.inner.on_stop(&ctx);
                    }
                }
            }
            PluginAsyncEvent::ServiceConfigurationUpdated {
                plugin,
                service,
                old_raw,
                new_raw,
            } => {
                let plugins = self.plugins.clone();
                if let Err(e) = PluginManager::handle_config_updated(
                    plugins, &plugin, &service, &old_raw, &new_raw,
                ) {
                    tlog!(Error, "plugin {plugin} service {service}, apply new plugin configuration error: {e}");
                }
            }
            PluginAsyncEvent::PluginRemoved { name } => {
                let plugins = self.plugins.clone();
                if let Err(e) = PluginManager::handle_plugin_removed(plugins, &name) {
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
