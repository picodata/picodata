use crate::config::PicodataConfig;
use crate::info::PICODATA_VERSION;
use crate::plugin::background;
use crate::plugin::rpc;
use crate::plugin::LibraryWrapper;
use crate::plugin::PluginError::{PluginNotFound, ServiceCollision};
use crate::plugin::ServiceState;
use crate::plugin::{
    build_service_routes_replace_dml, reenterable_plugin_cas_request, remove_routes, topology,
    Manifest, PluginAsyncEvent, PluginCallbackError, PluginError, PluginIdentifier,
    PreconditionCheckResult, Result,
};
use crate::schema::{PluginDef, ServiceDef, ServiceRouteItem, ServiceRouteKey, ADMIN_ID};
use crate::storage::Catalog;
use crate::traft::network::ConnectionPool;
use crate::traft::network::WorkerOptions;
use crate::traft::node;
use crate::traft::node::Node;
use crate::version::Version;
use crate::{tlog, traft, warn_or_panic};
use abi_stable::derive_macro_reexports::{RErr, RResult, RSlice};
use abi_stable::std_types::RStr;
use picodata_plugin::background::FfiBackgroundJobCancellationToken;
use picodata_plugin::error_code::ErrorCode::PluginError as PluginErrorCode;
use picodata_plugin::metrics::FfiMetricsHandler;
use picodata_plugin::plugin::interface::FnServiceRegistrar;
use picodata_plugin::plugin::interface::ServiceId;
use picodata_plugin::plugin::interface::{PicoContext, ServiceRegistry};
use picodata_plugin::util::DisplayErrorLocation;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::fs;
use std::fs::ReadDir;
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::fiber;
use tarantool::session;
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

    // it is ok to run plugin with different patch version
    picodata
        .cmp_up_to_minor(&picoplugin)
        .is_eq()
        .then_some(())
        .ok_or(PluginError::IncompatiblePicopluginVersion(
            picoplugin_version.to_string(),
        ))?;

    // it is ok to run another patch version
    // but we still ought to warn user
    if !picodata.cmp_up_to_patch(&picoplugin).is_eq() {
        tlog!(
            Warning,
            "Plugin version {picoplugin} does not match picodata version {picodata}"
        );
    }

    Ok(())
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

type MetricsHandlerMap = HashMap<ServiceId, Rc<FfiMetricsHandler>>;
type BackgroundJobCancellationTokenMap =
    HashMap<ServiceId, Vec<(SmolStr, FfiBackgroundJobCancellationToken)>>;

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

    /// Plugin-defined metrics callbacks.
    pub(crate) metrics_handlers: fiber::Mutex<MetricsHandlerMap>,

    pub(crate) background_job_cancellation_tokens: fiber::Mutex<BackgroundJobCancellationTokenMap>,

    /// Fiber for handle async events, those handlers need it for avoided yield's.
    /// Id is only stored for debugging.
    #[allow(dead_code)]
    async_event_fiber_id: fiber::FiberId,
}

impl PluginManager {
    /// Create a new plugin manager.
    pub fn new(storage: Catalog) -> Self {
        let (rx, tx) = fiber::channel::Channel::new(1000).into_clones();
        let plugins: Rc<fiber::Mutex<HashMap<String, PluginState>>> =
            Rc::new(fiber::Mutex::default());

        let options = WorkerOptions::default();
        let pool = ConnectionPool::new(storage, options);

        let async_event_fiber_id = fiber::Builder::new()
            .name("plugin_manager_loop")
            .func(move || plugin_manager_async_event_loop(tx))
            .defer_non_joinable()
            .expect("Plugin manager fiber should not fail")
            .expect("fiber id is supported");

        Self {
            plugins,
            pool,
            events_queue: Some(rx),
            metrics_handlers: Default::default(),
            background_job_cancellation_tokens: Default::default(),
            async_event_fiber_id,
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

        self.handle_plugin_load(ident, &service_defs)?;

        Ok(())
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
    pub(crate) fn handle_instance_online(&self) -> Result<()> {
        let node = node::global().expect("node must be already initialized");
        // first, clear all routes handled by this instances
        remove_routes(Duration::from_secs(10))
            .map_err(|traft_err| PluginError::RemoteError(traft_err.to_string()))?;

        // now plugins can be enabled and routing table filled again
        let deadline = fiber::clock().saturating_add(Duration::from_secs(10));
        let check_and_make_op = || {
            let instance_name = node.topology_cache.my_instance_name();
            let enabled_plugins = node.storage.plugins.all_enabled()?;

            let mut items = Vec::new();
            for plugin in enabled_plugins {
                let ident = plugin.into_identifier();
                // no need to load already loaded plugins
                if self.plugins.lock().get(&ident.name).is_none() {
                    self.try_load(&ident)?;
                }

                items.extend(self.plugins.lock()[&ident.name].services.iter().map(|svc| {
                    ServiceRouteItem::new_healthy(instance_name.into(), &ident, &svc.id.service)
                }));
            }
            if items.is_empty() {
                return Ok(PreconditionCheckResult::AlreadyApplied);
            }
            Ok(PreconditionCheckResult::DoOp((
                build_service_routes_replace_dml(&items),
                vec![],
            )))
        };

        let _su =
            session::su(ADMIN_ID).expect("cant fail because admin should always have session");
        reenterable_plugin_cas_request(node, check_and_make_op, deadline)
            .map_err(|traft_err| PluginError::RemoteError(traft_err.to_string()))?;

        Ok(())
    }

    fn handle_async_event(&self, event: PluginAsyncEvent) -> traft::Result<()> {
        match event {
            PluginAsyncEvent::ServiceConfigurationUpdated {
                ident,
                service,
                old_raw,
                new_raw,
            } => {
                let res = self.handle_config_updated(&ident, &service, &old_raw, &new_raw);
                if let Err(e) = res {
                    tlog!(Error, "plugin {ident} service {service}, apply new plugin configuration error: {e}");
                }
            }
            PluginAsyncEvent::PluginDisabled { name } => {
                let res = self.handle_plugin_disabled(&name);
                if let Err(e) = res {
                    tlog!(Error, "plugin {name} remove error: {e}");
                }
            }
        }

        Ok(())
    }

    /// Call `on_stop` callback at services and remove plugin from managed.
    fn handle_plugin_disabled(&self, plugin_name: &str) -> traft::Result<()> {
        let node = node::global().expect("node must be already initialized");
        let ctx = context_from_node(node);

        let plugin = self.plugins.lock().remove(plugin_name);

        if let Some(plugin_state) = plugin {
            // stop all background jobs and remove metrics first
            self.stop_background_jobs(&plugin_state.services);
            self.remove_metrics_handlers(&plugin_state.services);

            for service in plugin_state.services.iter() {
                stop_service(service, &ctx);
            }
        }

        Ok(())
    }

    /// Stop all plugin services.
    pub(crate) fn handle_instance_shutdown(&self) {
        let node = node::global().expect("node must be already initialized");
        let ctx = context_from_node(node);

        let plugins = self.plugins.lock();
        let services_to_stop = plugins.values().flat_map(|state| &state.services);

        // stop all background jobs and remove metrics first
        self.stop_background_jobs(services_to_stop.clone());
        self.remove_metrics_handlers(services_to_stop.clone());

        for service in services_to_stop {
            stop_service(service, &ctx);
        }
    }

    /// Call `on_config_change` at services. Poison services if error at callbacks happens.
    fn handle_config_updated(
        &self,
        plugin_identity: &PluginIdentifier,
        service: &str,
        old_cfg_raw: &[u8],
        new_cfg_raw: &[u8],
    ) -> traft::Result<()> {
        let node = node::global()?;
        let deadline = fiber::clock().saturating_add(Duration::from_secs(10));
        let check_and_make_op = || {
            let mut ctx = context_from_node(node);
            let storage = &node.storage;

            let maybe_service = {
                let lock = self.plugins.lock();
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
                return Ok(PreconditionCheckResult::AlreadyApplied);
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
                        return Ok(PreconditionCheckResult::DoOp((
                            build_service_routes_replace_dml(&[route]),
                            vec![],
                        )));
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
                    return Ok(PreconditionCheckResult::DoOp((
                        build_service_routes_replace_dml(&[route]),
                        vec![],
                    )));
                }
            }

            Ok(PreconditionCheckResult::AlreadyApplied)
        };

        let _su =
            session::su(ADMIN_ID).expect("cant fail because admin should always have session");
        reenterable_plugin_cas_request(node, check_and_make_op, deadline)?;

        Ok(())
    }

    /// Call `on_leader_change` at services. Poison services if error at callbacks happens.
    pub(crate) fn handle_rs_leader_change(&self) -> traft::Result<()> {
        let node = node::global()?;
        let deadline = fiber::clock().saturating_add(Duration::from_secs(10));
        let check_and_make_op = || {
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
            if routes_to_replace.is_empty() {
                return Ok(PreconditionCheckResult::AlreadyApplied);
            }
            Ok(PreconditionCheckResult::DoOp((
                build_service_routes_replace_dml(&routes_to_replace),
                vec![],
            )))
        };

        let _su =
            session::su(ADMIN_ID).expect("cant fail because admin should always have session");
        reenterable_plugin_cas_request(node, check_and_make_op, deadline)?;

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

    pub(crate) fn handle_service_disabled(&self, plugin_ident: &PluginIdentifier, service: &str) {
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
        self.stop_background_jobs(&[service_to_del.clone()]);
        self.remove_metrics_handlers(&[service_to_del.clone()]);

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

    /// Call user defined service configuration validation.
    pub(crate) fn handle_before_service_reconfigured(
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

    /// Queue event for deferred execution.
    /// May be called from transactions because never yields.
    ///
    /// # Arguments
    ///
    /// * `event`: queued event
    pub fn add_async_event_to_queue(&self, event: PluginAsyncEvent) -> Result<()> {
        self.events_queue
            .as_ref()
            .expect("infallible")
            .try_send(event)
            .map_err(|_| PluginError::AsyncEventQueueFull)
    }

    fn remove_metrics_handlers<'a>(
        &self,
        services: impl IntoIterator<Item = &'a Rc<ServiceState>>,
    ) {
        let mut handlers = self.metrics_handlers.lock();
        for service in services {
            let id = &service.id;
            let handler = handlers.remove(id);
            if handler.is_some() {
                tlog!(Info, "unregistered metrics handler for `{id}`");
            }
        }
    }

    fn stop_background_jobs<'a>(&self, services: impl IntoIterator<Item = &'a Rc<ServiceState>>) {
        let mut guard = self.background_job_cancellation_tokens.lock();

        let mut service_jobs = vec![];
        for service in services {
            let Some(jobs) = guard.remove(&service.id) else {
                continue;
            };

            service_jobs.push((service, jobs));
        }

        // Release the lock.
        drop(guard);

        for (service, jobs) in &service_jobs {
            background::cancel_jobs(service, jobs);
        }

        const DEFAULT_BACKGROUND_JOB_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

        let start = fiber::clock();
        for (service, jobs) in &service_jobs {
            let timeout = service.background_job_shutdown_timeout.get();
            let timeout = timeout.unwrap_or(DEFAULT_BACKGROUND_JOB_SHUTDOWN_TIMEOUT);
            let deadline = start.saturating_add(timeout);

            background::wait_jobs_finished(service, jobs, deadline);
        }
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
fn plugin_manager_async_event_loop(event_chan: fiber::channel::Channel<PluginAsyncEvent>) {
    while let Some(event) = event_chan.recv() {
        let node =
            node::global().expect("initialized by the time plugin async events start happening");
        if let Err(e) = node.plugin_manager.handle_async_event(event) {
            tlog!(Error, "plugin async event handler error: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_versions_match_exactly() {
        // we just try the exact same version
        let plugin_version = Version::try_from(PICODATA_VERSION)
            .expect("correct picodata version")
            .to_string();

        assert!(ensure_picodata_version_compatible(plugin_version.as_str()).is_ok());
    }

    #[test]
    fn test_versions_match_minor_different_patch() {
        // try X.Y.Z+1
        let plugin_version = Version::try_from(PICODATA_VERSION)
            .expect("correct picodata version")
            .next_by_patch()
            .to_string();

        // Same major and minor, different patch
        assert!(ensure_picodata_version_compatible(plugin_version.as_str()).is_ok());
    }

    #[test]
    fn test_versions_different_minor() {
        // try X.Y+1.Z
        let plugin_version = Version::try_from(PICODATA_VERSION)
            .expect("correct picodata version")
            .next_by_minor()
            .to_string();

        let result = ensure_picodata_version_compatible(plugin_version.as_str());
        assert!(matches!(
            result,
            Err(PluginError::IncompatiblePicopluginVersion(_))
        ));
    }

    #[test]
    fn test_versions_different_major() {
        // try X+1.Y.Z
        let plugin_version = Version::try_from(PICODATA_VERSION)
            .expect("correct picodata version")
            .next_by_major()
            .to_string();

        let result = ensure_picodata_version_compatible(plugin_version.as_str());
        assert!(matches!(
            result,
            Err(PluginError::IncompatiblePicopluginVersion(_))
        ));
    }
}
