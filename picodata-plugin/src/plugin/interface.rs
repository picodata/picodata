use crate::background;
use crate::background::ServiceWorkerManager;
use crate::error_code::ErrorCode;
use crate::util::FfiSafeStr;
pub use abi_stable;
use abi_stable::pmr::{RErr, RResult, RSlice};
use abi_stable::std_types::{RBox, RHashMap, ROk, RString, RVec};
use abi_stable::{sabi_trait, RTuple, StableAbi};
use serde::de::DeserializeOwned;
use smol_str::SmolStr;
use std::error::Error;
use std::fmt::Display;
use std::time::Duration;
use tarantool::error::TarantoolErrorCode;
use tarantool::error::{BoxError, IntoBoxError};

/// Context of current instance. Produced by picodata.
#[repr(C)]
#[derive(StableAbi, Debug)]
pub struct PicoContext {
    is_master: bool,
    pub plugin_name: FfiSafeStr,
    pub service_name: FfiSafeStr,
    pub plugin_version: FfiSafeStr,
}

impl PicoContext {
    #[inline]
    pub fn new(is_master: bool) -> PicoContext {
        Self {
            is_master,
            plugin_name: "<unset>".into(),
            service_name: "<unset>".into(),
            plugin_version: "<unset>".into(),
        }
    }

    /// # Safety
    ///
    /// Note: this is for internal use only. Plugin developers should never
    /// be copying pico context.
    #[inline]
    pub unsafe fn clone(&self) -> Self {
        Self {
            is_master: self.is_master,
            plugin_name: self.plugin_name,
            service_name: self.service_name,
            plugin_version: self.plugin_version,
        }
    }

    /// Return true if the current instance is a replicaset leader.
    #[inline]
    pub fn is_master(&self) -> bool {
        self.is_master
    }

    /// Return [`ServiceWorkerManager`] for current service.
    #[deprecated = "use `register_job`, `register_tagged_job` or `cancel_background_jobs_by_tag` directly instead"]
    pub fn worker_manager(&self) -> ServiceWorkerManager {
        ServiceWorkerManager::new(self.make_service_id())
    }

    // TODO:
    // pub fn register_job(&self) -> ServiceWorkerManager {
    // pub fn register_tagged_job(&self) -> ServiceWorkerManager {
    // pub fn cancel_job_by_tag(&self) -> ServiceWorkerManager {

    #[inline(always)]
    pub fn register_metrics_callback(&self, callback: impl Fn() -> String) -> Result<(), BoxError> {
        crate::metrics::register_metrics_handler(self, callback)
    }

    /// Add a new job to the execution.
    /// Job work life cycle will be tied to the service life cycle;
    /// this means that job will be canceled just before service is stopped.
    ///
    /// # Arguments
    ///
    /// * `job`: callback that will be executed in separated fiber.
    ///   Note that it is your responsibility to organize job graceful shutdown, see a
    ///   [`background::CancellationToken`] for details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use picodata_plugin::background::CancellationToken;
    ///
    /// # use picodata_plugin::plugin::interface::PicoContext;
    /// # fn on_start(context: PicoContext) {
    ///
    /// // this job will print "hello" every second,
    /// // and print "bye" after being canceled
    /// fn hello_printer(cancel: CancellationToken) {
    ///     while cancel.wait_timeout(Duration::from_secs(1)).is_err() {
    ///         println!("hello!");
    ///     }
    ///     println!("job cancelled, bye!")
    /// }
    /// context.register_job(hello_printer).unwrap();
    ///
    /// # }
    /// ```
    #[inline(always)]
    pub fn register_job<F>(&self, job: F) -> Result<(), BoxError>
    where
        F: FnOnce(background::CancellationToken) + 'static,
    {
        background::register_job(&self.make_service_id(), job)
    }

    /// Same as [`Self::register_job`] but caller may provide a special tag.
    /// This tag may be used for manual job cancellation using [`Self::cancel_tagged_jobs`].
    ///
    /// # Arguments
    ///
    /// * `job`: callback that will be executed in separated fiber
    /// * `tag`: tag, that will be related to a job, single tag may be related to the multiple jobs
    #[inline(always)]
    pub fn register_tagged_job<F>(&self, job: F, tag: &str) -> Result<(), BoxError>
    where
        F: FnOnce(background::CancellationToken) + 'static,
    {
        background::register_tagged_job(&self.make_service_id(), job, tag)
    }

    /// Cancel all jobs related to the given `tag`.
    /// This function return after all related jobs will be gracefully shutdown or
    /// after `timeout` duration.
    ///
    /// Returns error with code [`TarantoolErrorCode::Timeout`] in case some
    /// jobs didn't finish within `timeout`.
    ///
    /// May also theoretically return error with code [`ErrorCode::NoSuchService`]
    /// in case the service doesn't exist anymore (highly unlikely).
    ///
    /// See also [`Self::register_tagged_job`].
    #[inline(always)]
    pub fn cancel_tagged_jobs(&self, tag: &str, timeout: Duration) -> Result<(), BoxError> {
        let res = background::cancel_jobs_by_tag(&self.make_service_id(), tag, timeout)?;
        if res.n_timeouts != 0 {
            #[rustfmt::skip]
            return Err(BoxError::new(TarantoolErrorCode::Timeout, format!("some background jobs didn't finish in time (expected: {}, timed out: {})", res.n_total, res.n_timeouts)));
        }

        Ok(())
    }

    /// In case when jobs were canceled by `picodata` use this function for determine
    /// a shutdown timeout - time duration that `picodata` uses to ensure that all
    /// jobs gracefully end.
    ///
    /// By default, 5-second timeout are used.
    #[inline(always)]
    pub fn set_jobs_shutdown_timeout(&self, timeout: Duration) {
        crate::background::set_jobs_shutdown_timeout(
            self.plugin_name(),
            self.service_name(),
            self.plugin_version(),
            timeout,
        )
    }

    #[inline(always)]
    pub fn make_service_id(&self) -> ServiceId {
        ServiceId::new(
            self.plugin_name(),
            self.service_name(),
            self.plugin_version(),
        )
    }

    #[inline]
    pub fn plugin_name(&self) -> &str {
        // SAFETY: safe because lifetime is managed by borrow checker
        unsafe { self.plugin_name.as_str() }
    }

    #[inline]
    pub fn service_name(&self) -> &str {
        // SAFETY: safe because lifetime is managed by borrow checker
        unsafe { self.service_name.as_str() }
    }

    #[inline]
    pub fn plugin_version(&self) -> &str {
        // SAFETY: safe because lifetime is managed by borrow checker
        unsafe { self.plugin_version.as_str() }
    }
}

/// Unique service identifier.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ServiceId {
    pub plugin: SmolStr,
    pub service: SmolStr,
    pub version: SmolStr,
}

impl std::fmt::Display for ServiceId {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}:v{}", self.plugin, self.service, self.version,)
    }
}

impl ServiceId {
    #[inline(always)]
    pub fn new(
        plugin: impl Into<SmolStr>,
        service: impl Into<SmolStr>,
        version: impl Into<SmolStr>,
    ) -> Self {
        Self {
            plugin: plugin.into(),
            service: service.into(),
            version: version.into(),
        }
    }

    #[inline(always)]
    pub fn plugin(&self) -> &str {
        &self.plugin
    }

    #[inline(always)]
    pub fn service(&self) -> &str {
        &self.service
    }

    #[inline(always)]
    pub fn version(&self) -> &str {
        &self.version
    }
}

// --------------------------- user interface ------------------------------------------------------

/// Error type, return it from your callbacks.
pub type ErrorBox = Box<dyn Error>;

pub type CallbackResult<T> = Result<T, ErrorBox>;

/// Service trait. Implement it in your code to create a service.
pub trait Service {
    /// Use this associated type to define configuration of your service.
    type Config: DeserializeOwned;

    /// Callback to handle service configuration change once instance receives it.
    ///
    /// # Idempotency
    ///
    /// **WARNING** This callback may be called several times in a row.
    /// It is the responsibility of the plugin author to make this function idempotent.
    ///
    /// # Poison
    ///
    /// Return an error here to poison current instance.
    /// This will not cancel reconfiguration process,
    /// but will mark instance as unavailable for rpc messaging.
    ///
    /// Instance can be "healed"
    /// if any of `on_leader_change` or `on_config_change`
    /// callbacks in the future return `Ok`.
    ///
    /// # Arguments
    ///
    /// * `ctx`: instance context
    /// * `new_config`: new configuration
    /// * `old_config`: previous defined configuration
    fn on_config_change(
        &mut self,
        ctx: &PicoContext,
        new_config: Self::Config,
        old_config: Self::Config,
    ) -> CallbackResult<()> {
        _ = ctx;
        _ = new_config;
        _ = old_config;
        Ok(())
    }

    /// Called at service start on every instance.
    ///
    /// # Idempotency
    ///
    /// **WARNING** This callback may be called several times in a row (without
    /// any calls to [`Self::on_stop`]). It is the responsibility of the plugin
    /// author to make this function idempotent.
    ///
    /// An error returned here abort plugin load clusterwide thus forcing
    /// `on_stop` callback execution on every instance.
    ///
    /// # Arguments
    ///
    /// * `context`: instance context
    /// * `config`: initial configuration
    fn on_start(&mut self, context: &PicoContext, config: Self::Config) -> CallbackResult<()> {
        _ = context;
        _ = config;
        Ok(())
    }

    /// Called on instance shutdown, plugin removal or failure of the initial load.
    /// Returned error will only be logged causing no effects on plugin lifecycle.
    ///
    /// # Idempotency
    ///
    /// **WARNING** This callback may be called several times in a row.
    /// It is the responsibility of the plugin author to make this function idempotent.
    ///
    /// # Arguments
    ///
    /// * `context`: instance context
    fn on_stop(&mut self, context: &PicoContext) -> CallbackResult<()> {
        _ = context;
        Ok(())
    }

    /// Called when replicaset leader is changed.
    /// This callback will be called exactly on two instances - the old leader and the new one.
    ///
    /// # Idempotency
    ///
    /// **WARNING** This callback may be called several times in a row.
    /// It is the responsibility of the plugin author to make this function idempotent.
    ///
    /// # Poison
    ///
    /// Return an error here to poison current instance.
    /// This will not cancel reconfiguration process,
    /// but will mark instance as unavailable for rpc messaging.
    ///
    /// Instance can be "healed"
    /// if any of `on_leader_change` or `on_config_change`
    /// callbacks in the future return `Ok`.
    ///
    /// # Arguments
    ///
    /// * `context`: instance context
    fn on_leader_change(&mut self, context: &PicoContext) -> CallbackResult<()> {
        _ = context;
        Ok(())
    }

    /// `on_healthcheck` is a callback
    /// that should be called to determine if the service is functioning properly
    /// On an error instance will be poisoned
    ///
    /// # Unimplemented
    ///
    /// **WARNING** This feature is not yet implemented.
    /// The callback is never called.
    /// TODO.
    fn on_health_check(&self, context: &PicoContext) -> CallbackResult<()> {
        _ = context;
        Ok(())
    }
}

// ---------------------------- internal implementation ----------------------------------------------

/// Safe trait for sending a service trait object between ABI boundary.
/// Define interface like [`Service`] trait but using safe types from [`abi_stable`] crate.
#[sabi_trait]
pub trait ServiceStable {
    fn on_health_check(&self, context: &PicoContext) -> RResult<(), ()>;
    fn on_start(&mut self, context: &PicoContext, configuration: RSlice<u8>) -> RResult<(), ()>;
    fn on_stop(&mut self, context: &PicoContext) -> RResult<(), ()>;
    fn on_leader_change(&mut self, context: &PicoContext) -> RResult<(), ()>;
    fn on_config_change(
        &mut self,
        ctx: &PicoContext,
        new_config: RSlice<u8>,
        old_config: RSlice<u8>,
    ) -> RResult<(), ()>;
}

/// Implementation of [`ServiceStable`]
pub struct ServiceProxy<C: DeserializeOwned> {
    service: Box<dyn Service<Config = C>>,
}

impl<C: DeserializeOwned> ServiceProxy<C> {
    pub fn from_service(service: Box<dyn Service<Config = C>>) -> Self {
        Self { service }
    }
}

/// Use this function for conversion between user error and picodata internal error.
/// This conversion forces allocations because using user-error "as-is"
/// may lead to use-after-free errors.
/// UAF can happen if user error points into memory allocated by dynamic lib and lives
/// longer than dynamic lib memory (that was unmapped by system).
fn error_into_tt_error<T>(source: impl Display) -> RResult<T, ()> {
    let tt_error = BoxError::new(ErrorCode::PluginError, source.to_string());
    tt_error.set_last_error();
    RErr(())
}

macro_rules! rtry {
    ($expr: expr) => {
        match $expr {
            Ok(k) => k,
            Err(e) => return error_into_tt_error(e),
        }
    };
}

impl<C: DeserializeOwned> ServiceStable for ServiceProxy<C> {
    fn on_health_check(&self, context: &PicoContext) -> RResult<(), ()> {
        match self.service.on_health_check(context) {
            Ok(_) => ROk(()),
            Err(e) => error_into_tt_error(e),
        }
    }

    fn on_start(&mut self, context: &PicoContext, configuration: RSlice<u8>) -> RResult<(), ()> {
        let configuration: C = rtry!(rmp_serde::from_slice(configuration.as_slice()));
        match self.service.on_start(context, configuration) {
            Ok(_) => ROk(()),
            Err(e) => error_into_tt_error(e),
        }
    }

    fn on_stop(&mut self, context: &PicoContext) -> RResult<(), ()> {
        match self.service.on_stop(context) {
            Ok(_) => ROk(()),
            Err(e) => error_into_tt_error(e),
        }
    }

    fn on_leader_change(&mut self, context: &PicoContext) -> RResult<(), ()> {
        match self.service.on_leader_change(context) {
            Ok(_) => ROk(()),
            Err(e) => error_into_tt_error(e),
        }
    }

    fn on_config_change(
        &mut self,
        ctx: &PicoContext,
        new_config: RSlice<u8>,
        old_config: RSlice<u8>,
    ) -> RResult<(), ()> {
        let new_config: C = rtry!(rmp_serde::from_slice(new_config.as_slice()));
        let old_config: C = rtry!(rmp_serde::from_slice(old_config.as_slice()));

        let res = self.service.on_config_change(ctx, new_config, old_config);
        match res {
            Ok(_) => ROk(()),
            Err(e) => error_into_tt_error(e),
        }
    }
}

/// Final safe service trait object type. Can be used on both sides of ABI.
pub type ServiceBox = ServiceStable_TO<'static, RBox<()>>;

// ---------------------------- Registrar ----------------------------------------------

pub type FnServiceRegistrar = extern "C" fn(registry: &mut ServiceRegistry);

/// The reason for the existence of this trait is that [`abi_stable`] crate doesn't support
/// closures.
#[sabi_trait]
trait Factory {
    fn make(&self) -> ServiceBox;
}

type FactoryBox = Factory_TO<'static, RBox<()>>;

/// The reason for the existence of this struct is that [`abi_stable`] crate doesn't support
/// closures.
struct FactoryImpl<S: Service + 'static> {
    factory_fn: fn() -> S,
}

impl<S: Service + 'static> Factory for FactoryImpl<S> {
    fn make(&self) -> ServiceBox {
        let boxed = Box::new((self.factory_fn)());
        ServiceBox::from_value(ServiceProxy::from_service(boxed), sabi_trait::TD_Opaque)
    }
}

/// Service name and plugin version pair.
type ServiceIdent = RTuple!(RString, RString);

/// Config validator stable trait.
/// The reason for the existence of this trait is that [`abi_stable`] crate doesn't support
/// closures.
#[sabi_trait]
pub trait Validator {
    /// Validate plugin configuration.
    ///
    /// # Idempotency
    ///
    /// **WARNING** This callback may be called several times in a row.
    /// It is the responsibility of the plugin author to make this function idempotent.
    ///
    fn validate(&self, config: RSlice<u8>) -> RResult<(), ()>;
}

pub type ValidatorBox = Validator_TO<'static, RBox<()>>;

/// The reason for the existence of this struct is that [`abi_stable`] crate doesn't support
/// closures.
struct ValidatorImpl<CONFIG: DeserializeOwned + 'static> {
    func: fn(config: CONFIG) -> CallbackResult<()>,
}

impl<C: DeserializeOwned> Validator for ValidatorImpl<C> {
    fn validate(&self, config: RSlice<u8>) -> RResult<(), ()> {
        let config: C = rtry!(rmp_serde::from_slice(config.as_slice()));
        let res = (self.func)(config);
        match res {
            Ok(_) => ROk(()),
            Err(e) => error_into_tt_error(e),
        }
    }
}

/// Registry for services. Used by picodata to create instances of services.
#[repr(C)]
#[derive(Default, StableAbi)]
pub struct ServiceRegistry {
    services: RHashMap<ServiceIdent, RVec<FactoryBox>>,
    validators: RHashMap<ServiceIdent, ValidatorBox>,
}

impl ServiceRegistry {
    /// Add new service to the registry.
    ///
    /// # Arguments
    ///
    /// * `name`: service name
    /// * `plugin_version`: version of service's plugin
    /// * `factory`: new service instance factory
    pub fn add<S: Service + 'static>(
        &mut self,
        name: &str,
        plugin_version: &str,
        factory: fn() -> S,
    ) {
        let factory_inner = FactoryImpl {
            factory_fn: factory,
        };
        let factory_inner =
            FactoryBox::from_value(factory_inner, abi_stable::sabi_trait::TD_Opaque);

        let ident = ServiceIdent::from((RString::from(name), RString::from(plugin_version)));

        if self.validators.get(&ident).is_none() {
            // default validator implementation,
            // just check that configuration may be deserialized into `S::Config` type
            let validator = ValidatorImpl {
                func: |_: S::Config| Ok(()),
            };
            let validator_stable = ValidatorBox::from_value(validator, sabi_trait::TD_Opaque);
            self.validators.insert(ident.clone(), validator_stable);
        }

        let entry = self.services.entry(ident).or_default();
        entry.push(factory_inner);
    }

    /// Create service from service name and plugin version pair.
    /// Return an error if there is more than one factory suitable for creating a service.
    #[allow(clippy::result_unit_err)]
    pub fn make(&self, service_name: &str, version: &str) -> Result<Option<ServiceBox>, ()> {
        let ident = ServiceIdent::from((RString::from(service_name), RString::from(version)));
        let maybe_factories = self.services.get(&ident);

        match maybe_factories {
            None => Ok(None),
            Some(factories) if factories.len() == 1 => {
                Ok(factories.first().map(|factory| factory.make()))
            }
            Some(_) => Err(()),
        }
    }

    /// Return true if registry contains needle service, false elsewhere.
    /// Return an error if there is more than one factory suitable for creating a service.
    #[allow(clippy::result_unit_err)]
    pub fn contains(&self, service_name: &str, version: &str) -> Result<bool, ()> {
        let ident = ServiceIdent::from((RString::from(service_name), RString::from(version)));
        match self.services.get(&ident) {
            None => Ok(false),
            Some(factories) if factories.len() == 1 => Ok(true),
            Some(_) => Err(()),
        }
    }

    /// Add validator for service configuration. Called before new configration is loaded.
    /// Returning an error for validator will abort configuration change clusterwide.
    ///
    /// # Arguments
    ///
    /// * `service_name`: service name which configuration will be validated
    /// * `plugin_version`: plugin version
    /// * `validator`: validation function
    pub fn add_config_validator<S: Service>(
        &mut self,
        service_name: &str,
        plugin_version: &str,
        validator: fn(S::Config) -> CallbackResult<()>,
    ) where
        S::Config: DeserializeOwned + 'static,
    {
        let ident =
            ServiceIdent::from((RString::from(service_name), RString::from(plugin_version)));

        let validator = ValidatorImpl { func: validator };
        let validator_stable = ValidatorBox::from_value(validator, sabi_trait::TD_Opaque);
        self.validators.insert(ident, validator_stable);
    }

    /// Remove config validator for service.
    pub fn remove_config_validator(
        &mut self,
        service_name: &str,
        version: &str,
    ) -> Option<ValidatorBox> {
        let ident = ServiceIdent::from((RString::from(service_name), RString::from(version)));
        self.validators.remove(&ident).into_option()
    }

    /// Return a registered list of (service name, plugin version) pairs.
    pub fn dump(&self) -> Vec<(String, String)> {
        self.services
            .keys()
            .map(|key| {
                let service = key.0.to_string();
                let version = key.1.to_string();
                (service, version)
            })
            .collect()
    }
}
