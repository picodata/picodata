use abi_stable::std_types::{RBox, RHashMap, ROk, RString, RVec};
use abi_stable::{sabi_trait, RTuple, StableAbi};
use linkme::distributed_slice;
use std::error::Error;
use std::fmt::Display;

pub use abi_stable;
use abi_stable::pmr::{RErr, RResult, RSlice};
use serde::de::DeserializeOwned;
use tarantool::error::{BoxError, IntoBoxError};

/// Context of current instance. Produced by picodata.
#[repr(C)]
#[derive(StableAbi)]
pub struct PicoContext {
    is_master: bool,
}

impl PicoContext {
    pub fn new(is_master: bool) -> PicoContext {
        Self { is_master }
    }

    /// Return true if the current instance is a replicaset leader.
    pub fn is_master(&self) -> bool {
        self.is_master
    }
}

/// TODO
#[repr(C)]
#[derive(StableAbi)]
pub struct DDL {
    ddl_query: RString,
}

impl DDL {
    pub fn new(ddl_query: &str) -> DDL {
        DDL {
            ddl_query: ddl_query.into(),
        }
    }
}

// --------------------------- user interface ------------------------------------------------------

/// Error type, return it from your callbacks.
pub type ErrorBox = Box<dyn Error>;

pub type CallbackResult<T> = Result<T, ErrorBox>;

/// Service trait. Implement it in your code to create a service.
pub trait Service {
    /// Use this associated type to define configuration of your service.
    type CFG: DeserializeOwned;

    /// Called before new configration is loaded.
    ///
    /// Returning an error here will abort configuration change clusterwide.
    ///
    /// # Arguments
    ///
    /// * `cfg`: target configuration
    fn on_cfg_validate(&self, cfg: Self::CFG) -> CallbackResult<()> {
        _ = cfg;
        Ok(())
    }

    /// Callback to handle service configuration change once instance receives it.
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
    /// * `new_cfg`: new configuration
    /// * `old_cfg`: previous defined configuration
    fn on_config_change(
        &mut self,
        ctx: &PicoContext,
        new_cfg: Self::CFG,
        old_cfg: Self::CFG,
    ) -> CallbackResult<()> {
        _ = ctx;
        _ = new_cfg;
        _ = old_cfg;
        Ok(())
    }

    /// Called at service start on every instance.
    ///
    /// An error returned here abort plugin load clusterwide thus forcing
    /// `on_stop` callback execution on every instance.
    ///
    /// # Arguments
    ///
    /// * `context`: instance context
    /// * `cfg`: initial configuration
    fn on_start(&mut self, context: &PicoContext, cfg: Self::CFG) -> CallbackResult<()> {
        _ = context;
        _ = cfg;
        Ok(())
    }

    /// Called on instance shutdown, plugin removal or failure of the initial load.
    /// Returned error will only be logged causing no effects on plugin lifecycle.
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

    /// Define data schema.
    /// TODO.
    fn schema(&self) -> Vec<DDL> {
        vec![]
    }

    /// `on_healthcheck` is a callback
    /// that should be called to determine if the service is functioning properly
    /// On an error instance will be poisoned
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
    fn schema(&self) -> RVec<DDL>;
    fn on_cfg_validate(&self, configuration: RSlice<u8>) -> RResult<(), ()>;
    fn on_health_check(&self, context: &PicoContext) -> RResult<(), ()>;
    fn on_start(&mut self, context: &PicoContext, configuration: RSlice<u8>) -> RResult<(), ()>;
    fn on_stop(&mut self, context: &PicoContext) -> RResult<(), ()>;
    fn on_leader_change(&mut self, context: &PicoContext) -> RResult<(), ()>;
    fn on_config_change(
        &mut self,
        ctx: &PicoContext,
        new_cfg: RSlice<u8>,
        old_cfg: RSlice<u8>,
    ) -> RResult<(), ()>;
}

/// Implementation of [`ServiceStable`]
pub struct ServiceProxy<C: DeserializeOwned> {
    service: Box<dyn Service<CFG = C>>,
}

impl<C: DeserializeOwned> ServiceProxy<C> {
    pub fn from_service(service: Box<dyn Service<CFG = C>>) -> Self {
        Self { service }
    }
}

// TODO move this code into tarantool-module
const PLUGIN_ERROR_CODE: u32 = 333;

/// Use this function for conversion between user error and picodata internal error.
/// This conversion forces allocations because using user-error "as-is"
/// may lead to use-after-free errors.
/// UAF can happen if user error points into memory allocated by dynamic lib and lives
/// longer than dynamic lib memory (that was unmapped by system).
fn error_into_tt_error<T>(source: impl Display) -> RResult<T, ()> {
    let tt_error = BoxError::new(PLUGIN_ERROR_CODE, source.to_string());
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
    fn schema(&self) -> RVec<DDL> {
        self.service.schema().into()
    }

    fn on_cfg_validate(&self, configuration: RSlice<u8>) -> RResult<(), ()> {
        let configuration: C = rtry!(rmp_serde::from_slice(configuration.as_slice()));
        let res = self.service.on_cfg_validate(configuration);
        match res {
            Ok(_) => ROk(()),
            Err(e) => error_into_tt_error(e),
        }
    }

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
        new_cfg: RSlice<u8>,
        old_cfg: RSlice<u8>,
    ) -> RResult<(), ()> {
        let new_cfg: C = rtry!(rmp_serde::from_slice(new_cfg.as_slice()));
        let old_cfg: C = rtry!(rmp_serde::from_slice(old_cfg.as_slice()));

        let res = self.service.on_config_change(ctx, new_cfg, old_cfg);
        match res {
            Ok(_) => ROk(()),
            Err(e) => error_into_tt_error(e),
        }
    }
}

/// Final safe service trait object type. Can be used on both sides of ABI.
pub type ServiceBox = ServiceStable_TO<'static, RBox<()>>;

// ---------------------------- Registrar ----------------------------------------------

/// List of registrar functions.
#[distributed_slice]
pub static REGISTRARS: [extern "C" fn(registry: &mut ServiceRegistry)] = [..];

#[no_mangle]
extern "C" fn registrars() -> RSlice<'static, extern "C" fn(registry: &mut ServiceRegistry)> {
    RSlice::from_slice(&REGISTRARS)
}

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

/// Registry for services. Used by picodata to create instances of services.
#[repr(C)]
#[derive(Default, StableAbi)]
pub struct ServiceRegistry {
    services: RHashMap<ServiceIdent, RVec<FactoryBox>>,
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
        let entry = self.services.entry(ident).or_default();
        entry.push(factory_inner);
    }

    /// Create service from service name and plugin version pair.
    /// Return an error if there is more than one factory suitable for creating a service.
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
    pub fn contains(&self, service_name: &str, version: &str) -> Result<bool, ()> {
        let ident = ServiceIdent::from((RString::from(service_name), RString::from(version)));
        match self.services.get(&ident) {
            None => Ok(false),
            Some(factories) if factories.len() == 1 => Ok(true),
            Some(_) => Err(()),
        }
    }
}
