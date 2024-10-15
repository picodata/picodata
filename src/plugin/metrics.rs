use crate::tlog;
use picodata_plugin::metrics::FfiMetricsHandler;
use picodata_plugin::plugin::interface::ServiceId;
use picodata_plugin::util::RegionGuard;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::rc::Rc;
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;
use tarantool::fiber::mutex::MutexGuard;
use tarantool::fiber::Mutex;

static mut HANDLERS: Option<Mutex<MetricsHandlerMap>> = None;

type MetricsHandlerMap = HashMap<ServiceId, Rc<FfiMetricsHandler>>;

pub(crate) fn init_handlers() {
    unsafe {
        HANDLERS = Some(Mutex::new(HashMap::new()));
    }
}

#[inline]
fn handlers() -> MutexGuard<'static, MetricsHandlerMap> {
    // SAFETY: global variable access: safe in tx thread.
    let handlers = unsafe { HANDLERS.as_ref() };
    let handlers = handlers.expect("should be initialized at startup");
    handlers.lock()
}

pub fn register_metrics_handler(handler: FfiMetricsHandler) -> Result<(), BoxError> {
    let identifier = &handler.identifier;
    if !identifier.path().is_empty() {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "route path must be empty for a metrics handler"));
    }

    if identifier.plugin().is_empty() {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "RPC route plugin name cannot be empty"));
    }

    if identifier.service().is_empty() {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "RPC route service name cannot be empty"));
    }

    if identifier.version().is_empty() {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "RPC route service version cannot be empty"));
    }

    let mut handlers = handlers();
    let service_id = identifier.service_id();
    let entry = handlers.entry(service_id);

    let entry = match entry {
        Entry::Vacant(e) => e,
        Entry::Occupied(e) => {
            let service_id = e.key();
            let old_handler = e.get();
            #[rustfmt::skip]
            if old_handler.identity() != handler.identity() {
                let message = format!("metrics handler for `{service_id}` is already registered with a different handler");
                return Err(BoxError::new(TarantoolErrorCode::FunctionExists, message));
            } else {
                tlog!(Info, "metrics handler `{service_id}` is already registered");
                return Ok(());
            };
        }
    };

    let service_id = entry.key();
    tlog!(Info, "registered metrics handler for `{service_id}`");
    entry.insert(Rc::new(handler));
    Ok(())
}

pub fn unregister_metrics_handler(service_id: &ServiceId) {
    // SAFETY: global variable access: safe in tx thread.
    let handler = handlers().remove(service_id);
    if handler.is_some() {
        tlog!(Info, "unregistered metrics handler for `{service_id}`");
    }
}

pub fn get_plugin_metrics() -> String {
    let _guard = RegionGuard::new();

    let mut res = String::new();

    let handlers = handlers();
    // Note: must first copy all references to a local vec, because the callbacks may yield.
    let handler_copies: Vec<_> = handlers.values().cloned().collect();
    // Release the lock.
    drop(handlers);

    for handler in handler_copies {
        let Ok(data) = handler.call() else {
            let service_id = &handler.identifier;
            let e = BoxError::last();
            #[rustfmt::skip]
            tlog!(Error, "failed calling metrics callback for `{service_id}`: {e}");
            continue;
        };

        res.push_str(data);
        res.push('\n');
    }

    res
}
