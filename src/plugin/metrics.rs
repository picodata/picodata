use crate::tlog;
use crate::traft::node;
use picodata_plugin::metrics::FfiMetricsHandler;
use picodata_plugin::util::RegionGuard;
use std::collections::hash_map::Entry;
use std::rc::Rc;
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;

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

    let node = node::global()?;
    let mut handlers = node.plugin_manager.metrics_handlers.lock();
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

pub fn get_plugin_metrics() -> String {
    let _guard = RegionGuard::new();

    let mut res = String::new();

    let Ok(node) = node::global() else {
        return "".into();
    };

    let handlers = node.plugin_manager.metrics_handlers.lock();
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
