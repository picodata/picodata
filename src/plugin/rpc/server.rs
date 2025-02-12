use crate::error_code::ErrorCode;
use crate::tlog;
use crate::util::on_scope_exit;
use picodata_plugin::transport::context::FfiSafeContext;
use picodata_plugin::transport::rpc::server::FfiRpcHandler;
use picodata_plugin::util::RegionBuffer;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ptr::addr_of_mut;
use std::rc::Rc;
use tarantool::error::BoxError;
use tarantool::error::Error as TntError;
use tarantool::error::TarantoolErrorCode;
use tarantool::fiber;
use tarantool::tuple::RawBytes;
use tarantool::unwrap_ok_or;

////////////////////////////////////////////////////////////////////////////////
// proc_rpc_dispatch
////////////////////////////////////////////////////////////////////////////////

pub fn proc_rpc_dispatch_impl(args: &RawBytes) -> Result<&'static RawBytes, TntError> {
    let msgpack_args = msgpack_read_array(args)?;
    let [path, mut input, context] = msgpack_args[..] else {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::IllegalParams, format!("expected 3 arguments, got {}", msgpack_args.len())).into());
    };

    // 1st argument is path
    let path: &str = unwrap_ok_or!(rmp_serde::from_slice(path),
        Err(e) => return Err(BoxError::new(TarantoolErrorCode::IllegalParams, format!("first argument (path) must be a string: {e}")).into())
    );

    // 2nd argument is input
    let input_len = unwrap_ok_or!(rmp::decode::read_bin_len(&mut input),
        Err(e) => return Err(BoxError::new(TarantoolErrorCode::IllegalParams, format!("second argument (input) must be binary data: {e}")).into())
    );
    if input.len() != input_len as usize {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::InvalidMsgpack, format!("msgpack binary header is invalid: stated size is {input_len}, while it is actualy {}", input.len())).into());
    }

    // 3rd argument is context
    let Ok(context) = FfiSafeContext::decode_msgpack(path, context) else {
        let e = BoxError::last();
        #[rustfmt::skip]
        return Err(BoxError::new(e.error_code(), format!("failed to decode third argument (context): {}", e.message())).into());
    };

    // SAFETY: safe because `key` doesn't outlive `args`
    let key = unsafe {
        RpcHandlerKey {
            plugin: context.plugin_name.as_str(),
            service: context.service_name.as_str(),
            path,
        }
    };

    // SAFETY: safe because
    // - keys don't leak
    // - we don't hold on to a reference to the global data, and other fibers
    //   may safely mutate the HANDLERS hashmap.
    let maybe_handler = unsafe { handlers_mut().get(&key).cloned() };

    let Some(handler) = maybe_handler else {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::NoSuchFunction, format!("no RPC endpoint `{}.{}{path}` is registered", key.plugin, key.service)).into());
    };

    // SAFETY: safe because it doesn't outlive `args`
    let v_requestor = unsafe { context.plugin_version.as_str() };

    let v_handler = handler.identifier.version();
    if v_handler != v_requestor {
        return Err(BoxError::new(ErrorCode::WrongPluginVersion, format!("RPC request to an endpoint `{plugin}.{service}{path}` with incompatible version (requestor: {v_requestor}, handler: {v_handler})",
            plugin=key.plugin,
            service=key.service,
        )).into());
    }

    // TODO: check service is not poisoned

    let old_name = fiber::name();
    // NOTE: we use the scope guard for catch_unwind safety
    let _guard = on_scope_exit(move || fiber::set_name(&old_name));
    fiber::set_name(handler.identifier.route_repr());

    let result = handler.call(input, &context);

    // Change the name back in case the RPC is executed locally.
    drop(_guard);

    let output = result.map_err(|()| BoxError::last())?;

    let mut buffer = RegionBuffer::new();
    rmp::encode::write_bin(&mut buffer, output)?;

    // Note: region will be cleaned up by tarantool when the fiber is returned
    // to the iproto fiber pool, but before the data is copied to the iproto
    // network buffer, so it's always safe to leak region allocations from
    // stored procedures
    let (slice, _) = buffer.into_raw_parts();

    Ok(RawBytes::new(slice))
}

#[tarantool::proc(packed_args)]
pub fn proc_rpc_dispatch(args: &RawBytes) -> Result<&'static RawBytes, TntError> {
    proc_rpc_dispatch_impl(args)
}

////////////////////////////////////////////////////////////////////////////////
// handler storage
////////////////////////////////////////////////////////////////////////////////

static mut HANDLERS: Option<RpcHandlerMap> = None;

type RpcHandlerMap = HashMap<RpcHandlerKey<'static>, Rc<FfiRpcHandler>>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RpcHandlerKey<'a> {
    plugin: &'a str,
    service: &'a str,
    path: &'a str,
}

pub(crate) fn init_handlers() {
    unsafe {
        HANDLERS = Some(HashMap::new());
    }
}

unsafe fn handlers_mut() -> &'static mut RpcHandlerMap {
    (*addr_of_mut!(HANDLERS))
        .as_mut()
        .expect("should be initialized at startup")
}

pub fn register_rpc_handler(handler: FfiRpcHandler) -> Result<(), BoxError> {
    let identifier = &handler.identifier;
    if identifier.path().is_empty() {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "RPC route path cannot be empty"));
    } else if !identifier.path().starts_with('/') {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::IllegalParams, format!("RPC route path must start with '/', got '{}'", identifier.path())));
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

    let key = RpcHandlerKey {
        plugin: identifier.plugin(),
        service: identifier.service(),
        path: identifier.path(),
    };

    // SAFETY: this is safe as long as we never let users touch `RpcHandlerKey`,
    // it must not outlive the `handler`, which should be fine because they're
    // stored together in the hash map.
    let entry = unsafe {
        handlers_mut().entry(std::mem::transmute::<RpcHandlerKey, RpcHandlerKey<'static>>(key))
    };

    let entry = match entry {
        Entry::Vacant(e) => e,
        Entry::Occupied(e) => {
            let key = e.key();
            let old_handler = e.get();

            let v_old = old_handler.identifier.version();
            let v_new = handler.identifier.version();
            #[rustfmt::skip]
            if v_old != v_new {
                let message = format!("RPC endpoint `{plugin}.{service}{path}` is already registered with a different version (old: {v_old}, new: {v_new})", plugin=key.plugin, service=key.service, path=key.path);
                return Err(BoxError::new(TarantoolErrorCode::FunctionExists, message));
            } else if old_handler.identity() != handler.identity() {
                let message = format!("RPC endpoint `{}` is already registered with a different handler", old_handler.identifier);
                return Err(BoxError::new(TarantoolErrorCode::FunctionExists, message));
            } else {
                tlog!(Info, "RPC endpoint `{}` is already registered", handler.identifier);
                return Ok(());
            };
        }
    };

    tlog!(Info, "registered RPC endpoint `{}`", handler.identifier);
    entry.insert(Rc::new(handler));
    Ok(())
}

pub fn unregister_all_rpc_handlers(plugin_name: &str, service_name: &str, plugin_version: &str) {
    // SAFETY:
    // - only called from main thread
    // - &'static mut never leaves the scope of this function
    // - function never yields
    let handlers = unsafe { handlers_mut() };
    handlers.retain(|_, handler| {
        let matches = handler.identifier.plugin() == plugin_name
            && handler.identifier.service() == service_name
            && handler.identifier.version() == plugin_version;
        if matches {
            tlog!(Info, "unregistered RPC endpoint `{}`", handler.identifier);
            // Don't retain
            false
        } else {
            // Do retain
            true
        }
    })
}

////////////////////////////////////////////////////////////////////////////////
// miscellaneous
////////////////////////////////////////////////////////////////////////////////

fn msgpack_read_array(data: &[u8]) -> Result<Vec<&[u8]>, TntError> {
    let mut iterator = std::io::Cursor::new(data);
    let count = rmp::decode::read_array_len(&mut iterator)?;
    let mut result = Vec::with_capacity(count as _);
    let mut start = iterator.position() as usize;
    for _ in 0..count {
        tarantool::msgpack::skip_value(&mut iterator)?;
        let end = iterator.position() as usize;
        let value = &data[start..end];
        result.push(value);
        start = end;
    }

    Ok(result)
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

// #[cfg(feature = "internal_test")]
mod test {
    use super::*;
    use picodata_plugin::transport::rpc;
    use std::cell::Cell;
    use tarantool::fiber;

    #[derive(Clone, Default)]
    struct DropCheck(Rc<Cell<bool>>);
    impl Drop for DropCheck {
        fn drop(&mut self) {
            self.0.set(true)
        }
    }

    fn call_rpc_local(
        plugin: &str,
        service: &str,
        version: &str,
        path: &str,
        input: &[u8],
    ) -> Result<&'static RawBytes, TntError> {
        let mut buffer = vec![];
        let request_id = tarantool::uuid::Uuid::random();
        crate::plugin::rpc::client::encode_request_arguments(
            &mut buffer,
            path,
            input,
            &request_id,
            plugin,
            service,
            version,
        )
        .unwrap();
        proc_rpc_dispatch_impl(buffer.as_slice().into())
    }

    #[tarantool::test]
    fn rpc_handler_no_use_after_free() {
        init_handlers();

        let plugin_name = "plugin";
        let service_name = "service";
        let plugin_version = "3.14.78-rc37.2";

        let cond_tx = Rc::new(fiber::Cond::new());
        let cond_rx = cond_tx.clone();
        let drop_check_rx = DropCheck::default();
        let drop_check_tx = drop_check_rx.clone();
        let n_simultaneous_fibers_rx = Rc::new(Cell::new(0));
        let n_simultaneous_fibers_tx = n_simultaneous_fibers_rx.clone();

        let builder = unsafe {
            rpc::RouteBuilder::from_service_info(plugin_name, service_name, plugin_version)
        };
        builder
            .path("/test-path")
            .register(move |request, context| {
                _ = request;
                _ = context;

                _ = &drop_check_tx;

                let was = n_simultaneous_fibers_tx.get();
                n_simultaneous_fibers_tx.set(was + 1);

                cond_rx.wait();

                Ok(Default::default())
            })
            .unwrap();

        // Control checks:
        // - the closure isn't dropped yet (obviously)
        assert_eq!(drop_check_rx.0.get(), false);
        // - no fibers have entered the closure yet
        assert_eq!(n_simultaneous_fibers_rx.get(), 0);

        let jh1 = fiber::start(|| {
            call_rpc_local(plugin_name, service_name, plugin_version, "/test-path", b"").unwrap()
        });
        let jh2 = fiber::start(|| {
            call_rpc_local(plugin_name, service_name, plugin_version, "/test-path", b"").unwrap()
        });

        // - no reason for the closure to be dropped yet
        assert_eq!(drop_check_rx.0.get(), false);
        // - both fibers have entered the closure
        assert_eq!(n_simultaneous_fibers_rx.get(), 2);

        // Unregister the handler. Now the closure should be dropped ASAP
        unregister_all_rpc_handlers(plugin_name, service_name, plugin_version);

        // - The closure has still not been dropped, because the fiber's a keeping it alive (holding strong references)
        assert_eq!(drop_check_rx.0.get(), false);

        // Wake up and join the fibers.
        cond_tx.broadcast();
        jh1.join();
        jh2.join();

        // - Finally all strong references to the closure have been dropped, and so was the closure
        assert_eq!(drop_check_rx.0.get(), true);
    }
}
