use crate::error_code::ErrorCode;
use crate::plugin::Service;
use crate::tlog;
use picoplugin::transport::context::FfiSafeContext;
use picoplugin::transport::rpc::server::FfiRpcHandler;
use picoplugin::util::RegionBuffer;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use tarantool::error::BoxError;
use tarantool::error::Error as TntError;
use tarantool::error::TarantoolErrorCode;
use tarantool::fiber;
use tarantool::tuple::RawBytes;
use tarantool::unwrap_ok_or;

////////////////////////////////////////////////////////////////////////////////
// proc_rpc_dispatch
////////////////////////////////////////////////////////////////////////////////

#[tarantool::proc(packed_args)]
pub fn proc_rpc_dispatch(args: &RawBytes) -> Result<&'static RawBytes, TntError> {
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

    // SAFETY: safe because keys don't leak
    let maybe_handler = unsafe { handlers_mut().get(&key) };

    let Some(handler) = maybe_handler else {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::NoSuchFunction, format!("no RPC endpoint `{}.{}{path}` is registered", key.plugin, key.service)).into());
    };

    // SAFETY: safe because it doesn't outlive `args`
    let v_requestor = unsafe { context.plugin_version.as_str() };

    if handler.version() != v_requestor {
        return Err(BoxError::new(ErrorCode::WrongPluginVersion, format!("RPC request to an endpoint `{plugin}.{service}{path}` with incompatible version (requestor: {v_requestor}, handler: {v_handler})",
            plugin=key.plugin,
            service=key.service,
            v_handler=handler.version(),
        )).into());
    }

    // TODO: check service is not poisoned

    fiber::set_name(handler.route_repr());
    let output = handler
        .call(input, &context)
        .map_err(|()| BoxError::last())?;

    let mut buffer = RegionBuffer::new();
    rmp::encode::write_bin(&mut buffer, output)?;

    // Note: region will be cleaned up by tarantool when the fiber is returned
    // to the iproto fiber pool, but before the data is copied to the iproto
    // network buffer, so it's always safe to leak region allocations from
    // stored procedures
    let (slice, _) = buffer.into_raw_parts();

    Ok(RawBytes::new(slice))
}

////////////////////////////////////////////////////////////////////////////////
// handler storage
////////////////////////////////////////////////////////////////////////////////

static mut HANDLERS: Option<RpcHandlerMap> = None;

type RpcHandlerMap = HashMap<RpcHandlerKey<'static>, FfiRpcHandler>;

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
    HANDLERS.as_mut().expect("should be initialized at startup")
}

pub fn register_rpc_handler(handler: FfiRpcHandler) -> Result<(), BoxError> {
    if handler.path().is_empty() {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "RPC route path cannot be empty"));
    } else if !handler.path().starts_with('/') {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::IllegalParams, format!("RPC route path must start with '/', got '{}'", handler.path())));
    }

    if handler.plugin().is_empty() {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "RPC route plugin name cannot be empty"));
    }

    if handler.service().is_empty() {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "RPC route service name cannot be empty"));
    }

    if handler.version().is_empty() {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::IllegalParams, "RPC route service version cannot be empty"));
    }

    let key = RpcHandlerKey {
        plugin: handler.plugin(),
        service: handler.service(),
        path: handler.path(),
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

            #[rustfmt::skip]
            if old_handler.version() != handler.version() {
                let message = format!("RPC endpoint `{plugin}.{service}{path}` is already registered with a different version (old: {old_version}, new: {new_version})", plugin=key.plugin, service=key.service, path=key.path, old_version=old_handler.version(), new_version=handler.version());
                return Err(BoxError::new(TarantoolErrorCode::FunctionExists, message));
            } else if old_handler.identity() != handler.identity() {
                let message = format!("RPC endpoint `{plugin}.{service}:v{version}{path}` is already registered with a different handler", plugin = key.plugin, service = key.service, version = old_handler.version(), path = key.path);
                return Err(BoxError::new(TarantoolErrorCode::FunctionExists, message));
            } else {
                tlog!(
                    Info,
                    "RPC endpoint `{plugin}.{service}:v{version}{path}` is already registered",
                    plugin = handler.plugin(),
                    service = handler.service(),
                    version = handler.version(),
                    path = handler.path(),
                );
                return Ok(());
            };
        }
    };

    tlog!(
        Info,
        "registered RPC endpoint `{}.{}-v{}{}`",
        handler.plugin(),
        handler.service(),
        handler.version(),
        handler.path(),
    );
    entry.insert(handler);
    Ok(())
}

pub fn unregister_all_rpc_handlers(service: &Service) {
    // SAFETY: safe because we don't leak any references to the stored data
    let handlers = unsafe { handlers_mut() };
    handlers.retain(|_, handler| {
        let matches = handler.plugin() == service.plugin_name
            && handler.service() == service.name
            && handler.version() == service.version;
        if matches {
            tlog!(
                Info,
                "unregistered RPC endpoint `{}.{}-v{}{}`",
                handler.plugin(),
                handler.service(),
                handler.version(),
                handler.path(),
            );
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
