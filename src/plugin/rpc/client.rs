use crate::error_code::ErrorCode;
use crate::instance::InstanceId;
use crate::plugin::{rpc, PluginIdentifier};
use crate::replicaset::Replicaset;
use crate::schema::ServiceRouteItem;
use crate::schema::ServiceRouteKey;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::network::ConnectionPool;
use crate::traft::node::Node;
use crate::vshard;
use picoplugin::transport::context::ContextFieldId;
use picoplugin::transport::rpc::client::FfiSafeRpcTargetSpecifier;
use picoplugin::util::copy_to_region;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::error::Error as TntError;
use tarantool::error::IntoBoxError;
use tarantool::error::TarantoolErrorCode;
use tarantool::fiber;
use tarantool::tuple::TupleBuffer;
use tarantool::tuple::{RawByteBuf, RawBytes};
use tarantool::uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////
// rpc out
////////////////////////////////////////////////////////////////////////////////

fn process_rpc_output(mut output: &[u8]) -> Result<&'static [u8], Error> {
    let output_len = rmp::decode::read_bin_len(&mut output).map_err(|e| {
        BoxError::new(
            TarantoolErrorCode::InvalidMsgpack,
            format!("expected bin: {e}"),
        )
    })?;
    if output.len() != output_len as usize {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::InvalidMsgpack, format!("this is weird: {output_len} != {}", output.len())).into());
    }

    let res = copy_to_region(output)?;
    return Ok(res);
}

/// Returns data allocated on the region allocator (or statically allocated).
pub(crate) fn send_rpc_request(
    plugin_identity: &PluginIdentifier,
    service: &str,
    target: &FfiSafeRpcTargetSpecifier,
    path: &str,
    input: &[u8],
    timeout: f64,
) -> Result<&'static [u8], Error> {
    let node = crate::traft::node::global()?;
    let pool = &node.plugin_manager.pool;

    let timeout = Duration::from_secs_f64(timeout);

    let instance_id = resolve_rpc_target(plugin_identity, service, target, node)?;

    let my_instance_id = node
        .raft_storage
        .instance_id()?
        .expect("should be persisted at this point");

    if path.starts_with('.') {
        return call_builtin_stored_proc(pool, path, input, &instance_id, timeout);
    }

    let mut buffer = Vec::new();
    let request_id = Uuid::random();
    encode_request_arguments(
        &mut buffer,
        path,
        input,
        &request_id,
        &plugin_identity.name,
        service,
        &plugin_identity.version,
    )
    .expect("can't fail encoding into an array");
    // Safe because buffer contains a msgpack array
    let args = unsafe { TupleBuffer::from_vec_unchecked(buffer) };

    if instance_id == my_instance_id {
        let output = rpc::server::proc_rpc_dispatch_impl(args.as_ref().into())?;
        return process_rpc_output(output);
    };

    crate::error_injection!("RPC_NETWORK_ERROR" => return Err(Error::other("injected error")));
    tlog!(Debug, "sending plugin RPC request";
        "instance_id" => %instance_id,
        "request_id" => %request_id,
        "path" => path,
    );
    let future = pool.call_raw(
        &instance_id,
        crate::proc_name!(rpc::server::proc_rpc_dispatch),
        &args,
        Some(timeout),
    )?;
    // FIXME: remove this extra allocation for RawByteBuf
    let output: RawByteBuf = fiber::block_on(future)?;
    process_rpc_output(&output)
}

fn call_builtin_stored_proc(
    pool: &ConnectionPool,
    proc: &str,
    input: &[u8],
    instance_id: &InstanceId,
    timeout: Duration,
) -> Result<&'static [u8], Error> {
    // Call a builtin picodata stored procedure
    let Some(proc) = crate::rpc::to_static_proc_name(proc) else {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::NoSuchFunction, format!("unknown static stored procedure {proc}")).into());
    };

    let args = RawBytes::new(input);

    let future = pool.call_raw(instance_id, proc, args, Some(timeout))?;
    // FIXME: remove this extra allocation for RawByteBuf
    let output: RawByteBuf = fiber::block_on(future)?;

    copy_to_region(&output).map_err(Into::into)
}

fn encode_request_arguments(
    buffer: &mut Vec<u8>,
    path: &str,
    input: &[u8],
    request_id: &Uuid,
    plugin: &str,
    service: &str,
    plugin_version: &str,
) -> Result<(), TntError> {
    buffer.reserve(
        // array header
        1
        // str path
        + 5 + path.len()
        // bin input
        + 5 + input.len()
        // context header
        + 1
        // key + uuid
        + 1 + 18
        // key + str plugin
        + 1 + 5 + plugin.len()
        // key + str service
        + 1 + 5 + service.len()
        // key + str plugin_version
        + 1 + 5 + plugin_version.len(),
    );

    rmp::encode::write_array_len(buffer, 3)?;

    // Encode path
    rmp::encode::write_str(buffer, path)?;

    // Encode input
    rmp::encode::write_bin(buffer, input)?;

    // Encode context
    {
        rmp::encode::write_map_len(buffer, 4)?;

        // Encode request_id
        rmp::encode::write_uint(buffer, ContextFieldId::RequestId as _)?;
        rmp_serde::encode::write(buffer, request_id)?;

        // Encode service
        rmp::encode::write_uint(buffer, ContextFieldId::PluginName as _)?;
        rmp_serde::encode::write(buffer, plugin)?;

        // Encode service
        rmp::encode::write_uint(buffer, ContextFieldId::ServiceName as _)?;
        rmp_serde::encode::write(buffer, service)?;

        // Encode plugin_version
        rmp::encode::write_uint(buffer, ContextFieldId::PluginVersion as _)?;
        rmp_serde::encode::write(buffer, plugin_version)?;
    }

    Ok(())
}

fn get_replicaset_uuid_by_bucket_id(
    tier_name: &str,
    bucket_id: u64,
    node: &Node,
) -> Result<String, Error> {
    let res = vshard::get_replicaset_uuid_by_bucket_id(tier_name, bucket_id);
    if res.is_err() && node.storage.tiers.by_name(tier_name)?.is_none() {
        return Err(Error::NoSuchTier(tier_name.into()));
    }

    res
}

fn get_instance_id_of_master_of_replicaset(
    tier_name: &str,
    bucket_id: u64,
    node: &Node,
) -> Result<InstanceId, Error> {
    let replicaset_uuid = get_replicaset_uuid_by_bucket_id(tier_name, bucket_id, node)?;
    let tuple = node.storage.replicasets.by_uuid_raw(&replicaset_uuid)?;
    let Some(master_id) = tuple
        .field(Replicaset::FIELD_TARGET_MASTER_ID)
        .map_err(IntoBoxError::into_box_error)?
    else {
        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::StorageCorrupted, "couldn't find 'target_master_id' field in _pico_replicaset tuple").into());
    };

    Ok(master_id)
}

fn resolve_rpc_target(
    ident: &PluginIdentifier,
    service: &str,
    target: &FfiSafeRpcTargetSpecifier,
    node: &Node,
) -> Result<InstanceId, Error> {
    let mut instance_id = None;
    match target {
        FfiSafeRpcTargetSpecifier::InstanceId(iid) => {
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            instance_id = Some(InstanceId::from(unsafe { iid.as_str() }));
        }

        &FfiSafeRpcTargetSpecifier::Replicaset {
            replicaset_id,
            to_master: true,
        } => {
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            let replicaset_id = unsafe { replicaset_id.as_str() };
            let tuple = node.storage.replicasets.get_raw(replicaset_id)?;
            let Some(master_id) = tuple
                .field(Replicaset::FIELD_TARGET_MASTER_ID)
                .map_err(IntoBoxError::into_box_error)?
            else {
                #[rustfmt::skip]
                return Err(BoxError::new(ErrorCode::StorageCorrupted, "couldn't find 'target_master_id' field in _pico_replicaset tuple").into());
            };
            instance_id = Some(master_id);
        }

        &FfiSafeRpcTargetSpecifier::BucketId {
            bucket_id,
            to_master: true,
        } => {
            let current_instance_tier = node
                .raft_storage
                .tier()?
                .expect("storage for instance should exists");

            let master_id =
                get_instance_id_of_master_of_replicaset(&current_instance_tier, bucket_id, node)?;
            instance_id = Some(master_id);
        }

        &FfiSafeRpcTargetSpecifier::TierAndBucketId {
            to_master: true,
            tier,
            bucket_id,
        } => {
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            let tier = unsafe { tier.as_str() };
            let master_id = get_instance_id_of_master_of_replicaset(tier, bucket_id, node)?;
            instance_id = Some(master_id);
        }

        // These cases are handled below
        #[rustfmt::skip]
        FfiSafeRpcTargetSpecifier::BucketId { to_master: false, .. } => {}
        #[rustfmt::skip]
        FfiSafeRpcTargetSpecifier::Replicaset { to_master: false, .. } => {}
        FfiSafeRpcTargetSpecifier::Any => {}
        FfiSafeRpcTargetSpecifier::TierAndBucketId {
            to_master: false, ..
        } => {}
    }

    if let Some(instance_id) = instance_id {
        // A single instance was chosen
        check_route_to_instance(node, ident, service, &instance_id)?;
        return Ok(instance_id);
    } else {
        // Need to pick an instance from a group, fallthrough
    }

    let my_instance_id = node
        .raft_storage
        .instance_id()?
        .expect("should be persisted at this point");

    let mut candidates = node
        .storage
        .service_route_table
        .get_available_instances(ident, service)?;
    #[rustfmt::skip]
    if candidates.is_empty() {
        if node.storage.services.get(ident, service)?.is_none() {
            return Err(BoxError::new(ErrorCode::NoSuchService, "service '{plugin}.{service}' not found").into());
        } else {
            return Err(BoxError::new(ErrorCode::ServiceNotStarted, format!("service '{ident}.{service}' is not started on any instance")).into());
        }
    };

    let mut tier_and_replicaset_uuid = None;

    match target {
        #[rustfmt::skip]
        FfiSafeRpcTargetSpecifier::InstanceId { .. }
        | FfiSafeRpcTargetSpecifier::Replicaset { to_master: true, .. }
        | FfiSafeRpcTargetSpecifier::TierAndBucketId { to_master: true, .. }
        | FfiSafeRpcTargetSpecifier::BucketId { to_master: true, .. } => unreachable!("handled above"),

        &FfiSafeRpcTargetSpecifier::Replicaset {
            replicaset_id,
            to_master: false,
        } => {
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            let replicaset_id = unsafe { replicaset_id.as_str() };
            let tuple = node.storage.replicasets.get_raw(replicaset_id)?;
            let Some(found_replicaset_uuid) = tuple
                .field(Replicaset::FIELD_REPLICASET_UUID)
                .map_err(IntoBoxError::into_box_error)?
            else {
                #[rustfmt::skip]
                return Err(BoxError::new(ErrorCode::StorageCorrupted, "couldn't find 'replicaset_uuid' field in _pico_replicaset tuple").into());
            };

            let Some(found_tier) = tuple
                .field::<'_, String>(Replicaset::FIELD_TIER)
                .map_err(IntoBoxError::into_box_error)?
            else {
                #[rustfmt::skip]
                return Err(BoxError::new(ErrorCode::StorageCorrupted, "couldn't find 'tier' field in _pico_replicaset tuple").into());
            };

            tier_and_replicaset_uuid = Some((found_tier, found_replicaset_uuid));
        }

        &FfiSafeRpcTargetSpecifier::BucketId {
            bucket_id,
            to_master: false,
        } => {
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            let tier = node
                .raft_storage
                .tier()?
                .expect("storage for instance should exists");

            let replicaset_uuid = get_replicaset_uuid_by_bucket_id(&tier, bucket_id, node)?;
            tier_and_replicaset_uuid = Some((tier, replicaset_uuid));
        }

        &FfiSafeRpcTargetSpecifier::TierAndBucketId {
            tier,
            bucket_id,
            to_master: false,
        } => {
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            let tier = unsafe { tier.as_str().to_string() };
            let replicaset_uuid = get_replicaset_uuid_by_bucket_id(&tier, bucket_id, node)?;
            tier_and_replicaset_uuid = Some((tier, replicaset_uuid));
        }

        &FfiSafeRpcTargetSpecifier::Any => {}
    }

    if let Some((tier, replicaset_uuid)) = tier_and_replicaset_uuid {
        // Need to pick a replica from given replicaset

        let replicas = match vshard::get_replicaset_priority_list(&tier, &replicaset_uuid) {
            Ok(replicas) => replicas,
            Err(err) => {
                if node.storage.tiers.by_name(&tier)?.is_none() {
                    return Err(Error::NoSuchTier(tier));
                }

                return Err(err);
            }
        };

        // XXX: this shouldn't be a problem if replicasets aren't too big,
        // but if they are we might want to construct a HashSet from candidates
        for instance_id in replicas {
            if my_instance_id == instance_id && candidates.len() > 1 {
                // Prefer someone else instead of self
                continue;
            }
            if candidates.contains(&instance_id) {
                return Ok(instance_id);
            }
        }

        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::ServiceNotAvailable, format!("no {replicaset_uuid} replicas are available for service {ident}.{service}")).into());
    } else {
        // Need to pick any instance with the given plugin.service

        // TODO: find a better strategy then just the random one
        let random_index = rand::random::<usize>() % candidates.len();
        let mut instance_id = std::mem::take(&mut candidates[random_index]);
        if instance_id == my_instance_id && candidates.len() > 1 {
            // Prefer someone else instead of self
            let index = (random_index + 1) % candidates.len();
            instance_id = std::mem::take(&mut candidates[index]);
        }
        return Ok(instance_id);
    }
}

fn check_route_to_instance(
    node: &Node,
    ident: &PluginIdentifier,
    service: &str,
    instance_id: &InstanceId,
) -> Result<(), Error> {
    let res = node.storage.service_route_table.get_raw(&ServiceRouteKey {
        instance_id,
        plugin_name: &ident.name,
        plugin_version: &ident.version,
        service_name: service,
    })?;
    #[rustfmt::skip]
    let Some(tuple) = res else {
        if node.storage.instances.get_raw(instance_id).is_ok() {
            return Err(BoxError::new(ErrorCode::ServiceNotStarted, format!("service '{ident}.{service}' is not running on {instance_id}")).into());
        } else {
            return Err(BoxError::new(ErrorCode::NoSuchInstance, format!("instance with instance_id \"{instance_id}\" not found")).into());
        }
    };
    let res = tuple
        .field(ServiceRouteItem::FIELD_POISON)
        .map_err(IntoBoxError::into_box_error)?;
    let Some(is_poisoned) = res else {
        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::StorageCorrupted, "invalid contents has _pico_service_route").into());
    };
    if is_poisoned {
        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::ServicePoisoned, format!("service '{ident}.{service}' is poisoned on {instance_id}")).into());
    }
    Ok(())
}
