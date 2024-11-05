use crate::error_code::ErrorCode;
use crate::has_states;
use crate::instance::InstanceName;
use crate::plugin::{rpc, PluginIdentifier};
use crate::replicaset::Replicaset;
use crate::schema::ServiceRouteItem;
use crate::schema::ServiceRouteKey;
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::network::ConnectionPool;
use crate::traft::node::Node;
use crate::vshard;
use picodata_plugin::transport::context::ContextFieldId;
use picodata_plugin::transport::rpc::client::FfiSafeRpcTargetSpecifier;
use picodata_plugin::util::copy_to_region;
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

    let instance_name = resolve_rpc_target(plugin_identity, service, target, node)?;

    let my_instance_name = node
        .raft_storage
        .instance_name()?
        .expect("should be persisted at this point");

    if path.starts_with('.') {
        return call_builtin_stored_proc(pool, path, input, &instance_name, timeout);
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

    if instance_name == my_instance_name {
        let output = rpc::server::proc_rpc_dispatch_impl(args.as_ref().into())?;
        return process_rpc_output(output);
    };

    crate::error_injection!("RPC_NETWORK_ERROR" => return Err(Error::other("injected error")));
    tlog!(Debug, "sending plugin RPC request";
        "instance_name" => %instance_name,
        "request_id" => %request_id,
        "path" => path,
    );
    let future = pool.call_raw(
        &instance_name,
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
    instance_name: &InstanceName,
    timeout: Duration,
) -> Result<&'static [u8], Error> {
    // Call a builtin picodata stored procedure
    let Some(proc) = crate::rpc::to_static_proc_name(proc) else {
        #[rustfmt::skip]
        return Err(BoxError::new(TarantoolErrorCode::NoSuchFunction, format!("unknown static stored procedure {proc}")).into());
    };

    let args = RawBytes::new(input);

    let future = pool.call_raw(instance_name, proc, args, Some(timeout))?;
    // FIXME: remove this extra allocation for RawByteBuf
    let output: RawByteBuf = fiber::block_on(future)?;

    copy_to_region(&output).map_err(Into::into)
}

pub(crate) fn encode_request_arguments(
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

fn get_instance_name_of_master_of_replicaset(
    tier_name: &str,
    bucket_id: u64,
    node: &Node,
) -> Result<InstanceName, Error> {
    let replicaset_uuid = get_replicaset_uuid_by_bucket_id(tier_name, bucket_id, node)?;
    let tuple = node.storage.replicasets.by_uuid_raw(&replicaset_uuid)?;
    let Some(master_name) = tuple
        .field(Replicaset::FIELD_TARGET_MASTER_NAME)
        .map_err(IntoBoxError::into_box_error)?
    else {
        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::StorageCorrupted, "couldn't find 'target_master_name' field in _pico_replicaset tuple").into());
    };

    Ok(master_name)
}

fn resolve_rpc_target(
    ident: &PluginIdentifier,
    service: &str,
    target: &FfiSafeRpcTargetSpecifier,
    node: &Node,
) -> Result<InstanceName, Error> {
    use FfiSafeRpcTargetSpecifier as Target;

    let mut instance_name = None;
    match target {
        Target::InstanceName(iid) => {
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            instance_name = Some(InstanceName::from(unsafe { iid.as_str() }));
        }

        &Target::Replicaset {
            replicaset_name,
            to_master: true,
        } => {
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            let replicaset_name = unsafe { replicaset_name.as_str() };
            let tuple = node.storage.replicasets.get_raw(replicaset_name)?;
            let Some(master_name) = tuple
                .field(Replicaset::FIELD_TARGET_MASTER_NAME)
                .map_err(IntoBoxError::into_box_error)?
            else {
                #[rustfmt::skip]
                return Err(BoxError::new(ErrorCode::StorageCorrupted, "couldn't find 'target_master_name' field in _pico_replicaset tuple").into());
            };
            instance_name = Some(master_name);
        }

        &Target::BucketId {
            bucket_id,
            to_master: true,
        } => {
            let current_instance_tier = node
                .raft_storage
                .tier()?
                .expect("storage for instance should exists");

            let master_name =
                get_instance_name_of_master_of_replicaset(&current_instance_tier, bucket_id, node)?;
            instance_name = Some(master_name);
        }

        &Target::TierAndBucketId {
            to_master: true,
            tier,
            bucket_id,
        } => {
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            let tier = unsafe { tier.as_str() };
            let master_name = get_instance_name_of_master_of_replicaset(tier, bucket_id, node)?;
            instance_name = Some(master_name);
        }

        // These cases are handled below
        #[rustfmt::skip]
        Target::BucketId { to_master: false, .. }
        | Target::TierAndBucketId { to_master: false, .. }
        | Target::Replicaset { to_master: false, .. }
        | Target::Any => {}
    }

    if let Some(instance_name) = instance_name {
        // A single instance was chosen
        check_route_to_instance(node, ident, service, &instance_name)?;
        return Ok(instance_name);
    } else {
        // Need to pick an instance from a group, fallthrough
    }

    let my_instance_name = node
        .raft_storage
        .instance_name()?
        .expect("should be persisted at this point");

    let mut all_instances_with_service = node
        .storage
        .service_route_table
        .get_available_instances(ident, service)?;
    #[rustfmt::skip]
    if all_instances_with_service.is_empty() {
        if node.storage.services.get(ident, service)?.is_none() {
            return Err(BoxError::new(ErrorCode::NoSuchService, "service '{plugin}.{service}' not found").into());
        } else {
            return Err(BoxError::new(ErrorCode::ServiceNotStarted, format!("service '{ident}.{service}' is not started on any instance")).into());
        }
    };

    remove_expelled_instances(node, &mut all_instances_with_service)?;

    let mut tier_and_replicaset_uuid = None;

    match target {
        #[rustfmt::skip]
        Target::InstanceName { .. }
        | Target::Replicaset { to_master: true, .. }
        | Target::TierAndBucketId { to_master: true, .. }
        | Target::BucketId { to_master: true, .. } => unreachable!("handled above"),

        &Target::Replicaset {
            replicaset_name,
            to_master: false,
        } => {
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            let replicaset_name = unsafe { replicaset_name.as_str() };
            let tuple = node.storage.replicasets.get_raw(replicaset_name)?;
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

        &Target::BucketId {
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

        &Target::TierAndBucketId {
            tier,
            bucket_id,
            to_master: false,
        } => {
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            let tier = unsafe { tier.as_str().to_string() };
            let replicaset_uuid = get_replicaset_uuid_by_bucket_id(&tier, bucket_id, node)?;
            tier_and_replicaset_uuid = Some((tier, replicaset_uuid));
        }

        &Target::Any => {}
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

        let mut skipped_self = false;

        // XXX: this shouldn't be a problem if replicasets aren't too big,
        // but if they are we might want to construct a HashSet from candidates
        for instance_name in replicas {
            if my_instance_name == instance_name {
                // Prefer someone else instead of self
                skipped_self = true;
                continue;
            }
            if all_instances_with_service.contains(&instance_name) {
                return Ok(instance_name);
            }
        }

        // In case there's no other suitable candidates, fallback to calling self
        if skipped_self && all_instances_with_service.contains(&my_instance_name) {
            return Ok(my_instance_name);
        }

        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::ServiceNotAvailable, format!("no {replicaset_uuid} replicas are available for service {ident}.{service}")).into());
    } else {
        // Need to pick any instance with the given plugin.service
        let mut candidates = all_instances_with_service;

        // TODO: find a better strategy then just the random one
        let random_index = rand::random::<usize>() % candidates.len();
        let mut instance_name = std::mem::take(&mut candidates[random_index]);
        if instance_name == my_instance_name && candidates.len() > 1 {
            // Prefer someone else instead of self
            let index = (random_index + 1) % candidates.len();
            instance_name = std::mem::take(&mut candidates[index]);
        }
        return Ok(instance_name);
    }
}

fn remove_expelled_instances(
    node: &Node,
    instance_names: &mut Vec<InstanceName>,
) -> Result<(), Error> {
    let mut index = 0;
    while index < instance_names.len() {
        let name = &instance_names[index];
        let instance = node.storage.instances.get(name)?;
        if has_states!(instance, Expelled -> *) {
            instance_names.swap_remove(index);
        } else {
            index += 1;
        }
    }

    Ok(())
}

fn check_route_to_instance(
    node: &Node,
    ident: &PluginIdentifier,
    service: &str,
    instance_name: &InstanceName,
) -> Result<(), Error> {
    let instance = node.storage.instances.get(instance_name)?;
    if has_states!(instance, Expelled -> *) {
        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::InstanceExpelled, format!("instance named '{instance_name}' was expelled")).into());
    }

    let res = node.storage.service_route_table.get_raw(&ServiceRouteKey {
        instance_name,
        plugin_name: &ident.name,
        plugin_version: &ident.version,
        service_name: service,
    })?;
    #[rustfmt::skip]
    let Some(tuple) = res else {
        return Err(BoxError::new(ErrorCode::ServiceNotStarted, format!("service '{ident}.{service}' is not running on {instance_name}")).into());
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
        return Err(BoxError::new(ErrorCode::ServicePoisoned, format!("service '{ident}.{service}' is poisoned on {instance_name}")).into());
    }
    Ok(())
}
