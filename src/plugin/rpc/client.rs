use crate::error_code::ErrorCode;
use crate::has_states;
use crate::instance::InstanceName;
use crate::plugin::{rpc, PluginIdentifier};
use crate::replicaset::Replicaset;
use crate::replicaset::ReplicasetState;
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
use tarantool::tuple::Tuple;
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

fn resolve_rpc_target(
    plugin: &PluginIdentifier,
    service: &str,
    target: &FfiSafeRpcTargetSpecifier,
    node: &Node,
) -> Result<InstanceName, Error> {
    use FfiSafeRpcTargetSpecifier as Target;

    let mut to_master_chosen = false;
    let mut tier_and_bucket_id = None;
    let mut by_replicaset_name = None;
    let mut tier_name_owned;
    match target {
        Target::InstanceName(name) => {
            //
            // Request to a specific instance, single candidate
            //
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            let instance_name = InstanceName::from(unsafe { name.as_str() });

            // A single instance was chosen
            check_route_to_instance(node, plugin, service, &instance_name)?;
            return Ok(instance_name);
        }

        &Target::Replicaset {
            replicaset_name,
            to_master,
        } => {
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            let name = unsafe { replicaset_name.as_str() };
            to_master_chosen = to_master;
            by_replicaset_name = Some(name);
        }
        &Target::BucketId {
            bucket_id,
            to_master,
        } => {
            tier_name_owned = node
                .raft_storage
                .tier()?
                .expect("storage for instance should exists");
            let tier = &*tier_name_owned;
            to_master_chosen = to_master;
            tier_and_bucket_id = Some((tier, bucket_id));
        }
        &Target::TierAndBucketId {
            to_master,
            tier,
            bucket_id,
        } => {
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            let tier = unsafe { tier.as_str() };
            to_master_chosen = to_master;
            tier_and_bucket_id = Some((tier, bucket_id));
        }
        Target::Any => {}
    }

    let mut by_bucket_id = None;
    let mut tier_and_replicaset_uuid = None;
    let mut replicaset_tuple = None;
    let mut replicaset_uuid_owned;
    if let Some((tier, bucket_id)) = tier_and_bucket_id {
        replicaset_uuid_owned = vshard::get_replicaset_uuid_by_bucket_id(tier, bucket_id)?;
        let uuid = &*replicaset_uuid_owned;
        by_bucket_id = Some((tier, bucket_id, uuid));
        tier_and_replicaset_uuid = Some((tier, uuid));
    }

    //
    // Request to replicaset master, single candidate
    //
    if to_master_chosen {
        if let Some(replicaset_name) = by_replicaset_name {
            let tuple = node.storage.replicasets.get_raw(replicaset_name)?;
            replicaset_tuple = Some(tuple);
        }

        if let Some((_, _, replicaset_uuid)) = by_bucket_id {
            let tuple = node.storage.replicasets.by_uuid_raw(replicaset_uuid)?;
            replicaset_tuple = Some(tuple);
        }

        let tuple = replicaset_tuple.expect("set in one of ifs above");
        let Some(master_name) = tuple
            .field(Replicaset::FIELD_TARGET_MASTER_NAME)
            .map_err(IntoBoxError::into_box_error)?
        else {
            #[rustfmt::skip]
            return Err(BoxError::new(ErrorCode::StorageCorrupted, "couldn't find 'target_master_name' field in _pico_replicaset tuple").into());
        };

        // A single instance was chosen
        check_route_to_instance(node, plugin, service, &master_name)?;
        return Ok(master_name);
    }

    if let Some(replicaset_name) = by_replicaset_name {
        let tuple = node.storage.replicasets.get_raw(replicaset_name)?;
        let Some(found_replicaset_uuid) = tuple
            .field(Replicaset::FIELD_REPLICASET_UUID)
            .map_err(IntoBoxError::into_box_error)?
        else {
            #[rustfmt::skip]
            return Err(BoxError::new(ErrorCode::StorageCorrupted, "couldn't find 'replicaset_uuid' field in _pico_replicaset tuple").into());
        };
        replicaset_uuid_owned = found_replicaset_uuid;

        let Some(found_tier) = tuple
            .field::<'_, String>(Replicaset::FIELD_TIER)
            .map_err(IntoBoxError::into_box_error)?
        else {
            #[rustfmt::skip]
            return Err(BoxError::new(ErrorCode::StorageCorrupted, "couldn't find 'tier' field in _pico_replicaset tuple").into());
        };
        tier_name_owned = found_tier;

        replicaset_tuple = Some(tuple);
        tier_and_replicaset_uuid = Some((&tier_name_owned, &replicaset_uuid_owned));
    }

    let my_instance_name = node
        .raft_storage
        .instance_name()?
        .expect("should be persisted at this point");

    // Get list of all possible targets
    let mut all_instances_with_service = node
        .storage
        .service_route_table
        .get_available_instances(plugin, service)?;

    // Get list of all possible targets
    filter_instances_by_state(node, &mut all_instances_with_service)?;

    #[rustfmt::skip]
    if all_instances_with_service.is_empty() {
        if node.storage.services.get(plugin, service)?.is_none() {
            return Err(BoxError::new(ErrorCode::NoSuchService, "service '{plugin}.{service}' not found").into());
        } else {
            return Err(BoxError::new(ErrorCode::ServiceNotStarted, format!("service '{plugin}.{service}' is not started on any instance")).into());
        }
    };

    //
    // Request to any replica in replicaset, multiple candidates
    //
    if let Some((tier, replicaset_uuid)) = tier_and_replicaset_uuid {
        // Need to pick a replica from given replicaset

        let replicas = vshard::get_replicaset_priority_list(tier, replicaset_uuid)?;

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

        check_replicaset_is_not_expelled(node, replicaset_uuid, replicaset_tuple)?;

        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::ServiceNotAvailable, format!("no {replicaset_uuid} replicas are available for service {plugin}.{service}")).into());
    }

    //
    // Request to any instance with given service, multiple candidates
    //
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

fn filter_instances_by_state(
    node: &Node,
    instance_names: &mut Vec<InstanceName>,
) -> Result<(), Error> {
    let mut index = 0;
    while index < instance_names.len() {
        let name = &instance_names[index];
        let instance = node.storage.instances.get(name)?;
        if has_states!(instance, Expelled -> *) || !instance.may_respond() {
            instance_names.swap_remove(index);
        } else {
            index += 1;
        }
    }

    Ok(())
}

fn check_route_to_instance(
    node: &Node,
    plugin: &PluginIdentifier,
    service: &str,
    instance_name: &InstanceName,
) -> Result<(), Error> {
    let instance = node.storage.instances.get(instance_name)?;
    if has_states!(instance, Expelled -> *) {
        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::InstanceExpelled, format!("instance named '{instance_name}' was expelled")).into());
    }

    if !instance.may_respond() {
        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::InstanceUnavaliable, format!("instance with instance_name \"{instance_name}\" can't respond due it's state"),).into());
    }
    let res = node.storage.service_route_table.get_raw(&ServiceRouteKey {
        instance_name,
        plugin_name: &plugin.name,
        plugin_version: &plugin.version,
        service_name: service,
    })?;
    #[rustfmt::skip]
    let Some(tuple) = res else {
        return Err(BoxError::new(ErrorCode::ServiceNotStarted, format!("service '{plugin}.{service}' is not running on {instance_name}")).into());
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
        return Err(BoxError::new(ErrorCode::ServicePoisoned, format!("service '{plugin}.{service}' is poisoned on {instance_name}")).into());
    }
    Ok(())
}

fn check_replicaset_is_not_expelled(
    node: &Node,
    uuid: &str,
    maybe_tuple: Option<Tuple>,
) -> Result<(), Error> {
    let tuple;
    if let Some(t) = maybe_tuple {
        tuple = t;
    } else {
        tuple = node.storage.replicasets.by_uuid_raw(uuid)?;
    }

    let state = tuple.field(Replicaset::FIELD_STATE)?;
    let state: ReplicasetState = state.expect("replicaset should always have a state column");

    if state == ReplicasetState::Expelled {
        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::ReplicasetExpelled, format!("replicaset with id {uuid} was expelled")).into());
    }

    Ok(())
}
