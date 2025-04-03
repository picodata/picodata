use crate::error_code::ErrorCode;
use crate::has_states;
use crate::instance::InstanceName;
use crate::plugin::{rpc, PluginIdentifier};
use crate::replicaset::ReplicasetState;
use crate::tlog;
use crate::topology_cache::ServiceRouteCheck;
use crate::topology_cache::TopologyCache;
use crate::topology_cache::TopologyCacheRef;
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
    let topology = &node.topology_cache;

    let timeout = Duration::from_secs_f64(timeout);

    let instance_name = resolve_rpc_target(plugin_identity, service, target, node, topology)?;

    let my_instance_name = topology.my_instance_name();
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
    topology: &TopologyCache,
) -> Result<InstanceName, Error> {
    use FfiSafeRpcTargetSpecifier as Target;

    let mut to_master_chosen = false;
    let mut tier_and_bucket_id = None;
    let mut by_replicaset_name = None;
    match target {
        Target::InstanceName(name) => {
            //
            // Request to a specific instance, single candidate
            //
            // SAFETY: it's required that argument pointers are valid for the lifetime of this function's call
            let instance_name = unsafe { name.as_str() };

            // A single instance was chosen
            check_route_to_instance(topology, plugin, service, instance_name)?;
            return Ok(instance_name.into());
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
            let tier = topology.my_tier_name();
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
    let replicaset_uuid_owned;
    if let Some((tier_name, bucket_id)) = tier_and_bucket_id {
        // This call must be done here, because this function may yield
        // but later we take a volatile reference to TopologyCache which can't be held across yields
        replicaset_uuid_owned = vshard::get_replicaset_uuid_by_bucket_id(tier_name, bucket_id)?;
        let uuid = &*replicaset_uuid_owned;
        by_bucket_id = Some((tier_name, bucket_id, uuid));
        tier_and_replicaset_uuid = Some((tier_name, uuid));
    }

    let topology_ref = topology.get();

    //
    // Request to replicaset master, single candidate
    //
    if to_master_chosen {
        let mut target_master_name = None;

        if let Some(replicaset_name) = by_replicaset_name {
            let replicaset = topology_ref.replicaset_by_name(replicaset_name)?;
            target_master_name = Some(&replicaset.target_master_name);
        }

        if let Some((_, _, replicaset_uuid)) = by_bucket_id {
            let replicaset = topology_ref.replicaset_by_uuid(replicaset_uuid)?;
            target_master_name = Some(&replicaset.target_master_name);
        }

        let instance_name = target_master_name.expect("set in one of ifs above");

        // A single instance was chosen
        check_route_to_instance(topology, plugin, service, instance_name)?;
        return Ok(instance_name.clone());
    }

    if let Some(replicaset_name) = by_replicaset_name {
        let replicaset = topology_ref.replicaset_by_name(replicaset_name)?;
        tier_and_replicaset_uuid = Some((&replicaset.tier, &replicaset.uuid));
    }

    // Get list of all possible targets
    let mut all_instances_with_service: Vec<_> = topology_ref
        .instances_running_service(&plugin.name, &plugin.version, service)
        .collect();

    // Remove non-online instances
    filter_instances_by_state(&topology_ref, &mut all_instances_with_service)?;

    #[rustfmt::skip]
    if all_instances_with_service.is_empty() {
        // TODO: put service definitions into TopologyCache as well
        if node.storage.services.get(plugin, service)?.is_none() {
            return Err(BoxError::new(ErrorCode::NoSuchService, format!("service '{plugin}.{service}' not found")).into());
        } else {
            return Err(BoxError::new(ErrorCode::ServiceNotStarted, format!("service '{plugin}.{service}' is not started on any instance")).into());
        }
    };

    let my_instance_name = topology.my_instance_name();

    //
    // Request to any replica in replicaset, multiple candidates
    //
    if let Some((tier, replicaset_uuid)) = tier_and_replicaset_uuid {
        let replicaset = topology_ref.replicaset_by_uuid(replicaset_uuid)?;
        if replicaset.state == ReplicasetState::Expelled {
            #[rustfmt::skip]
            return Err(BoxError::new(ErrorCode::ReplicasetExpelled, format!("replicaset with id {replicaset_uuid} was expelled")).into());
        }

        // Need to pick a replica from given replicaset

        let replicas = vshard::get_replicaset_priority_list(tier, replicaset_uuid)?;

        let mut skipped_self = false;

        // XXX: this shouldn't be a problem if replicasets aren't too big,
        // but if they are we might want to construct a HashSet from candidates
        for instance_name in replicas {
            if my_instance_name == &*instance_name {
                // Prefer someone else instead of self
                skipped_self = true;
                continue;
            }
            if all_instances_with_service.contains(&&*instance_name) {
                return Ok(instance_name);
            }
        }

        // In case there's no other suitable candidates, fallback to calling self
        if skipped_self && all_instances_with_service.contains(&my_instance_name) {
            return Ok(my_instance_name.into());
        }

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
    return Ok(instance_name.into());
}

fn filter_instances_by_state(
    topology_ref: &TopologyCacheRef,
    instance_names: &mut Vec<&str>,
) -> Result<(), Error> {
    let mut index = 0;
    while index < instance_names.len() {
        let name = &instance_names[index];
        let instance = topology_ref.instance_by_name(name)?;
        if has_states!(instance, Expelled -> *) || !instance.may_respond() {
            instance_names.swap_remove(index);
        } else {
            index += 1;
        }
    }

    Ok(())
}

fn check_route_to_instance(
    topology: &TopologyCache,
    plugin: &PluginIdentifier,
    service: &str,
    instance_name: &str,
) -> Result<(), Error> {
    let topology_ref = topology.get();
    let instance = topology_ref.instance_by_name(instance_name)?;
    if has_states!(instance, Expelled -> *) {
        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::InstanceExpelled, format!("instance named '{instance_name}' was expelled")).into());
    }
    if !instance.may_respond() {
        #[rustfmt::skip]
        return Err(BoxError::new(ErrorCode::InstanceUnavaliable, format!("instance with instance_name \"{instance_name}\" can't respond due it's state"),).into());
    }

    let check =
        topology_ref.check_service_route(&plugin.name, &plugin.version, service, instance_name);

    match check {
        ServiceRouteCheck::Ok => {}
        ServiceRouteCheck::RoutePoisoned => {
            #[rustfmt::skip]
            return Err(BoxError::new(ErrorCode::ServicePoisoned, format!("service '{plugin}.{service}' is poisoned on {instance_name}")).into());
        }
        ServiceRouteCheck::ServiceNotEnabled => {
            #[rustfmt::skip]
            return Err(BoxError::new(ErrorCode::ServiceNotStarted, format!("service '{plugin}.{service}' is not running on {instance_name}")).into());
        }
    }

    Ok(())
}
