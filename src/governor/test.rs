#![allow(clippy::inconsistent_digit_grouping)]
use crate::cas;
use crate::governor::batch::LastStepInfo;
use crate::governor::ActionKind;
use crate::governor::GovernorStatus;
use crate::governor::IterationEnd;
use crate::governor::Loop;
use crate::governor::State;
use crate::instance;
use crate::instance::tests::check_field_assignment;
use crate::instance::Instance;
use crate::instance::InstanceName;
use crate::instance::StateVariant;
use crate::plugin::Manifest;
use crate::plugin::PluginIdentifier;
use crate::plugin::PluginOp;
use crate::replicaset::Replicaset;
use crate::replicaset::ReplicasetState;
use crate::rpc;
use crate::rpc::replicasets_masters;
use crate::schema::Distribution;
use crate::schema::PluginDef;
use crate::schema::ServiceDef;
use crate::schema::ServiceRouteItem;
use crate::schema::ServiceRouteKey;
use crate::schema::TableDef;
use crate::schema::ADMIN_ID;
use crate::storage::Plugins;
use crate::storage::Properties;
use crate::storage::PropertyName;
use crate::storage::SystemTable;
use crate::tier::Tier;
use crate::tlog;
use crate::topology_cache::TopologyCacheRef;
use crate::traft::error::Error;
use crate::traft::network;
use crate::traft::network::RaftIdOrName;
use crate::traft::node;
use crate::traft::op::Ddl;
use crate::traft::op::Dml;
use crate::traft::op::Op;
use crate::traft::op::PluginRaftOp;
use crate::traft::RaftId;
use ::tarantool::fiber::r#async::watch;
use raft::prelude::ConfState;
use rand::Rng;
use rand::SeedableRng;
use smol_str::format_smolstr;
use smol_str::SmolStr;
use std::cell::Cell;
use std::collections::HashSet;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;
use tarantool::fiber;
use tarantool::tuple::Tuple;

////////////////////////////////////////////////////////////////////////////////
// test setup functions
////////////////////////////////////////////////////////////////////////////////

/// Spawn a pretend governor_loop fiber.
///
/// The fiber runs the govenor loop iteration function, and after each iteration:
/// - checks and logs the statuses
/// - handles requests to sleep (by adjusting timings in `last_step_info`)
/// - guards against inifinite looping
/// - stops once governor performs all pending actions and goes idle
///
/// NOTE: it must be in a separate fiber, because `Loop::governor_loop` blocks
/// when sending RPCs and CAS requests. These requests must be handled via
/// override channels in the calling code.
fn spawn_pretend_governor_loop(mut state: State) -> Rc<Cell<PretendGovernorLoopState>> {
    const MAX_SAFE_ITERATIONS: usize = 1000;

    let pretend_state = Rc::new(Cell::new(PretendGovernorLoopState::default()));

    fiber::Builder::new()
        .name("governor_loop")
        .func_async({
            let pretend_state = pretend_state.clone();

            async move {
                loop {
                    // Call the govenor_loop iteration function
                    let res = Loop::governor_loop(&mut state).await;
                    tlog!(Info, "{res:?}");

                    let mut s = pretend_state.get();
                    s.num_iterations += 1;
                    // Guard against bugs
                    if s.num_iterations > MAX_SAFE_ITERATIONS {
                        panic!("infinite loop detected");
                    }

                    // Report the status to logs for debugging
                    let status = state.governor_status.subscribe().get_cloned();
                    tlog!(
                        Info,
                        "num_iterations: {}, status: {status:?}",
                        s.num_iterations
                    );
                    if let Some(last_error) = status.last_error {
                        tlog!(Error, "last governor error: {last_error:?}");
                    }

                    // Pretend to sleep if needed
                    if let IterationEnd::Sleep(timeout) = res {
                        // SAFETY: this function is only for tests
                        unsafe {
                            state
                                .last_step_info
                                .nudge_last_try_instants_backwards(timeout);
                        }

                        s.num_backoff_sleeps += 1;
                        s.backoff_sleep_total += timeout;
                    }

                    // Break out of the loop if governor is finished
                    if status.last_step_kind == Some(ActionKind::GoIdle) {
                        s.governor_idle = true;
                        pretend_state.set(s);
                        break;
                    }

                    // Don't forget to update the shared state
                    pretend_state.set(s);
                }
            }
        })
        .start_non_joinable()
        .unwrap();

    pretend_state
}

/// A helper struct with info about the pretend governor_loop inner goings-on
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PretendGovernorLoopState {
    governor_idle: bool,
    num_iterations: usize,
    num_backoff_sleeps: usize,
    backoff_sleep_total: Duration,
}

fn setup_topology(
    node: &node::Node,
    instances: &[Instance],
    replicasets: Option<&[Replicaset]>,
    tiers: Option<&[Tier]>,
) {
    //
    // Setup _pico_instance
    //

    let mut voters = vec![];
    let mut learners = vec![];
    for instance in instances {
        if voters.len() < 5 {
            voters.push(instance.raft_id);
        } else {
            learners.push(instance.raft_id);
        }
        node.storage.instances.put(instance).unwrap();
        node.topology_cache
            .update_instance(None, Some(instance.clone()));
    }

    //
    // Setup _pico_replicaset
    //

    if let Some(replicasets) = replicasets {
        // Callers wants to control the replicaset setup
        for replicaset in replicasets {
            node.storage.replicasets.put(replicaset).unwrap();
            node.topology_cache
                .update_replicaset(None, Some(replicaset.clone()));
        }
    } else {
        // Caller wants automatic replicaset setup
        // TODO: check replicaset_name-s in instances, create one for each
        let mut replicaset = Replicaset::with_one_instance(&instances[0]);
        replicaset.state = ReplicasetState::Ready;

        node.storage.replicasets.put(&replicaset).unwrap();
        node.topology_cache
            .update_replicaset(None, Some(replicaset));
    }

    //
    // Setup _pico_tier
    //

    if let Some(tiers) = tiers {
        // Callers want to control the tier setup
        for tier in tiers {
            node.storage.tiers.put(tier).unwrap();
            node.topology_cache.update_tier(None, Some(tier.clone()));
        }
    } else {
        // Caller wants automatic tier setup
        // TODO: check replicaset_name-s in instances, create one for each
        let mut tier = Tier::default();
        tier.name = instances[0].tier.clone();

        node.storage.tiers.put(&tier).unwrap();
        node.topology_cache.update_tier(None, Some(tier));
    }

    //
    // Setup _raft_state
    //

    let conf_state = ConfState {
        voters,
        learners,
        ..Default::default()
    };
    tarantool::transaction::transaction(|| node.raft_storage.persist_conf_state(&conf_state))
        .unwrap();
}

fn setup_governor_loop_state(
    node: &node::Node,
) -> (
    State,
    watch::Receiver<GovernorStatus>,
    watch::Sender<node::Status>,
    network::OverrideChannel,
    cas::OverrideChannel,
) {
    let (_, waker_rx) = watch::channel(());
    let (governor_status_tx, governor_status_rx) =
        watch::channel(GovernorStatus::new("initializing"));
    let (raft_status_tx, raft_status_rx) = watch::channel(node::Status::for_tests());
    let pool = node.pool.clone();
    let rpc_requests = pool.test_override.clone().unwrap();
    let cas_requests = node.cas_test_override.clone().unwrap();

    let state = State {
        governor_status: governor_status_tx,
        storage: node.storage.clone(),
        raft_storage: node.raft_storage.clone(),
        raft_status: raft_status_rx,
        waker: waker_rx,
        pool,
        last_step_info: LastStepInfo::new(),
    };

    (
        state,
        governor_status_rx,
        raft_status_tx,
        rpc_requests,
        cas_requests,
    )
}

fn generate_replicasets(
    next_raft_id: RaftId,
    num_instances: usize,
    replication_factor: usize,
    tier_name: Option<&SmolStr>,
) -> (Vec<Instance>, Vec<Replicaset>) {
    let mut instances = vec![];
    let mut replicasets = vec![];
    let mut last_replicaset_size = 0;
    let tier_name = tier_name
        .cloned()
        .unwrap_or_else(|| Instance::for_tests().tier);

    for i in 0..(num_instances as RaftId) {
        let mut instance = Instance::for_tests();
        instance.raft_id = next_raft_id + i;
        instance.tier = tier_name.clone();
        instance.uuid = uuid_v4();

        if replicasets.is_empty() || last_replicaset_size == replication_factor {
            let tier_name = &instance.tier;
            let n = replicasets.len() + 1;
            let replicaset_name = format_smolstr!("{tier_name}_{n}");
            instance.name = format_smolstr!("{replicaset_name}_{last_replicaset_size}").into();
            instance.replicaset_name = replicaset_name.into();
            instance.replicaset_uuid = uuid_v4();

            let mut replicaset = Replicaset::with_one_instance(&instance);
            replicaset.state = ReplicasetState::Ready;
            replicasets.push(replicaset);
            last_replicaset_size = 1;
        } else {
            let replicaset = replicasets.last().unwrap();
            let replicaset_name = &replicaset.name;
            instance.name = format_smolstr!("{replicaset_name}_{last_replicaset_size}").into();
            instance.replicaset_name = replicaset.name.clone();
            instance.replicaset_uuid = replicaset.uuid.clone();

            last_replicaset_size += 1;
        }

        instances.push(instance);
    }

    (instances, replicasets)
}

fn uuid_v4() -> SmolStr {
    format_smolstr!("{}", uuid::Uuid::new_v4().to_hyphenated())
}

fn dummy_instance(raft_id: u64) -> Instance {
    let mut instance = Instance::for_tests();
    instance.raft_id = raft_id;
    instance.name = InstanceName(format_smolstr!("default_1_{raft_id}"));
    instance.uuid = uuid_v4();
    instance
}

fn masters_and_replicas(
    topology_ref: &TopologyCacheRef,
) -> (HashSet<InstanceName>, HashSet<InstanceName>) {
    let mut masters = HashSet::new();
    let mut replicas = HashSet::new();

    for instance in topology_ref.all_instances() {
        let replicaset = topology_ref
            .replicaset_by_uuid(&instance.replicaset_uuid)
            .unwrap();
        if replicaset.current_master_name == instance.name {
            masters.insert(instance.name.clone());
        } else {
            replicas.insert(instance.name.clone());
        }
    }

    (masters, replicas)
}

#[derive(Default, Debug, Clone, Copy, PartialEq)]
struct BatchingRpcTestParameters {
    /// seed for the random number generator
    seed: u64,
    /// number of instances in the cluster
    num_instances: usize,
    /// value for the `governor_rpc_batch_size`
    batch_size: usize,
    /// probability of a given RPC request failing
    p_rpc_err: f64,
    /// probability of a given CAS request failing
    p_cas_err: f64,
    replication_factor: usize,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BatchingRpcTestStats {
    /// Number of RPC batches observed
    num_batches: u64,
    /// Number of RPC requests failed
    num_rpc_fails: u64,
    /// Number of CAS requests failed
    num_cas_fails: u64,
}

////////////////////////////////////////////////////////////////////////////////
// test cases
////////////////////////////////////////////////////////////////////////////////

//
// test proc_before_online step
//

#[tarantool::test]
fn test_governor_loop_proc_before_online_batching_fixed() {
    // Run with a fixed seed each time for some stability
    let seed = 1770229753053178;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 100,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.1,
        ..Default::default()
    };
    do_governor_loop_proc_before_online_batching(params);
}

#[tarantool::test]
fn test_governor_loop_proc_before_online_batching_random() {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    // And also run with a random seed for chaos
    let seed = (time.as_secs_f64() * 1000_000.0) as u64;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 100,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.1,
        ..Default::default()
    };

    do_governor_loop_proc_before_online_batching(params);
}

fn do_governor_loop_proc_before_online_batching(params: BatchingRpcTestParameters) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(params.seed);
    tlog!(Info, "random seed: {}", params.seed);

    let node = node::Node::for_tests();

    //
    // Setup cluster topology
    //

    let mut instances = vec![];
    for raft_id in 1..=(params.num_instances as RaftId) {
        let mut instance = dummy_instance(raft_id);
        instance.current_state.variant = StateVariant::Offline;
        instances.push(instance);
    }

    setup_topology(node, &instances, None, None);

    //
    // Setup other things
    //

    node.alter_system_parameters
        .borrow_mut()
        .governor_rpc_batch_size = params.batch_size;

    let (state, _, _, rpc_requests, cas_requests) = setup_governor_loop_state(node);

    //
    // Spawn the pretend governor_loop fiber
    //
    let pretend_state = spawn_pretend_governor_loop(state);

    //
    // Loop until pretend governor_loop finishes, handle governor's RPC & CAS requests
    //

    let mut ok_so_far = HashSet::new();
    let mut stats = BatchingRpcTestStats::default();

    while !pretend_state.get().governor_idle {
        //
        // Handle governor's RPCs
        //

        let rpcs = rpc_requests.receive_all(Duration::from_millis(50));

        stats.num_batches += 1;
        tlog!(
            Info,
            "RPC batch #{} size: {}",
            stats.num_batches,
            rpcs.len()
        );

        let mut targets = vec![];
        let mut expected_ok_targets = HashSet::new();

        for rpc in rpcs {
            // Verify RPC request
            let (id, request) = rpc;
            let RaftIdOrName::Name(name) = id else {
                panic!("expected instance name, got {id:?}");
            };
            targets.push(name.clone());

            assert_eq!(request.proc, ".proc_enable_all_plugins");
            let resp = rpc::before_online::Response {};

            // Send the RPC response to governor
            let fail = rng.gen_bool(params.p_rpc_err);
            if fail {
                request
                    .on_result
                    .handle(Err(Error::other("injected error")));
                stats.num_rpc_fails += 1;

                continue;
            }

            request.on_result.handle(Ok(Tuple::new(&[resp]).unwrap()));

            // Remember which RPCs got an Ok response
            expected_ok_targets.insert(name);
        }

        // RPCs are sent out in batches of given size (may be shorter)
        assert!(targets.len() <= params.batch_size, "{targets:?}");

        // Allow governor to handle RPC results and send the CAS requests
        fiber::reschedule();

        //
        // Handle governor's CAS requests
        //

        let mut actual_ok_targets = HashSet::new();

        let cas_requests = cas_requests.try_receive_all();
        tlog!(Info, "CAS batch size: {}", cas_requests.len());

        // Parse CAS requests
        for (request, response) in cas_requests {
            let dmls = request.op.dmls().unwrap();
            for dml in dmls {
                let Dml::Update {
                    table,
                    key,
                    ops,
                    initiator,
                } = dml
                else {
                    panic!("expected Dml::Update, got {dml:?}");
                };

                assert_eq!(*table, crate::storage::Instances::TABLE_ID);
                assert_eq!(*initiator, ADMIN_ID);

                let [name]: [InstanceName; 1] = rmp_serde::from_slice(key.as_ref()).unwrap();
                actual_ok_targets.insert(name);

                assert_eq!(ops.len(), 1);
                let current_state: instance::State =
                    check_field_assignment(&ops[0], "current_state");
                assert_eq!(current_state.variant, StateVariant::Online);
            }

            tlog!(Info, "cas_request: {request:?}");
            response.send(Ok(cas::Response::for_tests())).unwrap();
        }

        // Make sure all instances which responded Ok get a corresponding CAS request
        assert_eq!(expected_ok_targets, actual_ok_targets);

        // Apply CAS requests
        let fail = rng.gen_bool(params.p_cas_err);
        if !fail {
            for name in actual_ok_targets {
                let old_instance = node.topology_cache.clone_instance_by_name(&name).unwrap();
                let mut new_instance = old_instance.clone();
                new_instance.current_state.variant = StateVariant::Online;
                node.topology_cache
                    .update_instance(Some(old_instance), Some(new_instance));

                assert!(
                    !ok_so_far.contains(&name),
                    "got a repeat CAS request for instance {name}"
                );
                ok_so_far.insert(name);
            }
        } else {
            // It's possible that the CAS request get's dropped due to
            // a conflict. Governor should handle this fine
            stats.num_cas_fails += 1;
        }
    }

    // Make sure governor did something before going idle
    assert_ne!(pretend_state.get().num_iterations, 1);

    // Make sure all instances were handled
    if ok_so_far.len() != params.num_instances {
        let mut missing = vec![];
        let topology_ref = node.topology_cache.get();
        for instance in topology_ref.all_instances() {
            if !ok_so_far.contains(&instance.name) {
                missing.push(instance.clone());
            }
        }

        assert_eq!(missing, vec![]);
    }

    // Log the stats
    let s = pretend_state.get();
    tlog!(Info, "stats: {params:#0.3?}, {s:#0.3?}, {stats:#0.3?}")
}

//
// test proc_sharding step
//

#[tarantool::test]
fn test_governor_loop_proc_sharding_batching_fixed() {
    // Run with a fixed seed each time for some stability
    let seed = 1770370183368619;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 100,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.8,
        ..Default::default()
    };
    do_governor_loop_proc_sharding_batching(params);
}

#[tarantool::test]
fn test_governor_loop_proc_sharding_batching_random() {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    // And also run with a random seed for chaos
    let seed = (time.as_secs_f64() * 1000_000.0) as u64;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 100,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.8,
        ..Default::default()
    };
    do_governor_loop_proc_sharding_batching(params);
}

fn do_governor_loop_proc_sharding_batching(params: BatchingRpcTestParameters) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(params.seed);
    tlog!(Info, "random seed: {}", params.seed);

    let node = node::Node::for_tests();

    //
    // Setup cluster topology
    //

    let mut instances = vec![];
    for raft_id in 1..=(params.num_instances as RaftId) {
        instances.push(dummy_instance(raft_id));
    }

    let mut replicaset = Replicaset::with_one_instance(&instances[0]);
    replicaset.state = ReplicasetState::Ready;

    let mut tier = Tier::default();
    tier.name = instances[0].tier.clone();
    tier.vshard_bootstrapped = true;
    tier.target_vshard_config_version = tier.current_vshard_config_version + 1;

    setup_topology(node, &instances, Some(&[replicaset]), Some(&[tier.clone()]));

    //
    // Setup other things
    //

    node.alter_system_parameters
        .borrow_mut()
        .governor_rpc_batch_size = params.batch_size;

    let (state, _, _, rpc_requests, cas_requests) = setup_governor_loop_state(node);

    //
    // Spawn the pretend governor_loop fiber
    //
    let pretend_state = spawn_pretend_governor_loop(state);

    //
    // Loop until pretend governor_loop finishes, handle governor's RPC & CAS requests
    //

    let mut ok_so_far = HashSet::new();
    let mut first_cas_request_skipped = false;
    let mut cas_request_handled = false;
    let mut stats = BatchingRpcTestStats::default();

    while !pretend_state.get().governor_idle {
        //
        // Handle governor's RPCs
        //

        let rpcs = rpc_requests.receive_all(Duration::from_millis(50));

        stats.num_batches += 1;
        tlog!(
            Info,
            "RPC batch #{} size: {}",
            stats.num_batches,
            rpcs.len()
        );

        let mut targets = vec![];

        for rpc in rpcs {
            // Verify RPC request
            let (id, request) = rpc;
            let RaftIdOrName::Name(name) = id else {
                panic!("expected instance name, got {id:?}");
            };
            targets.push(name.clone());

            assert_eq!(request.proc, ".proc_sharding");
            let resp = rpc::sharding::Response {};

            // Send the RPC response to governor
            let fail = rng.gen_bool(params.p_rpc_err);
            if fail {
                request
                    .on_result
                    .handle(Err(Error::other("injected error")));
                stats.num_rpc_fails += 1;

                continue;
            }

            request.on_result.handle(Ok(Tuple::new(&[resp]).unwrap()));

            // Remember which RPCs got an Ok response
            assert!(
                !ok_so_far.contains(&name),
                "{name} got a repeat RPC after Ok response"
            );
            ok_so_far.insert(name);
        }

        // RPCs are sent out in batches of given size (may be shorter)
        assert!(targets.len() <= params.batch_size, "{targets:?}");

        // Allow governor to handle RPC results and send the CAS requests
        fiber::reschedule();

        //
        // Handle governor's CAS requests
        //

        let cas_requests = cas_requests.try_receive_all();
        tlog!(Info, "CAS batch size: {}", cas_requests.len());

        if !cas_requests.is_empty() {
            // Make sure CAS request is only sent once every instance handled the RPC
            assert_eq!(ok_so_far.len(), params.num_instances);

            // Verify CAS request
            let tier = node
                .topology_cache
                .get()
                .tier_by_name(&tier.name)
                .unwrap()
                .clone();
            assert_ne!(
                tier.current_vshard_config_version,
                tier.target_vshard_config_version
            );

            let mut cas_requests = cas_requests.into_iter();
            let (request, response) = cas_requests.next().unwrap();

            let dmls = request.op.dmls().unwrap();
            assert_eq!(dmls.len(), 1);
            let dml = &dmls[0];

            let Dml::Update {
                table,
                key,
                ops,
                initiator,
            } = dml
            else {
                panic!("expected Dml::Update, got {dml:?}");
            };

            assert_eq!(*table, crate::storage::Tiers::TABLE_ID);
            assert_eq!(*initiator, ADMIN_ID);

            let [tier_name]: [SmolStr; 1] = rmp_serde::from_slice(key.as_ref()).unwrap();
            assert_eq!(tier_name, tier.name);

            let mut ops = ops.iter();
            let op = ops.next().unwrap();
            let current_vshard_config_version: u64 =
                check_field_assignment(op, "current_vshard_config_version");
            assert_eq!(
                current_vshard_config_version,
                tier.target_vshard_config_version
            );
            assert_eq!(ops.next(), None);

            tlog!(Info, "cas_request: {request:?}");

            // Apply CAS request
            let fail = rng.gen_bool(params.p_cas_err);
            if
            // Fail the first attempt always just to check what how we handle that
            first_cas_request_skipped
                    // Next time fail randomly
                    && !fail
            {
                response.send(Ok(cas::Response::for_tests())).unwrap();

                let mut new_tier = tier.clone();
                new_tier.current_vshard_config_version = current_vshard_config_version;
                node.topology_cache.update_tier(Some(tier), Some(new_tier));
                cas_request_handled = true;
            } else {
                // It's possible that the CAS request get's dropped due to
                // a conflict. Governor should handle this fine
                response.send(Err(Error::other("injected error"))).unwrap();
                stats.num_cas_fails += 1;
                first_cas_request_skipped = true;
            }

            // Expecting only 1 CAS request
            assert_eq!(cas_requests.next().map(|(r, _)| r), None);
        }
    }

    assert!(cas_request_handled);

    // Make sure governor did something before going idle
    assert_ne!(pretend_state.get().num_iterations, 1);

    // Log the stats
    let s = pretend_state.get();
    tlog!(Info, "stats: {params:#0.3?}, {s:#0.3?}, {stats:#0.3?}")
}

//
// test DDL step
//

#[tarantool::test]
fn test_governor_loop_proc_apply_schema_change_batching_fixed() {
    // Run with a fixed seed each time for some stability
    let seed = 1770229753053178;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 300,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.1,
        replication_factor: 3,
    };

    do_governor_loop_proc_apply_schema_change_batching(params);
}

#[tarantool::test]
fn test_governor_loop_proc_apply_schema_change_batching_random() {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    // And also run with a random seed for chaos
    let seed = (time.as_secs_f64() * 1000_000.0) as u64;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 300,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.1,
        replication_factor: 3,
    };

    do_governor_loop_proc_apply_schema_change_batching(params);
}

fn do_governor_loop_proc_apply_schema_change_batching(params: BatchingRpcTestParameters) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(params.seed);
    tlog!(Info, "random seed: {}", params.seed);

    let node = node::Node::for_tests();

    //
    // Setup cluster topology
    //

    let (instances, replicasets) =
        generate_replicasets(1, params.num_instances, params.replication_factor, None);
    let num_masters = replicasets.len();

    setup_topology(node, &instances, Some(&replicasets), None);

    // Setup the pending DDL operation
    let ddl = Ddl::CreateTable {
        id: 42069,
        name: "table_for_the_purposes_of_testing".into(),
        format: vec![],
        primary_key: vec![],
        distribution: Distribution::Global,
        engine: tarantool::space::SpaceEngineType::Memtx,
        owner: ADMIN_ID,
    };
    node.storage
        .properties
        .put(PropertyName::PendingSchemaChange, &ddl)
        .unwrap();
    node.storage
        .properties
        .put(PropertyName::PendingSchemaVersion, &111)
        .unwrap();

    //
    // Setup other things
    //

    node.alter_system_parameters
        .borrow_mut()
        .governor_rpc_batch_size = params.batch_size;

    let (state, _, _, rpc_requests, cas_requests) = setup_governor_loop_state(node);

    //
    // Spawn the pretend governor_loop fiber
    //
    let pretend_state = spawn_pretend_governor_loop(state);

    //
    // Loop until pretend governor_loop finishes, handle governor's RPC & CAS requests
    //

    let mut ok_so_far = HashSet::new();
    let mut first_cas_request_skipped = false;
    let mut cas_request_handled = false;
    let mut stats = BatchingRpcTestStats::default();

    while !pretend_state.get().governor_idle {
        //
        // Handle governor's RPCs
        //

        let rpcs = rpc_requests.receive_all(Duration::from_millis(50));

        stats.num_batches += 1;
        tlog!(
            Info,
            "RPC batch #{} size: {}",
            stats.num_batches,
            rpcs.len()
        );

        let mut targets = vec![];

        for rpc in rpcs {
            // Verify RPC request
            let (id, request) = rpc;
            let RaftIdOrName::Name(name) = id else {
                panic!("expected instance name, got {id:?}");
            };
            targets.push(name.clone());

            assert_eq!(request.proc, ".proc_apply_schema_change");
            let resp = rpc::ddl_apply::Response::Ok {};

            // Send the RPC response to governor
            let fail = rng.gen_bool(params.p_rpc_err);
            if fail {
                request
                    .on_result
                    .handle(Err(Error::other("injected error")));
                stats.num_rpc_fails += 1;

                continue;
            }

            request.on_result.handle(Ok(Tuple::new(&[resp]).unwrap()));

            // Remember which RPCs got an Ok response
            assert!(
                !ok_so_far.contains(&name),
                "{name} got a repeat RPC after Ok response"
            );
            ok_so_far.insert(name);
        }

        // RPCs are sent out in batches of given size (may be shorter)
        assert!(targets.len() <= params.batch_size, "{targets:?}");

        // Allow governor to handle RPC results and send the CAS requests
        fiber::reschedule();

        //
        // Handle governor's CAS requests
        //

        let cas_requests = cas_requests.try_receive_all();
        tlog!(Info, "CAS batch size: {}", cas_requests.len());

        if !cas_requests.is_empty() {
            // Make sure CAS request is only sent once every instance handled the RPC
            assert_eq!(ok_so_far.len(), num_masters);

            // Verify CAS request
            assert_ne!(
                node.storage.properties.pending_schema_change().unwrap(),
                None
            );
            assert_ne!(
                node.storage.properties.pending_schema_version().unwrap(),
                None
            );

            let mut cas_requests = cas_requests.into_iter();
            let (request, response) = cas_requests.next().unwrap();

            let Op::DdlCommit = &request.op else {
                panic!("expected DdlCommit, got {:?}", request.op);
            };

            tlog!(Info, "cas_request: {request:?}");

            // Apply CAS request
            let fail = rng.gen_bool(params.p_cas_err);
            if
            // Fail the first attempt always just to check what how we handle that
            first_cas_request_skipped
                    // Next time fail randomly
                    && !fail
            {
                response.send(Ok(cas::Response::for_tests())).unwrap();

                node.storage
                    .properties
                    .delete(PropertyName::PendingSchemaChange)
                    .unwrap();
                node.storage
                    .properties
                    .delete(PropertyName::PendingSchemaVersion)
                    .unwrap();
                cas_request_handled = true;
            } else {
                // It's possible that the CAS request get's dropped due to
                // a conflict. Governor should handle this fine
                response.send(Err(Error::other("injected error"))).unwrap();
                stats.num_cas_fails += 1;
                first_cas_request_skipped = true;
            }

            // Expecting only 1 CAS request
            assert_eq!(cas_requests.next().map(|(r, _)| r), None);
        }
    }

    // Make sure only masters received the RPC
    let all_masters = replicasets_masters(&node.topology_cache.get());
    let all_masters = HashSet::from_iter(all_masters);
    assert_eq!(all_masters, ok_so_far);

    assert!(cas_request_handled);

    // Log the stats
    let s = pretend_state.get();
    tlog!(Info, "stats: {params:#0.3?}, {s:#0.3?}, {stats:#0.3?}")
}

//
// test TRUNCATE TABLE
//

#[tarantool::test]
fn test_governor_loop_truncate_table_batching_fixed() {
    // Run with a fixed seed each time for some stability
    let seed = 1770229753053178;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 300,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.1,
        replication_factor: 3,
    };

    do_governor_loop_truncate_table_batching(params);
}

#[tarantool::test]
fn test_governor_loop_truncate_table_batching_random() {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    // And also run with a random seed for chaos
    let seed = (time.as_secs_f64() * 1000_000.0) as u64;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 300,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.1,
        replication_factor: 3,
    };

    do_governor_loop_truncate_table_batching(params);
}

fn do_governor_loop_truncate_table_batching(params: BatchingRpcTestParameters) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(params.seed);
    tlog!(Info, "random seed: {}", params.seed);

    let node = node::Node::for_tests();

    //
    // Setup cluster topology
    //

    let mut tier_1 = Tier::default();
    tier_1.name = SmolStr::new_static("tier_1");
    tier_1.replication_factor = params.replication_factor as _;

    let mut tier_2 = Tier::default();
    tier_2.name = SmolStr::new_static("tier_2");
    tier_2.replication_factor = 2;

    let mut tier_3 = Tier::default();
    tier_3.name = SmolStr::new_static("tier_3");
    tier_3.replication_factor = 1;

    let tier_1_size = params.num_instances / 2;
    let tier_2_size = params.num_instances / 4;
    let tier_3_size = params.num_instances / 4;

    let next_raft_id = 1;
    let (tier_1_instances, tier_1_replicasets) = generate_replicasets(
        next_raft_id,
        tier_1_size,
        tier_1.replication_factor as _,
        Some(&tier_1.name),
    );
    let next_raft_id = tier_1_instances.last().unwrap().raft_id + 1;
    let (tier_2_instances, tier_2_replicasets) = generate_replicasets(
        next_raft_id,
        tier_2_size,
        tier_2.replication_factor as _,
        Some(&tier_2.name),
    );
    let next_raft_id = tier_2_instances.last().unwrap().raft_id + 1;
    let (tier_3_instances, tier_3_replicasets) = generate_replicasets(
        next_raft_id,
        tier_3_size,
        tier_3.replication_factor as _,
        Some(&tier_3.name),
    );

    let all_instances: Vec<_> = std::iter::empty()
        .chain(tier_1_instances.iter().cloned())
        .chain(tier_2_instances.iter().cloned())
        .chain(tier_3_instances.iter().cloned())
        .collect();
    let all_replicasets: Vec<_> = std::iter::empty()
        .chain(tier_1_replicasets.iter().cloned())
        .chain(tier_2_replicasets.iter().cloned())
        .chain(tier_3_replicasets.iter().cloned())
        .collect();
    let all_tiers = vec![tier_1.clone(), tier_2.clone(), tier_3.clone()];
    setup_topology(
        node,
        &all_instances,
        Some(&all_replicasets),
        Some(&all_tiers),
    );

    //
    // Setup the pending TRUNCATE TABLE operation
    //
    let mut table_def = TableDef::for_tests();
    let first_field = table_def.format[0].name.as_str();
    table_def.distribution = Distribution::ShardedByField {
        field: first_field.into(),
        tier: tier_1.name.clone(),
    };

    let ddl = Ddl::TruncateTable {
        id: table_def.id,
        initiator: ADMIN_ID,
    };

    node.storage.pico_table.put(&table_def).unwrap();
    node.storage
        .properties
        .put(PropertyName::PendingSchemaChange, &ddl)
        .unwrap();
    node.storage
        .properties
        .put(PropertyName::PendingSchemaVersion, &111)
        .unwrap();

    //
    // Setup other things
    //

    node.alter_system_parameters
        .borrow_mut()
        .governor_rpc_batch_size = params.batch_size;

    let (state, _, _, rpc_requests, cas_requests) = setup_governor_loop_state(node);

    //
    // Spawn the pretend governor_loop fiber
    //
    let pretend_state = spawn_pretend_governor_loop(state);

    //
    // First governor does a vshard.map_callrw
    //

    let mut ok_so_far = HashSet::new();
    let mut first_cas_request_skipped = false;
    let mut cas_request_handled = false;
    let mut stats = BatchingRpcTestStats::default();

    {
        let rpcs = rpc_requests.receive_all(Duration::from_millis(50));

        stats.num_batches += 1;
        tlog!(
            Info,
            "RPC batch #{} size: {}",
            stats.num_batches,
            rpcs.len()
        );

        let mut rpcs = rpcs.into_iter();
        let rpc = rpcs.next().unwrap();

        // Verify RPC request
        let (id, request) = rpc;

        // Special hack value used just for testing
        let RaftIdOrName::RaftId(0) = id else {
            panic!("expected RaftId(0), got {id:?}");
        };

        assert_eq!(request.proc, "vshard.map_callrw");

        // Fail the first time just to see what happens
        request
            .on_result
            .handle(Err(Error::other("injected error")));
        stats.num_rpc_fails += 1;

        // Expecting only 1 RPC request
        assert_eq!(rpcs.next().map(|(r, _)| r), None);
    }

    // Allow governor to handle RPC results and send the CAS requests
    fiber::reschedule();

    // Governor retries vshard.map_callrw until it succeeds
    {
        let rpcs = rpc_requests.receive_all(Duration::from_millis(50));

        stats.num_batches += 1;
        tlog!(
            Info,
            "RPC batch #{} size: {}",
            stats.num_batches,
            rpcs.len()
        );

        let mut rpcs = rpcs.into_iter();
        let rpc = rpcs.next().unwrap();

        // Verify RPC request
        let (id, request) = rpc;

        // Special hack value used just for testing
        let RaftIdOrName::RaftId(0) = id else {
            panic!("expected RaftId(0), got {id:?}");
        };

        assert_eq!(request.proc, "vshard.map_callrw");
        let resp = ();

        // Succeed the second time around so we can proceed to the following steps
        request.on_result.handle(Ok(Tuple::new(&[resp]).unwrap()));

        // Expecting only 1 RPC request
        assert_eq!(rpcs.next().map(|(r, _)| r), None);
    }

    //
    // Loop until pretend governor_loop finishes, handle governor's RPC & CAS requests
    //

    let tier_1_instance_names: HashSet<_> = tier_1_instances
        .iter()
        .map(|instance| instance.name.clone())
        .collect();

    let all_masters = replicasets_masters(&node.topology_cache.get());
    let other_tier_masters: HashSet<_> = all_masters
        .into_iter()
        .filter(|instance| !tier_1_instance_names.contains(instance))
        .collect();

    while !pretend_state.get().governor_idle {
        //
        // Handle governor's RPCs
        //

        let rpcs = rpc_requests.receive_all(Duration::from_millis(50));

        stats.num_batches += 1;
        tlog!(
            Info,
            "RPC batch #{} size: {}",
            stats.num_batches,
            rpcs.len()
        );

        let mut targets = vec![];

        for rpc in rpcs {
            // Verify RPC request
            let (id, request) = rpc;
            let RaftIdOrName::Name(name) = id else {
                panic!("expected instance name, got {id:?}");
            };
            targets.push(name.clone());

            assert_eq!(request.proc, ".proc_apply_schema_change");
            let resp = rpc::ddl_apply::Response::Ok {};

            // Send the RPC response to governor
            let fail = rng.gen_bool(params.p_rpc_err);
            if fail {
                request
                    .on_result
                    .handle(Err(Error::other("injected error")));
                stats.num_rpc_fails += 1;

                continue;
            }

            request.on_result.handle(Ok(Tuple::new(&[resp]).unwrap()));

            // Remember which RPCs got an Ok response
            assert!(
                !ok_so_far.contains(&name),
                "{name} got a repeat RPC after Ok response"
            );
            ok_so_far.insert(name);
        }

        // RPCs are sent out in batches of given size (may be shorter)
        assert!(targets.len() <= params.batch_size, "{targets:?}");

        // Allow governor to handle RPC results and send the CAS requests
        fiber::reschedule();

        //
        // Handle governor's CAS requests
        //

        let cas_requests = cas_requests.try_receive_all();
        tlog!(Info, "CAS batch size: {}", cas_requests.len());

        if !cas_requests.is_empty() {
            // Make sure CAS request is only sent once every instance handled the RPC
            assert_eq!(ok_so_far.len(), other_tier_masters.len());

            // Verify CAS request
            assert_ne!(
                node.storage.properties.pending_schema_change().unwrap(),
                None
            );
            assert_ne!(
                node.storage.properties.pending_schema_version().unwrap(),
                None
            );

            let mut cas_requests = cas_requests.into_iter();
            let (request, response) = cas_requests.next().unwrap();

            let Op::DdlCommit = &request.op else {
                panic!("expected DdlCommit, got {:?}", request.op);
            };

            tlog!(Info, "cas_request: {request:?}");

            // Apply CAS request
            let fail = rng.gen_bool(params.p_cas_err);
            if
            // Fail the first attempt always just to check what how we handle that
            first_cas_request_skipped
                    // Next time fail randomly
                    && !fail
            {
                response.send(Ok(cas::Response::for_tests())).unwrap();

                node.storage
                    .properties
                    .delete(PropertyName::PendingSchemaChange)
                    .unwrap();
                node.storage
                    .properties
                    .delete(PropertyName::PendingSchemaVersion)
                    .unwrap();
                cas_request_handled = true;
            } else {
                // It's possible that the CAS request get's dropped due to
                // a conflict. Governor should handle this fine
                response.send(Err(Error::other("injected error"))).unwrap();
                stats.num_cas_fails += 1;
                first_cas_request_skipped = true;
            }

            // Expecting only 1 CAS request
            assert_eq!(cas_requests.next().map(|(r, _)| r), None);
        }
    }

    // Make sure only masters of other replicasets received the RPC
    assert_eq!(ok_so_far, other_tier_masters);

    assert!(cas_request_handled);

    // Log the stats
    let s = pretend_state.get();
    tlog!(Info, "stats: {params:#0.3?}, {s:#0.3?}, {stats:#0.3?}")
}

//
// test BACKUP step
//

#[tarantool::test]
fn test_governor_loop_backup_batching_fixed() {
    // Run with a fixed seed each time for some stability
    let seed = 1770229753053178;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 102,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.8,
        replication_factor: 3,
    };

    do_governor_loop_backup_batching(params);
}

#[tarantool::test]
fn test_governor_loop_backup_batching_random() {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    // And also run with a random seed for chaos
    let seed = (time.as_secs_f64() * 1000_000.0) as u64;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 102,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.8,
        replication_factor: 3,
    };

    do_governor_loop_backup_batching(params);
}

fn do_governor_loop_backup_batching(params: BatchingRpcTestParameters) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(params.seed);
    tlog!(Info, "random seed: {}", params.seed);

    let node = node::Node::for_tests();

    //
    // Setup cluster topology
    //

    let (instances, replicasets) =
        generate_replicasets(1, params.num_instances, params.replication_factor, None);

    setup_topology(node, &instances, Some(&replicasets), None);

    // Setup the pending BACKUP operation
    let ddl = Ddl::Backup { timestamp: 3333 };
    node.storage
        .properties
        .put(PropertyName::PendingSchemaChange, &ddl)
        .unwrap();
    node.storage
        .properties
        .put(PropertyName::PendingSchemaVersion, &111)
        .unwrap();

    //
    // Setup other things
    //

    node.alter_system_parameters
        .borrow_mut()
        .governor_rpc_batch_size = params.batch_size;

    let (state, _, _, rpc_requests, cas_requests) = setup_governor_loop_state(node);

    //
    // Spawn the pretend governor_loop fiber
    //
    let pretend_state = spawn_pretend_governor_loop(state);

    //
    // Loop until pretend governor_loop finishes, handle governor's RPC & CAS requests
    //

    let mut ok_masters = HashSet::new();
    let mut ok_replicas = HashSet::new();
    let mut first_cas_request_skipped = false;
    let mut cas_request_handled = false;
    let mut stats = BatchingRpcTestStats::default();

    let (all_masters, all_replicas) = masters_and_replicas(&node.topology_cache.get());

    while !pretend_state.get().governor_idle {
        //
        // Handle governor's RPCs
        //

        let rpcs = rpc_requests.receive_all(Duration::from_millis(50));

        stats.num_batches += 1;
        tlog!(
            Info,
            "RPC batch #{} size: {}",
            stats.num_batches,
            rpcs.len()
        );

        let mut targets = vec![];

        for rpc in rpcs {
            // Verify RPC request
            let (id, request) = rpc;
            let RaftIdOrName::Name(name) = id else {
                panic!("expected instance name, got {id:?}");
            };
            targets.push(name.clone());

            assert_eq!(request.proc, ".proc_apply_backup");
            let resp = rpc::ddl_backup::Response::BackupPath(PathBuf::new());

            // Send the RPC response to governor
            let fail = rng.gen_bool(params.p_rpc_err);
            if fail {
                request
                    .on_result
                    .handle(Err(Error::other("injected error")));
                stats.num_rpc_fails += 1;

                continue;
            }

            request.on_result.handle(Ok(Tuple::new(&[resp]).unwrap()));

            // Remember which RPCs got an Ok response
            if all_masters.contains(&name) {
                assert_eq!(
                    ok_replicas.len(),
                    0,
                    "replicas only get RPCs after all masters"
                );
                assert!(
                    !ok_masters.contains(&name),
                    "{name} got a repeat RPC after Ok response"
                );
                ok_masters.insert(name);
            } else {
                assert_eq!(
                    ok_masters.len(),
                    all_masters.len(),
                    "replicas only get RPCs after all masters"
                );
                assert!(
                    !ok_replicas.contains(&name),
                    "{name} got a repeat RPC after Ok response"
                );
                ok_replicas.insert(name);
            }
        }

        // RPCs are sent out in batches of given size (may be shorter)
        assert!(targets.len() <= params.batch_size, "{targets:?}");

        // Allow governor to handle RPC results and send the CAS requests
        fiber::reschedule();

        //
        // Handle governor's CAS requests
        //

        let cas_requests = cas_requests.try_receive_all();
        tlog!(Info, "CAS batch size: {}", cas_requests.len());

        if !cas_requests.is_empty() {
            // Make sure CAS request is only sent once every instance handled the RPC
            assert_eq!(ok_masters.len(), all_masters.len());
            assert_eq!(ok_replicas.len(), all_replicas.len());

            // Verify CAS request
            assert_ne!(
                node.storage.properties.pending_schema_change().unwrap(),
                None
            );
            assert_ne!(
                node.storage.properties.pending_schema_version().unwrap(),
                None
            );

            let mut cas_requests = cas_requests.into_iter();
            let (request, response) = cas_requests.next().unwrap();

            let Op::DdlCommit = &request.op else {
                panic!("expected DdlCommit, got {:?}", request.op);
            };

            tlog!(Info, "cas_request: {request:?}");

            // Apply CAS request
            let fail = rng.gen_bool(params.p_cas_err);
            if
            // Fail the first attempt always just to check what how we handle that
            first_cas_request_skipped
                    // Next time fail randomly
                    && !fail
            {
                response.send(Ok(cas::Response::for_tests())).unwrap();

                node.storage
                    .properties
                    .delete(PropertyName::PendingSchemaChange)
                    .unwrap();
                node.storage
                    .properties
                    .delete(PropertyName::PendingSchemaVersion)
                    .unwrap();
                cas_request_handled = true;
            } else {
                // It's possible that the CAS request get's dropped due to
                // a conflict. Governor should handle this fine
                response.send(Err(Error::other("injected error"))).unwrap();
                stats.num_cas_fails += 1;
                first_cas_request_skipped = true;
            }

            // Expecting only 1 CAS request
            assert_eq!(cas_requests.next().map(|(r, _)| r), None);
        }
    }

    assert!(cas_request_handled);

    // Log the stats
    let s = pretend_state.get();
    tlog!(Info, "stats: {params:#0.3?}, {s:#0.3?}, {stats:#0.3?}")
}

//
// test CREATE PLUGIN step
//

#[tarantool::test]
fn test_governor_loop_create_plugin_batching_fixed() {
    // Run with a fixed seed each time for some stability
    let seed = 1770229753053178;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 102,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.8,
        replication_factor: 3,
    };

    do_governor_loop_create_plugin_batching(params);
}

#[tarantool::test]
fn test_governor_loop_create_plugin_batching_random() {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    // And also run with a random seed for chaos
    let seed = (time.as_secs_f64() * 1000_000.0) as u64;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 102,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.8,
        replication_factor: 3,
    };

    do_governor_loop_create_plugin_batching(params);
}

fn do_governor_loop_create_plugin_batching(params: BatchingRpcTestParameters) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(params.seed);
    tlog!(Info, "random seed: {}", params.seed);

    let node = node::Node::for_tests();

    //
    // Setup cluster topology
    //

    let mut instances = vec![];
    for raft_id in 1..=(params.num_instances as RaftId) {
        instances.push(dummy_instance(raft_id));
    }

    setup_topology(node, &instances, None, None);

    // Setup the pending CREATE PLUGIN operation
    let plugin_manifest = Manifest::for_tests();

    let plugin_op = PluginOp::CreatePlugin {
        manifest: plugin_manifest.clone(),
        inherit_entities: Default::default(),
        inherit_topology: Default::default(),
    };
    node.storage
        .properties
        .put(PropertyName::PendingPluginOperation, &plugin_op)
        .unwrap();

    //
    // Setup other things
    //

    node.alter_system_parameters
        .borrow_mut()
        .governor_rpc_batch_size = params.batch_size;

    let (state, _, _, rpc_requests, cas_requests) = setup_governor_loop_state(node);

    //
    // Spawn the pretend governor_loop fiber
    //
    let pretend_state = spawn_pretend_governor_loop(state);

    //
    // First check what happens when one of the RPCs fails
    //

    let mut num_fails = 0;
    let mut ok_so_far = HashSet::new();

    {
        let rpcs = rpc_requests.receive_all(Duration::from_millis(50));
        let num_rpcs = rpcs.len();
        for (rpc, i) in rpcs.into_iter().zip(1..) {
            // Verify RPC request
            let (id, request) = rpc;
            let RaftIdOrName::Name(name) = id else {
                panic!("expected instance name, got {id:?}");
            };

            assert_eq!(request.proc, ".proc_load_plugin_dry_run");
            let resp = rpc::load_plugin_dry_run::Response::Ok {};

            // Send the RPC response to governor
            let random_fail = rng.gen_bool(params.p_rpc_err);
            let last_rpc = i == num_rpcs;
            // Make sure we get at least one RPC failure
            let no_fails_so_far = num_fails == 0;
            if random_fail || (last_rpc && no_fails_so_far) {
                request
                    .on_result
                    .handle(Err(Error::other("injected error")));
                num_fails += 1;

                continue;
            }

            // Send the RPC response to governor
            request.on_result.handle(Ok(Tuple::new(&[resp]).unwrap()));

            // Remember which RPCs got an Ok response
            assert!(
                !ok_so_far.contains(&name),
                "{name} got a repeat RPC after Ok response"
            );
            ok_so_far.insert(name);
        }
    }

    // Allow governor to handle RPC results and send the CAS requests
    fiber::reschedule();

    {
        let cas_requests = cas_requests.try_receive_all();
        let mut cas_requests = cas_requests.into_iter();
        let (request, response) = cas_requests.next().unwrap();

        let Op::Plugin(PluginRaftOp::Abort { cause }) = &request.op else {
            panic!("expected PluginRaftOp::Abort, got {:?}", request.op);
        };

        assert_eq!(cause.message, "injected error");

        // Don't apply CAS request
        response.send(Err(Error::other("injected error"))).unwrap();

        // Expecting only 1 CAS request
        assert_eq!(cas_requests.next().map(|(r, _)| r), None);
    }

    //
    // Now let's try again without failing a single RPC so that the operation succeeds
    //

    let mut first_cas_request_skipped = false;
    let mut cas_request_handled = false;
    let mut stats = BatchingRpcTestStats::default();

    while !pretend_state.get().governor_idle {
        //
        // Handle governor's RPCs
        //

        let rpcs = rpc_requests.receive_all(Duration::from_millis(50));

        stats.num_batches += 1;
        tlog!(
            Info,
            "RPC batch #{} size: {}",
            stats.num_batches,
            rpcs.len()
        );

        let mut targets = vec![];

        for rpc in rpcs {
            // Verify RPC request
            let (id, request) = rpc;
            let RaftIdOrName::Name(name) = id else {
                panic!("expected instance name, got {id:?}");
            };
            targets.push(name.clone());

            assert_eq!(request.proc, ".proc_load_plugin_dry_run");
            let resp = rpc::load_plugin_dry_run::Response::Ok {};

            // Send the RPC response to governor
            request.on_result.handle(Ok(Tuple::new(&[resp]).unwrap()));

            // Remember which RPCs got an Ok response
            assert!(
                !ok_so_far.contains(&name),
                "{name} got a repeat RPC after Ok response"
            );
            ok_so_far.insert(name);
        }

        // RPCs are sent out in batches of given size (may be shorter)
        assert!(targets.len() <= params.batch_size, "{targets:?}");

        // Allow governor to handle RPC results and send the CAS requests
        fiber::reschedule();

        //
        // Handle governor's CAS requests
        //

        let cas_requests = cas_requests.try_receive_all();
        tlog!(Info, "CAS batch size: {}", cas_requests.len());

        if !cas_requests.is_empty() {
            // Make sure CAS request is only sent once every instance handled the RPC
            assert_eq!(ok_so_far.len(), params.num_instances);

            // Verify CAS request
            assert_ne!(node.storage.properties.pending_plugin_op().unwrap(), None);

            let mut cas_requests = cas_requests.into_iter();
            let (request, response) = cas_requests.next().unwrap();

            let dmls = request.op.dmls().unwrap();

            let mut seen_dml_create_plugin_def = false;
            let mut seen_dml_delete_pending_plugin_op = false;

            for dml in dmls {
                if let Dml::Replace {
                    table: Plugins::TABLE_ID,
                    tuple,
                    initiator,
                } = dml
                {
                    assert_eq!(*initiator, ADMIN_ID);

                    let plugin_def: PluginDef = rmp_serde::from_slice(tuple.as_ref()).unwrap();
                    assert_eq!(plugin_def.enabled, false);
                    assert_eq!(plugin_def, plugin_manifest.plugin_def());

                    assert!(!seen_dml_create_plugin_def);
                    seen_dml_create_plugin_def = true;
                    continue;
                }

                if let Dml::Delete {
                    table: Properties::TABLE_ID,
                    key,
                    initiator,
                    metainfo,
                } = dml
                {
                    assert_eq!(*initiator, ADMIN_ID);
                    assert_eq!(metainfo, &None);

                    let [name]: [PropertyName; 1] = rmp_serde::from_slice(key.as_ref()).unwrap();
                    assert_eq!(name, PropertyName::PendingPluginOperation);

                    assert!(!seen_dml_delete_pending_plugin_op);
                    seen_dml_delete_pending_plugin_op = true;
                    continue;
                }

                panic!("unexpected Dml: {dml:?}");
            }

            assert!(seen_dml_create_plugin_def);
            assert!(seen_dml_delete_pending_plugin_op);

            // Apply CAS request
            let fail = rng.gen_bool(params.p_cas_err);
            if
            // Fail the first attempt always just to check what how we handle that
            first_cas_request_skipped
                    // Next time fail randomly
                    && !fail
            {
                response.send(Ok(cas::Response::for_tests())).unwrap();

                node.storage
                    .properties
                    .delete(PropertyName::PendingPluginOperation)
                    .unwrap();
                cas_request_handled = true;
            } else {
                // It's possible that the CAS request get's dropped due to
                // a conflict. Governor should handle this fine
                response.send(Err(Error::other("injected error"))).unwrap();
                stats.num_cas_fails += 1;
                first_cas_request_skipped = true;
            }

            // Expecting only 1 CAS request
            assert_eq!(cas_requests.next().map(|(r, _)| r), None);
        }
    }

    assert!(cas_request_handled);

    // Log the stats
    let s = pretend_state.get();
    tlog!(Info, "stats: {params:#0.3?}, {s:#0.3?}, {stats:#0.3?}")
}

//
// test ALTER PLUGIN ENABLE step
//

#[tarantool::test]
fn test_governor_loop_enable_plugin_batching_fixed() {
    // Run with a fixed seed each time for some stability
    let seed = 1770229753053178;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 102,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.8,
        replication_factor: 3,
    };

    do_governor_loop_enable_plugin_batching(params);
}

#[tarantool::test]
fn test_governor_loop_enable_plugin_batching_random() {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    // And also run with a random seed for chaos
    let seed = (time.as_secs_f64() * 1000_000.0) as u64;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 102,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.8,
        replication_factor: 3,
    };

    do_governor_loop_enable_plugin_batching(params);
}

fn do_governor_loop_enable_plugin_batching(params: BatchingRpcTestParameters) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(params.seed);
    tlog!(Info, "random seed: {}", params.seed);

    let node = node::Node::for_tests();

    //
    // Setup cluster topology
    //

    let mut instances = vec![];
    for raft_id in 1..=(params.num_instances as RaftId) {
        instances.push(dummy_instance(raft_id));
    }

    setup_topology(node, &instances, None, None);

    // Setup the pending ALTER PLUGIN ENABLE operation
    let plugin = PluginIdentifier::new("dummy-plugin".into(), "1.2.3-pre-rc1".into());

    let plugin_op = PluginOp::EnablePlugin {
        plugin: plugin.clone(),
        timeout: Duration::from_secs(30),
    };
    node.storage
        .properties
        .put(PropertyName::PendingPluginOperation, &plugin_op)
        .unwrap();

    //
    // Setup other things
    //

    node.alter_system_parameters
        .borrow_mut()
        .governor_rpc_batch_size = params.batch_size;

    let (state, _, _, rpc_requests, cas_requests) = setup_governor_loop_state(node);

    //
    // Spawn the pretend governor_loop fiber
    //
    let pretend_state = spawn_pretend_governor_loop(state);

    //
    // Loop until pretend governor_loop finishes, handle governor's RPC & CAS requests
    //

    let mut ok_so_far = HashSet::new();
    let mut first_cas_request_skipped = false;
    let mut cas_request_handled = false;
    let mut stats = BatchingRpcTestStats::default();

    while !pretend_state.get().governor_idle {
        //
        // Handle governor's RPCs
        //

        let rpcs = rpc_requests.receive_all(Duration::from_millis(50));

        stats.num_batches += 1;
        tlog!(
            Info,
            "RPC batch #{} size: {}",
            stats.num_batches,
            rpcs.len()
        );

        let mut targets = vec![];

        for rpc in rpcs {
            // Verify RPC request
            let (id, request) = rpc;
            let RaftIdOrName::Name(name) = id else {
                panic!("expected instance name, got {id:?}");
            };
            targets.push(name.clone());

            assert_eq!(request.proc, ".proc_enable_plugin");
            let resp = rpc::enable_plugin::Response::Ok {};

            // Send the RPC response to governor
            let fail = rng.gen_bool(params.p_rpc_err);
            if fail {
                request
                    .on_result
                    .handle(Err(Error::other("injected error")));
                stats.num_rpc_fails += 1;

                continue;
            }

            // Send the RPC response to governor
            request.on_result.handle(Ok(Tuple::new(&[resp]).unwrap()));

            // Remember which RPCs got an Ok response
            assert!(
                !ok_so_far.contains(&name),
                "{name} got a repeat RPC after Ok response"
            );
            ok_so_far.insert(name);
        }

        // RPCs are sent out in batches of given size (may be shorter)
        assert!(targets.len() <= params.batch_size, "{targets:?}");

        // Allow governor to handle RPC results and send the CAS requests
        fiber::reschedule();

        //
        // Handle governor's CAS requests
        //

        let cas_requests = cas_requests.try_receive_all();
        tlog!(Info, "CAS batch size: {}", cas_requests.len());

        if !cas_requests.is_empty() {
            // Make sure CAS request is only sent once every instance handled the RPC
            assert_eq!(ok_so_far.len(), params.num_instances);

            // Verify CAS request
            assert_ne!(node.storage.properties.pending_plugin_op().unwrap(), None);

            let mut cas_requests = cas_requests.into_iter();
            let (request, response) = cas_requests.next().unwrap();

            let dmls = request.op.dmls().unwrap();

            let mut seen_dml_enable_plugin_def = false;
            let mut seen_dml_delete_pending_plugin_op = false;

            for dml in dmls {
                if let Dml::Update {
                    table: Plugins::TABLE_ID,
                    initiator,
                    key,
                    ops,
                } = dml
                {
                    assert_eq!(*initiator, ADMIN_ID);

                    let actual_plugin: PluginIdentifier =
                        rmp_serde::from_slice(key.as_ref()).unwrap();
                    assert_eq!(actual_plugin, plugin);

                    assert_eq!(ops.len(), 1);
                    let enabled: bool = check_field_assignment(&ops[0], "enabled");
                    assert_eq!(enabled, true);

                    assert!(!seen_dml_enable_plugin_def);
                    seen_dml_enable_plugin_def = true;
                    continue;
                }

                if let Dml::Delete {
                    table: Properties::TABLE_ID,
                    key,
                    initiator,
                    metainfo,
                } = dml
                {
                    assert_eq!(*initiator, ADMIN_ID);
                    assert_eq!(metainfo, &None);

                    let [name]: [PropertyName; 1] = rmp_serde::from_slice(key.as_ref()).unwrap();
                    assert_eq!(name, PropertyName::PendingPluginOperation);

                    assert!(!seen_dml_delete_pending_plugin_op);
                    seen_dml_delete_pending_plugin_op = true;
                    continue;
                }

                panic!("unexpected Dml: {dml:?}");
            }

            assert!(seen_dml_enable_plugin_def);
            assert!(seen_dml_delete_pending_plugin_op);

            // Apply CAS request
            let fail = rng.gen_bool(params.p_cas_err);
            if
            // Fail the first attempt always just to check what how we handle that
            first_cas_request_skipped
                    // Next time fail randomly
                    && !fail
            {
                response.send(Ok(cas::Response::for_tests())).unwrap();

                node.storage
                    .properties
                    .delete(PropertyName::PendingPluginOperation)
                    .unwrap();
                cas_request_handled = true;
            } else {
                // It's possible that the CAS request get's dropped due to
                // a conflict. Governor should handle this fine
                response.send(Err(Error::other("injected error"))).unwrap();
                stats.num_cas_fails += 1;
                first_cas_request_skipped = true;
            }

            // Expecting only 1 CAS request
            assert_eq!(cas_requests.next().map(|(r, _)| r), None);
        }
    }

    assert!(cas_request_handled);

    // Log the stats
    let s = pretend_state.get();
    tlog!(Info, "stats: {params:#0.3?}, {s:#0.3?}, {stats:#0.3?}")
}

//
// test ALTER PLUGIN ADD SERVICE TO TIER step
//

#[tarantool::test]
fn test_governor_loop_alter_plugin_add_service_to_tier_batching_fixed() {
    // Run with a fixed seed each time for some stability
    let seed = 1770229753053178;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 102,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.8,
        replication_factor: 3,
    };

    do_governor_loop_alter_plugin_add_service_to_tier_batching(params);
}

#[tarantool::test]
fn test_governor_loop_alter_plugin_add_service_to_tier_batching_random() {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    // And also run with a random seed for chaos
    let seed = (time.as_secs_f64() * 1000_000.0) as u64;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 102,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.8,
        replication_factor: 3,
    };

    do_governor_loop_alter_plugin_add_service_to_tier_batching(params);
}

fn do_governor_loop_alter_plugin_add_service_to_tier_batching(params: BatchingRpcTestParameters) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(params.seed);
    tlog!(Info, "random seed: {}", params.seed);

    let node = node::Node::for_tests();

    //
    // Setup cluster topology
    //

    let mut instances = vec![];
    for raft_id in 1..=(params.num_instances as RaftId) {
        instances.push(dummy_instance(raft_id));
    }

    setup_topology(node, &instances, None, None);

    //
    // Setup the pending ALTER PLUGIN ADD SERVICE TO TIER operation
    //
    let add_tier = instances[0].tier.clone();

    let mut plugin_def = PluginDef::for_tests();
    plugin_def.enabled = true;
    let service_def = ServiceDef::for_tests();

    node.storage.plugins.put(&plugin_def).unwrap();

    node.storage.services.put(&service_def).unwrap();

    let plugin_op = PluginOp::AlterServiceTiers {
        plugin: plugin_def.identifier(),
        service: service_def.name.clone(),
        tier: add_tier.clone(),
        kind: crate::plugin::TopologyUpdateOpKind::Add,
    };
    node.storage
        .properties
        .put(PropertyName::PendingPluginOperation, &plugin_op)
        .unwrap();

    //
    // Setup other things
    //

    node.alter_system_parameters
        .borrow_mut()
        .governor_rpc_batch_size = params.batch_size;

    let (state, _, _, rpc_requests, cas_requests) = setup_governor_loop_state(node);

    //
    // Spawn the pretend governor_loop fiber
    //
    let pretend_state = spawn_pretend_governor_loop(state);

    //
    // First check what happens when one of the RPCs fails
    //

    let mut num_fails = 0;
    let mut ok_so_far = HashSet::new();

    {
        let rpcs = rpc_requests.receive_all(Duration::from_millis(50));
        let num_rpcs = rpcs.len();
        for (rpc, i) in rpcs.into_iter().zip(1..) {
            // Verify RPC request
            let (id, request) = rpc;
            let RaftIdOrName::Name(name) = id else {
                panic!("expected instance name, got {id:?}");
            };

            assert_eq!(request.proc, ".proc_enable_service");
            let resp = rpc::enable_service::Response {};

            // Remember which RPCs got an Ok response
            assert!(
                !ok_so_far.contains(&name),
                "{name} got a repeat RPC after Ok response"
            );
            ok_so_far.insert(name);

            // Send the RPC response to governor
            let random_fail = rng.gen_bool(params.p_rpc_err);
            let last_rpc = i == num_rpcs;
            // Make sure we get at least one RPC failure
            let no_fails_so_far = num_fails == 0;
            if random_fail || (last_rpc && no_fails_so_far) {
                request
                    .on_result
                    .handle(Err(Error::other("injected error")));
                num_fails += 1;

                continue;
            }

            // Send the RPC response to governor
            request.on_result.handle(Ok(Tuple::new(&[resp]).unwrap()));
        }
    }

    // Allow governor to handle RPC results and send the CAS requests
    fiber::reschedule();

    // If one of the RPCs fails governor sends proc_disable_service RPC to every Ok instance so far
    {
        let mut disabled_ok = HashSet::new();

        while !pretend_state.get().governor_idle {
            let rpcs = rpc_requests.receive_all(Duration::from_millis(50));
            if rpcs.is_empty() {
                // Only loop here until all outstanding RPCs are handled
                break;
            }

            // RPCs are sent out in batches of given size (may be shorter)
            assert!(rpcs.len() <= params.batch_size);

            for rpc in rpcs {
                // Verify RPC request
                let (id, request) = rpc;
                let RaftIdOrName::Name(name) = id else {
                    panic!("expected instance name, got {id:?}");
                };

                assert_eq!(request.proc, ".proc_disable_service");
                let resp = rpc::disable_service::Response {};

                // Remember which RPCs got an Ok response
                assert!(
                    !disabled_ok.contains(&name),
                    "{name} got a repeat RPC after Ok response"
                );
                disabled_ok.insert(name);

                // Send the RPC response to governor
                let fail = rng.gen_bool(params.p_rpc_err);
                if fail {
                    request
                        .on_result
                        .handle(Err(Error::other("injected error")));

                    continue;
                }

                request.on_result.handle(Ok(Tuple::new(&[resp]).unwrap()));
            }
        }

        assert_eq!(disabled_ok, ok_so_far);
    }

    // Allow governor to handle RPC results and send the CAS requests
    fiber::reschedule();

    {
        let cas_requests = cas_requests.try_receive_all();
        let mut cas_requests = cas_requests.into_iter();
        let (request, response) = cas_requests.next().unwrap();

        let Op::Plugin(PluginRaftOp::Abort { cause }) = &request.op else {
            panic!("expected PluginRaftOp::Abort, got {:?}", request.op);
        };

        assert_eq!(cause.message, "injected error");

        // Don't apply CAS request
        response.send(Err(Error::other("injected error"))).unwrap();

        // Expecting only 1 CAS request
        assert_eq!(cas_requests.next().map(|(r, _)| r), None);
    }

    //
    // Now let's try again without failing a single RPC so that the operation succeeds
    //

    let mut first_cas_request_skipped = false;
    let mut cas_request_handled = false;
    let mut stats = BatchingRpcTestStats::default();

    while !pretend_state.get().governor_idle {
        //
        // Handle governor's RPCs
        //

        let rpcs = rpc_requests.receive_all(Duration::from_millis(50));

        stats.num_batches += 1;
        tlog!(
            Info,
            "RPC batch #{} size: {}",
            stats.num_batches,
            rpcs.len()
        );

        let mut targets = vec![];

        for rpc in rpcs {
            // Verify RPC request
            let (id, request) = rpc;
            let RaftIdOrName::Name(name) = id else {
                panic!("expected instance name, got {id:?}");
            };
            targets.push(name.clone());

            assert_eq!(request.proc, ".proc_enable_service");
            let resp = rpc::enable_service::Response {};

            // Send the RPC response to governor
            request.on_result.handle(Ok(Tuple::new(&[resp]).unwrap()));

            // Remember which RPCs got an Ok response
            assert!(
                !ok_so_far.contains(&name),
                "{name} got a repeat RPC after Ok response"
            );
            ok_so_far.insert(name);
        }

        // RPCs are sent out in batches of given size (may be shorter)
        assert!(targets.len() <= params.batch_size, "{targets:?}");

        // Allow governor to handle RPC results and send the CAS requests
        fiber::reschedule();

        //
        // Handle governor's CAS requests
        //

        let mut actual_ok_targets = HashSet::new();

        let cas_requests = cas_requests.try_receive_all();
        tlog!(Info, "CAS batch size: {}", cas_requests.len());

        if !cas_requests.is_empty() {
            // Make sure CAS request is only sent once every instance handled the RPC
            assert_eq!(ok_so_far.len(), params.num_instances);

            // Verify CAS request
            assert_ne!(node.storage.properties.pending_plugin_op().unwrap(), None);

            let mut cas_requests = cas_requests.into_iter();
            let (request, response) = cas_requests.next().unwrap();

            let mut seen_dml_alter_service_def = false;
            let mut seen_dml_delete_pending_plugin_op = false;

            let dmls = request.op.dmls().unwrap();
            for dml in dmls {
                if let Dml::Replace {
                    table: crate::storage::ServiceRouteTable::TABLE_ID,
                    tuple,
                    initiator,
                } = dml
                {
                    assert_eq!(*initiator, ADMIN_ID);

                    let item: ServiceRouteItem = rmp_serde::from_slice(tuple.as_ref()).unwrap();

                    assert_eq!(item.plugin_name, plugin_def.name);
                    assert_eq!(item.service_name, service_def.name);
                    assert_eq!(item.plugin_version, plugin_def.version);
                    assert_eq!(item.poison, false);

                    actual_ok_targets.insert(item.instance_name);
                    continue;
                };

                if let Dml::Replace {
                    table: crate::storage::Services::TABLE_ID,
                    tuple,
                    initiator,
                } = dml
                {
                    assert_eq!(*initiator, ADMIN_ID);

                    let new_service: ServiceDef = rmp_serde::from_slice(tuple.as_ref()).unwrap();
                    assert_eq!(new_service.plugin_name, service_def.plugin_name);
                    assert_eq!(new_service.name, service_def.name);
                    assert_eq!(new_service.version, service_def.version);
                    assert_eq!(new_service.description, service_def.description);
                    for old_tier in &service_def.tiers {
                        assert!(
                            new_service.tiers.contains(old_tier),
                            "{old_tier:?} not in {new_service:?}"
                        );
                    }
                    assert!(
                        !service_def.tiers.contains(&add_tier),
                        "{add_tier:?} in {service_def:?}"
                    );
                    assert!(
                        new_service.tiers.contains(&add_tier),
                        "{add_tier:?} not in {new_service:?}"
                    );

                    assert!(!seen_dml_alter_service_def);
                    seen_dml_alter_service_def = true;
                    continue;
                }

                if let Dml::Delete {
                    table: Properties::TABLE_ID,
                    key,
                    initiator,
                    metainfo,
                } = dml
                {
                    assert_eq!(*initiator, ADMIN_ID);
                    assert_eq!(metainfo, &None);

                    let [name]: [PropertyName; 1] = rmp_serde::from_slice(key.as_ref()).unwrap();
                    assert_eq!(name, PropertyName::PendingPluginOperation);

                    assert!(!seen_dml_delete_pending_plugin_op);
                    seen_dml_delete_pending_plugin_op = true;
                    continue;
                }

                panic!("unexpected {dml:?}");
            }

            assert!(seen_dml_alter_service_def);
            assert!(seen_dml_delete_pending_plugin_op);

            // Make sure all instances which responded Ok get a corresponding CAS request
            assert_eq!(ok_so_far, actual_ok_targets);

            // Apply CAS request
            let fail = rng.gen_bool(params.p_cas_err);
            if
            // Fail the first attempt always just to check what how we handle that
            first_cas_request_skipped
                    // Next time fail randomly
                    && !fail
            {
                response.send(Ok(cas::Response::for_tests())).unwrap();

                node.storage
                    .properties
                    .delete(PropertyName::PendingPluginOperation)
                    .unwrap();
                cas_request_handled = true;
            } else {
                // It's possible that the CAS request get's dropped due to
                // a conflict. Governor should handle this fine
                response.send(Err(Error::other("injected error"))).unwrap();
                stats.num_cas_fails += 1;
                first_cas_request_skipped = true;
            }

            // Expecting only 1 CAS request
            assert_eq!(cas_requests.next().map(|(r, _)| r), None);
        }
    }

    assert!(cas_request_handled);

    // Log the stats
    let s = pretend_state.get();
    tlog!(Info, "stats: {params:#0.3?}, {s:#0.3?}, {stats:#0.3?}")
}

//
// test ALTER PLUGIN REMOVE SERVICE FROM TIER step
//

#[tarantool::test]
fn test_governor_loop_alter_plugin_remove_service_from_tier_batching_fixed() {
    // Run with a fixed seed each time for some stability
    let seed = 1770229753053178;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 102,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.8,
        replication_factor: 3,
    };

    do_governor_loop_alter_plugin_remove_service_from_tier_batching(params);
}

#[tarantool::test]
fn test_governor_loop_alter_plugin_remove_service_from_tier_batching_random() {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    // And also run with a random seed for chaos
    let seed = (time.as_secs_f64() * 1000_000.0) as u64;

    let params = BatchingRpcTestParameters {
        seed,
        num_instances: 102,
        batch_size: 20,
        p_rpc_err: 0.666,
        p_cas_err: 0.8,
        replication_factor: 3,
    };

    do_governor_loop_alter_plugin_remove_service_from_tier_batching(params);
}

fn do_governor_loop_alter_plugin_remove_service_from_tier_batching(
    params: BatchingRpcTestParameters,
) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(params.seed);
    tlog!(Info, "random seed: {}", params.seed);

    let node = node::Node::for_tests();

    //
    // Setup cluster topology
    //

    let mut instances = vec![];
    for raft_id in 1..=(params.num_instances as RaftId) {
        instances.push(dummy_instance(raft_id));
    }

    setup_topology(node, &instances, None, None);

    //
    // Setup the pending ALTER PLUGIN REMOVE SERVICE FROM TIER operation
    //
    let remove_tier = instances[0].tier.clone();

    let mut plugin_def = PluginDef::for_tests();
    plugin_def.enabled = true;
    let mut service_def = ServiceDef::for_tests();
    service_def.tiers = vec![remove_tier.clone()];

    node.storage.plugins.put(&plugin_def).unwrap();

    node.storage.services.put(&service_def).unwrap();

    let plugin_op = PluginOp::AlterServiceTiers {
        plugin: plugin_def.identifier(),
        service: service_def.name.clone(),
        tier: remove_tier.clone(),
        kind: crate::plugin::TopologyUpdateOpKind::Remove,
    };
    node.storage
        .properties
        .put(PropertyName::PendingPluginOperation, &plugin_op)
        .unwrap();

    //
    // Setup other things
    //

    node.alter_system_parameters
        .borrow_mut()
        .governor_rpc_batch_size = params.batch_size;

    let (state, _, _, rpc_requests, cas_requests) = setup_governor_loop_state(node);

    //
    // Spawn the pretend governor_loop fiber
    //
    let pretend_state = spawn_pretend_governor_loop(state);

    //
    // Loop until pretend governor_loop finishes, handle governor's RPC & CAS requests
    //

    let mut ok_so_far = HashSet::new();
    let mut first_cas_request_skipped = false;
    let mut cas_request_handled = false;
    let mut stats = BatchingRpcTestStats::default();

    while !pretend_state.get().governor_idle {
        //
        // Handle governor's RPCs
        //

        let rpcs = rpc_requests.receive_all(Duration::from_millis(50));

        stats.num_batches += 1;
        tlog!(
            Info,
            "RPC batch #{} size: {}",
            stats.num_batches,
            rpcs.len()
        );

        let mut targets = vec![];

        for rpc in rpcs {
            // Verify RPC request
            let (id, request) = rpc;
            let RaftIdOrName::Name(name) = id else {
                panic!("expected instance name, got {id:?}");
            };
            targets.push(name.clone());

            assert_eq!(request.proc, ".proc_disable_service");
            let resp = rpc::disable_service::Response {};

            // Send the RPC response to governor
            let fail = rng.gen_bool(params.p_rpc_err);
            if fail {
                request
                    .on_result
                    .handle(Err(Error::other("injected error")));
                stats.num_rpc_fails += 1;

                continue;
            }

            // Send the RPC response to governor
            request.on_result.handle(Ok(Tuple::new(&[resp]).unwrap()));

            // Remember which RPCs got an Ok response
            assert!(
                !ok_so_far.contains(&name),
                "{name} got a repeat RPC after Ok response"
            );
            ok_so_far.insert(name);
        }

        // RPCs are sent out in batches of given size (may be shorter)
        assert!(targets.len() <= params.batch_size, "{targets:?}");

        // Allow governor to handle RPC results and send the CAS requests
        fiber::reschedule();

        //
        // Handle governor's CAS requests
        //

        let mut actual_ok_targets = HashSet::new();

        let cas_requests = cas_requests.try_receive_all();
        tlog!(Info, "CAS batch size: {}", cas_requests.len());

        if !cas_requests.is_empty() {
            // Make sure CAS request is only sent once every instance handled the RPC
            assert_eq!(ok_so_far.len(), params.num_instances);

            // Verify CAS request
            assert_ne!(node.storage.properties.pending_plugin_op().unwrap(), None);

            let mut cas_requests = cas_requests.into_iter();
            let (request, response) = cas_requests.next().unwrap();

            let mut seen_dml_alter_service_def = false;
            let mut seen_dml_delete_pending_plugin_op = false;

            let dmls = request.op.dmls().unwrap();
            for dml in dmls {
                if let Dml::Delete {
                    table: crate::storage::ServiceRouteTable::TABLE_ID,
                    key,
                    initiator,
                    metainfo,
                } = dml
                {
                    assert_eq!(*initiator, ADMIN_ID);
                    assert_eq!(metainfo, &None);

                    let key: ServiceRouteKey = rmp_serde::from_slice(key.as_ref()).unwrap();

                    assert_eq!(key.plugin_name, plugin_def.name);
                    assert_eq!(key.service_name, service_def.name);
                    assert_eq!(key.plugin_version, plugin_def.version);

                    actual_ok_targets.insert(InstanceName::from(key.instance_name));
                    continue;
                };

                if let Dml::Replace {
                    table: crate::storage::Services::TABLE_ID,
                    tuple,
                    initiator,
                } = dml
                {
                    assert_eq!(*initiator, ADMIN_ID);

                    let new_service: ServiceDef = rmp_serde::from_slice(tuple.as_ref()).unwrap();
                    assert_eq!(new_service.plugin_name, service_def.plugin_name);
                    assert_eq!(new_service.name, service_def.name);
                    assert_eq!(new_service.version, service_def.version);
                    assert_eq!(new_service.description, service_def.description);
                    for new_tier in &new_service.tiers {
                        assert!(
                            service_def.tiers.contains(new_tier),
                            "{new_tier:?} not in {service_def:?}"
                        );
                    }
                    assert!(
                        service_def.tiers.contains(&remove_tier),
                        "{remove_tier:?} in {service_def:?}"
                    );
                    assert!(
                        !new_service.tiers.contains(&remove_tier),
                        "{remove_tier:?} not in {new_service:?}"
                    );

                    assert!(!seen_dml_alter_service_def);
                    seen_dml_alter_service_def = true;
                    continue;
                }

                if let Dml::Delete {
                    table: Properties::TABLE_ID,
                    key,
                    initiator,
                    metainfo,
                } = dml
                {
                    assert_eq!(*initiator, ADMIN_ID);
                    assert_eq!(metainfo, &None);

                    let [name]: [PropertyName; 1] = rmp_serde::from_slice(key.as_ref()).unwrap();
                    assert_eq!(name, PropertyName::PendingPluginOperation);

                    assert!(!seen_dml_delete_pending_plugin_op);
                    seen_dml_delete_pending_plugin_op = true;
                    continue;
                }

                panic!("unexpected {dml:?}");
            }

            assert!(seen_dml_alter_service_def);
            assert!(seen_dml_delete_pending_plugin_op);

            // Make sure all instances which responded Ok get a corresponding CAS request
            assert_eq!(ok_so_far, actual_ok_targets);

            // Apply CAS request
            let fail = rng.gen_bool(params.p_cas_err);
            if
            // Fail the first attempt always just to check what how we handle that
            first_cas_request_skipped
                    // Next time fail randomly
                    && !fail
            {
                response.send(Ok(cas::Response::for_tests())).unwrap();

                node.storage
                    .properties
                    .delete(PropertyName::PendingPluginOperation)
                    .unwrap();
                cas_request_handled = true;
            } else {
                // It's possible that the CAS request get's dropped due to
                // a conflict. Governor should handle this fine
                response.send(Err(Error::other("injected error"))).unwrap();
                stats.num_cas_fails += 1;
                first_cas_request_skipped = true;
            }

            // Expecting only 1 CAS request
            assert_eq!(cas_requests.next().map(|(r, _)| r), None);
        }
    }

    assert!(cas_request_handled);

    // Log the stats
    let s = pretend_state.get();
    tlog!(Info, "stats: {params:#0.3?}, {s:#0.3?}, {stats:#0.3?}")
}
