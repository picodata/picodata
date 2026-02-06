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
use crate::replicaset::Replicaset;
use crate::replicaset::ReplicasetState;
use crate::rpc;
use crate::rpc::replicasets_masters;
use crate::schema::Distribution;
use crate::schema::ADMIN_ID;
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
        // Callers wants to control the tier setup
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
    num_instances: usize,
    replication_factor: usize,
) -> (Vec<Instance>, Vec<Replicaset>) {
    let mut instances = vec![];
    let mut replicasets = vec![];
    let mut last_replicaset_size = 0;

    for raft_id in 1..=(num_instances as RaftId) {
        let mut instance = dummy_instance(raft_id);

        if replicasets.is_empty() || last_replicaset_size == replication_factor {
            let tier_name = &instance.tier;
            let n = replicasets.len() + 1;
            instance.replicaset_name = format_smolstr!("{tier_name}_{n}").into();
            instance.replicaset_uuid = uuid_v4();

            let mut replicaset = Replicaset::with_one_instance(&instance);
            replicaset.state = ReplicasetState::Ready;
            replicasets.push(replicaset);
            last_replicaset_size = 1;
        } else {
            let replicaset = replicasets.last().unwrap();
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

fn masters_and_replicas(topology_ref: &TopologyCacheRef) -> (HashSet<InstanceName>, HashSet<InstanceName>) {
    let mut masters = HashSet::new();
    let mut replicas = HashSet::new();

    for instance in topology_ref.all_instances() {
        let replicaset = topology_ref.replicaset_by_uuid(&instance.replicaset_uuid).unwrap();
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

                assert!(!ok_so_far.contains(&name), "got a repeat CAS request for instance {name}");
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
            assert!(!ok_so_far.contains(&name), "{name} got a repeat RPC after Ok response");
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
        generate_replicasets(params.num_instances, params.replication_factor);
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
            assert!(!ok_so_far.contains(&name), "{name} got a repeat RPC after Ok response");
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
        generate_replicasets(params.num_instances, params.replication_factor);

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
                assert_eq!(ok_replicas.len(), 0, "replicas only get RPCs after all masters");
                assert!(!ok_masters.contains(&name), "{name} got a repeat RPC after Ok response");
                ok_masters.insert(name);
            } else {
                assert_eq!(ok_masters.len(), all_masters.len(), "replicas only get RPCs after all masters");
                assert!(!ok_replicas.contains(&name), "{name} got a repeat RPC after Ok response");
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
