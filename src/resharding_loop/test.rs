//! The resharding tests themselves. The simulation they run in lives in
//! [`crate::simulation`].

use crate::catalog::pico_bucket::BucketIdRange;
use crate::catalog::pico_bucket::BucketRecord;
use crate::instance::Instance;
use crate::replicaset::Replicaset;
use crate::resharding_loop::action::ReshardingActionKind;
use crate::resharding_loop::ReshardingStatus;
use crate::simulation::action::PretendAction;
use crate::simulation::action_helpers::assert_bucket_partition;
use crate::simulation::action_helpers::catch_up_on_raft_log;
use crate::simulation::action_helpers::catch_up_on_sharded_wal;
use crate::simulation::action_helpers::commit_master_switchover;
use crate::simulation::action_helpers::crash_instance;
use crate::simulation::action_helpers::drop_next_cas_request;
use crate::simulation::action_helpers::request_resharding_status;
use crate::simulation::action_helpers::restart_instance;
use crate::simulation::action_helpers::run_until_cas_request_is_outstanding;
use crate::simulation::action_helpers::run_until_last_action;
use crate::simulation::action_helpers::set_sharded_replication_paused;
use crate::simulation::action_helpers::timeout_cas_wait;
use crate::simulation::cluster::PretendCluster;
use crate::simulation::engine::run_until;
use crate::simulation::engine::run_until_converged;
use crate::simulation::engine::step_once;
use crate::simulation::engine::MAX_SAFE_ITERATIONS;
use crate::simulation::raft::PretendCasOutcome;
use crate::simulation::sharding::first_half;
use crate::simulation::sharding::full_range;
use crate::simulation::sharding::second_half;
use crate::tier::Tier;
use crate::tlog;
use crate::traft::RaftId;
use crate::util::test_rng_seed;
use smol_str::SmolStr;
use std::rc::Rc;

struct ReshardingTestParameters {
    seed: u64,
}

impl ReshardingTestParameters {
    fn with_seed(seed: u64) -> Self {
        Self { seed }
    }

    fn with_fresh_seed() -> Self {
        let seed = test_rng_seed();
        Self { seed }
    }
}

fn pretend_buckets_range(
    range: BucketIdRange,
    tier: &Tier,
    replicaset: &Replicaset,
) -> BucketRecord {
    BucketRecord::new_active(
        tier.name.clone(),
        *range.start(),
        *range.end(),
        replicaset.name.clone().into(),
    )
}

pub type PretendTierSpec<'a> = (&'a str, u64, &'a [PretendReplicasetSpec<'a>]);
pub type PretendReplicasetSpec<'a> = (
    &'a str,
    PretendInstanceSpec<'a>,
    &'a [PretendInstanceSpec<'a>],
    BucketIdRange,
);
pub type PretendInstanceSpec<'a> = (RaftId, &'a str);

pub fn setup_by_spec(seed: u64, tier_specs: &[PretendTierSpec<'_>]) -> Rc<PretendCluster> {
    let mut instances = vec![];
    let mut replicasets = vec![];
    let mut tiers = vec![];
    let mut bucket_records = vec![];

    for (tier_name, bucket_count, replicaset_specs) in tier_specs {
        let tier = pretend_tier_info(tier_name, *bucket_count);

        for (replicaset_name, master_spec, replica_specs, initial_buckets) in *replicaset_specs {
            let (master_raft_id, master_name) = master_spec;
            let replicaset = pretend_replicaset_info(replicaset_name, master_name, &tier.name);

            instances.push(pretend_instance_info(
                *master_raft_id,
                master_name,
                &replicaset,
            ));

            for (raft_id, name) in *replica_specs {
                instances.push(pretend_instance_info(*raft_id, name, &replicaset));
            }

            // An empty range means the replicaset joins the topology owning
            // nothing — there is no such thing as an empty `_pico_bucket`
            // record, so it simply gets none.
            if !initial_buckets.is_empty() {
                bucket_records.push(pretend_buckets_range(
                    initial_buckets.clone(),
                    &tier,
                    &replicaset,
                ));
            }
            replicasets.push(replicaset);
        }

        tiers.push(tier);
    }

    PretendCluster::setup(seed, &instances, &replicasets, &tiers, &bucket_records)
}

pub fn uuid_v4() -> SmolStr {
    smol_str::format_smolstr!("{}", uuid::Uuid::new_v4().to_hyphenated())
}

pub fn pretend_instance_info(
    raft_id: RaftId,
    name: impl AsRef<str>,
    replicaset: &Replicaset,
) -> Instance {
    let mut instance = Instance::for_tests();
    instance.raft_id = raft_id;
    instance.name = name.as_ref().into();
    instance.uuid = uuid_v4();
    instance.replicaset_name = replicaset.name.clone();
    instance.replicaset_uuid = replicaset.uuid.clone();
    instance.tier = replicaset.tier.clone();
    instance
}

pub fn pretend_replicaset_info(
    name: impl AsRef<str>,
    master_name: impl AsRef<str>,
    tier_name: impl AsRef<str>,
) -> Replicaset {
    let mut replicaset = Replicaset::for_tests();
    replicaset.name = name.as_ref().into();
    replicaset.uuid = uuid_v4();
    replicaset.current_master_name = master_name.as_ref().into();
    replicaset.target_master_name = master_name.as_ref().into();
    replicaset.tier = tier_name.as_ref().into();
    replicaset.current_bucket_state_version = 0;
    replicaset.target_bucket_state_version = 1;
    replicaset
}

pub fn pretend_tier_info(name: impl AsRef<str>, tier_bucket_count: u64) -> Tier {
    let mut tier = Tier::default();
    tier.name = name.as_ref().into();
    tier.bucket_count = tier_bucket_count;
    tier
}

////////////////////////////////////////////////////////////////////////////////
// timeout verdicts
////////////////////////////////////////////////////////////////////////////////

/// Injects a timeout error into every single unique timeoutable wait which can
/// happen during a sharding initialization round and makes sure the simulation
/// still convergence, meaning that the production code handles those timeout
/// errors gracefully.
fn do_simulation_timeout_verdicts_retry(params: ReshardingTestParameters) {
    tlog!(Info, "random seed: {}", params.seed);

    let (red_tier, red_replicaset, red_master, red_count) = ("red", "red_1", "red_1_1", 3000);
    let (blue_tier, blue_replicaset, blue_master, blue_count) =
        ("blue", "blue_1", "blue_1_1", 1500);
    let cluster = setup_by_spec(
        params.seed,
        &[
            (
                red_tier,
                red_count,
                &[(red_replicaset, (2, red_master), &[], full_range(red_count))],
            ),
            (
                blue_tier,
                blue_count,
                &[(
                    blue_replicaset,
                    (3, blue_master),
                    &[],
                    full_range(blue_count),
                )],
            ),
        ],
    );

    // Set the special flag which enables the "time out each unique wait once" mode
    cluster.set_timeout_each_wait_once(true);

    request_resharding_status(&cluster, red_master, ReshardingStatus::Initialize);
    request_resharding_status(&cluster, blue_master, ReshardingStatus::Initialize);
    run_until_converged(&cluster);

    let fired_timeout = cluster
        .trace
        .borrow()
        .iter()
        .any(|e| matches!(e, PretendAction::TimeoutWait { .. }));
    assert!(
        fired_timeout,
        "no fiber waits were timed out during the run\n{}",
        cluster.dump_state(),
    );
}

#[::tarantool::test]
fn simulation_timeout_verdicts_retry_fixed() {
    let params = ReshardingTestParameters::with_seed(1770229753053183);
    do_simulation_timeout_verdicts_retry(params);
}

#[::tarantool::test]
fn simulation_timeout_verdicts_retry_random() {
    do_simulation_timeout_verdicts_retry(ReshardingTestParameters::with_fresh_seed());
}

////////////////////////////////////////////////////////////////////////////////
// dropped CAS proposals
////////////////////////////////////////////////////////////////////////////////

/// Check that a dropped CAS proposal is succesfully retried eventually.
fn do_simulation_cas_proposal_dropped(params: ReshardingTestParameters) {
    tlog!(Info, "random seed: {}", params.seed);

    let (tier, replicaset, master, bucket_count) = ("red", "red_1", "red_1_1", 3000);
    let cluster = setup_by_spec(
        params.seed,
        &[(
            tier,
            bucket_count,
            &[(replicaset, (2, master), &[], full_range(bucket_count))],
        )],
    );

    request_resharding_status(&cluster, master, ReshardingStatus::Initialize);
    run_until_cas_request_is_outstanding(&cluster, master);
    drop_next_cas_request(&cluster, master);
    run_until_converged(&cluster);

    assert!(
        cluster
            .instance(master)
            .loop_state
            .borrow()
            .last_error
            .is_some(),
        "the injected CAS drop never fired"
    );
}

#[::tarantool::test]
fn simulation_cas_proposal_dropped_fixed() {
    let params = ReshardingTestParameters::with_seed(1770229753053188);
    do_simulation_cas_proposal_dropped(params);
}

#[::tarantool::test]
fn simulation_cas_proposal_dropped_random() {
    do_simulation_cas_proposal_dropped(ReshardingTestParameters::with_fresh_seed());
}

/// Check that even if a CAS request succeeds, but response doesn't arrive to
/// the sender, the sender still eventually finds out about the success.
fn do_simulation_cas_committed_after_timeout(params: ReshardingTestParameters) {
    tlog!(Info, "random seed: {}", params.seed);

    let bucket_count = 3000;
    let (tier, replicaset, master) = ("red", "red_1", "red_1_1");
    let cluster = setup_by_spec(
        params.seed,
        &[(
            tier,
            bucket_count,
            &[(replicaset, (2, master), &[], full_range(bucket_count))],
        )],
    );

    request_resharding_status(&cluster, master, ReshardingStatus::Initialize);
    run_until_cas_request_is_outstanding(&cluster, master);
    timeout_cas_wait(&cluster, master);
    run_until_converged(&cluster);

    assert!(
        cluster
            .instance(master)
            .loop_state
            .borrow()
            .last_error
            .is_some(),
        "the fired timeout never reached the proposer"
    );

    // The whole point of the run: the abandoned proposal was still resolved,
    // and committed, after its proposer had been told it timed out.
    let trace = cluster.trace.borrow();
    let timed_out = trace
        .iter()
        .position(|action| matches!(action, PretendAction::TimeoutWait { .. }))
        .expect("the forced timeout is in the trace");
    let committed_after = trace[timed_out..].iter().any(|action| {
        matches!(
            action,
            PretendAction::ResolveCasRequest {
                outcome: Some(PretendCasOutcome::Committed { .. }),
                ..
            }
        )
    });
    assert!(
        committed_after,
        "the abandoned proposal never committed\n{}",
        cluster.dump_state(),
    );
}

#[::tarantool::test]
fn simulation_cas_committed_after_timeout_fixed() {
    let params = ReshardingTestParameters::with_seed(1770229753053189);
    do_simulation_cas_committed_after_timeout(params);
}

#[::tarantool::test]
fn simulation_cas_committed_after_timeout_random() {
    do_simulation_cas_committed_after_timeout(ReshardingTestParameters::with_fresh_seed());
}

////////////////////////////////////////////////////////////////////////////////
// a lagging replica's sharded stream
////////////////////////////////////////////////////////////////////////////////

/// Check that a lagging replica eventually catches up to the master.
fn do_simulation_replica_sharded_lag(params: ReshardingTestParameters) {
    tlog!(Info, "random seed: {}", params.seed);

    let (tier, replicaset) = ("red", "red_1");
    let (master_name, replica_name) = ("red_1_1", "red_1_2");
    let cluster = setup_by_spec(
        params.seed,
        &[(
            tier,
            3000,
            &[(replicaset, (2, master_name), &[(3, replica_name)], 1..=300)],
        )],
    );

    // The replica is cut off before the master writes anything at all.
    set_sharded_replication_paused(&cluster, replica_name, true);
    request_resharding_status(&cluster, master_name, ReshardingStatus::Initialize);
    request_resharding_status(&cluster, replica_name, ReshardingStatus::Initialize);

    // Drive to a standstill by hand: `run_until_converged` would assert every
    // instance has applied its whole sharded WAL, which is precisely what the
    // pause forbids.
    let mut steps = 0;
    while step_once(&cluster) {
        steps += 1;
        assert!(
            steps < MAX_SAFE_ITERATIONS,
            "simulation: gave up after {MAX_SAFE_ITERATIONS} steps\n{}",
            cluster.dump_state(),
        );
    }

    let master = cluster.instance(master_name);
    let replica = cluster.instance(replica_name);
    assert!(
        !master.local_buckets.borrow().is_empty(),
        "the master wrote nothing, so there is no lag to observe\n{}",
        cluster.dump_state(),
    );
    assert!(
        replica.local_buckets.borrow().is_empty(),
        "a replica with a paused stream must not have applied anything\n{}",
        cluster.dump_state(),
    );
    assert!(replica.sharded_lsn.get() < replica.sharded_wal.len());

    // Put the stream back and let it replay.
    set_sharded_replication_paused(&cluster, replica_name, false);
    run_until_converged(&cluster);
}

#[::tarantool::test]
fn simulation_replica_sharded_lag_fixed() {
    let params = ReshardingTestParameters {
        seed: 1770229753053187,
    };
    do_simulation_replica_sharded_lag(params);
}

#[::tarantool::test]
fn simulation_replica_sharded_lag_random() {
    do_simulation_replica_sharded_lag(ReshardingTestParameters::with_fresh_seed());
}

////////////////////////////////////////////////////////////////////////////////
// crash/restart
////////////////////////////////////////////////////////////////////////////////

/// Which stage of the initial distribution [`do_simulation_crash_restart`]
/// crashes an instance at.
#[derive(Clone, Copy, Debug)]
enum CrashCheckpoint {
    ActualizeShardedState,
    ActualizeBucketStateVersion,
}

/// Drives an instance to `checkpoint`, crashes it mid-action, restarts it,
/// nudges it back into action the way a governor noticing it came back would,
/// and asserts the distribution still converges to the same final state as
/// the crash-free run.
fn do_simulation_crash_restart(params: ReshardingTestParameters, checkpoint: CrashCheckpoint) {
    tlog!(
        Info,
        "random seed: {}, checkpoint: {checkpoint:?}",
        params.seed
    );

    let (tier, bucket_count) = ("red", 3000);
    let (crashed_replicaset, peer_replicaset) = ("red_1", "red_2");
    let (crashed, peer) = ("red_1_1", "red_2_1");
    // Both replicasets own half of the tier's buckets, so both have real
    // work to do and the crashed one is crashed with a peer still running.
    let cluster = setup_by_spec(
        params.seed,
        &[(
            tier,
            bucket_count,
            &[
                (
                    crashed_replicaset,
                    (2, crashed),
                    &[],
                    first_half(bucket_count),
                ),
                (peer_replicaset, (3, peer), &[], second_half(bucket_count)),
            ],
        )],
    );

    request_resharding_status(&cluster, crashed, ReshardingStatus::Initialize);
    request_resharding_status(&cluster, peer, ReshardingStatus::Initialize);

    match checkpoint {
        CrashCheckpoint::ActualizeShardedState => {
            run_until_last_action(
                &cluster,
                crashed,
                ReshardingActionKind::ActualizeShardedState,
            );
        }
        CrashCheckpoint::ActualizeBucketStateVersion => {
            run_until_last_action(
                &cluster,
                crashed,
                ReshardingActionKind::ActualizeBucketStateVersion,
            );
        }
    }

    crash_instance(&cluster, crashed);
    restart_instance(&cluster, crashed);
    // A restarted instance has no memory of what was requested of it, and a
    // real governor keeps re-poking until the versions actualize.
    request_resharding_status(&cluster, crashed, ReshardingStatus::Initialize);
    request_resharding_status(&cluster, peer, ReshardingStatus::Initialize);
    run_until_converged(&cluster);

    assert_bucket_partition(&cluster, tier, bucket_count);
}

#[::tarantool::test]
fn simulation_crash_restart_actualize_sharded_state_fixed() {
    let params = ReshardingTestParameters::with_seed(1770229753053184);
    do_simulation_crash_restart(params, CrashCheckpoint::ActualizeShardedState);
}

#[::tarantool::test]
fn simulation_crash_restart_actualize_bucket_state_version_fixed() {
    let params = ReshardingTestParameters::with_seed(1770229753053185);
    do_simulation_crash_restart(params, CrashCheckpoint::ActualizeBucketStateVersion);
}

#[::tarantool::test]
fn simulation_crash_restart_random() {
    let params = ReshardingTestParameters::with_fresh_seed();
    let seed = params.seed;
    let checkpoint = match seed % 2 {
        0 => CrashCheckpoint::ActualizeShardedState,
        _ => CrashCheckpoint::ActualizeBucketStateVersion,
    };
    do_simulation_crash_restart(params, checkpoint);
}

////////////////////////////////////////////////////////////////////////////////
// everything-at-once fault campaign
////////////////////////////////////////////////////////////////////////////////

/// Runs a simulation with all fault injection types enabled.
///
/// Asserts that every fault injection type was actually observed.
fn do_simulation_fault_campaign(params: ReshardingTestParameters) {
    tlog!(Info, "random seed: {}", params.seed);

    let (tier, bucket_count) = ("red", 3000);
    let (first_replicaset, second_replicaset) = ("red_1", "red_2");
    let (first_master, second_master) = ("red_1_1", "red_2_1");
    let cluster = setup_by_spec(
        params.seed,
        &[(
            tier,
            bucket_count,
            &[
                (
                    first_replicaset,
                    (2, first_master),
                    &[],
                    first_half(bucket_count),
                ),
                (
                    second_replicaset,
                    (3, second_master),
                    &[],
                    second_half(bucket_count),
                ),
            ],
        )],
    );

    // CAS fault probability needs to be higher, because they're more rare.
    cluster.set_cas_fault_probability(0.9);
    cluster.set_timeout_probability(0.5);
    request_resharding_status(&cluster, first_master, ReshardingStatus::Initialize);
    request_resharding_status(&cluster, second_master, ReshardingStatus::Initialize);
    run_until_converged(&cluster);

    // Off before the final settle, so the assertions below are about a
    // cluster which has finished recovering rather than one still being hit.
    cluster.set_cas_fault_probability(0.0);
    cluster.set_timeout_probability(0.0);
    run_until_converged(&cluster);

    assert_bucket_partition(&cluster, tier, bucket_count);

    let trace = cluster.trace.borrow();
    let cas_faults = trace
        .iter()
        .filter(|action| {
            matches!(
                action,
                PretendAction::ResolveCasRequest {
                    outcome: Some(PretendCasOutcome::Dropped),
                    ..
                }
            )
        })
        .count();
    assert!(
        cas_faults > 0,
        "the CAS half of the campaign never fired\n{}",
        cluster.dump_state(),
    );

    let timeouts = trace
        .iter()
        .filter(|action| matches!(action, PretendAction::TimeoutWait { .. }))
        .count();
    assert!(
        timeouts > 0,
        "the timeout half of the campaign never fired\n{}",
        cluster.dump_state(),
    );
}

#[::tarantool::test]
fn simulation_fault_campaign_fixed() {
    let params = ReshardingTestParameters::with_seed(1770229753053191);
    do_simulation_fault_campaign(params);
}

#[::tarantool::test]
fn simulation_fault_campaign_random() {
    do_simulation_fault_campaign(ReshardingTestParameters::with_fresh_seed());
}

////////////////////////////////////////////////////////////////////////////////
// master switchover
////////////////////////////////////////////////////////////////////////////////

/// Checks that master switchover is handled correctly during initial sharding.
fn do_simulation_master_switchover(params: ReshardingTestParameters) {
    tlog!(Info, "random seed: {}", params.seed);

    let (tier, bucket_count) = ("red", 3000);
    let (switching_replicaset, other_replicaset) = ("red_1", "red_2");
    let (old_master, new_master) = ("red_1_1", "red_1_2");
    let (other_master, other_replica) = ("red_2_1", "red_2_2");
    let cluster = setup_by_spec(
        params.seed,
        &[(
            tier,
            bucket_count,
            &[
                // A master and a replica, owning the first half of the tier.
                (
                    switching_replicaset,
                    (2, old_master),
                    &[(3, new_master)],
                    first_half(bucket_count),
                ),
                // The other replicaset owns the second half, and just gets on with it.
                (
                    other_replicaset,
                    (4, other_master),
                    &[(5, other_replica)],
                    second_half(bucket_count),
                ),
            ],
        )],
    );

    for name in cluster.instance_names() {
        request_resharding_status(&cluster, &name, ReshardingStatus::Initialize);
    }

    // Wait until old master moves a bucket.
    run_until(
        &cluster,
        || {
            !cluster
                .instance(old_master)
                .local_buckets
                .borrow()
                .is_empty()
        },
        &format!("simulation converged before {old_master} made any progress"),
    );

    // Clean switchover: catch the replica up with the sharded WAL first,
    // then flip. The nudge to the new master must land *before*
    // `catch_up_on_raft_log` lets it
    // discover it's master, production reads `want_status` once at
    // `resharding_loop()` entry, so if the flip is observed before a fresh
    // request lands, the promoted replica would (re-)run the stale
    // `requested_status` instead.
    catch_up_on_sharded_wal(&cluster, new_master);
    commit_master_switchover(&cluster, switching_replicaset, new_master);
    request_resharding_status(&cluster, new_master, ReshardingStatus::Initialize);
    catch_up_on_raft_log(&cluster);
    run_until_converged(&cluster);

    assert_bucket_partition(&cluster, tier, bucket_count);
    assert!(
        cluster.instance(new_master).is_master(),
        "{new_master} must be {switching_replicaset}'s master now"
    );
    assert!(
        !cluster.instance(old_master).is_master(),
        "{old_master} must be demoted"
    );
}

#[::tarantool::test]
fn simulation_master_switchover_fixed() {
    let params = ReshardingTestParameters::with_seed(1770229753053190);
    do_simulation_master_switchover(params);
}

#[::tarantool::test]
fn simulation_master_switchover_random() {
    do_simulation_master_switchover(ReshardingTestParameters::with_fresh_seed());
}

////////////////////////////////////////////////////////////////////////////////
// trace determinism
////////////////////////////////////////////////////////////////////////////////

/// The determinism seal: the same seed must produce the same action trace and
/// the same final state, every time. Everything else in this file is only
/// worth anything if this holds.
#[::tarantool::test]
pub fn simulation_trace_is_deterministic() {
    const RERUNS: usize = 5;

    {
        let seed = 1770229753053178;
        let (red_count, blue_count) = (3000, 1500);
        let (red_tier, red_replicaset, red_master) = ("red", "red_1", "red_1_1");
        let (blue_tier, blue_replicaset, blue_master) = ("blue", "blue_1", "blue_1_1");
        let mut traces = Vec::with_capacity(RERUNS);
        let mut final_states = Vec::with_capacity(RERUNS);

        for _ in 0..RERUNS {
            let cluster = setup_by_spec(
                seed,
                &[
                    (
                        red_tier,
                        red_count,
                        &[(red_replicaset, (2, red_master), &[], full_range(red_count))],
                    ),
                    (
                        blue_tier,
                        blue_count,
                        &[(
                            blue_replicaset,
                            (3, blue_master),
                            &[],
                            full_range(blue_count),
                        )],
                    ),
                ],
            );

            request_resharding_status(&cluster, red_master, ReshardingStatus::Initialize);
            request_resharding_status(&cluster, blue_master, ReshardingStatus::Initialize);
            run_until_converged(&cluster);

            traces.push(cluster.trace.borrow().clone());
            final_states.push((
                cluster.instance(red_master).local_buckets.borrow().clone(),
                cluster.instance(blue_master).local_buckets.borrow().clone(),
            ));
        }

        for i in 1..RERUNS {
            assert_eq!(
                traces[0], traces[i],
                "[two_tier] action trace diverged on rerun {i}"
            );
            assert_eq!(
                final_states[0], final_states[i],
                "[two_tier] final cluster state diverged on rerun {i}"
            );
        }
    }

    {
        let seed = 1770229753053180;
        let bucket_count = 3000;
        let tier = "red";
        let (first_replicaset, second_replicaset) = ("red_1", "red_2");
        let (first_master, first_replica) = ("red_1_1", "red_1_2");
        let (second_master, second_replica) = ("red_2_1", "red_2_2");
        let mut traces = Vec::with_capacity(RERUNS);
        let mut final_states = Vec::with_capacity(RERUNS);

        for _ in 0..RERUNS {
            let cluster = setup_by_spec(
                seed,
                &[(
                    tier,
                    bucket_count,
                    &[
                        (
                            first_replicaset,
                            (2, first_master),
                            &[(3, first_replica)],
                            first_half(bucket_count),
                        ),
                        (
                            second_replicaset,
                            (4, second_master),
                            &[(5, second_replica)],
                            second_half(bucket_count),
                        ),
                    ],
                )],
            );

            for name in cluster.instance_names() {
                request_resharding_status(&cluster, &name, ReshardingStatus::Initialize);
            }
            run_until_converged(&cluster);

            traces.push(cluster.trace.borrow().clone());
            final_states.push(
                cluster
                    .instances()
                    .into_iter()
                    .map(|i| i.local_buckets.borrow().clone())
                    .collect::<Vec<_>>(),
            );
        }

        for i in 1..RERUNS {
            assert_eq!(
                traces[0], traces[i],
                "[replicated] action trace diverged on rerun {i}"
            );
            assert_eq!(
                final_states[0], final_states[i],
                "[replicated] final cluster state diverged on rerun {i}"
            );
        }
    }
}
