use crate::cas;
use crate::catalog::pico_bucket::BucketIdRange;
use crate::catalog::pico_bucket::BucketState;
use crate::column_name;
use crate::debug_assert_action_kind;
use crate::replicaset::Replicaset;
use crate::resharding_loop::action::ReshardingAction;
use crate::resharding_loop::action::*;
use crate::schema::ADMIN_ID;
use crate::storage::Replicasets;
use crate::storage::SystemTable;
use crate::tlog;
use crate::topology_cache::TopologyCacheRef;
use crate::traft::error::Error;
use crate::traft::node;
use crate::traft::op::Dml;
use crate::traft::op::Op;
use crate::traft::RaftIndex;
use crate::util::NoYieldsRefCell;
use crate::vshard;
use crate::vshard::VshardBucketRecord;
use crate::vshard::VshardBucketState;
use crate::vshard::VshardBucketState as VBS;
use crate::vshard::SPACE_BUCKET;
use crate::Result;
use ::tarantool::fiber::r#async::watch;
use find_sharded_bucket_updates::find_sharded_bucket_updates;
use smol_str::format_smolstr;
use smol_str::SmolStr;
use std::rc::Rc;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::error::IntoBoxError;
use tarantool::fiber;
use tarantool::fiber::r#async::timeout::IntoTimeout;
use tarantool::index::IteratorType;
use tarantool::space::UpdateOps;
use tarantool::time::Instant;
use tarantool::transaction::transaction;

mod action;
mod find_sharded_bucket_updates;

pub use action::ReshardingActionKind;

////////////////////////////////////////////////////////////////////////////////
// ReshardingLoop
////////////////////////////////////////////////////////////////////////////////

pub struct ReshardingLoop {
    /// Fiber id of the resharding_loop. Is used for waking up the fiber.
    fiber_id: fiber::FiberId,

    /// When governor sends us a RPC with a request to do resharding
    /// [`Self::do_resharding`] method is called which updates value in this
    /// field.
    /// Afterwards the value is checked in [`resharding_loop`] where the request is
    /// handled.
    /// Once the request is handled `actual_status` is updated.
    requested_status: watch::Sender<(ReshardingStatus, u64)>,

    /// Is updated to the value from `requested_status` in [`resharding_loop`]
    /// once the request is handled.
    pub actual_status: watch::Receiver<(ReshardingStatus, u64)>,

    /// Mutable long-living state of the resharding loop which is observable
    /// from other fibers.
    pub state: Rc<NoYieldsRefCell<ReshardingLoopState>>,
}

/// [`resharding_loop`] sleeps for this many seconds when it encounters an
/// unhandled error before going on another loop iteration.
const RESHARDING_LOOP_SHORT_RETRY: Duration = Duration::from_millis(300);

impl ReshardingLoop {
    pub fn start() -> Self {
        let (requested_status_tx, requested_status_rx) =
            watch::channel((ReshardingStatus::Idle, 0));
        let (actual_status_tx, actual_status_rx) = watch::channel((ReshardingStatus::Idle, 0));
        let state = ReshardingLoopState::default();
        let state = Rc::new(NoYieldsRefCell::new(state));
        let state_tx = state.clone();

        let fiber_id = fiber::Builder::new()
            .name("resharding_loop")
            .func_async(async move {
                let mut requested_status_rx = requested_status_rx;
                let mut actual_status_tx = actual_status_tx;
                loop {
                    let res =
                        resharding_loop(&mut requested_status_rx, &mut actual_status_tx, &state_tx)
                            .await;
                    match res {
                        Ok(()) => {
                            // TODO backoff
                        }
                        Err(e) => {
                            tlog!(Warning, "unhandled error: {e}");

                            state_tx.borrow_mut().last_error = Some(e.into_box_error());

                            _ = requested_status_rx
                                .changed()
                                .timeout(RESHARDING_LOOP_SHORT_RETRY)
                                .await;
                        }
                    }
                }
            })
            .defer_non_joinable()
            .expect("starting a fiber shouldn't fail")
            .expect("fiber id is supported");

        Self {
            fiber_id,
            requested_status: requested_status_tx,
            actual_status: actual_status_rx,
            state,
        }
    }

    pub async fn do_resharding(
        &self,
        deadline: Instant,
        request_status: ReshardingStatus,
    ) -> Result<()> {
        // Request is handled in `resharding_loop`.
        let waiting_for_version = self.request_action(request_status);

        loop {
            let (status, current_version) = self.actual_status.get();
            tlog!(Debug, "resharding status: {status:?}:{current_version}");
            if current_version >= waiting_for_version && status == ReshardingStatus::Idle {
                break;
            }

            // Value is updated in `resharding_loop`
            _ = self
                .actual_status
                .clone()
                .changed()
                .deadline(deadline)
                .await;

            if fiber::clock() > deadline {
                tlog!(Debug, "timed out waiting for resharding to complete");

                if let Some(last_error) = self.state.borrow().last_error.clone() {
                    return Err(last_error.into());
                }

                return Err(Error::timeout());
            }
        }

        tlog!(Debug, "resharding completed");

        Ok(())
    }

    #[inline]
    pub fn request_action(&self, status: ReshardingStatus) -> u64 {
        let version = self.actual_status.get().1;
        let waiting_for_version = version + 1;

        _ = self.requested_status.send((status, waiting_for_version));

        // The above call to `send` will awake the fiber if it's currently
        // blocked on the status channel. But it could also be blocked on a call
        // to `wait_index`, in which case this call will wake that fiber up.
        tlog!(Debug, "wake resharding loop up");
        fiber::wakeup(self.fiber_id);

        waiting_for_version
    }

    /// A dummy instance of this struct to be used in tests which don't care
    /// about the real behaviour.
    pub fn for_tests() -> Self {
        let (requested_status, _) = watch::channel((ReshardingStatus::Idle, 0));
        let (_, actual_status) = watch::channel((ReshardingStatus::Idle, 0));

        Self {
            fiber_id: 0,
            requested_status,
            actual_status,
            state: Default::default(),
        }
    }
}

/// An enumeration of states the `resharding_loop` can be in.
///
/// This is mainly internal implementation details, subject to change. Code in
/// other modules in general should not rely on these states to determine if
/// rebalancing is finished in the cluster and/or repliaset, instead the source
/// of truth should be consulted, i.e. `current_bucket_state_version` &
/// `target_bucket_state_version` columns in `_pico_tier` & `_pico_replicaset`.
///
/// See [`resharding_loop`] for illustrative usage examples.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReshardingStatus {
    /// No resharding action is being performed at the moment.
    #[default]
    Idle,
    /// Initial bucket distribution should be or is being created on this
    /// replicaset.
    Initialize,
    /// Buckets should be or are being rebalanced between this and some other
    /// replicasets.
    Resharding,
    /// Bucket rebalancing is being finalized on this replicaset.
    CleaningUp,
}

impl ReshardingStatus {
    #[inline(always)]
    pub fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }
}

#[derive(Default, Debug)]
pub struct ReshardingLoopState {
    pub last_action: ReshardingActionKind,

    /// If resharding_loop's iteration resulted in an error, it's value is
    /// stored here.
    pub last_error: Option<BoxError>,
}

////////////////////////////////////////////////////////////////////////////////
// resharding_loop
////////////////////////////////////////////////////////////////////////////////

/// The main loop for the resharding logic. It handles the requests which come
/// from governor in the form of updates to value in `requested_status`.
///
/// Once the request is handled the value in `actual_status` is updated to one
/// from `requested_status`.
///
/// `actual_status` will also be updated with intermidiate values mainly for
/// debugging purposes at this point.
///
/// `state` is the mutable state with information about the inner goings on of
/// the resharding loop mainly for debugging purposes.
async fn resharding_loop(
    requested_status: &mut watch::Receiver<(ReshardingStatus, u64)>,
    actual_status: &mut watch::Sender<(ReshardingStatus, u64)>,
    state: &Rc<NoYieldsRefCell<ReshardingLoopState>>,
) -> Result<()> {
    let (_curr_status, curr_version) = actual_status.get();
    // Is updated in `ReshardingLoop::do_resharding`
    let (want_status, next_version) = requested_status.get();
    tlog!(Debug, "requested status: {want_status:?} {next_version}");

    if want_status == ReshardingStatus::Idle || next_version == curr_version {
        // Sleep until a resharding action is requested
        _ = requested_status.changed().await;
        return Ok(());
    }

    let node = node::global()?;
    let my_instance_name = node.topology_cache.my_instance_name();
    let i_am_replicaset_master = node.topology_cache.with(|topology_ref| {
        topology_ref
            .this_replicaset()
            .effective_master_name()
            .map(|n| &**n)
            == Some(my_instance_name)
    });

    // Only master has the right to do resharding
    if !i_am_replicaset_master {
        tlog!(Debug, "not replicaset master, going to sleep");

        _ = node.wait_index_change(Duration::from_secs(10));
        return Ok(());
    }

    // XXX Maybe let's have a sepparate parameter for resharding timeouts?
    // But than again maybe let's not...
    let cas_timeout = node
        .alter_system_parameters
        .borrow()
        .governor_raft_op_timeout();

    let applied = node.get_index();

    let action = plan_resharding_action(state, want_status)?;
    let action_kind = action.kind();
    state.borrow_mut().last_action = action_kind;

    //
    // Execute action
    //

    type Action = ReshardingAction;
    match action {
        Action::ActualizeShardedState(ActualizeShardedState {
            from_state,
            to_state,
            range,
            changes,
        }) => {
            actual_status.send_modify(|(status, _)| *status = ReshardingStatus::Resharding);
            let (start, end) = range.into_inner();
            tlog!(Debug, "resharding_loop_status = '{action_kind}' (_bucket update {start}..{end} {from_state:?} -> '{to_state}')");

            log_bucket_changes(&changes, &node.topology_cache.get())?; // TODO(resharding): remove from final implementation

            transaction(|| -> Result<_> {
                for bucket in changes {
                    SPACE_BUCKET.replace(&bucket)?;
                }

                // TODO(sharding): update _schema.local_bucket_state_version so
                // that replicas can synchronize global _pico_bucket updates
                // with sharded table updates

                Ok(())
            })?;

            inspect_space_bucket(&node.topology_cache.get())?; // TODO(resharding): remove from final implementation
        }

        Action::ActualizeBucketStateVersion(ActualizeBucketStateVersion { version_bump }) => {
            actual_status.send_modify(|(status, _)| *status = ReshardingStatus::CleaningUp);
            tlog!(
                Debug,
                "resharding_loop_status = '{action_kind}' (_pico_replicaset set current_bucket_state_version = target_bucket_state_version)"
            );

            // Wait for vshard discovery to complete before bumping version
            // This ensures route_map is up-to-date on this instance
            let tier_name = node.topology_cache.my_tier_name();
            vshard::wait_router_discovery_complete(tier_name, cas_timeout)?;

            do_cas_requests(applied, version_bump.into_iter().collect(), cas_timeout)?;
        }

        Action::GoIdle => {
            tlog!(Debug, "resharding_loop_status = '{action_kind}'");

            inspect_space_bucket(&node.topology_cache.get())?; // TODO(resharding): remove from final implementation
            assert_all_buckets_rw()?; // TODO(resharding): remove from final implementation

            _ = actual_status.send((ReshardingStatus::Idle, next_version));
        }
    }

    tlog!(Debug, "done action");

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// plan_resharding_action
////////////////////////////////////////////////////////////////////////////////

fn plan_resharding_action(
    _state: &NoYieldsRefCell<ReshardingLoopState>,
    want_status: ReshardingStatus,
) -> Result<ReshardingAction> {
    let _guard = tarantool::fiber::NoYieldsGuard::with_message("no yields allowed here!");

    let node = node::global()?;

    let topology_ref = node.topology_cache.get();
    let this_replicaset = topology_ref.this_replicaset();

    ////////////////////////////////////////////////////////////////////////////
    //
    // check if bucket state version is already up to date
    //
    if this_replicaset.target_bucket_state_version == this_replicaset.current_bucket_state_version {
        // Version is already actualized which means no action is needed
        tlog!(Debug, "replicaset distribution version is actualized");

        return Ok(ReshardingAction::GoIdle);
    }

    //
    // handle initial bucket distribution
    //
    if want_status == ReshardingStatus::Initialize {
        let action = initialize_sharded_states(this_replicaset, &topology_ref)?;
        debug_assert_action_kind!(
            action,
            ReshardingAction::ActualizeBucketStateVersion { .. }
                | ReshardingAction::ActualizeShardedState { .. }
        );
        return Ok(action);
    }

    tlog!(Warning, "resharding not yet implemented");
    return Ok(ReshardingAction::GoIdle);
}

////////////////////////////////////////////////////////////////////////////////
// initialize_sharded_states
////////////////////////////////////////////////////////////////////////////////

fn initialize_sharded_states(
    this_replicaset: &Replicaset,
    topology_ref: &TopologyCacheRef,
) -> Result<ReshardingAction> {
    let bucket_ranges = &topology_ref.buckets_info(&this_replicaset.tier)?.ranges;

    let mut ranges: Vec<BucketIdRange> = vec![];

    //
    // Lookup bucket ranges of interest in _pico_bucket
    //
    for (range, _) in bucket_ranges.iter().zip(0..) {
        if range.current_replicaset_name != *this_replicaset.name {
            // Not our buckets
            continue;
        }

        if let BucketState::Unknown(unknown) = &range.state {
            // FIXME: rate limit the log message
            tlog!(
                Warning,
                "unknown bucket state '{unknown}' for bucket range {range:?}, going to skip"
            );
            continue;
        }

        debug_assert!(range.state.is_active(), "{range:?}");

        if let Some(last) = ranges.last() {
            debug_assert!(*last.end() < range.bucket_id_start, "must be ordered");
        }

        ranges.push(range.bucket_ids());
    }

    if ranges.is_empty() {
        // No action needed on this replicaset
        tlog!(
            Debug,
            "bucket distribution initialized, actualizing distribution version"
        );

        let version_bump = make_actualization_dmls(this_replicaset)?;

        return Ok(ReshardingAction::ActualizeBucketStateVersion(
            ActualizeBucketStateVersion { version_bump },
        ));
    }

    inspect_space_bucket(topology_ref)?; // TODO(resharding): remove from final implementation

    //
    // Check states in _bucket
    //

    for range in ranges.iter().cloned() {
        // Note: must first set as "receiving" because otherwise vshard's on_commit trigger will fail
        let (from_state, to_state) = (None, VBS::Receiving);
        let changes = find_sharded_bucket_updates(range.clone(), from_state, to_state, None)?;
        if !changes.is_empty() {
            return Ok(ReshardingAction::ActualizeShardedState(
                ActualizeShardedState {
                    from_state,
                    to_state,
                    range,
                    changes,
                },
            ));
        }

        let (from_state, to_state) = (Some(VBS::Receiving), VBS::Active);
        let changes = find_sharded_bucket_updates(range.clone(), from_state, to_state, None)?;
        if !changes.is_empty() {
            return Ok(ReshardingAction::ActualizeShardedState(
                ActualizeShardedState {
                    from_state,
                    to_state,
                    range,
                    changes,
                },
            ));
        }
    }

    //
    // All _bucket states are up to date with _pico_bucket
    //

    tlog!(
        Debug,
        "bucket distribution initialized, actualizing distribution version"
    );

    let version_bump = make_actualization_dmls(this_replicaset)?;

    Ok(ReshardingAction::ActualizeBucketStateVersion(
        ActualizeBucketStateVersion { version_bump },
    ))
}

////////////////////////////////////////////////////////////////////////////////
// miscellaneous
////////////////////////////////////////////////////////////////////////////////

fn make_actualization_dmls(replicaset: &Replicaset) -> Result<Vec<Dml>> {
    let mut dmls = vec![];

    let dml = Replicasets::dml_update(
        &[&replicaset.name],
        UpdateOps::new()
            // Actualize replicaset's bucket distribution version
            .into_assign(
                column_name!(Replicaset, current_bucket_state_version),
                replicaset.target_bucket_state_version,
            )?,
    );
    dmls.push(dml);

    Ok(dmls)
}

// FIXME currently all resharding steps are going to be serialized:
// - send data to r1
// - do cas and wait
// - send data to r2
// - do cas and wait
// ...
//
// It's better if we don't block awaiting the CAS response and instead save that
// future into an array and go send a data chunk to another replicaset. So it
// looks more like this:
// - send data to r1
// - do cas don't wait
// - send data to r2
// - do cas don't wait
// - ...
// - wait for all cas responses at once
fn do_cas_requests(applied: RaftIndex, dmls: Vec<Dml>, timeout: Duration) -> Result<()> {
    tlog!(Debug, "sending cas request: {dmls:?}");
    let op = Op::single_dml_or_batch(dmls);
    let predicate = cas::Predicate::new(applied, []);
    let request = cas::Request::new(op, predicate, ADMIN_ID)?;
    let deadline = fiber::clock().saturating_add(timeout);
    let res = cas::compare_and_swap_and_wait(&request, deadline)?;
    // If a retriable error happens we just go to the next loop iteration and
    // try again
    res.no_retries()?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// debugging
////////////////////////////////////////////////////////////////////////////////

fn log_bucket_changes(
    changes: &[VshardBucketRecord],
    topology_ref: &TopologyCacheRef,
) -> Result<()> {
    let mut iter = changes.iter().cloned();

    let Some(mut curr_bucket) = iter.next() else {
        return Ok(());
    };

    let mut end = curr_bucket.bucket_id;

    for bucket in iter {
        assert!(bucket.bucket_id > end);
        if bucket.bucket_id > end + 1
            || (bucket.state, &bucket.peer) != (curr_bucket.state, &curr_bucket.peer)
        {
            // Flush current range
            inspect_vshard_bucket_range("replace", &curr_bucket, end, topology_ref)?;
            curr_bucket.clone_from(&bucket);
        }

        end = bucket.bucket_id;
    }

    inspect_vshard_bucket_range("replace", &curr_bucket, end, topology_ref)?;

    Ok(())
}

fn inspect_space_bucket(topology_ref: &TopologyCacheRef) -> Result<()> {
    let mut iter = SPACE_BUCKET.select(IteratorType::All, &())?;

    let Some(tuple) = iter.next() else {
        tlog!(Debug, "space '_bucket' is empty!");
        return Ok(());
    };

    tlog!(Debug, "space '_bucket' contents:");

    let mut curr_bucket: VshardBucketRecord = tuple.decode()?;
    let mut end = curr_bucket.bucket_id;

    for tuple in iter {
        let bucket: VshardBucketRecord = tuple.decode()?;
        assert!(bucket.bucket_id > end);
        if bucket.bucket_id > end + 1
            || (bucket.state, &bucket.peer) != (curr_bucket.state, &curr_bucket.peer)
        {
            // Flush range
            inspect_vshard_bucket_range("", &curr_bucket, end, topology_ref)?;
            curr_bucket.clone_from(&bucket);
        }

        end = bucket.bucket_id;
    }

    inspect_vshard_bucket_range("", &curr_bucket, end, topology_ref)?;

    Ok(())
}

fn inspect_vshard_bucket_range(
    prefix: &str,
    bucket: &VshardBucketRecord,
    end: u64,
    topology_ref: &TopologyCacheRef,
) -> Result<()> {
    let start = bucket.bucket_id;
    let range = if start == end {
        format_smolstr!("{start}")
    } else {
        format_smolstr!("{start}..{end}")
    };

    let peer = if let Some(peer_uuid) = &bucket.peer {
        let peer_name = &topology_ref.replicaset_by_uuid(peer_uuid)?.name;
        format_smolstr!(", {peer_name}, ({peer_uuid})")
    } else {
        SmolStr::default()
    };

    let state = bucket.state;
    tlog!(Debug, "_bucket:{prefix} [{range}, {state:?}{peer}]");

    Ok(())
}

fn assert_all_buckets_rw() -> Result<()> {
    let index_status = SPACE_BUCKET
        .index("status")
        .expect("database constraint violation");

    const NON_RW_STATUSES: &[VshardBucketState] = &[
        VshardBucketState::Pinned,
        VshardBucketState::Sending,
        VshardBucketState::Sent,
        VshardBucketState::Receiving,
        VshardBucketState::Garbage,
    ];
    for status in NON_RW_STATUSES {
        if let Some(tuple) = index_status.min(&[status])? {
            panic!("non rw tuple {tuple:?}");
        }
    }

    Ok(())
}
