//! Tarantool election integration for synchronous-replication tiers.
//!
//! Picodata keeps the designated master in `_pico_replicaset`, while
//! Tarantool's manual elections are the authority that makes that instance
//! writable. The `box.watch("box.election")` callback forwards election
//! information to a single worker fiber. The worker calls leadership
//! side effects (notify plugins, truncate of unlogged tables) and
//! re-promotes the designated local master when a recovered majority
//! has no election leader.
//!
//! We need the election worker because we must be able to promote the master
//! after a replication flap (quorum lost, then fencing, then quorum recovery).
//! The governor cannot react to these changes quickly enough
//! (`governor_auto_offline_timeout` = 30s by default while fencing timeout is
//! 4s by default). So we need a separate fiber to do this: an instance
//! self-promotes.
//!
//! Self-promotion is guarded by a Raft index read and the
//! [`self_promotion_conditions_hold`] predicate. This prevents an old master
//! with a stale Raft index from self-promoting.
//!
//! There are two `box_promote` calls in the code: one from the governor,
//! one from the election worker (this file). We have a self-promotion
//! guard here, as described above. In the future we would like to have
//! a single place that calls `box_promote`: the worker should somehow notify
//! the governor about the need for promotion.
//! See <https://git.picodata.io/core/picodata/-/work_items/3100>.

use crate::mailbox::Mailbox;
use crate::replicaset::has_synchro_quorum;
use crate::rpc::replication::handle_election_leader_change;
use crate::tarantool::{box_info_election, box_is_ro, box_promote};
use crate::tlog;
use crate::traft;
use crate::traft::node;
use std::cell::Cell;
use std::time::Duration;
use tarantool::fiber;
use tarantool::time::Instant;
use tarantool::tlua;

/// Periodic reconciliation covers leadership loss/recovery which happens
/// without a subsequent election broadcast.
const RECONCILE_PERIOD: Duration = Duration::from_secs(1);

/// Bounds how long we wait for a linearized view of `_pico_replicaset`
/// before falling back on the local one.
const SELF_PROMOTE_QUORUM_READ_TIMEOUT: Duration = Duration::from_secs(1);

/// Avoid term churn while the runtime view of quorum lags reality.
const SELF_PROMOTE_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, PartialEq, Eq)]
struct ElectionEvent {
    term: u64,
    role: String,
    is_ro: bool,
    leader: u32,
}

impl ElectionEvent {
    #[inline]
    fn is_writable_leader(&self) -> bool {
        self.role == "leader" && !self.is_ro
    }
}

/// The payload of a `box.election` broadcast.
#[derive(tlua::LuaRead)]
struct BoxElectionEvent {
    term: Option<u64>,
    role: Option<String>,
    is_ro: Option<bool>,
    leader: Option<u32>,
}

impl BoxElectionEvent {
    /// Returns `None` for the placeholder payload of an unconfigured raft.
    /// A real broadcast always carries all four fields.
    fn into_event(self) -> Option<ElectionEvent> {
        let (Some(term), Some(role), Some(is_ro), Some(leader)) =
            (self.term, self.role, self.is_ro, self.leader)
        else {
            return None;
        };

        Some(ElectionEvent {
            term,
            role,
            is_ro,
            leader,
        })
    }
}

thread_local! {
    static WATCHER_STARTED: Cell<bool> = Cell::default();
}

/// Register the non-yielding election observer and start its worker.
pub(crate) fn start_synchro_election_watcher() -> traft::Result<()> {
    if WATCHER_STARTED.get() {
        return Ok(());
    }

    let tx = Mailbox::new();
    let rx = tx.clone();

    let lua = ::tarantool::lua_state();
    lua.exec_with(
        "box.watch('box.election', ...)",
        tlua::Function::new(move |_: tlua::Ignore, payload: BoxElectionEvent| {
            if let Some(event) = payload.into_event() {
                tx.send(event);
            }
        }),
    )?;

    fiber::Builder::new()
        .name("synchro_election_watcher")
        .func(move || election_worker(rx))
        .defer_non_joinable()
        .map_err(|e| traft::error::Error::other(format!("failed to spawn election worker: {e}")))?;

    WATCHER_STARTED.set(true);
    tlog!(Info, "election: registered box.election watcher");
    Ok(())
}

fn election_worker(mailbox: Mailbox<ElectionEvent>) {
    let mut last_processed = None;
    let mut events = Vec::new();
    let mut retry_promote_at = None;

    loop {
        if fiber::is_cancelled() {
            return;
        }

        let mut latest = None;
        for event in events.drain(..) {
            process_election_event(&event, &mut last_processed);
            latest = Some(event);
        }

        // A periodic read for state changes which did not produce a new broadcast.
        if latest.is_none() {
            match read_election_event() {
                Ok(event) => {
                    process_election_event(&event, &mut last_processed);
                    latest = Some(event);
                }
                Err(e) => {
                    tlog!(Warning, "election: failed to read election state: {e}");
                }
            }
        }

        if let Some(event) = latest {
            maybe_promote_self(&event, &mut retry_promote_at);
        }

        events = mailbox.receive_all(RECONCILE_PERIOD);
    }
}

fn process_election_event(event: &ElectionEvent, last_processed: &mut Option<ElectionEvent>) {
    if last_processed.as_ref() == Some(event) {
        return;
    }

    if let Err(e) = handle_election_leader_change(event.is_writable_leader()) {
        // Do not mark the event processed. A later periodic pass retries the
        // unlogged-table truncation/plugin notification.
        tlog!(
            Warning,
            "election: failed handling leadership transition: {e}"
        );
        return;
    }

    *last_processed = Some(event.clone());
}

fn read_election_event() -> traft::Result<ElectionEvent> {
    let election = box_info_election()?;
    Ok(ElectionEvent {
        term: election.term,
        role: election.state,
        is_ro: box_is_ro(),
        leader: election.leader,
    })
}

fn maybe_promote_self(event: &ElectionEvent, retry_at: &mut Option<Instant>) {
    if event.leader != 0 || event.is_writable_leader() {
        return;
    }
    if retry_at.is_some_and(|retry_at| fiber::clock() < retry_at) {
        return;
    }
    if !self_promotion_conditions_hold() {
        return;
    }

    promote_self_if_still_needed(retry_at);
}

fn promote_self_if_still_needed(retry_at: &mut Option<Instant>) {
    let Ok(node) = node::global() else {
        return;
    };

    // The governor may be retargeting mastership right now. The local topology
    // cache is fed by the raft log and may be arbitrarily behind it, so
    // linearize first: after a quorum read our view contains every decision
    // which was committed before we asked.
    let applied_before = node.get_index();
    if node.read_index(SELF_PROMOTE_QUORUM_READ_TIMEOUT).is_err() {
        // We couldn't catch up. Either the raft leader is unreachable, or our
        // own apply is stalled - which is what a read-only master with a
        // pending schema change does (see `NodeImpl::apply_entry`). A stalled
        // apply is a reason to promote, not to wait: nothing else will unstick
        // it. A moving applied index means we're merely catching up, so let
        // the next reconciliation decide with fresh data instead.
        if node.get_index() != applied_before {
            return;
        }
        tlog!(
            Warning,
            "election: promoting on an unlinearized view, raft apply is stalled"
        );
    }

    let should_promote = self_promotion_conditions_hold()
        && read_election_event()
            .map(|event| event.leader == 0 && !event.is_writable_leader())
            .unwrap_or(false);

    if !should_promote {
        return;
    }

    tlog!(
        Info,
        "election: designated master has no election leader, promoting self"
    );
    match box_promote() {
        Ok(()) => {
            *retry_at = None;
            tlog!(Info, "election: self-promotion succeeded");
        }
        Err(e) => {
            *retry_at = Some(fiber::clock().saturating_add(SELF_PROMOTE_BACKOFF));
            tlog!(Warning, "election: self-promotion failed: {e}");
        }
    }
}

fn self_promotion_conditions_hold() -> bool {
    let Ok(node) = node::global() else {
        return false;
    };
    if !node
        .alter_system_parameters
        .borrow()
        .is_synchronous_replication()
    {
        return false;
    }

    let my_name = node.topology_cache.my_instance_name();
    let topology = node.topology_cache.get();
    let Ok(replicaset) = topology.replicaset_by_name(node.topology_cache.my_replicaset_name())
    else {
        return false;
    };

    // Equality of both fields also means no switchover is in progress.
    replicaset.current_master_name == my_name
        && replicaset.target_master_name == my_name
        && has_synchro_quorum(replicaset, &topology)
}
