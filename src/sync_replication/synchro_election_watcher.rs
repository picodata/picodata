//! Tarantool election integration.
//!
//! Every tier runs elections, except for an async tier of a cluster in the
//! middle of a rolling upgrade from 26.2, see
//! [`super::ELECTIONS_ON_EVERY_TIER_SINCE`]. An instance which doesn't run
//! elections yet ignores the election events.
//!
//! Picodata keeps the designated master in `_pico_replicaset`, while
//! Tarantool's manual elections are the authority that makes that instance
//! writable. The `box.watch("box.election")` callback forwards election
//! information to a single worker fiber. The worker calls leadership
//! side effects (notify plugins, truncate of unlogged tables) and
//! re-promotes the designated local master when a recovered majority
//! has no election leader. It also lifts the fence of `box.cfg.read_only`
//! from the designated master if a switchover fenced it, but was abandoned,
//! see [`is_fenced_leader`].
//!
//! # Who promotes the master
//!
//! The governor decides which instance is the master, and transfers the
//! mastership by promoting the new one, see [`proc_replication_promote`].
//! But the master also loses the election leadership with no decision of the
//! governor: after a replication flap (quorum lost, then fencing, then quorum
//! recovery) the replicaset has no leader, and in `manual` election mode
//! nobody is elected until someone promotes an instance. The governor cannot
//! react to this quickly enough (`governor_auto_offline_timeout` = 30s by
//! default while fencing timeout is 4s by default). It may even be unable to:
//! the raft apply of a read-only master may be stuck at an entry which only a
//! writable master can apply, and if the governor runs on that very instance,
//! it waits for the apply, see `handle_self_read_only` in the governor. So the
//! worker promotes the designated master itself.
//!
//! Every promotion goes through [`promote`], which serializes them. The worker
//! promotes this instance only while it's the designated master and no
//! switchover is in progress, so that it doesn't compete with the governor. It
//! learns that from the local topology after a raft read index, which keeps an
//! old master with a stale raft index from promoting itself, see
//! [`promote_self_if_still_needed`]. A master whose raft apply is stuck can't
//! catch up, so it takes the governor's word for it instead, see
//! [`note_master_designation`].
//!
//! [`proc_replication_promote`]: crate::rpc::replication::proc_replication_promote

use super::{elections_enabled, enough_live_members_to_win_election, promote, unfence};
use crate::mailbox::Mailbox;
use crate::replicaset::has_synchro_quorum;
use crate::rpc::replication::handle_election_leader_change;
use crate::tarantool::{box_info_election, box_is_ro, box_ro_reason, ELECTION_TIMEOUT};
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

/// The reconciliation period while the governor's word that this instance is
/// the master stands, see [`note_master_designation`]. Right after a
/// replication reconfiguration the peers are reconnecting, and no election
/// broadcast tells when they're back.
const DESIGNATED_RECONCILE_PERIOD: Duration = Duration::from_millis(100);

/// How long the governor's word that this instance is the master stands, see
/// [`note_master_designation`].
const GOVERNOR_DESIGNATION_TTL: Duration = ELECTION_TIMEOUT;

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

/// A message to the election worker.
enum Message {
    /// A `box.election` broadcast.
    Election(ElectionEvent),
    /// The governor has named this instance the master, see
    /// [`note_master_designation`].
    MasterDesignated,
}

thread_local! {
    static WATCHER_STARTED: Cell<bool> = Cell::default();

    /// The mailbox of the election worker. It exists before the worker
    /// starts, so that the messages sent earlier aren't lost.
    static MAILBOX: Mailbox<Message> = Mailbox::new();

    /// Until when the governor's word that this instance is the master
    /// stands, see [`note_master_designation`].
    static GOVERNOR_DESIGNATION_EXPIRES_AT: Cell<Option<Instant>> = Cell::default();
}

/// Register the non-yielding election observer and start its worker.
pub(crate) fn start_synchro_election_watcher() -> traft::Result<()> {
    if WATCHER_STARTED.get() {
        return Ok(());
    }

    let tx = MAILBOX.with(Mailbox::clone);
    let rx = tx.clone();

    let lua = ::tarantool::lua_state();
    lua.exec_with(
        "box.watch('box.election', ...)",
        tlua::Function::new(move |_: tlua::Ignore, payload: BoxElectionEvent| {
            if let Some(event) = payload.into_event() {
                tx.send(Message::Election(event));
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

/// Tells the election worker whether the governor has just named this
/// instance the replicaset master, see
/// [`ConfigureReplicationRequest::is_master`].
///
/// If it has, the worker promotes this instance as soon as it can, if the
/// replicaset has no leader. Until [`GOVERNOR_DESIGNATION_TTL`] passes, the
/// governor's word stands in for the local topology, unless the worker could
/// linearize the latter, see [`self_promotion_conditions_hold`].
///
/// The local topology may lag far behind: e.g. a new replicaset master may
/// lose the leadership as soon as the second member is activated, because the
/// replication reconfiguration raises the quorum while the peers are
/// reconnecting. If it's still catching up the raft log at that moment, it
/// may stop at an entry which only a writable master can apply, before it
/// even knows its replicaset. The governor's view is up to date.
///
/// [`ConfigureReplicationRequest::is_master`]: crate::rpc::replication::ConfigureReplicationRequest::is_master
pub(crate) fn note_master_designation(is_master: bool) {
    if !is_master {
        GOVERNOR_DESIGNATION_EXPIRES_AT.set(None);
        return;
    }

    let expires_at = fiber::clock().saturating_add(GOVERNOR_DESIGNATION_TTL);
    GOVERNOR_DESIGNATION_EXPIRES_AT.set(Some(expires_at));
    MAILBOX.with(|mailbox| mailbox.send(Message::MasterDesignated));
}

/// Returns `true` if the governor's word that this instance is the master
/// stands, see [`note_master_designation`].
fn designated_by_governor() -> bool {
    GOVERNOR_DESIGNATION_EXPIRES_AT
        .get()
        .is_some_and(|expires_at| fiber::clock() < expires_at)
}

fn election_worker(mailbox: Mailbox<Message>) {
    let mut last_processed = None;
    let mut messages = Vec::new();
    let mut retry_promote_at = None;

    loop {
        if fiber::is_cancelled() {
            return;
        }

        if !elections_enabled() {
            // The governor will switch this instance to elections once the
            // whole cluster is upgraded. Until then the leadership is managed
            // by the governor and `box.cfg.read_only`.
            mailbox.receive_all(RECONCILE_PERIOD);
            continue;
        }

        let mut latest = None;
        for message in messages.drain(..) {
            match message {
                Message::Election(event) => {
                    process_election_event(&event, &mut last_processed);
                    latest = Some(event);
                }
                Message::MasterDesignated => {
                    // A failed self-promotion was decided on older data.
                    retry_promote_at = None;
                }
            }
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

        let period = if designated_by_governor() {
            DESIGNATED_RECONCILE_PERIOD
        } else {
            RECONCILE_PERIOD
        };
        messages = mailbox.receive_all(period);
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
    if event.is_writable_leader() {
        return;
    }
    if event.leader != 0 && !is_fenced_leader() {
        return;
    }
    if retry_at.is_some_and(|retry_at| fiber::clock() < retry_at) {
        return;
    }
    if !self_promotion_conditions_hold(false) {
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
    let linearized = match node.read_index(SELF_PROMOTE_QUORUM_READ_TIMEOUT) {
        Ok(_) => true,
        Err(_) => {
            // We couldn't catch up. Either the raft leader is unreachable, or
            // our own apply is stalled - which is what a read-only master with
            // a pending schema change does (see `NodeImpl::apply_entry`). A
            // stalled apply is a reason to promote, not to wait: nothing else
            // will unstick it. A moving applied index means we're merely
            // catching up, so let the next reconciliation decide with fresh
            // data. Unless the governor has just named this instance the
            // master: its view is fresh already, while catching up may take
            // longer than its word stands.
            if node.get_index() != applied_before && !designated_by_governor() {
                return;
            }
            tlog!(
                Warning,
                "election: promoting on an unlinearized view, raft read index failed"
            );
            false
        }
    };

    if !self_promotion_conditions_hold(linearized) {
        return;
    }

    if is_fenced_leader() {
        // The switchover which fenced this instance was abandoned before
        // anyone else was promoted, e.g. the new master lost the election.
        tlog!(
            Info,
            "election: designated master is fenced by box.cfg.read_only, making self writable"
        );
        if let Err(e) = unfence() {
            tlog!(Warning, "election: failed to make self writable: {e}");
        }
        return;
    }

    let should_promote = read_election_event()
        .map(|event| event.leader == 0 && !event.is_writable_leader())
        .unwrap_or(false);

    if !should_promote {
        return;
    }

    tlog!(
        Info,
        "election: designated master has no election leader, promoting self"
    );
    match promote() {
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

/// Returns `true` if this instance is the election leader, but
/// `box.cfg.read_only` keeps it from accepting writes: it's an old master
/// fenced by [`proc_replication_demote`].
///
/// Under elections a follower is read-only for the "election" reason, and a
/// leader which doesn't own the synchro queue yet for the "synchro" one.
///
/// [`proc_replication_demote`]: crate::rpc::replication::proc_replication_demote
fn is_fenced_leader() -> bool {
    box_ro_reason() == Some("config")
}

/// Returns `true` if this instance is the designated master, no switchover is
/// in progress, and enough members are alive to win an election.
///
/// This instance is the designated master if the local topology says so. If
/// the topology isn't `linearized`, the governor's word counts too, see
/// [`note_master_designation`]. A linearized topology is at least as fresh
/// as the view of the governor, so then it decides alone.
fn self_promotion_conditions_hold(linearized: bool) -> bool {
    let Ok(node) = node::global() else {
        return false;
    };
    if !elections_enabled() {
        return false;
    }

    let my_name = node.catalog_cache.my_instance_name();
    let topology = node.catalog_cache.get();
    let replicaset = topology
        .replicaset_by_name(node.catalog_cache.my_replicaset_name())
        .ok();

    // Equality of both fields also means no switchover is in progress.
    let designated_in_topology = replicaset.is_some_and(|replicaset| {
        replicaset.current_master_name == my_name && replicaset.target_master_name == my_name
    });
    if !designated_in_topology && (linearized || !designated_by_governor()) {
        return false;
    }

    // The topology from the raft log is checked first: it's up to date most of
    // the time, while right after a replication reconfiguration, which may
    // depose the leader, the Tarantool view has no live peers for a moment.
    //
    // But after a restart the local topology says that every member, this
    // instance included, is offline until the instance applies the raft
    // entries which say otherwise. A master which can't apply them before
    // it's writable again, e.g. a pending schema change, would never promote
    // itself, so then the Tarantool view decides.
    let db_config = node.alter_system_parameters.borrow();
    replicaset.is_some_and(|replicaset| has_synchro_quorum(replicaset, &topology, &db_config))
        || enough_live_members_to_win_election()
}
