//! This module incapsulates most of the application-specific logics.
//!
//! It's responsible for
//! - handling proposals,
//! - handling configuration changes,
//! - processing raft `Ready` - persisting entries, communicating with other raft nodes.

use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::raft::StateRole as RaftStateRole;
use ::raft::INVALID_ID;
use ::tarantool::error::TransactionError;
use ::tarantool::fiber;
use ::tarantool::fiber::Mutex;
use ::tarantool::proc;
use ::tarantool::tlua;
use ::tarantool::transaction::start_transaction;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::rc::Rc;
use std::time::Duration;
use std::time::Instant;

use crate::stringify_cfunc;
use crate::traft::ContextCoercion as _;
use crate::traft::Peer;
use crate::traft::RaftId;
use ::tarantool::util::IntoClones as _;
use protobuf::Message as _;
use std::iter::FromIterator as _;

use crate::cache::CachedCell;
use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::event;
use crate::traft::event::Event;
use crate::traft::failover;
use crate::traft::notify::Notify;
use crate::traft::ConnectionPool;
use crate::traft::LogicalClock;
use crate::traft::Storage;
use crate::traft::Topology;
use crate::traft::TopologyRequest;
use crate::traft::{JoinRequest, JoinResponse, UpdatePeerRequest};

use super::OpResult;

type RawNode = raft::RawNode<Storage>;

#[derive(Clone, Debug, tlua::Push, tlua::PushInto)]
pub struct Status {
    /// `raft_id` of the current instance
    pub id: u64,
    /// `raft_id` of the leader instance
    pub leader_id: Option<u64>,
    /// One of "Follower", "Candidate", "Leader", "PreCandidate"
    pub raft_state: String,
    /// Whether instance has finished its `postjoin`
    /// initialization stage
    pub is_ready: bool,
}

/// The heart of `traft` module - the Node.
pub struct Node {
    raw_node: Rc<Mutex<RawNode>>,
    raft_id: RaftId,
    _main_loop: fiber::UnitJoinHandle<'static>,
    _conf_change_loop: fiber::UnitJoinHandle<'static>,
    status: Rc<RefCell<Status>>,
    notifications: Rc<RefCell<HashMap<LogicalClock, Notify>>>,
    topology_cache: CachedCell<u64, Topology>,
    lc: Cell<Option<LogicalClock>>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("raft_id", &self.raft_id)
            .finish_non_exhaustive()
    }
}

impl Node {
    pub const TICK: Duration = Duration::from_millis(100);

    pub fn new(cfg: &raft::Config) -> Result<Self, RaftError> {
        let raw_node = RawNode::new(cfg, Storage, &tlog::root())?;
        let raw_node = Rc::new(Mutex::new(raw_node));
        let status = Rc::new(RefCell::new(Status {
            id: cfg.id,
            leader_id: None,
            raft_state: "Follower".into(),
            is_ready: false,
        }));

        let notifications = Rc::new(RefCell::new(HashMap::new()));

        let main_loop_fn = {
            let status = status.clone();
            let raw_node = raw_node.clone();
            let notifications = notifications.clone();
            move || raft_main_loop(status, raw_node, notifications)
        };

        let conf_change_loop_fn = {
            let status = status.clone();
            move || raft_conf_change_loop(status)
        };

        let node = Node {
            raw_node,
            raft_id: cfg.id,
            notifications,
            status,
            _main_loop: fiber::Builder::new()
                .name("raft_main_loop")
                .proc(main_loop_fn)
                .start()
                .unwrap(),
            _conf_change_loop: fiber::Builder::new()
                .name("raft_conf_change_loop")
                .proc(conf_change_loop_fn)
                .start()
                .unwrap(),
            topology_cache: CachedCell::new(),
            lc: {
                let id = Storage::raft_id().unwrap().unwrap();
                let gen = Storage::gen().unwrap().unwrap_or(0) + 1;
                Storage::persist_gen(gen).unwrap();
                Cell::new(Some(LogicalClock::new(id, gen)))
            },
        };

        // Wait for the node to enter the main loop
        node.tick(0);
        Ok(node)
    }

    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }

    pub fn mark_as_ready(&self) {
        self.status.borrow_mut().is_ready = true;
        event::broadcast(Event::StatusChanged);
    }

    pub fn wait_status(&self) {
        event::wait(Event::StatusChanged).expect("Events system wasn't initialized");
    }

    pub fn read_index(&self, timeout: Duration) -> Result<u64, Error> {
        let mut raw_node = self.raw_node.lock();

        // In some states `raft-rs` ignores the ReadIndex request.
        // Check it preliminary, don't wait for the timeout.
        //
        // See for details:
        // - <https://github.com/tikv/raft-rs/blob/v0.6.0/src/raft.rs#L2058>
        // - <https://github.com/tikv/raft-rs/blob/v0.6.0/src/raft.rs#L2323>
        let leader_doesnt_exist = raw_node.raft.leader_id == INVALID_ID;
        let term_just_started =
            raw_node.raft.state == RaftStateRole::Leader && !raw_node.raft.commit_to_current_term();
        if leader_doesnt_exist || term_just_started {
            return Err(RaftError::ProposalDropped.into());
        }

        let (rx, tx) = Notify::new().into_clones();
        let lc = self.next_lc();
        self.notifications.borrow_mut().insert(lc, tx);

        // read_index puts this context into an Entry,
        // so we've got to compose full EntryContext,
        // despite single LogicalClock would be enough
        let ctx = traft::EntryContextNormal {
            lc,
            op: traft::Op::Nop,
        }
        .to_bytes();
        raw_node.read_index(ctx);
        drop(raw_node);
        event::broadcast(Event::RaftLoopNeeded);
        rx.recv_timeout::<u64>(timeout)
    }

    pub fn propose<T: OpResult + Into<traft::Op>>(
        &self,
        op: T,
        timeout: Duration,
    ) -> Result<T::Result, Error> {
        let mut raw_node = self.raw_node.lock();
        let (rx, tx) = Notify::new().into_clones();
        let lc = self.next_lc();
        self.notifications.borrow_mut().insert(lc, tx);
        let ctx = traft::EntryContextNormal { lc, op: op.into() }.to_bytes();
        raw_node.propose(ctx, vec![])?;
        drop(raw_node);
        event::broadcast(Event::RaftLoopNeeded);
        rx.recv_timeout::<T::Result>(timeout)
    }

    pub fn campaign(&self) -> Result<(), Error> {
        let mut raw_node = self.raw_node.lock();
        raw_node.campaign()?;
        drop(raw_node);
        event::broadcast(Event::RaftLoopNeeded);
        // even though we don't expect a response, we still should let the
        // main_loop do an iteration
        fiber::reschedule();
        Ok(())
    }

    pub fn step(&self, msg: raft::Message) {
        let mut raw_node = self.raw_node.lock();
        if msg.to != raw_node.raft.id {
            return;
        }
        if let Err(e) = raw_node.step(msg) {
            tlog!(Error, "{e}");
        }
        drop(raw_node);
        event::broadcast(Event::RaftLoopNeeded);
        // even though we don't expect a response, we still should let the
        // main_loop do an iteration
        fiber::reschedule();
    }

    pub fn tick(&self, n_times: u32) {
        let mut raw_node = self.raw_node.lock();
        for _ in 0..n_times {
            raw_node.tick();
        }
        drop(raw_node);
        event::broadcast(Event::RaftLoopNeeded);
        // even though we don't expect a response, we still should let the
        // main_loop do an iteration
        fiber::reschedule();
    }

    pub fn timeout_now(&self) {
        self.step(raft::Message {
            to: self.raft_id,
            from: self.raft_id,
            msg_type: raft::MessageType::MsgTimeoutNow,
            ..Default::default()
        })
    }

    pub fn handle_topology_request(&self, req: TopologyRequest) -> Result<traft::Peer, Error> {
        let mut raw_node = self.raw_node.lock();

        let status = raw_node.status();
        if status.ss.raft_state != RaftStateRole::Leader {
            return Err(RaftError::ConfChangeError("not a leader".into()).into());
        }

        let mut topology = self
            .topology_cache
            .pop(&raw_node.raft.term)
            .unwrap_or_else(|| {
                let peers = Storage::peers().unwrap();
                let replication_factor = Storage::replication_factor().unwrap().unwrap();
                Topology::from_peers(peers).with_replication_factor(replication_factor)
            });

        let peer_result = match req {
            TopologyRequest::Join(JoinRequest {
                instance_id,
                replicaset_id,
                advertise_address,
                failure_domain,
                ..
            }) => topology.join(
                instance_id,
                replicaset_id,
                advertise_address,
                failure_domain,
            ),

            TopologyRequest::UpdatePeer(UpdatePeerRequest {
                instance_id,
                health,
                failure_domain,
                ..
            }) => topology.update_peer(&instance_id, health, failure_domain),
        };

        let mut peer = crate::unwrap_ok_or!(peer_result, Err(e) => {
            self.topology_cache.put(raw_node.raft.term, topology);
            return Err(RaftError::ConfChangeError(e).into());
        });

        peer.commit_index = raw_node.raft.raft_log.last_index() + 1;

        let lc = self.next_lc();
        let ctx = traft::EntryContextNormal {
            op: traft::Op::PersistPeer { peer },
            lc,
        };

        let (rx, tx) = Notify::new().into_clones();
        self.notifications.borrow_mut().insert(lc, tx);
        raw_node.propose(ctx.to_bytes(), vec![])?;
        self.topology_cache.put(raw_node.raft.term, topology);
        drop(raw_node);
        event::broadcast(Event::RaftLoopNeeded);
        rx.recv::<Peer>()
    }

    fn propose_conf_change(&self, term: u64, conf_change: raft::ConfChangeV2) -> Result<(), Error> {
        let mut raw_node = self.raw_node.lock();
        // In some states proposing a ConfChange is impossible.
        // Check if there's a reason to reject it.

        #[allow(clippy::never_loop)]
        let reason: Option<&str> = loop {
            // Checking leadership is only needed for the
            // correct latch management. It doesn't affect
            // raft correctness. Checking the instance is a
            // leader makes sure the proposed `ConfChange`
            // is appended to the raft log immediately
            // instead of sending `MsgPropose` over the
            // network.
            if raw_node.raft.state != RaftStateRole::Leader {
                break Some("not a leader");
            }

            if term != raw_node.raft.term {
                break Some("raft term mismatch");
            }

            // Without this check the node would silently ignore the conf change.
            // See https://github.com/tikv/raft-rs/blob/v0.6.0/src/raft.rs#L2014-L2026
            if raw_node.raft.has_pending_conf() {
                break Some("already has pending confchange");
            }

            break None;
        };

        if let Some(e) = reason {
            return Err(RaftError::ConfChangeError(e.into()).into());
        }

        let prev_index = raw_node.raft.raft_log.last_index();
        raw_node.propose_conf_change(vec![], conf_change)?;

        // oops, current instance isn't actually a leader
        // (which is impossible in theory, but we're not
        // sure in practice) and sent the ConfChange message
        // to the raft network instead of appending it to the
        // raft log.
        let last_index = raw_node.raft.raft_log.last_index();
        assert_eq!(last_index, prev_index + 1);

        let (rx, tx) = Notify::new().into_clones();
        with_joint_state_latch(|joint_state_latch| {
            assert!(joint_state_latch.take().is_none());
            joint_state_latch.set(Some(JointStateLatch {
                index: last_index,
                notify: tx,
            }));
        });

        drop(raw_node);
        event::broadcast(Event::RaftLoopNeeded);
        rx.recv()
    }

    fn next_lc(&self) -> LogicalClock {
        let mut lc = self.lc.take().expect("it's always Some");
        lc.inc();
        self.lc.set(Some(lc));
        lc
    }
}

#[derive(Debug)]
struct JointStateLatch {
    /// Index of the latest ConfChange entry proposed.
    /// Helps detecting when the entry is overridden
    /// due to a re-election.
    index: u64,

    /// Make a notification when the latch is unlocked.
    /// Notification is a `Result<Box<()>>`.
    notify: Notify,
}

fn with_joint_state_latch<F, R>(f: F) -> R
where
    F: FnOnce(&Cell<Option<JointStateLatch>>) -> R,
{
    thread_local! {
        static JOINT_STATE_LATCH: Cell<Option<JointStateLatch>> = Cell::new(None);
    }
    JOINT_STATE_LATCH.with(f)
}

fn handle_committed_entries(
    entries: Vec<raft::Entry>,
    notifications: &mut HashMap<LogicalClock, Notify>,
    raw_node: &mut RawNode,
    pool: &mut ConnectionPool,
    topology_changed: &mut bool,
) {
    for entry in &entries {
        let entry = match traft::Entry::try_from(entry) {
            Ok(v) => v,
            Err(e) => {
                tlog!(
                    Error,
                    "error parsing (and applying) an entry: {e}, entry = {entry:?}"
                );
                continue;
            }
        };

        match entry.entry_type {
            raft::EntryType::EntryNormal => {
                handle_committed_normal_entry(entry, notifications, pool, topology_changed)
            }
            raft::EntryType::EntryConfChange | raft::EntryType::EntryConfChangeV2 => {
                handle_committed_conf_change(entry, raw_node)
            }
        }
    }

    if let Some(last_entry) = entries.last() {
        if let Err(e) = Storage::persist_applied(last_entry.index) {
            tlog!(
                Error,
                "error persisting applied index: {e}";
                "index" => last_entry.index
            );
        };
    }
}

fn handle_committed_normal_entry(
    entry: traft::Entry,
    notifications: &mut HashMap<LogicalClock, Notify>,
    pool: &mut ConnectionPool,
    topology_changed: &mut bool,
) {
    assert_eq!(entry.entry_type, raft::EntryType::EntryNormal);
    let result = entry.op().unwrap_or(&traft::Op::Nop).on_commit();

    if let Some(lc) = entry.lc() {
        if let Some(notify) = notifications.remove(lc) {
            notify.notify_ok_any(result);
        }
    }

    if let Some(traft::Op::PersistPeer { peer }) = entry.op() {
        pool.connect(peer.raft_id, peer.peer_address.clone());
        *topology_changed = true;
    }

    with_joint_state_latch(|joint_state_latch| {
        if let Some(latch) = joint_state_latch.take() {
            if entry.index != latch.index {
                joint_state_latch.set(Some(latch));
                return;
            }

            // It was expected to be a ConfChange entry, but it's
            // normal. Raft must have overriden it, or there was
            // a re-election.
            let e = RaftError::ConfChangeError("rolled back".into());

            latch.notify.notify_err(e);
        }
    });
}

fn handle_committed_conf_change(entry: traft::Entry, raw_node: &mut RawNode) {
    let latch_unlock = || {
        with_joint_state_latch(|joint_state_latch| {
            if let Some(latch) = joint_state_latch.take() {
                latch.notify.notify_ok(());
                event::broadcast(Event::LeaveJointState);
            }
        });
    };

    // Beware: a tiny difference in type names (`V2` or not `V2`)
    // makes a significant difference in `entry.data` binary layout and
    // in joint state transitions.

    // `ConfChangeTransition::Auto` implies that `ConfChangeV2` may be
    // applied in an instant without entering the joint state.

    let conf_state = match entry.entry_type {
        raft::EntryType::EntryConfChange => {
            let mut cc = raft::ConfChange::default();
            cc.merge_from_bytes(&entry.data).unwrap();

            latch_unlock();

            raw_node.apply_conf_change(&cc).unwrap()
        }
        raft::EntryType::EntryConfChangeV2 => {
            let mut cc = raft::ConfChangeV2::default();
            cc.merge_from_bytes(&entry.data).unwrap();

            // Unlock the latch when either of conditions is met:
            // - conf_change will leave the joint state;
            // - or it will be applied without even entering one.
            if cc.leave_joint() || cc.enter_joint().is_none() {
                latch_unlock();
            }

            // ConfChangeTransition::Auto implies that at this
            // moment raft-rs will implicitly propose another empty
            // conf change that represents leaving the joint state.
            raw_node.apply_conf_change(&cc).unwrap()
        }
        _ => unreachable!(),
    };

    let raft_id = &raw_node.raft.id;
    let voters_old = Storage::voters().unwrap();
    if voters_old.contains(raft_id) && !conf_state.voters.contains(raft_id) {
        event::broadcast_when(Event::Demoted, Event::LeaveJointState).ok();
    }

    Storage::persist_conf_state(&conf_state).unwrap();
}

fn handle_read_states(
    read_states: Vec<raft::ReadState>,
    notifications: &mut HashMap<LogicalClock, Notify>,
) {
    for rs in read_states {
        let ctx = match traft::EntryContextNormal::read_from_bytes(&rs.request_ctx) {
            Ok(Some(v)) => v,
            Ok(None) => continue,
            Err(_) => {
                tlog!(Error, "abnormal entry, read_state = {rs:?}");
                continue;
            }
        };

        if let Some(notify) = notifications.remove(&ctx.lc) {
            notify.notify_ok(rs.index);
        }
    }
}

fn handle_messages(messages: Vec<raft::Message>, pool: &ConnectionPool) {
    for msg in messages {
        if let Err(e) = pool.send(&msg) {
            tlog!(Error, "{e}");
        }
    }
}

fn raft_main_loop(
    status: Rc<RefCell<Status>>,
    raw_node: Rc<Mutex<RawNode>>,
    notifications: Rc<RefCell<HashMap<LogicalClock, Notify>>>,
) {
    let mut next_tick = Instant::now() + Node::TICK;
    let mut pool = ConnectionPool::builder()
        .handler_name(stringify_cfunc!(raft_interact))
        .call_timeout(Node::TICK * 4)
        .connect_timeout(Node::TICK * 4)
        .inactivity_timeout(Duration::from_secs(60))
        .build();

    for peer in Storage::peers().unwrap() {
        pool.connect(peer.raft_id, peer.peer_address);
    }

    loop {
        // Clean up obsolete notifications
        notifications
            .borrow_mut()
            .retain(|_, notify: &mut Notify| !notify.is_closed());

        event::wait_timeout(Event::RaftLoopNeeded, Node::TICK).expect("Events must be initialized");

        let mut raw_node = raw_node.lock();
        let now = Instant::now();
        if now > next_tick {
            next_tick = now + Node::TICK;
            raw_node.tick();
        }

        // Get the `Ready` with `RawNode::ready` interface.
        if !raw_node.has_ready() {
            continue;
        }

        let mut ready: raft::Ready = raw_node.ready();
        let mut topology_changed = false;

        start_transaction(|| -> Result<(), TransactionError> {
            if !ready.messages().is_empty() {
                // Send out the messages come from the node.
                let messages = ready.take_messages();
                handle_messages(messages, &pool);
            }

            if !ready.snapshot().is_empty() {
                // This is a snapshot, we need to apply the snapshot at first.
                unimplemented!();
                // Storage::apply_snapshot(ready.snapshot()).unwrap();
            }

            let committed_entries = ready.take_committed_entries();
            handle_committed_entries(
                committed_entries,
                &mut *notifications.borrow_mut(),
                &mut raw_node,
                &mut pool,
                &mut topology_changed,
            );

            if !ready.entries().is_empty() {
                let e = ready.entries();
                // Append entries to the Raft log.
                Storage::persist_entries(e).unwrap();
            }

            if let Some(hs) = ready.hs() {
                // Raft HardState changed, and we need to persist it.
                // let hs = hs.clone();
                Storage::persist_hard_state(hs).unwrap();
            }

            if let Some(ss) = ready.ss() {
                let mut status = status.borrow_mut();
                status.leader_id = (ss.leader_id != INVALID_ID).then(|| ss.leader_id);
                status.raft_state = format!("{:?}", ss.raft_state);
                event::broadcast(Event::StatusChanged);
            }

            if !ready.persisted_messages().is_empty() {
                // Send out the persisted messages come from the node.
                let messages = ready.take_persisted_messages();
                handle_messages(messages, &pool);
            }

            let read_states = ready.take_read_states();
            handle_read_states(read_states, &mut *notifications.borrow_mut());

            Ok(())
        })
        .unwrap();

        // Advance the Raft.
        let mut light_rd = raw_node.advance(ready);

        // Update commit index.
        start_transaction(|| -> Result<(), TransactionError> {
            if let Some(commit) = light_rd.commit_index() {
                Storage::persist_commit(commit).unwrap();
            }

            // Send out the messages.
            let messages = light_rd.take_messages();
            handle_messages(messages, &pool);

            // Apply all committed entries.
            let committed_entries = light_rd.take_committed_entries();
            handle_committed_entries(
                committed_entries,
                &mut *notifications.borrow_mut(),
                &mut raw_node,
                &mut pool,
                &mut topology_changed,
            );

            // Advance the apply index.
            raw_node.advance_apply();
            Ok(())
        })
        .unwrap();

        if topology_changed {
            event::broadcast(Event::TopologyChanged);
            if let Some(peer) = traft::Storage::peer_by_raft_id(raw_node.raft.id).unwrap() {
                let mut box_cfg = crate::tarantool::cfg().unwrap();
                assert_eq!(box_cfg.replication_connect_quorum, 0);
                box_cfg.replication =
                    traft::Storage::box_replication(&peer.replicaset_id, None).unwrap();
                crate::tarantool::set_cfg(&box_cfg);
            }
        }
    }
}

fn raft_conf_change_loop(status: Rc<RefCell<Status>>) {
    loop {
        if status.borrow().raft_state != "Leader" {
            event::wait(Event::StatusChanged).expect("Events system must be initialized");
            continue;
        }

        let term = Storage::term().unwrap().unwrap_or(0);
        let voter_ids: HashSet<RaftId> = HashSet::from_iter(Storage::voters().unwrap());
        let learner_ids: HashSet<RaftId> = HashSet::from_iter(Storage::learners().unwrap());
        let peer_is_active: HashMap<RaftId, bool> = Storage::peers()
            .unwrap()
            .into_iter()
            .map(|peer| (peer.raft_id, peer.is_active()))
            .collect();

        let (active_voters, to_demote): (Vec<RaftId>, Vec<RaftId>) = voter_ids
            .iter()
            .partition(|id| peer_is_active.get(id).copied().unwrap_or(false));

        let active_learners: Vec<RaftId> = learner_ids
            .iter()
            .copied()
            .filter(|id| peer_is_active.get(id).copied().unwrap_or(false))
            .collect();

        let new_peers: Vec<RaftId> = peer_is_active
            .iter()
            .map(|(&id, _)| id)
            .filter(|id| !voter_ids.contains(id) && !learner_ids.contains(id))
            .collect();

        let mut changes: Vec<raft::ConfChangeSingle> = Vec::new();

        const VOTER: bool = true;
        const LEARNER: bool = false;

        changes.extend(
            to_demote
                .into_iter()
                .map(|id| conf_change_single(id, LEARNER)),
        );

        let total_active = active_voters.len() + active_learners.len() + new_peers.len();

        let new_peers_to_promote;
        match failover::voters_needed(active_voters.len(), total_active) {
            0 => {
                new_peers_to_promote = 0;
            }
            pos @ 1..=i64::MAX => {
                let pos = pos as usize;
                if pos < active_learners.len() {
                    for &raft_id in &active_learners[0..pos] {
                        changes.push(conf_change_single(raft_id, VOTER))
                    }
                    new_peers_to_promote = 0;
                } else {
                    for &raft_id in &active_learners {
                        changes.push(conf_change_single(raft_id, VOTER))
                    }
                    new_peers_to_promote = pos - active_learners.len();
                    assert!(new_peers_to_promote <= new_peers.len());
                    for &raft_id in &new_peers[0..new_peers_to_promote] {
                        changes.push(conf_change_single(raft_id, VOTER))
                    }
                }
            }
            neg @ i64::MIN..=-1 => {
                let neg = -neg as usize;
                assert!(neg < active_voters.len());
                for &raft_id in &active_voters[0..neg] {
                    changes.push(conf_change_single(raft_id, LEARNER))
                }
                new_peers_to_promote = 0;
            }
        }

        for &raft_id in &new_peers[new_peers_to_promote..] {
            changes.push(conf_change_single(raft_id, LEARNER))
        }

        if changes.is_empty() {
            event::wait(Event::TopologyChanged).expect("Events system must be initialized");
            continue;
        }

        let conf_change = raft::ConfChangeV2 {
            transition: raft::ConfChangeTransition::Auto,
            changes: changes.into(),
            ..Default::default()
        };

        let node = global().expect("must be initialized");
        // main_loop gives the warranty that every ProposeConfChange
        // will sometimes be handled and there's no need in timeout.
        // It also guarantees that the notification will arrive only
        // after the node leaves the joint state.
        match node.propose_conf_change(term, conf_change) {
            Ok(()) => tlog!(Info, "conf_change processed"),
            Err(e) => {
                tlog!(Warning, "conf_change failed: {e}");
                fiber::sleep(Duration::from_secs(1));
            }
        }
    }
}

fn conf_change_single(node_id: RaftId, is_voter: bool) -> raft::ConfChangeSingle {
    let change_type = if is_voter {
        raft::ConfChangeType::AddNode
    } else {
        raft::ConfChangeType::AddLearnerNode
    };
    raft::ConfChangeSingle {
        change_type,
        node_id,
        ..Default::default()
    }
}

static mut RAFT_NODE: Option<Box<Node>> = None;

pub fn set_global(node: Node) {
    unsafe {
        assert!(
            RAFT_NODE.is_none(),
            "discovery::set_global() called twice, it's a leak"
        );
        RAFT_NODE = Some(Box::new(node));
    }
}

pub fn global() -> Result<&'static Node, Error> {
    // Uninitialized raft node is a regular case. This case may take
    // place while the instance is executing `start_discover()` function.
    // It has already started listening, but the node is only initialized
    // in `postjoin()`.
    unsafe { RAFT_NODE.as_deref() }.ok_or(Error::Uninitialized)
}

#[proc(packed_args)]
fn raft_interact(pbs: Vec<traft::MessagePb>) -> Result<(), Box<dyn std::error::Error>> {
    let node = global()?;
    for pb in pbs {
        node.step(raft::Message::try_from(pb)?);
    }
    Ok(())
}

#[proc(packed_args)]
fn raft_join(req: JoinRequest) -> Result<JoinResponse, Box<dyn std::error::Error>> {
    let node = global()?;

    let cluster_id = Storage::cluster_id()?.ok_or("cluster_id is not set yet")?;

    if req.cluster_id != cluster_id {
        return Err(Box::new(Error::ClusterIdMismatch {
            instance_cluster_id: req.cluster_id,
            cluster_cluster_id: cluster_id,
        }));
    }

    let peer = node.handle_topology_request(req.into())?;
    let box_replication = Storage::box_replication(&peer.replicaset_id, Some(peer.commit_index))?;

    // A joined peer needs to communicate with other nodes.
    // Provide it the list of raft voters in response.
    let mut raft_group = vec![];
    for raft_id in Storage::voters()?.into_iter() {
        if let Some(peer) = Storage::peer_by_raft_id(raft_id)? {
            raft_group.push(peer);
        } else {
            crate::warn_or_panic!("peer missing in storage, raft_id: {}", raft_id);
        }
    }

    Ok(JoinResponse {
        peer,
        raft_group,
        box_replication,
    })
}
