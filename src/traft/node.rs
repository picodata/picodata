//! This module incapsulates most of the application-specific logics.
//!
//! It's responsible for
//! - handling proposals,
//! - handling configuration changes,
//! - processing raft `Ready` - persisting entries, communicating with other raft nodes.

use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::raft::StateRole as RaftStateRole;
use ::raft::StorageError;
use ::raft::INVALID_ID;
use ::tarantool::error::TransactionError;
use ::tarantool::fiber;
use ::tarantool::fiber::{Cond, Mutex};
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
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
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
use crate::traft::Op;
use crate::traft::Topology;
use crate::traft::TopologyRequest;
use crate::traft::{
    ExpelRequest, ExpelResponse, JoinRequest, JoinResponse, SyncRaftRequest, UpdatePeerRequest,
};
use crate::traft::{RaftSpaceAccess, Storage};

use super::Grade;
use super::OpResult;

type RawNode = raft::RawNode<RaftSpaceAccess>;

#[derive(Clone, Debug, tlua::Push, tlua::PushInto)]
pub struct Status {
    /// `raft_id` of the current instance
    pub id: RaftId,
    /// `raft_id` of the leader instance
    pub leader_id: Option<RaftId>,
    /// One of "Follower", "Candidate", "Leader", "PreCandidate"
    pub raft_state: String,
    /// Whether instance has finished its `postjoin`
    /// initialization stage
    pub is_ready: bool,
}

/// The heart of `traft` module - the Node.
pub struct Node {
    inner_node: Rc<Mutex<InnerNode>>,
    pub(super) storage: RaftSpaceAccess,
    _main_loop: fiber::UnitJoinHandle<'static>,
    _conf_change_loop: fiber::UnitJoinHandle<'static>,
    status: Rc<RefCell<Status>>,
    raft_loop_cond: Rc<Cond>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("raft_id", &self.raft_id())
            .finish_non_exhaustive()
    }
}

impl Node {
    pub const TICK: Duration = Duration::from_millis(100);

    /// Initialize the raft node.
    /// **This function yields**
    pub fn new(storage: RaftSpaceAccess) -> Result<Self, RaftError> {
        let inner_node = InnerNode::new(storage.clone())?;

        let status = Rc::new(RefCell::new(Status {
            id: inner_node.raft_id(),
            leader_id: None,
            raft_state: "Follower".into(),
            is_ready: false,
        }));

        let inner_node = Rc::new(Mutex::new(inner_node));
        let raft_loop_cond = Rc::new(Cond::new());

        let main_loop_fn = {
            let status = status.clone();
            let inner_node = inner_node.clone();
            let raft_loop_cond = raft_loop_cond.clone();
            move || raft_main_loop(status, inner_node, raft_loop_cond)
        };

        let conf_change_loop_fn = {
            let status = status.clone();
            let storage = storage.clone();
            move || raft_conf_change_loop(status, storage)
        };

        let node = Node {
            inner_node,
            status,
            raft_loop_cond,
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
            storage,
        };

        // Wait for the node to enter the main loop
        node.tick_and_yield(0);
        Ok(node)
    }

    pub fn raft_id(&self) -> RaftId {
        self.status.borrow().id
    }

    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }

    pub fn mark_as_ready(&self) {
        self.status.borrow_mut().is_ready = true;
        event::broadcast(Event::StatusChanged);
    }

    /// Wait for the status to be changed.
    /// **This function yields**
    pub fn wait_status(&self) {
        event::wait(Event::StatusChanged).expect("Events system wasn't initialized");
    }

    /// **This function yields**
    pub fn wait_for_read_state(&self, timeout: Duration) -> Result<RaftIndex, Error> {
        let notify = self.raw_operation(|inner_node| inner_node.read_state_async())?;
        notify.recv_timeout::<RaftIndex>(timeout)
    }

    /// Propose an operation and wait for it's result.
    /// **This function yields**
    pub fn propose_and_wait<T: OpResult + Into<traft::Op>>(
        &self,
        op: T,
        timeout: Duration,
    ) -> Result<T::Result, Error> {
        let notify = self.raw_operation(|inner_node| inner_node.propose_async(op))?;
        notify.recv_timeout::<T::Result>(timeout)
    }

    /// Become a candidate and wait for a main loop round so that there's a
    /// chance we become the leader.
    /// **This function yields**
    pub fn campaign_and_yield(&self) -> Result<(), Error> {
        self.raw_operation(|inner_node| inner_node.campaign())?;
        // Even though we don't expect a response, we still should let the
        // main_loop do an iteration. Without rescheduling, the Ready state
        // wouldn't be processed, the Status wouldn't be updated, and some
        // assertions may fail (e.g. in `postjoin()` in `main.rs`).
        fiber::reschedule();
        Ok(())
    }

    /// **This function yields**
    pub fn step_and_yield(&self, msg: raft::Message) {
        self.raw_operation(|inner_node| inner_node.step(msg))
            .map_err(|e| tlog!(Error, "{e}"))
            .ok();
        // even though we don't expect a response, we still should let the
        // main_loop do an iteration
        fiber::reschedule();
    }

    /// **This function yields**
    pub fn tick_and_yield(&self, n_times: u32) {
        self.raw_operation(|inner_node| inner_node.tick(n_times));
        // even though we don't expect a response, we still should let the
        // main_loop do an iteration
        fiber::reschedule();
    }

    /// **This function yields**
    pub fn timeout_now(&self) {
        let raft_id = self.raft_id();
        self.step_and_yield(raft::Message {
            to: raft_id,
            from: raft_id,
            msg_type: raft::MessageType::MsgTimeoutNow,
            ..Default::default()
        })
    }

    /// Process the topology request and propose [`PersistPeer`] entry if
    /// appropriate.
    ///
    /// Returns an error if the callee node isn't a Raft leader.
    ///
    /// **This function yields**
    pub fn handle_topology_request_and_wait(
        &self,
        req: TopologyRequest,
    ) -> Result<traft::Peer, Error> {
        let notify =
            self.raw_operation(|inner_node| inner_node.process_topology_request_async(req))?;
        notify.recv::<Peer>()
    }

    /// Only the conf_change_loop on a leader is eligible to call this function.
    ///
    /// **This function yields**
    fn propose_conf_change_and_wait(
        &self,
        term: RaftTerm,
        conf_change: raft::ConfChangeV2,
    ) -> Result<(), Error> {
        let notify = self
            .raw_operation(|inner_node| inner_node.propose_conf_change_async(term, conf_change))?;
        notify.recv()
    }

    /// This function **may yield** if `self.raw_node` is acquired.
    #[inline]
    fn raw_operation<R>(&self, f: impl FnOnce(&mut InnerNode) -> R) -> R {
        let mut inner_node = self.inner_node.lock();
        let res = f(&mut *inner_node);
        drop(inner_node);
        self.raft_loop_cond.broadcast();
        res
    }

    #[inline]
    pub fn all_traft_entries(&self) -> ::tarantool::Result<Vec<traft::Entry>> {
        self.storage.all_traft_entries()
    }
}

struct InnerNode {
    pub raw_node: RawNode,
    pub notifications: HashMap<LogicalClock, Notify>,
    topology_cache: CachedCell<RaftTerm, Topology>,
    storage: RaftSpaceAccess,
    pool: ConnectionPool,
    lc: LogicalClock,
}

impl InnerNode {
    fn new(
        mut storage: RaftSpaceAccess,
        // TODO: provide clusterwide space access
    ) -> Result<Self, RaftError> {
        let box_err = |e| StorageError::Other(Box::new(e));

        let raft_id: RaftId = storage.raft_id().map_err(box_err)?.unwrap();
        let applied: RaftIndex = storage.applied().map_err(box_err)?.unwrap_or(0);
        let lc = {
            let gen = storage.gen().unwrap().unwrap_or(0) + 1;
            storage.persist_gen(gen).unwrap();
            LogicalClock::new(raft_id, gen)
        };

        let mut pool = ConnectionPool::builder()
            .handler_name(stringify_cfunc!(raft_interact))
            .call_timeout(Node::TICK * 4)
            .connect_timeout(Node::TICK * 4)
            .inactivity_timeout(Duration::from_secs(60))
            .build();

        for peer in Storage::peers()? {
            pool.connect(peer.raft_id, peer.peer_address);
        }

        let cfg = raft::Config {
            id: raft_id,
            applied,
            pre_vote: true,
            ..Default::default()
        };

        let raw_node = RawNode::new(&cfg, storage.clone(), &tlog::root())?;

        Ok(Self {
            raw_node,
            notifications: Default::default(),
            topology_cache: CachedCell::new(),
            storage,
            pool,
            lc,
        })
    }

    fn raft_id(&self) -> RaftId {
        self.raw_node.raft.id
    }

    pub fn read_state_async(&mut self) -> Result<Notify, RaftError> {
        // In some states `raft-rs` ignores the ReadIndex request.
        // Check it preliminary, don't wait for the timeout.
        //
        // See for details:
        // - <https://github.com/tikv/raft-rs/blob/v0.6.0/src/raft.rs#L2058>
        // - <https://github.com/tikv/raft-rs/blob/v0.6.0/src/raft.rs#L2323>
        let leader_doesnt_exist = self.raw_node.raft.leader_id == INVALID_ID;
        let term_just_started = // ...
            self.raw_node.raft.state == RaftStateRole::Leader
            && !self.raw_node.raft.commit_to_current_term();

        if leader_doesnt_exist || term_just_started {
            return Err(RaftError::ProposalDropped);
        }

        let (lc, notify) = self.schedule_notification();
        // read_index puts this context into an Entry,
        // so we've got to compose full EntryContext,
        // despite single LogicalClock would be enough
        let ctx = traft::EntryContextNormal::new(lc, Op::Nop);
        self.raw_node.read_index(ctx.to_bytes());
        Ok(notify)
    }

    pub fn propose_async<T>(&mut self, op: T) -> Result<Notify, RaftError>
    where
        T: Into<traft::Op>,
    {
        let (lc, notify) = self.schedule_notification();
        let ctx = traft::EntryContextNormal::new(lc, op);
        self.raw_node.propose(ctx.to_bytes(), vec![])?;
        Ok(notify)
    }

    pub fn campaign(&mut self) -> Result<(), RaftError> {
        self.raw_node.campaign()
    }

    pub fn step(&mut self, msg: raft::Message) -> Result<(), RaftError> {
        if msg.to != self.raft_id() {
            return Ok(());
        }

        // TODO check it's not a MsgPropose with op::PersistPeer.
        // TODO check it's not a MsgPropose with ConfChange.
        self.raw_node.step(msg)
    }

    pub fn tick(&mut self, n_times: u32) {
        for _ in 0..n_times {
            self.raw_node.tick();
        }
    }

    /// Process the topology request and propose [`PersistPeer`] entry if
    /// appropriate.
    ///
    /// Returns an error if the callee node isn't a Raft leader.
    ///
    /// **This function yields**
    pub fn process_topology_request_async(
        &mut self,
        req: TopologyRequest,
    ) -> Result<Notify, RaftError> {
        if self.raw_node.raft.state != RaftStateRole::Leader {
            return Err(RaftError::ConfChangeError("not a leader".into()));
        }

        let mut topology = self
            .topology_cache
            .pop(&self.raw_node.raft.term)
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
            TopologyRequest::UpdatePeer(req) => topology.update_peer(req),
        };

        let mut peer = crate::unwrap_ok_or!(peer_result, Err(e) => {
            self.topology_cache.put(self.raw_node.raft.term, topology);
            return Err(RaftError::ConfChangeError(e));
        });

        peer.commit_index = self.raw_node.raft.raft_log.last_index() + 1;

        let (lc, notify) = self.schedule_notification();
        let ctx = traft::EntryContextNormal::new(lc, Op::PersistPeer { peer });
        self.raw_node.propose(ctx.to_bytes(), vec![])?;
        self.topology_cache.put(self.raw_node.raft.term, topology);
        Ok(notify)
    }

    fn propose_conf_change_async(
        &mut self,
        term: RaftTerm,
        conf_change: raft::ConfChangeV2,
    ) -> Result<Notify, RaftError> {
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
            if self.raw_node.raft.state != RaftStateRole::Leader {
                break Some("not a leader");
            }

            if term != self.raw_node.raft.term {
                break Some("raft term mismatch");
            }

            // Without this check the node would silently ignore the conf change.
            // See https://github.com/tikv/raft-rs/blob/v0.6.0/src/raft.rs#L2014-L2026
            if self.raw_node.raft.has_pending_conf() {
                break Some("already has pending confchange");
            }

            break None;
        };

        if let Some(e) = reason {
            return Err(RaftError::ConfChangeError(e.into()));
        }

        let prev_index = self.raw_node.raft.raft_log.last_index();
        self.raw_node.propose_conf_change(vec![], conf_change)?;
        let last_index = self.raw_node.raft.raft_log.last_index();

        // oops, current instance isn't actually a leader
        // (which is impossible in theory, but we're not
        // sure in practice) and sent the ConfChange message
        // to the raft network instead of appending it to the
        // raft log.
        assert_eq!(last_index, prev_index + 1);

        let (rx, tx) = Notify::new().into_clones();
        with_joint_state_latch(|joint_state_latch| {
            assert!(joint_state_latch.take().is_none());
            event::broadcast(Event::JointStateEnter);
            joint_state_latch.set(Some(JointStateLatch {
                index: last_index,
                notify: tx,
            }));
        });
        Ok(rx)
    }

    /// Is called during a transaction
    fn handle_committed_entries(
        &mut self,
        entries: Vec<raft::Entry>,
        topology_changed: &mut bool,
        expelled: &mut bool,
    ) {
        for entry in &entries {
            let entry = match traft::Entry::try_from(entry) {
                Ok(v) => v,
                Err(e) => {
                    tlog!(Error, "abnormal entry: {e}, entry = {entry:?}");
                    continue;
                }
            };

            match entry.entry_type {
                raft::EntryType::EntryNormal => {
                    self.handle_committed_normal_entry(entry, topology_changed, expelled)
                }
                raft::EntryType::EntryConfChange | raft::EntryType::EntryConfChangeV2 => {
                    self.handle_committed_conf_change(entry)
                }
            }
        }

        if let Some(last_entry) = entries.last() {
            if let Err(e) = self.storage.persist_applied(last_entry.index) {
                tlog!(
                    Error,
                    "error persisting applied index: {e}";
                    "index" => last_entry.index
                );
            } else {
                event::broadcast(Event::RaftEntryApplied);
            }
        }
    }

    /// Is called during a transaction
    fn handle_committed_normal_entry(
        &mut self,
        entry: traft::Entry,
        topology_changed: &mut bool,
        expelled: &mut bool,
    ) {
        assert_eq!(entry.entry_type, raft::EntryType::EntryNormal);
        let result = entry.op().unwrap_or(&traft::Op::Nop).on_commit();

        if let Some(lc) = entry.lc() {
            if let Some(notify) = self.notifications.remove(lc) {
                notify.notify_ok_any(result);
            }
        }

        if let Some(traft::Op::PersistPeer { peer }) = entry.op() {
            self.pool.connect(peer.raft_id, peer.peer_address.clone());
            *topology_changed = true;
            if peer.grade == Grade::Expelled && peer.raft_id == self.raft_id() {
                // cannot exit during a transaction
                *expelled = true;
            }
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
                event::broadcast(Event::JointStateDrop);
            }
        });
    }

    /// Is called during a transaction
    fn handle_committed_conf_change(&mut self, entry: traft::Entry) {
        let latch_unlock = || {
            with_joint_state_latch(|joint_state_latch| {
                if let Some(latch) = joint_state_latch.take() {
                    latch.notify.notify_ok(());
                    event::broadcast(Event::JointStateLeave);
                }
            });
        };

        // Beware: a tiny difference in type names (`V2` or not `V2`)
        // makes a significant difference in `entry.data` binary layout and
        // in joint state transitions.

        // `ConfChangeTransition::Auto` implies that `ConfChangeV2` may be
        // applied in an instant without entering the joint state.

        let (is_joint, conf_state) = match entry.entry_type {
            raft::EntryType::EntryConfChange => {
                let mut cc = raft::ConfChange::default();
                cc.merge_from_bytes(&entry.data).unwrap();

                latch_unlock();

                (false, self.raw_node.apply_conf_change(&cc).unwrap())
            }
            raft::EntryType::EntryConfChangeV2 => {
                let mut cc = raft::ConfChangeV2::default();
                cc.merge_from_bytes(&entry.data).unwrap();

                // Unlock the latch when either of conditions is met:
                // - conf_change will leave the joint state;
                // - or it will be applied without even entering one.
                let leave_joint = cc.leave_joint() || cc.enter_joint().is_none();
                if leave_joint {
                    latch_unlock();
                }

                // ConfChangeTransition::Auto implies that at this
                // moment raft-rs will implicitly propose another empty
                // conf change that represents leaving the joint state.
                (!leave_joint, self.raw_node.apply_conf_change(&cc).unwrap())
            }
            _ => unreachable!(),
        };

        let raft_id = &self.raft_id();
        let voters_old = self.storage.voters().unwrap().unwrap_or_default();
        if voters_old.contains(raft_id) && !conf_state.voters.contains(raft_id) {
            if is_joint {
                event::broadcast_when(Event::Demoted, Event::JointStateLeave).ok();
            } else {
                event::broadcast(Event::Demoted);
            }
        }

        self.storage.persist_conf_state(&conf_state).unwrap();
    }

    /// Is called during a transaction
    fn handle_read_states(&mut self, read_states: Vec<raft::ReadState>) {
        for rs in read_states {
            let ctx = match traft::EntryContextNormal::read_from_bytes(&rs.request_ctx) {
                Ok(Some(v)) => v,
                Ok(None) => continue,
                Err(e) => {
                    tlog!(Error, "abnormal read_state: {e}, read_state = {rs:?}");
                    continue;
                }
            };

            if let Some(notify) = self.notifications.remove(&ctx.lc) {
                notify.notify_ok(rs.index);
            }
        }
    }

    /// Is called during a transaction
    fn handle_messages(&self, messages: Vec<raft::Message>) {
        for msg in messages {
            if let Err(e) = self.pool.send(&msg) {
                tlog!(Error, "{e}");
            }
        }
    }

    fn advance(
        &mut self,
        status: &RefCell<Status>,
        topology_changed: &mut bool,
        expelled: &mut bool,
    ) {
        // Get the `Ready` with `RawNode::ready` interface.
        if !self.raw_node.has_ready() {
            return;
        }

        let mut ready: raft::Ready = self.raw_node.ready();

        start_transaction(|| -> Result<(), TransactionError> {
            if !ready.messages().is_empty() {
                // Send out the messages come from the node.
                let messages = ready.take_messages();
                self.handle_messages(messages);
            }

            if !ready.snapshot().is_empty() {
                // This is a snapshot, we need to apply the snapshot at first.
                unimplemented!();
            }

            let committed_entries = ready.take_committed_entries();
            self.handle_committed_entries(committed_entries, topology_changed, expelled);

            if !ready.entries().is_empty() {
                let e = ready.entries();
                // Append entries to the Raft log.
                self.storage.persist_entries(e).unwrap();
            }

            if let Some(hs) = ready.hs() {
                // Raft HardState changed, and we need to persist it.
                // let hs = hs.clone();
                self.storage.persist_hard_state(hs).unwrap();
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
                self.handle_messages(messages);
            }

            let read_states = ready.take_read_states();
            self.handle_read_states(read_states);

            Ok(())
        })
        .unwrap();

        // Advance the Raft.
        let mut light_rd = self.raw_node.advance(ready);

        // Update commit index.
        start_transaction(|| -> Result<(), TransactionError> {
            if let Some(commit) = light_rd.commit_index() {
                self.storage.persist_commit(commit).unwrap();
            }

            // Send out the messages.
            let messages = light_rd.take_messages();
            self.handle_messages(messages);

            // Apply all committed entries.
            let committed_entries = light_rd.take_committed_entries();
            self.handle_committed_entries(committed_entries, topology_changed, expelled);

            // Advance the apply index.
            self.raw_node.advance_apply();
            Ok(())
        })
        .unwrap();
    }

    #[inline]
    fn cleanup_notifications(&mut self) {
        self.notifications
            .retain(|_, notify: &mut Notify| !notify.is_closed());
    }

    /// Generates a pair of logical clock and a notification channel.
    /// Logical clock is a unique identifier suitable for tagging
    /// entries in raft log. Notification is broadcasted when the
    /// corresponding entry is committed.
    #[inline]
    fn schedule_notification(&mut self) -> (LogicalClock, Notify) {
        let (rx, tx) = Notify::new().into_clones();
        let lc = {
            self.lc.inc();
            self.lc
        };
        self.notifications.insert(lc, tx);
        (lc, rx)
    }
}

#[derive(Debug)]
struct JointStateLatch {
    /// Index of the latest ConfChange entry proposed.
    /// Helps detecting when the entry is overridden
    /// due to a re-election.
    index: RaftIndex,

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

fn raft_main_loop(
    status: Rc<RefCell<Status>>,
    inner_node: Rc<Mutex<InnerNode>>,
    raft_loop_cond: Rc<Cond>,
) {
    let mut next_tick = Instant::now() + Node::TICK;

    loop {
        raft_loop_cond.wait_timeout(Node::TICK);

        let mut inner_node = inner_node.lock();
        inner_node.cleanup_notifications();

        let now = Instant::now();
        if now > next_tick {
            next_tick = now + Node::TICK;
            inner_node.raw_node.tick();
        }

        let mut topology_changed = false;
        let mut expelled = false;
        inner_node.advance(&status, &mut topology_changed, &mut expelled);

        if expelled {
            crate::tarantool::exit(0);
        }

        if topology_changed {
            event::broadcast(Event::TopologyChanged);
            if let Some(peer) =
                traft::Storage::peer_by_raft_id(inner_node.raw_node.raft.id).unwrap()
            {
                let mut box_cfg = crate::tarantool::cfg().unwrap();
                assert_eq!(box_cfg.replication_connect_quorum, 0);
                box_cfg.replication =
                    traft::Storage::box_replication(&peer.replicaset_id, None).unwrap();
                crate::tarantool::set_cfg(&box_cfg);
            }
        }
    }
}

fn raft_conf_change(storage: &RaftSpaceAccess, peers: &[Peer]) -> Option<raft::ConfChangeV2> {
    let voter_ids: HashSet<RaftId> =
        HashSet::from_iter(storage.voters().unwrap().unwrap_or_default());
    let learner_ids: HashSet<RaftId> =
        HashSet::from_iter(storage.learners().unwrap().unwrap_or_default());
    let peer_is_active: HashMap<RaftId, bool> = peers
        .iter()
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

    if total_active == 0 {
        return None;
    }

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
        return None;
    }

    let conf_change = raft::ConfChangeV2 {
        transition: raft::ConfChangeTransition::Auto,
        changes: changes.into(),
        ..Default::default()
    };

    Some(conf_change)
}

fn raft_conf_change_loop(status: Rc<RefCell<Status>>, storage: RaftSpaceAccess) {
    loop {
        if status.borrow().raft_state != "Leader" {
            event::wait(Event::StatusChanged).expect("Events system must be initialized");
            continue;
        }

        let peers = Storage::peers().unwrap();
        let term = storage.term().unwrap().unwrap_or(0);
        let node = global().expect("must be initialized");

        if let Some(conf_change) = raft_conf_change(&storage, &peers) {
            // main_loop gives the warranty that every ProposeConfChange
            // will sometimes be handled and there's no need in timeout.
            // It also guarantees that the notification will arrive only
            // after the node leaves the joint state.
            match node.propose_conf_change_and_wait(term, conf_change) {
                Ok(()) => tlog!(Info, "conf_change processed"),
                Err(e) => {
                    tlog!(Warning, "conf_change failed: {e}");
                    fiber::sleep(Duration::from_secs(1));
                }
            }
            continue;
        }

        event::wait(Event::TopologyChanged).expect("Events system must be initialized");
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
        node.step_and_yield(raft::Message::try_from(pb)?);
    }
    Ok(())
}

#[proc(packed_args)]
fn raft_join(req: JoinRequest) -> Result<JoinResponse, Box<dyn std::error::Error>> {
    let node = global()?;

    let cluster_id = node
        .storage
        .cluster_id()?
        .ok_or("cluster_id is not set yet")?;

    if req.cluster_id != cluster_id {
        return Err(Box::new(Error::ClusterIdMismatch {
            instance_cluster_id: req.cluster_id,
            cluster_cluster_id: cluster_id,
        }));
    }

    let peer = node.handle_topology_request_and_wait(req.into())?;
    let box_replication = Storage::box_replication(&peer.replicaset_id, Some(peer.commit_index))?;

    // A joined peer needs to communicate with other nodes.
    // Provide it the list of raft voters in response.
    let mut raft_group = vec![];
    for raft_id in node.storage.voters()?.unwrap_or_default().into_iter() {
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

// Lua API entrypoint, run on any node.
pub fn expel_wrapper(instance_id: String) -> Result<(), traft::error::Error> {
    match expel_by_instance_id(instance_id) {
        Ok(ExpelResponse {}) => Ok(()),
        Err(e) => Err(traft::error::Error::Other(e)),
    }
}

fn expel_by_instance_id(instance_id: String) -> Result<ExpelResponse, Box<dyn std::error::Error>> {
    let cluster_id = global()?
        .storage
        .cluster_id()?
        .ok_or("cluster_id is not set yet")?;

    expel(ExpelRequest {
        instance_id,
        cluster_id,
    })
}

// NetBox entrypoint. Run on any node.
#[proc(packed_args)]
fn raft_expel(req: ExpelRequest) -> Result<ExpelResponse, Box<dyn std::error::Error>> {
    expel(req)
}

// Netbox entrypoint. For run on Leader only. Don't call directly, use `raft_expel` instead.
#[proc(packed_args)]
fn raft_expel_on_leader(req: ExpelRequest) -> Result<ExpelResponse, Box<dyn std::error::Error>> {
    expel_on_leader(req)
}

fn expel(req: ExpelRequest) -> Result<ExpelResponse, Box<dyn std::error::Error>> {
    let node = global()?;
    let leader_id = node.status().leader_id.ok_or("leader_id not found")?;
    let leader = Storage::peer_by_raft_id(leader_id).unwrap().unwrap();
    let leader_address = leader.peer_address;

    let fn_name = stringify_cfunc!(traft::node::raft_expel_on_leader);

    match crate::tarantool::net_box_call(&leader_address, fn_name, &req, Duration::MAX) {
        Ok::<traft::ExpelResponse, _>(_resp) => Ok(ExpelResponse {}),
        Err(e) => Err(Box::new(e)),
    }
}

fn expel_on_leader(req: ExpelRequest) -> Result<ExpelResponse, Box<dyn std::error::Error>> {
    let cluster_id = global()?
        .storage
        .cluster_id()?
        .ok_or("cluster_id is not set yet")?;

    if req.cluster_id != cluster_id {
        return Err(Box::new(Error::ClusterIdMismatch {
            instance_cluster_id: req.cluster_id,
            cluster_cluster_id: cluster_id,
        }));
    }

    let node = global()?;

    let leader_id = node.status().leader_id.ok_or("leader_id not found")?;

    if node.raft_id() != leader_id {
        return Err(Box::from("not a leader"));
    }

    let req2 = UpdatePeerRequest::new(req.instance_id, req.cluster_id).with_grade(Grade::Expelled);
    node.handle_topology_request_and_wait(req2.into())?;

    Ok(ExpelResponse {})
}

// NetBox entrypoint. Run on any node.
#[proc(packed_args)]
fn raft_sync_raft(req: SyncRaftRequest) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + req.timeout;
    loop {
        if global()?.storage.commit().unwrap().unwrap() >= req.commit {
            return Ok(());
        }

        let now = Instant::now();
        if now > deadline {
            return Err(Box::new(Error::Timeout));
        }

        event::wait_timeout(Event::RaftEntryApplied, deadline - now)?;
    }
}

// Run on Leader
fn call_raft_sync_raft(promotee: &Peer, commit: u64) -> Result<(), Box<dyn std::error::Error>> {
    let fn_name = stringify_cfunc!(traft::node::raft_sync_raft);
    let req = SyncRaftRequest {
        commit,
        timeout: Duration::from_secs(10),
    };

    match crate::tarantool::net_box_call(&promotee.peer_address, fn_name, &req, Duration::MAX) {
        Ok::<(), _>(_) => Ok(()),
        Err(e) => Err(Box::new(e)),
    }
}

// Run on Leader by topology governor
fn sync_raft(promotee: &Peer) -> Result<(), Box<dyn std::error::Error>> {
    let commit = global()?.storage.commit().unwrap().unwrap();

    match call_raft_sync_raft(promotee, commit) {
        Ok(_) => {
            let node = global()?;

            let leader_id = node.status().leader_id.ok_or("leader_id not found")?;

            if node.raft_id() != leader_id {
                return Err(Box::from("not a leader"));
            }

            let instance_id = promotee.instance_id.clone();
            let cluster_id = node
                .storage
                .cluster_id()?
                .ok_or("cluster_id is not set yet")?;

            let req = UpdatePeerRequest::new(instance_id, cluster_id).with_grade(Grade::RaftSynced);

            node.handle_topology_request_and_wait(req.into())?;

            Ok(())
        }
        Err(e) => Err(e),
    }
}
