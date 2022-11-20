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
use ::tarantool::error::{TarantoolError, TransactionError};
use ::tarantool::fiber;
use ::tarantool::fiber::{Cond, Mutex};
use ::tarantool::proc;
use ::tarantool::tlua;
use ::tarantool::transaction::start_transaction;
use std::borrow::Cow;
use std::cell::Cell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::iter::repeat;
use std::rc::Rc;
use std::time::Duration;
use std::time::Instant;
use tarantool::space::UpdateOps;

use crate::governor::raft_conf_change;
use crate::governor::waiting_migrations;
use crate::kvcell::KVCell;
use crate::r#loop::{FlowControl, Loop};
use crate::stringify_cfunc;
use crate::traft::rpc;
use crate::traft::storage::ClusterSpace;
use crate::traft::ContextCoercion as _;
use crate::traft::OpDML;
use crate::traft::Peer;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
use crate::unwrap_some_or;
use crate::warn_or_panic;
use ::tarantool::util::IntoClones as _;
use protobuf::Message as _;

use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::event;
use crate::traft::event::Event;
use crate::traft::notify::Notify;
use crate::traft::rpc::sharding::cfg::ReplicasetWeights;
use crate::traft::rpc::{replication, sharding, sync};
use crate::traft::storage::StateKey;
use crate::traft::ConnectionPool;
use crate::traft::LogicalClock;
use crate::traft::Op;
use crate::traft::Topology;
use crate::traft::TopologyRequest;
use crate::traft::{JoinRequest, JoinResponse, UpdatePeerRequest};
use crate::traft::{RaftSpaceAccess, Storage};

use super::OpResult;
use super::{CurrentGrade, CurrentGradeVariant, TargetGradeVariant};

type RawNode = raft::RawNode<RaftSpaceAccess>;

::tarantool::define_str_enum! {
    pub enum RaftState {
        Follower = "Follower",
        Candidate = "Candidate",
        Leader = "Leader",
        PreCandidate = "PreCandidate",
    }
}

impl RaftState {
    pub fn is_leader(&self) -> bool {
        matches!(self, Self::Leader)
    }
}

impl From<RaftStateRole> for RaftState {
    fn from(role: RaftStateRole) -> Self {
        match role {
            RaftStateRole::Follower => Self::Follower,
            RaftStateRole::Candidate => Self::Candidate,
            RaftStateRole::Leader => Self::Leader,
            RaftStateRole::PreCandidate => Self::PreCandidate,
        }
    }
}

#[derive(Copy, Clone, Debug, tlua::Push, tlua::PushInto)]
pub struct Status {
    /// `raft_id` of the current instance
    pub id: RaftId,
    /// `raft_id` of the leader instance
    pub leader_id: Option<RaftId>,
    /// Current term number
    pub term: RaftTerm,
    /// Current raft state
    pub raft_state: RaftState,
}

impl Status {
    pub fn check_term(&self, requested_term: RaftTerm) -> traft::Result<()> {
        if requested_term != self.term {
            return Err(Error::TermMismatch {
                requested: requested_term,
                current: self.term,
            });
        }
        Ok(())
    }
}

/// The heart of `traft` module - the Node.
pub struct Node {
    /// RaftId of the Node.
    //
    // It appears twice in the Node: here and in `status.id`.
    // This is a concious decision.
    // `self.raft_id()` is used in Rust API, and
    // `self.status()` is mostly useful in Lua API.
    raft_id: RaftId,

    node_impl: Rc<Mutex<NodeImpl>>,
    pub(crate) storage: Storage,
    main_loop: MainLoop,
    _conf_change_loop: fiber::UnitJoinHandle<'static>,
    status: Rc<Cell<Status>>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("raft_id", &self.raft_id)
            .finish_non_exhaustive()
    }
}

impl Node {
    /// Initialize the raft node.
    /// **This function yields**
    pub fn new(storage: Storage) -> Result<Self, RaftError> {
        let node_impl = NodeImpl::new(storage.clone())?;

        let raft_id = node_impl.raft_id();
        let status = Rc::new(Cell::new(Status {
            id: raft_id,
            leader_id: None,
            term: traft::INIT_RAFT_TERM,
            raft_state: RaftState::Follower,
        }));

        let node_impl = Rc::new(Mutex::new(node_impl));

        let conf_change_loop_fn = {
            let status = status.clone();
            let storage = storage.clone();
            move || raft_conf_change_loop(status, storage)
        };

        let node = Node {
            raft_id,
            main_loop: MainLoop::start(status.clone(), node_impl.clone()), // yields
            _conf_change_loop: fiber::Builder::new()
                .name("governor_loop")
                .proc(conf_change_loop_fn)
                .start()
                .unwrap(),
            node_impl,
            storage,
            status,
        };

        // Wait for the node to enter the main loop
        node.tick_and_yield(0);
        Ok(node)
    }

    pub fn raft_id(&self) -> RaftId {
        self.raft_id
    }

    pub fn status(&self) -> Status {
        self.status.get()
    }

    /// Wait for the status to be changed.
    /// **This function yields**
    pub fn wait_status(&self) {
        event::wait(Event::StatusChanged).expect("Events system wasn't initialized");
    }

    /// **This function yields**
    pub fn wait_for_read_state(&self, timeout: Duration) -> traft::Result<RaftIndex> {
        let notify = self.raw_operation(|node_impl| node_impl.read_state_async())?;
        notify.recv_timeout::<RaftIndex>(timeout)
    }

    /// Propose an operation and wait for it's result.
    /// **This function yields**
    pub fn propose_and_wait<T: OpResult + Into<traft::Op>>(
        &self,
        op: T,
        timeout: Duration,
    ) -> traft::Result<T::Result> {
        let notify = self.raw_operation(|node_impl| node_impl.propose_async(op))?;
        notify.recv_timeout::<T::Result>(timeout)
    }

    /// Become a candidate and wait for a main loop round so that there's a
    /// chance we become the leader.
    /// **This function yields**
    pub fn campaign_and_yield(&self) -> traft::Result<()> {
        self.raw_operation(|node_impl| node_impl.campaign())?;
        // Even though we don't expect a response, we still should let the
        // main_loop do an iteration. Without rescheduling, the Ready state
        // wouldn't be processed, the Status wouldn't be updated, and some
        // assertions may fail (e.g. in `postjoin()` in `main.rs`).
        fiber::reschedule();
        Ok(())
    }

    /// **This function yields**
    pub fn step_and_yield(&self, msg: raft::Message) {
        self.raw_operation(|node_impl| node_impl.step(msg))
            .map_err(|e| tlog!(Error, "{e}"))
            .ok();
        // even though we don't expect a response, we still should let the
        // main_loop do an iteration
        fiber::reschedule();
    }

    /// **This function yields**
    pub fn tick_and_yield(&self, n_times: u32) {
        self.raw_operation(|node_impl| node_impl.tick(n_times));
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

    /// Processes the topology request and appends [`Op::PersistPeer`]
    /// entry to the raft log (if successful).
    ///
    /// Returns the resulting peer when the entry is committed.
    ///
    /// Returns an error if the callee node isn't a raft leader.
    ///
    /// **This function yields**
    pub fn handle_topology_request_and_wait(
        &self,
        req: TopologyRequest,
    ) -> traft::Result<Box<traft::Peer>> {
        let notify =
            self.raw_operation(|node_impl| node_impl.process_topology_request_async(req))?;
        notify.recv()
    }

    /// Only the conf_change_loop on a leader is eligible to call this function.
    ///
    /// **This function yields**
    fn propose_conf_change_and_wait(
        &self,
        term: RaftTerm,
        conf_change: raft::ConfChangeV2,
    ) -> traft::Result<()> {
        let notify =
            self.raw_operation(|node_impl| node_impl.propose_conf_change_async(term, conf_change))?;
        notify.recv()
    }

    /// Attempt to transfer leadership to a given node and yield.
    ///
    /// **This function yields**
    pub fn transfer_leadership_and_yield(&self, new_leader_id: RaftId) {
        self.raw_operation(|node_impl| node_impl.raw_node.transfer_leader(new_leader_id));
        fiber::reschedule();
    }

    /// This function **may yield** if `self.node_impl` mutex is acquired.
    #[inline]
    fn raw_operation<R>(&self, f: impl FnOnce(&mut NodeImpl) -> R) -> R {
        let mut node_impl = self.node_impl.lock();
        let res = f(&mut node_impl);
        drop(node_impl);
        self.main_loop.wakeup();
        res
    }

    #[inline]
    pub fn all_traft_entries(&self) -> ::tarantool::Result<Vec<traft::Entry>> {
        self.storage.raft.all_traft_entries()
    }
}

struct NodeImpl {
    pub raw_node: RawNode,
    pub notifications: HashMap<LogicalClock, Notify>,
    topology_cache: KVCell<RaftTerm, Topology>,
    joint_state_latch: KVCell<RaftIndex, Notify>,
    storage: Storage,
    pool: ConnectionPool,
    lc: LogicalClock,
}

impl NodeImpl {
    fn new(storage: Storage) -> Result<Self, RaftError> {
        let box_err = |e| StorageError::Other(Box::new(e));

        let raft_id: RaftId = storage.raft.raft_id().map_err(box_err)?.unwrap();
        let applied: RaftIndex = storage.raft.applied().map_err(box_err)?.unwrap_or(0);
        let lc = {
            let gen = storage.raft.gen().unwrap().unwrap_or(0) + 1;
            storage.raft.persist_gen(gen).unwrap();
            LogicalClock::new(raft_id, gen)
        };

        let pool = ConnectionPool::builder(storage.peers.clone())
            .handler_name(stringify_cfunc!(proc_raft_interact))
            .call_timeout(MainLoop::TICK * 4)
            .connect_timeout(MainLoop::TICK * 4)
            .inactivity_timeout(Duration::from_secs(60))
            .build();

        let cfg = raft::Config {
            id: raft_id,
            applied,
            pre_vote: true,
            ..Default::default()
        };

        let raw_node = RawNode::new(&cfg, storage.raft.clone(), &tlog::root())?;

        Ok(Self {
            raw_node,
            notifications: Default::default(),
            topology_cache: KVCell::new(),
            joint_state_latch: KVCell::new(),
            storage,
            pool,
            lc,
        })
    }

    fn raft_id(&self) -> RaftId {
        self.raw_node.raft.id
    }

    /// Provides mutable access to the Topology struct which reflects
    /// uncommitted state of the cluster. Ensures the node is a leader.
    /// In case it's not — returns an error.
    ///
    /// It's important to access topology through this function so that
    /// new changes are consistent with uncommitted ones.
    fn topology_mut(&mut self) -> Result<&mut Topology, Error> {
        if self.raw_node.raft.state != RaftStateRole::Leader {
            self.topology_cache.take(); // invalidate the cache
            return Err(Error::NotALeader);
        }

        let current_term = self.raw_node.raft.term;

        let topology: Topology = unwrap_some_or! {
            self.topology_cache.take_or_drop(&current_term),
            {
                let peers = self.storage.peers.all_peers()?;
                let replication_factor = self
                    .storage
                    .state
                    .get(StateKey::ReplicationFactor)?
                    .ok_or_else(|| Error::other("missing replication_factor value in storage"))?;
                Topology::from_peers(peers).with_replication_factor(replication_factor)
            }
        };

        Ok(self.topology_cache.insert(current_term, topology))
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

    /// Processes the topology request and appends [`Op::PersistPeer`]
    /// entry to the raft log (if successful).
    ///
    /// Returns an error if the callee node isn't a Raft leader.
    ///
    /// **This function yields**
    pub fn process_topology_request_async(
        &mut self,
        req: TopologyRequest,
    ) -> traft::Result<Notify> {
        let topology = self.topology_mut()?;
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

        let mut peer = peer_result.map_err(RaftError::ConfChangeError)?;
        peer.commit_index = self.raw_node.raft.raft_log.last_index() + 1;

        let (lc, notify) = self.schedule_notification();
        let ctx = traft::EntryContextNormal::new(lc, Op::persist_peer(peer));

        // Important! Calling `raw_node.propose()` may result in
        // `ProposalDropped` error, but the topology has already been
        // modified. The correct handling of this case should be the
        // following.
        //
        // The `topology_cache` should be preserved. It won't be fully
        // consistent anymore, but that's bearable. (TODO: examine how
        // the particular requests are handled). At least it doesn't
        // much differ from the case of overriding the entry due to a
        // re-election.
        //
        // On the other hand, dropping topology_cache may be much more
        // harmful. Loss of the uncommitted entries could result in
        // assigning the same `raft_id` to a two different nodes.
        //
        self.raw_node.propose(ctx.to_bytes(), vec![])?;

        Ok(notify)
    }

    fn propose_conf_change_async(
        &mut self,
        term: RaftTerm,
        conf_change: raft::ConfChangeV2,
    ) -> Result<Notify, RaftError> {
        // In some states proposing a ConfChange is impossible.
        // Check if there's a reason to reject it.

        // Checking leadership is only needed for the
        // correct latch management. It doesn't affect
        // raft correctness. Checking the instance is a
        // leader makes sure the proposed `ConfChange`
        // is appended to the raft log immediately
        // instead of sending `MsgPropose` over the
        // network.
        if self.raw_node.raft.state != RaftStateRole::Leader {
            return Err(RaftError::ConfChangeError("not a leader".into()));
        }

        if term != self.raw_node.raft.term {
            return Err(RaftError::ConfChangeError("raft term mismatch".into()));
        }

        // Without this check the node would silently ignore the conf change.
        // See https://github.com/tikv/raft-rs/blob/v0.6.0/src/raft.rs#L2014-L2026
        if self.raw_node.raft.has_pending_conf() {
            return Err(RaftError::ConfChangeError(
                "already has pending confchange".into(),
            ));
        }

        let prev_index = self.raw_node.raft.raft_log.last_index();
        self.raw_node.propose_conf_change(vec![], conf_change)?;

        // Ensure the ConfChange was actually appended to the log.
        // Otherwise it's a problem: current instance isn't actually a
        // leader (which is impossible in theory, but we're not sure in
        // practice) and sent the message to the raft network. It may
        // lead to an inconsistency.
        let last_index = self.raw_node.raft.raft_log.last_index();
        assert_eq!(last_index, prev_index + 1);

        if !self.joint_state_latch.is_empty() {
            warn_or_panic!("joint state latch is locked");
        }

        let (rx, tx) = Notify::new().into_clones();
        self.joint_state_latch.insert(last_index, tx);
        event::broadcast(Event::JointStateEnter);

        Ok(rx)
    }

    /// Is called during a transaction
    fn handle_committed_entries(
        &mut self,
        entries: &[raft::Entry],
        topology_changed: &mut bool,
        expelled: &mut bool,
    ) {
        for entry in entries {
            let entry = match traft::Entry::try_from(entry) {
                Ok(v) => v,
                Err(e) => {
                    tlog!(Error, "abnormal entry: {e}"; "entry" => ?entry);
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
            if let Err(e) = self.storage.raft.persist_applied(last_entry.index) {
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
        let result = entry
            .op()
            .unwrap_or(&traft::Op::Nop)
            .on_commit(&self.storage.peers);

        if let Some(lc) = entry.lc() {
            if let Some(notify) = self.notifications.remove(lc) {
                notify.notify_ok_any(result);
            }
        }

        if let Some(traft::Op::PersistPeer { peer }) = entry.op() {
            *topology_changed = true;
            if peer.current_grade == CurrentGradeVariant::Expelled && peer.raft_id == self.raft_id()
            {
                // cannot exit during a transaction
                *expelled = true;
            }
        }

        if let Some(notify) = self.joint_state_latch.take_or_keep(&entry.index) {
            // It was expected to be a ConfChange entry, but it's
            // normal. Raft must have overriden it, or there was
            // a re-election.
            let e = RaftError::ConfChangeError("rolled back".into());

            notify.notify_err(e);
            event::broadcast(Event::JointStateDrop);
        }
    }

    /// Is called during a transaction
    fn handle_committed_conf_change(&mut self, entry: traft::Entry) {
        let mut latch_unlock = || {
            if let Some(notify) = self.joint_state_latch.take() {
                notify.notify_ok(());
                event::broadcast(Event::JointStateLeave);
            }
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
        let voters_old = self.storage.raft.voters().unwrap().unwrap_or_default();
        if voters_old.contains(raft_id) && !conf_state.voters.contains(raft_id) {
            if is_joint {
                event::broadcast_when(Event::Demoted, Event::JointStateLeave).ok();
            } else {
                event::broadcast(Event::Demoted);
            }
        }

        self.storage.raft.persist_conf_state(&conf_state).unwrap();
    }

    /// Is called during a transaction
    fn handle_read_states(&mut self, read_states: &[raft::ReadState]) {
        for rs in read_states {
            let ctx = match traft::EntryContextNormal::read_from_bytes(&rs.request_ctx) {
                Ok(Some(v)) => v,
                Ok(None) => continue,
                Err(e) => {
                    tlog!(Error, "abnormal read_state: {e}"; "read_state" => ?rs);
                    continue;
                }
            };

            if let Some(notify) = self.notifications.remove(&ctx.lc) {
                notify.notify_ok(rs.index);
            }
        }
    }

    /// Is called during a transaction
    fn handle_messages(&mut self, messages: Vec<raft::Message>) {
        for msg in messages {
            if let Err(e) = self.pool.send(msg) {
                tlog!(Error, "{e}");
            }
        }
    }

    /// Processes a so-called "ready state" of the [`raft::RawNode`].
    ///
    /// This includes:
    /// - Sending messages to other instances (raft nodes);
    /// - Applying committed entries;
    /// - Persisting uncommitted entries;
    /// - Persisting hard state (term, vote, commit);
    /// - Notifying pending fibers;
    ///
    /// See also:
    ///
    /// - <https://github.com/tikv/raft-rs/blob/v0.6.0/src/raw_node.rs#L85>
    /// - or better <https://github.com/etcd-io/etcd/blob/v3.5.5/raft/node.go#L49>
    ///
    /// This function yields.
    fn advance(&mut self, status: &Cell<Status>, topology_changed: &mut bool, expelled: &mut bool) {
        // Get the `Ready` with `RawNode::ready` interface.
        if !self.raw_node.has_ready() {
            return;
        }

        let mut ready: raft::Ready = self.raw_node.ready();

        // Send out messages to the other nodes.
        self.handle_messages(ready.take_messages());

        // This is a snapshot, we need to apply the snapshot at first.
        if !ready.snapshot().is_empty() {
            unimplemented!();
        }

        if let Some(ss) = ready.ss() {
            let mut s = status.get();
            s.leader_id = (ss.leader_id != INVALID_ID).then_some(ss.leader_id);
            s.raft_state = ss.raft_state.into();
            status.set(s);
            event::broadcast(Event::StatusChanged);
        }

        self.handle_read_states(ready.read_states());

        if let Err(e) = start_transaction(|| -> Result<(), TransactionError> {
            // Apply committed entries.
            self.handle_committed_entries(ready.committed_entries(), topology_changed, expelled);

            // Persist uncommitted entries in the raft log.
            self.storage.raft.persist_entries(ready.entries()).unwrap();

            // Raft HardState changed, and we need to persist it.
            if let Some(hs) = ready.hs() {
                self.storage.raft.persist_hard_state(hs).unwrap();

                let mut s = status.get();
                s.term = hs.term;
                status.set(s);
            }

            Ok(())
        }) {
            tlog!(Warning, "dropping raft ready: {ready:#?}");
            panic!("transaction failed: {e}, {}", TarantoolError::last());
        }

        // This bunch of messages is special. It must be sent only
        // AFTER the HardState, Entries and Snapshot are persisted
        // to the stable storage.
        self.handle_messages(ready.take_persisted_messages());

        // Advance the Raft.
        let mut light_rd = self.raw_node.advance(ready);

        // Send out messages to the other nodes.
        self.handle_messages(light_rd.take_messages());

        if let Err(e) = start_transaction(|| -> Result<(), TransactionError> {
            // Update commit index.
            if let Some(commit) = light_rd.commit_index() {
                self.storage.raft.persist_commit(commit).unwrap();
            }

            // Apply committed entries.
            self.handle_committed_entries(light_rd.committed_entries(), topology_changed, expelled);

            Ok(())
        }) {
            tlog!(Warning, "dropping raft light ready: {light_rd:#?}");
            panic!("transaction failed: {e}, {}", TarantoolError::last());
        }

        // Advance the apply index.
        self.raw_node.advance_apply();
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

struct MainLoop {
    _loop: Option<Loop>,
    loop_cond: Rc<Cond>,
    stop_flag: Rc<Cell<bool>>,
}

struct MainLoopArgs {
    status: Rc<Cell<Status>>,
    node_impl: Rc<Mutex<NodeImpl>>,
}

struct MainLoopState {
    next_tick: Instant,
    loop_cond: Rc<Cond>,
    stop_flag: Rc<Cell<bool>>,
}

impl MainLoop {
    pub const TICK: Duration = Duration::from_millis(100);

    fn start(status: Rc<Cell<Status>>, node_impl: Rc<Mutex<NodeImpl>>) -> Self {
        let loop_cond: Rc<Cond> = Default::default();
        let stop_flag: Rc<Cell<bool>> = Default::default();

        let args = MainLoopArgs { status, node_impl };
        let initial_state = MainLoopState {
            next_tick: Instant::now(),
            loop_cond: loop_cond.clone(),
            stop_flag: stop_flag.clone(),
        };

        Self {
            // implicit yield
            _loop: Some(Loop::start(
                "raft_main_loop",
                Self::iter_fn,
                args,
                initial_state,
            )),
            loop_cond,
            stop_flag,
        }
    }

    pub fn wakeup(&self) {
        self.loop_cond.broadcast();
    }

    fn iter_fn(args: &MainLoopArgs, state: &mut MainLoopState) -> FlowControl {
        state.loop_cond.wait_timeout(Self::TICK); // yields
        if state.stop_flag.take() {
            return FlowControl::Break;
        }

        let mut node_impl = args.node_impl.lock(); // yields
        if state.stop_flag.take() {
            return FlowControl::Break;
        }

        node_impl.cleanup_notifications();

        let now = Instant::now();
        if now > state.next_tick {
            state.next_tick = now + Self::TICK;
            node_impl.raw_node.tick();
        }

        let mut topology_changed = false;
        let mut expelled = false;
        node_impl.advance(&args.status, &mut topology_changed, &mut expelled); // yields
        if state.stop_flag.take() {
            return FlowControl::Break;
        }

        if expelled {
            crate::tarantool::exit(0);
        }

        if topology_changed {
            event::broadcast(Event::TopologyChanged);
        }

        FlowControl::Continue
    }
}

impl Drop for MainLoop {
    fn drop(&mut self) {
        self.stop_flag.set(true);
        self.loop_cond.broadcast();
        self._loop.take().unwrap().join(); // yields
    }
}

fn raft_conf_change_loop(status: Rc<Cell<Status>>, storage: Storage) {
    let mut pool = ConnectionPool::builder(storage.peers.clone())
        .call_timeout(Duration::from_secs(1))
        .connect_timeout(Duration::from_millis(500))
        .inactivity_timeout(Duration::from_secs(60))
        .build();

    // TODO: don't hardcode this
    const SYNC_TIMEOUT: Duration = Duration::from_secs(10);

    'governor: loop {
        if !status.get().raft_state.is_leader() {
            event::wait(Event::StatusChanged).expect("Events system must be initialized");
            continue 'governor;
        }

        let peers = storage.peers.all_peers().unwrap();
        let term = status.get().term;
        let cluster_id = storage.raft.cluster_id().unwrap().unwrap();
        let node = global().expect("must be initialized");

        ////////////////////////////////////////////////////////////////////////
        // conf change
        let voters = storage.raft.voters().unwrap().unwrap_or_default();
        let learners = storage.raft.learners().unwrap().unwrap_or_default();
        if let Some(conf_change) = raft_conf_change(&peers, &voters, &learners) {
            // main_loop gives the warranty that every ProposeConfChange
            // will sometimes be handled and there's no need in timeout.
            // It also guarantees that the notification will arrive only
            // after the node leaves the joint state.
            tlog!(Info, "proposing conf_change"; "cc" => ?conf_change);
            if let Err(e) = node.propose_conf_change_and_wait(term, conf_change) {
                tlog!(Warning, "failed proposing conf_change: {e}");
                fiber::sleep(Duration::from_secs(1));
            }
            continue 'governor;
        }

        ////////////////////////////////////////////////////////////////////////
        // offline/expel
        let to_offline = peers
            .iter()
            .filter(|peer| peer.current_grade != CurrentGradeVariant::Offline)
            // TODO: process them all, not just the first one
            .find(|peer| {
                let (target, current) = (peer.target_grade.variant, peer.current_grade.variant);
                matches!(target, TargetGradeVariant::Offline)
                    || !matches!(current, CurrentGradeVariant::Expelled)
                        && matches!(target, TargetGradeVariant::Expelled)
            });
        if let Some(peer) = to_offline {
            tlog!(
                Info,
                "processing {} {} -> {}",
                peer.instance_id,
                peer.current_grade,
                peer.target_grade
            );

            // transfer leadership, if we're the one who goes offline
            if peer.raft_id == node.raft_id {
                if let Some(new_leader) = maybe_responding(&peers).find(|peer| {
                    // FIXME: linear search
                    voters.contains(&peer.raft_id)
                }) {
                    tlog!(
                        Info,
                        "transferring leadership to {}",
                        new_leader.instance_id
                    );
                    node.transfer_leadership_and_yield(new_leader.raft_id);
                    event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                    continue 'governor;
                }
            }

            let replicaset_id = &peer.replicaset_id;
            // choose a new replicaset master if needed
            let res = (|| -> traft::Result<_> {
                let replicaset = storage.replicasets.get(replicaset_id)?;
                if replicaset
                    .map(|r| r.master_id == peer.instance_id)
                    .unwrap_or(false)
                {
                    let new_master =
                        maybe_responding(&peers).find(|p| p.replicaset_id == replicaset_id);
                    if let Some(peer) = new_master {
                        let mut ops = UpdateOps::new();
                        ops.assign("master_id", &peer.instance_id)?;

                        let op = OpDML::update(ClusterSpace::Replicasets, &[replicaset_id], ops)?;
                        tlog!(Info, "proposing replicaset master change"; "op" => ?op);
                        // TODO: don't hard code the timeout
                        node.propose_and_wait(op, Duration::from_secs(3))??;
                    } else {
                        tlog!(Info, "skip proposing replicaset master change");
                    }
                }
                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning, "failed proposing replicaset master change: {e}");
                // TODO: don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                continue 'governor;
            }

            // reconfigure vshard storages and routers
            let res = (|| -> traft::Result<_> {
                let commit = storage.raft.commit()?.unwrap();
                let reqs = maybe_responding(&peers)
                    .filter(|peer| {
                        peer.current_grade == CurrentGradeVariant::ShardingInitialized
                            || peer.current_grade == CurrentGradeVariant::Online
                    })
                    .map(|peer| {
                        tlog!(Info,
                            "calling rpc::sharding";
                            "instance_id" => %peer.instance_id
                        );
                        (
                            peer.instance_id.clone(),
                            sharding::Request {
                                term,
                                commit,
                                timeout: SYNC_TIMEOUT,
                                bootstrap: false,
                            },
                        )
                    });
                // TODO: don't hard code timeout
                let res = call_all(&mut pool, reqs, Duration::from_secs(3))?;
                for (_, resp) in res {
                    let sharding::Response {} = resp?;
                }
                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning, "failed calling rpc::sharding: {e}");
                // TODO: don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                continue 'governor;
            }

            // update peer's CurrentGrade
            let req = UpdatePeerRequest::new(peer.instance_id.clone(), cluster_id.clone())
                .with_current_grade(peer.target_grade.into());
            tlog!(Info,
                "handling UpdatePeerRequest";
                "current_grade" => %req.current_grade.expect("just set"),
                "instance_id" => %req.instance_id,
            );
            if let Err(e) = node.handle_topology_request_and_wait(req.into()) {
                tlog!(Warning,
                    "failed handling UpdatePeerRequest: {e}";
                    "instance_id" => %peer.instance_id,
                );
                // TODO: don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                continue 'governor;
            }

            let replicaset_peers = storage
                .peers
                .replicaset_peers(replicaset_id)
                .expect("storage error")
                .filter(|peer| !peer.is_expelled())
                .collect::<Vec<_>>();
            let may_respond = replicaset_peers.iter().filter(|peer| peer.may_respond());
            // Check if it makes sense to call box.ctl.promote,
            // otherwise we risk unpredictable delays
            if replicaset_peers.len() / 2 + 1 > may_respond.count() {
                tlog!(Warning,
                    "replicaset lost quorum";
                    "replicaset_id" => %replicaset_id,
                );
                continue 'governor;
            }

            let res = (|| -> traft::Result<_> {
                // Promote the replication leader again
                // because of tarantool bugs
                if let Some(replicaset) = storage.replicasets.get(replicaset_id)? {
                    tlog!(Info,
                        "calling rpc::replication::promote";
                        "instance_id" => %replicaset.master_id
                    );
                    let commit = storage.raft.commit()?.unwrap();
                    pool.call_and_wait_timeout(
                        &replicaset.master_id,
                        replication::promote::Request {
                            term,
                            commit,
                            timeout: SYNC_TIMEOUT,
                        },
                        // TODO: don't hard code timeout
                        Duration::from_secs(3),
                    )?;
                }
                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning,
                    "failed calling rpc::replication::promote: {e}";
                    "replicaset_id" => %replicaset_id,
                );
            }

            continue 'governor;
        }

        ////////////////////////////////////////////////////////////////////////
        // raft sync
        // TODO: putting each stage in a different function
        // will make the control flow more readable
        let to_sync = peers.iter().find(|peer| {
            peer.has_grades(CurrentGradeVariant::Offline, TargetGradeVariant::Online)
                || peer.is_reincarnated()
        });
        if let Some(peer) = to_sync {
            let (rx, tx) = fiber::Channel::new(1).into_clones();
            let commit = storage.raft.commit().unwrap().unwrap();
            pool.call(
                &peer.raft_id,
                sync::Request {
                    commit,
                    timeout: SYNC_TIMEOUT,
                },
                move |res| tx.send(res).expect("mustn't fail"),
            )
            .expect("shouldn't fail");
            let res = rx.recv().expect("ought not fail");
            let res = res.and_then(|sync::Response { commit }| {
                // TODO: change `Info` to `Debug`
                tlog!(Info, "peer synced";
                    "commit" => commit,
                    "instance_id" => &*peer.instance_id,
                );

                let req = UpdatePeerRequest::new(peer.instance_id.clone(), cluster_id)
                    .with_current_grade(CurrentGrade::raft_synced(peer.target_grade.incarnation));
                global()
                    .expect("can't be deinitialized")
                    .handle_topology_request_and_wait(req.into())
            });
            match res {
                Ok(peer) => {
                    tlog!(Info, "raft sync processed");
                    debug_assert!(peer.current_grade == CurrentGradeVariant::RaftSynced);
                }
                Err(e) => {
                    tlog!(Warning, "raft sync failed: {e}";
                        "instance_id" => %peer.instance_id,
                        "address" => %peer.peer_address,
                    );

                    // TODO: don't hard code timeout
                    event::wait_timeout(Event::TopologyChanged, Duration::from_millis(300))
                        .unwrap();
                }
            }

            continue 'governor;
        }

        ////////////////////////////////////////////////////////////////////////
        // replication
        let to_replicate = peers
            .iter()
            // TODO: find all such peers in a given replicaset,
            // not just the first one
            .find(|peer| {
                peer.has_grades(CurrentGradeVariant::RaftSynced, TargetGradeVariant::Online)
            });
        if let Some(peer) = to_replicate {
            let replicaset_id = &peer.replicaset_id;
            let replicaset_iids = maybe_responding(&peers)
                .filter(|peer| peer.replicaset_id == replicaset_id)
                .map(|peer| peer.instance_id.clone())
                .collect::<Vec<_>>();

            let res = (|| -> traft::Result<_> {
                let commit = storage.raft.commit()?.unwrap();
                let reqs = replicaset_iids
                    .iter()
                    .cloned()
                    .zip(repeat(replication::Request {
                        term,
                        commit,
                        timeout: SYNC_TIMEOUT,
                        replicaset_instances: replicaset_iids.clone(),
                        replicaset_id: replicaset_id.clone(),
                    }));
                // TODO: don't hard code timeout
                let res = call_all(&mut pool, reqs, Duration::from_secs(3))?;

                for (peer_iid, resp) in res {
                    let replication::Response { lsn } = resp?;
                    // TODO: change `Info` to `Debug`
                    tlog!(Info, "configured replication with peer";
                        "instance_id" => &*peer_iid,
                        "lsn" => lsn,
                    );
                }

                let req = UpdatePeerRequest::new(peer.instance_id.clone(), cluster_id)
                    .with_current_grade(CurrentGrade::replicated(peer.target_grade.incarnation));
                node.handle_topology_request_and_wait(req.into())?;

                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning, "failed to configure replication: {e}");
                // TODO: don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                continue 'governor;
            }

            let res = (|| -> Result<_, Error> {
                let master_id =
                    if let Some(replicaset) = storage.replicasets.get(&peer.replicaset_id)? {
                        Cow::Owned(replicaset.master_id)
                    } else {
                        let vshard_bootstrapped = storage.state.vshard_bootstrapped()?;
                        let req = traft::OpDML::insert(
                            ClusterSpace::Replicasets,
                            &traft::Replicaset {
                                replicaset_id: peer.replicaset_id.clone(),
                                replicaset_uuid: peer.replicaset_uuid.clone(),
                                master_id: peer.instance_id.clone(),
                                weight: if vshard_bootstrapped { 0. } else { 1. },
                                current_schema_version: 0,
                            },
                        )?;
                        // TODO: don't hard code the timeout
                        node.propose_and_wait(req, Duration::from_secs(3))??;
                        Cow::Borrowed(&peer.instance_id)
                    };

                let commit = storage.raft.commit()?.unwrap();
                pool.call_and_wait_timeout(
                    &*master_id,
                    replication::promote::Request {
                        term,
                        commit,
                        timeout: SYNC_TIMEOUT,
                    },
                    // TODO: don't hard code timeout
                    Duration::from_secs(3),
                )?;
                tlog!(Debug, "promoted replicaset master";
                    "instance_id" => %master_id,
                    "replicaset_id" => %peer.replicaset_id,
                );
                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning, "failed to promote replicaset master: {e}";
                    "replicaset_id" => %replicaset_id,
                );
            }

            tlog!(Info, "configured replication"; "replicaset_id" => %replicaset_id);

            continue 'governor;
        }

        ////////////////////////////////////////////////////////////////////////
        // init sharding
        let to_shard = peers.iter().find(|peer| {
            peer.has_grades(CurrentGradeVariant::Replicated, TargetGradeVariant::Online)
        });
        if let Some(peer) = to_shard {
            let res = (|| -> traft::Result<()> {
                let vshard_bootstrapped = storage.state.vshard_bootstrapped()?;
                let commit = storage.raft.commit()?.unwrap();
                let reqs = maybe_responding(&peers).map(|peer| {
                    (
                        peer.instance_id.clone(),
                        sharding::Request {
                            term,
                            commit,
                            timeout: SYNC_TIMEOUT,
                            bootstrap: !vshard_bootstrapped && peer.raft_id == node.raft_id,
                        },
                    )
                });
                // TODO: don't hard code timeout
                let res = call_all(&mut pool, reqs, Duration::from_secs(3))?;

                for (peer_iid, resp) in res {
                    let sharding::Response {} = resp?;

                    // TODO: change `Info` to `Debug`
                    tlog!(Info, "initialized sharding with peer";
                        "instance_id" => &*peer_iid,
                    );
                }

                let req = UpdatePeerRequest::new(peer.instance_id.clone(), cluster_id)
                    .with_current_grade(CurrentGrade::sharding_initialized(
                        peer.target_grade.incarnation,
                    ));
                node.handle_topology_request_and_wait(req.into())?;

                if !vshard_bootstrapped {
                    // TODO: if this fails, it will only rerun next time vshard
                    // gets reconfigured
                    node.propose_and_wait(
                        traft::OpDML::replace(
                            ClusterSpace::State,
                            &(StateKey::VshardBootstrapped, true),
                        )?,
                        // TODO: don't hard code the timeout
                        Duration::from_secs(3),
                    )??;
                }

                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning, "failed to initialize sharding: {e}");
                // TODO: don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                continue 'governor;
            }

            let res = (|| -> Result<(), Error> {
                // Promote the replication leaders again
                // because of tarantool bugs
                let replicasets = storage.replicasets.iter()?;
                let masters = replicasets.map(|r| r.master_id).collect::<HashSet<_>>();
                let commit = storage.raft.commit()?.unwrap();
                let reqs = maybe_responding(&peers)
                    .filter(|peer| masters.contains(&peer.instance_id))
                    .map(|peer| peer.instance_id.clone())
                    .zip(repeat(replication::promote::Request {
                        term,
                        commit,
                        timeout: SYNC_TIMEOUT,
                    }));
                // TODO: don't hard code timeout
                let res = call_all(&mut pool, reqs, Duration::from_secs(3))?;
                for (peer_iid, resp) in res {
                    resp?;
                    tlog!(Debug, "promoted replicaset master"; "instance_id" => %peer_iid);
                }
                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning, "failed to promote replicaset masters: {e}");
            }

            tlog!(Info, "sharding is initialized");

            continue 'governor;
        }

        ////////////////////////////////////////////////////////////////////////
        // sharding weights
        let to_update_weights = peers.iter().find(|peer| {
            peer.has_grades(
                CurrentGradeVariant::ShardingInitialized,
                TargetGradeVariant::Online,
            )
        });
        if let Some(peer) = to_update_weights {
            let res = if let Some(added_weights) =
                get_weight_changes(maybe_responding(&peers), &storage)
            {
                (|| -> traft::Result<()> {
                    for (replicaset_id, weight) in added_weights {
                        let mut ops = UpdateOps::new();
                        ops.assign("weight", weight)?;
                        node.propose_and_wait(
                            traft::OpDML::update(ClusterSpace::Replicasets, &[replicaset_id], ops)?,
                            // TODO: don't hard code the timeout
                            Duration::from_secs(3),
                        )??;
                    }

                    let peer_ids = maybe_responding(&peers).map(|peer| peer.instance_id.clone());
                    let commit = storage.raft.commit()?.unwrap();
                    let reqs = peer_ids.zip(repeat(sharding::Request {
                        term,
                        commit,
                        timeout: SYNC_TIMEOUT,
                        bootstrap: false,
                    }));
                    // TODO: don't hard code timeout
                    let res = call_all(&mut pool, reqs, Duration::from_secs(3))?;

                    for (peer_iid, resp) in res {
                        resp?;
                        // TODO: change `Info` to `Debug`
                        tlog!(Info, "peer is online"; "instance_id" => &*peer_iid);
                    }

                    let req = UpdatePeerRequest::new(peer.instance_id.clone(), cluster_id)
                        .with_current_grade(CurrentGrade::online(peer.target_grade.incarnation));
                    node.handle_topology_request_and_wait(req.into())?;
                    Ok(())
                })()
            } else {
                (|| -> traft::Result<()> {
                    let to_online = peers.iter().filter(|peer| {
                        peer.has_grades(
                            CurrentGradeVariant::ShardingInitialized,
                            TargetGradeVariant::Online,
                        )
                    });
                    for Peer {
                        instance_id,
                        target_grade,
                        ..
                    } in to_online
                    {
                        let cluster_id = cluster_id.clone();
                        let req = UpdatePeerRequest::new(instance_id.clone(), cluster_id)
                            .with_current_grade(CurrentGrade::online(target_grade.incarnation));
                        node.handle_topology_request_and_wait(req.into())?;
                        // TODO: change `Info` to `Debug`
                        tlog!(Info, "peer is online"; "instance_id" => &**instance_id);
                    }
                    Ok(())
                })()
            };
            if let Err(e) = res {
                tlog!(Warning, "updating sharding weights failed: {e}");

                // TODO: don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                continue 'governor;
            }

            tlog!(Info, "sharding is configured");

            continue 'governor;
        }

        ////////////////////////////////////////////////////////////////////////
        // applying migrations
        let desired_schema_version = storage.state.desired_schema_version().unwrap();
        let replicasets = storage.replicasets.iter().unwrap().collect::<Vec<_>>();
        let mut migrations = storage.migrations.iter().unwrap().collect::<Vec<_>>();
        let commit = storage.raft.commit().unwrap().unwrap();
        for (mid, rids) in waiting_migrations(&mut migrations, &replicasets, desired_schema_version)
        {
            let migration = storage.migrations.get(mid).unwrap().unwrap();
            for rid in rids {
                let replicaset = storage
                    .replicasets
                    .get(rid.to_string().as_str())
                    .unwrap()
                    .unwrap();
                let peer = storage.peers.get(&replicaset.master_id).unwrap();
                let req = rpc::migration::apply::Request {
                    term,
                    commit,
                    timeout: SYNC_TIMEOUT,
                    migration_id: migration.id,
                };
                let res = pool.call_and_wait(&peer.raft_id, req);
                match res {
                    Ok(_) => {
                        let mut ops = UpdateOps::new();
                        ops.assign("current_schema_version", migration.id).unwrap();
                        let op = OpDML::update(
                            ClusterSpace::Replicasets,
                            &[replicaset.replicaset_id.clone()],
                            ops,
                        )
                        .unwrap();
                        node.propose_and_wait(op, Duration::MAX).unwrap().unwrap();
                        tlog!(
                            Info,
                            "Migration {0} applied to replicaset {1}",
                            migration.id,
                            replicaset.replicaset_id
                        );
                    }
                    Err(e) => {
                        tlog!(
                            Warning,
                            "Could not apply migration {0} to replicaset {1}, error: {2}",
                            migration.id,
                            replicaset.replicaset_id,
                            e
                        );
                        continue 'governor;
                    }
                }
            }
        }
        event::broadcast(Event::MigrateDone);

        event::wait_any(&[Event::TopologyChanged, Event::ClusterStateChanged])
            .expect("Events system must be initialized");
    }

    #[allow(clippy::type_complexity)]
    fn call_all<R, I>(
        pool: &mut ConnectionPool,
        reqs: impl IntoIterator<Item = (I, R)>,
        timeout: Duration,
    ) -> traft::Result<Vec<(I, traft::Result<R::Response>)>>
    where
        R: traft::rpc::Request,
        I: traft::network::PeerId + Clone + std::fmt::Debug + 'static,
    {
        // TODO: this crap is only needed to wait until results of all
        // the calls are ready. There are several ways to rafactor this:
        // - we could use a std-style channel that unblocks the reading end
        //   once all the writing ends have dropped
        //   (fiber::Channel cannot do that for now)
        // - using the std Futures we could use futures::join!
        //
        // Those things aren't implemented yet, so this is what we do
        let reqs = reqs.into_iter().collect::<Vec<_>>();
        if reqs.is_empty() {
            return Ok(vec![]);
        }
        static mut SENT_COUNT: usize = 0;
        unsafe { SENT_COUNT = 0 };
        let (cond_rx, cond_tx) = Rc::new(fiber::Cond::new()).into_clones();
        let peer_count = reqs.len();
        let (rx, tx) = fiber::Channel::new(peer_count as _).into_clones();
        for (id, req) in reqs {
            let tx = tx.clone();
            let cond_tx = cond_tx.clone();
            let id_copy = id.clone();
            pool.call(&id, req, move |res| {
                tx.send((id_copy, res)).expect("mustn't fail");
                unsafe { SENT_COUNT += 1 };
                if unsafe { SENT_COUNT } == peer_count {
                    cond_tx.signal()
                }
            })
            .expect("shouldn't fail");
        }
        // TODO: don't hard code timeout
        if !cond_rx.wait_timeout(timeout) {
            return Err(Error::Timeout);
        }

        Ok(rx.into_iter().take(peer_count).collect())
    }

    #[inline(always)]
    fn get_weight_changes<'p>(
        peers: impl IntoIterator<Item = &'p Peer>,
        storage: &Storage,
    ) -> Option<ReplicasetWeights> {
        let replication_factor = storage.state.replication_factor().expect("storage error");
        let replicaset_weights = storage.replicasets.weights().expect("storage error");
        let mut replicaset_sizes = HashMap::new();
        let mut weight_changes = HashMap::new();
        for peer @ Peer { replicaset_id, .. } in peers {
            if !peer.may_respond() {
                continue;
            }
            let replicaset_size = replicaset_sizes.entry(replicaset_id.clone()).or_insert(0);
            *replicaset_size += 1;
            if *replicaset_size >= replication_factor && replicaset_weights[replicaset_id] == 0. {
                weight_changes.entry(replicaset_id.clone()).or_insert(1.);
            }
        }
        (!weight_changes.is_empty()).then_some(weight_changes)
    }

    #[inline(always)]
    fn maybe_responding(peers: &[Peer]) -> impl Iterator<Item = &Peer> {
        peers.iter().filter(|peer| peer.may_respond())
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

pub fn global() -> traft::Result<&'static Node> {
    // Uninitialized raft node is a regular case. This case may take
    // place while the instance is executing `start_discover()` function.
    // It has already started listening, but the node is only initialized
    // in `postjoin()`.
    unsafe { RAFT_NODE.as_deref() }.ok_or(Error::Uninitialized)
}

#[proc(packed_args)]
fn proc_raft_interact(pbs: Vec<traft::MessagePb>) -> traft::Result<()> {
    crate::tarantool::fiber_name("proc_raft_interact");

    let node = global()?;
    for pb in pbs {
        node.step_and_yield(raft::Message::try_from(pb).map_err(Error::other)?);
    }
    Ok(())
}

#[proc(packed_args)]
fn proc_raft_join(req: JoinRequest) -> traft::Result<JoinResponse> {
    crate::tarantool::fiber_name("proc_raft_join");

    let node = global()?;

    let cluster_id = node
        .storage
        .raft
        .cluster_id()?
        .expect("cluster_id is set on boot");

    if req.cluster_id != cluster_id {
        return Err(Error::ClusterIdMismatch {
            instance_cluster_id: req.cluster_id,
            cluster_cluster_id: cluster_id,
        });
    }

    let peer = node.handle_topology_request_and_wait(req.into())?;
    let box_replication = node
        .storage
        .peers
        .replicaset_peer_addresses(&peer.replicaset_id, Some(peer.commit_index))?;

    // A joined peer needs to communicate with other nodes.
    // Provide it the list of raft voters in response.
    let mut raft_group = vec![];
    for raft_id in node.storage.raft.voters()?.unwrap_or_default().into_iter() {
        match node.storage.peers.get(&raft_id) {
            Err(e) => {
                crate::warn_or_panic!("failed reading peer with id `{}`: {}", raft_id, e);
            }
            Ok(peer) => raft_group.push(peer),
        }
    }

    Ok(JoinResponse {
        peer,
        raft_group,
        box_replication,
    })
}
