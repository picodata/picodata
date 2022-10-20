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
use std::cell::Cell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::rc::Rc;
use std::time::Duration;
use std::time::Instant;
use tarantool::space::UpdateOps;

use crate::kvcell::KVCell;
use crate::r#loop::{FlowControl, Loop};
use crate::stringify_cfunc;
use crate::traft::storage::ClusterSpace;
use crate::traft::ContextCoercion as _;
use crate::traft::InstanceId;
use crate::traft::Peer;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
use crate::warn_or_panic;
use crate::{unwrap_ok_or, unwrap_some_or};
use ::tarantool::util::IntoClones as _;
use protobuf::Message as _;
use std::iter::FromIterator as _;

use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::event;
use crate::traft::event::Event;
use crate::traft::failover;
use crate::traft::notify::Notify;
use crate::traft::rpc::sharding::cfg::ReplicasetWeights;
use crate::traft::rpc::{replication, sharding};
use crate::traft::storage::peer_field;
use crate::traft::storage::{State, StateKey};
use crate::traft::ConnectionPool;
use crate::traft::LogicalClock;
use crate::traft::Op;
use crate::traft::Topology;
use crate::traft::TopologyRequest;
use crate::traft::{
    ExpelRequest, ExpelResponse, JoinRequest, JoinResponse, SyncRaftRequest, SyncRaftResponse,
    UpdatePeerRequest,
};
use crate::traft::{RaftSpaceAccess, Storage};

use super::OpResult;
use super::{CurrentGrade, TargetGrade};

type RawNode = raft::RawNode<RaftSpaceAccess>;

crate::define_str_enum! {
    pub enum RaftState {
        Follower = "Follower",
        Candidate = "Candidate",
        Leader = "Leader",
        PreCandidate = "PreCandidate",
    }
    FromStr::Err = UnknownRaftState;
}
#[derive(thiserror::Error, Debug)]
#[error("unknown raft state {0}")]
pub struct UnknownRaftState(pub String);

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
    /// Current raft state
    pub raft_state: RaftState,
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
                .name("raft_conf_change_loop")
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
    pub fn wait_for_read_state(&self, timeout: Duration) -> Result<RaftIndex, Error> {
        let notify = self.raw_operation(|node_impl| node_impl.read_state_async())?;
        notify.recv_timeout::<RaftIndex>(timeout)
    }

    /// Propose an operation and wait for it's result.
    /// **This function yields**
    pub fn propose_and_wait<T: OpResult + Into<traft::Op>>(
        &self,
        op: T,
        timeout: Duration,
    ) -> Result<T::Result, Error> {
        let notify = self.raw_operation(|node_impl| node_impl.propose_async(op))?;
        notify.recv_timeout::<T::Result>(timeout)
    }

    /// Become a candidate and wait for a main loop round so that there's a
    /// chance we become the leader.
    /// **This function yields**
    pub fn campaign_and_yield(&self) -> Result<(), Error> {
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
    ) -> Result<traft::Peer, Error> {
        let notify =
            self.raw_operation(|node_impl| node_impl.process_topology_request_async(req))?;
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
        let notify =
            self.raw_operation(|node_impl| node_impl.propose_conf_change_async(term, conf_change))?;
        notify.recv()
    }

    /// This function **may yield** if `self.node_impl` mutex is acquired.
    #[inline]
    fn raw_operation<R>(&self, f: impl FnOnce(&mut NodeImpl) -> R) -> R {
        let mut node_impl = self.node_impl.lock();
        let res = f(&mut *node_impl);
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
    fn new(mut storage: Storage) -> Result<Self, RaftError> {
        let box_err = |e| StorageError::Other(Box::new(e));

        let raft_id: RaftId = storage.raft.raft_id().map_err(box_err)?.unwrap();
        let applied: RaftIndex = storage.raft.applied().map_err(box_err)?.unwrap_or(0);
        let lc = {
            let gen = storage.raft.gen().unwrap().unwrap_or(0) + 1;
            storage.raft.persist_gen(gen).unwrap();
            LogicalClock::new(raft_id, gen)
        };

        let pool = ConnectionPool::builder(storage.peers.clone())
            .handler_name(stringify_cfunc!(raft_interact))
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
    fn topology_mut(&mut self) -> Result<&mut Topology, RaftError> {
        let box_err = |e| StorageError::Other(Box::new(e));

        if self.raw_node.raft.state != RaftStateRole::Leader {
            let e = RaftError::ConfChangeError("not a leader".into());
            self.topology_cache.take(); // invalidate the cache
            return Err(e);
        }

        let current_term = self.raw_node.raft.term;

        let topology: Topology = unwrap_some_or! {
            self.topology_cache.take_or_drop(&current_term),
            {
                let peers = self.storage.peers.all_peers().map_err(box_err)?;
                let replication_factor = self.storage.state.get(StateKey::ReplicationFactor)
                    .map_err(box_err)?.unwrap();
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
    ) -> Result<Notify, Error> {
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
        let ctx = traft::EntryContextNormal::new(lc, Op::PersistPeer { peer });

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
            if peer.current_grade == CurrentGrade::Expelled && peer.raft_id == self.raft_id() {
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

        start_transaction(|| -> Result<(), TransactionError> {
            // Apply committed entries.
            self.handle_committed_entries(ready.committed_entries(), topology_changed, expelled);

            // Persist uncommitted entries in the raft log.
            self.storage.raft.persist_entries(ready.entries()).unwrap();

            // Raft HardState changed, and we need to persist it.
            if let Some(hs) = ready.hs() {
                self.storage.raft.persist_hard_state(hs).unwrap();
            }

            Ok(())
        })
        .unwrap();

        // This bunch of messages is special. It must be sent only
        // AFTER the HardState, Entries and Snapshot are persisted
        // to the stable storage.
        self.handle_messages(ready.take_persisted_messages());

        // Advance the Raft.
        let mut light_rd = self.raw_node.advance(ready);

        // Send out messages to the other nodes.
        self.handle_messages(light_rd.take_messages());

        start_transaction(|| -> Result<(), TransactionError> {
            // Update commit index.
            if let Some(commit) = light_rd.commit_index() {
                self.storage.raft.persist_commit(commit).unwrap();
            }

            // Apply committed entries.
            self.handle_committed_entries(light_rd.committed_entries(), topology_changed, expelled);

            Ok(())
        })
        .unwrap();

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

fn raft_conf_change_loop(status: Rc<Cell<Status>>, storage: Storage) {
    let mut pool = ConnectionPool::builder(storage.peers.clone())
        .call_timeout(Duration::from_secs(1))
        .connect_timeout(Duration::from_millis(500))
        .inactivity_timeout(Duration::from_secs(60))
        .build();

    'governor: loop {
        if !status.get().raft_state.is_leader() {
            event::wait(Event::StatusChanged).expect("Events system must be initialized");
            continue 'governor;
        }

        let raft_id = status.get().id;
        let peers = storage.peers.all_peers().unwrap();
        let term = storage.raft.term().unwrap().unwrap_or(0);
        let node = global().expect("must be initialized");

        ////////////////////////////////////////////////////////////////////////
        // conf change
        if let Some(conf_change) = raft_conf_change(&storage.raft, &peers) {
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
            continue 'governor;
        }

        ////////////////////////////////////////////////////////////////////////
        // offline
        let to_offline = peers
            .iter()
            .filter(|peer| peer.current_grade != CurrentGrade::Offline)
            .find(|peer| peer.target_grade == TargetGrade::Offline);
        if let Some(peer) = to_offline {
            // TODO: everybody needs to rerun vshard.*.cfg
            let instance_id = peer.instance_id.clone();
            let cluster_id = storage.raft.cluster_id().unwrap().unwrap();
            let req = UpdatePeerRequest::new(instance_id, cluster_id)
                .with_current_grade(CurrentGrade::Offline);
            let res = node.handle_topology_request_and_wait(req.into());
            if let Err(e) = res {
                tlog!(Warning, "failed to set peer offline: {e}";
                    "instance_id" => &*peer.instance_id,
                );
            }
            continue 'governor;
        }

        ////////////////////////////////////////////////////////////////////////
        // raft sync
        // TODO: putting each stage in a different function
        // will make the control flow more readable
        let to_sync = peers
            .iter()
            .find(|peer| peer.has_grades(CurrentGrade::Offline, TargetGrade::Online));
        if let Some(peer) = to_sync {
            let commit = storage.raft.commit().unwrap().unwrap();

            let (rx, tx) = fiber::Channel::new(1).into_clones();
            pool.call(
                &peer.raft_id,
                SyncRaftRequest {
                    commit,
                    timeout: Duration::from_secs(10),
                },
                move |res| tx.send(res).expect("mustn't fail"),
            )
            .expect("shouldn't fail");
            let res = rx.recv().expect("ought not fail");
            let res = res.and_then(|SyncRaftResponse { commit }| {
                // TODO: change `Info` to `Debug`
                tlog!(Info, "peer synced";
                    "commit" => commit,
                    "instance_id" => &*peer.instance_id,
                );
                let cluster_id = storage.raft.cluster_id().unwrap().unwrap();

                let req = UpdatePeerRequest::new(peer.instance_id.clone(), cluster_id)
                    .with_current_grade(CurrentGrade::RaftSynced);
                global()
                    .expect("can't be deinitialized")
                    .handle_topology_request_and_wait(req.into())
            });
            match res {
                Ok(peer) => {
                    tlog!(Info, "raft sync processed");
                    debug_assert!(peer.current_grade == CurrentGrade::RaftSynced);
                }
                Err(e) => {
                    tlog!(Warning, "raft sync failed: {e}";
                        "instance_id" => &*peer.instance_id,
                        "peer" => &peer.peer_address,
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
        // TODO: putting each stage in a different function
        // will make error handling much easier
        let to_replicate = peers
            .iter()
            .find(|peer| peer.has_grades(CurrentGrade::RaftSynced, TargetGrade::Online));
        if let Some(peer) = to_replicate {
            let replicaset_id = &peer.replicaset_id;
            let replicaset_iids = unwrap_ok_or!(
                // TODO: filter out Offline & Expelled peers
                // TODO: use `peers` instead
                storage.peers.replicaset_fields::<peer_field::InstanceId>(replicaset_id),
                Err(e) => {
                    tlog!(Warning, "failed reading replicaset instances: {e}";
                        "replicaset_id" => replicaset_id,
                    );

                    // TODO: don't hard code timeout
                    event::wait_timeout(Event::TopologyChanged, Duration::from_millis(300))
                        .unwrap();
                    continue 'governor;
                }
            );

            let replicaset_size = replicaset_iids.len();
            let res = call_all(
                &mut pool,
                replicaset_iids.clone(),
                replication::Request {
                    replicaset_instances: replicaset_iids,
                    replicaset_id: replicaset_id.clone(),
                    // TODO: what if someone goes offline/expelled?
                    promote: replicaset_size == 1,
                },
                // TODO: don't hard code timeout
                Duration::from_secs(3),
            );
            let res = unwrap_ok_or!(res,
                Err(e) => {
                    tlog!(Warning, "failed to configure replication: {e}");
                    continue 'governor;
                }
            );

            let cluster_id = storage.raft.cluster_id().unwrap().unwrap();
            for (peer_iid, resp) in res {
                let cluster_id = cluster_id.clone();
                let peer_iid_2 = peer_iid.clone();
                let res = resp.and_then(move |replication::Response { lsn }| {
                    let mut req = UpdatePeerRequest::new(peer_iid_2, cluster_id)
                        .with_current_grade(CurrentGrade::Replicated);
                    if replicaset_size == 1 {
                        // TODO: ignore expelled peers
                        // TODO: ignore offline peers
                        req = req.with_is_master(true);
                    }
                    node.handle_topology_request_and_wait(req.into())
                        .map(|_| lsn)
                });
                match res {
                    Ok(lsn) => {
                        // TODO: change `Info` to `Debug`
                        tlog!(Info, "configured replication with peer";
                            "instance_id" => &*peer_iid,
                            "lsn" => lsn,
                        );
                    }
                    Err(e) => {
                        tlog!(Warning, "failed to configure replication: {e}";
                            "instance_id" => &*peer_iid,
                        );

                        // TODO: don't hard code timeout
                        event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1))
                            .unwrap();
                        continue 'governor;
                    }
                }
            }
            let replicaset_weight = storage
                .state
                .replicaset_weight(replicaset_id)
                .expect("storage error");
            if replicaset_weight.is_none() {
                if let Err(e) = (|| -> Result<(), Error> {
                    let vshard_bootstrapped = storage.state.vshard_bootstrapped()?;
                    let weight = if vshard_bootstrapped { 0. } else { 1. };
                    let mut ops = UpdateOps::new();
                    ops.assign(format!("['value']['{replicaset_id}']"), weight)?;
                    let req = traft::OpDML::update(
                        ClusterSpace::State,
                        &[StateKey::ReplicasetWeights],
                        ops,
                    )?;
                    // TODO: don't hard code the timeout
                    node.propose_and_wait(req, Duration::from_secs(3))??;
                    Ok(())
                })() {
                    // TODO: what if all replicas have changed their grade
                    // successfully, but the replicaset_weight failed to set?
                    tlog!(Warning, "failed to set replicaset weight: {e}";
                        "replicaset_id" => replicaset_id,
                    );

                    // TODO: don't hard code timeout
                    event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                    continue 'governor;
                }
            }

            tlog!(Info, "configured replication"; "replicaset_id" => replicaset_id);

            continue 'governor;
        }

        ////////////////////////////////////////////////////////////////////////
        // init sharding
        let need_sharding = peers
            .iter()
            .any(|peer| peer.has_grades(CurrentGrade::Replicated, TargetGrade::Online));
        if need_sharding {
            let res = (|| -> Result<(), Error> {
                // TODO: filter out Offline & Expelled peers
                let peer_ids = peers.iter().map(|peer| peer.instance_id.clone());
                let res = call_all(
                    &mut pool,
                    peer_ids,
                    sharding::Request {
                        leader_id: raft_id,
                        term,
                        weights: None,
                    },
                    // TODO: don't hard code timeout
                    Duration::from_secs(3),
                )?;

                let cluster_id = storage
                    .raft
                    .cluster_id()?
                    .expect("no cluster_id in storage");
                for (peer_iid, resp) in res {
                    let sharding::Response {} = resp?;
                    let req = UpdatePeerRequest::new(peer_iid.clone(), cluster_id.clone())
                        .with_current_grade(CurrentGrade::ShardingInitialized);
                    node.handle_topology_request_and_wait(req.into())?;

                    // TODO: change `Info` to `Debug`
                    tlog!(Info, "initialized sharding with peer";
                        "instance_id" => &*peer_iid,
                    );
                }
                Ok(())
            })();
            if let Err(e) = res {
                tlog!(Warning, "failed to initialize sharding: {e}");
                // TODO: don't hard code timeout
                event::wait_timeout(Event::TopologyChanged, Duration::from_secs(1)).unwrap();
                continue 'governor;
            }

            tlog!(Info, "sharding is initialized");

            continue 'governor;
        }

        ////////////////////////////////////////////////////////////////////////
        // sharding weights
        let maybe_need_weights_update = peers
            .iter()
            .any(|peer| peer.has_grades(CurrentGrade::ShardingInitialized, TargetGrade::Online));
        if maybe_need_weights_update {
            let res = if let Some(new_weights) = get_new_weights(&peers, &storage.state) {
                (|| -> Result<(), Error> {
                    let res = call_all(
                        &mut pool,
                        peers.iter().map(|peer| peer.instance_id.clone()),
                        sharding::Request {
                            leader_id: raft_id,
                            term,
                            weights: Some(new_weights.clone()),
                        },
                        // TODO: don't hard code timeout
                        Duration::from_secs(3),
                    )?;

                    let cluster_id = storage.raft.cluster_id()?.unwrap();
                    for (peer_iid, resp) in res {
                        let cluster_id = cluster_id.clone();
                        let peer_iid_2 = peer_iid.clone();
                        resp.and_then(move |sharding::Response {}| {
                            let req = UpdatePeerRequest::new(peer_iid_2, cluster_id)
                                .with_current_grade(CurrentGrade::Online);
                            node.handle_topology_request_and_wait(req.into())
                        })?;
                        // TODO: change `Info` to `Debug`
                        tlog!(Info, "peer is online"; "instance_id" => &*peer_iid);
                    }

                    node.propose_and_wait(
                        // TODO: OpDML::update with just the changes
                        traft::OpDML::replace(
                            ClusterSpace::State,
                            &(StateKey::ReplicasetWeights, new_weights),
                        )?,
                        // TODO: don't hard code the timeout
                        Duration::from_secs(3),
                    )??;
                    Ok(())
                })()
            } else {
                (|| -> Result<(), Error> {
                    let cluster_id = storage.raft.cluster_id()?.unwrap();
                    let to_online = peers.iter().filter(|peer| {
                        peer.has_grades(CurrentGrade::ShardingInitialized, TargetGrade::Online)
                    });
                    for Peer { instance_id, .. } in to_online {
                        let cluster_id = cluster_id.clone();
                        let req = UpdatePeerRequest::new(instance_id.clone(), cluster_id)
                            .with_current_grade(CurrentGrade::Online);
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

        event::wait(Event::TopologyChanged).expect("Events system must be initialized");
    }

    #[allow(clippy::type_complexity)]
    fn call_all<R, I>(
        pool: &mut ConnectionPool,
        ids: impl IntoIterator<Item = I>,
        req: R,
        timeout: Duration,
    ) -> Result<Vec<(I, Result<R::Response, Error>)>, Error>
    where
        R: traft::rpc::Request + Clone,
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
        let ids = ids.into_iter().collect::<Vec<_>>();
        static mut SENT_COUNT: usize = 0;
        unsafe { SENT_COUNT = 0 };
        let (cond_rx, cond_tx) = Rc::new(fiber::Cond::new()).into_clones();
        let peer_count = ids.len();
        let (rx, tx) = fiber::Channel::new(peer_count as _).into_clones();
        for id in &ids {
            let tx = tx.clone();
            let cond_tx = cond_tx.clone();
            let id_copy = id.clone();
            pool.call(id, req.clone(), move |res| {
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

    fn get_new_weights(peers: &[Peer], state: &State) -> Option<ReplicasetWeights> {
        let replication_factor = state.replication_factor().expect("storage error");
        let mut replicaset_weights = state.replicaset_weights().expect("storage error");
        let mut replicaset_sizes = HashMap::new();
        let mut weights_changed = false;
        for Peer { replicaset_id, .. } in peers {
            let replicaset_size = replicaset_sizes.entry(replicaset_id.clone()).or_insert(0);
            *replicaset_size += 1;
            if *replicaset_size >= replication_factor && replicaset_weights[replicaset_id] == 0. {
                weights_changed = true;
                *replicaset_weights.get_mut(replicaset_id).unwrap() = 1.;
            }
        }
        weights_changed.then_some(replicaset_weights)
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
        .raft
        .cluster_id()?
        .ok_or("cluster_id is not set yet")?;

    if req.cluster_id != cluster_id {
        return Err(Box::new(Error::ClusterIdMismatch {
            instance_cluster_id: req.cluster_id,
            cluster_cluster_id: cluster_id,
        }));
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

// Lua API entrypoint, run on any node.
pub fn expel_wrapper(instance_id: InstanceId) -> Result<(), traft::error::Error> {
    match expel_by_instance_id(instance_id) {
        Ok(ExpelResponse {}) => Ok(()),
        Err(e) => Err(traft::error::Error::Other(e)),
    }
}

fn expel_by_instance_id(
    instance_id: InstanceId,
) -> Result<ExpelResponse, Box<dyn std::error::Error>> {
    let cluster_id = global()?
        .storage
        .raft
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
    let leader = node.storage.peers.get(&leader_id).unwrap();
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
        .raft
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

    let req2 = UpdatePeerRequest::new(req.instance_id, req.cluster_id)
        .with_current_grade(CurrentGrade::Expelled);
    node.handle_topology_request_and_wait(req2.into())?;

    Ok(ExpelResponse {})
}

// NetBox entrypoint. Run on any node.
#[proc(packed_args)]
fn raft_sync_raft(req: SyncRaftRequest) -> Result<SyncRaftResponse, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + req.timeout;
    loop {
        let commit = global()?.storage.raft.commit().unwrap().unwrap();
        if commit >= req.commit {
            return Ok(SyncRaftResponse { commit });
        }

        let now = Instant::now();
        if now > deadline {
            return Err(Box::new(Error::Timeout));
        }

        event::wait_timeout(Event::RaftEntryApplied, deadline - now)?;
    }
}
