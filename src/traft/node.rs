//! This module incapsulates most of the application-specific logics.
//!
//! It's responsible for
//! - handling proposals,
//! - handling configuration changes,
//! - processing raft `Ready` - persisting entries, communicating with other raft nodes.

use crate::governor;
use crate::has_grades;
use crate::instance::Instance;
use crate::kvcell::KVCell;
use crate::loop_start;
use crate::r#loop::FlowControl;
use crate::rpc;
use crate::schema::ddl_abort_on_master;
use crate::schema::{Distribution, IndexDef, SpaceDef};
use crate::storage::local_schema_version;
use crate::storage::SnapshotData;
use crate::storage::ToEntryIter as _;
use crate::storage::{Clusterwide, ClusterwideSpaceId, PropertyName};
use crate::stringify_cfunc;
use crate::sync;
use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::event;
use crate::traft::event::Event;
use crate::traft::notify::{notification, Notifier, Notify};
use crate::traft::op::{Ddl, Dml, Op, OpResult, PersistInstance};
use crate::traft::Address;
use crate::traft::ConnectionPool;
use crate::traft::ContextCoercion as _;
use crate::traft::LogicalClock;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::RaftSpaceAccess;
use crate::traft::RaftTerm;
use crate::traft::Topology;
use crate::unwrap_some_or;
use crate::util::instant_saturating_add;
use crate::util::AnyWithTypeName;
use crate::warn_or_panic;
use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::raft::StateRole as RaftStateRole;
use ::raft::StorageError;
use ::raft::INVALID_ID;
use ::tarantool::error::{TarantoolError, TransactionError};
use ::tarantool::fiber;
use ::tarantool::fiber::mutex::MutexGuard;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::fiber::r#async::{oneshot, watch};
use ::tarantool::fiber::Mutex;
use ::tarantool::index::FieldType as IFT;
use ::tarantool::index::Part;
use ::tarantool::proc;
use ::tarantool::space::FieldType as SFT;
use ::tarantool::space::SpaceId;
use ::tarantool::tlua;
use ::tarantool::transaction::start_transaction;
use ::tarantool::tuple::Decode;
use ::tarantool::vclock::Vclock;
use protobuf::Message as _;
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::rc::Rc;
use std::time::Duration;
use std::time::Instant;
use ApplyEntryResult::*;

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

type StorageWatchers = HashMap<SpaceId, watch::Sender<()>>;
type StorageChanges = HashSet<SpaceId>;

/// The heart of `traft` module - the Node.
pub struct Node {
    /// RaftId of the Node.
    //
    // It appears twice in the Node: here and in `status.id`.
    // This is a concious decision.
    // `self.raft_id()` is used in Rust API, and
    // `self.status()` is mostly useful in Lua API.
    pub(crate) raft_id: RaftId,

    node_impl: Rc<Mutex<NodeImpl>>,
    pub(crate) storage: Clusterwide,
    pub(crate) raft_storage: RaftSpaceAccess,
    pub(crate) main_loop: MainLoop,
    pub(crate) governor_loop: governor::Loop,
    status: watch::Receiver<Status>,
    watchers: Rc<Mutex<StorageWatchers>>,
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
    pub fn new(storage: Clusterwide, raft_storage: RaftSpaceAccess) -> Result<Self, RaftError> {
        let node_impl = NodeImpl::new(storage.clone(), raft_storage.clone())?;

        let raft_id = node_impl.raft_id();
        let status = node_impl.status.subscribe();

        let node_impl = Rc::new(Mutex::new(node_impl));
        let watchers = Rc::new(Mutex::new(HashMap::new()));

        let node = Node {
            raft_id,
            main_loop: MainLoop::start(node_impl.clone(), watchers.clone()), // yields
            governor_loop: governor::Loop::start(
                status.clone(),
                storage.clone(),
                raft_storage.clone(),
            ),
            node_impl,
            storage,
            raft_storage,
            status,
            watchers,
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

    pub(crate) fn node_impl(&self) -> MutexGuard<NodeImpl> {
        self.node_impl.lock()
    }

    /// Wait for the status to be changed.
    /// **This function yields**
    pub fn wait_status(&self) {
        fiber::block_on(self.status.clone().changed()).unwrap();
    }

    /// Returns current applied [`RaftIndex`].
    pub fn get_index(&self) -> RaftIndex {
        self.raft_storage
            .applied()
            .expect("reading from memtx should never fail")
    }

    /// Performs the quorum read operation.
    ///
    /// If works the following way:
    ///
    /// 1. The instance forwards a request (`MsgReadIndex`) to a raft
    ///    leader. In case there's no leader at the moment, the function
    ///    returns `Err(ProposalDropped)`.
    /// 2. Raft leader tracks its `commit_index` and broadcasts a
    ///    heartbeat to followers to make certain that it's still a
    ///    leader.
    /// 3. As soon as the heartbeat is acknowlenged by the quorum, the
    ///    function returns that index.
    /// 4. The instance awaits when the index is applied. If timeout
    ///    expires beforehand, the function returns `Err(Timeout)`.
    ///
    /// Returns current applied [`RaftIndex`].
    ///
    /// **This function yields**
    pub fn read_index(&self, timeout: Duration) -> traft::Result<RaftIndex> {
        let deadline = instant_saturating_add(Instant::now(), timeout);

        let notify = self.raw_operation(|node_impl| node_impl.read_index_async())?;
        let index: RaftIndex = fiber::block_on(notify.recv_timeout(timeout))?;

        self.wait_index(index, deadline.saturating_duration_since(Instant::now()))
    }

    /// Waits for [`RaftIndex`] to be applied to the storage locally.
    ///
    /// Returns current applied [`RaftIndex`]. It can be equal to or
    /// greater than the target one. If timeout expires beforehand, the
    /// function returns `Err(Timeout)`.
    ///
    /// **This function yields**
    #[inline]
    pub fn wait_index(&self, target: RaftIndex, timeout: Duration) -> traft::Result<RaftIndex> {
        let deadline = instant_saturating_add(Instant::now(), timeout);
        loop {
            let current = self.get_index();
            if current >= target {
                return Ok(current);
            }

            if let Some(timeout) = deadline.checked_duration_since(Instant::now()) {
                event::wait_timeout(event::Event::EntryApplied, timeout)?;
            } else {
                return Err(Error::Timeout);
            }
        }
    }

    /// Propose an operation and wait for it's result.
    /// **This function yields**
    pub fn propose_and_wait<T: OpResult + Into<Op>>(
        &self,
        op: T,
        timeout: Duration,
    ) -> traft::Result<T::Result> {
        let notify = self.raw_operation(|node_impl| node_impl.propose_async(op))?;
        fiber::block_on(notify.recv_timeout::<T::Result>(timeout))
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

    /// Processes the [`rpc::join::Request`] and appends necessary
    /// entries to the raft log (if successful).
    ///
    /// Returns the resulting [`Instance`] when the entry is committed.
    ///
    /// Returns an error if the callee node isn't a raft leader.
    ///
    /// **This function yields**
    pub fn handle_join_request_and_wait(
        &self,
        req: rpc::join::Request,
    ) -> traft::Result<(Box<Instance>, HashSet<Address>)> {
        let (notify_addr, notify_instance, replication_addresses) =
            self.raw_operation(|node_impl| node_impl.process_join_request_async(req))?;
        fiber::block_on(async {
            let (addr, instance) = futures::join!(notify_addr.recv_any(), notify_instance.recv());
            addr?;
            instance.map(|i| (Box::new(i), replication_addresses))
        })
    }

    /// Processes the [`rpc::update_instance::Request`] and appends
    /// [`Op::PersistInstance`] entry to the raft log (if successful).
    ///
    /// Returns `Ok(())` when the entry is committed.
    ///
    /// Returns an error if the callee node isn't a raft leader.
    ///
    /// **This function yields**
    pub fn handle_update_instance_request_and_wait(
        &self,
        req: rpc::update_instance::Request,
    ) -> traft::Result<()> {
        let notify =
            self.raw_operation(|node_impl| node_impl.process_update_instance_request_async(req))?;
        fiber::block_on(notify.recv_any())?;
        Ok(())
    }

    /// Only the conf_change_loop on a leader is eligible to call this function.
    ///
    /// **This function yields**
    pub(crate) fn propose_conf_change_and_wait(
        &self,
        term: RaftTerm,
        conf_change: raft::ConfChangeV2,
    ) -> traft::Result<()> {
        let notify =
            self.raw_operation(|node_impl| node_impl.propose_conf_change_async(term, conf_change))?;
        fiber::block_on(notify).unwrap()?;
        Ok(())
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
    #[track_caller]
    fn raw_operation<R>(&self, f: impl FnOnce(&mut NodeImpl) -> R) -> R {
        let mut node_impl = self.node_impl.lock();
        let res = f(&mut node_impl);
        drop(node_impl);
        self.main_loop.wakeup();
        res
    }

    #[inline]
    pub fn all_traft_entries(&self) -> ::tarantool::Result<Vec<traft::Entry>> {
        self.raft_storage.all_traft_entries()
    }

    /// Returns a watch which will be notified when a clusterwide space is
    /// modified via the specified `index`.
    ///
    /// You can also pass a [`ClusterwideSpace`] in which case the space's
    /// primary index will be used.
    #[inline(always)]
    pub fn storage_watcher(&self, space: impl Into<SpaceId>) -> watch::Receiver<()> {
        use std::collections::hash_map::Entry;
        let mut watchers = self.watchers.lock();
        match watchers.entry(space.into()) {
            Entry::Vacant(entry) => {
                let (tx, rx) = watch::channel(());
                entry.insert(tx);
                rx
            }
            Entry::Occupied(entry) => entry.get().subscribe(),
        }
    }
}

pub(crate) struct NodeImpl {
    pub raw_node: RawNode,
    pub notifications: HashMap<LogicalClock, Notifier>,
    topology_cache: KVCell<RaftTerm, Topology>,
    joint_state_latch: KVCell<RaftIndex, oneshot::Sender<Result<(), RaftError>>>,
    storage: Clusterwide,
    raft_storage: RaftSpaceAccess,
    pool: ConnectionPool,
    lc: LogicalClock,
    status: watch::Sender<Status>,
}

impl NodeImpl {
    fn new(storage: Clusterwide, raft_storage: RaftSpaceAccess) -> Result<Self, RaftError> {
        let box_err = |e| StorageError::Other(Box::new(e));

        let raft_id: RaftId = raft_storage
            .raft_id()
            .map_err(box_err)?
            .expect("raft_id should be set by the time the node is being initialized");
        let applied: RaftIndex = raft_storage.applied().map_err(box_err)?;
        let lc = {
            let gen = raft_storage.gen().unwrap() + 1;
            raft_storage.persist_gen(gen).unwrap();
            LogicalClock::new(raft_id, gen)
        };

        let pool = ConnectionPool::builder(storage.clone())
            .handler_name(stringify_cfunc!(proc_raft_interact))
            .call_timeout(MainLoop::TICK.saturating_mul(4))
            .build();

        let cfg = raft::Config {
            id: raft_id,
            applied,
            pre_vote: true,
            ..Default::default()
        };

        let raw_node = RawNode::new(&cfg, raft_storage.clone(), &tlog::root())?;

        let (status, _) = watch::channel(Status {
            id: raft_id,
            leader_id: None,
            term: traft::INIT_RAFT_TERM,
            raft_state: RaftState::Follower,
        });

        Ok(Self {
            raw_node,
            notifications: Default::default(),
            topology_cache: KVCell::new(),
            joint_state_latch: KVCell::new(),
            storage,
            raft_storage,
            pool,
            lc,
            status,
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
                let mut instances = vec![];
                for instance @ Instance { raft_id, .. } in self.storage.instances.iter()? {
                    instances.push((instance, self.storage.peer_addresses.try_get(raft_id)?))
                }
                let replication_factor = self
                    .storage
                    .properties
                    .get(PropertyName::ReplicationFactor)?
                    .ok_or_else(|| Error::other("missing replication_factor value in storage"))?;
                Topology::new(instances).with_replication_factor(replication_factor)
            }
        };

        Ok(self.topology_cache.insert(current_term, topology))
    }

    pub fn read_index_async(&mut self) -> Result<Notify, RaftError> {
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

    /// **Doesn't yield**
    #[inline]
    // TODO: rename and document
    pub fn propose_async<T>(&mut self, op: T) -> Result<Notify, RaftError>
    where
        T: Into<Op>,
    {
        let (lc, notify) = self.schedule_notification();
        let ctx = traft::EntryContextNormal::new(lc, op.into());
        self.raw_node.propose(ctx.to_bytes(), vec![])?;
        Ok(notify)
    }

    /// Proposes a raft entry to be appended to the log and returns raft index
    /// at which it is expected to be committed unless it gets rejected.
    ///
    /// **Doesn't yield**
    pub fn propose(&mut self, op: Op) -> Result<RaftIndex, RaftError> {
        self.lc.inc();
        let ctx = traft::EntryContextNormal::new(self.lc, op);
        self.raw_node.propose(ctx.to_bytes(), vec![])?;
        let index = self.raw_node.raft.raft_log.last_index();
        Ok(index)
    }

    pub fn campaign(&mut self) -> Result<(), RaftError> {
        self.raw_node.campaign()
    }

    pub fn step(&mut self, msg: raft::Message) -> Result<(), RaftError> {
        if msg.to != self.raft_id() {
            return Ok(());
        }

        // TODO check it's not a MsgPropose with op::PersistInstance.
        // TODO check it's not a MsgPropose with ConfChange.
        self.raw_node.step(msg)
    }

    pub fn tick(&mut self, n_times: u32) {
        for _ in 0..n_times {
            self.raw_node.tick();
        }
    }

    /// Processes the [`rpc::join::Request`] and appends necessary
    /// entries to the raft log (if successful).
    ///
    /// Returns an error if the callee node isn't a Raft leader.
    ///
    /// **This function doesn't yield**
    pub fn process_join_request_async(
        &mut self,
        req: rpc::join::Request,
    ) -> traft::Result<(Notify, Notify, HashSet<Address>)> {
        let topology = self.topology_mut()?;
        let (instance, address, replication_addresses) = topology
            .join(
                req.instance_id,
                req.replicaset_id,
                req.advertise_address,
                req.failure_domain,
            )
            .map_err(RaftError::ConfChangeError)?;
        let peer_address = traft::PeerAddress {
            raft_id: instance.raft_id,
            address,
        };
        let op_addr = Dml::replace(ClusterwideSpaceId::Address, &peer_address).expect("can't fail");
        let op_instance = PersistInstance::new(instance);
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
        Ok((
            self.propose_async(op_addr)?,
            self.propose_async(op_instance)?,
            replication_addresses,
        ))
    }

    /// Processes the [`rpc::update_instance::Request`] and appends
    /// [`Op::PersistInstance`] entry to the raft log (if successful).
    ///
    /// Returns an error if the callee node isn't a Raft leader.
    ///
    /// **This function doesn't yield**
    pub fn process_update_instance_request_async(
        &mut self,
        req: rpc::update_instance::Request,
    ) -> traft::Result<Notify> {
        let topology = self.topology_mut()?;
        let instance = topology
            .update_instance(req)
            .map_err(RaftError::ConfChangeError)?;
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
        Ok(self.propose_async(PersistInstance::new(instance))?)
    }

    fn propose_conf_change_async(
        &mut self,
        term: RaftTerm,
        conf_change: raft::ConfChangeV2,
    ) -> Result<oneshot::Receiver<Result<(), RaftError>>, RaftError> {
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

        let (tx, rx) = oneshot::channel();
        self.joint_state_latch.insert(last_index, tx);
        event::broadcast(Event::JointStateEnter);

        Ok(rx)
    }

    /// Is called during a transaction
    fn handle_committed_entries(
        &mut self,
        entries: &[raft::Entry],
        wake_governor: &mut bool,
        expelled: &mut bool,
        storage_changes: &mut StorageChanges,
    ) -> traft::Result<()> {
        let mut entries = entries.iter().peekable();

        while let Some(&entry) = entries.peek() {
            let entry = match traft::Entry::try_from(entry) {
                Ok(v) => v,
                Err(e) => {
                    tlog!(Error, "abnormal entry: {e}"; "entry" => ?entry);
                    continue;
                }
            };

            let mut apply_entry_result = EntryApplied;
            start_transaction(|| -> tarantool::Result<()> {
                let entry_index = entry.index;
                match entry.entry_type {
                    raft::EntryType::EntryNormal => {
                        apply_entry_result = self.handle_committed_normal_entry(
                            entry,
                            wake_governor,
                            expelled,
                            storage_changes,
                        );
                        if apply_entry_result != EntryApplied {
                            return Ok(());
                        }
                    }
                    raft::EntryType::EntryConfChange | raft::EntryType::EntryConfChangeV2 => {
                        self.handle_committed_conf_change(entry)
                    }
                }

                let res = self.raft_storage.persist_applied(entry_index);
                event::broadcast(Event::EntryApplied);
                if let Err(e) = res {
                    tlog!(
                        Error,
                        "error persisting applied index: {e}";
                        "index" => entry_index
                    );
                }

                Ok(())
            })?;

            match apply_entry_result {
                WaitVclockAndRetry => {
                    if let Err(e) = self.check_vclock_and_sleep() {
                        let timeout = MainLoop::TICK;
                        tlog!(
                            Warning,
                            "failed syncing with replication master: {e}, retrying in {:?}...",
                            timeout
                        );
                        fiber::sleep(timeout);
                    }
                    continue;
                }
                SleepAndRetry => {
                    let timeout = MainLoop::TICK * 4;
                    fiber::sleep(timeout);
                    continue;
                }
                EntryApplied => {
                    // Actually advance the iterator.
                    let _ = entries.next();
                }
            }
        }

        Ok(())
    }

    /// Is called during a transaction
    fn handle_committed_normal_entry(
        &mut self,
        entry: traft::Entry,
        wake_governor: &mut bool,
        expelled: &mut bool,
        storage_changes: &mut StorageChanges,
    ) -> ApplyEntryResult {
        assert_eq!(entry.entry_type, raft::EntryType::EntryNormal);
        let lc = entry.lc();
        let index = entry.index;
        let op = entry.into_op().unwrap_or(Op::Nop);
        tlog!(Debug, "applying entry: {op}"; "index" => index);

        match &op {
            Op::PersistInstance(PersistInstance(instance)) => {
                *wake_governor = true;
                storage_changes.insert(ClusterwideSpaceId::Instance.into());
                if has_grades!(instance, Expelled -> *) && instance.raft_id == self.raft_id() {
                    // cannot exit during a transaction
                    *expelled = true;
                }
            }
            Op::Dml(op) => {
                let space = op.space();
                if space == ClusterwideSpaceId::Property as SpaceId
                    || space == ClusterwideSpaceId::Replicaset as SpaceId
                {
                    *wake_governor = true;
                }
                storage_changes.insert(space);
            }
            Op::DdlPrepare { .. } => {
                *wake_governor = true;
            }
            _ => {}
        }

        let storage_properties = &self.storage.properties;

        // apply the operation
        let mut result = Box::new(()) as Box<dyn AnyWithTypeName>;
        match op {
            Op::Nop => {}
            Op::PersistInstance(op) => {
                let instance = op.0;
                self.storage.instances.put(&instance).unwrap();
                result = instance as _;
            }
            Op::Dml(op) => {
                let res = match &op {
                    Dml::Insert { space, tuple } => self.storage.insert(*space, tuple).map(Some),
                    Dml::Replace { space, tuple } => self.storage.replace(*space, tuple).map(Some),
                    Dml::Update { space, key, ops } => self.storage.update(*space, key, ops),
                    Dml::Delete { space, key } => self.storage.delete(*space, key),
                };
                result = Box::new(res) as _;
            }
            Op::DdlPrepare {
                ddl,
                schema_version,
            } => {
                self.apply_op_ddl_prepare(ddl, schema_version)
                    .expect("storage error");
            }
            Op::DdlCommit => {
                let v_local = local_schema_version().expect("storage error");
                let pending_version = storage_properties
                    .pending_schema_version()
                    .expect("storage error")
                    .expect("granted we don't mess up log compaction, this should not be None");
                // This instance is catching up to the cluster and must sync
                // with replication master on it's own.
                if v_local < pending_version {
                    let is_master = self.is_replicaset_master().expect("storage_error");
                    if !is_master {
                        return WaitVclockAndRetry;
                    }
                }

                // Update pico metadata.
                let ddl = storage_properties
                    .pending_schema_change()
                    .expect("storage error")
                    .expect("granted we don't mess up log compaction, this should not be None");
                match ddl {
                    Ddl::CreateSpace { id, .. } => {
                        self.storage
                            .spaces
                            .update_operable(id, true)
                            .expect("storage error");
                        self.storage
                            .indexes
                            .update_operable(id, 0, true)
                            .expect("storage error");
                        // For now we just assume that during space creation index with id 1
                        // exists if and only if it is a bucket_id index.
                        let res = self.storage.indexes.update_operable(id, 1, true);
                        // TODO: maybe we should first check if this index
                        // exists or check the space definition if this should
                        // be done, but for now we just ignore the error "no such index"
                        let _ = res;
                    }
                    _ => {
                        todo!()
                    }
                }

                // Update tarantool metadata.
                // This instance is catching up to the cluster and is a
                // replication master, so it must apply the schema change on it's
                // own.
                // FIXME: copy-pasted from above
                if v_local < pending_version {
                    let is_master = self.is_replicaset_master().expect("storage_error");
                    if is_master {
                        let resp = rpc::ddl_apply::apply_schema_change(
                            &self.storage,
                            &ddl,
                            pending_version,
                        )
                        .expect("storage error");
                        match resp {
                            rpc::ddl_apply::Response::Abort { reason } => {
                                tlog!(Warning, "failed applying committed ddl operation: {reason}";
                                    "ddl" => ?ddl,
                                );
                                return SleepAndRetry;
                            }
                            rpc::ddl_apply::Response::Ok => {}
                        }
                    }
                }

                storage_properties
                    .delete(PropertyName::PendingSchemaChange)
                    .expect("storage error");
                storage_properties
                    .delete(PropertyName::PendingSchemaVersion)
                    .expect("storage error");
                storage_properties
                    .put(PropertyName::GlobalSchemaVersion, &pending_version)
                    .expect("storage error");
            }
            Op::DdlAbort => {
                let v_local = local_schema_version().expect("storage error");
                let pending_version: u64 = storage_properties
                    .pending_schema_version()
                    .expect("storage error")
                    .expect("granted we don't mess up log compaction, this should not be None");
                // This condition means, schema versions must always increase
                // even after an DdlAbort
                if v_local == pending_version {
                    let is_master = self.is_replicaset_master().expect("storage_error");
                    if !is_master {
                        return WaitVclockAndRetry;
                    }
                }

                // Update pico metadata.
                let ddl = storage_properties
                    .pending_schema_change()
                    .expect("storage error")
                    .expect("granted we don't mess up log compaction, this should not be None");
                match ddl {
                    Ddl::CreateSpace { id, .. } => {
                        self.storage.indexes.delete(id, 0).expect("storage error");
                        self.storage.spaces.delete(id).expect("storage error");
                    }
                    _ => {
                        todo!()
                    }
                }

                // Update tarantool metadata.
                // FIXME: copy-pasted from above
                if v_local == pending_version {
                    let is_master = self.is_replicaset_master().expect("storage_error");
                    if is_master {
                        let v_global = storage_properties
                            .global_schema_version()
                            .expect("storage error");
                        ddl_abort_on_master(&ddl, v_global).expect("storage error");
                    }
                }

                storage_properties
                    .delete(PropertyName::PendingSchemaChange)
                    .expect("storage error");
                storage_properties
                    .delete(PropertyName::PendingSchemaVersion)
                    .expect("storage error");
            }
        }

        if let Some(lc) = &lc {
            if let Some(notify) = self.notifications.remove(lc) {
                notify.notify_ok_any(result);
            }
        }

        if let Some(notify) = self.joint_state_latch.take_or_keep(&index) {
            // It was expected to be a ConfChange entry, but it's
            // normal. Raft must have overriden it, or there was
            // a re-election.
            let e = RaftError::ConfChangeError("rolled back".into());

            let _ = notify.send(Err(e));
            event::broadcast(Event::JointStateDrop);
        }

        EntryApplied
    }

    fn apply_op_ddl_prepare(&self, ddl: Ddl, schema_version: u64) -> traft::Result<()> {
        debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

        match ddl.clone() {
            Ddl::CreateSpace {
                id,
                name,
                mut format,
                mut primary_key,
                distribution,
            } => {
                use ::tarantool::util::NumOrStr::*;

                let mut last_pk_part_index = 0;
                for pk_part in &mut primary_key {
                    let (index, field) = match &pk_part.field {
                        Num(index) => {
                            if *index as usize >= format.len() {
                                // Ddl prepare operations should be verified before being proposed,
                                // so this shouldn't ever happen. But ignoring this is safe anyway,
                                // because proc_apply_schema_change will catch the error and ddl will be aborted.
                                tlog!(
                                    Warning,
                                    "invalid primary key part: field index {index} is out of bound"
                                );
                                continue;
                            }
                            (*index, &format[*index as usize])
                        }
                        Str(name) => {
                            let field_index = format.iter().zip(0..).find(|(f, _)| f.name == *name);
                            let Some((field, index)) = field_index else {
                                // Ddl prepare operations should be verified before being proposed,
                                // so this shouldn't ever happen. But ignoring this is safe anyway,
                                // because proc_apply_schema_change will catch the error and ddl will be aborted.
                                tlog!(Warning, "invalid primary key part: field '{name}' not found");
                                continue;
                            };
                            // We store all index parts as field indexes.
                            pk_part.field = Num(index);
                            (index, field)
                        }
                    };
                    let Some(field_type) =
                        crate::schema::try_space_field_type_to_index_field_type(field.field_type) else
                    {
                        // Ddl prepare operations should be verified before being proposed,
                        // so this shouldn't ever happen. But ignoring this is safe anyway,
                        // because proc_apply_schema_change will catch the error and ddl will be aborted.
                        tlog!(Warning, "invalid primary key part: field type {} cannot be part of an index", field.field_type);
                        continue;
                    };
                    // We overwrite the one provided in the request because
                    // there's no reason for it to be there, we know the type
                    // right here.
                    pk_part.r#type = Some(field_type);
                    pk_part.is_nullable = Some(field.is_nullable);
                    last_pk_part_index = last_pk_part_index.max(index);
                }

                let primary_key_def = IndexDef {
                    id: 0,
                    name: "primary_key".into(),
                    space_id: id,
                    schema_version,
                    parts: primary_key,
                    operable: false,
                    // TODO: support other cases
                    unique: true,
                    local: true,
                };
                let res = self.storage.indexes.insert(&primary_key_def);
                if let Err(e) = res {
                    // Ignore the error for now, let governor deal with it.
                    tlog!(
                        Warning,
                        "failed creating index '{}': {e}",
                        primary_key_def.name
                    );
                }

                match distribution {
                    Distribution::Global => {
                        // Nothing else is needed
                    }
                    Distribution::ShardedByField { .. } => {
                        todo!()
                    }
                    Distribution::ShardedImplicitly { .. } => {
                        // TODO: if primary key is not the first field or
                        // there's some space between key parts, we want
                        // bucket_id to go closer to the beginning of the tuple,
                        // but this will require to update primary key part
                        // indexes, so somebody should do that at some point.
                        let bucket_id_index = last_pk_part_index + 1;
                        format.insert(bucket_id_index as _, ("bucket_id", SFT::Unsigned).into());

                        let bucket_id_def = IndexDef {
                            id: 1,
                            name: "bucket_id".into(),
                            space_id: id,
                            schema_version,
                            parts: vec![Part::field(bucket_id_index)
                                .field_type(IFT::Unsigned)
                                .is_nullable(false)],
                            operable: false,
                            unique: false,
                            // TODO: support other cases
                            local: true,
                        };
                        let res = self.storage.indexes.insert(&bucket_id_def);
                        if let Err(e) = res {
                            // Ignore the error for now, let governor deal with it.
                            tlog!(
                                Warning,
                                "failed creating index '{}': {e}",
                                bucket_id_def.name
                            );
                        }
                    }
                }

                let space_def = SpaceDef {
                    id,
                    name,
                    distribution,
                    schema_version,
                    format,
                    operable: false,
                };
                let res = self.storage.spaces.insert(&space_def);
                if let Err(e) = res {
                    // Ignore the error for now, let governor deal with it.
                    tlog!(Warning, "failed creating space '{}': {e}", space_def.name);
                }
            }

            Ddl::CreateIndex {
                space_id,
                index_id,
                by_fields,
            } => {
                let _ = (space_id, index_id, by_fields);
                todo!();
            }
            Ddl::DropSpace { id } => {
                let _ = id;
                todo!();
            }
            Ddl::DropIndex { index_id, space_id } => {
                let _ = (index_id, space_id);
                todo!();
            }
        }

        self.storage
            .properties
            .put(PropertyName::PendingSchemaChange, &ddl)?;
        self.storage
            .properties
            .put(PropertyName::PendingSchemaVersion, &schema_version)?;
        self.storage
            .properties
            .put(PropertyName::NextSchemaVersion, &(schema_version + 1))?;

        Ok(())
    }

    /// Is called during a transaction
    fn handle_committed_conf_change(&mut self, entry: traft::Entry) {
        let mut latch_unlock = || {
            if let Some(notify) = self.joint_state_latch.take() {
                let _ = notify.send(Ok(()));
                event::broadcast(Event::JointStateLeave);
            }
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

                self.raw_node.apply_conf_change(&cc).unwrap()
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
                self.raw_node.apply_conf_change(&cc).unwrap()
            }
            _ => unreachable!(),
        };

        self.raft_storage.persist_conf_state(&conf_state).unwrap();
    }

    /// Is called during a transaction
    fn handle_read_states(&mut self, read_states: &[raft::ReadState]) {
        for rs in read_states {
            if rs.request_ctx.is_empty() {
                continue;
            }
            let ctx = crate::unwrap_ok_or!(
                traft::EntryContextNormal::from_bytes(&rs.request_ctx),
                Err(e) => {
                    tlog!(Error, "abnormal read_state: {e}"; "read_state" => ?rs);
                    continue;
                }
            );

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
    fn advance(
        &mut self,
        wake_governor: &mut bool,
        expelled: &mut bool,
        storage_changes: &mut StorageChanges,
    ) {
        // Get the `Ready` with `RawNode::ready` interface.
        if !self.raw_node.has_ready() {
            return;
        }

        let mut ready: raft::Ready = self.raw_node.ready();

        // Send out messages to the other nodes.
        self.handle_messages(ready.take_messages());

        // This is a snapshot, we need to apply the snapshot at first.
        let snapshot = ready.snapshot();
        let snapshot_data = (|| -> Option<SnapshotData> {
            if snapshot.is_empty() {
                return None;
            }
            let snapshot_data = crate::unwrap_ok_or!(
                SnapshotData::decode(snapshot.get_data()),
                Err(e) => {
                    tlog!(Warning, "skipping snapshot, which failed to deserialize: {e}");
                    return None;
                }
            );

            let v_local = local_schema_version().expect("storage error");
            if v_local > snapshot_data.schema_version {
                // Maybe we should at least apply snapshot metadata?
                tlog!(
                    Warning,
                    "skipping stale snapshot: local schema version: {}, snapshot schema version: {}",
                    v_local,
                    snapshot_data.schema_version,
                );
                return None;
            }

            if !self.is_replicaset_master().expect("storage error") {
                loop {
                    let v_local = local_schema_version().expect("storage error");
                    if v_local >= snapshot_data.schema_version {
                        break;
                    }
                    if let Err(e) = self.check_vclock_and_sleep() {
                        let timeout = MainLoop::TICK;
                        tlog!(
                            Warning,
                            "failed syncing with replication master: {e}, retrying in {:?}...",
                            timeout
                        );
                        fiber::sleep(timeout);
                    }
                    continue;
                }
            }

            Some(snapshot_data)
        })();

        if let Some(snapshot_data) = snapshot_data {
            if let Err(e) = start_transaction(|| -> traft::Result<()> {
                let meta = snapshot.get_metadata();
                self.raft_storage.handle_snapshot_metadata(meta)?;
                // FIXME: apply_snapshot_data calls truncate on clusterwide
                // spaces and even though they're all local spaces doing
                // truncate on them is not allowed on read_only instances.
                // Related issue in tarantool:
                // https://github.com/tarantool/tarantool/issues/5616
                let is_readonly: bool = crate::tarantool::eval("return box.info.ro")?;
                if is_readonly {
                    crate::tarantool::eval("box.cfg { read_only = false }")?;
                }
                let res = self
                    .storage
                    .apply_snapshot_data(&snapshot_data, !is_readonly);
                if is_readonly {
                    crate::tarantool::exec("box.cfg { read_only = true }")?;
                }
                #[allow(clippy::let_unit_value)]
                let _ = res?;

                // TODO: As long as the snapshot was sent to us in response to
                // a rejected MsgAppend (which is the only possible case
                // currently), we will send a MsgAppendResponse back which will
                // automatically reset our status from Snapshot to Replicate.
                // But when we implement support for manual snapshot requests,
                // we will have to also implement sending a MsgSnapStatus,
                // to reset out status explicitly to avoid leader ignoring us
                // indefinitely after that point.
                Ok(())
            }) {
                tlog!(Warning, "dropping raft ready: {ready:#?}");
                panic!("transaction failed: {e}, {}", TarantoolError::last());
            }
        }

        if let Some(ss) = ready.ss() {
            if let Err(e) = self.status.send_modify(|s| {
                s.leader_id = (ss.leader_id != INVALID_ID).then_some(ss.leader_id);
                s.raft_state = ss.raft_state.into();
            }) {
                tlog!(Warning, "failed updating node status: {e}";
                    "leader_id" => ss.leader_id,
                    "raft_state" => ?ss.raft_state,
                )
            }
        }

        self.handle_read_states(ready.read_states());

        // Apply committed entries.
        let res = self.handle_committed_entries(
            ready.committed_entries(),
            wake_governor,
            expelled,
            storage_changes,
        );
        if let Err(e) = res {
            tlog!(Warning, "dropping raft ready: {ready:#?}");
            panic!("transaction failed: {e}, {}", TarantoolError::last());
        }

        if let Err(e) = start_transaction(|| -> Result<(), TransactionError> {
            // Persist uncommitted entries in the raft log.
            self.raft_storage.persist_entries(ready.entries()).unwrap();

            // Raft HardState changed, and we need to persist it.
            if let Some(hs) = ready.hs() {
                self.raft_storage.persist_hard_state(hs).unwrap();
                if let Err(e) = self.status.send_modify(|s| s.term = hs.term) {
                    tlog!(Warning, "failed updating current term: {e}"; "term" => hs.term)
                }
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

        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            self.raft_storage.persist_commit(commit).unwrap();
        }

        // Apply committed entries.
        let res = self.handle_committed_entries(
            light_rd.committed_entries(),
            wake_governor,
            expelled,
            storage_changes,
        );
        if let Err(e) = res {
            tlog!(Warning, "dropping raft light ready: {light_rd:#?}");
            panic!("transaction failed: {e}, {}", TarantoolError::last());
        }

        // Advance the apply index.
        self.raw_node.advance_apply();
    }

    fn check_vclock_and_sleep(&mut self) -> traft::Result<()> {
        assert!(self.raw_node.raft.state != RaftStateRole::Leader);
        let my_id = self.raw_node.raft.id;

        let my_instance_info = self.storage.instances.get(&my_id)?;
        let replicaset_id = my_instance_info.replicaset_id;
        let replicaset = self.storage.replicasets.get(&replicaset_id)?;
        let replicaset = replicaset.ok_or_else(|| {
            Error::other(format!("replicaset info for id {replicaset_id} not found"))
        })?;
        if replicaset.master_id == my_instance_info.instance_id {
            return Err(Error::other(
                "check_vclock_and_sleep called on replicaset master",
            ));
        }
        let master = self.storage.instances.get(&replicaset.master_id)?;
        let master_vclock =
            fiber::block_on(sync::call_get_vclock(&mut self.pool, &master.raft_id))?;
        let local_vclock = Vclock::current();
        if matches!(
            local_vclock.partial_cmp(&master_vclock),
            None | Some(Ordering::Less)
        ) {
            tlog!(Info, "blocking raft loop until replication progresses";
                "master_vclock" => ?master_vclock,
                "local_vclock" => ?local_vclock,
            );
            fiber::sleep(MainLoop::TICK * 4);
        }

        Ok(())
    }

    fn is_replicaset_master(&self) -> traft::Result<bool> {
        let my_raft_id = self.raw_node.raft.id;
        let my_instance_info = self.storage.instances.get(&my_raft_id)?;
        let replicaset_id = my_instance_info.replicaset_id;
        let replicaset = self.storage.replicasets.get(&replicaset_id)?;
        let res = if let Some(replicaset) = replicaset {
            my_instance_info.instance_id == replicaset.master_id
        } else {
            // Replicaset wasn't initialized yet, fallback to lua eval
            let is_ro: bool = crate::tarantool::eval("return box.info.ro")?;
            !is_ro
        };
        Ok(res)
    }

    #[inline]
    fn cleanup_notifications(&mut self) {
        self.notifications.retain(|_, notify| !notify.is_closed());
    }

    /// Generates a pair of logical clock and a notification channel.
    /// Logical clock is a unique identifier suitable for tagging
    /// entries in raft log. Notification is broadcasted when the
    /// corresponding entry is committed.
    #[inline]
    fn schedule_notification(&mut self) -> (LogicalClock, Notify) {
        let (tx, rx) = notification();
        let lc = {
            self.lc.inc();
            self.lc
        };
        self.notifications.insert(lc, tx);
        (lc, rx)
    }
}

/// Return value of [`handle_committed_normal_entry`], explains what should be
/// done as result of attempting to apply a given entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApplyEntryResult {
    /// This is a replica and it needs to sync with replicaset master.
    WaitVclockAndRetry,

    /// This entry failed to apply for some reason, and must be retried later.
    SleepAndRetry,

    /// Entry applied successfully, proceed to next entry.
    EntryApplied,
}

pub(crate) struct MainLoop {
    _loop: Option<fiber::UnitJoinHandle<'static>>,
    loop_waker: watch::Sender<()>,
    stop_flag: Rc<Cell<bool>>,
}

struct MainLoopArgs {
    node_impl: Rc<Mutex<NodeImpl>>,
}

struct MainLoopState {
    next_tick: Instant,
    loop_waker: watch::Receiver<()>,
    stop_flag: Rc<Cell<bool>>,
    watchers: Rc<Mutex<StorageWatchers>>,
}

impl MainLoop {
    pub const TICK: Duration = Duration::from_millis(100);

    fn start(node_impl: Rc<Mutex<NodeImpl>>, watchers: Rc<Mutex<StorageWatchers>>) -> Self {
        let (loop_waker_tx, loop_waker_rx) = watch::channel(());
        let stop_flag: Rc<Cell<bool>> = Default::default();

        let args = MainLoopArgs { node_impl };
        let initial_state = MainLoopState {
            next_tick: Instant::now(),
            loop_waker: loop_waker_rx,
            stop_flag: stop_flag.clone(),
            watchers,
        };

        Self {
            // implicit yield
            _loop: loop_start!("raft_main_loop", Self::iter_fn, args, initial_state),
            loop_waker: loop_waker_tx,
            stop_flag,
        }
    }

    pub fn wakeup(&self) {
        let _ = self.loop_waker.send(());
    }

    async fn iter_fn(args: &MainLoopArgs, state: &mut MainLoopState) -> FlowControl {
        let _ = state.loop_waker.changed().timeout(Self::TICK).await;
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
            state.next_tick = instant_saturating_add(now, Self::TICK);
            node_impl.raw_node.tick();
        }

        let mut wake_governor = false;
        let mut expelled = false;
        let mut storage_changes = StorageChanges::new();
        node_impl.advance(&mut wake_governor, &mut expelled, &mut storage_changes); // yields
        drop(node_impl);
        if state.stop_flag.take() {
            return FlowControl::Break;
        }

        {
            // node_impl lock must be dropped before this to avoid deadlocking
            let mut watchers = state.watchers.lock();
            for index in storage_changes {
                if let Some(tx) = watchers.get(&index) {
                    let res = tx.send(());
                    if res.is_err() {
                        watchers.remove(&index);
                    }
                }
            }
        }

        if expelled {
            crate::tarantool::exit(0);
        }

        if wake_governor {
            if let Err(e) = async { global()?.governor_loop.wakeup() }.await {
                tlog!(Warning, "failed waking up governor: {e}");
            }
        }

        FlowControl::Continue
    }
}

impl Drop for MainLoop {
    fn drop(&mut self) {
        self.stop_flag.set(true);
        let _ = self.loop_waker.send(());
        self._loop.take().unwrap().join(); // yields
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
    let node = global()?;
    for pb in pbs {
        node.step_and_yield(raft::Message::try_from(pb).map_err(Error::other)?);
    }
    Ok(())
}
