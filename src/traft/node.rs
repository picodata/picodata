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
use crate::storage::ToEntryIter as _;
use crate::storage::{Clusterwide, ClusterwideSpace, ClusterwideSpaceIndex, PropertyName};
use crate::stringify_cfunc;
use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::event;
use crate::traft::event::Event;
use crate::traft::notify::{notification, Notifier, Notify};
use crate::traft::op::{Dml, Op, OpResult, PersistInstance};
use crate::traft::rpc::{join, update_instance};
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
use ::tarantool::proc;
use ::tarantool::tlua;
use ::tarantool::transaction::start_transaction;
use protobuf::Message as _;
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::rc::Rc;
use std::time::Duration;
use std::time::Instant;

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

type StorageWatchers = HashMap<ClusterwideSpaceIndex, watch::Sender<()>>;
type StorageChanges = HashSet<ClusterwideSpaceIndex>;

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
    main_loop: MainLoop,
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

    /// **This function yields**
    pub fn wait_for_read_state(&self, timeout: Duration) -> traft::Result<RaftIndex> {
        let notify = self.raw_operation(|node_impl| node_impl.read_state_async())?;
        fiber::block_on(notify.recv_timeout::<RaftIndex>(timeout))
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

    /// Processes the [`join::Request`] request and appends necessary
    /// entries to the raft log (if successful).
    ///
    /// Returns the resulting [`Instance`] when the entry is committed.
    ///
    /// Returns an error if the callee node isn't a raft leader.
    ///
    /// **This function yields**
    pub fn handle_join_request_and_wait(
        &self,
        req: join::Request,
    ) -> traft::Result<(Box<Instance>, HashSet<Address>)> {
        let (notify_addr, notify_instance, replication_addresses) =
            self.raw_operation(|node_impl| node_impl.process_join_request_async(req))?;
        fiber::block_on(async {
            let (addr, instance) = futures::join!(notify_addr.recv_any(), notify_instance.recv());
            addr?;
            instance.map(|i| (Box::new(i), replication_addresses))
        })
    }

    /// Processes the [`update_instance::Request`] request and appends
    /// [`Op::PersistInstance`] entry to the raft log (if successful).
    ///
    /// Returns `Ok(())` when the entry is committed.
    ///
    /// Returns an error if the callee node isn't a raft leader.
    ///
    /// **This function yields**
    pub fn handle_update_instance_request_and_wait(
        &self,
        req: update_instance::Request,
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
    pub fn storage_watcher(&self, index: impl Into<ClusterwideSpaceIndex>) -> watch::Receiver<()> {
        self.storage_watcher_impl(index.into())
    }

    /// A non generic version for optimization.
    fn storage_watcher_impl(&self, index: ClusterwideSpaceIndex) -> watch::Receiver<()> {
        use std::collections::hash_map::Entry;
        let mut watchers = self.watchers.lock();
        match watchers.entry(index) {
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

        let raft_id: RaftId = raft_storage.raft_id().map_err(box_err)?.unwrap();
        let applied: RaftIndex = raft_storage.applied().map_err(box_err)?.unwrap_or(0);
        let lc = {
            let gen = raft_storage.gen().unwrap().unwrap_or(0) + 1;
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

    /// Processes the [`join::Request`] request and appends necessary entries
    /// to the raft log (if successful).
    ///
    /// Returns an error if the callee node isn't a Raft leader.
    ///
    /// **This function doesn't yield**
    pub fn process_join_request_async(
        &mut self,
        req: join::Request,
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
        let op_addr = Dml::replace(ClusterwideSpace::Address, &peer_address).expect("can't fail");
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

    /// Processes the [`update_instance::Request`] request and appends [`Op::PersistInstance`]
    /// entry to the raft log (if successful).
    ///
    /// Returns an error if the callee node isn't a Raft leader.
    ///
    /// **This function doesn't yield**
    pub fn process_update_instance_request_async(
        &mut self,
        req: update_instance::Request,
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
                raft::EntryType::EntryNormal => self.handle_committed_normal_entry(
                    entry,
                    wake_governor,
                    expelled,
                    storage_changes,
                ),
                raft::EntryType::EntryConfChange | raft::EntryType::EntryConfChangeV2 => {
                    self.handle_committed_conf_change(entry)
                }
            }
        }

        if let Some(last_entry) = entries.last() {
            if let Err(e) = self.raft_storage.persist_applied(last_entry.index) {
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
        wake_governor: &mut bool,
        expelled: &mut bool,
        storage_changes: &mut StorageChanges,
    ) {
        assert_eq!(entry.entry_type, raft::EntryType::EntryNormal);
        let lc = entry.lc();
        let index = entry.index;
        let op = entry.into_op().unwrap_or(Op::Nop);

        match &op {
            Op::PersistInstance(PersistInstance(instance)) => {
                *wake_governor = true;
                storage_changes.insert(ClusterwideSpace::Instance.into());
                if has_grades!(instance, Expelled -> *) && instance.raft_id == self.raft_id() {
                    // cannot exit during a transaction
                    *expelled = true;
                }
            }
            Op::Dml(op) => {
                if matches!(
                    op.space(),
                    ClusterwideSpace::Property | ClusterwideSpace::Replicaset
                ) {
                    *wake_governor = true;
                }
                storage_changes.insert(op.index());
            }
            _ => {}
        }

        // apply the operation
        let result = self.apply_op(op).expect("storage error");

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
    }

    fn apply_op(&self, op: Op) -> traft::Result<Box<dyn AnyWithTypeName>> {
        let res = match op {
            Op::Nop => Box::new(()),
            Op::Info { msg } => {
                crate::tlog!(Info, "{msg}");
                ().into_box_dyn_any()
            }
            Op::EvalLua(op) => op.result().into_box_dyn_any(),
            Op::ReturnOne(op) => op.result().into_box_dyn_any(),
            Op::PersistInstance(op) => {
                let instance = op.result();
                self.storage.instances.put(&instance).unwrap();
                instance as Box<dyn AnyWithTypeName>
            }
            Op::Dml(op) => op.result().into_box_dyn_any(),
            Op::DdlPrepare { .. } => todo!(),
            Op::DdlCommit => todo!(),
            Op::DdlAbort => todo!(),
        };
        Ok(res)
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
        let voters_old = self.raft_storage.voters().unwrap().unwrap_or_default();
        if voters_old.contains(raft_id) && !conf_state.voters.contains(raft_id) {
            if is_joint {
                event::broadcast_when(Event::Demoted, Event::JointStateLeave).ok();
            } else {
                event::broadcast(Event::Demoted);
            }
        }

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
        if !snapshot.is_empty() {
            if let Err(e) = start_transaction(|| -> tarantool::Result<()> {
                let meta = snapshot.get_metadata();
                self.raft_storage.handle_snapshot_metadata(meta)?;
                self.storage.apply_snapshot_data(snapshot.get_data())?;

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

        if let Err(e) = start_transaction(|| -> Result<(), TransactionError> {
            // Apply committed entries.
            self.handle_committed_entries(
                ready.committed_entries(),
                wake_governor,
                expelled,
                storage_changes,
            );

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

        if let Err(e) = start_transaction(|| -> Result<(), TransactionError> {
            // Update commit index.
            if let Some(commit) = light_rd.commit_index() {
                self.raft_storage.persist_commit(commit).unwrap();
            }

            // Apply committed entries.
            self.handle_committed_entries(
                light_rd.committed_entries(),
                wake_governor,
                expelled,
                storage_changes,
            );

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

struct MainLoop {
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
