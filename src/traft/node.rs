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
use crate::reachability::InstanceReachabilityManager;
use crate::rpc;
use crate::schema::{Distribution, IndexDef, TableDef};
use crate::sentinel;
use crate::storage::acl;
use crate::storage::ddl_meta_drop_space;
use crate::storage::SnapshotData;
use crate::storage::{ddl_abort_on_master, ddl_meta_space_update_operable};
use crate::storage::{local_schema_version, set_local_schema_version};
use crate::storage::{Clusterwide, ClusterwideTable, PropertyName};
use crate::stringify_cfunc;
use crate::sync;
use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::event;
use crate::traft::event::Event;
use crate::traft::network::WorkerOptions;
use crate::traft::notify::{notification, Notifier, Notify};
use crate::traft::op::{Acl, Ddl, Dml, Op, OpResult};
use crate::traft::ConnectionPool;
use crate::traft::ContextCoercion as _;
use crate::traft::LogicalClock;
use crate::traft::RaftEntryId;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::RaftSpaceAccess;
use crate::traft::RaftTerm;
use crate::unwrap_ok_or;
use crate::unwrap_some_or;
use crate::util::AnyWithTypeName;
use crate::warn_or_panic;

use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::raft::StateRole as RaftStateRole;
use ::raft::StorageError;
use ::raft::INVALID_ID;
use ::tarantool::error::TarantoolError;
use ::tarantool::fiber;
use ::tarantool::fiber::mutex::MutexGuard;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::fiber::r#async::{oneshot, watch};
use ::tarantool::fiber::Mutex;
use ::tarantool::index::FieldType as IFT;
use ::tarantool::index::Part;
use ::tarantool::proc;
use ::tarantool::space::FieldType as SFT;
use ::tarantool::time::Instant;
use ::tarantool::tlua;
use ::tarantool::transaction::transaction;
use ::tarantool::tuple::Decode;
use ::tarantool::vclock::Vclock;
use protobuf::Message as _;

use std::cell::Cell;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::rc::Rc;
use std::time::Duration;
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
    /// Current state of the main loop.
    ///
    /// Set this before yielding from [`NodeImpl::advance`].
    pub main_loop_status: &'static str,
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
    pub(crate) raft_id: RaftId,

    node_impl: Rc<Mutex<NodeImpl>>,
    pub(crate) storage: Clusterwide,
    pub(crate) raft_storage: RaftSpaceAccess,
    pub(crate) main_loop: MainLoop,
    pub(crate) governor_loop: governor::Loop,
    pub(crate) sentinel_loop: sentinel::Loop,
    status: watch::Receiver<Status>,

    /// Should be locked during join and update instance request
    /// to avoid costly cas conflicts during concurrent requests.
    pub instances_update: Mutex<()>,
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
    ///
    /// **This function yields**
    pub fn new(storage: Clusterwide, raft_storage: RaftSpaceAccess) -> Result<Self, RaftError> {
        let opts = WorkerOptions {
            raft_msg_handler: stringify_cfunc!(proc_raft_interact),
            call_timeout: MainLoop::TICK.saturating_mul(4),
            ..Default::default()
        };
        let mut pool = ConnectionPool::new(storage.clone(), opts);
        let instance_reachability = Rc::new(RefCell::new(InstanceReachabilityManager::new(
            storage.clone(),
        )));
        pool.instance_reachability = instance_reachability.clone();
        let pool = Rc::new(pool);

        let node_impl = NodeImpl::new(pool.clone(), storage.clone(), raft_storage.clone())?;

        let raft_id = node_impl.raft_id();
        let status = node_impl.status.subscribe();

        let node_impl = Rc::new(Mutex::new(node_impl));

        let node = Node {
            raft_id,
            main_loop: MainLoop::start(node_impl.clone()), // yields
            governor_loop: governor::Loop::start(
                pool.clone(),
                status.clone(),
                storage.clone(),
                raft_storage.clone(),
            ),
            sentinel_loop: sentinel::Loop::start(
                pool,
                status.clone(),
                storage.clone(),
                raft_storage.clone(),
                instance_reachability,
            ),
            node_impl,
            storage,
            raft_storage,
            status,
            instances_update: Mutex::new(()),
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
        let deadline = fiber::clock().saturating_add(timeout);

        let notify = self.raw_operation(|node_impl| node_impl.read_index_async())?;
        let index: RaftIndex = fiber::block_on(notify.recv_timeout(timeout))?;

        self.wait_index(index, deadline.duration_since(fiber::clock()))
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
        tlog!(Debug, "waiting for applied index {target}");
        let deadline = fiber::clock().saturating_add(timeout);
        loop {
            let current = self.get_index();
            if current >= target {
                tlog!(
                    Debug,
                    "done waiting for applied index {target}, current: {current}"
                );
                return Ok(current);
            }

            if event::wait_deadline(event::Event::EntryApplied, deadline)?.is_timeout() {
                tlog!(
                    Debug,
                    "failed waiting for applied index {target}: timeout, current: {current}"
                );
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
}

pub(crate) struct NodeImpl {
    pub raw_node: RawNode,
    pub notifications: HashMap<LogicalClock, Notifier>,
    joint_state_latch: KVCell<RaftIndex, oneshot::Sender<Result<(), RaftError>>>,
    storage: Clusterwide,
    raft_storage: RaftSpaceAccess,
    pool: Rc<ConnectionPool>,
    lc: LogicalClock,
    status: watch::Sender<Status>,
    instance_reachability: Rc<RefCell<InstanceReachabilityManager>>,
}

impl NodeImpl {
    fn new(
        pool: Rc<ConnectionPool>,
        storage: Clusterwide,
        raft_storage: RaftSpaceAccess,
    ) -> Result<Self, RaftError> {
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

        let cfg = raft::Config {
            id: raft_id,
            applied,
            pre_vote: true,
            ..Default::default()
        };

        let raw_node = RawNode::new(&cfg, raft_storage.clone(), tlog::root())?;

        let (status, _) = watch::channel(Status {
            id: raft_id,
            leader_id: None,
            term: traft::INIT_RAFT_TERM,
            raft_state: RaftState::Follower,
            main_loop_status: "idle",
        });

        Ok(Self {
            raw_node,
            notifications: Default::default(),
            joint_state_latch: KVCell::new(),
            storage,
            raft_storage,
            instance_reachability: pool.instance_reachability.clone(),
            pool,
            lc,
            status,
        })
    }

    fn raft_id(&self) -> RaftId {
        self.raw_node.raft.id
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

        // TODO check it's not a MsgPropose with op::Dml for updating _pico_instance.
        // TODO check it's not a MsgPropose with ConfChange.
        self.raw_node.step(msg)
    }

    pub fn tick(&mut self, n_times: u32) {
        for _ in 0..n_times {
            self.raw_node.tick();
        }
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
            transaction(|| -> tarantool::Result<()> {
                self.main_loop_status("handling committed entries");

                let entry_index = entry.index;
                match entry.entry_type {
                    raft::EntryType::EntryNormal => {
                        apply_entry_result =
                            self.handle_committed_normal_entry(entry, wake_governor, expelled);
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
                SleepAndRetry => {
                    self.main_loop_status("blocked by raft entry");
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
    ) -> ApplyEntryResult {
        assert_eq!(entry.entry_type, raft::EntryType::EntryNormal);
        let lc = entry.lc();
        let index = entry.index;
        let op = entry.into_op().unwrap_or(Op::Nop);
        tlog!(Debug, "applying entry: {op}"; "index" => index);

        match &op {
            Op::Dml(op) => match op.space().try_into() {
                Ok(ClusterwideTable::Property | ClusterwideTable::Replicaset) => {
                    *wake_governor = true;
                }
                Ok(ClusterwideTable::Instance) => {
                    *wake_governor = true;
                    let instance = match op {
                        Dml::Insert { tuple, .. } => Some(tuple),
                        Dml::Replace { tuple, .. } => Some(tuple),
                        Dml::Update { .. } => None,
                        Dml::Delete { .. } => None,
                    };
                    if let Some(new) = instance {
                        let new: Instance = rmp_serde::from_slice(new.as_ref())
                            .expect("should be a valid instance tuple");

                        // Check if we're handling a "new node joined" event:
                        // * Either there's no tuple for this node in the storage;
                        // * Or its raft id has changed, meaning it's no longer the same node.
                        let prev = self.storage.instances.get(&new.instance_id).ok();
                        if prev.as_ref().map(|x| x.raft_id) != Some(new.raft_id) {
                            let instance_id = &new.instance_id;
                            crate::audit!(
                                message: "added a new instance `{instance_id}` to the cluster",
                                title: "create_database",
                                severity: Low,
                            );
                        }

                        if prev.as_ref().map(|x| x.current_grade) != Some(new.current_grade) {
                            let instance_id = &new.instance_id;
                            let grade = &new.current_grade;
                            crate::audit!(
                                message: "current grade of instance `{instance_id}` changed to {grade}",
                                title: "change_current_grade",
                                severity: Medium,
                            );
                        }

                        if prev.as_ref().map(|x| x.target_grade) != Some(new.target_grade) {
                            let instance_id = &new.instance_id;
                            let grade = &new.target_grade;
                            crate::audit!(
                                message: "target grade of instance `{instance_id}` changed to {grade}",
                                title: "change_target_grade",
                                severity: Low,
                            );
                        }

                        if has_grades!(new, Expelled -> *) && new.raft_id == self.raft_id() {
                            // cannot exit during a transaction
                            *expelled = true;
                        }
                    }
                }
                _ => {}
            },
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
            Op::Dml(op) => {
                let res = match &op {
                    Dml::Insert { table, tuple } => self.storage.insert(*table, tuple).map(Some),
                    Dml::Replace { table, tuple } => self.storage.replace(*table, tuple).map(Some),
                    Dml::Update { table, key, ops } => self.storage.update(*table, key, ops),
                    Dml::Delete { table, key } => self.storage.delete(*table, key),
                };
                result = Box::new(res) as _;
            }
            Op::DdlPrepare {
                ddl,
                schema_version,
            } => {
                self.apply_op_ddl_prepare(ddl, schema_version)
                    .expect("storage should not fail");
            }
            Op::DdlCommit => {
                let v_local = local_schema_version().expect("storage should not fail");
                let v_pending = storage_properties
                    .pending_schema_version()
                    .expect("storage should not fail")
                    .expect("granted we don't mess up log compaction, this should not be None");
                let ddl = storage_properties
                    .pending_schema_change()
                    .expect("storage should not fail")
                    .expect("granted we don't mess up log compaction, this should not be None");

                // This instance is catching up to the cluster.
                if v_local < v_pending {
                    if self.is_readonly() {
                        return SleepAndRetry;
                    } else {
                        // Master applies schema change at this point.
                        let res = rpc::ddl_apply::apply_schema_change(
                            &self.storage,
                            &ddl,
                            v_pending,
                            true,
                        );
                        match res {
                            Err(rpc::ddl_apply::Error::Other(err)) => {
                                panic!("storage should not fail, but failed with: {err}")
                            }
                            Err(rpc::ddl_apply::Error::Aborted(reason)) => {
                                tlog!(Warning, "failed applying committed ddl operation: {reason}";
                                    "ddl" => ?ddl,
                                );
                                return SleepAndRetry;
                            }
                            Ok(()) => {}
                        }
                    }
                }

                // Update pico metadata.
                match ddl {
                    Ddl::CreateTable { id, name, .. } => {
                        ddl_meta_space_update_operable(&self.storage, id, true)
                            .expect("storage shouldn't fail");

                        crate::audit!(
                            message: "created table `{name}`",
                            title: "create_table",
                            severity: Medium,
                        );
                    }

                    Ddl::DropTable { id } => {
                        let space_raw = self.storage.tables.get(id);
                        let space = space_raw.ok().flatten().expect("failed to get space");
                        ddl_meta_drop_space(&self.storage, id).expect("storage shouldn't fail");

                        let name = &space.name;
                        crate::audit!(
                            message: "dropped table `{name}`",
                            title: "drop_table",
                            severity: Medium,
                        );
                    }

                    _ => {
                        todo!()
                    }
                }

                storage_properties
                    .delete(PropertyName::PendingSchemaChange)
                    .expect("storage should not fail");
                storage_properties
                    .delete(PropertyName::PendingSchemaVersion)
                    .expect("storage should not fail");
                storage_properties
                    .put(PropertyName::GlobalSchemaVersion, &v_pending)
                    .expect("storage should not fail");
            }
            Op::DdlAbort => {
                let v_local = local_schema_version().expect("storage should not fail");
                let v_pending: u64 = storage_properties
                    .pending_schema_version()
                    .expect("storage should not fail")
                    .expect("granted we don't mess up log compaction, this should not be None");
                let ddl = storage_properties
                    .pending_schema_change()
                    .expect("storage should not fail")
                    .expect("granted we don't mess up log compaction, this should not be None");
                // This condition means, schema versions must always increase
                // even after an DdlAbort
                if v_local == v_pending {
                    if self.is_readonly() {
                        return SleepAndRetry;
                    } else {
                        let v_global = storage_properties
                            .global_schema_version()
                            .expect("storage should not fail");
                        ddl_abort_on_master(&ddl, v_global).expect("storage should not fail");
                    }
                }

                // Update pico metadata.
                match ddl {
                    Ddl::CreateTable { id, .. } => {
                        ddl_meta_drop_space(&self.storage, id).expect("storage shouldn't fail");
                    }

                    Ddl::DropTable { id } => {
                        ddl_meta_space_update_operable(&self.storage, id, true)
                            .expect("storage shouldn't fail");
                    }

                    _ => {
                        todo!()
                    }
                }

                storage_properties
                    .delete(PropertyName::PendingSchemaChange)
                    .expect("storage should not fail");
                storage_properties
                    .delete(PropertyName::PendingSchemaVersion)
                    .expect("storage should not fail");
            }

            Op::Acl(acl) => {
                let v_local = local_schema_version().expect("storage shoudl not fail");
                let v_pending = acl.schema_version();
                if v_local < v_pending {
                    if self.is_readonly() {
                        // Wait for tarantool replication with master to progress.
                        return SleepAndRetry;
                    } else {
                        match &acl {
                            Acl::CreateUser { user_def } => {
                                acl::on_master_create_user(user_def)
                                    .expect("creating user shouldn't fail");
                            }
                            Acl::ChangeAuth { user_id, auth, .. } => {
                                acl::on_master_change_user_auth(*user_id, auth)
                                    .expect("changing user auth shouldn't fail");
                            }
                            Acl::DropUser { user_id, .. } => {
                                acl::on_master_drop_user(*user_id)
                                    .expect("droping user shouldn't fail");
                            }
                            Acl::CreateRole { role_def } => {
                                acl::on_master_create_role(role_def)
                                    .expect("creating role shouldn't fail");
                            }
                            Acl::DropRole { role_id, .. } => {
                                acl::on_master_drop_role(*role_id)
                                    .expect("droping role shouldn't fail");
                            }
                            Acl::GrantPrivilege { priv_def } => {
                                acl::on_master_grant_privilege(priv_def)
                                    .expect("granting a privilege shouldn't fail");
                            }
                            Acl::RevokePrivilege { priv_def } => {
                                acl::on_master_revoke_privilege(priv_def)
                                    .expect("revoking a privilege shouldn't fail");
                            }
                        }
                        set_local_schema_version(v_pending).expect("storage should not fail");
                    }
                }

                match &acl {
                    Acl::CreateUser { user_def } => {
                        acl::global_create_user(&self.storage, user_def)
                            .expect("persisting a user definition shouldn't fail");
                    }
                    Acl::ChangeAuth { user_id, auth, .. } => {
                        acl::global_change_user_auth(&self.storage, *user_id, auth)
                            .expect("changing user definition shouldn't fail");
                    }
                    Acl::DropUser { user_id, .. } => {
                        acl::global_drop_user(&self.storage, *user_id)
                            .expect("droping a user definition shouldn't fail");
                    }
                    Acl::CreateRole { role_def } => {
                        acl::global_create_role(&self.storage, role_def)
                            .expect("persisting a role definition shouldn't fail");
                    }
                    Acl::DropRole { role_id, .. } => {
                        acl::global_drop_role(&self.storage, *role_id)
                            .expect("droping a role definition shouldn't fail");
                    }
                    Acl::GrantPrivilege { priv_def } => {
                        acl::global_grant_privilege(&self.storage, priv_def)
                            .expect("persiting a privilege definition shouldn't fail");
                    }
                    Acl::RevokePrivilege { priv_def } => {
                        acl::global_revoke_privilege(&self.storage, priv_def)
                            .expect("removing a privilege definition shouldn't fail");
                    }
                }

                storage_properties
                    .put(PropertyName::GlobalSchemaVersion, &v_pending)
                    .expect("storage should not fail");
                storage_properties
                    .put(PropertyName::NextSchemaVersion, &(v_pending + 1))
                    .expect("storage should not fail");
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
            Ddl::CreateTable {
                id,
                name,
                mut format,
                mut primary_key,
                distribution,
                engine,
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
                                tlog!(
                                    Warning,
                                    "invalid primary key part: field '{name}' not found"
                                );
                                continue;
                            };
                            // We store all index parts as field indexes.
                            pk_part.field = Num(index);
                            (index, field)
                        }
                    };
                    let Some(field_type) =
                        crate::schema::try_space_field_type_to_index_field_type(field.field_type)
                    else {
                        // Ddl prepare operations should be verified before being proposed,
                        // so this shouldn't ever happen. But ignoring this is safe anyway,
                        // because proc_apply_schema_change will catch the error and ddl will be aborted.
                        tlog!(
                            Warning,
                            "invalid primary key part: field type {} cannot be part of an index",
                            field.field_type
                        );
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
                    table_id: id,
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
                            table_id: id,
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

                let space_def = TableDef {
                    id,
                    name,
                    distribution,
                    schema_version,
                    format,
                    operable: false,
                    engine,
                };
                let res = self.storage.tables.insert(&space_def);
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

            Ddl::DropTable { id } => {
                ddl_meta_space_update_operable(&self.storage, id, false)
                    .expect("storage shouldn't fail");
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
        if messages.is_empty() {
            return;
        }

        self.main_loop_status("sending raft messages");
        let mut sent_count = 0;
        let mut skip_count = 0;

        let instance_reachability = self.instance_reachability.borrow();
        for msg in messages {
            if msg.msg_type == raft::MessageType::MsgHeartbeat
                && !instance_reachability.should_send_heartbeat_this_tick(msg.to)
            {
                skip_count += 1;
                continue;
            }
            sent_count += 1;
            if let Err(e) = self.pool.send(msg) {
                tlog!(Error, "{e}");
            }
        }

        tlog!(
            Debug,
            "done sending messages, sent: {sent_count}, skipped: {skip_count}"
        );
    }

    fn fetch_chunkwise_snapshot(
        &self,
        snapshot_data: &mut SnapshotData,
        entry_id: RaftEntryId,
    ) -> traft::Result<()> {
        #[rustfmt::skip]
        let mut position = snapshot_data.next_chunk_position
            .expect("shouldn't be None if this function is called");
        let space_dumps = &mut snapshot_data.space_dumps;
        #[cfg(debug_assertions)]
        let mut last_space_id = 0;
        #[cfg(debug_assertions)]
        let mut last_space_tuple_count = 0;

        loop {
            self.main_loop_status("receiving snapshot");

            let Some(leader_id) = self.status.get().leader_id else {
                tlog!(
                    Warning,
                    "leader id is unknown while trying to request next snapshot chunk"
                );
                return Err(Error::LeaderUnknown);
            };

            #[cfg(debug_assertions)]
            {
                let last = space_dumps.last().expect("should not be empty");

                if last.space_id != last_space_id {
                    last_space_tuple_count = 0;
                }
                last_space_id = last.space_id;

                let mut tuples = last.tuples.as_ref();
                let count = rmp::decode::read_array_len(&mut tuples)
                    .expect("space dump should contain a msgpack array");
                last_space_tuple_count += count;

                assert_eq!(last_space_id, position.space_id);
                assert_eq!(last_space_tuple_count, position.tuple_offset);
            }
            let req = rpc::snapshot::Request { entry_id, position };

            const SNAPSHOT_CHUNK_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
            tlog!(Debug, "requesting next snapshot chunk";
                "entry_id" => %entry_id,
                "position" => %position,
            );

            let fut = self
                .pool
                .call(&leader_id, &req, SNAPSHOT_CHUNK_REQUEST_TIMEOUT);
            let fut = unwrap_ok_or!(fut,
                Err(e) => {
                    tlog!(Warning, "failed requesting next snapshot chunk: {e}");
                    self.main_loop_status("error when receiving snapshot");
                    fiber::sleep(MainLoop::TICK * 4);
                    continue;
                }
            );

            let resp = fiber::block_on(fut);
            let mut resp = unwrap_ok_or!(resp,
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("read view not available") {
                        tlog!(Warning, "aborting snapshot retrieval: {e}");
                        return Err(e);
                    }

                    tlog!(Warning, "failed requesting next snapshot chunk: {e}");
                    self.main_loop_status("error when receiving snapshot");
                    fiber::sleep(MainLoop::TICK * 4);
                    continue;
                }
            );
            space_dumps.append(&mut resp.snapshot_data.space_dumps);

            position = unwrap_some_or!(resp.snapshot_data.next_chunk_position, {
                tlog!(Debug, "received final snapshot chunk");
                break;
            });
        }

        Ok(())
    }

    #[inline(always)]
    fn main_loop_status(&self, status: &'static str) {
        tlog!(Debug, "main_loop_status = '{status}'");
        self.status
            .send_modify(|s| s.main_loop_status = status)
            .expect("status shouldn't ever be borrowed across yields");
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
    fn advance(&mut self, wake_governor: &mut bool, expelled: &mut bool) {
        // Handle any unreachable nodes from previous iteration.
        let unreachables = self
            .instance_reachability
            .borrow_mut()
            .take_unreachables_to_report();
        for raft_id in unreachables {
            self.raw_node.report_unreachable(raft_id);

            // TODO: remove infos when instances are expelled.
            let Some(pr) = self.raw_node.raft.mut_prs().get_mut(raft_id) else {
                continue;
            };
            // NOTE: Raft-rs will not check if the node should be paused until
            // a new raft entry is appended to the log. This means that once an
            // instance goes silent it will still be bombarded with heartbeats
            // until someone proposes an operation. This is a workaround for
            // that particular case.
            // The istance's state would've only changed if it was not in the
            // Snapshot state, so we have to check for that.
            if pr.state == ::raft::ProgressState::Probe {
                pr.pause();
            }
        }

        // Get the `Ready` with `RawNode::ready` interface.
        if !self.raw_node.has_ready() {
            return;
        }

        let mut ready: raft::Ready = self.raw_node.ready();

        // Apply soft state changes before anything else, so that this info is
        // awailable for other fibers as soon as main loop yields.
        if let Some(ss) = ready.ss() {
            self.status
                .send_modify(|s| {
                    s.leader_id = (ss.leader_id != INVALID_ID).then_some(ss.leader_id);
                    s.raft_state = ss.raft_state.into();
                })
                .expect("status shouldn't ever be borrowed across yields");
        }

        // Send out messages to the other nodes.
        self.handle_messages(ready.take_messages());

        // Handle read states before applying snapshot which may fail.
        self.handle_read_states(ready.read_states());

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

            let v_snapshot = snapshot_data.schema_version;

            loop {
                let v_local = local_schema_version().expect("storage souldn't fail");
                let v_global = self
                    .storage
                    .properties
                    .global_schema_version()
                    .expect("storage shouldn't fail");

                assert!(
                    v_global <= v_local,
                    "global schema version is only ever increased after local"
                );

                assert!(
                    v_global <= v_snapshot,
                    "global schema version updates are distributed via raft"
                );

                if v_local > v_snapshot {
                    tlog!(
                        Warning,
                        "skipping stale snapshot: local schema version: {}, snapshot schema version: {}",
                        v_local,
                        snapshot_data.schema_version,
                    );
                    return None;
                }

                if !self.is_readonly() {
                    // Replicaset leader applies the schema changes directly.
                    return Some(snapshot_data);
                }

                if v_local == v_snapshot {
                    // Replicaset follower has synced schema with the leader,
                    // now global space dumps should be handled.
                    return Some(snapshot_data);
                }

                tlog!(Debug, "v_local: {v_local}, v_snapshot: {v_snapshot}");
                self.main_loop_status("awaiting replication");
                // Replicaset follower needs to sync with leader via tarantool
                // replication.
                let timeout = MainLoop::TICK * 4;
                fiber::sleep(timeout);
            }
        })();

        if let Some(mut snapshot_data) = snapshot_data {
            if snapshot_data.next_chunk_position.is_some() {
                self.main_loop_status("receiving snapshot");
                let entry_id = RaftEntryId {
                    index: snapshot.get_metadata().index,
                    term: snapshot.get_metadata().term,
                };
                if let Err(e) = self.fetch_chunkwise_snapshot(&mut snapshot_data, entry_id) {
                    // Error has been logged.
                    _ = e;
                    tlog!(Warning, "dropping snapshot data");
                    return;
                }
            }

            self.main_loop_status("applying snapshot");

            if let Err(e) = transaction(|| -> traft::Result<()> {
                let meta = snapshot.get_metadata();
                self.raft_storage.handle_snapshot_metadata(meta)?;
                // FIXME: apply_snapshot_data calls truncate on clusterwide
                // spaces and even though they're all local spaces doing
                // truncate on them is not allowed on read_only instances.
                // Related issue in tarantool:
                // https://github.com/tarantool/tarantool/issues/5616
                let is_readonly = self.is_readonly();
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

        // Apply committed entries.
        let res = self.handle_committed_entries(ready.committed_entries(), wake_governor, expelled);
        if let Err(e) = res {
            tlog!(Warning, "dropping raft ready: {ready:#?}");
            panic!("transaction failed: {e}, {}", TarantoolError::last());
        }

        if let Err(e) = transaction(|| -> Result<(), &str> {
            if !ready.entries().is_empty() || ready.hs().is_some() {
                self.main_loop_status("persisting entries");
            }

            // Persist uncommitted entries in the raft log.
            self.raft_storage.persist_entries(ready.entries()).unwrap();

            // Raft HardState changed, and we need to persist it.
            if let Some(hs) = ready.hs() {
                self.raft_storage.persist_hard_state(hs).unwrap();
                self.status
                    .send_modify(|s| s.term = hs.term)
                    .expect("status shouldn't ever be borrowed across yields");
            }

            Ok(())
        }) {
            tlog!(Warning, "dropping raft ready: {ready:#?}");
            panic!("transaction failed: {e}");
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
        let res =
            self.handle_committed_entries(light_rd.committed_entries(), wake_governor, expelled);
        if let Err(e) = res {
            tlog!(Warning, "dropping raft light ready: {light_rd:#?}");
            panic!("transaction failed: {e}, {}", TarantoolError::last());
        }

        // Advance the apply index.
        self.raw_node.advance_apply();

        self.main_loop_status("idle");
    }

    #[allow(dead_code)]
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
        let master_vclock = fiber::block_on(sync::call_get_vclock(&self.pool, &master.raft_id))?;
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

    /// Check if this is a read only replica. This function is called when we
    /// need to determine if this instance should be changing the schema
    /// definition or if it should instead synchronize with a master.
    ///
    /// Note: it would be a little more reliable to check if the replica is
    /// chosen to be a master by checking master_id in _pico_replicaset, but
    /// currently we cannot do that, because tarantool replication is being
    /// done asynchronously with raft log replication. Basically instance needs
    /// to know it's a replicaset master before it can access the replicaset
    /// info.
    fn is_readonly(&self) -> bool {
        let is_ro: bool = crate::tarantool::eval("return box.info.ro")
            .expect("checking read-onlyness should never fail");
        is_ro
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

/// Return value of [`NodeImpl::handle_committed_normal_entry`], explains what should be
/// done as result of attempting to apply a given entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApplyEntryResult {
    /// This entry failed to apply for some reason, and must be retried later.
    SleepAndRetry,

    /// Entry applied successfully, proceed to next entry.
    EntryApplied,
}

pub(crate) struct MainLoop {
    _loop: Option<fiber::JoinHandle<'static, ()>>,
    loop_waker: watch::Sender<()>,
    stop_flag: Rc<Cell<bool>>,
}

struct MainLoopState {
    node_impl: Rc<Mutex<NodeImpl>>,
    next_tick: Instant,
    loop_waker: watch::Receiver<()>,
    stop_flag: Rc<Cell<bool>>,
}

impl MainLoop {
    pub const TICK: Duration = Duration::from_millis(100);

    fn start(node_impl: Rc<Mutex<NodeImpl>>) -> Self {
        let (loop_waker_tx, loop_waker_rx) = watch::channel(());
        let stop_flag: Rc<Cell<bool>> = Default::default();

        let state = MainLoopState {
            node_impl,
            next_tick: Instant::now(),
            loop_waker: loop_waker_rx,
            stop_flag: stop_flag.clone(),
        };

        Self {
            // implicit yield
            _loop: loop_start!("raft_main_loop", Self::iter_fn, state),
            loop_waker: loop_waker_tx,
            stop_flag,
        }
    }

    pub fn wakeup(&self) {
        let _ = self.loop_waker.send(());
    }

    async fn iter_fn(state: &mut MainLoopState) -> FlowControl {
        let _ = state.loop_waker.changed().timeout(Self::TICK).await;
        if state.stop_flag.take() {
            return FlowControl::Break;
        }

        // FIXME: potential deadlock - can't use sync mutex in async fn
        let mut node_impl = state.node_impl.lock(); // yields
        if state.stop_flag.take() {
            return FlowControl::Break;
        }

        node_impl.cleanup_notifications();

        let now = Instant::now();
        if now > state.next_tick {
            state.next_tick = now.saturating_add(Self::TICK);
            node_impl.raw_node.tick();
        }

        let mut wake_governor = false;
        let mut expelled = false;
        node_impl.advance(&mut wake_governor, &mut expelled); // yields
        drop(node_impl);
        if state.stop_flag.take() {
            return FlowControl::Break;
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
