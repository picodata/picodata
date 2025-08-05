//! This module incapsulates most of the application-specific logics.
//!
//! It's responsible for
//! - handling proposals,
//! - handling configuration changes,
//! - processing raft `Ready` - persisting entries, communicating with other raft nodes.

use crate::access_control::user_by_id;
use crate::access_control::UserMetadata;
use crate::catalog;
use crate::catalog::governor_queue::GovernorOpStatus;
use crate::config::apply_parameter;
use crate::config::AlterSystemParametersRef;
use crate::config::PicodataConfig;
use crate::error_code::ErrorCode;
use crate::governor;
use crate::has_states;
use crate::instance::Instance;
use crate::kvcell::KVCell;
use crate::loop_start;
use crate::metrics;
use crate::plugin::manager::PluginManager;
use crate::plugin::migration;
use crate::plugin::PluginAsyncEvent;
use crate::proc_name;
use crate::reachability::instance_reachability_manager;
use crate::reachability::InstanceReachabilityManagerRef;
use crate::rpc;
use crate::rpc::snapshot::proc_raft_snapshot_next_chunk;
use crate::schema::system_table_definitions;
use crate::schema::RoutineDef;
use crate::schema::RoutineKind;
use crate::schema::SchemaObjectType;
use crate::schema::{Distribution, IndexDef, IndexOption, TableDef};
use crate::sentinel;
use crate::static_ref;
use crate::storage::cached_key_def;
use crate::storage::schema::acl;
use crate::storage::schema::ddl_abort_on_master;
use crate::storage::schema::ddl_meta_drop_routine;
use crate::storage::schema::ddl_meta_drop_space;
use crate::storage::schema::ddl_meta_space_update_operable;
use crate::storage::schema::ddl_truncate_space_on_master;
use crate::storage::snapshot::RaftSnapshot;
use crate::storage::snapshot::SnapshotData;
use crate::storage::space_by_id;
use crate::storage::DbConfig;
use crate::storage::ToEntryIter;
use crate::storage::{self, Catalog, PropertyName, SystemTable};
use crate::storage::{local_schema_version, set_local_schema_version};
use crate::tlog;
use crate::topology_cache::TopologyCache;
use crate::traft;
use crate::traft::error::box_error_eq;
use crate::traft::error::to_error_other;
use crate::traft::error::with_modified_message;
use crate::traft::error::Error;
use crate::traft::network::WorkerOptions;
use crate::traft::op::PluginRaftOp;
use crate::traft::op::{Acl, Ddl, Dml, Op};
use crate::traft::Flags;
use crate::traft::LogicalClock;
use crate::traft::RaftEntryId;
use crate::traft::RaftId;
use crate::traft::RaftIndex;
use crate::traft::RaftMessageExt;
use crate::traft::RaftSpaceAccess;
use crate::traft::RaftTerm;
use crate::traft::{ConnectionPool, RaftMessageFlags};
use crate::unwrap_ok_or;
use crate::unwrap_some_or;
use crate::util::NoYieldsRefCell;
use crate::warn_or_panic;
use ::raft::prelude as raft;
use ::raft::storage::Storage as _;
use ::raft::Error as RaftError;
use ::raft::ProgressState;
use ::raft::StateRole as RaftStateRole;
use ::raft::INVALID_ID;
use ::tarantool::error::BoxError;
use ::tarantool::error::TarantoolErrorCode;
use ::tarantool::fiber;
use ::tarantool::fiber::mutex::MutexGuard;
use ::tarantool::fiber::r#async::timeout::Error as TimeoutError;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::fiber::r#async::{oneshot, watch};
use ::tarantool::fiber::Mutex;
use ::tarantool::index::IndexType;
use ::tarantool::proc;
use ::tarantool::space::FieldType as SFT;
use ::tarantool::space::SpaceId;
use ::tarantool::time::Instant;
use ::tarantool::tlua;
use ::tarantool::transaction::transaction;
use ::tarantool::tuple::RawByteBuf;
use ::tarantool::tuple::{Decode, Tuple};
use picodata_plugin::util::DisplayErrorLocation;
use protobuf::Message as _;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::time::Duration;
use tarantool::error::IntoBoxError;
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
    pub(crate) storage: Catalog,
    pub(crate) topology_cache: Rc<TopologyCache>,
    pub(crate) raft_storage: RaftSpaceAccess,
    pub(crate) main_loop: MainLoop,
    pub(crate) governor_loop: governor::Loop,
    pub(crate) sentinel_loop: sentinel::Loop,
    #[allow(unused)]
    pub(crate) pool: Rc<ConnectionPool>,
    status: watch::Receiver<Status>,
    applied: watch::Receiver<RaftIndex>,

    pub(crate) main_loop_info: Rc<NoYieldsRefCell<MainLoopInfo>>,

    /// Should be locked during join and update instance request
    /// to avoid costly cas conflicts during concurrent requests.
    pub instances_update: Mutex<()>,
    /// Manage plugins loaded or must be loaded at this node.
    pub plugin_manager: Rc<PluginManager>,
    pub(crate) instance_reachability: InstanceReachabilityManagerRef,
    pub alter_system_parameters: AlterSystemParametersRef,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("raft_id", &self.raft_id)
            .finish_non_exhaustive()
    }
}

static mut RAFT_NODE: Option<Box<Node>> = None;

impl Node {
    /// Initialize the global raft node singleton. Returns a reference to it.
    ///
    /// Returns an error in case of storage failure.
    ///
    /// If `for_tests` is `true` then the worker fibers are not started. This is
    /// only used for testing purposes.
    ///
    /// **This function yields**
    pub fn init(
        storage: Catalog,
        raft_storage: RaftSpaceAccess,
        alter_system_parameters: AlterSystemParametersRef,
        for_tests: bool,
    ) -> Result<&'static Self, Error> {
        // SAFETY: only accessed from main thread, and never mutated after
        // initialization (initialization happens later in this function)
        if unsafe { static_ref!(const RAFT_NODE).is_some() } {
            return Err(Error::other("raft node is already initialized"));
        }

        let tls_connector = if for_tests {
            None
        } else {
            crate::iproto::get_tls_connector()
        };

        let opts = WorkerOptions {
            raft_msg_handler: proc_name!(proc_raft_interact),
            tls_connector: tls_connector.cloned(),
            ..Default::default()
        };
        let mut pool = ConnectionPool::new(storage.clone(), opts);
        let instance_reachability = instance_reachability_manager(alter_system_parameters.clone());
        pool.instance_reachability = Some(instance_reachability.clone());
        let pool = Rc::new(pool);
        let plugin_manager = Rc::new(PluginManager::new(storage.clone(), tls_connector.cloned()));

        let main_loop_info = Rc::new(NoYieldsRefCell::new(MainLoopInfo::default()));

        let node_impl = NodeImpl::new(
            pool.clone(),
            storage.clone(),
            raft_storage.clone(),
            alter_system_parameters.clone(),
            plugin_manager.clone(),
            instance_reachability.clone(),
            main_loop_info.clone(),
        )?;
        let topology_cache = node_impl.topology_cache.clone();

        let raft_id = node_impl.raft_id();
        let status = node_impl.status.subscribe();
        let applied = node_impl.applied.subscribe();

        let node_impl = Rc::new(Mutex::new(node_impl));

        // Raft main loop accesses the global node refernce,
        // so it must be initilized before the main loop starts.
        let guard = crate::util::NoYieldsGuard::new();

        let main_loop = if for_tests {
            MainLoop::for_tests()
        } else {
            MainLoop::start(node_impl.clone())
        };

        let governor_loop = if for_tests {
            governor::Loop::for_tests()
        } else {
            governor::Loop::start(
                pool.clone(),
                status.clone(),
                storage.clone(),
                raft_storage.clone(),
            )
        };

        let sentinel_loop = if for_tests {
            sentinel::Loop::for_tests()
        } else {
            sentinel::Loop::start(pool.clone(), status.clone(), instance_reachability.clone())
        };

        let node = Node {
            raft_id,
            main_loop,
            governor_loop,
            sentinel_loop,
            pool,
            node_impl,
            topology_cache,
            storage,
            raft_storage,
            status,
            main_loop_info,
            applied,
            instances_update: Mutex::new(()),
            plugin_manager,
            instance_reachability,
            alter_system_parameters,
        };

        // SAFETY: only accessed from main thread, and never mutated after this
        unsafe { RAFT_NODE = Some(Box::new(node)) };
        let node = global().expect("just initialized it");

        drop(guard);

        // Wait for the node to enter the main loop
        node.tick_and_yield(0);
        Ok(node)
    }

    /// Initializes the global node instance for testing purposes.
    pub fn for_tests() -> &'static Self {
        let storage = Catalog::for_tests();
        let raft_storage = RaftSpaceAccess::for_tests();
        let alter_system_parameters = Default::default();
        Self::init(storage.clone(), raft_storage, alter_system_parameters, true).unwrap()
    }

    #[inline(always)]
    pub fn raft_id(&self) -> RaftId {
        self.raft_id
    }

    #[inline(always)]
    pub fn status(&self) -> Status {
        self.status.get()
    }

    #[inline(always)]
    #[track_caller]
    pub(crate) fn node_impl(&self) -> MutexGuard<'_, NodeImpl> {
        self.node_impl.lock()
    }

    /// Wait for the status to be changed.
    /// **This function yields**
    #[inline(always)]
    pub fn wait_status(&self) {
        fiber::block_on(self.status.clone().changed()).unwrap();
    }

    /// Returns current applied [`RaftIndex`].
    #[inline(always)]
    pub fn get_index(&self) -> RaftIndex {
        self.applied.get()
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
    #[track_caller]
    pub fn read_index(&self, timeout: Duration) -> traft::Result<RaftIndex> {
        let deadline = fiber::clock().saturating_add(timeout);

        let rx = self.raw_operation(|node_impl| node_impl.read_index_async())?;
        let index: RaftIndex = match fiber::block_on(rx.timeout(timeout)) {
            Ok(v) => v,
            Err(_) => return Err(Error::timeout()),
        };

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
    // TODO: this should also take a term and return an error if the term
    // changes, because it means that leader has changed and the entry got
    // rolled back.
    #[track_caller]
    pub fn wait_index(&self, target: RaftIndex, timeout: Duration) -> traft::Result<RaftIndex> {
        // tlog!(Debug, "waiting for applied index {target}");
        let mut applied = self.applied.clone();
        let deadline = fiber::clock().saturating_add(timeout);
        fiber::block_on(async {
            loop {
                let current = self.get_index();
                if current >= target {
                    // tlog!(
                    //     Debug,
                    //     "done waiting for applied index {target}, current: {current}"
                    // );
                    return Ok(current);
                }

                let timeout = deadline.duration_since(fiber::clock());
                let res = applied.changed().timeout(timeout).await;
                if let Err(TimeoutError::Expired) = res {
                    tlog!(
                        Debug,
                        "failed waiting for applied index {target}: timeout, current: {current}"
                    );
                    return Err(Error::timeout());
                }
            }
        })
    }

    /// Proposes a `Op::Nop` operation to raft log.
    /// Returns the index of the resuling entry.
    ///
    /// If called on a non leader, returns an error.
    ///
    /// **This function yields**
    #[inline]
    pub fn propose_nop(&self) -> traft::Result<RaftIndex> {
        let entry_id = self.raw_operation(|node_impl| node_impl.propose_async(Op::Nop))?;
        Ok(entry_id.index)
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
    pub fn step_and_yield(&self, msg: RaftMessageExt) {
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

    #[inline]
    pub fn is_readonly(&self) -> bool {
        crate::tarantool::box_is_ro()
    }
}

pub(crate) struct NodeImpl {
    pub raw_node: RawNode,
    pub read_state_wakers: HashMap<LogicalClock, oneshot::Sender<RaftIndex>>,
    joint_state_latch: KVCell<RaftIndex, oneshot::Sender<Result<(), RaftError>>>,
    storage: Catalog,
    topology_cache: Rc<TopologyCache>,
    alter_system_parameters: AlterSystemParametersRef,
    raft_storage: RaftSpaceAccess,
    pool: Rc<ConnectionPool>,
    lc: LogicalClock,
    status: watch::Sender<Status>,
    /// Index of the last applied raft log entry.
    applied: watch::Sender<RaftIndex>,
    /// Index of the last committed raft log entry.
    commit: watch::Sender<RaftIndex>,

    main_loop_info: Rc<NoYieldsRefCell<MainLoopInfo>>,

    instance_reachability: InstanceReachabilityManagerRef,
    plugin_manager: Rc<PluginManager>,

    /// Stores the first snapshot chunk while snapshot application is blocked.
    pending_raft_snapshot: Option<RaftSnapshot>,

    /// This is set to raft id of last instance which sent us a raft message with
    /// [`Flags::EXPECTING_SNAPSHOT_STATUS`] flag set in it.
    ///
    /// We will try to send a snapshot report to our leader (falling back to the sender if the leader is not known).
    /// This will happen regardless of whether we have actually applied a snapshot or not.
    report_snapshot_status_to: Option<RaftId>,

    /// Persistent outgoing messages preserved when being blocked on replicaset sync.
    pending_persisted_messages: Vec<raft::Message>,
}

#[derive(Debug, Clone, PartialEq)]
struct AppliedDml {
    table: SpaceId,
    new_tuple: Tuple,
}

impl NodeImpl {
    fn new(
        pool: Rc<ConnectionPool>,
        storage: Catalog,
        raft_storage: RaftSpaceAccess,
        alter_system_parameters: AlterSystemParametersRef,
        plugin_manager: Rc<PluginManager>,
        instance_reachability: InstanceReachabilityManagerRef,
        main_loop_info: Rc<NoYieldsRefCell<MainLoopInfo>>,
    ) -> traft::Result<Self> {
        let raft_id: RaftId = raft_storage
            .raft_id()?
            .expect("raft_id should be set by the time the node is being initialized");
        let applied: RaftIndex = raft_storage.applied()?;
        let commit: RaftIndex = raft_storage.commit()?;
        let term: RaftTerm = raft_storage.term()?;
        let lc = {
            let gen = raft_storage.gen().unwrap() + 1;
            raft_storage.persist_gen(gen).unwrap();
            LogicalClock::new(raft_id, gen)
        };

        let cfg = raft::Config {
            id: raft_id,
            applied,
            pre_vote: true,
            // Send heartbeat every 10 raft main loop ticks (see MainLoop::TICK).
            heartbeat_tick: 10,
            // Raft-rs suggests this to be 10 * heartbeat_tick.
            election_tick: 100,
            // XXX: this value is pretty random, we should really do some
            // testing to determine the best value for it.
            max_size_per_msg: 64,
            ..Default::default()
        };

        let raw_node = RawNode::new(&cfg, raft_storage.clone(), tlog::root())?;

        let (status, _) = watch::channel(Status {
            id: raft_id,
            leader_id: None,
            term,
            raft_state: RaftState::Follower,
            main_loop_status: "idle",
        });
        let (applied, _) = watch::channel(applied);
        let (commit, _) = watch::channel(commit);

        // Note that this is not a memory leak. This is equivalent to storing
        // the `Box` onto a global variable which is never dropped except that
        // we don't have to fight the borrow checker.
        let cluster_name = raft_storage.cluster_name()?;
        let cluster_name = Box::leak(cluster_name.into_boxed_str());
        let cluster_uuid = raft_storage.cluster_uuid()?;
        let cluster_uuid = Box::leak(cluster_uuid.into_boxed_str());

        let topology_cache = Rc::new(TopologyCache::load(
            &storage,
            raft_id,
            cluster_name,
            cluster_uuid,
        )?);

        Ok(Self {
            raw_node,
            read_state_wakers: Default::default(),
            joint_state_latch: KVCell::new(),
            storage,
            topology_cache,
            alter_system_parameters,
            raft_storage,
            instance_reachability,
            pool,
            lc,
            status,
            applied,
            commit,
            main_loop_info,
            plugin_manager,
            pending_raft_snapshot: None,
            report_snapshot_status_to: None,
            pending_persisted_messages: Vec::new(),
        })
    }

    fn raft_id(&self) -> RaftId {
        self.raw_node.raft.id
    }

    pub fn read_index_async(&mut self) -> Result<oneshot::Receiver<RaftIndex>, RaftError> {
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

        let (lc, rx) = self.schedule_read_state_waker();
        // read_index puts this context into an Entry,
        // so we've got to compose full EntryContext,
        // despite single LogicalClock would be enough
        let ctx = traft::ReadStateContext { lc };
        self.raw_node.read_index(ctx.to_raft_ctx());
        Ok(rx)
    }

    /// **Doesn't yield**
    ///
    /// Returns id of the proposed entry, which can be used to await it's
    /// application.
    ///
    /// Returns an error if current instance is not a raft leader, because
    /// followers should propose raft log entries via [`proc_cas_v2`].
    ///
    /// NOTE: the proposed entry may still be dropped, and the entry at that
    /// index may be some other one. It's the callers responsibility to verify
    /// which entry got committed.
    ///
    /// [`proc_cas_v2`]: crate::cas::proc_cas_v2
    #[inline]
    pub fn propose_async<T>(&mut self, op: T) -> Result<RaftEntryId, Error>
    where
        T: Into<Op>,
    {
        if self.raw_node.raft.state != RaftStateRole::Leader {
            return Err(Error::NotALeader);
        }

        let index_before = self.raw_node.raft.raft_log.last_index();
        let term = self.raw_node.raft.term;

        let context = traft::EntryContext::Op(op.into());

        // Check resulting raft log entry does not exceed the maximum tuple size limit.
        let entry = context.to_raft_entry();
        let tuple_size = traft::Entry::tuple_size(index_before + 1, term, &[], &entry.context);
        if tuple_size > PicodataConfig::max_tuple_size() {
            let message = format!("tuple size {tuple_size} exceeds the allowed limit");
            tlog!(Warning, "{message}");
            return Err(BoxError::new(TarantoolErrorCode::MemtxMaxTupleSize, message).into());
        }

        // Copy-pasted from `raft::raw_node::RawNode::propose`
        let mut m = raft::Message::default();
        m.set_msg_type(raft::MessageType::MsgPropose);
        m.from = self.raw_node.raft.id;
        m.set_entries(vec![entry]);

        crate::error_injection!("RAFT_PROPOSAL_DROPPED" => return Err(traft::error::Error::Raft(RaftError::ProposalDropped)));
        self.raw_node.raft.step(m)?;

        let index = self.raw_node.raft.raft_log.last_index();
        debug_assert!(index == index_before + 1);

        let entry_id = RaftEntryId { term, index };
        Ok(entry_id)
    }

    pub fn campaign(&mut self) -> traft::Result<()> {
        // When a raft node is initialized the term is set to 0, but it's
        // increased as soon as a raft configuration is applied. So term = 0
        // means that this node has not seen any configuration changes yet
        // and it makes no sense trying to promote to leader (actually
        // it makes sense NOT to promote, as there are some `.unwrap()`
        // calls in raft-rs which will panic in this case).
        if self.status.get().term == 0 {
            return Err(
                to_error_other("initial raft configuration wasn't applied yet (term = 0)").into(),
            );
        }

        // This is a guard against another naked `.unwrap()` in raft-rs.
        if self
            .raw_node
            .raft
            .prs()
            .get(self.raw_node.raft.id)
            .is_none()
        {
            return Err(to_error_other("initial raft configuration wasn't applied yet").into());
        }

        self.raw_node.campaign()?;

        Ok(())
    }

    pub fn step(&mut self, msg: RaftMessageExt) -> Result<(), RaftError> {
        if msg.inner.to != self.raft_id() {
            return Ok(());
        }

        // This happens on instance which received the snapshot
        if msg.flags.contains(Flags::EXPECTING_SNAPSHOT_STATUS)
            || msg.inner.msg_type() == raft::MessageType::MsgSnapshot
        {
            self.report_snapshot_status_to = Some(msg.inner.from);
        }

        // This happens on instance which sent out the snapshot
        if msg.flags.contains(Flags::SNAPSHOT_STATUS_SUCCESS) {
            self.raw_node
                .report_snapshot(msg.inner.from, raft::SnapshotStatus::Finish)
        } else if msg.flags.contains(Flags::SNAPSHOT_STATUS_FAILURE) {
            self.raw_node
                .report_snapshot(msg.inner.from, raft::SnapshotStatus::Failure)
        }

        if msg.flags.contains(Flags::SKIP_RAW_NODE_STEP) {
            return Ok(());
        }

        // TODO check it's not a MsgPropose with op::Dml for updating _pico_instance.
        // TODO check it's not a MsgPropose with ConfChange.
        self.raw_node.step(msg.inner)
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

        Ok(rx)
    }

    /// Returns `Err` if an unexpected error happens.
    /// Returns `Ok(Some(err))` if raft entry is blocked, this can happen when a
    /// DdlCommit is being applied on the read-only replica for example.
    /// Return `Ok(None)` when everything is fine.
    ///
    /// TODO: this function should just return `Result<()>` and `Err` should
    /// always cause a later retry of `NodeImpl::advance`, but we can't do this
    /// until <https://git.picodata.io/core/picodata/-/issues/1149> if fixed.
    fn handle_committed_entries(
        &mut self,
        entries: &[raft::Entry],
    ) -> traft::Result<Option<BoxError>> {
        let applied = self.applied.get();
        let commit = self.commit.get();

        if commit == applied {
            // Everything is applied
            return Ok(None);
        }

        let next_applied = applied + 1;

        let traft_entries;
        if entries.is_empty() || entries[0].index > next_applied {
            // This happens when we failed to apply an entry on last iteration.
            // Raft-rs only reports newly committed entries, so we need to get
            // the old ones directly from storage.
            traft_entries = self
                .raft_storage
                .entries(next_applied, commit + 1, None)
                .expect("raft entries should decode correctly");
        } else {
            traft_entries = entries
                .iter()
                .skip_while(|entry| entry.index < next_applied)
                .map(|entry| {
                    traft::Entry::try_from(entry).expect("raft entries should decode correctly")
                })
                .collect();
        }

        if traft_entries.is_empty() {
            // This is unlikely, but not a problem, any committed unhandled
            // entries will be handled on the next iteration of raft_main_loop
            return Ok(None);
        }

        debug_assert_eq!(traft_entries[0].index, next_applied);

        let mut last_applied = None;
        let mut last_error = None;

        for entry in traft_entries {
            let entry_index = entry.index;
            let entry_term = entry.term;

            self.main_loop_info.borrow_mut().last_entry = Some(entry.clone());

            let mut apply_entry_result = EntryApplied(vec![]);
            transaction(|| -> tarantool::Result<()> {
                self.main_loop_status("handling committed entries");

                match entry.entry_type {
                    raft::EntryType::EntryNormal => {
                        apply_entry_result = self.handle_committed_normal_entry(entry);
                        if !matches!(apply_entry_result, EntryApplied(_)) {
                            return Ok(());
                        }
                    }
                    raft::EntryType::EntryConfChange | raft::EntryType::EntryConfChangeV2 => {
                        self.handle_committed_conf_change(entry)
                    }
                }

                let res = self.raft_storage.persist_applied(entry_index, entry_term);
                if let Err(e) = res {
                    tlog!(
                        Error,
                        "error persisting applied index: {e}";
                        "index" => entry_index
                    );
                }

                Ok(())
            })?;

            let dmls = match apply_entry_result {
                EntryApplied(v) => v,
                SleepAndRetry(e) => {
                    last_error = Some(e);
                    break;
                }
            };

            let current_tier = self.topology_cache.my_tier_name();
            // currently only parameters from _pico_db_config processed outside of transaction (here)
            for AppliedDml { table, new_tuple } in dmls {
                debug_assert!(table == DbConfig::TABLE_ID);
                apply_parameter(&self.alter_system_parameters, new_tuple, current_tier)?;
            }

            // Update node's applied index
            self.applied
                .send(entry_index)
                .expect("applied shouldn't ever be borrowed across yields");

            crate::error_injection!("BLOCK_AFTER_APPLIED_ENTRY_IF_OWN_TARGET_STATE_OFFLINE" => {
                if self.topology_cache.my_target_state().variant == crate::instance::StateVariant::Offline {
                    crate::error_injection!(block "BLOCK_AFTER_APPLIED_ENTRY_IF_OWN_TARGET_STATE_OFFLINE");
                }
            });

            last_applied = Some(entry_index);
        }

        let Some(last_applied) = last_applied else {
            // We can only get here if there was an error.
            debug_assert!(last_error.is_some());
            let error = last_error.unwrap_or_else(|| to_error_other("unknown error"));
            let stats = self.main_loop_info.borrow();
            let last_entry = stats.last_entry.as_ref().expect("just assigned");
            let message = format!(
                "failed to apply raft entry {last_entry}: {}",
                error.message()
            );
            return Ok(Some(with_modified_message(error, message)));
        };

        // Advance the applied index.
        self.raw_node.advance_apply_to(last_applied);

        crate::error_injection!(exit "EXIT_AFTER_RAFT_HANDLES_COMMITTED_ENTRIES");

        Ok(None)
    }

    fn wake_governor_if_needed(&self, op: &Op) {
        let wake_governor = match &op {
            Op::Dml(op) => dml_is_governor_wakeup_worthy(op),
            Op::BatchDml { ops } => ops.iter().any(dml_is_governor_wakeup_worthy),
            Op::DdlPrepare { .. } => true,
            _ => false,
        };

        #[inline(always)]
        fn dml_is_governor_wakeup_worthy(op: &Dml) -> bool {
            matches!(
                op.table_id(),
                storage::Properties::TABLE_ID
                    | storage::Replicasets::TABLE_ID
                    | storage::Instances::TABLE_ID
                    | storage::Tiers::TABLE_ID
                    | catalog::governor_queue::GovernorQueue::TABLE_ID
            )
        }

        // NOTE: this may be premature, because the dml may fail to apply and/or
        // the transaction may be rolled back, but we ignore this for the sake
        // of simplicity, as nothing bad can happen if governor makes another
        // idle iteration.
        if wake_governor {
            let res = global()
                .expect("node must be initialized by this point")
                .governor_loop
                .wakeup();
            if let Err(e) = res {
                tlog!(Warning, "failed waking up governor: {e}");
            }
        }
    }

    /// Actions needed when applying a DML entry.
    ///
    /// Returns Ok(_) if entry was applied successfully
    fn handle_dml_entry(&self, op: &Dml) -> Result<Option<AppliedDml>, BoxError> {
        let table_id = op.table_id();
        let initiator = op.initiator();

        // In order to implement the audit log events, we have to compare
        // tuples from certain system spaces before and after a DML operation.
        //
        // In theory, we could move this code to `do_dml`, but in practice we
        // cannot do that just yet, because we aren't allowed to universally call
        // `extract_key` for arbitrary tuple/space pairs due to insufficient validity
        // checks on its side -- it might just crash for user input.
        //
        // Remeber, we're not the only ones using CaS; users may call it for their
        // own spaces, thus emitting unrestricted (and unsafe) DML records.
        //
        // TODO: merge this into `do_dml` once `box_tuple_extract_key` is fixed.
        let old = match table_id {
            s @ (storage::Properties::TABLE_ID
            | storage::Instances::TABLE_ID
            | storage::Replicasets::TABLE_ID
            | storage::Tiers::TABLE_ID
            | storage::ServiceRouteTable::TABLE_ID) => {
                let s = space_by_id(s).expect("system space must exist");
                match &op {
                    // There may be no previous version for inserts.
                    Dml::Insert { .. } => Ok(None),
                    Dml::Update { key, .. } => s.get(key),
                    Dml::Delete { key, .. } => s.get(key),
                    Dml::Replace { tuple, .. } => {
                        let tuple = Tuple::from(tuple);
                        let key_def =
                            cached_key_def(s.id(), 0).expect("index for space must be found");
                        let key = key_def
                            .extract_key(&tuple)
                            .expect("cas should validate operation before committing a log entry");
                        s.get(&key)
                    }
                }
            }
            _ => Ok(None),
        };

        // Perform DML and combine both tuple versions into a pair.
        let res = old.and_then(|old| {
            let new = self.storage.do_dml(op)?;
            Ok((old, new))
        });
        let (old, new) = match res {
            Ok(v) => v,
            Err(e) => {
                return Err(storage_corrupted(format!("global DML failed: {e}")));
            }
        };

        // FIXME: all of this should be done only after the transaction is committed
        // See <https://git.picodata.io/core/picodata/-/issues/1149>
        match table_id {
            storage::Instances::TABLE_ID => {
                let old = old
                    .as_ref()
                    .map(|x| x.decode().expect("schema upgrade not supported yet"));

                let new = new
                    .as_ref()
                    .map(|x| x.decode().expect("format was already verified"));

                // Handle insert, replace, update in _pico_instance
                if let Some(new) = &new {
                    // Dml::Delete mandates that new tuple is None.
                    assert!(!matches!(op, Dml::Delete { .. }));

                    let initiator_def = user_by_id(initiator).expect("user must exist");

                    do_audit_logging_for_instance_update(old.as_ref(), new, &initiator_def);

                    if old.as_ref().map(|x| x.current_state) != Some(new.current_state) {
                        metrics::record_instance_state(
                            &new.tier,
                            &new.name,
                            &new.current_state.variant,
                        );
                    }
                }

                self.topology_cache.update_instance(old, new);
            }

            storage::Replicasets::TABLE_ID => {
                let old = old
                    .as_ref()
                    .map(|x| x.decode().expect("schema upgrade not supported yet"));
                let new = new
                    .as_ref()
                    .map(|x| x.decode().expect("format was already verified"));
                self.topology_cache.update_replicaset(old, new);
            }

            storage::Tiers::TABLE_ID => {
                let old = old
                    .as_ref()
                    .map(|x| x.decode().expect("schema upgrade not supported yet"));
                let new = new
                    .as_ref()
                    .map(|x| x.decode().expect("format was already verified"));
                self.topology_cache.update_tier(old, new);
            }

            storage::ServiceRouteTable::TABLE_ID => {
                let old = old
                    .as_ref()
                    .map(|x| x.decode().expect("schema upgrade not supported yet"));
                let new = new
                    .as_ref()
                    .map(|x| x.decode().expect("format was already verified"));
                self.topology_cache.update_service_route(old, new);
            }

            storage::DbConfig::TABLE_ID => {
                let new_tuple = new.expect("can't delete tuple from _pico_db_config");
                return Ok(Some(AppliedDml {
                    table: storage::DbConfig::TABLE_ID,
                    new_tuple,
                }));
            }

            _ => {}
        }

        Ok(None)
    }

    /// Is called during a transaction
    fn handle_committed_normal_entry(&mut self, entry: traft::Entry) -> ApplyEntryResult {
        assert_eq!(entry.entry_type, raft::EntryType::EntryNormal);
        let index = entry.index;
        let op = entry.into_op().unwrap_or(Op::Nop);
        tlog!(Debug, "applying entry: {op}"; "index" => index);

        self.wake_governor_if_needed(&op);

        let storage_properties = &self.storage.properties;
        let mut res = ApplyEntryResult::EntryApplied(vec![]);

        // apply the operation
        match op {
            Op::Nop => {}
            Op::BatchDml { ops } => {
                let mut applied_dmls = Vec::with_capacity(ops.len());
                for op in ops.into_iter() {
                    match self.handle_dml_entry(&op) {
                        Ok(Some(applied_dml)) => applied_dmls.push(applied_dml),
                        Err(e) => return SleepAndRetry(e),
                        _ => (),
                    }
                }

                res = EntryApplied(applied_dmls);
            }
            Op::Dml(op) => match self.handle_dml_entry(&op) {
                Ok(applied_dml) => res = EntryApplied(Vec::from_iter(applied_dml)),
                Err(e) => return SleepAndRetry(e),
            },
            Op::DdlPrepare {
                ddl,
                schema_version,
                governor_op_id,
            } => {
                crate::error_injection!("STALL_BEFORE_APPLYING_DDL_PREPARE" => return SleepAndRetry(to_error_other("injected error")));

                tlog!(Info, "Applying DdlPrepare for {ddl:?}");

                self.apply_op_ddl_prepare(ddl, schema_version, governor_op_id)
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

                tlog!(Info, "Applying DdlCommit for {ddl:?}");

                // This instance is catching up to the cluster.
                if v_local < v_pending {
                    tlog!(
                        Info,
                        "Catching up from {v_local} to {v_pending} for {ddl:?}"
                    );
                    if self.is_readonly() {
                        return SleepAndRetry(read_only(
                            "awaiting DDL results from master replica",
                        ));
                    } else {
                        // Master applies schema change at this point.
                        // Note: Unlike RPC handler `proc_apply_schema_change`, there is no need
                        // for a schema change lock. When instance is catching up to the cluster,
                        // RPCs will be blocked waiting for the applied index from the request to
                        // be applied on master *, so no concurrent changes can happen.
                        //
                        // * https://git.picodata.io/picodata/picodata/picodata/-/blob/ccba5cf1956e41b31eac8cdfacd0e4344033dda1/src/rpc/ddl_apply.rs#L32
                        let res = rpc::ddl_apply::apply_schema_change(
                            &self.storage,
                            &ddl,
                            v_pending,
                            true,
                            self.topology_cache.my_tier_name(),
                        );
                        match res {
                            Err(rpc::ddl_apply::Error::Other(err)) => {
                                panic!("storage should not fail, but failed with: {err}")
                            }
                            Err(rpc::ddl_apply::Error::Aborted(reason)) => {
                                tlog!(Error, "failed applying committed ddl operation: {reason}";
                                    "ddl" => ?ddl,
                                );
                                return SleepAndRetry(storage_corrupted(format!(
                                    "DDL aborted while catching up: {reason}"
                                )));
                            }
                            Ok(_) => {}
                        }
                    }
                } else if self.is_readonly() {
                    tlog!(
                        Info,
                        "local_schema_version = {v_local} (replica, ddl commit)"
                    );
                }

                // Update pico metadata.
                match ddl {
                    Ddl::Backup { timestamp } => {
                        // Update `operable` flag as far as we've disabled it
                        // when handling DdlPrepare.
                        for table_def in self
                            .storage
                            .pico_table
                            .iter()
                            .expect("storage should not fail")
                        {
                            ddl_meta_space_update_operable(&self.storage, table_def.id, true)
                                .expect("storage shouldn't fail");
                        }

                        self.storage
                            .properties
                            .put(PropertyName::LastBackupTimestamp, &timestamp)
                            .expect("_pico_property update should not fail");
                    }

                    Ddl::CreateTable {
                        id, name, owner, ..
                    } => {
                        ddl_meta_space_update_operable(&self.storage, id, true)
                            .expect("storage shouldn't fail");

                        let initiator_def = user_by_id(owner).expect("user must exist");

                        crate::audit!(
                            message: "created table `{name}`",
                            title: "create_table",
                            severity: Medium,
                            name: &name,
                            initiator: initiator_def.name,
                        );
                    }

                    Ddl::DropTable { id, initiator } => {
                        let space_raw = self.storage.pico_table.get(id);
                        let space = space_raw.ok().flatten().expect("failed to get space");
                        ddl_meta_drop_space(&self.storage, id).expect("storage shouldn't fail");

                        let initiator_def = user_by_id(initiator).expect("user must exist");

                        let name = &space.name;
                        crate::audit!(
                            message: "dropped table `{name}`",
                            title: "drop_table",
                            severity: Medium,
                            name: &name,
                            initiator: initiator_def.name,
                        );
                    }

                    Ddl::TruncateTable { id, initiator } => {
                        // Truncate on global tables is applied on all replicas
                        // directly, because global tables are implemented as
                        // tarantool local tables.
                        if ddl.is_truncate_on_global_table(&self.storage) {
                            let abort_reason = ddl_truncate_space_on_master(id)
                                .expect("truncating a local space shouldn't fail");
                            if let Some(e) = abort_reason {
                                crate::warn_or_panic!("global table truncate failed: {e}");
                            }
                        }

                        ddl_meta_space_update_operable(&self.storage, id, true)
                            .expect("storage shouldn't fail");

                        let initiator_def = user_by_id(initiator).expect("user must exist");

                        let space_raw = self.storage.pico_table.get(id);
                        let space = space_raw.ok().flatten().expect("failed to get space");
                        let name = &space.name;
                        crate::audit!(
                            message: "truncated table `{name}`",
                            title: "truncate_table",
                            severity: Medium,
                            name: &name,
                            initiator: initiator_def.name,
                        );
                    }

                    Ddl::RenameTable {
                        table_id,
                        old_name,
                        new_name,
                        initiator_id,
                        schema_version,
                        ..
                    } => {
                        self.storage
                            .pico_table
                            .update_operable(table_id, true)
                            .expect("storage shouldn't fail");
                        self.storage
                            .pico_table
                            .update_schema_version(table_id, schema_version)
                            .expect("storage shouldn't fail");

                        let initiator_def = user_by_id(initiator_id).expect("user must exist");
                        crate::audit!(
                            message: "renamed table `{old_name}` to `{new_name}`",
                            title: "rename_table",
                            severity: Medium,
                            old_name: &old_name,
                            new_name: &new_name,
                            initiator: initiator_def.name,
                        );
                    }

                    Ddl::ChangeFormat {
                        table_id,
                        initiator_id,
                        schema_version,
                        ..
                    } => {
                        ddl_meta_space_update_operable(&self.storage, table_id, true)
                            .expect("storage shouldn't fail");
                        // TODO: it would be nice to fuse the update of `operable` field with the update of `schema_version`
                        // this needs some API design though
                        self.storage
                            .pico_table
                            .update_schema_version(table_id, schema_version)
                            .expect("storage shouldn't fail");

                        let initiator_def = user_by_id(initiator_id).expect("user must exist");

                        let space_raw = self.storage.pico_table.get(table_id);
                        let space = space_raw.ok().flatten().expect("failed to get space");
                        let name = &space.name;
                        crate::audit!(
                            message: "changed table format for `{name}`",
                            title: "change_table_format",
                            severity: Medium,
                            name: &name,
                            initiator: initiator_def.name,
                        );
                    }

                    Ddl::CreateProcedure {
                        id, name, owner, ..
                    } => {
                        self.storage
                            .routines
                            .update_operable(id, true)
                            .expect("storage shouldn't fail");

                        let initiator_def = user_by_id(owner).expect("user must exist");

                        crate::audit!(
                            message: "created procedure `{name}`",
                            title: "create_procedure",
                            severity: Medium,
                            name: &name,
                            initiator: initiator_def.name,
                        );
                    }

                    Ddl::DropProcedure { id, initiator } => {
                        let routine = self.storage.routines.by_id(id);
                        let routine = routine.ok().flatten().expect("routine must exist");
                        ddl_meta_drop_routine(&self.storage, id).expect("storage shouldn't fail");

                        let initiator_def = user_by_id(initiator).expect("user must exist");

                        let name = &routine.name;
                        crate::audit!(
                            message: "dropped procedure `{name}`",
                            title: "drop_procedure",
                            severity: Medium,
                            name: &name,
                            initiator: initiator_def.name,
                        );
                    }
                    Ddl::RenameProcedure {
                        routine_id,
                        old_name,
                        new_name,
                        initiator_id,
                        ..
                    } => {
                        self.storage
                            .routines
                            .update_operable(routine_id, true)
                            .expect("storage shouldn't fail");

                        let initiator_def = user_by_id(initiator_id).expect("user must exist");
                        crate::audit!(
                            message: "renamed procedure `{old_name}` to `{new_name}`",
                            title: "rename_procedure",
                            severity: Medium,
                            old_name: &old_name,
                            new_name: &new_name,
                            initiator: initiator_def.name,
                        );
                    }

                    Ddl::CreateIndex {
                        index_id,
                        space_id,
                        name,
                        initiator,
                        ..
                    } => {
                        self.storage
                            .indexes
                            .update_operable(space_id, index_id, true)
                            .expect("storage shouldn't fail");

                        let initiator_def = user_by_id(initiator).expect("user must exist");

                        crate::audit!(
                            message: "created index `{name}`",
                            title: "create_index",
                            severity: Medium,
                            name: &name,
                            initiator: initiator_def.name,
                        );
                    }

                    Ddl::DropIndex {
                        space_id,
                        index_id,
                        initiator,
                    } => {
                        let index = self.storage.indexes.get(space_id, index_id);
                        let index = index.ok().flatten().expect("index must exist");
                        let name = &index.name;
                        self.storage
                            .indexes
                            .delete(space_id, index_id)
                            .expect("storage shouldn't fail");
                        let initiator_def = user_by_id(initiator).expect("user must exist");

                        crate::audit!(
                            message: "dropped index `{name}`",
                            title: "drop_index",
                            severity: Medium,
                            name: &index.name,
                            initiator: initiator_def.name,
                        );
                    }

                    Ddl::RenameIndex {
                        space_id,
                        index_id,
                        old_name,
                        new_name,
                        initiator_id,
                        ..
                    } => {
                        self.storage
                            .indexes
                            .update_operable(space_id, index_id, true)
                            .expect("storage shouldn't fail");
                        let initiator_def = user_by_id(initiator_id).expect("user must exist");

                        crate::audit!(
                            message: "renamed index `{old_name}` to `{new_name}`",
                            title: "rename_index",
                            severity: Medium,
                            old_name: &old_name,
                            new_name: &new_name,
                            initiator: initiator_def.name,
                        );
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

                if let Some(governor_op_id) = storage_properties
                    .pending_governor_op_id()
                    .expect("getting of pending governor operation id should not fail")
                {
                    self.storage
                        .governor_queue
                        .update_status(governor_op_id, GovernorOpStatus::Done)
                        .expect("update governor operation status should not fail");
                    storage_properties
                        .delete(PropertyName::PendingGovernorOpId)
                        .expect("delete pending governor operation id should not fail");
                }
            }
            Op::DdlAbort { .. } => {
                crate::error_injection!(block "BLOCK_GOVERNOR_BEFORE_DDL_ABORT");
                let v_local = local_schema_version().expect("storage should not fail");
                let v_pending: u64 = storage_properties
                    .pending_schema_version()
                    .expect("storage should not fail")
                    .expect("granted we don't mess up log compaction, this should not be None");
                let ddl = storage_properties
                    .pending_schema_change()
                    .expect("storage should not fail")
                    .expect("granted we don't mess up log compaction, this should not be None");

                tlog!(Info, "Applying DdlAbort for {ddl:?}");

                // This condition means, schema versions must always increase
                // even after an DdlAbort
                if v_local == v_pending {
                    if self.is_readonly() {
                        return SleepAndRetry(read_only(
                            "awaiting DDL results from master replica",
                        ));
                    } else {
                        let v_global = storage_properties
                            .global_schema_version()
                            .expect("storage should not fail");
                        ddl_abort_on_master(&self.storage, &ddl, v_global)
                            .expect("storage should not fail");
                    }
                }

                // Update pico metadata.
                match ddl {
                    Ddl::Backup { .. } => {
                        // Update `operable` flag as far as we've disabled it
                        // when handling DdlPrepare.
                        for table_def in self
                            .storage
                            .pico_table
                            .iter()
                            .expect("storage should not fail")
                        {
                            ddl_meta_space_update_operable(&self.storage, table_def.id, true)
                                .expect("storage shouldn't fail");
                        }
                    }

                    Ddl::CreateTable { id, .. } => {
                        ddl_meta_drop_space(&self.storage, id).expect("storage shouldn't fail");
                    }

                    Ddl::DropTable { id, .. } => {
                        ddl_meta_space_update_operable(&self.storage, id, true)
                            .expect("storage shouldn't fail");
                    }

                    Ddl::RenameTable {
                        table_id, old_name, ..
                    } => {
                        self.storage
                            .pico_table
                            .rename(table_id, old_name.as_str())
                            .expect("storage shouldn't fail");
                        self.storage
                            .pico_table
                            .update_operable(table_id, true)
                            .expect("storage shouldn't fail");
                    }

                    Ddl::TruncateTable { .. } => {
                        unreachable!("TRUNCATE execution should not reach Op::DdlAbort handling")
                    }
                    Ddl::ChangeFormat {
                        table_id,
                        ref old_format,
                        ref column_renames,
                        ..
                    } => {
                        ddl_meta_space_update_operable(&self.storage, table_id, true)
                            .expect("storage shouldn't fail");
                        let reversed_renames = column_renames.reversed();

                        // rollback column renames in indices metadata
                        for mut index in self
                            .storage
                            .indexes
                            .by_space_id(table_id)
                            .expect("storage shouldn't fail")
                        {
                            if reversed_renames.transform_index_columns(&mut index) {
                                self.storage
                                    .indexes
                                    .put(&index)
                                    .expect("storage shouldn't fail");
                            }
                        }

                        self.storage
                            .pico_table
                            .update_format(table_id, old_format, &reversed_renames)
                            .expect("storage shouldn't fail");
                    }
                    Ddl::CreateProcedure { id, .. } => {
                        self.storage
                            .privileges
                            .delete_all_by_object(SchemaObjectType::Routine, id.into())
                            .expect("storage shouldn't fail");
                        self.storage
                            .routines
                            .delete(id)
                            .expect("storage shouldn't fail");
                    }
                    Ddl::DropProcedure { id, .. } => {
                        self.storage
                            .routines
                            .update_operable(id, true)
                            .expect("storage shouldn't fail");
                    }

                    Ddl::RenameProcedure {
                        routine_id,
                        old_name,
                        ..
                    } => {
                        self.storage
                            .routines
                            .rename(routine_id, old_name.as_str())
                            .expect("storage shouldn't fail");
                        self.storage
                            .routines
                            .update_operable(routine_id, true)
                            .expect("storage shouldn't fail");
                    }

                    Ddl::CreateIndex {
                        space_id, index_id, ..
                    } => {
                        self.storage
                            .indexes
                            .delete(space_id, index_id)
                            .expect("storage shouldn't fail");
                    }

                    Ddl::DropIndex {
                        space_id, index_id, ..
                    } => {
                        self.storage
                            .indexes
                            .update_operable(space_id, index_id, true)
                            .expect("storage shouldn't fail");
                    }
                    Ddl::RenameIndex {
                        space_id,
                        index_id,
                        old_name,
                        ..
                    } => {
                        self.storage
                            .indexes
                            .rename(space_id, index_id, &old_name)
                            .expect("storage shouldn't fail");
                        self.storage
                            .indexes
                            .update_operable(space_id, index_id, true)
                            .expect("storage shouldn't fail");
                    }
                }

                tlog!(Info, "Removing PendingSchemaChange");
                storage_properties
                    .delete(PropertyName::PendingSchemaChange)
                    .expect("storage should not fail");
                storage_properties
                    .delete(PropertyName::PendingSchemaVersion)
                    .expect("storage should not fail");

                if let Some(governor_op_id) = storage_properties
                    .pending_governor_op_id()
                    .expect("getting of pending governor operation id should not fail")
                {
                    self.storage
                        .governor_queue
                        .update_status(governor_op_id, GovernorOpStatus::Failed)
                        .expect("update governor operation status should not fail");
                    storage_properties
                        .delete(PropertyName::PendingGovernorOpId)
                        .expect("delete pending governor operation id should not fail");
                }
            }

            Op::Plugin(PluginRaftOp::DisablePlugin { ident, cause }) => {
                let plugin = self
                    .storage
                    .plugins
                    .get(&ident)
                    .expect("storage should not fail");

                if let Some(mut plugin) = plugin {
                    plugin.enabled = false;
                    self.storage
                        .plugins
                        .put(&plugin)
                        .expect("storage should not fail");

                    self.storage
                        .service_route_table
                        .delete_by_plugin(&ident)
                        .expect("hallelujah");

                    // XXX this place is not ideal. This is one of the few cases when we delete the "pending_plugin_operation"
                    // via a special raft operation. In all other cases we use Op::Dml for this. The problem is, there's
                    // no way to batch a Op::Dml with a Op::Plugin(DisablePlugin), but we do need this right now in our
                    // governor algorithm.
                    // The better solution would be to redesign the governor's algorithm for enabling/disabling plugins.
                    // But for now this is not fine, the only drawback is inconsistency which is not critical.
                    if cause.is_some() {
                        let t = self
                            .storage
                            .properties
                            .delete(PropertyName::PendingPluginOperation)
                            .expect("storage food shot nail");
                        if t.is_none() {
                            #[rustfmt::skip]
                            warn_or_panic!("expected a 'pending_plugin_operation' to be in `_pico_property`");
                        }
                    }
                } else {
                    warn_or_panic!(
                        "got DisablePlugin for a non existent plugin: {ident}, cause: {cause:?}"
                    );
                }

                if let Err(e) = self
                    .plugin_manager
                    .add_async_event_to_queue(PluginAsyncEvent::PluginDisabled { name: ident.name })
                {
                    tlog!(Warning, "async plugin event: {e}");
                }
            }

            // TODO: this operation is just a set of Dml::Delete operations,
            // however doing this via CaS would be nightmare. It would be much
            // simpler if we supported FOREIGN KEY/ON DELETE CASCADE
            // semantics, but we don't...
            Op::Plugin(PluginRaftOp::DropPlugin { ident }) => {
                let maybe_plugin = self
                    .storage
                    .plugins
                    .get(&ident)
                    .expect("storage should not fail");

                if let Some(plugin) = maybe_plugin {
                    if plugin.enabled {
                        warn_or_panic!("Op::DropPlugin for an enabled plugin");
                        return EntryApplied(Vec::new());
                    }

                    let services = self
                        .storage
                        .services
                        .get_by_plugin(&ident)
                        .expect("storage should not fail");
                    for svc in services {
                        self.storage
                            .services
                            .delete(&svc.plugin_name, &svc.name, &svc.version)
                            .expect("storage should not fail");
                        self.storage
                            .plugin_config
                            .remove_by_entity(&ident, &svc.name)
                            .expect("storage should not fail");
                    }
                    self.storage
                        .plugins
                        .delete(&ident)
                        .expect("storage should not fail");
                }
            }

            Op::Plugin(PluginRaftOp::Abort { .. }) => {
                // XXX here we delete the "pending_plugin_operation" from
                // `_pico_property` via a special raft operation but in other
                // places we do it via Dml. This inconsistency is not ideal,
                // but currently there's no way around this as we can't batch
                // non-dml operations with dml ones
                let t = self
                    .storage
                    .properties
                    .delete(PropertyName::PendingPluginOperation)
                    .expect("storage food shot nail");
                if t.is_none() {
                    #[rustfmt::skip]
                    warn_or_panic!("expected a 'pending_plugin_operation' to be in `_pico_property`");
                }
            }

            Op::Plugin(PluginRaftOp::PluginConfigPartialUpdate { ident, updates }) => {
                for (service, config_part) in updates {
                    if service == migration::CONTEXT_ENTITY {
                        self.storage
                            .plugin_config
                            .replace_many(&ident, &service, config_part)
                            .expect("storage should not fail");

                        continue;
                    }

                    let maybe_service = self
                        .storage
                        .services
                        .get(&ident, &service)
                        .expect("storage should not fail");

                    if let Some(svc) = maybe_service {
                        let old_cfg = self
                            .storage
                            .plugin_config
                            .get_by_entity_as_mp(&ident, &svc.name)
                            .expect("storage should not fail");
                        self.storage
                            .plugin_config
                            .replace_many(&ident, &service, config_part)
                            .expect("storage should not fail");
                        let new_cfg = self
                            .storage
                            .plugin_config
                            .get_by_entity_as_mp(&ident, &svc.name)
                            .expect("storage should not fail");

                        let new_raw_cfg =
                            rmp_serde::encode::to_vec_named(&new_cfg).expect("out of memory");
                        let old_cfg_raw =
                            rmp_serde::encode::to_vec_named(&old_cfg).expect("out of memory");

                        if let Err(e) = self.plugin_manager.add_async_event_to_queue(
                            PluginAsyncEvent::ServiceConfigurationUpdated {
                                ident: ident.clone(),
                                service: svc.name,
                                old_raw: old_cfg_raw,
                                new_raw: new_raw_cfg,
                            },
                        ) {
                            tlog!(Warning, "async plugin event: {e}");
                        }
                    } else {
                        crate::warn_or_panic!(
                            "got request to update configuration of non-existent service {}.{}:{}",
                            ident.name,
                            service,
                            ident.version
                        )
                    }
                }
            }

            Op::Acl(acl) => {
                let v_local = local_schema_version().expect("storage should not fail");
                let v_pending = acl.schema_version();
                if v_local < v_pending {
                    if self.is_readonly() {
                        // Wait for tarantool replication with master to progress.
                        return SleepAndRetry(read_only(
                            "awaiting DDL results from master replica",
                        ));
                    } else {
                        match &acl {
                            Acl::CreateUser { user_def } => {
                                acl::on_master_create_user(user_def, true)
                                    .expect("creating user shouldn't fail");
                            }
                            Acl::RenameUser { user_id, name, .. } => {
                                acl::on_master_rename_user(*user_id, name)
                                    .expect("renaming user shouldn't fail");
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
                            Acl::RevokePrivilege { priv_def, .. } => {
                                acl::on_master_revoke_privilege(priv_def)
                                    .expect("revoking a privilege shouldn't fail");
                            }
                            Acl::AuditPolicy { .. } => {}
                        }
                        set_local_schema_version(v_pending, "ACL")
                            .expect("storage should not fail");
                    }
                } else if self.is_readonly() {
                    tlog!(Info, "local_schema_version = {v_local} (replica, acl)");
                }

                match &acl {
                    Acl::CreateUser { user_def } => {
                        acl::global_create_user(&self.storage, user_def)
                            .expect("persisting a user definition shouldn't fail");
                    }
                    Acl::RenameUser {
                        user_id,
                        name,
                        initiator,
                        ..
                    } => {
                        acl::global_rename_user(&self.storage, *user_id, name, *initiator)
                            .expect("persisting a user definition shouldn't fail");
                    }
                    Acl::ChangeAuth {
                        user_id,
                        auth,
                        initiator,
                        ..
                    } => {
                        acl::global_change_user_auth(&self.storage, *user_id, auth, *initiator)
                            .expect("changing user definition shouldn't fail");
                    }
                    Acl::DropUser {
                        user_id, initiator, ..
                    } => {
                        acl::global_drop_user(&self.storage, *user_id, *initiator)
                            .expect("droping a user definition shouldn't fail");
                    }
                    Acl::CreateRole { role_def } => {
                        acl::global_create_role(&self.storage, role_def)
                            .expect("persisting a role definition shouldn't fail");
                    }
                    Acl::DropRole {
                        role_id, initiator, ..
                    } => {
                        acl::global_drop_role(&self.storage, *role_id, *initiator)
                            .expect("droping a role definition shouldn't fail");
                    }
                    Acl::GrantPrivilege { priv_def } => {
                        acl::global_grant_privilege(&self.storage, priv_def)
                            .expect("persiting a privilege definition shouldn't fail");
                    }
                    Acl::RevokePrivilege {
                        priv_def,
                        initiator,
                    } => {
                        acl::global_revoke_privilege(&self.storage, priv_def, *initiator)
                            .expect("removing a privilege definition shouldn't fail");
                    }
                    Acl::AuditPolicy {
                        user_id,
                        policy_id,
                        enable,
                        initiator,
                        ..
                    } => {
                        acl::global_audit_policy_by_user(
                            &self.storage,
                            *user_id,
                            *policy_id,
                            *enable,
                            *initiator,
                        )
                        .expect("persisting audit policy settings should not fail");
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

        if let Some(notify) = self.joint_state_latch.take_or_keep(&index) {
            // It was expected to be a ConfChange entry, but it's
            // normal. Raft must have overriden it, or there was
            // a re-election.
            let e = RaftError::ConfChangeError("rolled back".into());

            let _ = notify.send(Err(e));
        }

        res
    }

    fn apply_op_ddl_prepare(
        &self,
        ddl: Ddl,
        schema_version: u64,
        governor_op_id: Option<u64>,
    ) -> traft::Result<()> {
        debug_assert!(unsafe { tarantool::ffi::tarantool::box_txn() });

        match ddl.clone() {
            Ddl::Backup { .. } => {
                let system_table_defs: Vec<TableDef> = system_table_definitions()
                    .into_iter()
                    .map(|(td, _)| td)
                    .collect();

                // Traverse all user spaces and
                // set their `operable` flag to `false`.
                for table_def in self
                    .storage
                    .pico_table
                    .iter()
                    .expect("storage should not fail")
                {
                    if system_table_defs.contains(&table_def) {
                        continue;
                    }
                    ddl_meta_space_update_operable(&self.storage, table_def.id, false)
                        .expect("storage shouldn't fail");
                }
            }
            Ddl::CreateTable {
                id,
                name,
                mut format,
                mut primary_key,
                distribution,
                engine,
                owner,
            } => {
                let mut last_pk_part_index = 0;
                for pk_part in &mut primary_key {
                    let name = &pk_part.field;
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
                    table_id: id,
                    id: 0,
                    name: format!("{id}_pkey"),
                    ty: IndexType::Tree,
                    opts: vec![IndexOption::Unique(true)],
                    parts: primary_key,
                    operable: false,
                    schema_version,
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
                    }
                }

                let table_def = TableDef {
                    id,
                    name,
                    distribution,
                    schema_version,
                    format,
                    operable: false,
                    engine,
                    owner,
                    // TODO: add description field into Ddl::CreateTable
                    description: "".into(),
                };
                let res = self.storage.pico_table.insert(&table_def);
                if let Err(e) = res {
                    // Ignore the error for now, let governor deal with it.
                    tlog!(Warning, "failed creating table '{}': {e}", table_def.name);
                }
            }
            Ddl::CreateIndex {
                space_id,
                index_id,
                name,
                ty,
                opts,
                by_fields,
                ..
            } => {
                let index_def = IndexDef {
                    table_id: space_id,
                    id: index_id,
                    name,
                    ty,
                    opts,
                    parts: by_fields,
                    operable: false,
                    schema_version,
                };
                self.storage
                    .indexes
                    .insert(&index_def)
                    .expect("storage shouldn't fail");
            }
            Ddl::RenameIndex {
                space_id,
                index_id,
                new_name,
                ..
            } => {
                self.storage
                    .indexes
                    .rename(space_id, index_id, &new_name)
                    .expect("index existence must have been checked before commit");
                self.storage
                    .indexes
                    .update_operable(space_id, index_id, false)
                    .expect("storage shouldn't fail");
            }
            Ddl::DropTable { id, .. } => {
                ddl_meta_space_update_operable(&self.storage, id, false)
                    .expect("storage shouldn't fail");
            }
            Ddl::RenameTable {
                table_id, new_name, ..
            } => {
                self.storage
                    .pico_table
                    .rename(table_id, new_name.as_str())
                    .expect("table existence must have been checked before commit");

                self.storage
                    .pico_table
                    .update_operable(table_id, false)
                    .expect("storage shouldn't fail");
            }
            Ddl::TruncateTable { id, .. } => {
                ddl_meta_space_update_operable(&self.storage, id, false)
                    .expect("storage shouldn't fail");
            }
            Ddl::DropIndex {
                index_id, space_id, ..
            } => {
                self.storage
                    .indexes
                    .update_operable(space_id, index_id, false)
                    .expect("storage shouldn't fail");
            }
            Ddl::CreateProcedure {
                id,
                name,
                params,
                language,
                body,
                security,
                owner,
            } => {
                let proc_def = RoutineDef {
                    id,
                    name,
                    kind: RoutineKind::Procedure,
                    params,
                    returns: vec![],
                    language,
                    body,
                    operable: false,
                    security,
                    schema_version,
                    owner,
                };
                self.storage
                    .routines
                    .put(&proc_def)
                    .expect("storage shouldn't fail");
            }
            Ddl::DropProcedure { id, .. } => {
                self.storage
                    .routines
                    .update_operable(id, false)
                    .expect("storage shouldn't fail");
            }
            Ddl::RenameProcedure {
                routine_id,
                new_name,
                ..
            } => {
                self.storage
                    .routines
                    .rename(routine_id, new_name.as_str())?
                    .expect("routine existence must have been checked before commit");

                self.storage
                    .routines
                    .update_operable(routine_id, false)
                    .expect("storage shouldn't fail");
            }
            Ddl::ChangeFormat {
                table_id,
                new_format,
                column_renames,
                ..
            } => {
                self.storage
                    .pico_table
                    .update_format(table_id, &new_format, &column_renames)
                    .expect("storage shouldn't fail");

                // apply column renames in indices metadata
                for mut index in self
                    .storage
                    .indexes
                    .by_space_id(table_id)
                    .expect("storage shouldn't fail")
                {
                    if column_renames.transform_index_columns(&mut index) {
                        self.storage
                            .indexes
                            .put(&index)
                            .expect("storage shouldn't fail");
                    }
                }

                self.storage
                    .pico_table
                    .update_operable(table_id, false)
                    .expect("storage shouldn't fail");
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
        if let Some(governor_op_id) = governor_op_id {
            self.storage
                .properties
                .put(PropertyName::PendingGovernorOpId, &governor_op_id)?;
        }

        Ok(())
    }

    /// Is called during a transaction
    fn handle_committed_conf_change(&mut self, entry: traft::Entry) {
        let mut latch_unlock = || {
            if let Some(notify) = self.joint_state_latch.take() {
                let _ = notify.send(Ok(()));
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

    fn handle_read_states(&mut self, read_states: &[raft::ReadState]) {
        for rs in read_states {
            if rs.request_ctx.is_empty() {
                continue;
            }
            let ctx = crate::unwrap_ok_or!(
                traft::ReadStateContext::from_raft_ctx(&rs.request_ctx),
                Err(e) => {
                    tlog!(Error, "abnormal read_state: {e}"; "read_state" => ?rs);
                    continue;
                }
            );

            if let Some(waker) = self.read_state_wakers.remove(&ctx.lc) {
                _ = waker.send(rs.index);
            }
        }
    }

    fn handle_messages(&mut self, messages: Vec<raft::Message>) {
        if messages.is_empty() {
            return;
        }

        self.main_loop_status("sending raft messages");
        let mut sent_count = 0;
        let mut skip_count = 0;

        let applied = self.applied.get();
        let timeout = self
            .alter_system_parameters
            .borrow()
            .governor_common_rpc_timeout();

        for msg in messages {
            if msg.msg_type() == raft::MessageType::MsgHeartbeat {
                // decrease the rate of heartbeats sent to nodes that we think are offline
                let instance_reachability = self.instance_reachability.borrow();
                let is_learner = self.raw_node.raft.prs().conf().learners().contains(&msg.to);
                if !instance_reachability.should_send_heartbeat_this_tick(msg.to, is_learner) {
                    skip_count += 1;
                    continue;
                }
            }

            let mut msg = RaftMessageExt::new(msg, applied);

            if let Some(progress) = self.raw_node.raft.prs().get(msg.inner.to) {
                if progress.state == ProgressState::Snapshot {
                    msg.flags.insert(Flags::EXPECTING_SNAPSHOT_STATUS);
                }
            }

            if self.raw_node.raft.leader_id == INVALID_ID {
                msg.flags |= Flags::LEADER_UNKNOWN;
            }

            if let Err(e) = self.pool.send(msg, timeout) {
                tlog!(Error, "{e}");
            }
            sent_count += 1;
        }

        // tlog!(
        //     Debug,
        //     "done sending messages, sent: {sent_count}, skipped: {skip_count}"
        // );
        _ = sent_count;
        _ = skip_count;
    }

    fn maybe_send_snapshot_report(&mut self, have_applied_snapshot: bool) {
        let applied = self.applied.get();
        let from = self.raw_node.raft.id;

        if self.pending_raft_snapshot.is_some() {
            // We have received a raft snapshot but we can't apply it yet
            // because we're probably blocked waiting for tarantool replication
            // to progress. Snapshot status is not ready to report
            return;
        }

        let snapshot_report;
        if have_applied_snapshot {
            // We received the snapshot and successfully applied it.
            debug_assert!(self.report_snapshot_status_to.is_some());
            let mut to = self.raw_node.raft.leader_id;
            // in case we do not properly know who the leader is, send the message to the requester of the snapshot
            // they _probably_ are the leader
            if to == INVALID_ID {
                if let Some(status_requester) = self.report_snapshot_status_to {
                    to = status_requester;
                }
            }
            self.report_snapshot_status_to = None;
            snapshot_report =
                RaftMessageExt::snapshot_report(applied, from, to, Flags::SNAPSHOT_STATUS_SUCCESS);
        } else if let Some(status_requester) = self.report_snapshot_status_to {
            // We didn't apply a snapshot but the leader is expecting a report
            // from us. This can sometimes happen for example if the leader
            // sends us a raft snapshot but we crash before applying it. In this
            // case we report failure to apply snapshot. This is needed because
            // of how raft-rs works under the hood...
            let mut to = self.raw_node.raft.leader_id;
            // in case we do not properly know who the leader is, send the message to the requester of the snapshot
            // they _probably_ are the leader
            if to == INVALID_ID {
                to = status_requester;
            }
            self.report_snapshot_status_to = None;
            snapshot_report =
                RaftMessageExt::snapshot_report(applied, from, to, Flags::SNAPSHOT_STATUS_FAILURE);
        } else {
            // We didn't receive a snapshot and nobody expects a status report.
            return;
        }

        let timeout = self
            .alter_system_parameters
            .borrow()
            .governor_common_rpc_timeout();
        if let Err(e) = self.pool.send(snapshot_report, timeout) {
            tlog!(Error, "{e}");
        }
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

            let fut = self.pool.call(
                &leader_id,
                proc_name!(proc_raft_snapshot_next_chunk),
                &req,
                SNAPSHOT_CHUNK_REQUEST_TIMEOUT,
            );
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
                    if e.error_code() == ErrorCode::RaftSnapshotReadViewNotAvailable as u32 {
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

    /// Prepare for applying the raft snapshot if it's not empty.
    ///
    /// This includes:
    /// - Verifying snapshot version against global & local ones;
    /// - Waiting until tarantool replication proceeds if this is a read-only replica;
    /// - Fetching the rest of the snashot chunks if the first one is not the only one;
    fn prepare_for_snapshot(
        &mut self,
        new_snapshot: &raft::Snapshot,
    ) -> traft::Result<Option<RaftSnapshot>> {
        let mut update_pending_snapshot = false;
        if !new_snapshot.is_empty() {
            if let Some(old_snapshot) = &self.pending_raft_snapshot {
                update_pending_snapshot = old_snapshot.is_out_of_date_with(new_snapshot.metadata());
                crate::error_injection!("IGNORE_NEWER_SNAPSHOT" => { update_pending_snapshot = false; });
            } else {
                update_pending_snapshot = true;
            }
        }
        if update_pending_snapshot {
            let data = SnapshotData::decode(new_snapshot.data());
            let data = match data {
                Ok(v) => v,
                Err(e) => {
                    tlog!(
                        Warning,
                        "skipping snapshot, which failed to deserialize: {e}"
                    );
                    return Err(e.into());
                }
            };

            crate::error_injection!(exit "EXIT_BEFORE_APPLYING_RAFT_SNAPSHOT");
            crate::error_injection!(block "BLOCK_BEFORE_APPLYING_RAFT_SNAPSHOT");

            let pending_raft_snapshot = RaftSnapshot::new(new_snapshot.metadata().clone(), data);
            self.pending_raft_snapshot = Some(pending_raft_snapshot);
        }

        let Some(snapshot) = &self.pending_raft_snapshot else {
            return Ok(None);
        };

        let v_snapshot = snapshot.data.schema_version;

        let v_local = local_schema_version().expect("storage souldn't fail");
        let v_global = self
            .storage
            .properties
            .global_schema_version()
            .expect("storage shouldn't fail");

        #[rustfmt::skip]
        debug_assert!(v_global <= v_local, "global schema version is only ever increased after local");

        #[rustfmt::skip]
        debug_assert!(v_global <= v_snapshot, "global schema version updates are distributed via raft");

        // Replicaset master applies the schema changes directly.
        if self.is_readonly() && v_local < v_snapshot {
            // Replicaset follower must sync schema with the master,
            // before global space dumps could be handled.
            // NOTE: we would block here indefinitely in case
            // `proc_replication` RPC fails, see doc-comments in the
            // `rpc::replication` modules.

            tlog!(Debug, "v_local: {v_local}, v_snapshot: {v_snapshot}");
            self.main_loop_status("awaiting replication");
            // Replicaset follower needs to sync with leader via tarantool
            // replication.
            tlog!(Warning, "blocked by replicaset sync");
            return Err(BoxError::new(
                ErrorCode::LocalSchemaNotUpToDate,
                "blocked by replicaset sync",
            )
            .into());
        }

        let mut snapshot = self
            .pending_raft_snapshot
            .take()
            .expect("always present here");

        if snapshot.data.next_chunk_position.is_some() {
            self.main_loop_status("receiving snapshot");
            let entry_id = RaftEntryId {
                index: snapshot.metadata.index,
                term: snapshot.metadata.term,
            };
            if let Err(e) = self.fetch_chunkwise_snapshot(&mut snapshot.data, entry_id) {
                // Error has been logged.
                tlog!(Warning, "dropping snapshot data");
                return Err(e);
            }
        }

        Ok(Some(snapshot))
    }

    #[inline(always)]
    fn main_loop_status(&self, status: &'static str) {
        if self.status.get().main_loop_status == status {
            return;
        }

        tlog!(Debug, "main_loop_status = '{status}'");
        self.status
            .send_modify(|s| s.main_loop_status = status)
            .expect("status shouldn't ever be borrowed across yields");
    }

    #[inline]
    fn main_loop_status_persisting(
        &self,
        have_hard_state: bool,
        have_entries: bool,
        have_snapshot: bool,
    ) {
        match (have_hard_state, have_entries, have_snapshot) {
            (true, true, _) => self.main_loop_status("persisting hard state and entries"),
            (true, _, true) => self.main_loop_status("persisting hard state and snapshot"),
            (true, false, false) => self.main_loop_status("persisting hard state"),
            (false, true, _) => self.main_loop_status("persisting entries"),
            (false, _, true) => self.main_loop_status("persisting snapshot"),
            (..) => {
                // Raft-rs never gives us both entries and snapshot in the same `Ready` record.
                crate::warn_or_panic!(
                    "impossible case: {have_hard_state} {have_entries} {have_snapshot}"
                )
            }
        }
    }

    /// Processes a so-called "ready state" of the [`raft::RawNode`].
    ///
    /// This includes:
    /// - Sending messages to other instances (raft nodes);
    /// - Handling raft snapshot:
    ///   - Verifying & waiting until snapshot data can be applied
    ///     (see [`Self::prepare_for_snapshot`] for more details);
    ///   - Persisting snapshot metadata;
    ///   - Compacting the raft log;
    ///   - Restoring local storage contents from the snapshot;
    /// - Persisting uncommitted entries;
    /// - Persisting hard state (term, vote, commit);
    /// - Applying committed entries;
    /// - Notifying pending fibers;
    /// - Waking up the governor loop, so that it can handle any global state
    ///   changes;
    ///
    /// Returns an error if the instance was expelled from the cluster.
    ///
    /// See also:
    ///
    /// - <https://github.com/tikv/raft-rs/blob/v0.6.0/src/raw_node.rs#L85>
    /// - or better <https://github.com/etcd-io/etcd/blob/v3.5.5/raft/node.go#L49>
    ///
    /// This function yields.
    fn advance(&mut self) -> traft::Result<()> {
        // Handle any unreachable nodes from previous iteration.
        let unreachables = self
            .instance_reachability
            .borrow_mut()
            .take_unreachables_to_report(self.applied.get());
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
        if !self.raw_node.has_ready() &&
            // if we have unapplied state we can't do this early exit
            // otherwise DDL will get stuck until a heartbeat is received
            self.applied.get() == self.commit.get()
        {
            return Ok(());
        }

        let mut ready: raft::Ready = self.raw_node.ready();

        // Apply soft state changes before anything else, so that this info is
        // available for other fibers as soon as main loop yields.
        if let Some(ss) = ready.ss() {
            self.status
                .send_modify(|s| {
                    s.leader_id = (ss.leader_id != INVALID_ID).then_some(ss.leader_id);
                    s.raft_state = ss.raft_state.into();
                })
                .expect("status shouldn't ever be borrowed across yields");

            let leader: Option<u64> = (ss.leader_id != INVALID_ID).then_some(ss.leader_id);
            metrics::record_raft_leader_id(leader);
            metrics::record_raft_state(ss.raft_state);
        }

        // These messages are only available on leader. Send them out ASAP.
        self.handle_messages(ready.take_messages());

        // Handle read states before applying snapshot which may fail.
        self.handle_read_states(ready.read_states());

        // Raft snapshot has arrived, check if we need to apply it.
        let raw_snapshot = ready.snapshot();
        let res = self.prepare_for_snapshot(raw_snapshot);
        let snapshot = match res {
            Ok(v) => v,
            Err(e) => {
                if let Error::BoxError(ref e) = e {
                    if e.error_code() == ErrorCode::LocalSchemaNotUpToDate as u32 {
                        // We are blocked on the replicaset sync, so we can't apply the snapshot right away.
                        // Nevertheless, raft-rs has generated some messages that will notify master of the snapshot application.
                        // We should preserve those messages, as they will not be re-generated in a future `advance` call
                        //   which will be able to progress with snapshot application.
                        self.pending_persisted_messages = ready.take_persisted_messages();
                    }
                };

                // Error was already logged
                if let Some(hard_state) = ready.hs() {
                    // Cannot persist this hard state change, because the snapshot
                    // can't yet be applied, but we must update the `term` info
                    // so that other fibers find out if raft leader has changed.
                    // This is needed so that `proc_replication` doesn't stop
                    // working if raft leader changes while we're blocked waiting on
                    // tarantool replication.
                    self.status
                        .send_modify(|s| s.term = hard_state.term)
                        .expect("status shouldn't ever be borrowed across yields");
                }

                return Err(e);
            }
        };

        // Persist stuff raft wants us to persist.
        let hard_state = ready.hs();
        let entries_to_persist = ready.entries();

        let have_hard_state = hard_state.is_some();
        let have_entries = !entries_to_persist.is_empty();
        let have_snapshot = snapshot.is_some();
        if have_hard_state || have_entries || have_snapshot {
            self.main_loop_status_persisting(have_hard_state, have_entries, have_snapshot);

            let mut new_term = None;
            let mut new_commit = None;
            let mut new_applied = None;
            let mut received_snapshot = false;
            let mut changed_parameters = Vec::new();

            if let Err(e) = transaction(|| -> Result<(), Error> {
                // Raft HardState changed, and we need to persist it.
                if let Some(hard_state) = hard_state {
                    tlog!(Debug, "hard state: {hard_state:?}");
                    self.raft_storage.persist_hard_state(hard_state)?;
                    new_term = Some(hard_state.term);
                    new_commit = Some(hard_state.commit);
                }

                // Persist uncommitted entries in the raft log.
                if !entries_to_persist.is_empty() {
                    #[rustfmt::skip]
                    debug_assert!(raw_snapshot.is_empty(), "can't have both the snapshot & log entries");

                    self.raft_storage.persist_entries(entries_to_persist)?;
                }

                if let Some(snapshot) = &snapshot {
                    #[rustfmt::skip]
                    debug_assert!(entries_to_persist.is_empty(), "can't have both the snapshot & log entries");

                    let meta = &snapshot.metadata;
                    let applied_index = self.raft_storage.applied()?;
                    // If we receive a stale snapshot with snapshot_index less
                    // then our commit_index, then raft-rs will skip it for us.
                    // See code in raft::Raft::restore.
                    debug_assert!(applied_index <= meta.index);

                    // Persist snapshot metadata and compact raft log if it wasn't empty.
                    self.raft_storage.handle_snapshot_metadata(meta)?;

                    if applied_index == meta.index {
                        // Skip snapshot with the same index, as an optimization,
                        // because the contents of global tables are already up to date.
                        tlog!(
                            Warning,
                            "skipping snapshot with the same index: {applied_index}"
                        )
                    } else {
                        // Persist the contents of the global tables from the snapshot data.
                        let is_master = !self.is_readonly();
                        changed_parameters = self
                            .storage
                            .apply_snapshot_data(&snapshot.data, is_master)?;
                        new_applied = Some(meta.index);
                        received_snapshot = true;
                    }
                }

                Ok(())
            }) {
                tlog!(Warning, "dropping raft ready: {ready:#?}");
                panic!("transaction failed: {e}");
            }

            if let Some(new_term) = new_term {
                self.status
                    .send_modify(|s| s.term = new_term)
                    .expect("status shouldn't ever be borrowed across yields");
            }

            if let Some(new_commit) = new_commit {
                self.commit
                    .send(new_commit)
                    .expect("commit shouldn't ever be borrowed across yields");
            }

            if let Some(new_applied) = new_applied {
                // handle_snapshot_metadata persists applied index, so we update the watch channel
                self.applied
                    .send(new_applied)
                    .expect("applied shouldn't ever be borrowed across yields");
            }

            if received_snapshot {
                // Need to reload the whole topology cache. We could be more
                // clever about it and only update the records which changed,
                // but at the moment this would require doing a full scan and
                // comparing each record, which is no better than full reload.
                // A better solution would be to store a raft index in each
                // tuple of each global table, then we would only need to check
                // if the index changed, but we don't have that..
                self.topology_cache
                    .full_reload(&self.storage)
                    .expect("schema upgrade not supported yet");

                // we need to refresh instance_state metric after snapshot application
                self.topology_cache.with(|topology_ref| {
                    for instance in topology_ref.all_instances() {
                        metrics::record_instance_state(
                            &instance.tier,
                            &instance.name,
                            &instance.current_state.variant,
                        );
                    }
                });

                let current_tier = self.topology_cache.my_tier_name();

                // apply changed dynamic parameters
                for changed_parameter in changed_parameters {
                    apply_parameter(
                        &self.alter_system_parameters,
                        Tuple::try_from_slice(&changed_parameter)?,
                        current_tier,
                    )?;
                }
            }

            if hard_state.is_some() {
                crate::error_injection!(exit "EXIT_AFTER_RAFT_PERSISTS_HARD_STATE");
            }
            if !entries_to_persist.is_empty() {
                crate::error_injection!(exit "EXIT_AFTER_RAFT_PERSISTS_ENTRIES");
            }
        }

        crate::error_injection!("BLOCK_WHEN_PERSISTING_DDL_COMMIT" => {
            for entry in entries_to_persist {
                let row = traft::Entry::try_from(entry).unwrap();
                let op = row.into_op();
                if let Some(Op::DdlCommit) = op {
                    crate::error_injection!(block "BLOCK_WHEN_PERSISTING_DDL_COMMIT");
                }
            }
        });

        // Send a snapshot status report if snapshot was applied and or somebody is expecting a report
        self.maybe_send_snapshot_report(snapshot.is_some());

        // These messages are only available on followers. They must be sent only
        // AFTER the HardState, Entries and Snapshot are persisted
        // to the stable storage.
        let persisted_messages = {
            let mut msgs = ready.take_persisted_messages();
            // take care of sending out the preserved messages
            msgs.extend(std::mem::take(&mut self.pending_persisted_messages));
            msgs
        };
        self.handle_messages(persisted_messages);

        let committed_entries = ready.take_committed_entries();

        // Advance the Raft. Make it know, that the necessary entries have been persisted.
        // If this is a leader, it may commit some of the newly persisted entries.
        let mut light_rd = self.raw_node.advance_append(ready);

        // Apply committed entries.
        let res = self.handle_committed_entries(&committed_entries);
        let res = match res {
            Ok(v) => v,
            Err(e) => {
                // FIXME don't panic <https://git.picodata.io/core/picodata/-/issues/1149>
                panic!("transaction failed: {e}");
            }
        };

        if let Some(e) = res {
            return Err(e.into());
        }

        // Send new message ASAP. (Only on leader)
        let messages = light_rd.take_messages();
        if !messages.is_empty() {
            debug_assert!(self.raw_node.raft.state == RaftStateRole::Leader);

            self.handle_messages(messages);
        }

        // Update commit index. (Only on leader)
        if let Some(commit) = light_rd.commit_index() {
            debug_assert!(self.raw_node.raft.state == RaftStateRole::Leader);

            if let Err(e) = transaction(|| -> Result<(), Error> {
                self.main_loop_status("persisting commit index");

                self.raft_storage.persist_commit(commit)?;
                self.commit
                    .send(commit)
                    .expect("commit shouldn't ever be borrowed across yields");

                Ok(())
            }) {
                tlog!(Warning, "dropping raft light ready: {light_rd:#?}");
                // FIXME don't panic <https://git.picodata.io/core/picodata/-/issues/1149>
                panic!("transaction failed: {e}");
            }

            crate::error_injection!(block "BLOCK_AFTER_RAFT_PERSISTS_COMMIT_INDEX");
        }

        // Apply committed entries.
        // These are probably entries which we've just persisted.
        let committed_entries = light_rd.committed_entries();
        let res = self.handle_committed_entries(committed_entries);
        let res = match res {
            Ok(v) => v,
            Err(e) => {
                // FIXME don't panic <https://git.picodata.io/core/picodata/-/issues/1149>
                panic!("transaction failed: {e}");
            }
        };

        if let Some(e) = res {
            return Err(e.into());
        }

        self.main_loop_status("idle");

        Ok(())
    }

    /// Check if this is a read only replica. This function is called when we
    /// need to determine if this instance should be changing the schema
    /// definition or if it should instead synchronize with a master.
    ///
    /// Note: it would be a little more reliable to check if the replica is
    /// chosen to be a master by checking master_name in _pico_replicaset, but
    /// currently we cannot do that, because tarantool replication is being
    /// done asynchronously with raft log replication. Basically instance needs
    /// to know it's a replicaset master before it can access the replicaset
    /// info.
    fn is_readonly(&self) -> bool {
        let is_ro: bool = crate::tarantool::eval("return box.info.ro")
            .expect("checking read-onlyness should never fail");
        is_ro
    }

    /// Generates a pair of logical clock and a notification channel.
    /// Logical clock is a unique identifier suitable for tagging
    /// entries in raft log. Notification is broadcasted when the
    /// corresponding entry is committed.
    #[inline]
    fn schedule_read_state_waker(&mut self) -> (LogicalClock, oneshot::Receiver<RaftIndex>) {
        let (tx, rx) = oneshot::channel();
        let lc = {
            self.lc.inc();
            self.lc
        };
        self.read_state_wakers.insert(lc, tx);
        (lc, rx)
    }

    /// `old_last_index` is the value of `RaftSpaceAccess::last_index` at the
    /// start of the raft_main_loop iteration (i.e. before any new entries are
    /// applied).
    fn do_raft_log_auto_compaction(&self, old_last_index: RaftIndex) -> traft::Result<()> {
        let mut compaction_needed = false;

        let max_size = self.alter_system_parameters.borrow().raft_wal_size_max;
        let current_size = self.raft_storage.raft_log_bsize()?;
        if current_size > max_size {
            #[rustfmt::skip]
            tlog!(Debug, "raft log size exceeds threshold ({current_size} > {max_size})");
            compaction_needed = true;
        }

        let max_count = self.alter_system_parameters.borrow().raft_wal_count_max;
        let current_count = self.raft_storage.raft_log_count()?;
        if current_count > max_count {
            #[rustfmt::skip]
            tlog!(Debug, "raft log entry count exceeds threshold ({current_count} > {max_count})");
            compaction_needed = true;
        }

        if !compaction_needed {
            return Ok(());
        }

        let compact_until = self.get_adjusted_compaction_index(old_last_index)?;

        transaction(|| -> traft::Result<()> {
            self.main_loop_status("log auto compaction");
            self.raft_storage.compact_log(compact_until)?;

            Ok(())
        })?;

        Ok(())
    }

    /// Determines the point until which we should compact the raft log.
    /// Ideally this is the whole log, but there might be some entries we want to preserve.
    ///
    /// This function is called when we have determined that raft log needs to
    /// be compacted. It returns the value to be passed to [`RaftSpaceAccess::compact_log`]
    /// (i.e. the exclusive limit of compaction).
    fn get_adjusted_compaction_index(&self, old_last_index: RaftIndex) -> traft::Result<RaftIndex> {
        let last_applied = self.applied.get();
        if old_last_index >= last_applied {
            // Still have unapplied entries since last check, don't need to
            // check the newly added ones, as they haven't been applied yet.
            let compact_until = last_applied + 1;
            return Ok(compact_until);
        }

        let newly_added_entries =
            self.raft_storage
                .entries(old_last_index + 1, last_applied + 1, None)?;

        let mut last_ddl_finalizer = None;
        let mut last_plugin_op_finalizer = None;

        // Check if there's a finalizer (e.g. DdlCommit, Plugin::Abort, etc.)
        // operation among the newly added entries. The finalizers a relied upon
        // by some client code which waits for the operation to be completed and
        // reports errors if they occured. In some (admitedly rare) cases a
        // finalizer being added to the raft log may trigger log compaction,
        // which would almost certainly guarantee that the user is not going to
        // receive the report. Unless of course we attempt to not compact the
        // finalizers, which is what we're doing right here.
        // It must also be noted that we make an effort to minimize the
        // probability of this happening, but nevertheless we give no guarantee
        // as the compaction may still remove a confirmation awaited for by some
        // laggy client.
        for entry in newly_added_entries.into_iter().rev() {
            let index = entry.index;
            let Some(op) = entry.into_op() else {
                continue;
            };

            if last_ddl_finalizer.is_none() && op.is_ddl_finalizer() {
                last_ddl_finalizer = Some(index);
            }
            if last_plugin_op_finalizer.is_none() && op.is_plugin_op_finalizer() {
                last_plugin_op_finalizer = Some(index);
            }
            if last_ddl_finalizer.is_some() && last_plugin_op_finalizer.is_some() {
                // Everything else is always ok to compact
                break;
            }
        }

        // Add 1 because this entry is to be removed.
        let mut compact_until = last_applied + 1;
        if let Some(index) = last_ddl_finalizer {
            if compact_until > index {
                tlog!(Debug, "preserving ddl finalizer raft op at index {index}");
                compact_until = index;
            }
        }
        if let Some(index) = last_plugin_op_finalizer {
            if compact_until > index {
                #[rustfmt::skip]
                tlog!(Debug, "preserving plugin finalizer raft op at index {index}");
                compact_until = index;
            }
        }

        Ok(compact_until)
    }
}

/// Return value of [`NodeImpl::handle_committed_normal_entry`], explains what should be
/// done as result of attempting to apply a given entry.
#[derive(Debug, Clone)]
enum ApplyEntryResult {
    /// This entry failed to apply for some reason, and must be retried later.
    SleepAndRetry(BoxError),

    /// Entry applied to persistent storage successfully and should be
    /// applied to non-persistent outside of transaction, proceed to next entry.
    EntryApplied(Vec<AppliedDml>),
}

#[inline]
#[track_caller]
fn storage_corrupted(e: impl Into<String>) -> BoxError {
    BoxError::new(ErrorCode::StorageCorrupted, e)
}

#[inline]
#[track_caller]
fn read_only(e: impl Into<String>) -> BoxError {
    BoxError::new(TarantoolErrorCode::Readonly, e)
}

pub(crate) struct MainLoop {
    #[allow(dead_code)]
    fiber_id: fiber::FiberId,
    loop_waker: watch::Sender<()>,
}

struct MainLoopState {
    node_impl: Rc<Mutex<NodeImpl>>,
    next_tick: Instant,
    loop_waker: watch::Receiver<()>,
}

impl MainLoop {
    pub const TICK: Duration = Duration::from_millis(100);

    /// A base timeout before logging a repeating error message.
    const ERROR_LOG_BACKOFF_BASE_DURATION: Duration = Duration::from_secs(1);

    /// When doing an exponential backoff this is the maximum value.
    const ERROR_LOG_BACKOFF_MAX_DURATION: Duration = Duration::from_secs(10 * 60);

    fn start(node_impl: Rc<Mutex<NodeImpl>>) -> Self {
        let (loop_waker_tx, loop_waker_rx) = watch::channel(());

        let state = MainLoopState {
            node_impl,
            next_tick: Instant::now_fiber(),
            loop_waker: loop_waker_rx,
        };

        Self {
            fiber_id: loop_start!("raft_main_loop", Self::raft_main_loop, state),
            loop_waker: loop_waker_tx,
        }
    }

    /// Dummy struct instance for when we initialize Node for unit tests.
    fn for_tests() -> Self {
        let (loop_waker, _) = watch::channel(());
        Self {
            fiber_id: 0,
            loop_waker,
        }
    }

    #[inline(always)]
    pub fn wakeup(&self) {
        let _ = self.loop_waker.send(());
    }

    async fn raft_main_loop(state: &mut MainLoopState) -> ControlFlow<()> {
        let _ = state.loop_waker.changed().timeout(Self::TICK).await;

        // FIXME: potential deadlock - can't use sync mutex in async fn
        let mut node_impl = state.node_impl.lock(); // yields

        node_impl
            .read_state_wakers
            .retain(|_, waker| !waker.is_closed());

        let now = Instant::now_fiber();
        if now > state.next_tick {
            state.next_tick = now.saturating_add(Self::TICK);
            node_impl.raw_node.tick();
        }

        // Save the index of last entry before applying any changes.
        // This is used later when doing raft log auto-compaction.
        let res = node_impl.raft_storage.last_index();
        if let Err(e) = &res {
            tlog!(Error, "failed getting last index: {e}");
        }
        let old_last_index = res.ok();

        let res = node_impl.advance(); // yields
        if let Err(e) = res {
            node_impl
                .main_loop_info
                .borrow_mut()
                .log_error_with_backoff(e.into_box_error());
        }

        if let Some(old_last_index) = old_last_index {
            let res = node_impl.do_raft_log_auto_compaction(old_last_index);
            if let Err(e) = res {
                tlog!(Error, "raft log auto-compaction failed: {e}");
            }
        }

        if let Some(me) = node_impl.topology_cache.get().try_this_instance() {
            if has_states!(me, Expelled -> *) {
                tlog!(Critical, "current instance is expelled from the cluster");
                tlog!(Info, "expelled, shutting down");
                crate::tarantool::exit(1);
            }
        }

        drop(node_impl);

        ControlFlow::Continue(())
    }
}

/// A helper struct which contains info about raft_main_loop's inner workings
/// mostly for the purposes of debugging.
#[derive(Default, Debug)]
pub struct MainLoopInfo {
    /// Raft log entry which we tried applying last.
    pub last_entry: Option<traft::Entry>,

    /// Last error which happened during raft_main_loop iteration.
    pub last_error: Option<MainLoopErrorInfo>,
}

impl MainLoopInfo {
    /// Keeps track of the last error and logs it with the exponential backoff
    /// strategy.
    fn log_error_with_backoff(&mut self, error: BoxError) {
        let last_error = self.on_error(error);

        if last_error.ok_to_log_error() {
            let e = &last_error.error;
            let loc = DisplayErrorLocation(&last_error.error);

            // NOTE: we're only doing exponential backoff for logging. We're
            // still going to be trying to apply the entry with the same
            // unchanged frequency. This is ok in this case because applying
            // raft entries is a purely local operation, no RPC to other
            // instances will be sent so there's no possibilty of DOS
            // atacking someone by mistake. Beware and be warned!
            if last_error.error.error_code() == TarantoolErrorCode::Readonly as u32 {
                // We return this type of error when readonly replicas are
                // blocked expecting DDL results from masters via tarantool
                // replication. This is a completely expected situation, so
                // we log this as warnings so as to not scare admins too much.
                tlog!(Warning, "error during raft main loop iteration: {loc}{e}");
            } else {
                tlog!(Error, "error during raft main loop iteration: {loc}{e}");
            }
            last_error.on_log_error();
        }
    }

    /// Update backoff machinery according to the error case.
    ///
    /// Returns info about the last error.
    fn on_error(&mut self, e: BoxError) -> &mut MainLoopErrorInfo {
        if self.last_error.is_none() {
            return self.last_error.insert(MainLoopErrorInfo::start(e));
        }

        // XXX I tried writing a `let Some() else {};`, but borrow checker is
        // hallucinating multiple mutable borrows, so that's why we don't have
        // nice things...
        let last_error = self.last_error.as_mut().expect("just checked");

        if box_error_eq(&e, &last_error.error) {
            last_error.count += 1;
        } else {
            *last_error = MainLoopErrorInfo::start(e);
        }

        last_error
    }
}

/// A helper struct for keeping track of how often we log a given error message
/// for the purposes of exponential backoff.
#[derive(Debug)]
pub struct MainLoopErrorInfo {
    pub error: BoxError,
    pub start: Instant,
    pub count: u64,
    /// Time when it's ok to log the repeated error message.
    /// Used to implement exponential backoff.
    pub ok_to_log_at: Instant,
}

impl MainLoopErrorInfo {
    /// Creates a new error info with given error and current time.
    fn start(error: BoxError) -> Self {
        let start = fiber::clock();
        let ok_to_log_at = start.saturating_add(MainLoop::ERROR_LOG_BACKOFF_BASE_DURATION);
        Self {
            error,
            start,
            count: 1,
            ok_to_log_at,
        }
    }

    /// Returns `true` if exponential backoff machinery thinks that it's time
    /// to log the error again.
    #[inline]
    fn ok_to_log_error(&self) -> bool {
        fiber::clock() >= self.ok_to_log_at
    }

    /// Update backoff machinery after the error was logged
    #[inline]
    fn on_log_error(&mut self) {
        let since_start = self.ok_to_log_at.duration_since(self.start);
        let delay = since_start.min(MainLoop::ERROR_LOG_BACKOFF_MAX_DURATION);
        self.ok_to_log_at = self.ok_to_log_at.saturating_add(delay)
    }
}

#[inline(always)]
#[track_caller]
pub fn global() -> Result<&'static Node, BoxError> {
    // Uninitialized raft node is a regular case. This case may take
    // place while the instance is executing `start_discover()` function.
    // It has already started listening, but the node is only initialized
    // in `postjoin()`.
    // SAFETY:
    // - static mut access: safe because node is only accessed from main thread
    // - &'static to static mut: safe because it is never mutated after initialization
    //
    // Note that this is basically the behavior of std::cell::OnceLock, but we
    // can't use it because it doesn't implement Sync, and we don't want to use
    // std::sync::OnceLock, because we don't want to pay for the atomic read
    // which we don't need.
    let res = unsafe { static_ref!(const RAFT_NODE).as_deref() };
    let Some(node) = res else {
        let loc = std::panic::Location::caller();
        tlog!(
            Debug,
            "{}:{}: attempt to access raft node before it's initialized",
            loc.file(),
            loc.line()
        );
        return Err(BoxError::new(ErrorCode::Uninitialized, "uninitialized yet"));
    };

    Ok(node)
}

#[proc(packed_args)]
fn proc_raft_interact(data: RawByteBuf) -> traft::Result<()> {
    let node = global()?;

    crate::error_injection!("IGNORE_ALL_RAFT_MESSAGES" => return Ok(()));

    let msg = RaftMessageExt::decode(&data)?;

    node.instance_reachability
        .borrow_mut()
        .report_communication_result(
            msg.inner.from,
            true,
            Some(msg.applied),
            Some(msg.flags.contains(RaftMessageFlags::LEADER_UNKNOWN)),
        );

    node.step_and_yield(msg);

    Ok(())
}

/// Internal API. Causes this instance to artificially timeout on waiting
/// for a heartbeat from raft leader. The instance then will start a new
/// election and transition to a 'PreCandidate' state.
///
/// This function yields. It returns when the raft node changes it's state.
///
/// Later the instance will likely become a leader, unless there are some
/// impediments, e.g. the loss of quorum or split-vote.
///
/// Example log:
/// ```ignore
///     received MsgTimeoutNow from 3 and starts an election
///         to get leadership., from: 3, term: 4, raft_id: 3
///
///     starting a new election, term: 4, raft_id: 3
///
///     became candidate at term 5, term: 5, raft_id: 3
///
///     broadcasting vote request, to: [4, 1], log_index: 54,
///         log_term: 4, term: 5, type: MsgRequestVote, raft_id: 3
///
///     received votes response, term: 5, type: MsgRequestVoteResponse,
///         approvals: 2, rejections: 0, from: 4, vote: true, raft_id: 3
///
///     became leader at term 5, term: 5, raft_id: 3
/// ```
#[proc(public = false)]
fn proc_raft_promote() -> traft::Result<()> {
    let node = global()?;
    node.campaign_and_yield()?;
    Ok(())
}

fn do_audit_logging_for_instance_update(
    old: Option<&Instance>,
    new: &Instance,
    initiator_def: &UserMetadata,
) {
    // Check if we're handling a "new node joined" event:
    // * Either there's no tuple for this node in the storage;
    // * Or its raft id has changed, meaning it's no longer the same node.
    // WARN: this condition will not pass on the joining instance
    // as it preemptively puts itself into `_pico_instance` table.
    // Locally it's logged in src/lib.rs.
    if old.map(|x| x.raft_id) != Some(new.raft_id) {
        let instance_name = &new.name;
        crate::audit!(
            message: "a new instance `{instance_name}` joined the cluster",
            title: "join_instance",
            severity: Low,
            instance_name: %instance_name,
            raft_id: %new.raft_id,
            initiator: &initiator_def.name,
        );
        crate::audit!(
            message: "local database created on `{instance_name}`",
            title: "create_local_db",
            severity: Low,
            instance_name: %instance_name,
            raft_id: %new.raft_id,
            initiator: &initiator_def.name,
        );
    }

    if old.map(|x| x.current_state) != Some(new.current_state) {
        let instance_name = &new.name;
        let state = &new.current_state;
        crate::audit!(
            message: "current state of instance `{instance_name}` changed to {state}",
            title: "change_current_state",
            severity: Medium,
            instance_name: %instance_name,
            raft_id: %new.raft_id,
            new_state: %state,
            initiator: &initiator_def.name,
        );
    }

    if old.map(|x| x.target_state) != Some(new.target_state) {
        let instance_name = &new.name;
        let state = &new.target_state;
        crate::audit!(
            message: "target state of instance `{instance_name}` changed to {state}",
            title: "change_target_state",
            severity: Low,
            instance_name: %instance_name,
            raft_id: %new.raft_id,
            new_state: %state,
            initiator: &initiator_def.name,
        );
    }

    if has_states!(new, Expelled -> *) {
        let instance_name = &new.name;
        crate::audit!(
            message: "instance `{instance_name}` was expelled from the cluster",
            title: "expel_instance",
            severity: Low,
            instance_name: %instance_name,
            raft_id: %new.raft_id,
            initiator: &initiator_def.name,
        );
        crate::audit!(
            message: "local database dropped on `{instance_name}`",
            title: "drop_local_db",
            severity: Low,
            instance_name: %instance_name,
            raft_id: %new.raft_id,
            initiator: &initiator_def.name,
        );
    }
}
