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
use ::tarantool::proc;
use ::tarantool::tlua;
use ::tarantool::transaction::start_transaction;
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::error::Error as StdError;
use std::rc::Rc;
use std::time::Duration;
use std::time::Instant;
use thiserror::Error;

use crate::traft::ContextCoercion as _;
use ::tarantool::util::IntoClones as _;
use protobuf::Message as _;
use protobuf::ProtobufEnum as _;

use crate::mailbox::Mailbox;
use crate::tlog;
use crate::traft;
use crate::traft::ConnectionPool;
use crate::traft::LogicalClock;
use crate::traft::Storage;
use crate::traft::Topology;
use crate::traft::{JoinRequest, JoinResponse};

type RawNode = raft::RawNode<Storage>;
type Notify = fiber::Channel<Result<u64, RaftError>>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("uninitialized yet")]
    Uninitialized,
    #[error("timeout")]
    Timeout,
    #[error("{0}")]
    Raft(#[from] RaftError),
    /// cluster_id of the joining peer mismatches the cluster_id of the cluster
    #[error("cannot join the instance to the cluster: cluster_id mismatch: cluster_id of the instance = {instance_cluster_id:?}, cluster_id of the cluster = {cluster_cluster_id:?}")]
    ClusterIdMismatch {
        instance_cluster_id: String,
        cluster_cluster_id: String,
    },
}

#[derive(Clone, Debug, tlua::Push, tlua::PushInto)]
pub struct Status {
    /// `raft_id` of the current instance
    pub id: u64,
    /// `raft_id` of the leader instance
    pub leader_id: Option<u64>,
    pub raft_state: String,
    pub is_ready: bool,
}

/// The heart of `traft` module - the Node.
#[derive(Debug)]
pub struct Node {
    _main_loop: fiber::UnitJoinHandle<'static>,
    _join_loop: fiber::UnitJoinHandle<'static>,
    main_inbox: Mailbox<NormalRequest>,
    join_inbox: Mailbox<(JoinRequest, Notify)>,
    status: Rc<RefCell<Status>>,
    status_cond: Rc<fiber::Cond>,
}

/// A request to the raft main loop.
#[derive(Clone, Debug)]
enum NormalRequest {
    /// Propose `raft::prelude::Entry` of `EntryNormal` kind.
    /// Make a notification when it's committed.
    ProposeNormal { op: traft::Op, notify: Notify },

    /// Propose `raft::prelude::Entry` of `EntryConfChange` kind.
    /// Make a notification when the second EntryConfChange is
    /// committed (that corresponds to the leaving of a joint state).
    ProposeConfChange {
        term: u64,
        peers: Vec<traft::Peer>,
        notify: Notify,
    },

    /// Get a read barrier. In some systems it's also called the "quorum read".
    /// Make a notification when index is read.
    ReadIndex { notify: Notify },

    /// Start a new raft term .
    /// Make a notification when request is processed.
    Campaign { notify: Notify },

    /// Handle message from anoher raft node.
    Step(raft::Message),

    /// Tick the node.
    /// Make a notification when request is processed.
    Tick { n_times: u32, notify: Notify },
}

impl Node {
    pub const TICK: Duration = Duration::from_millis(100);

    pub fn new(cfg: &raft::Config) -> Result<Self, RaftError> {
        let status_cond = Rc::new(fiber::Cond::new());
        let main_inbox = Mailbox::<NormalRequest>::new();
        let join_inbox = Mailbox::<(JoinRequest, Notify)>::new();
        let raw_node = RawNode::new(cfg, Storage, &tlog::root())?;
        let status = Rc::new(RefCell::new(Status {
            id: cfg.id,
            leader_id: None,
            raft_state: "Follower".into(),
            is_ready: false,
        }));

        let main_loop_fn = {
            let status = status.clone();
            let status_cond = status_cond.clone();
            let main_inbox = main_inbox.clone();
            move || raft_main_loop(main_inbox, status, status_cond, raw_node)
        };

        let join_loop_fn = {
            let main_inbox = main_inbox.clone();
            let join_inbox = join_inbox.clone();
            move || raft_join_loop(join_inbox, main_inbox)
        };

        let node = Node {
            main_inbox,
            join_inbox,
            status,
            status_cond,
            _main_loop: fiber::Builder::new()
                .name("raft_main_loop")
                .proc(main_loop_fn)
                .start()
                .unwrap(),
            _join_loop: fiber::Builder::new()
                .name("raft_join_loop")
                .proc(join_loop_fn)
                .start()
                .unwrap(),
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
        self.status_cond.broadcast();
    }

    pub fn wait_status(&self) {
        self.status_cond.wait();
    }

    pub fn read_index(&self, timeout: Duration) -> Result<u64, Error> {
        let (rx, tx) = Notify::new(1).into_clones();

        self.main_inbox
            .send(NormalRequest::ReadIndex { notify: tx });

        match rx.recv_timeout(timeout) {
            Ok(v) => v.map_err(|e| e.into()),
            Err(_) => {
                rx.close();
                Err(Error::Timeout)
            }
        }
    }

    pub fn propose(&self, op: traft::Op, timeout: Duration) -> Result<u64, Error> {
        let (rx, tx) = fiber::Channel::new(1).into_clones();

        self.main_inbox
            .send(NormalRequest::ProposeNormal { op, notify: tx });

        match rx.recv_timeout(timeout) {
            Ok(v) => v.map_err(|e| e.into()),
            Err(_) => {
                rx.close();
                Err(Error::Timeout)
            }
        }
    }

    pub fn campaign(&self) {
        let (rx, tx) = fiber::Channel::new(1).into_clones();
        let req = NormalRequest::Campaign { notify: tx };
        self.main_inbox.send(req);
        if let Some(Err(e)) = rx.recv() {
            tlog!(Error, "{e}");
        }
    }

    pub fn step(&self, msg: raft::Message) {
        let req = NormalRequest::Step(msg);
        self.main_inbox.send(req);
    }

    pub fn tick(&self, n_times: u32) {
        let (rx, tx) = fiber::Channel::new(1).into_clones();
        let req = NormalRequest::Tick {
            n_times,
            notify: tx,
        };
        self.main_inbox.send(req);
        rx.recv();
    }

    pub fn timeout_now(&self) {
        self.step(raft::Message {
            msg_type: raft::MessageType::MsgTimeoutNow,
            ..Default::default()
        })
    }

    pub fn join_one(&self, req: JoinRequest) -> Result<u64, RaftError> {
        let (rx, tx) = fiber::Channel::new(1).into_clones();

        self.join_inbox.send((req, tx));
        rx.recv().expect("that's a bug")
    }
}

fn raft_main_loop(
    main_inbox: Mailbox<NormalRequest>,
    status: Rc<RefCell<Status>>,
    status_cond: Rc<fiber::Cond>,
    mut raw_node: RawNode,
) {
    let mut next_tick = Instant::now() + Node::TICK;
    let mut pool = ConnectionPool::builder()
        .handler_name(".raft_interact")
        .call_timeout(Node::TICK * 4)
        .connect_timeout(Node::TICK * 4)
        .inactivity_timeout(Duration::from_secs(60))
        .build();

    for peer in Storage::peers().unwrap() {
        pool.connect(peer.raft_id, peer.peer_address);
    }

    let mut notifications: HashMap<LogicalClock, Notify> = HashMap::new();
    let mut lc = {
        let id = Storage::id().unwrap().unwrap();
        let gen = Storage::gen().unwrap().unwrap_or(0) + 1;
        Storage::persist_gen(gen).unwrap();
        LogicalClock::new(id, gen)
    };

    let mut joint_state_latch: Option<JointStateLatch> = None;

    struct JointStateLatch {
        index: u64,
        notify: Notify,
    }

    loop {
        // Clean up obsolete notifications
        notifications.retain(|_, notify: &mut Notify| !notify.is_closed());

        for req in main_inbox.receive_all(Node::TICK) {
            match req {
                NormalRequest::ProposeNormal { op, notify } => {
                    lc.inc();

                    let ctx = traft::EntryContextNormal { lc: lc.clone(), op }.to_bytes();
                    if let Err(e) = raw_node.propose(ctx, vec![]) {
                        notify.try_send(Err(e)).expect("that's a bug");
                    } else {
                        notifications.insert(lc.clone(), notify);
                    }
                }
                NormalRequest::ProposeConfChange {
                    term,
                    peers,
                    notify,
                } => {
                    // In some states proposing a ConfChange is impossible.
                    // Check if there's a reason to reject it.

                    #[allow(clippy::never_loop)]
                    let reason: Option<&str> = loop {
                        // Raft-rs allows proposing ConfChange from any node, but it may cause
                        // inconsistent raft_id generation. In picodata only the leader is
                        // permitted to step a ProposeConfChange message.
                        let status = raw_node.status();
                        if status.ss.raft_state != RaftStateRole::Leader {
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
                        let e = RaftError::ConfChangeError(e.into());
                        notify.try_send(Err(e)).expect("that's a bug");
                        continue;
                    }

                    let mut changes = Vec::with_capacity(peers.len());
                    for peer in &peers {
                        let change_type = match peer.voter {
                            true => raft::ConfChangeType::AddNode,
                            false => raft::ConfChangeType::AddLearnerNode,
                        };
                        changes.push(raft::ConfChangeSingle {
                            change_type,
                            node_id: peer.raft_id,
                            ..Default::default()
                        });
                    }

                    let cc = raft::ConfChangeV2 {
                        changes: changes.into(),
                        transition: raft::ConfChangeTransition::Implicit,
                        ..Default::default()
                    };

                    let ctx = traft::EntryContextConfChange { peers }.to_bytes();

                    let prev_index = raw_node.raft.raft_log.last_index();
                    if let Err(e) = raw_node.propose_conf_change(ctx, cc) {
                        notify.try_send(Err(e)).expect("that's a bug");
                    } else {
                        // oops, current instance isn't actually a leader
                        // (which is impossible in theory, but we're not
                        // sure in practice) and sent the ConfChange message
                        // to the raft network instead of appending it to the
                        // raft log.
                        let last_index = raw_node.raft.raft_log.last_index();
                        assert!(last_index == prev_index + 1);

                        joint_state_latch = Some(JointStateLatch {
                            index: last_index,
                            notify,
                        });
                    }
                }
                NormalRequest::ReadIndex { notify } => {
                    lc.inc();

                    // read_index puts this context into an Entry,
                    // so we've got to compose full EntryContext,
                    // despite single LogicalClock would be enough
                    let ctx = traft::EntryContextNormal {
                        lc: lc.clone(),
                        op: traft::Op::Nop,
                    }
                    .to_bytes();
                    raw_node.read_index(ctx);
                    notifications.insert(lc.clone(), notify);
                }
                NormalRequest::Campaign { notify } => {
                    let res = raw_node.campaign().map(|_| 0);
                    notify.try_send(res).expect("that's a bug");
                }
                NormalRequest::Step(msg) => {
                    if let Err(e) = raw_node.step(msg) {
                        tlog!(Error, "{e}");
                    }
                }
                NormalRequest::Tick { n_times, notify } => {
                    for _ in 0..n_times {
                        raw_node.tick();
                    }
                    notify.try_send(Ok(0)).expect("that's a bug");
                }
            }
        }

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

        fn handle_read_states(
            read_states: Vec<raft::ReadState>,
            notifications: &mut HashMap<LogicalClock, Notify>,
        ) {
            for rs in read_states {
                if let Some(ctx) = traft::EntryContextNormal::read_from_bytes(&rs.request_ctx)
                    .expect("Abnormal entry in message context")
                {
                    if let Some(notify) = notifications.remove(&ctx.lc) {
                        notify.try_send(Ok(rs.index)).ok();
                    }
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

        fn handle_committed_entries(
            entries: Vec<raft::Entry>,
            notifications: &mut HashMap<LogicalClock, Notify>,
            raw_node: &mut RawNode,
            pool: &mut ConnectionPool,
            joint_state_latch: &mut Option<JointStateLatch>,
        ) {
            for entry in entries
                .iter()
                .map(|e| traft::Entry::try_from(e).expect("wtf"))
            {
                match raft::EntryType::from_i32(entry.entry_type) {
                    Some(raft::EntryType::EntryNormal) => {
                        use traft::Op::*;

                        match entry.op() {
                            None => (),
                            Some(Nop) => (),
                            Some(Info { msg }) => tlog!(Info, "{msg}"),
                            Some(EvalLua { code }) => crate::tarantool::eval(code),
                        }

                        if let Some(lc) = entry.lc() {
                            if let Some(notify) = notifications.remove(lc) {
                                // The notification may already have timed out.
                                // Don't panic. Just ignore `try_send` error.
                                notify.try_send(Ok(entry.index)).ok();
                            }
                        }

                        if let Some(latch) = joint_state_latch {
                            if entry.index == latch.index {
                                // It was expected to be a ConfChange entry, but it's
                                // normal. Raft must have overriden it, or there was
                                // a re-election.
                                let e = RaftError::ConfChangeError("ignored".into());

                                // The `raft_join_loop` waits forever and never closes
                                // the notification channel. Panic if `try_send` fails.
                                latch.notify.try_send(Err(e)).expect("that's a bug");
                            }
                            *joint_state_latch = None;
                        }
                    }
                    Some(entry_type @ raft::EntryType::EntryConfChange)
                    | Some(entry_type @ raft::EntryType::EntryConfChangeV2) => {
                        for peer in entry.iter_peers() {
                            let peer = traft::Peer {
                                commit_index: entry.index,
                                ..peer.clone()
                            };
                            Storage::persist_peer(&peer).unwrap();
                            pool.connect(peer.raft_id, peer.peer_address);
                        }

                        let cs = match entry_type {
                            // Beware: this tiny difference in type names
                            // (`V2` or not `V2`) makes a significant
                            // difference in `entry.data` binary layout
                            // and in joint state transitions.
                            raft::EntryType::EntryConfChange => {
                                let mut cc = raft::ConfChange::default();
                                cc.merge_from_bytes(&entry.data).unwrap();

                                raw_node.apply_conf_change(&cc).unwrap()
                            }
                            raft::EntryType::EntryConfChangeV2 => {
                                let mut cc = raft::ConfChangeV2::default();
                                cc.merge_from_bytes(&entry.data).unwrap();

                                // Unlock the latch only when leaving the joint state
                                if cc.changes.is_empty() {
                                    if let Some(latch) = joint_state_latch {
                                        latch
                                            .notify
                                            .try_send(Ok(entry.index))
                                            .expect("that's a bug");
                                        *joint_state_latch = None;
                                    }
                                }

                                // ConfChangeTransition::Implicit implies that at this
                                // moment raft-rs will implicitly propose another empty
                                // conf change that represents leaving the joint state.
                                raw_node.apply_conf_change(&cc).unwrap()
                            }
                            _ => unreachable!(),
                        };

                        Storage::persist_conf_state(&cs).unwrap();
                    }
                    None => unreachable!(),
                }

                Storage::persist_applied(entry.index).unwrap();
            }
        }

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
                &mut notifications,
                &mut raw_node,
                &mut pool,
                &mut joint_state_latch,
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
                status.leader_id = match ss.leader_id {
                    INVALID_ID => None,
                    id => Some(id),
                };
                status.raft_state = format!("{:?}", ss.raft_state);
                status_cond.broadcast();
            }

            if !ready.persisted_messages().is_empty() {
                // Send out the persisted messages come from the node.
                let messages = ready.take_persisted_messages();
                handle_messages(messages, &pool);
            }

            let read_states = ready.take_read_states();
            handle_read_states(read_states, &mut notifications);

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
                &mut notifications,
                &mut raw_node,
                &mut pool,
                &mut joint_state_latch,
            );

            // Advance the apply index.
            raw_node.advance_apply();
            Ok(())
        })
        .unwrap();
    }
}

fn raft_join_loop(inbox: Mailbox<(JoinRequest, Notify)>, main_inbox: Mailbox<NormalRequest>) {
    loop {
        let batch = inbox.receive_all(Duration::MAX);
        let ids: Vec<_> = batch.iter().map(|(req, _)| &req.instance_id).collect();
        tlog!(Info, "processing batch: {ids:?}");

        let term = Storage::term().unwrap().unwrap_or(0);
        let mut topology = match Storage::peers() {
            Ok(v) => Topology::from_peers(v).with_replication_factor(2),
            Err(e) => {
                for (_, notify) in batch {
                    let e = RaftError::ConfChangeError(format!("{e}"));
                    notify.try_send(Err(e)).ok();
                }
                continue;
            }
        };

        for (req, notify) in &batch {
            if let Err(e) = topology.process(req) {
                let e = RaftError::ConfChangeError(e);
                notify.try_send(Err(e)).expect("that's a bug");
            }
        }

        let (rx, tx) = fiber::Channel::new(1).into_clones();
        main_inbox.send(NormalRequest::ProposeConfChange {
            term,
            peers: topology.diff(),
            notify: tx,
        });

        // main_loop gives the warranty that every ProposeConfChange
        // will sometimes be handled and there's no need in timeout.
        // It also guarantees that the notification will arrive only
        // after the node leaves the joint state.
        let res = rx.recv().expect("that's a bug");
        tlog!(Info, "batch processed: {ids:?}, {res:?}");
        for (_, notify) in batch {
            // RaftError doesn't implement the Clone trait,
            // so we have to be creative.
            match &res {
                Ok(v) => notify.try_send(Ok(*v)).ok(),
                Err(e) => {
                    let e = RaftError::ConfChangeError(format!("{e}"));
                    notify.try_send(Err(e)).ok()
                }
            };
        }
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
fn raft_interact(pbs: Vec<traft::MessagePb>) -> Result<(), Box<dyn StdError>> {
    let node = global()?;
    for pb in pbs {
        node.step(raft::Message::try_from(pb)?);
    }
    Ok(())
}

#[proc(packed_args)]
fn raft_join(req: JoinRequest) -> Result<JoinResponse, Box<dyn StdError>> {
    let node = global()?;

    let cluster_id = Storage::cluster_id()?.ok_or("cluster_id is not set yet")?;

    if req.cluster_id != cluster_id {
        return Err(Box::new(Error::ClusterIdMismatch {
            instance_cluster_id: req.cluster_id,
            cluster_cluster_id: cluster_id,
        }));
    }

    let instance_id = req.instance_id.clone();
    node.join_one(req)?;

    let peer = Storage::peer_by_instance_id(&instance_id)?
        .ok_or("the peer has misteriously disappeared")?;
    let raft_group = Storage::peers()?;
    let box_replication = Storage::box_replication(&peer)?;

    Ok(JoinResponse {
        peer,
        raft_group,
        box_replication,
    })
}
