use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use protobuf::Message as _;
use raft::prelude::{ConfChange, EntryType, Message};
use raft::{Config, RawNode};

use crate::error::Error;
use crate::fiber::Cond;

use super::fsm::Command;

pub struct ClusterNodeState {
    node: RefCell<RawNode<raft::storage::MemStorage>>,
    timeout: Duration,
    remaining_timeout: Cell<Duration>,
    recv_queue: RefCell<VecDeque<RecvMessage>>,
    recv_cond: Cond,
}

enum RecvMessage {
    Propose(Command),
    RaftMsg(Message),
    Conf(ConfChange),
}

impl ClusterNodeState {
    pub fn new(id: u64, peers: Vec<u64>, is_leader: bool) -> Result<Self, Error> {
        let raft_config = Config {
            id,
            ..Default::default()
        };
        let mut storage = raft::storage::MemStorage::new();
        let mut node = RawNode::with_default_logger(&raft_config, storage).unwrap();

        for id in peers {
            let mut conf_change = ConfChange::default();
            conf_change.node_id = id;
            conf_change.set_change_type(raft::eraftpb::ConfChangeType::AddNode);
            node.apply_conf_change(&conf_change).unwrap();
        }

        if is_leader {
            node.raft.become_candidate();
            node.raft.become_leader();
        }

        Ok(Self {
            node: RefCell::new(node),
            timeout: Duration::from_millis(100),
            remaining_timeout: Cell::new(Duration::from_millis(300)),
            recv_queue: RefCell::new(VecDeque::new()),
            recv_cond: Cond::new(),
        })
    }

    pub fn step(&self, send_queue: &mut VecDeque<Message>) {
        let now = Instant::now();
        let mut node = self.node.borrow_mut();

        // block if recv queue is empty (wait t <= remaining_timeout)
        if self.recv_queue.borrow().is_empty() {
            self.recv_cond.wait_timeout(self.remaining_timeout.get());
        }

        // dispatch next message from queue
        if let Some(msg) = self.recv_queue.borrow_mut().pop_front() {
            match msg {
                RecvMessage::Propose(_) => {
                    todo!();
                }
                RecvMessage::RaftMsg(msg) => node.step(msg).unwrap(),
                RecvMessage::Conf(_) => {}
            }
        }

        // tick the Raft node at regular intervals (see: `self.timeout`)
        let elapsed = now.elapsed();
        self.remaining_timeout
            .set(match self.remaining_timeout.get().checked_sub(elapsed) {
                None => {
                    node.tick();
                    self.timeout
                }
                Some(remaining_timeout) => remaining_timeout,
            });

        if node.has_ready() {
            let mut ready = node.ready();
            let store = node.mut_store();

            // if this is a snapshot: we need to apply the snapshot at first
            let snapshot = ready.snapshot();
            if !snapshot.is_empty() {
                store.wl().apply_snapshot(snapshot.clone()).unwrap();
            }

            // append entries to the Raft log
            let entries = ready.entries();
            if !entries.is_empty() {
                store.wl().append(entries).unwrap();
            }

            // if Raft hard-state changed: we need to persist it
            if let Some(hs) = ready.hs() {
                store.wl().set_hardstate(hs.clone());
            }

            for msgs in ready.take_messages() {
                send_queue.extend(msgs);
            }

            // advance the Raft.
            let mut light_ready = node.advance(ready);

            for msgs in light_ready.take_messages() {
                send_queue.extend(msgs);
            }

            // if newly committed log entries are available: apply to the state machine
            let committed_entries = light_ready.take_committed_entries();
            if !committed_entries.is_empty() {
                for entry in committed_entries {
                    if entry.get_data().is_empty() {
                        // when the peer becomes Leader it will send an empty entry
                        continue;
                    }
                    match entry.get_entry_type() {
                        EntryType::EntryNormal => {}
                        EntryType::EntryConfChange => {
                            let mut conf_change = ConfChange::default();
                            conf_change.merge_from_bytes(&entry.data).unwrap();

                            let conf_state = node.apply_conf_change(&conf_change).unwrap();
                            node.mut_store().wl().set_conf_state(conf_state);
                        }
                        EntryType::EntryConfChangeV2 => unimplemented!(),
                    }
                }
            }

            // advance the apply index.
            node.advance_apply();
        }
    }

    pub fn add_entry(&self, command: Command) {
        todo!()
    }

    pub fn handle_msg(&self, msg: Message) {
        todo!()
    }
}
