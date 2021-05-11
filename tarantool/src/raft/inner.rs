use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use protobuf::Message as _;
use raft::prelude::{ConfChange, EntryType, Message};
use raft::{Config, RawNode};

use crate::error::Error;
use crate::fiber::Cond;
use crate::raft::NodeOptions;

use super::fsm::Command;

pub struct NodeInner {
    node: RawNode<raft::storage::MemStorage>,
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

impl NodeInner {
    pub fn new(id: u64, options: &NodeOptions) -> Result<Self, Error> {
        let raft_config = Config {
            id,
            ..Default::default()
        };
        let storage = raft::storage::MemStorage::new();
        let node = RawNode::with_default_logger(&raft_config, storage)?;

        Ok(Self {
            node,
            timeout: options.tick_interval,
            remaining_timeout: Cell::new(options.tick_interval),
            recv_queue: RefCell::new(VecDeque::with_capacity(options.recv_queue_size)),
            recv_cond: Cond::new(),
        })
    }

    pub fn init(&mut self, peers: Vec<u64>, become_leader: bool) -> Result<(), Error> {
        let node = &mut self.node;
        for id in peers {
            let mut conf_change = ConfChange::default();
            conf_change.node_id = id;
            conf_change.set_change_type(raft::eraftpb::ConfChangeType::AddNode);
            node.apply_conf_change(&conf_change)?;
        }

        if become_leader {
            node.raft.become_candidate();
            node.raft.become_leader();
        }

        Ok(())
    }

    pub fn step(&mut self, send_queue: &mut VecDeque<Message>) -> Result<(), Error> {
        let now = Instant::now();
        let node = &mut self.node;

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
                RecvMessage::RaftMsg(msg) => node.step(msg)?,
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
                store.wl().apply_snapshot(snapshot.clone())?;
            }

            // append entries to the Raft log
            let entries = ready.entries();
            if !entries.is_empty() {
                store.wl().append(entries)?;
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
                            conf_change.merge_from_bytes(&entry.data)?;

                            let conf_state = node.apply_conf_change(&conf_change)?;
                            node.mut_store().wl().set_conf_state(conf_state);
                        }
                        _ => {}
                    }
                }
            }

            // advance the apply index.
            node.advance_apply();
        }

        Ok(())
    }

    pub fn add_entry(&self, command: Command) {
        todo!()
    }

    pub fn handle_msg(&self, msg: Message) {
        self.recv_queue
            .borrow_mut()
            .push_back(RecvMessage::RaftMsg(msg));
        self.recv_cond.signal();
    }
}
