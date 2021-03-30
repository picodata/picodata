use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

use raft::prelude::EntryType;
use raft::{is_empty_snap, Config, RawNode};

pub use fsm::Command;
use fsm::Fsm;
use protocol::{Protocol, Queue, QueueMsg};
pub use rpc::{init_rpc, raft_rpc_proc};
use storage::NodeStorage;

mod fsm;
mod protocol;
mod rpc;
mod storage;

pub struct Node {
    is_active: Cell<bool>,
    timeout: Duration,
    raft_node: RefCell<RawNode<NodeStorage>>,
    fsm: RefCell<Fsm>,
    protocol: Protocol,
    queue: Rc<Queue>,
}

pub struct NodeOptions {}

impl Node {
    pub fn bootstrap(bootstrap_addrs: Vec<&str>, options: NodeOptions) -> Node {
        let raft_config = Config {
            id: 1,
            peers: vec![1, 2],
            ..Default::default()
        };
        let timeout = Duration::from_millis(100);

        let storage = NodeStorage::new();
        let raft_node = RawNode::new(&raft_config, storage, vec![]).unwrap();

        Self {
            is_active: Cell::new(true),
            timeout,
            raft_node: RefCell::new(raft_node),
            fsm: RefCell::new(Fsm::new()),
            protocol: Protocol::new(),
            queue: Rc::new(Queue::new()),
        }
    }

    pub fn add_entry(&self, command: Command) {
        self.queue.send(QueueMsg::Propose(command))
    }

    pub fn start(&self) {
        let mut remaining_timeout = self.timeout;
        loop {
            if !self.is_active.get() {
                break;
            }

            let now = Instant::now();
            match self.queue.recv(remaining_timeout) {
                Some(QueueMsg::Propose(cmd)) => {
                    let serialized_cmd = rmp_serde::encode::to_vec(&cmd).unwrap();
                    self.raft_node
                        .borrow_mut()
                        .propose(vec![], serialized_cmd)
                        .expect("failed to propose FSM command");
                }
                Some(QueueMsg::Raft(raft_msg)) => {
                    self.raft_node
                        .borrow_mut()
                        .step(raft_msg)
                        .expect("failed to perform Raft step");
                }
                None => (),
            };

            // tick the Raft node at regular intervals (see: `self.timeout`)
            let elapsed = now.elapsed();
            if elapsed >= remaining_timeout {
                remaining_timeout = self.timeout;
                self.raft_node.borrow_mut().tick();
            } else {
                remaining_timeout -= elapsed;
            }

            if self.raft_node.borrow().has_ready() {
                let mut raft_node = self.raft_node.borrow_mut();
                let mut ready = raft_node.ready();

                // if this is a snapshot: we need to apply the snapshot at first
                if !is_empty_snap(ready.snapshot()) {
                    raft_node
                        .mut_store()
                        .apply_snapshot(ready.snapshot().clone())
                        .unwrap();
                }

                // append entries to the Raft log
                if !ready.entries().is_empty() {
                    raft_node.mut_store().append(ready.entries()).unwrap();
                }

                // if Raft hard-state changed: we need to persist it
                if let Some(hs) = ready.hs() {
                    raft_node.mut_store().set_hardstate(hs.clone()).unwrap();
                }

                let msgs = ready.messages.drain(..);
                for msg in msgs {
                    self.protocol.send(msg);
                }

                // if newly committed log entries are available: apply to the state machine
                if let Some(committed_entries) = ready.committed_entries.take() {
                    for entry in committed_entries {
                        // when the peer becomes leader it will send an empty entry
                        if entry.get_data().is_empty() {
                            continue;
                        }

                        let entry_index = entry.get_index();
                        match entry.get_entry_type() {
                            EntryType::EntryNormal => self.fsm.borrow_mut().handle_normal(entry),
                            EntryType::EntryConfChange => {
                                self.fsm.borrow_mut().handle_conf_change(entry)
                            }
                        }

                        raft_node
                            .mut_store()
                            .set_last_apply_index(entry_index)
                            .unwrap();
                    }
                }

                raft_node.advance(ready);
            }
        }
    }

    pub fn stop(&self) {
        self.is_active.set(false);
    }
}
