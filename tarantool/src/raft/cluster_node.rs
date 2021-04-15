use std::cell::RefCell;
use std::collections::VecDeque;

use raft::prelude::Message;
use raft::{Config, RawNode};

use crate::error::Error;

use super::fsm::Command;

pub struct ClusterNodeState {
    node: RefCell<RawNode<raft::storage::MemStorage>>,
}

impl ClusterNodeState {
    pub fn new(id: u64) -> Result<Self, Error> {
        let raft_config = Config {
            id,
            ..Default::default()
        };
        let mut storage = raft::storage::MemStorage::new();
        let mut node = RawNode::with_default_logger(&raft_config, storage).unwrap();

        node.raft.become_candidate();

        Ok(Self {
            node: RefCell::new(node),
        })
    }

    pub fn tick(&self, send_queue: &mut VecDeque<Message>) {
        let mut node = self.node.borrow_mut();

        // tick the Raft node at regular intervals (see: `self.timeout`)
        // let elapsed = now.elapsed();
        // if elapsed >= remaining_timeout {
        //     remaining_timeout = self.timeout;
        //     self.raft_node.borrow_mut().tick();
        // } else {
        //     remaining_timeout -= elapsed;
        // }
        node.tick();

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
                    // match entry.get_entry_type() {
                    //     EntryType::EntryNormal => {
                    //         // handle_normal(entry)
                    //     }
                    //     EntryType::EntryConfChange => {
                    //         let mut cc = ConfChange::default();
                    //         cc.merge_from_bytes(&entry.data).unwrap();
                    //         let cs = node.apply_conf_change(&cc).unwrap();
                    //         node.mut_store().wl().set_conf_state(cs);
                    //     }
                    //     EntryType::EntryConfChangeV2 => unimplemented!(),
                    // }
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
