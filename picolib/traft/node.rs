use raft::prelude::*;
use raft::Error as RaftError;
use std::ops::{Deref, DerefMut};

use std::cell::RefCell;
use std::rc::Rc;

use std::time::Duration;

use super::storage::Storage;
use crate::tlog;
use ::tarantool::fiber;

// pub type Node = RawNode<Storage>;
type RawNode = raft::RawNode<Storage>;

pub struct Node {
    raw_node: Rc<RefCell<RawNode>>,
    main_loop: Option<fiber::LuaUnitJoinHandle>,
}

impl Node {
    pub fn new(cfg: &raft::Config) -> Result<Self, RaftError> {
        let logger = tlog::root();
        let raw_node = RawNode::new(cfg, Storage, &logger)?;
        let raw_node = Rc::from(RefCell::from(raw_node));
        let ret = Node {
            raw_node,
            main_loop: None,
        };
        Ok(ret)
    }

    pub fn start(&mut self, handle_committed_data: fn(&[u8])) {
        assert!(self.main_loop.is_none(), "Raft loop is already started");

        let raw_node = self.raw_node.clone();
        let loop_fn = move || {
            loop {
                fiber::sleep(Duration::from_millis(100));
                // let mut stash: RefMut<Stash> = stash.borrow_mut();
                // let mut raft_node = stash.raft_node.as_mut().unwrap();
                let mut raw_node = raw_node.borrow_mut();
                raw_node.tick();
                on_ready(&mut raw_node, handle_committed_data);
            }
        };

        self.main_loop = Some(fiber::defer_proc(loop_fn));
    }
}

impl Deref for Node {
    type Target = Rc<RefCell<RawNode>>;

    fn deref(&self) -> &Self::Target {
        &self.raw_node
    }
}

impl DerefMut for Node {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.raw_node
    }
}

fn on_ready(raft_group: &mut RawNode, handle_committed_data: fn(&[u8])) {
    if !raft_group.has_ready() {
        return;
    }

    tlog!(Info, "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");

    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready: raft::Ready = raft_group.ready();
    tlog!(Info, "--- {:?}", ready);

    let handle_messages = |msgs: Vec<Message>| {
        for _msg in msgs {
            tlog!(Info, "--- handle message: {:?}", _msg);
            // Send messages to other peers.
        }
    };

    if !ready.messages().is_empty() {
        // Send out the messages come from the node.
        handle_messages(ready.take_messages());
    }

    if !ready.snapshot().is_empty() {
        // This is a snapshot, we need to apply the snapshot at first.
        let snap = ready.snapshot().clone();
        tlog!(Info, "--- apply_snapshot: {:?}", snap);
        unimplemented!();
        // store.wl().apply_snapshot(snap).unwrap();
    }

    let handle_committed_entries = |committed_entries: Vec<Entry>| {
        for entry in committed_entries {
            tlog!(Info, "--- committed_entry: {:?}", entry);
            Storage::persist_applied(entry.index);

            if entry.get_entry_type() == EntryType::EntryNormal {
                handle_committed_data(entry.get_data())
            }

            // TODO: handle EntryConfChange
        }
    };
    handle_committed_entries(ready.take_committed_entries());

    if !ready.entries().is_empty() {
        // Append entries to the Raft log.
        let entries = ready.entries();
        for entry in entries {
            tlog!(Info, "--- uncommitted_entry: {:?}", entry);
        }

        Storage::persist_entries(entries);
    }

    if let Some(hs) = ready.hs() {
        // Raft HardState changed, and we need to persist it.
        // let hs = hs.clone();
        tlog!(Info, "--- hard_state: {:?}", hs);
        Storage::persist_hard_state(&hs);
        // store.wl().set_hardstate(hs);
    }

    if !ready.persisted_messages().is_empty() {
        // Send out the persisted messages come from the node.
        handle_messages(ready.take_persisted_messages());
    }

    tlog!(Info, "ADVANCE -----------------------------------------");

    // Advance the Raft.
    let mut light_rd = raft_group.advance(ready);
    tlog!(Info, "--- {:?}", light_rd);
    // Update commit index.
    if let Some(commit) = light_rd.commit_index() {
        Storage::persist_commit(commit);
    }
    // Send out the messages.
    handle_messages(light_rd.take_messages());
    // Apply all committed entries.
    handle_committed_entries(light_rd.take_committed_entries());
    // Advance the apply index.
    raft_group.advance_apply();
    tlog!(Info, "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
}
