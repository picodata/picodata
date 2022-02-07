use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::tarantool::fiber;
use ::tarantool::util::IntoClones;

use std::time::Duration;
use std::time::Instant;

use crate::tlog;
use crate::traft::Storage;

type RawNode = raft::RawNode<Storage>;

pub struct Node {
    _main_loop: fiber::LuaUnitJoinHandle,
    inbox: fiber::Channel<Request>,
}

#[derive(Clone, Debug)]
enum Request {
    Propose(Vec<u8>),
    Step(raft::Message),
}

impl Node {
    pub const TICK: Duration = Duration::from_millis(100);

    pub fn new(cfg: &raft::Config, handle_committed_data: fn(&[u8])) -> Result<Self, RaftError> {
        let logger = tlog::root();
        let mut raw_node = RawNode::new(cfg, Storage, &logger)?;

        let (tx, rx) = fiber::Channel::new(0).into_clones();

        let loop_fn = move || {
            let mut next_tick = Instant::now() + Self::TICK;

            loop {
                use Request::*;
                match rx.recv_timeout(Self::TICK) {
                    Ok(Propose(data)) => {
                        raw_node.propose(vec![], data).unwrap();
                    }
                    Ok(Step(msg)) => {
                        raw_node.step(msg).unwrap();
                    }
                    Err(fiber::RecvError::Timeout) => (),
                    Err(fiber::RecvError::Disconnected) => unreachable!(),
                }

                let now = Instant::now();
                if now > next_tick {
                    next_tick = now + Self::TICK;
                    raw_node.tick();
                }

                on_ready(&mut raw_node, handle_committed_data);
            }
        };

        Ok(Node {
            _main_loop: fiber::defer_proc(loop_fn),
            inbox: tx,
        })
    }

    pub fn propose<T: Into<Vec<u8>>>(&self, data: T) {
        let req = Request::Propose(data.into());
        self.inbox.send(req).unwrap();
    }

    pub fn step(&self, msg: raft::Message) {
        let req = Request::Step(msg);
        self.inbox.send(req).unwrap();
    }
}

fn on_ready(raft_group: &mut RawNode, handle_committed_data: fn(&[u8])) {
    if !raft_group.has_ready() {
        return;
    }

    tlog!(Debug, "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");

    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready: raft::Ready = raft_group.ready();
    tlog!(Debug, "--- {:?}", ready);

    let handle_messages = |msgs: Vec<raft::Message>| {
        for _msg in msgs {
            tlog!(Debug, "--- handle message: {:?}", _msg);
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
        tlog!(Debug, "--- apply_snapshot: {:?}", snap);
        unimplemented!();
        // store.wl().apply_snapshot(snap).unwrap();
    }

    let handle_committed_entries = |committed_entries: Vec<raft::Entry>| {
        for entry in committed_entries {
            tlog!(Debug, "--- committed_entry: {:?}", entry);
            Storage::persist_applied(entry.index);

            if entry.get_entry_type() == raft::EntryType::EntryNormal {
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
            tlog!(Debug, "--- uncommitted_entry: {:?}", entry);
        }

        Storage::persist_entries(entries);
    }

    if let Some(hs) = ready.hs() {
        // Raft HardState changed, and we need to persist it.
        // let hs = hs.clone();
        tlog!(Debug, "--- hard_state: {:?}", hs);
        Storage::persist_hard_state(hs);
        // store.wl().set_hardstate(hs);
    }

    if !ready.persisted_messages().is_empty() {
        // Send out the persisted messages come from the node.
        handle_messages(ready.take_persisted_messages());
    }

    tlog!(Debug, "ADVANCE -----------------------------------------");

    // Advance the Raft.
    let mut light_rd = raft_group.advance(ready);
    tlog!(Debug, "--- {:?}", light_rd);
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
    tlog!(Debug, "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
}
