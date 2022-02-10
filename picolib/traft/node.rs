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

    pub fn new(cfg: &raft::Config, on_commit: fn(&[u8])) -> Result<Self, RaftError> {
        let raw_node = RawNode::new(cfg, Storage, &tlog::root())?;
        let (inbox, inbox_clone) = fiber::Channel::new(0).into_clones();
        let loop_fn = move || raft_main(inbox_clone, raw_node, on_commit);

        Ok(Node {
            inbox,
            _main_loop: fiber::defer_proc(loop_fn),
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

fn raft_main(inbox: fiber::Channel<Request>, mut raw_node: RawNode, on_commit: fn(&[u8])) {
    let mut next_tick = Instant::now() + Node::TICK;

    loop {
        match inbox.recv_timeout(Node::TICK) {
            Ok(Request::Propose(data)) => {
                raw_node.propose(vec![], data).unwrap();
            }
            Ok(Request::Step(msg)) => {
                raw_node.step(msg).unwrap();
            }
            Err(fiber::RecvError::Timeout) => (),
            Err(fiber::RecvError::Disconnected) => unreachable!(),
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

        let handle_messages = |msgs: Vec<raft::Message>| {
            for _msg in msgs {
                // Send messages to other peers.
            }
        };

        if !ready.messages().is_empty() {
            // Send out the messages come from the node.
            handle_messages(ready.take_messages());
        }

        if !ready.snapshot().is_empty() {
            // This is a snapshot, we need to apply the snapshot at first.
            unimplemented!();
        }

        let handle_committed_entries = |committed_entries: Vec<raft::Entry>| {
            for entry in committed_entries {
                Storage::persist_applied(entry.index).unwrap();

                if entry.get_entry_type() == raft::EntryType::EntryNormal {
                    on_commit(entry.get_data())
                }

                // TODO: handle EntryConfChange
            }
        };

        handle_committed_entries(ready.take_committed_entries());

        if !ready.entries().is_empty() {
            // Append entries to the Raft log.
            Storage::persist_entries(ready.entries()).unwrap();
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            // let hs = hs.clone();
            Storage::persist_hard_state(hs).unwrap();
        }

        if !ready.persisted_messages().is_empty() {
            // Send out the persisted messages come from the node.
            handle_messages(ready.take_persisted_messages());
        }

        // Advance the Raft.
        let mut light_rd = raw_node.advance(ready);

        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            Storage::persist_commit(commit).unwrap();
        }

        // Send out the messages.
        handle_messages(light_rd.take_messages());

        // Apply all committed entries.
        handle_committed_entries(light_rd.take_committed_entries());

        // Advance the apply index.
        raw_node.advance_apply();
    }
}
