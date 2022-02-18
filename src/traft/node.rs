use ::raft::prelude as raft;
use ::raft::Error as RaftError;
use ::tarantool::fiber;
use ::tarantool::util::IntoClones;
use std::collections::HashMap;
use std::convert::TryFrom;

use std::time::Duration;
use std::time::Instant;

use crate::tlog;
use crate::traft::ConnectionPool;
use crate::traft::LogicalClock;
use crate::traft::Storage;

type RawNode = raft::RawNode<Storage>;
type Notify = fiber::Channel<()>;

pub struct Node {
    _main_loop: fiber::LuaUnitJoinHandle<'static>,
    inbox: fiber::Channel<Request>,
}

#[derive(Clone, Debug)]
enum Request {
    Propose { data: Vec<u8> },
    ProposeWaitApplied { data: Vec<u8>, notify: Notify },
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

    pub fn propose(&self, data: impl Into<Vec<u8>>) {
        let req = Request::Propose { data: data.into() };
        self.inbox.send(req).unwrap();
    }

    pub fn propose_wait_applied(&self, data: impl Into<Vec<u8>>, timeout: Duration) -> bool {
        let (rx, tx) = fiber::Channel::new(1).into_clones();
        let now = Instant::now();

        let req = Request::ProposeWaitApplied {
            data: data.into(),
            notify: tx,
        };

        match self.inbox.send_timeout(req, timeout) {
            Err(fiber::SendError::Disconnected(_)) => unreachable!(),
            Err(fiber::SendError::Timeout(_)) => {
                rx.close();
                return false;
            }
            Ok(()) => (),
        }

        match rx.recv_timeout(timeout.saturating_sub(now.elapsed())) {
            Err(_) => {
                rx.close();
                false
            }
            Ok(()) => true,
        }
    }

    pub fn step(&self, msg: raft::Message) {
        let req = Request::Step(msg);
        self.inbox.send(req).unwrap();
    }
}

fn raft_main(inbox: fiber::Channel<Request>, mut raw_node: RawNode, on_commit: fn(&[u8])) {
    let mut next_tick = Instant::now() + Node::TICK;
    let mut pool = ConnectionPool::with_timeout(Node::TICK * 4);

    // This is a temporary hack until fair joining is implemented
    for peer in Storage::peers().unwrap() {
        pool.connect(peer.raft_id, &peer.uri);
    }

    let mut notifications: HashMap<LogicalClock, Notify> = HashMap::new();
    let mut lc = {
        let id = Storage::id().unwrap().unwrap();
        let gen = Storage::gen().unwrap().unwrap_or(0) + 1;
        Storage::persist_gen(gen).unwrap();
        LogicalClock::new(id, gen)
    };

    loop {
        // Clean up obsolete notifications
        notifications.retain(|_, notify: &mut Notify| !notify.is_closed());

        match inbox.recv_timeout(Node::TICK) {
            Ok(Request::Propose { data }) => {
                if let Err(e) = raw_node.propose(vec![], data) {
                    tlog!(Error, "{e}");
                }
            }
            Ok(Request::ProposeWaitApplied { data, notify }) => {
                lc.inc();
                if let Err(e) = raw_node.propose(Vec::from(&lc), data) {
                    tlog!(Error, "{e}");
                    notify.close();
                } else {
                    notifications.insert(lc.clone(), notify);
                }
            }
            Ok(Request::Step(msg)) => {
                if let Err(e) = raw_node.step(msg) {
                    tlog!(Error, "{e}");
                }
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
            for msg in msgs {
                if let Err(e) = pool.send(&msg) {
                    tlog!(Error, "{e}");
                }
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

        let mut handle_committed_entries = |committed_entries: Vec<raft::Entry>| {
            for entry in committed_entries {
                Storage::persist_applied(entry.index).unwrap();

                if entry.get_entry_type() == raft::EntryType::EntryNormal {
                    on_commit(entry.get_data());
                    if let Ok(lc) = LogicalClock::try_from(entry.get_context()) {
                        if let Some(notify) = notifications.remove(&lc) {
                            notify.try_send(()).ok();
                        }
                    }
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
