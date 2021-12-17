use slog::{debug, info, o};
use std::os::raw::c_int;
use ::tarantool::hlua;
use std::time::Duration;
mod tarantool;
use ::tarantool::fiber;

pub struct InnerTest {
    pub name: &'static str,
    pub body: fn(),
}
inventory::collect!(InnerTest);

use std::cell::RefCell;
use std::cell::RefMut;
use std::cell::Ref;
use std::rc::Rc;

use raft::prelude::*;

#[derive(Default)]
struct Stash {
    raft_node: Option<tarantool::RaftNode>,
    raft_loop: Option<fiber::LuaUnitJoinHandle>,
}

impl std::fmt::Debug for Stash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("raft_node", &self.raft_node.is_some())
            .field("raft_loop", &self.raft_loop.is_some())
            .finish()
    }
}

#[no_mangle]
pub extern "C" fn luaopen_picolib(l: *mut std::ffi::c_void) -> c_int {
    for (key, value) in std::env::vars() {
        if key.starts_with("PICODATA_") {
            println!("{}: {:?}", key, value);
        }
    }

    let command = std::env::var("PICODATA_COMMAND");
    match command.as_deref() {
        Ok("run") => {
            main_run();
        },
        Ok(_) => {},
        Err(_) => {},
    }

    unsafe {
        let l = hlua::Lua::from_existing_state(l, false);
        let luamod: hlua::LuaTable<_> = (&l).push(vec![()]).read().unwrap();
        luamod.set("VERSION", env!("CARGO_PKG_VERSION"));

        //
        // Export inner tests
        {
            let mut test = Vec::new();
            for t in inventory::iter::<InnerTest> {
                test.push((t.name, hlua::function0(t.body)));
            }
            luamod.set("test", test);
        }

        //
        // Export public API
        let stash: Rc<RefCell<Stash>> = Default::default();
        raft_init(&stash);
        {
            let stash = stash.clone();
            luamod.set("get_stash", hlua::function0(move || {get_stash(&stash)}));
        }
        {
            let stash = stash.clone();
            luamod.set("raft_propose", hlua::function1(move |x| {raft_propose(&stash, x)}));
        }

        use hlua::AsLua;
        (&l).push(&luamod).forget();
        1
    }
}

fn main_run() {
    let mut cfg = tarantool::Cfg {
        listen: None,
        ..Default::default()
    };

    std::env::var("PICODATA_DATA_DIR").ok().and_then(|v| {
        std::fs::create_dir_all(&v).unwrap();
        cfg.wal_dir = v.clone();
        cfg.memtx_dir = v.clone();
        Some(v)
    });

    tarantool::set_cfg(&cfg);
    tarantool::eval(
        r#"
        box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
        box.schema.space.create('raft_log', {
            if_not_exists = true,
            is_local = true,
            format = {
                {name = 'raft_index', type = 'unsigned', is_nullable = false},
                {name = 'raft_term', type = 'unsigned', is_nullable = false},
                {name = 'raft_id', type = 'unsigned', is_nullable = false},
                {name = 'command', type = 'string', is_nullable = false},
                {name = 'data', type = 'any', is_nullable = false},
            }
        })
        box.space.raft_log:create_index('pk', {
            if_not_exists = true,
            parts = {{'raft_index'}},
        })
        box.cfg({log_level = 6})
    "#,
    );

    std::env::var("PICODATA_LISTEN").ok().and_then(|v| {
        cfg.listen = Some(v.clone());
        Some(v)
    });

    tarantool::set_cfg(&cfg);

    let logger = slog::Logger::root(tarantool::SlogDrain, o!());

    info!(logger, "Hello, Rust!"; "module" => std::module_path!());
    debug!(
        logger,
        "Picodata running on {} {}",
        tarantool::package(),
        tarantool::version()
    );

    // raft_main();
}

fn get_stash(stash: &Rc<RefCell<Stash>>) {
    let stash: Ref<Stash> = stash.borrow();
    println!("{:?}", stash);
}

// A simple example about how to use the Raft library in Rust.
fn raft_init(stash: &Rc<RefCell<Stash>>) {
    // Create a storage for Raft, and here we just use a simple memory storage.
    // You need to build your own persistent storage in your production.
    // Please check the Storage trait in src/storage.rs to see how to implement one.
    let storage = tarantool::NodeStorage::new();

    let logger = slog::Logger::root(tarantool::SlogDrain, o!());

    // Create the configuration for the Raft node.
    let cfg = Config {
        // The unique ID for the Raft node.
        id: 1,
        // Election tick is for how long the follower may campaign again after
        // it doesn't receive any message from the leader.
        election_tick: 10,
        // Heartbeat tick is for how long the leader needs to send
        // a heartbeat to keep alive.
        heartbeat_tick: 3,
        // The max size limits the max size of each appended message. Mostly, 1 MB is enough.
        max_size_per_msg: 1024 * 1024 * 1024,
        // Max inflight msgs that the leader sends messages to follower without
        // receiving ACKs.
        max_inflight_msgs: 256,
        // The Raft applied index.
        // You need to save your applied index when you apply the committed Raft logs.
        applied: 0,
        ..Default::default()
    };

    // Create the Raft node.
    let r = tarantool::RaftNode::new(&cfg, storage, &logger).unwrap();
    stash.borrow_mut().raft_node = Some(r);

    let loop_fn = {
        let stash = stash.clone();
        move || {
            let logger = slog::Logger::root(tarantool::SlogDrain, o!());
            loop {
                fiber::sleep(Duration::from_millis(100));
                let mut stash: RefMut<Stash> = stash.borrow_mut();
                let mut raft_node = stash.raft_node.as_mut().unwrap();
                raft_node.tick();
                on_ready(&mut raft_node, &logger);
            }
        }
    };

    stash.borrow_mut().raft_loop = Some(fiber::defer_proc(loop_fn));
}

fn on_ready(
    raft_group: &mut tarantool::RaftNode,
    logger: &slog::Logger,
) {
    if !raft_group.has_ready() {
        return;
    }
    let store = raft_group.raft.raft_log.store.0.clone();

    info!(logger, "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");

    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready: Ready = raft_group.ready();
    info!(logger, "--- {:?}", ready);

    let handle_messages = |msgs: Vec<Message>| {
        for _msg in msgs {
            info!(logger, "--- handle message: {:?}", _msg);
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
        info!(logger, "--- apply_snapshot: {:?}", snap);
        store.wl().apply_snapshot(snap).unwrap();
    }

    let mut _last_apply_index = 0;
    let mut handle_committed_entries = |committed_entries: Vec<Entry>| {
        for entry in committed_entries {
            info!(logger, "--- committed_entry: {:?}", entry);
            // Mostly, you need to save the last apply index to resume applying
            // after restart. Here we just ignore this because we use a Memory storage.
            _last_apply_index = entry.index;

            if entry.data.is_empty() {
                // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                continue;
            }

            if entry.get_entry_type() == EntryType::EntryNormal {
                // let key = entry.data.get(0).unwrap();
                // if let Some(value) = cbs.remove(key) {
                // }
            }

            // TODO: handle EntryConfChange
        }
    };
    handle_committed_entries(ready.take_committed_entries());

    if !ready.entries().is_empty() {
        // Append entries to the Raft log.
        let entries = ready.entries();
        use serde::{Deserialize, Serialize};
        use ::tarantool::space::Space;
        use ::tarantool::tuple::{AsTuple, FunctionArgs, FunctionCtx};

        let mut space = Space::find("raft_log").unwrap();

        #[derive(Serialize, Deserialize)]
        struct Row {
            pub raft_index: u64,
            pub raft_term: u64,
            pub raft_id: u64,
            pub command: String,
            pub data: Vec<u8>,
        }

        impl Row {
            fn from(e: &Entry) -> Self {
                Self {
                    raft_index: e.get_index(),
                    raft_term: e.get_term(),
                    raft_id: 1,
                    command: format!("{:?}", e.get_entry_type()),
                    data: Vec::from(e.get_data()),
                }
            }
        }

        impl AsTuple for Row {}

        for entry in entries {
            space.insert(&Row::from(entry)).unwrap();
            info!(logger, "--- uncommitted_entry: {:?}", entry);
        }


        store.wl().append(entries).unwrap();
    }

    if let Some(hs) = ready.hs() {
        // Raft HardState changed, and we need to persist it.
        let hs = hs.clone();
        info!(logger, "--- hard_state: {:?}", hs);
        store.wl().set_hardstate(hs);
    }

    if !ready.persisted_messages().is_empty() {
        // Send out the persisted messages come from the node.
        handle_messages(ready.take_persisted_messages());
    }

    info!(logger, "ADVANCE -----------------------------------------");

    // Advance the Raft.
    let mut light_rd = raft_group.advance(ready);
    // Update commit index.
    if let Some(commit) = light_rd.commit_index() {
        store.wl().mut_hard_state().set_commit(commit);
    }
    // Send out the messages.
    handle_messages(light_rd.take_messages());
    // Apply all committed entries.
    handle_committed_entries(light_rd.take_committed_entries());
    // Advance the apply index.
    raft_group.advance_apply();
    info!(logger, "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
}

#[no_mangle]
fn raft_propose(stash: &Rc<RefCell<Stash>>, key: u8) {
    let mut stash: RefMut<Stash> = stash.borrow_mut();
    let raft_node = stash.raft_node.as_mut().unwrap();
    let logger = slog::Logger::root(tarantool::SlogDrain, o!());
    info!(logger, "propose {} .......................................", key);
    raft_node.propose(vec![], vec![key]).unwrap();
    info!(logger, ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,");
}
