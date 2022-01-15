use slog::{debug, info, o, error};
use std::os::raw::c_int;
use ::tarantool::tlua;
use rmp_serde;
use serde::{Deserialize, Serialize};

mod tarantool;
mod traft;

pub struct InnerTest {
    pub name: &'static str,
    pub body: fn(),
}
inventory::collect!(InnerTest);

use std::cell::RefCell;
use std::cell::RefMut;
use std::cell::Ref;
use std::convert::{TryFrom, TryInto};
use std::rc::Rc;

#[derive(Default)]
struct Stash {
    raft_node: Option<traft::Node>,
}

impl std::fmt::Debug for Stash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("raft_node", &self.raft_node.is_some())
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

    let stash: Rc<RefCell<Stash>> = Default::default();

    let command = std::env::var("PICODATA_COMMAND");
    match command.as_deref() {
        Ok("run") => {
            main_run(&stash);
        },
        Ok(_) => {},
        Err(_) => {},
    }

    unsafe {
        let l = tlua::Lua::from_existing_state(l, false);
        let luamod: tlua::LuaTable<_> = (&l).push(vec![()]).read().unwrap();
        luamod.set("VERSION", env!("CARGO_PKG_VERSION"));

        //
        // Export inner tests
        {
            let mut test = Vec::new();
            for t in inventory::iter::<InnerTest> {
                test.push((t.name, tlua::function0(t.body)));
            }
            luamod.set("test", test);
        }

        //
        // Export public API
        {
            let stash = stash.clone();
            luamod.set("get_stash", tlua::function0(move || get_stash(&stash)));
        }
        {
            let stash = stash.clone();
            luamod.set(
                "raft_test_propose",
                tlua::function1(move |x: String|
                    raft_propose(&stash, RaftEntryData::Info(format!("{}", x)))),
            );
        }
        {
            let stash = stash.clone();
            luamod.set(
                "broadcast_lua_eval",
                tlua::function1(
                    move |x: String| {
                        raft_propose(&stash, RaftEntryData::EvalLua(x))
                    }
                )
            )
        }
        {
            l.exec(r#"
                function inspect()
                    return
                        {raft_log = box.space.raft_log:fselect()},
                        {raft_state = box.space.raft_state:fselect()}
                end
            "#).unwrap();
        }

        use tlua::AsLua;
        (&l).push(&luamod).forget();
        1
    }
}

fn main_run(stash: &Rc<RefCell<Stash>>) {
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

    traft::Storage::init_schema();
    let raft_cfg = traft::Config {
        id: 1,
        applied: traft::Storage::applied().unwrap_or_default(),
        ..Default::default()
    };
    let mut node = traft::Node::new(&raft_cfg).unwrap();
    node.start(handle_committed_data);
    stash.borrow_mut().raft_node = Some(node);

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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RaftEntryData {
    Info(String),
    EvalLua(String),
}

impl TryFrom<&[u8]> for RaftEntryData {
    type Error = rmp_serde::decode::Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        rmp_serde::from_read_ref(data)
    }
}

impl From<RaftEntryData> for Vec<u8> {
    fn from(data: RaftEntryData) -> Self {
        let mut ser = rmp_serde::Serializer::new(Vec::new())
            .with_struct_map()
            .with_string_variants();
        data.serialize(&mut ser).unwrap();
        ser.into_inner()
    }
}

#[no_mangle]
fn raft_propose(stash: &Rc<RefCell<Stash>>, entry_data: RaftEntryData) {
    let mut stash: RefMut<Stash> = stash.borrow_mut();
    let raft_node = stash.raft_node.as_mut().unwrap();
    let logger = slog::Logger::root(tarantool::SlogDrain, o!());
    let data: Vec<u8> = entry_data.into();
    info!(
        logger,
        "propose binary data ({} bytes).......................................",
        data.len()
    );
    raft_node.borrow_mut().propose(vec![], data).unwrap();
    info!(logger, ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,");
}

fn handle_committed_data(logger: &slog::Logger, data: &[u8]) {
    use RaftEntryData::*;

    match data.try_into() {
        Ok(x) => match x {
            EvalLua(code) => crate::tarantool::eval(&code),
            Info(msg) => info!(logger, "{}", msg),
        }
        Err(why) => error!(logger, "cannot decode raft entry data: {}", why),
    }
}


