use slog::{debug, info, o};
use std::os::raw::c_int;
use ::tarantool::tlua;
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
            luamod.set("raft_propose", tlua::function1(move |x| raft_propose(&stash, x)));
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
    let mut node = traft::Node::new(&traft::Config::new(1)).unwrap();
    node.start();
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

#[no_mangle]
fn raft_propose(stash: &Rc<RefCell<Stash>>, key: u8) {
    let mut stash: RefMut<Stash> = stash.borrow_mut();
    let raft_node = stash.raft_node.as_mut().unwrap();
    let logger = slog::Logger::root(tarantool::SlogDrain, o!());
    info!(logger, "propose {} .......................................", key);
    raft_node.borrow_mut().propose(vec![], vec![key]).unwrap();
    info!(logger, ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,");
}
