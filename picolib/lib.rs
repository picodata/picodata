use ::tarantool::tlua;
use std::os::raw::c_int;

mod message;
mod tarantool;
mod tlog;
mod traft;

pub struct InnerTest {
    pub name: &'static str,
    pub body: fn(),
}
inventory::collect!(InnerTest);

use message::Message;
use std::cell::Ref;
use std::cell::RefCell;
use std::cell::RefMut;
use std::convert::TryFrom;
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
    let stash: Rc<RefCell<Stash>> = Default::default();

    if std::env::var("PICOLIB_NO_AUTORUN").is_ok() {
        // Skip box.cfg and other initialization
        // Used mostly for testing purposes
    } else {
        main_run(&stash);
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
            luamod.set("run", tlua::function0(move || main_run(&stash)));
        }
        {
            let stash = stash.clone();
            luamod.set("get_stash", tlua::function0(move || get_stash(&stash)));
        }
        {
            let stash = stash.clone();
            luamod.set(
                "raft_test_propose",
                tlua::function1(move |x: String| raft_propose(&stash, Message::Info { msg: x })),
            );
        }
        {
            let stash = stash.clone();
            luamod.set(
                "broadcast_lua_eval",
                tlua::function1(move |x: String| {
                    raft_propose(&stash, Message::EvalLua { code: x })
                }),
            )
        }
        {
            l.exec(
                r#"
                function inspect()
                    return
                        {raft_log = box.space.raft_log:fselect()},
                        {raft_state = box.space.raft_state:fselect()}
                end
            "#,
            )
            .unwrap();
        }

        use tlua::AsLua;
        (&l).push(&luamod).forget();
        1
    }
}

fn main_run(stash: &Rc<RefCell<Stash>>) {
    if tarantool::cfg().is_some() {
        // Already initialized
        return;
    }

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

    tlog!(Info, "Hello, Rust!"; "module" => std::module_path!());
    tlog!(
        Debug,
        "Picodata running on {} {}",
        tarantool::package(),
        tarantool::version()
    );
}

fn get_stash(stash: &Rc<RefCell<Stash>>) {
    let stash: Ref<Stash> = stash.borrow();
    println!("{:?}", stash);
}

#[no_mangle]
fn raft_propose(stash: &Rc<RefCell<Stash>>, msg: Message) {
    let mut stash: RefMut<Stash> = stash.borrow_mut();
    let raft_node = stash.raft_node.as_mut().unwrap();
    let data: Vec<u8> = msg.into();
    tlog!(
        Info,
        "propose binary data ({} bytes).......................................",
        data.len()
    );
    raft_node.borrow_mut().propose(vec![], data).unwrap();
    tlog!(Info, ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,");
}

fn handle_committed_data(data: &[u8]) {
    use Message::*;

    match Message::try_from(data) {
        Ok(x) => match x {
            EvalLua { code } => crate::tarantool::eval(&code),
            Info { msg } => tlog!(Info, "{}", msg),
            Empty => {}
        },
        Err(why) => tlog!(Error, "cannot decode raft entry data: {}", why),
    }
}
