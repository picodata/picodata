use ::tarantool::tlua;
use std::os::raw::c_int;

mod message;
pub mod stash;
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
use std::convert::TryFrom;

#[derive(Default)]
pub struct Stash {
    raft_node: RefCell<Option<traft::Node>>,
}

impl Stash {
    pub fn access() -> &'static Self {
        stash::access("_picolib_stash")
    }

    pub fn set_raft_node(&self, raft_node: traft::Node) {
        *self.raft_node.borrow_mut() = Some(raft_node);
    }

    pub fn raft_node(&self) -> Ref<Option<traft::Node>> {
        self.raft_node.borrow()
    }
}

impl std::fmt::Debug for Stash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("raft_node", &self.raft_node().is_some())
            .finish()
    }
}

#[no_mangle]
pub extern "C" fn luaopen_picolib(l: *mut std::ffi::c_void) -> c_int {
    if std::env::var("PICOLIB_NO_AUTORUN").is_ok() {
        // Skip box.cfg and other initialization
        // Used mostly for testing purposes
    } else {
        main_run();
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
        luamod.set("run", tlua::function0(main_run));
        luamod.set(
            "get_stash",
            tlua::function0(|| println!("{:?}", Stash::access())),
        );
        luamod.set(
            "raft_propose_info",
            tlua::function1(|x: String| raft_propose(Message::Info { msg: x })),
        );
        luamod.set(
            "raft_propose_eval",
            tlua::function1(|x: String| raft_propose(Message::EvalLua { code: x })),
        );
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

fn main_run() {
    let stash = Stash::access();

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
    stash.set_raft_node(node);

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

fn raft_propose(msg: Message) {
    let stash = Stash::access();
    let raft_ref = stash.raft_node();
    let raft_node = raft_ref.as_ref().unwrap();
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
