use ::raft::prelude as raft;
use ::tarantool::tlua;
use std::os::raw::c_int;

mod error;
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
use std::time::Duration;

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

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn luaopen_picolib(l: *mut std::ffi::c_void) -> c_int {
    // Perform box.cfg and other initialization.
    // It's disabled only for testing purposes.
    if std::env::var("PICOLIB_AUTORUN")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(true)
    {
        main_run();
    }

    let l = tlua::StaticLua::from_static(l);
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
        "raft_propose_info",
        tlua::function1(|x: String| raft_propose(Message::Info { msg: x })),
    );
    luamod.set(
        "raft_propose_eval",
        tlua::function2(|timeout: f64, x: String| {
            raft_propose_wait_applied(
                Message::EvalLua { code: x },
                Duration::from_secs_f64(timeout),
            )
        }),
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

    std::env::var("PICODATA_DATA_DIR").ok().map(|v| {
        std::fs::create_dir_all(&v).unwrap();
        cfg.wal_dir = v.clone();
        cfg.memtx_dir = v.clone();
        Some(v)
    });

    tarantool::set_cfg(&cfg);
    tarantool::eval(
        r#"
        box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
        box.cfg({log_level = 6})
    "#,
    );

    traft::Storage::init_schema();

    let raft_id: u64 = {
        // The id already stored in tarantool snashot
        let snap_id: Option<u64> = traft::Storage::id().unwrap();
        // The id passed in env vars (or command line args)
        let args_id: Option<u64> = std::env::var("PICODATA_RAFT_ID")
            .and_then(|v| {
                v.parse()
                    .map_err(|e| panic!("Bad PICODATA_RAFT_ID value \"{}\": {}", v, e))
            })
            .ok();

        match snap_id {
            None => {
                let id: u64 = args_id.unwrap_or(1);
                traft::Storage::persist_id(id).unwrap();
                id
            }
            Some(snap_id) => match args_id {
                Some(args_id) if args_id != snap_id => {
                    panic!(
                        "Already initialized with a different PICODATA_RAFT_ID:
  snapshot: {s}
 from args: {a}",
                        s = snap_id,
                        a = args_id
                    )
                }
                _ => snap_id,
            },
        }
    };

    let raft_cfg = raft::Config {
        id: raft_id,
        applied: traft::Storage::applied().unwrap().unwrap_or_default(),
        ..Default::default()
    };
    let node = traft::Node::new(&raft_cfg, handle_committed_data).unwrap();
    stash.set_raft_node(node);

    std::env::var("PICODATA_LISTEN").ok().map(|v| {
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
    let raft_node = raft_ref.as_ref().expect("Picodata not running yet");
    tlog!(Debug, "propose {:?} ................................", msg);
    raft_node.propose(&msg);
    tlog!(Debug, ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,");
}

fn raft_propose_wait_applied(msg: Message, timeout: Duration) -> bool {
    let stash = Stash::access();
    let raft_ref = stash.raft_node();
    let raft_node = raft_ref.as_ref().expect("Picodata not running yet");
    tlog!(Debug, "propose {:?} ................................", msg);
    let res = raft_node.propose_wait_applied(&msg, timeout);
    tlog!(Debug, ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,");
    res
}

fn handle_committed_data(data: &[u8]) {
    use Message::*;

    match Message::try_from(data) {
        Ok(x) => match x {
            EvalLua { code } => crate::tarantool::eval(&code),
            Info { msg } => tlog!(Info, "{msg}"),
            Empty => {}
        },
        Err(why) => tlog!(Error, "cannot decode raft entry data: {}", why),
    }
}
