use std::os::raw::{c_char, c_int, c_void};
use std::os::unix::process::CommandExt;
use std::process::Command;
use structopt::StructOpt;

use ::raft::prelude as raft;
use ::tarantool::error::TarantoolErrorCode::ProcC as ProcCError;
use ::tarantool::set_error;
use ::tarantool::tlua;
use ::tarantool::tuple::{FunctionArgs, FunctionCtx, Tuple};
use indoc::indoc;

mod app;
mod args;
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

fn picolib_setup(args: args::Run) {
    let l = ::tarantool::lua_state();
    let package: tlua::LuaTable<_> = l.get("package").expect("package == nil");
    let loaded: tlua::LuaTable<_> = package.get("loaded").expect("package.loaded == nil");
    loaded.set("picolib", &[()]);
    let luamod: tlua::LuaTable<_> = loaded
        .get("picolib")
        .expect("package.loaded.picolib == nil");

    // Also add a global picolib variable
    l.set("picolib", &luamod);

    luamod.set("VERSION", env!("CARGO_PKG_VERSION"));
    luamod.set("args", &args);

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
    luamod.set("rebootstrap", tlua::function0(rebootstrap));
    luamod.set("run", tlua::function0(move || start(&args)));
    luamod.set("raft_status", tlua::function0(raft_status));
    luamod.set("raft_tick", tlua::function1(raft_tick));
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
                        {raft_state = box.space.raft_state:fselect()},
                        {raft_group = box.space.raft_group:fselect()}
                end
            "#,
        )
        .unwrap();
    }
}

fn start(args: &args::Run) {
    let stash = Stash::access();

    if tarantool::cfg().is_some() {
        // Already initialized
        return;
    }

    let mut cfg = tarantool::Cfg {
        listen: None,
        wal_dir: args.data_dir.clone(),
        memtx_dir: args.data_dir.clone(),
        ..Default::default()
    };

    std::fs::create_dir_all(&args.data_dir).unwrap();

    tarantool::set_cfg(&cfg);
    tarantool::eval(
        r#"
        box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
        box.cfg({log_level = 5})
    "#,
    );

    traft::Storage::init_schema();
    init_handlers();

    let raft_id: u64 = {
        // The id already stored in tarantool snashot
        let snap_id = traft::Storage::id().expect("failed to get id from storage");
        // The id passed in env vars (or command line args)
        let args_id = args.raft_id;

        match (snap_id, args_id) {
            (None, _) => {
                let id = args_id.unwrap_or(1);
                traft::Storage::persist_id(id).expect("failed to persist id");
                id
            }
            (Some(snap_id), Some(args_id)) if args_id != snap_id => {
                panic!(
                    indoc!(
                        "
                        Already initialized with a different PICODATA_RAFT_ID:
                          snapshot: {s}
                         from args: {a}
                    "
                    ),
                    s = snap_id,
                    a = args_id
                )
            }
            (Some(snap_id), _) => snap_id,
        }
    };

    // This is a temporary hack until fair joining is implemented
    traft::Storage::persist_peers(&args.peers);

    let raft_cfg = raft::Config {
        id: raft_id,
        pre_vote: true,
        applied: traft::Storage::applied().unwrap().unwrap_or_default(),
        ..Default::default()
    };
    let node = traft::Node::new(&raft_cfg, handle_committed_data).unwrap();
    stash.set_raft_node(node);

    cfg.listen = Some(args.listen.clone());
    tarantool::set_cfg(&cfg);

    tlog!(Info, "Hello, Rust!"; "module" => std::module_path!());
    tlog!(
        Debug,
        "Picodata running on {} {}",
        tarantool::package(),
        tarantool::version()
    );
}

fn raft_tick(n_times: u32) {
    let stash = Stash::access();
    let raft_ref = stash.raft_node();
    let raft_node = raft_ref.as_ref().expect("Picodata not running yet");
    for _ in 0..n_times {
        raft_node.tick();
    }
}

fn raft_status() -> traft::Status {
    let stash = Stash::access();
    let raft_ref = stash.raft_node();
    let raft_node = raft_ref.as_ref().expect("Picodata not running yet");
    raft_node.status()
}

fn raft_propose(msg: Message) {
    let stash = Stash::access();
    let raft_ref = stash.raft_node();
    let raft_node = raft_ref.as_ref().expect("Picodata not running yet");
    raft_node.propose(&msg)
}

fn raft_propose_wait_applied(msg: Message, timeout: Duration) -> bool {
    let stash = Stash::access();
    let raft_ref = stash.raft_node();
    let raft_node = raft_ref.as_ref().expect("Picodata not running yet");
    raft_node.propose_wait_applied(&msg, timeout)
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

fn init_handlers() {
    crate::tarantool::eval(
        r#"
        box.schema.func.create('.raft_interact', {
            language = "C",
            if_not_exists = true
        })
    "#,
    );
}

#[no_mangle]
pub extern "C" fn raft_interact(_: FunctionCtx, args: FunctionArgs) -> c_int {
    let stash = Stash::access();
    let raft_ref = stash.raft_node();
    let raft_node = raft_ref
        .as_ref()
        .expect("picodata should already be running");

    // Conversion pipeline:
    // FunctionArgs -> Tuple -?-> traft::row::Message -?-> raft::Message;

    let m: traft::row::Message = match Tuple::from(args).into_struct() {
        Ok(v) => v,
        Err(e) => return set_error!(ProcCError, "{e}"),
    };

    let m = match raft::Message::try_from(m) {
        Ok(v) => v,
        Err(e) => return set_error!(ProcCError, "{e}"),
    };

    raft_node.step(m);
    0
}

fn main() {
    let res = match args::Picodata::from_args() {
        args::Picodata::Run(r) => run(r),
        args::Picodata::Tarantool(t) => tarantool(t),
        args::Picodata::Test(t) => test(t),
    };
    if let Err(e) = res {
        eprintln!("Fatal: {}", e)
    }
}

macro_rules! tarantool_main {
    (@impl $tt_args:expr, $cb:expr, $cb_data:expr) => {
        {
            extern "C" {
                fn tarantool_main(
                    argc: i32,
                    argv: *mut *mut c_char,
                    cb: Option<extern "C" fn(*mut c_void)>,
                    cb_data: *mut c_void,
                ) -> c_int;
            }

            let tt_args = $tt_args;
            // XXX: `argv` is a vec of pointers to data owned by `tt_args`, so
            // make sure `tt_args` outlives `argv`, because the compiler is not
            // gonna do that for you
            let argv = tt_args.iter().map(|a| a.as_ptr()).collect::<Vec<_>>();

            let rc = unsafe {
                tarantool_main(
                    argv.len() as _,
                    argv.as_ptr() as _,
                    $cb,
                    $cb_data,
                )
            };

            if rc == 0 {
                Ok(())
            } else {
                Err(format!("return code {rc}"))
            }
        }
    };
    ($tt_args:expr) => {
        tarantool_main!(@impl $tt_args, None, std::ptr::null_mut())
    };
    ($tt_args:expr, $pd_args:expr => $cb:expr) => {
        {
            extern "C" fn trampoline(data: *mut c_void) {
                let args = unsafe { Box::from_raw(data as _) };
                $cb(*args)
            }

            tarantool_main!(@impl
                $tt_args,
                Some(trampoline),
                Box::into_raw(Box::new($pd_args)) as _
            )
        }
    }
}

extern "C" {
    static mut wal_dir_lock: i32;
    fn close(fd: i32) -> i32;
}

const REBOOTSTRAP_ENV_VAR: &str = "PICODATA_REBOOTSTRAP";

static mut ARGV: Vec<String> = vec![];

fn rebootstrap() {
    std::env::set_var(REBOOTSTRAP_ENV_VAR, "1");
    unsafe {
        tarantool::eval("box.cfg{listen=''}; require'fiber'.sleep(0.2)");
        close(dbg!(wal_dir_lock));
        tlog!(Warning, "Calling exec with: {:?}", &ARGV);
        let _ = Command::new(&ARGV[0]).args(&ARGV[1..]).exec();
    }
}

fn run(args: args::Run) -> Result<(), String> {
    // Tarantool implicitly parses some environment variables.
    // We don't want them to affect the behavior and thus filter them out.
    for (k, _) in std::env::vars() {
        if k.starts_with("TT_") || k.starts_with("TARANTOOL_") {
            std::env::remove_var(k)
        }
    }

    unsafe {
        ARGV = std::env::args().collect(); // Used in rebootstrap
    }

    tarantool_main!(args.tt_args()?, args => |args: args::Run| {
        if std::env::var(REBOOTSTRAP_ENV_VAR).is_ok() {
            std::env::remove_var(REBOOTSTRAP_ENV_VAR);
            tlog!(Warning, "re-bootstrapping...");
            // cleanup data dir
            std::fs::read_dir(&args.data_dir)
                .expect("failed reading data_dir")
                .map(|entry| entry.unwrap_or_else(|e| panic!("Failed reading directory entry: {}", e)))
                .map(|entry| entry.path())
                .filter(|path| path.is_file())
                .filter(|f| {
                    f.extension()
                        .map(|ext| ext == "xlog" || ext == "snap")
                        .unwrap_or(false)
                })
                .for_each(|f| {
                    tlog!(Warning, "removing file: {}", (&f).to_string_lossy());
                    std::fs::remove_file(f).unwrap();
                });
        };

        if args.autorun {
            start(&args);
        }

        picolib_setup(args);
    })
}

fn tarantool(args: args::Tarantool) -> Result<(), String> {
    tarantool_main!(args.tt_args()?)
}

fn test(args: args::Test) -> Result<(), String> {
    // Tarantool implicitly parses some environment variables.
    // We don't want them to affect the behavior and thus filter them out.
    for (k, _) in std::env::vars() {
        if k.starts_with("TT_") || k.starts_with("TARANTOOL_") {
            std::env::remove_var(k)
        }
    }

    let mut args = args.run;
    args.listen = "127.0.0.1:0".into();

    let temp = tempfile::tempdir().map_err(|e| format!("Failed creating a temp directory: {e}"))?;
    std::env::set_current_dir(temp.path())
        .map_err(|e| format!("Failed chainging current directory: {e}"))?;

    tarantool_main!(args.tt_args()?, args => |args: args::Run| {
        start(&args);
        picolib_setup(args);
        run_tests();
    })
}

macro_rules! color {
    (@impl red) => { "\x1b[0;31m" };
    (@impl green) => { "\x1b[0;32m" };
    (@impl clear) => { "\x1b[0m" };
    (@impl $s:literal) => { $s };
    ($($s:tt)*) => {
        ::std::concat![ $( color!(@impl $s) ),* ]
    }
}

fn run_tests() {
    const PASSED: &str = color![green "ok" clear];
    const FAILED: &str = color![red "FAILED" clear];
    let mut cnt_passed = 0u32;
    let mut cnt_failed = 0u32;

    let now = std::time::Instant::now();

    for t in inventory::iter::<InnerTest> {
        eprint!("Running {} ... ", t.name);
        if let Err(e) = std::panic::catch_unwind(t.body) {
            eprintln!("{FAILED}");
            cnt_failed += 1;
            if let Some(msg) = e
                .downcast_ref::<String>()
                .map(|e| e.as_str())
                .or_else(|| e.downcast_ref::<&'static str>().copied())
            {
                eprintln!("\n{msg}\n");
            }
        } else {
            eprintln!("{PASSED}");
            cnt_passed += 1
        }
    }

    let ok = cnt_failed == 0;
    print!("test result: {}.", if ok { PASSED } else { FAILED });
    print!(" {cnt_passed} passed;");
    print!(" {cnt_failed} failed;");
    println!(" finished in {:.2}s", now.elapsed().as_secs_f32());
    eprintln!();

    std::process::exit(!ok as i32)
}
