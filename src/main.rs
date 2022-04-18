use nix::sys::termios::{tcgetattr, tcsetattr, SetArg::TCSADRAIN};
use nix::sys::wait::WaitStatus;
use nix::unistd::{self, fork, ForkResult};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

use ::raft::prelude as raft;
use ::tarantool::error::Error;
use ::tarantool::fiber;
use ::tarantool::tlua;
use ::tarantool::transaction::start_transaction;
use std::convert::TryFrom;
use std::time::{Duration, Instant};

use protobuf::Message as _;
use protobuf::ProtobufEnum as _;

mod app;
mod args;
mod discovery;
mod ipc;
mod mailbox;
mod tarantool;
mod tlog;
mod traft;

inventory::collect!(InnerTest);

pub struct InnerTest {
    pub name: &'static str,
    pub body: fn(),
}

fn picolib_setup(args: &args::Run) {
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
    luamod.set("args", args);

    luamod.set(
        "raft_status",
        tlua::function0(|| traft::node::global().map(|n| n.status())),
    );
    luamod.set(
        "raft_tick",
        tlua::function1(|n_times: u32| -> Result<(), traft::node::Error> {
            traft::node::global()?.tick(n_times);
            Ok(())
        }),
    );
    luamod.set(
        "raft_read_index",
        tlua::function1(|timeout: f64| -> Result<u64, traft::node::Error> {
            traft::node::global()?.read_index(Duration::from_secs_f64(timeout))
        }),
    );
    luamod.set(
        "raft_propose_info",
        tlua::function1(|x: String| -> Result<u64, traft::node::Error> {
            traft::node::global()?.propose(traft::Op::Info { msg: x }, Duration::from_secs(1))
        }),
    );
    luamod.set(
        "raft_timeout_now",
        tlua::function0(|| -> Result<(), traft::node::Error> {
            traft::node::global()?.timeout_now();
            Ok(())
        }),
    );
    luamod.set(
        "raft_propose_eval",
        tlua::function2(
            |timeout: f64, x: String| -> Result<u64, traft::node::Error> {
                traft::node::global()?.propose(
                    traft::Op::EvalLua { code: x },
                    Duration::from_secs_f64(timeout),
                )
            },
        ),
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

                function inspect_idx(idx)
                    local digest = require('digest')
                    local msgpack = require('msgpack')
                    local row = box.space.raft_log:get(idx)
                    local function decode(v)
                        local ok, ret = pcall(function()
                            return msgpack.decode(digest.base64_decode(v))
                        end)
                        return ok and ret or "???"
                    end
                    return {
                        decode(row.data),
                        decode(row.ctx),
                        nil
                    }
                end
            "#,
        )
        .unwrap();
    }
}

fn init_handlers() {
    tarantool::eval(
        r#"
        box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
        "#,
    );

    use discovery::proc_discover;
    declare_cfunc!(proc_discover);

    use traft::node::raft_interact;
    declare_cfunc!(raft_interact);

    use traft::node::raft_join;
    declare_cfunc!(raft_join);
}

fn rm_tarantool_files(data_dir: &str) {
    std::fs::read_dir(data_dir)
        .expect("[supervisor] failed reading data_dir")
        .map(|entry| entry.expect("[supervisor] failed reading directory entry"))
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .filter(|f| {
            f.extension()
                .map(|ext| ext == "xlog" || ext == "snap")
                .unwrap_or(false)
        })
        .for_each(|f| {
            tlog!(
                Warning,
                "[supervisor] removing file: {}",
                (&f).to_string_lossy()
            );
            std::fs::remove_file(f).unwrap();
        });
}

struct ExitStatus {
    raw: i32,
}

fn main() {
    let rc = match args::Picodata::from_args() {
        args::Picodata::Run(args) => main_run(args),
        args::Picodata::Test(args) => main_test(args),
        args::Picodata::Tarantool(args) => main_tarantool(args),
    };
    std::process::exit(rc.raw);
}

#[derive(Debug, Serialize, Deserialize)]
enum Entrypoint {
    StartDiscover(),
    StartJoin { leader_uri: String },
    // StartBoot(),
}

impl Entrypoint {
    fn exec(self, supervisor: Supervisor) {
        match self {
            Self::StartDiscover() => start_discover(supervisor),
            Self::StartJoin { leader_uri } => start_join(leader_uri, supervisor),
            // Self::StartBoot() => start_boot(supervisor),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct IpcMessage {
    next_entrypoint: Entrypoint,
    drop_db: bool,
}

struct Supervisor {
    pub args: args::Run,
    pub tx: ipc::Sender<IpcMessage>,
}

fn main_run(args: args::Run) -> ExitStatus {
    // Tarantool implicitly parses some environment variables.
    // We don't want them to affect the behavior and thus filter them out.
    for (k, _) in std::env::vars() {
        if k.starts_with("TT_") || k.starts_with("TARANTOOL_") {
            std::env::remove_var(k)
        }
    }

    let mut entrypoint = Entrypoint::StartDiscover {};

    // Tarantool running in a fork (or, to be more percise, the
    // libreadline) modifies termios settings to intercept echoed text.
    //
    // After subprocess termination it's not always possible to
    // restore the settings (e.g. in case of SIGSEGV). At least it
    // tries to. To preserve tarantool console operable, we cache
    // initial termios attributes and restore them manually.
    //
    let tcattr = tcgetattr(0).ok();

    loop {
        tlog!(Info, "[supervisor] running {:?}", entrypoint);

        let pipe = ipc::channel::<IpcMessage>();
        let (rx, tx) = pipe.expect("pipe creation failed");

        let pid = unsafe { fork() };
        match pid.expect("fork failed") {
            ForkResult::Child => {
                drop(rx);

                extern "C" fn trampoline(data: *mut libc::c_void) {
                    // let args = unsafe { Box::from_raw(data as _) };
                    let argbox = unsafe { Box::<(Entrypoint, Supervisor)>::from_raw(data as _) };
                    let (entrypoint, supervisor) = *argbox;
                    entrypoint.exec(supervisor);
                }

                // `argv` is a vec of pointers to data owned by `tt_args`, so
                // make sure `tt_args` outlives `argv`, because the compiler is not
                // gonna do that for you
                let tt_args = args.tt_args().unwrap();
                let argv = tt_args.iter().map(|a| a.as_ptr()).collect::<Vec<_>>();
                let supervisor = Supervisor { args, tx };
                let rc = unsafe {
                    tarantool::main(
                        argv.len() as _,
                        argv.as_ptr() as _,
                        Some(trampoline),
                        Box::into_raw(Box::new((entrypoint, supervisor))) as _,
                    )
                };
                return ExitStatus { raw: rc };
            }
            ForkResult::Parent { child } => {
                drop(tx);
                let msg = rx.recv().ok();

                let mut rc: i32 = 0;
                unsafe {
                    libc::waitpid(
                        child.into(),                // pid_t
                        &mut rc as *mut libc::c_int, // int*
                        0,                           // int options
                    )
                };

                // Restore termios configuration as planned
                if let Some(tcattr) = tcattr.as_ref() {
                    tcsetattr(0, TCSADRAIN, tcattr).unwrap();
                }

                tlog!(Info, "[supervisor] tarantool process finished: {:?}",
                    WaitStatus::from_raw(child, rc);
                );

                if let Some(msg) = msg {
                    entrypoint = msg.next_entrypoint;
                    if msg.drop_db {
                        tlog!(Info, "[supervisor] tarantool requested rebootstrap");
                        rm_tarantool_files(&args.data_dir);
                    }
                } else {
                    std::process::exit(rc);
                }
            }
        };
    }
}

fn start_discover(supervisor: Supervisor) {
    tlog!(Info, ">>>>> start_discover()");
    let args = &supervisor.args;

    picolib_setup(args);
    assert!(tarantool::cfg().is_none());

    let mut cfg = tarantool::Cfg {
        listen: None,
        read_only: false,
        wal_dir: args.data_dir.clone(),
        memtx_dir: args.data_dir.clone(),
        ..Default::default()
    };

    std::fs::create_dir_all(&args.data_dir).unwrap();
    tarantool::set_cfg(&cfg);

    traft::Storage::init_schema();

    // исходный discovery::discover() пришлось разбить на две части -
    // init_global и wait_global. К сожалению, они не могут быть атомарны,
    // потому что listen порт надо поднимать именно посередине. С неподнятым портом
    // уходить в кишки discovery::discover() нельзя - на запросы отвечать будет некому.
    // А если поднять порт до инициализации дискавери, то образуется временно́е окно,
    // и прилетевший пакет приведёт к панике "discovery error: expected DISCOVERY
    // to be set on instance startup"
    discovery::init_global(&args.peers);
    init_handlers();

    cfg.listen = Some(args.listen.clone());
    tarantool::set_cfg(&cfg);
    let role = discovery::wait_global();

    // TODO assert traft::Storage::instance_id == (null || args.instance_id)
    if traft::Storage::id().unwrap().is_some() {
        return postjoin(supervisor);
    }

    let msg = match role {
        discovery::Role::Leader { .. } => {
            return start_boot(supervisor);
            // let next_entrypoint = Entrypoint::StartBoot();
            // IpcMessage {
            //     next_entrypoint,
            //     drop_db: false,
            // }
        }
        discovery::Role::NonLeader { leader } => {
            let next_entrypoint = Entrypoint::StartJoin { leader_uri: leader };
            IpcMessage {
                next_entrypoint,
                drop_db: true,
            }
        }
    };

    supervisor.tx.send(&msg);
    std::process::exit(0);
}

fn start_boot(supervisor: Supervisor) {
    tlog!(Info, ">>>>> start_boot()");
    let args = &supervisor.args;

    // picolib_setup(&args);
    // assert!(tarantool::cfg().is_none());

    // let cfg = tarantool::Cfg {
    //     listen: None,
    //     read_only: false,
    //     wal_dir: args.data_dir.clone(),
    //     memtx_dir: args.data_dir.clone(),
    //     ..Default::default()
    // };

    // std::fs::create_dir_all(&args.data_dir).unwrap();
    // tarantool::set_cfg(&cfg);

    // traft::Storage::init_schema();
    // init_handlers();

    let raft_id = 1u64;

    start_transaction(|| -> Result<(), Error> {
        let cs = raft::ConfState {
            voters: vec![raft_id],
            ..Default::default()
        };

        let e0 = {
            let peer = traft::Peer {
                raft_id,
                instance_id: args.instance_id.clone(),
                peer_address: args.advertise_address(),
                voter: true,
                commit_index: 0,
            };

            let c1 = raft::ConfChange {
                change_type: raft::ConfChangeType::AddNode,
                node_id: raft_id,
                ..Default::default()
            };
            let ctx = traft::EntryContextConfChange {
                lc: traft::LogicalClock::new(1, 0),
                peers: vec![peer],
            };
            let e = traft::Entry {
                entry_type: raft::EntryType::EntryConfChange.value(),
                index: 1,
                term: 1,
                data: c1.write_to_bytes().unwrap(),
                context: Some(traft::EntryContext::ConfChange(ctx)),
            };

            raft::Entry::try_from(e).unwrap()
        };

        traft::Storage::persist_conf_state(&cs).unwrap();
        traft::Storage::persist_entries(&[e0]).unwrap();
        traft::Storage::persist_commit(1).unwrap();
        traft::Storage::persist_term(1).unwrap();
        traft::Storage::persist_id(raft_id).unwrap();
        Ok(())
    })
    .unwrap();

    postjoin(supervisor)
}

fn start_join(leader_uri: String, supervisor: Supervisor) {
    tlog!(Info, ">>>>> start_join({leader_uri})");
    let args = &supervisor.args;

    let req = traft::node::JoinRequest {
        instance_id: args.instance_id.clone(),
        replicaset_id: args.replicaset_id.clone(),
        voter: false,
        advertise_address: args.advertise_address(),
    };

    use traft::node::raft_join;
    let fn_name = stringify_cfunc!(raft_join);
    let resp: traft::node::JoinResponse = tarantool::net_box_call_retry(&leader_uri, fn_name, &req);

    picolib_setup(args);
    assert!(tarantool::cfg().is_none());

    let cfg = tarantool::Cfg {
        listen: None,
        read_only: false,
        // instance_uuid: resp.instance_uuid
        // replicaset_uuid: resp.replicaset_uuid
        replication: resp.box_replication.clone(),
        wal_dir: args.data_dir.clone(),
        memtx_dir: args.data_dir.clone(),
        ..Default::default()
    };

    std::fs::create_dir_all(&args.data_dir).unwrap();
    tarantool::set_cfg(&cfg);

    traft::Storage::init_schema();
    init_handlers();

    let raft_id = resp.peer.raft_id;
    start_transaction(|| -> Result<(), Error> {
        for peer in resp.raft_group {
            traft::Storage::persist_peer(&peer).unwrap();
        }
        traft::Storage::persist_id(raft_id).unwrap();
        Ok(())
    })
    .unwrap();

    postjoin(supervisor)
}

fn postjoin(supervisor: Supervisor) {
    tlog!(Info, ">>>>> postjoin()");
    let args = &supervisor.args;

    let raft_id = traft::Storage::id().unwrap().unwrap();
    let applied = traft::Storage::applied().unwrap().unwrap_or(0);
    let raft_cfg = raft::Config {
        id: raft_id,
        applied,
        pre_vote: true,
        ..Default::default()
    };

    let node = traft::node::Node::new(&raft_cfg);
    let node = node.expect("failed initializing raft node");

    let cs = traft::Storage::conf_state().unwrap();
    if cs.voters == vec![raft_cfg.id] {
        tlog!(
            Info,
            concat!(
                "this is the only vorer in cluster, ",
                "triggering election immediately"
            )
        );

        node.tick(1); // apply configuration, if any
        node.campaign(); // trigger election immediately
        assert_eq!(node.status().raft_state, "Leader");
    }

    traft::node::set_global(node);
    let node = traft::node::global().unwrap();

    tarantool::set_cfg(&tarantool::Cfg {
        listen: Some(args.listen.clone()),
        ..tarantool::cfg().unwrap()
    });

    while node.status().leader_id == 0 {
        node.wait_status();
    }

    loop {
        let timeout = Duration::from_secs(1);
        if let Err(e) = traft::node::global().unwrap().read_index(timeout) {
            tlog!(Warning, "unable to get a read barrier: {e}");
            continue;
        } else {
            break;
        }
    }

    loop {
        let timeout = Duration::from_millis(220);
        let me = traft::Storage::peer_by_raft_id(raft_id)
            .unwrap()
            .expect("peer not found");

        if me.voter && me.peer_address == args.advertise_address() {
            // already ok
            break;
        }

        tlog!(Warning, "initiating self-promotion of {me:?}");
        let req = traft::node::JoinRequest {
            instance_id: me.instance_id.clone(),
            replicaset_id: None, // TODO
            voter: true,
            advertise_address: args.advertise_address(),
        };

        let leader_id = node.status().leader_id;
        let leader = traft::Storage::peer_by_raft_id(leader_id).unwrap().unwrap();

        use traft::node::raft_join;
        let fn_name = stringify_cfunc!(raft_join);
        let now = Instant::now();
        match tarantool::net_box_call(&leader.peer_address, fn_name, &req, timeout) {
            Err(e) => {
                tlog!(Error, "failed to promote myself: {e}");
                fiber::sleep(timeout.saturating_sub(now.elapsed()));
                continue;
            }
            Ok(traft::node::JoinResponse { .. }) => {
                break;
            }
        };
    }
}

fn main_tarantool(args: args::Tarantool) -> ExitStatus {
    // XXX: `argv` is a vec of pointers to data owned by `tt_args`, so
    // make sure `tt_args` outlives `argv`, because the compiler is not
    // gonna do that for you
    let tt_args = args.tt_args();
    let argv = tt_args.iter().map(|a| a.as_ptr()).collect::<Vec<_>>();

    let rc = unsafe {
        tarantool::main(
            argv.len() as _,
            argv.as_ptr() as _,
            None,
            std::ptr::null_mut(),
        )
    };

    ExitStatus { raw: rc }
}

macro_rules! color {
    (@priv red) => { "\x1b[0;31m" };
    (@priv green) => { "\x1b[0;32m" };
    (@priv clear) => { "\x1b[0m" };
    (@priv $s:literal) => { $s };
    ($($s:tt)*) => {
        ::std::concat![ $( color!(@priv $s) ),* ]
    }
}

fn main_test(args: args::Test) -> ExitStatus {
    cleanup_env!();

    const PASSED: &str = color![green "ok" clear];
    const FAILED: &str = color![red "FAILED" clear];
    let mut cnt_passed = 0u32;
    let mut cnt_failed = 0u32;

    let now = std::time::Instant::now();

    println!();
    println!(
        "running {} tests",
        inventory::iter::<InnerTest>.into_iter().count()
    );
    for t in inventory::iter::<InnerTest> {
        print!("test {} ... ", t.name);

        let (mut rx, tx) = ipc::pipe().expect("pipe creation failed");
        let pid = unsafe { fork() };
        match pid.expect("fork failed") {
            ForkResult::Child => {
                drop(rx);
                unistd::close(0).ok(); // stdin
                unistd::dup2(*tx, 1).ok(); // stdout
                unistd::dup2(*tx, 2).ok(); // stderr
                drop(tx);

                // `argv` is a vec of pointers to data owned by `tt_args`, so
                // make sure `tt_args` outlives `argv`, because the compiler is not
                // gonna do that for you
                let tt_args = args.tt_args().unwrap();
                let argv = tt_args.iter().map(|a| a.as_ptr()).collect::<Vec<_>>();
                let rc = unsafe {
                    tarantool::main(
                        argv.len() as _,
                        argv.as_ptr() as _,
                        Some(test_one),
                        Box::into_raw(Box::new(t)) as _,
                    )
                };
                return ExitStatus { raw: rc };
            }
            ForkResult::Parent { child } => {
                drop(tx);
                let log = {
                    let mut buf = Vec::new();
                    use std::io::Read;
                    rx.read_to_end(&mut buf)
                        .map_err(|e| println!("error reading ipc pipe: {e}"))
                        .ok();
                    buf
                };

                let mut rc: i32 = 0;
                unsafe {
                    libc::waitpid(
                        child.into(),                // pid_t
                        &mut rc as *mut libc::c_int, // int*
                        0,                           // int options
                    )
                };

                if rc == 0 {
                    println!("{PASSED}");
                    cnt_passed += 1;
                } else {
                    println!("{FAILED}");
                    cnt_failed += 1;

                    use std::io::Write;
                    println!();
                    std::io::stderr()
                        .write_all(&log)
                        .map_err(|e| println!("error writing stderr: {e}"))
                        .ok();
                    println!();
                }
            }
        };
    }

    let ok = cnt_failed == 0;
    println!();
    print!("test result: {}.", if ok { PASSED } else { FAILED });
    print!(" {cnt_passed} passed;");
    print!(" {cnt_failed} failed;");
    println!(" finished in {:.2}s", now.elapsed().as_secs_f32());
    println!();

    ExitStatus { raw: !ok as _ }
}

extern "C" fn test_one(data: *mut libc::c_void) {
    let t = unsafe { Box::<&InnerTest>::from_raw(data as _) };
    let temp = tempfile::tempdir().expect("Failed creating a temp directory");
    std::env::set_current_dir(temp.path()).expect("Failed chainging current directory");

    let cfg = tarantool::Cfg {
        listen: Some("127.0.0.1:0".into()),
        read_only: false,
        ..Default::default()
    };

    tarantool::set_cfg(&cfg);
    traft::Storage::init_schema();
    tarantool::eval(
        r#"
        box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
        "#,
    );

    (t.body)();
    std::process::exit(0i32);
}
