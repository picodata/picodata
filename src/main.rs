use nix::sys::signal;
use nix::sys::termios::{tcgetattr, tcsetattr, SetArg::TCSADRAIN};
use nix::sys::wait::{waitpid, WaitStatus};
use nix::unistd::{self, fork, ForkResult};
use serde::{Deserialize, Serialize};

use ::raft::prelude as raft;
use ::tarantool::error::Error as TntError;
use ::tarantool::fiber;
use ::tarantool::tlua;
use ::tarantool::transaction::start_transaction;
use std::convert::TryFrom;
use std::time::{Duration, Instant};
use traft::storage::{ClusterSpace, StateKey};
use traft::ExpelRequest;
use traft::RaftSpaceAccess;

use clap::StructOpt as _;
use protobuf::Message as _;

use crate::traft::{Grade, LogicalClock, RaftIndex, TargetGrade, UpdatePeerRequest};
use traft::error::Error;

mod app;
mod args;
mod cache;
mod discovery;
mod ipc;
mod kvcell;
mod mailbox;
mod tarantool;
mod tlog;
mod traft;
mod util;

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
        tlua::function1(|n_times: u32| -> Result<(), Error> {
            traft::node::global()?.tick_and_yield(n_times);
            Ok(())
        }),
    );
    luamod.set(
        "raft_read_index",
        tlua::function1(|timeout: f64| -> Result<RaftIndex, Error> {
            traft::node::global()?.wait_for_read_state(Duration::from_secs_f64(timeout))
        }),
    );
    luamod.set(
        "raft_propose_info",
        tlua::function1(|x: String| -> Result<(), Error> {
            traft::node::global()?
                .propose_and_wait(traft::Op::Info { msg: x }, Duration::from_secs(1))
        }),
    );
    luamod.set(
        "raft_timeout_now",
        tlua::function0(|| -> Result<(), Error> {
            traft::node::global()?.timeout_now();
            Ok(())
        }),
    );
    #[rustfmt::skip]
    luamod.set(
        "exit",
        tlua::function1(|code: Option<i32>| {
            tarantool::exit(code.unwrap_or(0))
        }),
    );
    luamod.set(
        "expel",
        tlua::function1(|instance_id: String| -> Result<(), Error> {
            traft::node::expel_wrapper(instance_id)
        }),
    );
    #[derive(::tarantool::tlua::LuaRead)]
    struct ProposeEvalOpts {
        timeout: Option<f64>,
    }
    luamod.set(
        "raft_propose_eval",
        tlua::function2(
            |x: String, opts: Option<ProposeEvalOpts>| -> Result<(), Error> {
                let timeout = opts.and_then(|opts| opts.timeout).unwrap_or(10.0);
                traft::node::global()?
                    .propose_and_wait(
                        traft::OpEvalLua { code: x },
                        Duration::from_secs_f64(timeout),
                    )
                    .and_then(|res| res.map_err(Into::into))
            },
        ),
    );
    luamod.set(
        "raft_return_one",
        tlua::function1(|timeout: f64| -> Result<u8, Error> {
            traft::node::global()?
                .propose_and_wait(traft::OpReturnOne, Duration::from_secs_f64(timeout))
        }),
    );
    luamod.set("log", &[()]);
    #[rustfmt::skip]
    l.exec_with(
        "picolib.log.highlight_key = ...",
        tlua::function2(|key: String, color: Option<String>| -> Result<(), String> {
            let color = match color.as_deref() {
                None            => None,
                Some("red")     => Some(tlog::Color::Red),
                Some("green")   => Some(tlog::Color::Green),
                Some("blue")    => Some(tlog::Color::Blue),
                Some("cyan")    => Some(tlog::Color::Cyan),
                Some("yellow")  => Some(tlog::Color::Yellow),
                Some("magenta") => Some(tlog::Color::Magenta),
                Some("white")   => Some(tlog::Color::White),
                Some("black")   => Some(tlog::Color::Black),
                Some(other) => {
                    return Err(format!("unknown color: {other:?}"))
                }
            };
            tlog::highlight_key(key, color);
            Ok(())
        }),
    )
    .unwrap();
    l.exec_with(
        "picolib.log.clear_highlight = ...",
        tlua::function0(tlog::clear_highlight),
    )
    .unwrap();
    {
        l.exec(
            r#"
                function inspect()
                    return
                        {raft_log = picolib.raft_log()},
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

    #[derive(::tarantool::tlua::LuaRead)]
    struct RaftLogOpts {
        return_string: Option<bool>,
    }
    luamod.set(
        "raft_log",
        tlua::function1(
            |opts: Option<RaftLogOpts>| -> Result<Option<String>, Error> {
                let header = ["index", "term", "lc", "contents"];
                let [index, term, lc, contents] = header;
                let mut rows = vec![];
                let mut col_widths = header.map(|h| h.len());
                let node = traft::node::global()?;
                let entries = node
                    .all_traft_entries()
                    .map_err(|e| Error::Other(Box::new(e)))?;
                for entry in entries {
                    let row = [
                        entry.index.to_string(),
                        entry.term.to_string(),
                        entry
                            .lc()
                            .map(ToString::to_string)
                            .unwrap_or_else(String::new),
                        entry.payload().to_string(),
                    ];
                    for i in 0..col_widths.len() {
                        col_widths[i] = col_widths[i].max(row[i].len());
                    }
                    rows.push(row);
                }
                let [iw, tw, lw, mut cw] = col_widths;

                let total_width = 1 + header.len() + col_widths.iter().sum::<usize>();
                let cols = util::screen_size().1 as usize;
                if total_width > cols {
                    cw -= total_width - cols;
                }

                use std::io::Write;
                let mut buf: Vec<u8> = Vec::with_capacity(512);
                let row_sep = |buf: &mut Vec<u8>| {
                    writeln!(buf, "+{0:-^iw$}+{0:-^tw$}+{0:-^lw$}+{0:-^cw$}+", "").unwrap()
                };
                row_sep(&mut buf);
                writeln!(
                    buf,
                    "|{index: ^iw$}|{term: ^tw$}|{lc: ^lw$}|{contents: ^cw$}|"
                )
                .unwrap();
                row_sep(&mut buf);
                for [index, term, lc, contents] in rows {
                    if contents.len() <= cw {
                        writeln!(
                            buf,
                            "|{index: ^iw$}|{term: ^tw$}|{lc: ^lw$}|{contents: ^cw$}|"
                        )
                        .unwrap();
                    } else {
                        writeln!(
                            buf,
                            "|{index: ^iw$}|{term: ^tw$}|{lc: ^lw$}|{contents: ^cw$}|",
                            contents = &contents[..cw],
                        )
                        .unwrap();
                        let mut rest = &contents[cw..];
                        while !rest.is_empty() {
                            let clamped_cw = usize::min(rest.len(), cw);
                            writeln!(
                                buf,
                                "|{blank: ^iw$}|{blank: ^tw$}|{blank: ^lw$}|{contents: ^cw$}|",
                                blank = "~",
                                contents = &rest[..clamped_cw],
                            )
                            .unwrap();
                            rest = &rest[clamped_cw..];
                        }
                    }
                }
                row_sep(&mut buf);
                if opts.and_then(|opts| opts.return_string).unwrap_or(false) {
                    Ok(Some(String::from_utf8_lossy(&buf).into()))
                } else {
                    std::io::stdout().write_all(&buf).unwrap();
                    Ok(None)
                }
            },
        ),
    );
}

fn init_handlers() {
    tarantool::eval(
        r#"
        box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
        "#,
    )
    .unwrap();

    declare_cfunc!(discovery::proc_discover);
    declare_cfunc!(traft::node::raft_interact);
    declare_cfunc!(traft::node::raft_join);
    declare_cfunc!(traft::node::raft_expel_on_leader);
    declare_cfunc!(traft::node::raft_expel);
    declare_cfunc!(traft::node::raft_sync_raft);
    declare_cfunc!(traft::failover::raft_update_peer);
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
            println!("[supervisor] removing file: {}", (&f).to_string_lossy());
            std::fs::remove_file(f).unwrap();
        });
}

fn main() -> ! {
    match args::Picodata::parse() {
        args::Picodata::Run(args) => main_run(args),
        args::Picodata::Test(args) => main_test(args),
        args::Picodata::Tarantool(args) => main_tarantool(args),
        args::Picodata::Expel(args) => main_expel(args),
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Serialize, Deserialize)]
enum Entrypoint {
    StartDiscover,
    StartBoot,
    StartJoin { leader_address: String },
}

impl Entrypoint {
    fn exec(self, args: args::Run, to_supervisor: ipc::Sender<IpcMessage>) {
        match self {
            Self::StartDiscover => start_discover(&args, to_supervisor),
            Self::StartBoot => start_boot(&args),
            Self::StartJoin { leader_address } => start_join(&args, leader_address),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct IpcMessage {
    next_entrypoint: Entrypoint,
    drop_db: bool,
}

macro_rules! tarantool_main {
    (
        $tt_args:expr,
         callback_data: $cb_data:tt,
         callback_data_type: $cb_data_ty:ty,
         callback_body: $cb_body:expr
    ) => {{
        let tt_args = $tt_args;
        // `argv` is a vec of pointers to data owned by `tt_args`, so
        // make sure `tt_args` outlives `argv`, because the compiler is not
        // gonna do that for you
        let argv = tt_args.iter().map(|a| a.as_ptr()).collect::<Vec<_>>();
        extern "C" fn trampoline(data: *mut libc::c_void) {
            let args = unsafe { Box::from_raw(data as _) };
            let cb = |$cb_data: $cb_data_ty| $cb_body;
            cb(*args)
        }
        let cb_data: $cb_data_ty = $cb_data;
        unsafe {
            tarantool::main(
                argv.len() as _,
                argv.as_ptr() as _,
                Some(trampoline),
                Box::into_raw(Box::new(cb_data)) as _,
            )
        }
    }};
}

fn main_run(args: args::Run) -> ! {
    // Tarantool implicitly parses some environment variables.
    // We don't want them to affect the behavior and thus filter them out.
    for (k, _) in std::env::vars() {
        if k.starts_with("TT_") || k.starts_with("TARANTOOL_") {
            std::env::remove_var(k)
        }
    }

    // Tarantool running in a fork (or, to be more percise, the
    // libreadline) modifies termios settings to intercept echoed text.
    //
    // After subprocess termination it's not always possible to
    // restore the settings (e.g. in case of SIGSEGV). At least it
    // tries to. To preserve tarantool console operable, we cache
    // initial termios attributes and restore them manually.
    //
    let tcattr = tcgetattr(0).ok();

    // Intercept and forward signals to the child. As for the child
    // itself, one shouldn't worry about setting up signal handlers -
    // Tarantool does that implicitly.
    // See also: man 7 signal-safety
    static mut CHILD_PID: Option<libc::c_int> = None;
    extern "C" fn sigh(sig: libc::c_int) {
        println!("[supervisor:{}] got signal {sig}", unistd::getpid());
        unsafe {
            match CHILD_PID {
                Some(pid) => match libc::kill(pid, sig) {
                    0 => (),
                    _ => std::process::exit(0),
                },
                None => std::process::exit(0),
            };
        }
    }
    let sigaction = signal::SigAction::new(
        signal::SigHandler::Handler(sigh),
        signal::SaFlags::SA_RESTART,
        signal::SigSet::empty(),
    );
    unsafe {
        signal::sigaction(signal::SIGHUP, &sigaction).unwrap();
        signal::sigaction(signal::SIGINT, &sigaction).unwrap();
        signal::sigaction(signal::SIGTERM, &sigaction).unwrap();
        signal::sigaction(signal::SIGUSR1, &sigaction).unwrap();
    }

    let parent = unistd::getpid();
    let mut entrypoint = Entrypoint::StartDiscover {};
    loop {
        println!("[supervisor:{parent}] running {entrypoint:?}");

        let (from_child, to_parent) =
            ipc::channel::<IpcMessage>().expect("ipc channel creation failed");
        let (from_parent, to_child) = ipc::pipe().expect("ipc pipe creation failed");

        let pid = unsafe { fork() };
        match pid.expect("fork failed") {
            ForkResult::Child => {
                drop(from_child);
                drop(to_child);

                let rc = tarantool_main!(
                    args.tt_args().unwrap(),
                    callback_data: (entrypoint, args, to_parent, from_parent),
                    callback_data_type: (Entrypoint, args::Run, ipc::Sender<IpcMessage>, ipc::Fd),
                    callback_body: {
                        // We don't want a child to live without a supervisor.
                        //
                        // Usually, supervisor waits for child forever and retransmits
                        // termination signals. But if the parent is killed with a SIGKILL
                        // there's no way to pass anything.
                        //
                        // This fiber serves as a fuse - it tries to read from a pipe
                        // (that supervisor never writes to), and if the writing end is
                        // closed, it means the supervisor has terminated.
                        let fuse = fiber::Builder::new()
                            .name("supervisor_fuse")
                            .func(move || {
                                use ::tarantool::ffi::tarantool::CoIOFlags;
                                use ::tarantool::coio::coio_wait;
                                coio_wait(from_parent.0, CoIOFlags::READ, f64::INFINITY).ok();
                                tlog!(Warning, "Supervisor terminated, exiting");
                                std::process::exit(0);
                        });
                        std::mem::forget(fuse.start());

                        entrypoint.exec(args, to_parent)
                    }
                );
                std::process::exit(rc);
            }
            ForkResult::Parent { child } => {
                unsafe { CHILD_PID = Some(child.into()) };
                drop(from_parent);
                drop(to_parent);

                let msg = from_child.recv().ok();

                let status = waitpid(child, None);

                // Restore termios configuration as planned
                if let Some(tcattr) = tcattr.as_ref() {
                    tcsetattr(0, TCSADRAIN, tcattr).unwrap();
                }

                println!("[supervisor:{parent}] subprocess finished: {status:?}");

                if let Some(msg) = msg {
                    entrypoint = msg.next_entrypoint;
                    if msg.drop_db {
                        println!("[supervisor:{parent}] subprocess requested rebootstrap");
                        rm_tarantool_files(&args.data_dir);
                    }
                } else {
                    let rc = match status.unwrap() {
                        WaitStatus::Exited(_, rc) => rc,
                        WaitStatus::Signaled(_, sig, _) => sig as _,
                        _ => unreachable!("unexpected exit status"),
                    };
                    std::process::exit(rc);
                }
            }
        };
    }
}

fn init_common(args: &args::Run, cfg: &tarantool::Cfg) -> RaftSpaceAccess {
    std::fs::create_dir_all(&args.data_dir).unwrap();
    tarantool::set_cfg(cfg);

    traft::Storage::init_schema();
    let storage = RaftSpaceAccess::new().expect("RaftSpaceAccess initialization failed");
    init_handlers();
    traft::event::init();
    storage
}

fn start_discover(args: &args::Run, to_supervisor: ipc::Sender<IpcMessage>) {
    tlog!(Info, ">>>>> start_discover()");

    picolib_setup(args);
    assert!(tarantool::cfg().is_none());

    // Don't try to guess instance and replicaset uuids now,
    // finally, the box will be rebootstraped after discovery.
    let mut cfg = tarantool::Cfg {
        listen: None,
        read_only: false,
        wal_dir: args.data_dir.clone(),
        memtx_dir: args.data_dir.clone(),
        log_level: args.log_level() as u8,
        ..Default::default()
    };

    let storage = init_common(args, &cfg);
    discovery::init_global(&args.peers);

    cfg.listen = Some(args.listen.clone());
    tarantool::set_cfg(&cfg);

    // TODO assert traft::Storage::instance_id == (null || args.instance_id)
    if storage.raft_id().unwrap().is_some() {
        return postjoin(args, storage);
    }

    let role = discovery::wait_global();
    match role {
        discovery::Role::Leader { .. } => {
            let next_entrypoint = Entrypoint::StartBoot {};
            let msg = IpcMessage {
                next_entrypoint,
                drop_db: true,
            };
            to_supervisor.send(&msg);
            std::process::exit(0);
        }
        discovery::Role::NonLeader { leader } => {
            let next_entrypoint = Entrypoint::StartJoin {
                leader_address: leader,
            };
            let msg = IpcMessage {
                next_entrypoint,
                drop_db: true,
            };
            to_supervisor.send(&msg);
            std::process::exit(0);
        }
    };
}

fn start_boot(args: &args::Run) {
    tlog!(Info, ">>>>> start_boot()");

    let peer = traft::topology::initial_peer(
        args.instance_id.clone(),
        args.replicaset_id.clone(),
        args.advertise_address(),
        args.failure_domain(),
    );
    let raft_id = peer.raft_id;
    let instance_id = peer.instance_id.clone();

    picolib_setup(args);
    assert!(tarantool::cfg().is_none());

    let cfg = tarantool::Cfg {
        listen: None,
        read_only: false,
        instance_uuid: Some(peer.instance_uuid.clone()),
        replicaset_uuid: Some(peer.replicaset_uuid.clone()),
        wal_dir: args.data_dir.clone(),
        memtx_dir: args.data_dir.clone(),
        log_level: args.log_level() as u8,
        ..Default::default()
    };

    let mut storage = init_common(args, &cfg);

    start_transaction(|| -> Result<(), TntError> {
        let cs = raft::ConfState {
            voters: vec![raft_id],
            ..Default::default()
        };

        let mut lc = LogicalClock::new(raft_id, 0);

        let mut init_entries = Vec::new();
        init_entries.push({
            let ctx = traft::EntryContextNormal {
                op: traft::Op::PersistPeer { peer },
                lc,
            };
            let e = traft::Entry {
                entry_type: raft::EntryType::EntryNormal,
                index: (init_entries.len() + 1) as _,
                term: 1,
                data: vec![],
                context: Some(traft::EntryContext::Normal(ctx)),
            };

            raft::Entry::try_from(e).unwrap()
        });

        lc.inc();
        init_entries.push({
            let ctx = traft::EntryContextNormal {
                op: traft::OpDML::insert(
                    ClusterSpace::State,
                    &(StateKey::ReplicationFactor, args.init_replication_factor),
                )
                .expect("cannot fail")
                .into(),
                lc,
            };
            let e = traft::Entry {
                entry_type: raft::EntryType::EntryNormal,
                index: (init_entries.len() + 1) as _,
                term: 1,
                data: vec![],
                context: Some(traft::EntryContext::Normal(ctx)),
            };

            raft::Entry::try_from(e).unwrap()
        });

        init_entries.push({
            let conf_change = raft::ConfChange {
                change_type: raft::ConfChangeType::AddNode,
                node_id: raft_id,
                ..Default::default()
            };
            let e = traft::Entry {
                entry_type: raft::EntryType::EntryConfChange,
                index: (init_entries.len() + 1) as _,
                term: 1,
                data: conf_change.write_to_bytes().unwrap(),
                context: None,
            };

            raft::Entry::try_from(e).unwrap()
        });

        storage.persist_conf_state(&cs).unwrap();
        storage.persist_entries(&init_entries).unwrap();
        storage.persist_raft_id(raft_id).unwrap();
        storage.persist_instance_id(&instance_id).unwrap();
        storage.persist_cluster_id(&args.cluster_id).unwrap();

        let mut hs = raft::HardState::default();
        hs.set_commit(init_entries.len() as _);
        hs.set_term(1);
        storage.persist_hard_state(&hs).unwrap();
        Ok(())
    })
    .unwrap();

    postjoin(args, storage)
}

fn start_join(args: &args::Run, leader_address: String) {
    tlog!(Info, ">>>>> start_join({leader_address})");

    let req = traft::JoinRequest {
        cluster_id: args.cluster_id.clone(),
        instance_id: args.instance_id.clone(),
        replicaset_id: args.replicaset_id.clone(),
        advertise_address: args.advertise_address(),
        failure_domain: args.failure_domain(),
    };

    // Arch memo.
    // - There must be no timeouts. Retrying may lead to flooding the
    //   topology with phantom instances. No worry, specifying a
    //   particular `instance_id` for every instance protects from that
    //   flood.
    // - It's fine to retry "connection refused" errors.
    // - TODO renew leader_address if the current one says it's not a
    //   leader.
    let fn_name = stringify_cfunc!(traft::node::raft_join);
    let resp: traft::JoinResponse = loop {
        let now = Instant::now();
        // TODO: exponential decay
        let timeout = Duration::from_secs(1);
        match tarantool::net_box_call(&leader_address, fn_name, &req, Duration::MAX) {
            Err(TntError::IO(e)) => {
                tlog!(Warning, "join request failed: {e}");
                fiber::sleep(timeout.saturating_sub(now.elapsed()));
                continue;
            }
            Err(e) => {
                tlog!(Error, "join request failed: {e}");
                std::process::exit(-1);
            }
            Ok(resp) => {
                break resp;
            }
        }
    };

    picolib_setup(args);
    assert!(tarantool::cfg().is_none());

    let cfg = tarantool::Cfg {
        listen: Some(args.listen.clone()),
        read_only: false,
        instance_uuid: Some(resp.peer.instance_uuid.clone()),
        replicaset_uuid: Some(resp.peer.replicaset_uuid.clone()),
        replication: resp.box_replication.clone(),
        wal_dir: args.data_dir.clone(),
        memtx_dir: args.data_dir.clone(),
        log_level: args.log_level() as u8,
        ..Default::default()
    };

    let mut storage = init_common(args, &cfg);

    let raft_id = resp.peer.raft_id;
    start_transaction(|| -> Result<(), TntError> {
        traft::Storage::persist_peer(&resp.peer).unwrap();
        for peer in resp.raft_group {
            traft::Storage::persist_peer(&peer).unwrap();
        }
        storage.persist_raft_id(raft_id).unwrap();
        storage.persist_instance_id(&resp.peer.instance_id).unwrap();
        storage.persist_cluster_id(&args.cluster_id).unwrap();
        Ok(())
    })
    .unwrap();

    postjoin(args, storage)
}

fn postjoin(args: &args::Run, storage: RaftSpaceAccess) {
    tlog!(Info, ">>>>> postjoin()");

    let mut box_cfg = tarantool::cfg().unwrap();

    // Reset the quorum BEFORE initializing the raft node.
    // Otherwise it may stuck on `box.cfg({replication})` call.
    box_cfg.replication_connect_quorum = 0;
    tarantool::set_cfg(&box_cfg);

    let node = traft::node::Node::new(storage.clone());
    let node = node.expect("failed initializing raft node");
    traft::node::set_global(node);
    let node = traft::node::global().unwrap();
    let raft_id = node.raft_id();

    let cs = storage.conf_state().unwrap();
    if cs.voters == [raft_id] {
        tlog!(
            Info,
            concat!(
                "this is the only voter in cluster, ",
                "triggering election immediately"
            )
        );

        node.tick_and_yield(1); // apply configuration, if any
        node.campaign_and_yield().ok(); // trigger election immediately
        assert_eq!(node.status().raft_state, "Leader");
    }

    box_cfg.listen = Some(args.listen.clone());
    tarantool::set_cfg(&box_cfg);

    tlog!(Debug, "Getting a read barrier...");
    loop {
        if node.status().leader_id == None {
            // This check doesn't guarantee anything. It only eliminates
            // unnecesary requests that will fail for sure. For example,
            // re-election still may be abrupt while `node.read_index()`
            // implicitly yields.
            node.wait_status();
            continue;
        }

        let timeout = Duration::from_secs(10);
        if let Err(e) = node.wait_for_read_state(timeout) {
            tlog!(Debug, "unable to get a read barrier: {e}");
            fiber::sleep(Duration::from_millis(100));
            continue;
        } else {
            break;
        }
    }
    tlog!(Info, "Read barrier aquired, raft is ready");

    if let Err(e) = tarantool::on_shutdown(traft::failover::on_shutdown) {
        tlog!(Error, "failed setting on_shutdown trigger: {e}");
    }

    let peer = traft::Storage::peer_by_raft_id(raft_id).unwrap().unwrap();
    box_cfg.replication = traft::Storage::box_replication(&peer.replicaset_id, None).unwrap();
    tarantool::set_cfg(&box_cfg);

    loop {
        let peer = traft::Storage::peer_by_raft_id(raft_id)
            .unwrap()
            .expect("peer must be persisted at the time of postjoin");
        let cluster_id = storage
            .cluster_id()
            .unwrap()
            .expect("cluster_id must be persisted at the time of postjoin");

        tlog!(Info, "initiating self-activation of {}", peer.instance_id);
        let req = UpdatePeerRequest::new(peer.instance_id, cluster_id)
            .with_grade(Grade::Online)
            .with_target_grade(TargetGrade::Online)
            .with_failure_domain(args.failure_domain());

        let leader_id = node.status().leader_id.expect("leader_id deinitialized");
        let leader = traft::Storage::peer_by_raft_id(leader_id).unwrap().unwrap();

        // It's necessary to call `raft_update_peer` remotely on a
        // leader over net_box. It always fails otherwise. Only the
        // leader is permitted to propose PersistPeer entries.
        let fn_name = stringify_cfunc!(traft::failover::raft_update_peer);
        let now = Instant::now();
        let timeout = Duration::from_secs(10);
        match tarantool::net_box_call(&leader.peer_address, fn_name, &req, timeout) {
            Err(TntError::IO(e)) => {
                tlog!(Warning, "failed to activate myself: {e}");
                fiber::sleep(timeout.saturating_sub(now.elapsed()));
                continue;
            }
            Err(e) => {
                tlog!(Error, "failed to activate myself: {e}");
                std::process::exit(-1);
            }
            Ok(traft::UpdatePeerResponse { .. }) => {
                break;
            }
        };
    }

    node.mark_as_ready();
}

fn main_tarantool(args: args::Tarantool) -> ! {
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

    std::process::exit(rc);
}

fn main_expel(args: args::Expel) -> ! {
    let rc = tarantool_main!(
        args.tt_args().unwrap(),
        callback_data: (args,),
        callback_data_type: (args::Expel,),
        callback_body: {
            tt_expel(args)
        }
    );
    std::process::exit(rc);
}

fn tt_expel(args: args::Expel) {
    let fn_name = stringify_cfunc!(traft::node::raft_expel);
    let req = ExpelRequest {
        cluster_id: args.cluster_id,
        instance_id: args.instance_id,
    };
    match tarantool::net_box_call(&args.peer, fn_name, &req, Duration::MAX) {
        Ok::<traft::ExpelResponse, _>(_resp) => {
            tlog!(Info, "Success expel call");
            std::process::exit(0);
        }
        Err(e) => {
            tlog!(Error, "Failed to expel instance: {e}");
            std::process::exit(-1);
        }
    }
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

fn main_test(args: args::Test) -> ! {
    cleanup_env!();

    const PASSED: &str = color![green "ok" clear];
    const FAILED: &str = color![red "FAILED" clear];
    let mut cnt_passed = 0u32;
    let mut cnt_failed = 0u32;
    let mut cnt_skipped = 0u32;

    let now = std::time::Instant::now();

    println!();
    println!(
        "total {} tests",
        inventory::iter::<InnerTest>.into_iter().count()
    );
    for t in inventory::iter::<InnerTest> {
        if let Some(filter) = args.filter.as_ref() {
            if !t.name.contains(filter) {
                cnt_skipped += 1;
                continue;
            }
        }
        print!("test {} ... ", t.name);

        let (mut rx, tx) = ipc::pipe().expect("pipe creation failed");
        let pid = unsafe { fork() };
        match pid.expect("fork failed") {
            ForkResult::Child => {
                drop(rx);
                unistd::close(0).ok(); // stdin
                if !args.nocapture {
                    unistd::dup2(*tx, 1).ok(); // stdout
                    unistd::dup2(*tx, 2).ok(); // stderr
                }
                drop(tx);

                let rc = tarantool_main!(
                    args.tt_args().unwrap(),
                    callback_data: t,
                    callback_data_type: &InnerTest,
                    callback_body: test_one(t)
                );
                std::process::exit(rc);
            }
            ForkResult::Parent { child } => {
                drop(tx);
                let log = {
                    let mut buf = Vec::new();
                    use std::io::Read;
                    if !args.nocapture {
                        rx.read_to_end(&mut buf)
                            .map_err(|e| println!("error reading ipc pipe: {e}"))
                            .ok();
                    }
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

                    if args.nocapture {
                        continue;
                    }

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
    print!(" {cnt_skipped} skipped;");
    println!(" finished in {:.2}s", now.elapsed().as_secs_f32());
    println!();

    std::process::exit(!ok as _);
}

fn test_one(t: &InnerTest) {
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
    )
    .unwrap();

    (t.body)();
    std::process::exit(0i32);
}
