use serde::{Deserialize, Serialize};

use ::raft::prelude as raft;
use ::tarantool::error::Error as TntError;
use ::tarantool::fiber;
use ::tarantool::tlua;
use ::tarantool::transaction::start_transaction;
use std::convert::TryFrom;
use std::time::{Duration, Instant};
use storage::Clusterwide;
use storage::ToEntryIter as _;
use storage::{ClusterwideSpace, PropertyName};
use traft::rpc;
use traft::rpc::{join, update_instance};
use traft::RaftSpaceAccess;

use protobuf::Message as _;

use crate::instance::grade::TargetGradeVariant;
use crate::instance::InstanceId;
use crate::tlog::set_log_level;
use crate::traft::event::Event;
use crate::traft::op::{self, Op};
use crate::traft::{event, node, Migration};
use crate::traft::{LogicalClock, RaftIndex};
use traft::error::Error;

#[doc(hidden)]
mod app;
pub mod args;
pub mod discovery;
pub mod failure_domain;
pub mod governor;
pub mod instance;
pub mod ipc;
pub mod kvcell;
pub mod r#loop;
pub mod mailbox;
pub mod on_shutdown;
pub mod replicaset;
pub mod storage;
pub mod tarantool;
pub mod tlog;
pub mod traft;
pub mod util;

fn picolib_setup(args: &args::Run) {
    set_log_level(args.log_level());
    let l = ::tarantool::lua_state();
    l.exec(
        "package.loaded.pico = {}
        _G.pico = package.loaded.pico
        pico.space = setmetatable({}, { __index =
            function(self, space_name)
                return box.space['_picodata_' .. space_name]
            end
        })
        ",
    )
    .unwrap();
    let luamod: tlua::LuaTable<_> = l.get("pico").unwrap();

    luamod.set("VERSION", env!("CARGO_PKG_VERSION"));
    luamod.set("args", args);

    luamod.set(
        "whoami",
        tlua::function0(|| -> traft::Result<_> {
            let node = traft::node::global()?;
            let raft_storage = &node.raft_storage;

            Ok(tlua::AsTable((
                ("raft_id", raft_storage.raft_id()?),
                ("cluster_id", raft_storage.cluster_id()?),
                ("instance_id", raft_storage.instance_id()?),
            )))
        }),
    );

    luamod.set(
        "instance_info",
        tlua::function1(|iid: Option<String>| -> traft::Result<_> {
            let node = traft::node::global()?;
            let iid = iid.unwrap_or(node.raft_storage.instance_id()?.unwrap());
            let instance = node.storage.instances.get(&InstanceId::from(iid))?;
            let peer_address = node
                .storage
                .peer_addresses
                .get(instance.raft_id)?
                .unwrap_or_else(|| "<unknown>".into());

            Ok(tlua::AsTable((
                ("raft_id", instance.raft_id),
                ("advertise_address", peer_address),
                ("instance_id", instance.instance_id.0),
                ("instance_uuid", instance.instance_uuid),
                ("replicaset_id", instance.replicaset_id),
                ("replicaset_uuid", instance.replicaset_uuid),
                ("current_grade", instance.current_grade),
                ("target_grade", instance.target_grade),
            )))
        }),
    );

    luamod.set(
        "raft_status",
        tlua::function0(|| traft::node::global().map(|n| n.status())),
    );
    luamod.set(
        "raft_tick",
        tlua::function1(|n_times: u32| -> traft::Result<()> {
            traft::node::global()?.tick_and_yield(n_times);
            Ok(())
        }),
    );
    luamod.set(
        "raft_read_index",
        tlua::function1(|timeout: f64| -> traft::Result<RaftIndex> {
            traft::node::global()?.wait_for_read_state(Duration::from_secs_f64(timeout))
        }),
    );
    luamod.set(
        "raft_propose_nop",
        tlua::function0(|| {
            traft::node::global()?.propose_and_wait(Op::Nop, Duration::from_secs(1))
        }),
    );
    luamod.set(
        "raft_propose_info",
        tlua::function1(|x: String| -> traft::Result<()> {
            traft::node::global()?.propose_and_wait(Op::Info { msg: x }, Duration::from_secs(1))
        }),
    );
    luamod.set(
        "raft_timeout_now",
        tlua::function0(|| -> traft::Result<()> {
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
        tlua::function1(|instance_id: InstanceId| -> traft::Result<()> {
            let raft_storage = &traft::node::global()?.raft_storage;
            let cluster_id = raft_storage
                .cluster_id()?
                .expect("cluster_id is set on boot");
            rpc::net_box_call_to_leader(
                &rpc::expel::Request {
                    instance_id,
                    cluster_id,
                },
                Duration::MAX,
            )?;
            Ok(())
        }),
    );
    #[derive(::tarantool::tlua::LuaRead)]
    struct ProposeEvalOpts {
        timeout: Option<f64>,
    }
    luamod.set(
        "raft_propose_eval",
        tlua::function2(
            |x: String, opts: Option<ProposeEvalOpts>| -> traft::Result<()> {
                let timeout = opts.and_then(|opts| opts.timeout).unwrap_or(10.0);
                traft::node::global()?
                    .propose_and_wait(op::EvalLua { code: x }, Duration::from_secs_f64(timeout))
                    .and_then(|res| res.map_err(Into::into))
            },
        ),
    );
    luamod.set(
        "raft_return_one",
        tlua::function1(|timeout: f64| -> traft::Result<u8> {
            traft::node::global()?.propose_and_wait(op::ReturnOne, Duration::from_secs_f64(timeout))
        }),
    );
    // TODO: remove this
    if cfg!(debug_assertions) {
        luamod.set(
            "emit",
            tlua::Function::new(|event: String| -> traft::Result<()> {
                let event: traft::event::Event = event.parse().map_err(Error::other)?;
                traft::event::broadcast(event);
                Ok(())
            }),
        );
        luamod.set(
            "vshard_cfg",
            tlua::function0(|| -> traft::Result<traft::rpc::sharding::cfg::Cfg> {
                let node = traft::node::global()?;
                traft::rpc::sharding::cfg::Cfg::from_storage(&node.storage)
            }),
        );
        l.exec(
            "
            pico.test_space = function(name)
                local s = box.schema.space.create(name, {is_sync = true, if_not_exists = true})
                s:create_index('pk', {if_not_exists = true})
                return s
            end
        ",
        )
        .unwrap();
    }
    luamod.set("log", &[()]);
    #[rustfmt::skip]
    l.exec_with(
        "pico.log.highlight_key = ...",
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
        "pico.log.clear_highlight = ...",
        tlua::function0(tlog::clear_highlight),
    )
    .unwrap();

    #[derive(::tarantool::tlua::LuaRead, Default, Clone, Copy)]
    enum Justify {
        Left,
        #[default]
        Center,
        Right,
    }
    #[derive(::tarantool::tlua::LuaRead)]
    struct RaftLogOpts {
        return_string: Option<bool>,
        justify_contents: Option<Justify>,
    }
    luamod.set(
        "raft_log",
        tlua::function1(
            |opts: Option<RaftLogOpts>| -> traft::Result<Option<String>> {
                let mut return_string = false;
                let mut justify_contents = Default::default();
                if let Some(opts) = opts {
                    return_string = opts.return_string.unwrap_or(false);
                    justify_contents = opts.justify_contents.unwrap_or_default();
                }
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
                            .map(|lc| lc.to_string())
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
                let cols = if return_string {
                    256
                } else {
                    util::screen_size().1 as usize
                };
                if total_width > cols {
                    match cw.checked_sub(total_width - cols) {
                        Some(new_cw) if new_cw > 0 => cw = new_cw,
                        _ => {
                            return Err(Error::other("screen too small"));
                        }
                    }
                }

                use std::io::Write;
                let mut buf: Vec<u8> = Vec::with_capacity(512);
                let write_contents = move |buf: &mut Vec<u8>, contents: &str| match justify_contents
                {
                    Justify::Left => writeln!(buf, "{contents: <cw$}|"),
                    Justify::Center => writeln!(buf, "{contents: ^cw$}|"),
                    Justify::Right => writeln!(buf, "{contents: >cw$}|"),
                };

                let row_sep = |buf: &mut Vec<u8>| {
                    match justify_contents {
                        Justify::Left => {
                            writeln!(buf, "+{0:-^iw$}+{0:-^tw$}+{0:-^lw$}+{0:-<cw$}+", "")
                        }
                        Justify::Center => {
                            writeln!(buf, "+{0:-^iw$}+{0:-^tw$}+{0:-^lw$}+{0:-^cw$}+", "")
                        }
                        Justify::Right => {
                            writeln!(buf, "+{0:-^iw$}+{0:-^tw$}+{0:-^lw$}+{0:->cw$}+", "")
                        }
                    }
                    .unwrap()
                };
                row_sep(&mut buf);
                write!(buf, "|{index: ^iw$}|{term: ^tw$}|{lc: ^lw$}|").unwrap();
                write_contents(&mut buf, contents).unwrap();
                row_sep(&mut buf);
                for [index, term, lc, contents] in rows {
                    if contents.len() <= cw {
                        write!(buf, "|{index: ^iw$}|{term: ^tw$}|{lc: ^lw$}|").unwrap();
                        write_contents(&mut buf, &contents).unwrap();
                    } else {
                        write!(buf, "|{index: ^iw$}|{term: ^tw$}|{lc: ^lw$}|").unwrap();
                        write_contents(&mut buf, &contents[..cw]).unwrap();
                        let mut rest = &contents[cw..];
                        while !rest.is_empty() {
                            let clamped_cw = usize::min(rest.len(), cw);
                            write!(
                                buf,
                                "|{blank: ^iw$}|{blank: ^tw$}|{blank: ^lw$}|",
                                blank = "~",
                            )
                            .unwrap();
                            write_contents(&mut buf, &rest[..clamped_cw]).unwrap();
                            rest = &rest[clamped_cw..];
                        }
                    }
                }
                row_sep(&mut buf);
                if return_string {
                    Ok(Some(String::from_utf8_lossy(&buf).into()))
                } else {
                    std::io::stdout().write_all(&buf).unwrap();
                    Ok(None)
                }
            },
        ),
    );

    luamod.set(
        "add_migration",
        tlua::function2(|id: u64, body: String| -> traft::Result<()> {
            let migration = Migration { id, body };
            let op = op::Dml::insert(ClusterwideSpace::Migration, &migration)?;
            node::global()?.propose_and_wait(op, Duration::MAX)??;
            Ok(())
        }),
    );

    luamod.set(
        "push_schema_version",
        tlua::function1(|id: u64| -> traft::Result<()> {
            let op = op::Dml::replace(
                ClusterwideSpace::Property,
                &(PropertyName::DesiredSchemaVersion, id),
            )?;
            node::global()?.propose_and_wait(op, Duration::MAX)??;
            Ok(())
        }),
    );

    luamod.set(
        "migrate",
        tlua::Function::new(
            |m_id: Option<u64>, timeout: Option<f64>| -> traft::Result<Option<u64>> {
                let node = node::global()?;

                let Some(latest) = node.storage.migrations.get_latest()? else {
                    tlog!(Info, "there are no migrations to apply");
                    return Ok(None);
                };
                let current_version = node.storage.properties.desired_schema_version()?;
                let target_version = m_id.map(|id| id.min(latest.id)).unwrap_or(latest.id);
                if target_version <= current_version {
                    return Ok(Some(current_version));
                }

                let op = op::Dml::replace(
                    ClusterwideSpace::Property,
                    &(PropertyName::DesiredSchemaVersion, target_version),
                )?;
                node.propose_and_wait(op, Duration::MAX)??;

                let deadline = {
                    let timeout = timeout
                        .map(Duration::from_secs_f64)
                        .unwrap_or(Duration::MAX);
                    let now = Instant::now();
                    now.checked_add(timeout)
                        .unwrap_or_else(|| now + Duration::from_secs(30 * 365 * 24 * 60 * 60))
                };
                while node
                    .storage
                    .replicasets
                    .iter()?
                    .any(|r| r.current_schema_version < target_version)
                {
                    if event::wait_deadline(Event::MigrateDone, deadline)?.is_timeout() {
                        return Err(Error::Timeout);
                    }
                }

                Ok(Some(node.storage.properties.desired_schema_version()?))
            },
        ),
    );
}

macro_rules! lua_preload {
    ($lua:ident, $module:literal, $path_prefix:literal, $path:literal) => {
        $lua.exec_with(
            "local module, path, code = ...
            package.preload[module] = load(code, '@'..path)",
            ($module, $path, include_str!(concat!($path_prefix, $path))),
        )
        .unwrap();
    };
}

fn preload_vshard() {
    let lua = ::tarantool::lua_state();

    macro_rules! preload {
        ($module:literal, $path:literal) => {
            lua_preload!(lua, $module, "../vshard/", $path)
        };
    }

    preload!("vshard", "vshard/init.lua");
    preload!("vshard.cfg", "vshard/cfg.lua");
    preload!("vshard.consts", "vshard/consts.lua");
    preload!("vshard.error", "vshard/error.lua");
    preload!("vshard.hash", "vshard/hash.lua");
    preload!("vshard.heap", "vshard/heap.lua");
    preload!("vshard.registry", "vshard/registry.lua");
    preload!("vshard.replicaset", "vshard/replicaset.lua");
    preload!("vshard.rlist", "vshard/rlist.lua");
    preload!("vshard.router", "vshard/router/init.lua");
    preload!("vshard.storage", "vshard/storage/init.lua");
    preload!("vshard.storage.ref", "vshard/storage/ref.lua");
    preload!(
        "vshard.storage.reload_evolution",
        "vshard/storage/reload_evolution.lua"
    );
    preload!("vshard.storage.sched", "vshard/storage/sched.lua");
    preload!("vshard.util", "vshard/util.lua");
    preload!("vshard.version", "vshard/version.lua");
}

#[link(name = "httpd")]
extern "C" {
    fn luaopen_http_lib(lua_state: tlua::LuaState) -> libc::c_int;
}

fn preload_http() {
    let lua = ::tarantool::lua_state();

    // Load C part of the library
    lua.exec_with(
        "package.preload['http.lib'] = ...;",
        tlua::CFunction::new(luaopen_http_lib),
    )
    .unwrap();

    macro_rules! preload {
        ($module:literal, $path:literal) => {
            lua_preload!(lua, $module, "../http/", $path);
        };
    }

    preload!("http.server", "http/server.lua");
    preload!("http.codes", "http/codes.lua");
    preload!("http.mime_types", "http/mime_types.lua");
}

fn start_http_server(address: &str) {
    tlog!(Info, "starting http server at {address}");
    let lua = ::tarantool::lua_state();
    let (host, port) = address
        .rsplit_once(':')
        .expect("this part should have been checked at args parse");
    let port: u16 = port
        .parse()
        .expect("this part should have been checked at args parse");
    lua.exec_with(
        "local host, port = ...;
        local httpd = require('http.server').new(host, port);
        httpd:start();
        _G.pico.httpd = httpd",
        (host, port),
    )
    .expect("failed to start http server")
}

fn init_handlers() -> traft::Result<()> {
    tarantool::exec(
        r#"
        box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
        "#,
    )
    .unwrap();

    let lua = ::tarantool::lua_state();
    for proc in ::tarantool::proc::all_procs().iter() {
        lua.exec_with(
            "box.schema.func.create('.' .. ...,
                { language = 'C', if_not_exists = true }
            );",
            proc.name(),
        )
        .map_err(::tarantool::tlua::LuaError::from)?;
    }

    Ok(())
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Serialize, Deserialize)]
pub enum Entrypoint {
    StartDiscover,
    StartBoot,
    StartJoin { leader_address: String },
}

impl Entrypoint {
    pub fn exec(self, args: args::Run, to_supervisor: ipc::Sender<IpcMessage>) {
        match self {
            Self::StartDiscover => start_discover(&args, to_supervisor),
            Self::StartBoot => start_boot(&args),
            Self::StartJoin { leader_address } => start_join(&args, leader_address),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IpcMessage {
    pub next_entrypoint: Entrypoint,
    pub drop_db: bool,
}

/// Performs tarantool initialization calling `box.cfg` for the first time.
///
/// This function is called from:
///
/// - `start_discover`
/// - `start_boot`
/// - `start_join`
///
fn init_common(args: &args::Run, cfg: &tarantool::Cfg) -> (Clusterwide, RaftSpaceAccess) {
    std::fs::create_dir_all(&args.data_dir).unwrap();
    tarantool::set_cfg(cfg);

    // Load Lua libraries
    preload_vshard();
    preload_http();

    init_handlers().expect("failed initializing rpc handlers");
    traft::event::init();
    let storage = Clusterwide::new().expect("clusterwide storage initialization failed");
    let raft_storage = RaftSpaceAccess::new().expect("raft storage initialization failed");
    (storage, raft_storage)
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

    let (storage, raft_storage) = init_common(args, &cfg);
    discovery::init_global(&args.peers);

    cfg.listen = Some(args.listen.clone());
    tarantool::set_cfg(&cfg);

    // TODO assert traft::Storage::instance_id == (null || args.instance_id)
    if raft_storage.raft_id().unwrap().is_some() {
        tarantool::set_cfg_field("read_only", true).unwrap();
        return postjoin(args, storage, raft_storage);
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

    let (instance, address, _) = traft::topology::initial_instance(
        args.instance_id.clone(),
        args.replicaset_id.clone(),
        args.advertise_address(),
        args.failure_domain(),
    )
    .expect("failed adding initial instance");
    let raft_id = instance.raft_id;
    let instance_id = instance.instance_id.clone();

    picolib_setup(args);
    assert!(tarantool::cfg().is_none());

    let cfg = tarantool::Cfg {
        listen: None,
        read_only: false,
        instance_uuid: Some(instance.instance_uuid.clone()),
        replicaset_uuid: Some(instance.replicaset_uuid.clone()),
        wal_dir: args.data_dir.clone(),
        memtx_dir: args.data_dir.clone(),
        log_level: args.log_level() as u8,
        ..Default::default()
    };

    let (storage, raft_storage) = init_common(args, &cfg);

    let cs = raft::ConfState {
        voters: vec![raft_id],
        ..Default::default()
    };

    let init_entries: Vec<raft::Entry> = {
        let mut lc = LogicalClock::new(raft_id, 0);
        let mut init_entries = Vec::new();

        let mut init_entries_push_op = |op| {
            lc.inc();

            let ctx = traft::EntryContextNormal { op, lc };
            let e = traft::Entry {
                entry_type: raft::EntryType::EntryNormal,
                index: (init_entries.len() + 1) as _,
                term: traft::INIT_RAFT_TERM,
                data: vec![],
                context: Some(traft::EntryContext::Normal(ctx)),
            };

            init_entries.push(raft::Entry::try_from(e).unwrap());
        };

        init_entries_push_op(
            op::Dml::insert(
                ClusterwideSpace::Address,
                &traft::PeerAddress { raft_id, address },
            )
            .expect("cannot fail")
            .into(),
        );
        init_entries_push_op(traft::op::PersistInstance::new(instance).into());
        init_entries_push_op(
            op::Dml::insert(
                ClusterwideSpace::Property,
                &(
                    PropertyName::ReplicationFactor,
                    args.init_replication_factor,
                ),
            )
            .expect("cannot fail")
            .into(),
        );
        init_entries_push_op(
            op::Dml::insert(
                ClusterwideSpace::Property,
                &(PropertyName::DesiredSchemaVersion, 0),
            )
            .expect("cannot fail")
            .into(),
        );

        init_entries.push({
            let conf_change = raft::ConfChange {
                change_type: raft::ConfChangeType::AddNode,
                node_id: raft_id,
                ..Default::default()
            };
            let e = traft::Entry {
                entry_type: raft::EntryType::EntryConfChange,
                index: (init_entries.len() + 1) as _,
                term: traft::INIT_RAFT_TERM,
                data: conf_change.write_to_bytes().unwrap(),
                context: None,
            };

            raft::Entry::try_from(e).unwrap()
        });

        init_entries
    };

    let hs = raft::HardState {
        term: traft::INIT_RAFT_TERM,
        commit: init_entries.len() as _,
        ..Default::default()
    };

    start_transaction(|| -> Result<(), TntError> {
        raft_storage.persist_raft_id(raft_id).unwrap();
        raft_storage.persist_instance_id(&instance_id).unwrap();
        raft_storage.persist_cluster_id(&args.cluster_id).unwrap();
        raft_storage.persist_entries(&init_entries).unwrap();
        raft_storage.persist_conf_state(&cs).unwrap();
        raft_storage.persist_hard_state(&hs).unwrap();
        Ok(())
    })
    .unwrap();

    postjoin(args, storage, raft_storage)
}

fn start_join(args: &args::Run, leader_address: String) {
    tlog!(Info, ">>>>> start_join({leader_address})");

    let req = join::Request {
        cluster_id: args.cluster_id.clone(),
        instance_id: args.instance_id.clone(),
        replicaset_id: args.replicaset_id.clone(),
        advertise_address: args.advertise_address(),
        failure_domain: args.failure_domain(),
    };

    let mut leader_address = leader_address;

    // Arch memo.
    // - There must be no timeouts. Retrying may lead to flooding the
    //   topology with phantom instances. No worry, specifying a
    //   particular `instance_id` for every instance protects from that
    //   flood.
    // - It's fine to retry "connection refused" errors.
    // - TODO renew leader_address if the current one says it's not a
    //   leader.
    let resp: traft::rpc::join::OkResponse = loop {
        let now = Instant::now();
        // TODO: exponential decay
        let timeout = Duration::from_secs(1);
        match rpc::net_box_call(&leader_address, &req, Duration::MAX) {
            Ok(join::Response::Ok(resp)) => {
                break resp;
            }
            Ok(join::Response::ErrNotALeader(maybe_new_leader)) => {
                tlog!(Warning, "join request failed: not a leader, retry...");
                if let Some(new_leader) = maybe_new_leader {
                    leader_address = new_leader.address;
                } else {
                    fiber::sleep(Duration::from_millis(100));
                }
                continue;
            }
            Err(TntError::IO(e)) => {
                tlog!(Warning, "join request failed: {e}, retry...");
                fiber::sleep(timeout.saturating_sub(now.elapsed()));
                continue;
            }
            Err(e) => {
                tlog!(Error, "join request failed: {e}");
                std::process::exit(-1);
            }
        }
    };

    picolib_setup(args);
    assert!(tarantool::cfg().is_none());

    let cfg = tarantool::Cfg {
        listen: Some(args.listen.clone()),
        read_only: resp.box_replication.len() > 1,
        instance_uuid: Some(resp.instance.instance_uuid.clone()),
        replicaset_uuid: Some(resp.instance.replicaset_uuid.clone()),
        replication: resp.box_replication.clone(),
        wal_dir: args.data_dir.clone(),
        memtx_dir: args.data_dir.clone(),
        log_level: args.log_level() as u8,
        ..Default::default()
    };

    let (storage, raft_storage) = init_common(args, &cfg);

    let raft_id = resp.instance.raft_id;
    start_transaction(|| -> Result<(), TntError> {
        storage.instances.put(&resp.instance).unwrap();
        for traft::PeerAddress { raft_id, address } in resp.peer_addresses {
            storage.peer_addresses.put(raft_id, &address).unwrap();
        }
        raft_storage.persist_raft_id(raft_id).unwrap();
        raft_storage
            .persist_instance_id(&resp.instance.instance_id)
            .unwrap();
        raft_storage.persist_cluster_id(&args.cluster_id).unwrap();
        Ok(())
    })
    .unwrap();

    postjoin(args, storage, raft_storage)
}

fn postjoin(args: &args::Run, storage: Clusterwide, raft_storage: RaftSpaceAccess) {
    tlog!(Info, ">>>>> postjoin()");

    if let Some(ref address) = args.http_listen {
        start_http_server(address);
    }
    // Execute postjoin script if present
    if let Some(ref script) = args.script {
        let l = ::tarantool::lua_state();
        l.exec_with("dofile(...)", script)
            .unwrap_or_else(|err| panic!("failed to execute postjoin script: {err}"))
    }

    let mut box_cfg = tarantool::cfg().unwrap();

    // Reset the quorum BEFORE initializing the raft node.
    // Otherwise it may stuck on `box.cfg({replication})` call.
    box_cfg.replication_connect_quorum = 0;
    tarantool::set_cfg(&box_cfg);

    let node = traft::node::Node::new(storage.clone(), raft_storage.clone());
    let node = node.expect("failed initializing raft node");
    traft::node::set_global(node);
    let node = traft::node::global().unwrap();
    let raft_id = node.raft_id();

    let cs = raft_storage.conf_state().unwrap();
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
        assert!(node.status().raft_state.is_leader());
    }

    box_cfg.listen = Some(args.listen.clone());
    tarantool::set_cfg(&box_cfg);

    if let Err(e) = tarantool::on_shutdown(|| fiber::block_on(on_shutdown::callback())) {
        tlog!(Error, "failed setting on_shutdown trigger: {e}");
    }

    loop {
        let instance = storage
            .instances
            .get(&raft_id)
            .expect("instance must be persisted at the time of postjoin");
        let cluster_id = raft_storage
            .cluster_id()
            .unwrap()
            .expect("cluster_id must be persisted at the time of postjoin");
        let leader_id = node.status().leader_id;
        let leader_address = leader_id.and_then(|id| storage.peer_addresses.try_get(id).ok());
        let Some(leader_address) = leader_address else {
            // FIXME: don't hard code timeout
            let timeout = Duration::from_millis(1000);
            tlog!(Debug, "leader address is still unkown, retrying in {timeout:?}");
            fiber::sleep(timeout);
            continue;
        };

        tlog!(
            Info,
            "initiating self-activation of {}",
            instance.instance_id
        );
        let req = update_instance::Request::new(instance.instance_id, cluster_id)
            .with_target_grade(TargetGradeVariant::Online)
            .with_failure_domain(args.failure_domain());

        // It's necessary to call `proc_update_instance` remotely on a
        // leader over net_box. It always fails otherwise. Only the
        // leader is permitted to propose PersistInstance entries.
        let now = Instant::now();
        let timeout = Duration::from_secs(10);
        match rpc::net_box_call(&leader_address, &req, timeout) {
            Ok(update_instance::Response::Ok) => {
                break;
            }
            Ok(update_instance::Response::ErrNotALeader) => {
                tlog!(Warning, "failed to activate myself: not a leader, retry...");
                fiber::sleep(Duration::from_millis(100));
                continue;
            }
            Err(TntError::IO(e)) => {
                tlog!(Warning, "failed to activate myself: {e}, retry...");
                fiber::sleep(timeout.saturating_sub(now.elapsed()));
                continue;
            }
            Err(e) => {
                tlog!(Error, "failed to activate myself: {e}");
                std::process::exit(-1);
            }
        };
    }
}

pub fn tt_expel(args: args::Expel) {
    let req = rpc::expel::Request {
        cluster_id: args.cluster_id,
        instance_id: args.instance_id,
    };
    let res = rpc::net_box_call(
        &args.peer_address,
        &rpc::expel::redirect::Request(req),
        Duration::MAX,
    );
    match res {
        Ok(_) => {
            tlog!(Info, "Success expel call");
            std::process::exit(0);
        }
        Err(e) => {
            tlog!(Error, "Failed to expel instance: {e}");
            std::process::exit(-1);
        }
    }
}
