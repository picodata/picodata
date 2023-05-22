#![allow(clippy::let_and_return)]
use serde::{Deserialize, Serialize};

use ::raft::prelude as raft;
use ::tarantool::error::Error as TntError;
use ::tarantool::fiber;
use ::tarantool::fiber::r#async::timeout;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::tlua;
use ::tarantool::transaction::start_transaction;
use ::tarantool::tuple::Decode;
use rpc::{join, update_instance};
use std::convert::TryFrom;
use std::time::{Duration, Instant};
use storage::tweak_max_space_id;
use storage::Clusterwide;
use storage::{ClusterwideSpaceId, PropertyName};
use traft::RaftSpaceAccess;
use traft::RaftTerm;

use protobuf::Message as _;

use crate::instance::grade::TargetGradeVariant;
use crate::instance::InstanceId;
use crate::schema::CreateSpaceParams;
use crate::tlog::set_log_level;
use crate::traft::node;
use crate::traft::op::{self, Op};
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
pub mod rpc;
pub mod schema;
pub mod sql;
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
        "raft_propose",
        tlua::function1(|lua: tlua::LuaState| -> traft::Result<RaftIndex> {
            use tlua::{AnyLuaString, AsLua, LuaError, LuaTable};
            let lua = unsafe { tlua::Lua::from_static(lua) };
            let t: LuaTable<_> = AsLua::read(&lua).map_err(|(_, e)| LuaError::from(e))?;
            let mp: AnyLuaString = lua
                .eval_with("return require 'msgpack'.encode(...)", &t)
                .map_err(LuaError::from)?;
            let op: Op = Decode::decode(mp.as_bytes())?;

            let node = traft::node::global()?;
            let mut node_impl = node.node_impl();
            let index = node_impl.propose(op)?;
            node.main_loop.wakeup();
            // Release the lock
            drop(node_impl);
            Ok(index)
        }),
    );
    luamod.set(
        "raft_propose_mp",
        tlua::function1(|op: tlua::AnyLuaString| -> traft::Result<()> {
            let op: Op = Decode::decode(op.as_bytes())?;
            traft::node::global()?.propose_and_wait(op, Duration::from_secs(1))
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
            fiber::block_on(rpc::network_call_to_leader(&rpc::expel::Request {
                instance_id,
                cluster_id,
            }))?;
            Ok(())
        }),
    );
    // TODO: remove this
    if cfg!(debug_assertions) {
        use ::tarantool::index::IteratorType;
        use ::tarantool::space::Space;
        use ::tarantool::tuple::Tuple;
        luamod.set(
            "prop",
            tlua::Function::new(|property: Option<String>| -> traft::Result<Vec<Tuple>> {
                if let Some(property) = property {
                    let _ = property;
                    todo!();
                }
                let space = Space::find("_picodata_property").expect("always defined");
                let iter = space.select(IteratorType::All, &())?;
                let mut res = vec![];
                for t in iter {
                    res.push(t);
                }
                Ok(res)
            }),
        );
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
            tlua::function0(|| -> traft::Result<rpc::sharding::cfg::Cfg> {
                let node = traft::node::global()?;
                rpc::sharding::cfg::Cfg::from_storage(&node.storage)
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

    // Trims raft log up to the given index (excluding the index
    // itself). Returns the new `first_index` after the log compaction.
    luamod.set("raft_compact_log", {
        tlua::Function::new(|up_to: u64| -> traft::Result<u64> {
            let raft_storage = &node::global()?.raft_storage;
            let ret = start_transaction(|| raft_storage.compact_log(up_to));
            Ok(ret?)
        })
    });

    luamod.set(
        "cas",
        tlua::Function::new(
            |op: op::DmlInLua, predicate: rpc::cas::Predicate| -> traft::Result<RaftIndex> {
                let op = op::Dml::from_lua_args(op).map_err(Error::other)?;
                let (index, _) = compare_and_swap(op.into(), predicate)?;
                Ok(index)
            },
        ),
    );

    luamod.set("create_space", {
        tlua::function2(
            |params: CreateSpaceParams,
             // Specifying the timeout identifies how long user is ready to wait for ddl to be applied.
             // But it does not provide guarantees that a ddl will be aborted if wait for commit timeouts.
             timeout_sec: f64|
             -> traft::Result<RaftIndex> {
                let timeout = Duration::from_secs_f64(timeout_sec);
                let storage = &node::global()?.storage;
                params.validate(storage)?;
                // TODO: check space creation and rollback
                // box.begin() box.schema.space.create() box.rollback()
                let op = params.into_ddl(storage);
                let index = schema::prepare_ddl(op, timeout)?;
                let commit_index = schema::wait_for_ddl_commit(index, timeout)?;
                Ok(commit_index)
            },
        )
    });
}

/// Performs a clusterwide compare and swap operation.
///
/// E.g. it checks the `predicate` on leader and if no conflicting entries were found
/// appends the `op` to the raft log and returns its index and term.
///
/// # Errors
/// See [`rpc::cas::Error`] for CaS-specific errors.
/// It can also return general picodata errors in cases of faulty network or storage.
pub fn compare_and_swap(
    op: Op,
    predicate: rpc::cas::Predicate,
) -> traft::Result<(RaftIndex, RaftTerm)> {
    let node = node::global()?;
    let request = rpc::cas::Request {
        cluster_id: node
            .raft_storage
            .cluster_id()?
            .expect("cluster_id really shouldn't be returning Option"),
        predicate,
        op,
    };
    loop {
        let Some(leader_id) = node.status().leader_id else {
            tlog!(Warning, "leader id is unknown, waiting for status change...");
            node.wait_status();
            continue;
        };
        let leader_address = unwrap_ok_or!(
            node.storage.peer_addresses.try_get(leader_id),
            Err(e) => {
                tlog!(Warning, "failed getting leader address: {e}");
                tlog!(Info, "going to retry in while...");
                fiber::sleep(Duration::from_millis(250));
                continue;
            }
        );
        let resp = rpc::network_call(&leader_address, &request);
        let resp = fiber::block_on(resp.timeout(Duration::from_secs(3)));
        match resp {
            Ok(rpc::cas::Response { index, term }) => return Ok((index, term)),
            Err(e) => {
                tlog!(Warning, "{e}");
                return Err(e.into());
            }
        }
    }
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

fn init_sbroad() {
    let lua = ::tarantool::lua_state();

    macro_rules! preload {
        ($module:literal, $path:literal) => {
            lua_preload!(lua, $module, "../", $path);
        };
    }

    preload!("sbroad", "src/sql/init.lua");
    preload!("sbroad.helper", "sbroad/sbroad-core/src/helper.lua");
    preload!(
        "sbroad.core-router",
        "sbroad/sbroad-core/src/core-router.lua"
    );
    preload!(
        "sbroad.core-storage",
        "sbroad/sbroad-core/src/core-storage.lua"
    );

    // Create SQL function for bucket calculation.
    lua.exec(
        r#"
        box.schema.func.create('BUCKET_ID', {
            language = 'Lua',
            body = [[
                function(x)
                    return box.func['.calculate_bucket_id']:call({ x })
                end
            ]],
            if_not_exists = true,
            param_list = {'string'},
            returns = 'unsigned',
            aggregate = 'none',
            exports = {'SQL'},
        })
    "#,
    )
    .unwrap();

    lua.exec(
        r#"
        _G.pico.sql = require('sbroad').sql;
        _G.pico.trace = require('sbroad').trace;
    "#,
    )
    .unwrap();
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

#[cfg(feature = "webui")]
fn start_webui() {
    use ::tarantool::tlua::PushInto;
    use std::collections::HashMap;

    /// This structure is used to check that `json` contains valid data
    /// at deserialization.
    #[derive(Deserialize, Debug, PushInto)]
    struct File {
        body: String,
        mime: String,
    }

    let lua = ::tarantool::lua_state();
    let bundle = include_str!(concat!(
        concat!(env!("OUT_DIR"), "/../../"), // build root
        "picodata-webui/bundle.json"
    ));
    let bundle: HashMap<String, File> =
        serde_json::from_str(bundle).expect("failed to parse Web UI bundle");
    if !bundle.contains_key("index.html") {
        panic!("'index.html' not present in Web UI bundle")
    }
    lua.exec_with(
        "local bundle = ...;
        for filename, file in pairs(bundle) do
            local handler = function ()
                return {
                    status = 200,
                    headers = { ['content-type'] = file['mime'] },
                    body = file['body'],
                }
            end
            if filename == 'index.html' then
                pico.httpd:route({path = '/', method = 'GET'}, handler);
            end
            pico.httpd:route({path = '/' .. filename, method = 'GET'}, handler);
        end",
        bundle,
    )
    .expect("failed to add Web UI routes")
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
    init_sbroad();

    init_handlers().expect("failed initializing rpc handlers");
    traft::event::init();

    tweak_max_space_id().expect("failed setting max_id");
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
                ClusterwideSpaceId::Address,
                &traft::PeerAddress { raft_id, address },
            )
            .expect("cannot fail")
            .into(),
        );
        init_entries_push_op(traft::op::PersistInstance::new(instance).into());
        init_entries_push_op(
            op::Dml::insert(
                ClusterwideSpaceId::Property,
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
                ClusterwideSpaceId::Property,
                &(PropertyName::CurrentSchemaVersion, 0),
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
    let resp: rpc::join::OkResponse = loop {
        let now = Instant::now();
        // TODO: exponential delay
        let timeout = Duration::from_secs(1);
        match fiber::block_on(rpc::network_call(&leader_address, &req)) {
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
            Err(TntError::Tcp(e)) => {
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
        if cfg!(feature = "webui") {
            tlog!(Info, "Web UI is enabled");
        } else {
            tlog!(Info, "Web UI is disabled");
        }
        #[cfg(feature = "webui")]
        start_webui();
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
        match fiber::block_on(rpc::network_call(&leader_address, &req).timeout(timeout)) {
            Ok(update_instance::Response::Ok) => {
                break;
            }
            Ok(update_instance::Response::ErrNotALeader) => {
                tlog!(Warning, "failed to activate myself: not a leader, retry...");
                fiber::sleep(Duration::from_millis(100));
                continue;
            }
            Err(timeout::Error::Failed(TntError::Tcp(e))) => {
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

pub async fn tt_expel(args: args::Expel) {
    let req = rpc::expel::Request {
        cluster_id: args.cluster_id,
        instance_id: args.instance_id,
    };
    let res = rpc::network_call(&args.peer_address, &rpc::expel::redirect::Request(req)).await;
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
