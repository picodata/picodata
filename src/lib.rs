#![allow(clippy::let_and_return)]
use serde::{Deserialize, Serialize};

use ::raft::prelude as raft;
use ::tarantool::error::Error as TntError;
use ::tarantool::fiber;
use ::tarantool::fiber::r#async::timeout;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::tlua;
use ::tarantool::transaction::start_transaction;
use rpc::{join, update_instance};
use std::convert::TryFrom;
use std::time::{Duration, Instant};
use storage::tweak_max_space_id;
use storage::Clusterwide;
use storage::{ClusterwideSpaceId, PropertyName};
use traft::RaftSpaceAccess;

use protobuf::Message as _;

use crate::instance::grade::TargetGradeVariant;
use crate::traft::op;
use crate::traft::LogicalClock;

#[doc(hidden)]
mod app;
pub mod args;
pub mod cas;
pub mod discovery;
pub mod failure_domain;
pub mod governor;
pub mod instance;
pub mod ipc;
pub mod kvcell;
pub mod r#loop;
mod luamod;
pub mod mailbox;
pub mod on_shutdown;
pub mod replicaset;
pub mod rpc;
pub mod schema;
pub mod sql;
pub mod storage;
pub mod sync;
pub mod tarantool;
pub mod tlog;
pub mod traft;
pub mod util;

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

/// Initializes Tarantool stored procedures.
///
/// Those are used for inter-instance communication
/// (discovery, rpc, public proc api).
fn init_handlers() {
    tarantool::exec(
        r#"
        box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
        "#,
    )
    .expect("box.schema.user.grant should never fail");

    let lua = ::tarantool::lua_state();
    for proc in ::tarantool::proc::all_procs().iter() {
        lua.exec_with(
            "box.schema.func.create('.' .. ...,
                { language = 'C', if_not_exists = true }
            );",
            proc.name(),
        )
        .expect("box.schema.func.create should never fail");
    }
}

/// Sets interactive prompt to display `picodata>`.
fn set_console_prompt() {
    tarantool::exec(
        r#"
        local console = require('console')

        console.on_start(function(self)
            self.prompt = "picodata"
        end)
        "#,
    )
    .expect("setting prompt should never fail")
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

    set_console_prompt();
    init_handlers();
    traft::event::init();

    tweak_max_space_id().expect("setting max_id should never fail");
    let storage = Clusterwide::new().expect("storage initialization should never fail");
    let raft_storage =
        RaftSpaceAccess::new().expect("raft storage initialization should never fail");
    (storage, raft_storage)
}

fn start_discover(args: &args::Run, to_supervisor: ipc::Sender<IpcMessage>) {
    tlog!(Info, ">>>>> start_discover()");

    tlog::set_log_level(args.log_level());
    luamod::setup(args);
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

    tlog::set_log_level(args.log_level());
    luamod::setup(args);
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
                &(PropertyName::GlobalSchemaVersion, 0),
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

    tlog::set_log_level(args.log_level());
    luamod::setup(args);
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
            .expect("storage should never fail");
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
