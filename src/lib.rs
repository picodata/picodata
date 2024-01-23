#![allow(unknown_lints)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::let_and_return)]
#![allow(clippy::needless_return)]
#![allow(clippy::needless_late_init)]
#![allow(clippy::unwrap_or_default)]
#![allow(clippy::redundant_static_lifetimes)]
use serde::{Deserialize, Serialize};

use ::raft::prelude as raft;
use ::tarantool::error::Error as TntError;
use ::tarantool::fiber::r#async::timeout::{self, IntoTimeout};
use ::tarantool::time::Instant;
use ::tarantool::tlua;
use ::tarantool::transaction::transaction;
use ::tarantool::{fiber, session};
use rpc::{join, update_instance};
use std::cell::OnceCell;
use std::collections::HashMap;
use std::time::Duration;
use storage::Clusterwide;
use traft::RaftSpaceAccess;

use crate::access_control::user_by_id;
use crate::cli::args;
use crate::cli::args::Address;
use crate::cli::init_cfg::InitCfg;
use crate::instance::Grade;
use crate::instance::GradeVariant::*;
use crate::instance::Instance;
use crate::plugin::*;
use crate::schema::ADMIN_ID;
use crate::tier::{Tier, DEFAULT_TIER};
use crate::traft::op;
use crate::util::{effective_user_id, listen_admin_console, unwrap_or_terminate};

mod access_control;
pub mod audit;
mod bootstrap_entries;
pub mod cas;
pub mod cli;
pub mod discovery;
pub mod error_injection;
pub mod failure_domain;
pub mod governor;
pub mod info;
pub mod instance;
pub mod ipc;
pub mod kvcell;
pub mod r#loop;
mod luamod;
pub mod mailbox;
pub mod on_shutdown;
pub mod plugin;
pub mod reachability;
pub mod replicaset;
pub mod rpc;
pub mod schema;
pub mod sentinel;
pub mod sql;
pub mod storage;
pub mod sync;
pub mod tarantool;
pub mod tier;
pub mod tlog;
pub mod traft;
pub mod util;
pub mod vshard;

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

    preload!("pgproto", "src/sql/pgproto.lua");
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

    for (module, func) in &[
        ("sbroad", "sql"),
        ("pgproto", "pg_bind"),
        ("pgproto", "pg_close"),
        ("pgproto", "pg_describe"),
        ("pgproto", "pg_execute"),
        ("pgproto", "pg_parse"),
        ("pgproto", "pg_portals"),
    ] {
        let program = format!(
            r#"
            _G.pico.{func} = require('{module}').{func};
            box.schema.func.create('pico.{func}', {{if_not_exists = true}});
            box.schema.role.grant('public', 'execute', 'function', 'pico.{func}',
                {{if_not_exists = true}})
            "#
        );
        lua.exec(&program).unwrap();
    }
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

fn start_http_server(Address { host, port, .. }: &Address) {
    tlog!(Info, "starting http server at {host}:{port}");
    let lua = ::tarantool::lua_state();
    lua.exec_with(include_str!("http_server.lua"), (host, port))
        .expect("failed to start http server")
}

#[cfg(feature = "webui")]
fn start_webui() {
    use ::tarantool::tlua::PushInto;

    /// This structure is used to check that `json` contains valid data
    /// at deserialization.
    #[derive(Deserialize, Debug, PushInto)]
    struct File {
        body: String,
        mime: String,
    }

    let lua = ::tarantool::lua_state();
    let bundle = include_str!(env!("WEBUI_BUNDLE"));
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
                pico.httpd:route({path = '/*path', method = 'GET'}, handler);
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

    lua.exec(
        r#"
        box.schema.role.grant('public', 'execute', 'function', '.dispatch_query', {if_not_exists = true})
        "#).expect("grant execute on .dispatch_query to public should never fail");

    lua.exec(
        r#"
        box.schema.func.create('pico.cas', {if_not_exists = true});
        box.schema.role.grant('public', 'execute', 'function', 'pico.cas', {if_not_exists = true})
    "#,
    )
    .expect("pico.cas registration should never fail");
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

fn redirect_interactive_sql() {
    tarantool::exec(
        r#"
        local console = require('console')
        assert(pico.sql)
        console.set_sql_executor(pico.sql)
        "#,
    )
    .expect("overriding sql executor shouldn't fail")
}

/// Sets a check for user exceeding maximum number of login attempts through `picodata connect`.
/// Also see [`storage::PropertyName::MaxLoginAttempts`].
fn set_login_attempts_check(storage: Clusterwide) {
    enum Verdict {
        AuthOk,
        AuthFail,
        UnknownUser,
        UserBlocked,
    }

    // Determines the outcome of an authentication attempt.
    let compute_auth_verdict = move |user: String, status: bool| {
        use std::collections::hash_map::Entry;

        // It's ok to lose this information during restart, so we keep it as a static.
        static mut LOGIN_ATTEMPTS: OnceCell<HashMap<String, usize>> = OnceCell::new();

        // SAFETY: Accessing `USER_ATTEMPTS` is safe as it is only done from a single thread
        let attempts = unsafe {
            LOGIN_ATTEMPTS.get_or_init(HashMap::new);
            LOGIN_ATTEMPTS.get_mut().expect("is initialized")
        };

        let max_login_attempts = || {
            storage
                .properties
                .max_login_attempts()
                .expect("accessing storage should not fail")
        };

        let user_exists = storage
            .users
            .by_name(&user)
            .expect("accessing storage should not fail")
            .is_some();

        // Prevent DOS attacks by first checking whether the user exists.
        // If it doesn't, we shouldn't even bother tracking its attempts.
        // Too many hashmap records will cause a global OOM event.
        if !user_exists {
            return Verdict::UnknownUser;
        }

        match attempts.entry(user) {
            Entry::Occupied(e) if *e.get() >= max_login_attempts() => {
                // The account is suspended until restart
                Verdict::UserBlocked
            }
            Entry::Occupied(mut e) => {
                if status {
                    // Forget about previous failures
                    e.remove();
                    Verdict::AuthOk
                } else {
                    *e.get_mut() += 1;
                    Verdict::AuthFail
                }
            }
            Entry::Vacant(e) => {
                if status {
                    Verdict::AuthOk
                } else {
                    // Remember the failure, but don't raise an error yet
                    e.insert(1);
                    Verdict::AuthFail
                }
            }
        }
    };

    let lua = ::tarantool::lua_state();
    lua.exec_with(
        "box.session.on_auth(...)",
        tlua::function3(move |user: String, status: bool, lua: tlua::LuaState| {
            const ERROR: &str = "Maximum number of login attempts exceeded";

            match compute_auth_verdict(user.clone(), status) {
                Verdict::AuthOk => {
                    crate::audit!(
                        message: "successfully authenticated user `{user}`",
                        title: "auth_ok",
                        severity: High,
                        user: &user,
                        initiator: &user,
                        verdict: "user is not blocked",
                    );
                }
                Verdict::AuthFail => {
                    crate::audit!(
                        message: "failed to authenticate user `{user}`",
                        title: "auth_fail",
                        severity: High,
                        user: &user,
                        initiator: &user,
                        verdict: "user is not blocked",
                    );
                }
                Verdict::UnknownUser => {
                    crate::audit!(
                        message: "failed to authenticate unknown user `{user}`",
                        title: "auth_fail",
                        severity: High,
                        user: &user,
                        initiator: &user,
                        verdict: "user is not blocked",
                    );
                }
                Verdict::UserBlocked => {
                    crate::audit!(
                        message: "failed to authenticate user `{user}`",
                        title: "auth_fail",
                        severity: High,
                        user: &user,
                        initiator: &user,
                        verdict: format_args!("{ERROR}; user will be blocked indefinitely"),
                    );

                    // Raises an error instead of returning it as a function result.
                    // This is the behavior required by `on_auth` trigger to drop the connection.
                    // All the drop implementations are called, no need to clean anything up.
                    tlua::error!(lua, "{}", ERROR);
                }
            }
        }),
    )
    .expect("setting on auth trigger should not fail")
}

fn set_on_access_denied_audit_trigger() {
    let lua = ::tarantool::lua_state();

    lua.exec_with(
        "box.session.on_access_denied(...)",
        tlua::function4(
            move |privilege_type: String,
                  object_type: String,
                  object_name: String,
                  _lua: tlua::LuaState| {
                let effective_user = effective_user_id();

                // we do not have box.session.user() equivalent that returns an id straight away
                // so we look up the user by id.
                // Note: since we're in a user context we may not have access to use space.
                let user = session::with_su(ADMIN_ID, || {
                    user_by_id(effective_user).expect("user exists").name
                })
                .expect("must be able to su into admin");

                crate::audit!(
                    message: "{privilege_type} access to {object_type} `{object_name}` is denied for user `{user}`",
                    title: "access_denied",
                    severity: Medium,
                    privilege_type: &privilege_type,
                    object_type: &object_type,
                    object_name: &object_name,
                    initiator: &user,
                );
            },
        ),
    )
    .expect("setting on auth trigger should not fail")
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Serialize, Deserialize)]
pub enum Entrypoint {
    StartDiscover,
    StartBoot,
    StartJoin { leader_address: String },
}

impl Entrypoint {
    pub fn exec(self, args: cli::args::Run, to_supervisor: ipc::Sender<IpcMessage>) {
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

    tarantool::exec(
        "require 'compat' {
            c_func_iproto_multireturn = 'new',
        }",
    )
    .unwrap();

    set_console_prompt();
    redirect_interactive_sql();
    init_handlers();

    let storage = Clusterwide::try_get(true).expect("storage initialization should never fail");
    set_login_attempts_check(storage.clone());
    set_on_access_denied_audit_trigger();
    let raft_storage =
        RaftSpaceAccess::new().expect("raft storage initialization should never fail");
    (storage.clone(), raft_storage)
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
        vinyl_dir: args.data_dir.clone(),
        log_level: args.log_level() as u8,
        ..Default::default()
    };

    let (storage, raft_storage) = init_common(args, &cfg);
    discovery::init_global(
        args.peers
            .iter()
            .map(|Address { host, port, .. }| format!("{host}:{port}")),
    );

    cfg.listen = Some(format!("{}:{}", args.listen.host, args.listen.port));
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

    let init_cfg = match &args.init_cfg {
        Some(path) => unwrap_or_terminate(InitCfg::try_from_yaml_file(path)),
        None => {
            tlog!(Info, "init-cfg wasn't set");
            tlog!(
                Info,
                "filling init-cfg with default tier `{}` using replication-factor={}",
                DEFAULT_TIER,
                args.init_replication_factor
            );

            let tier = vec![Tier::with_replication_factor(args.init_replication_factor)];
            InitCfg { tier }
        }
    };

    let tiers = init_cfg.tier;

    let current_instance_tier = unwrap_or_terminate(
        tiers
            .iter()
            .find(|tier| tier.name == args.tier)
            .cloned()
            .ok_or(format!(
                "tier '{}' for current instance is not found in init-cfg",
                args.tier
            )),
    );

    let instance = Instance::new(
        None,
        args.instance_id.clone(),
        args.replicaset_id.clone(),
        Grade::new(Offline, 0),
        Grade::new(Offline, 0),
        args.failure_domain(),
        &current_instance_tier.name,
    );
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
        vinyl_dir: args.data_dir.clone(),
        log_level: args.log_level() as u8,
        ..Default::default()
    };

    let (storage, raft_storage) = init_common(args, &cfg);

    let cs = raft::ConfState {
        voters: vec![raft_id],
        ..Default::default()
    };

    let bootstrap_entries = bootstrap_entries::prepare(args, &instance, &tiers);

    let hs = raft::HardState {
        term: traft::INIT_RAFT_TERM,
        commit: bootstrap_entries.len() as _,
        ..Default::default()
    };

    transaction(|| -> Result<(), TntError> {
        raft_storage.persist_raft_id(raft_id).unwrap();
        raft_storage.persist_instance_id(&instance_id).unwrap();
        raft_storage
            .persist_tier(&current_instance_tier.name)
            .unwrap();
        raft_storage.persist_cluster_id(&args.cluster_id).unwrap();
        raft_storage.persist_entries(&bootstrap_entries).unwrap();
        raft_storage.persist_conf_state(&cs).unwrap();
        raft_storage.persist_hard_state(&hs).unwrap();
        Ok(())
    })
    .unwrap();

    postjoin(args, storage, raft_storage)
}

fn start_join(args: &args::Run, instance_address: String) {
    tlog!(Info, ">>>>> start_join({instance_address})");

    let req = join::Request {
        cluster_id: args.cluster_id.clone(),
        instance_id: args.instance_id.clone(),
        replicaset_id: args.replicaset_id.clone(),
        advertise_address: args.advertise_address(),
        failure_domain: args.failure_domain(),
        tier: args.tier.clone(),
    };

    // Arch memo.
    // - There must be no timeouts. Retrying may lead to flooding the
    //   topology with phantom instances. No worry, specifying a
    //   particular `instance_id` for every instance protects from that
    //   flood.
    // - It's fine to retry "connection refused" errors.
    let resp: rpc::join::Response = loop {
        let now = Instant::now();
        // TODO: exponential delay
        let timeout = Duration::from_secs(1);
        match fiber::block_on(rpc::network_call(&instance_address, &req)) {
            Ok(resp) => {
                break resp;
            }
            Err(TntError::Tcp(e)) => {
                tlog!(Warning, "join request failed: {e}, retrying...");
                fiber::sleep(timeout.saturating_sub(now.elapsed()));
                continue;
            }
            Err(TntError::IO(e)) => {
                tlog!(Warning, "join request failed: {e}, retrying...");
                fiber::sleep(timeout.saturating_sub(now.elapsed()));
                continue;
            }
            Err(e) => {
                tlog!(Error, "join request failed: {e}, shutting down...");
                std::process::exit(-1);
            }
        }
    };

    tlog::set_log_level(args.log_level());
    luamod::setup(args);
    assert!(tarantool::cfg().is_none());

    let cfg = tarantool::Cfg {
        listen: Some(format!("{}:{}", args.listen.host, args.listen.port)),
        read_only: resp.box_replication.len() > 1,
        instance_uuid: Some(resp.instance.instance_uuid.clone()),
        replicaset_uuid: Some(resp.instance.replicaset_uuid.clone()),
        replication: resp.box_replication.clone(),
        wal_dir: args.data_dir.clone(),
        memtx_dir: args.data_dir.clone(),
        vinyl_dir: args.data_dir.clone(),
        log_level: args.log_level() as u8,
        ..Default::default()
    };

    let (storage, raft_storage) = init_common(args, &cfg);

    let raft_id = resp.instance.raft_id;
    transaction(|| -> Result<(), TntError> {
        storage.instances.put(&resp.instance).unwrap();
        for traft::PeerAddress { raft_id, address } in resp.peer_addresses {
            storage.peer_addresses.put(raft_id, &address).unwrap();
        }
        raft_storage.persist_raft_id(raft_id).unwrap();
        raft_storage
            .persist_instance_id(&resp.instance.instance_id)
            .unwrap();
        raft_storage.persist_cluster_id(&args.cluster_id).unwrap();
        raft_storage.persist_tier(&args.tier).unwrap();
        Ok(())
    })
    .unwrap();

    postjoin(args, storage, raft_storage)
}

fn postjoin(args: &args::Run, storage: Clusterwide, raft_storage: RaftSpaceAccess) {
    tlog!(Info, ">>>>> postjoin()");

    if let Some(config) = &args.audit {
        audit::init(config, &raft_storage);
    }

    PluginList::global_init(&args.plugins);

    if let Some(addr) = &args.http_listen {
        start_http_server(addr);
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

    let node = traft::node::Node::init(storage.clone(), raft_storage.clone());
    let node = node.expect("failed initializing raft node");
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

    box_cfg.listen = Some(format!("{}:{}", args.listen.host, args.listen.port));
    tarantool::set_cfg(&box_cfg);

    unwrap_or_terminate(listen_admin_console(args));

    if let Err(e) =
        tarantool::on_shutdown(move || fiber::block_on(on_shutdown::callback(PluginList::get())))
    {
        tlog!(Error, "failed setting on_shutdown trigger: {e}");
    }

    // We will shut down, if we don't receive a confirmation of target grade
    // change from leader before this time.
    let activation_deadline = Instant::now().saturating_add(Duration::from_secs(10));

    // This will be doubled on each retry.
    let mut retry_timeout = Duration::from_millis(250);

    // Activates instance
    loop {
        let Ok(instance) = storage.instances.get(&raft_id) else {
            // This can happen if for example a snapshot arrives
            // and we truncate _pico_instance (read uncommitted btw).
            // In this case we also just wait some more.
            let timeout = Duration::from_millis(100);
            fiber::sleep(timeout);
            continue;
        };
        let cluster_id = raft_storage
            .cluster_id()
            .expect("storage should never fail");
        // Doesn't have to be leader - can be any online peer
        let leader_id = node.status().leader_id;
        let leader_address = leader_id.and_then(|id| storage.peer_addresses.try_get(id).ok());
        let Some(leader_address) = leader_address else {
            // FIXME: don't hard code timeout
            let timeout = Duration::from_millis(250);
            tlog!(
                Debug,
                "leader address is still unkown, retrying in {timeout:?}"
            );
            fiber::sleep(timeout);
            continue;
        };

        tlog!(
            Info,
            "initiating self-activation of {}",
            instance.instance_id
        );
        let req = update_instance::Request::new(instance.instance_id, cluster_id)
            .with_target_grade(Online)
            .with_failure_domain(args.failure_domain());
        let now = Instant::now();
        let fut = rpc::network_call(&leader_address, &req).timeout(activation_deadline - now);
        match fiber::block_on(fut) {
            Ok(update_instance::Response {}) => {
                break;
            }
            Err(timeout::Error::Failed(TntError::Tcp(e))) => {
                let timeout = retry_timeout.saturating_sub(now.elapsed());
                retry_timeout *= 2;
                #[rustfmt::skip]
                tlog!(Warning, "failed to activate myself: {e}, retrying in {timeout:.02?}...");
                fiber::sleep(timeout);
                continue;
            }
            Err(timeout::Error::Failed(TntError::IO(e))) => {
                let timeout = retry_timeout.saturating_sub(now.elapsed());
                retry_timeout *= 2;
                #[rustfmt::skip]
                tlog!(Warning, "failed to activate myself: {e}, retrying in {timeout:.02?}...");
                fiber::sleep(timeout);
                continue;
            }
            Err(e) => {
                tlog!(Error, "failed to activate myself: {e}, shutting down...");
                std::process::exit(-1);
            }
        }
    }

    // Wait for target grade to change to Online, so that sentinel doesn't send
    // a redundant update instance request.
    // Otherwise incarnations grow by 2 every time.
    let timeout = Duration::from_secs(10);
    let deadline = fiber::clock().saturating_add(timeout);
    loop {
        if let Ok(instance) = storage.instances.get(&raft_id) {
            if has_grades!(instance, * -> Online) {
                tlog!(Info, "self-activated successfully");
                break;
            }
        } else {
            // This can happen if for example a snapshot arrives
            // and we truncate _pico_instance (read uncommitted btw).
            // In this case we also just wait some more.
        }
        if fiber::clock() > deadline {
            tlog!(
                Warning,
                "didn't receive confirmation of self activation in time"
            );
            break;
        }
        let index = node.get_index();
        _ = node.wait_index(index + 1, deadline.duration_since(fiber::clock()));
    }

    node.sentinel_loop.on_self_activate();

    PluginList::get().iter().for_each(|plugin| {
        plugin.start();
    });
}
