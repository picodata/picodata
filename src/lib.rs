#![allow(unknown_lints)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::let_and_return)]
#![allow(clippy::needless_return)]
#![allow(clippy::needless_late_init)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::unwrap_or_default)]
#![allow(clippy::redundant_static_lifetimes)]
#![allow(clippy::redundant_pattern_matching)]
#![allow(clippy::vec_init_then_push)]
use serde::{Deserialize, Serialize};
use std::path::Path;

use ::raft::prelude as raft;
use ::tarantool::error::Error as TntError;
use ::tarantool::fiber::r#async::timeout::{self, IntoTimeout};
use ::tarantool::time::Instant;
use ::tarantool::tlua;
use ::tarantool::transaction::transaction;
use ::tarantool::{fiber, session};
use rpc::{join, update_instance};
use sql::otm::SqlStatTables;
use std::time::Duration;
use storage::Clusterwide;
use traft::RaftSpaceAccess;

use crate::access_control::user_by_id;
use crate::address::Address;
use crate::instance::Grade;
use crate::instance::GradeVariant::*;
use crate::instance::Instance;
use crate::schema::ADMIN_ID;
use crate::schema::PICO_SERVICE_USER_NAME;
use crate::traft::error::Error;
use crate::traft::op;
use crate::util::effective_user_id;
use config::PicodataConfig;

mod access_control;
pub mod address;
pub mod audit;
mod bootstrap_entries;
pub mod cas;
pub mod cbus;
pub mod cli;
pub mod config;
pub mod discovery;
pub mod error_injection;
pub mod failure_domain;
pub mod governor;
pub mod http_server;
pub mod info;
pub mod instance;
pub mod introspection;
pub mod ipc;
pub mod kvcell;
pub mod r#loop;
mod luamod;
pub mod mailbox;
pub mod on_shutdown;
mod pgproto;
mod pico_service;
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
pub mod to_rmpv_named;
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
    preload!("sbroad.builtins", "sbroad/sbroad-core/src/builtins.lua");
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
        ("pgproto", "pg_close_stmt"),
        ("pgproto", "pg_close_portal"),
        ("pgproto", "pg_describe_stmt"),
        ("pgproto", "pg_describe_portal"),
        ("pgproto", "pg_execute"),
        ("pgproto", "pg_parse"),
        ("pgproto", "pg_statements"),
        ("pgproto", "pg_portals"),
        ("pgproto", "pg_close_client_stmts"),
        ("pgproto", "pg_close_client_portals"),
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

    lua.exec(
        r#"
        require('sbroad.builtins').init()
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
    preload!("http.version", "http/version.lua");
    preload!("http.codes", "http/codes.lua");
    preload!("http.mime_types", "http/mime_types.lua");
}

fn start_http_server(Address { host, port, .. }: &Address) {
    tlog!(Info, "starting http server at {host}:{port}");
    let lua = ::tarantool::lua_state();
    lua.exec_with(
        r#"
        local host, port = ...;
        local httpd = require('http.server').new(host, port);
        httpd:start();
        _G.pico.httpd = httpd
        "#,
        (host, port),
    )
    .expect("failed to start http server");
    lua.exec_with(
        "pico.httpd:route({method = 'GET', path = 'api/v1/tiers' }, ...)",
        tlua::Function::new(|| -> _ {
            http_server::wrap_api_result!(http_server::http_api_tiers())
        }),
    )
    .expect("failed to add route api/v1/tiers to http server");
    lua.exec_with(
        "pico.httpd:route({method = 'GET', path = 'api/v1/cluster' }, ...)",
        tlua::Function::new(|| -> _ {
            http_server::wrap_api_result!(http_server::http_api_cluster())
        }),
    )
    .expect("failed to add route api/v1/cluster to http server");
    lua.exec(
        "pico.httpd:route({method = 'GET', path = 'metrics' }, require('metrics.plugins.prometheus').collect_http)",
    )
    .expect("failed to add route metrics to http server");
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
    let lua = ::tarantool::lua_state();
    for proc in ::tarantool::proc::all_procs().iter() {
        lua.exec_with(
            "local name, is_public = ...
            local proc_name = '.' .. name
            box.schema.func.create(proc_name, {language = 'C', if_not_exists = true})
            if is_public then
                box.schema.role.grant('public', 'execute', 'function', proc_name, {if_not_exists = true})
            end
            ",
            (proc.name(), proc.is_public()),
        )
        .expect("this shouldn't fail");
    }

    lua.exec(
        r#"
        box.schema.role.grant('public', 'execute', 'function', '.proc_sql_dispatch', {if_not_exists = true})
        "#,
    )
    .expect("grant execute on .proc_sql_dispatch to public should never fail");

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
    // set_sql_executor still needs some kind of lua wrapper around our Proc API
    //  - `.proc_dispatch_sql`. So this might be the only case where we still need
    // `pico.sql`
    tarantool::exec(
        r#"
        local console = require('console')
        assert(pico.sql)
        console.set_sql_executor(pico.sql)
        "#,
    )
    .expect("overriding sql executor shouldn't fail")
}

/// Sets a check that will performed when a user is logging in
/// Checks for user exceeding maximum number of login attempts and if user was blocked.
///
/// Also see [`storage::PropertyName::MaxLoginAttempts`].
fn set_login_check(storage: Clusterwide) {
    const MAX_ATTEMPTS_EXCEEDED: &str = "Maximum number of login attempts exceeded";
    const NO_LOGIN_PRIVILEGE: &str = "User does not have login privilege";

    enum Verdict {
        AuthOk,
        AuthFail,
        UnknownUser,
        UserBlocked(&'static str),
    }

    // Determines the outcome of an authentication attempt.
    let compute_auth_verdict = move |user_name: String, successful_authentication: bool| {
        use std::collections::hash_map::Entry;

        // If the user is pico service (used for internal communication) we don't perform any additional checks.
        // Map result to print audit message, tarantool handles auth automatically.
        //
        // The reason for not performaing checks is twofold:
        // 1. We might not have the user or required privileges in _pico_* spaces yet.
        // 2. We should never block pico service user or instances would loose ability to communicate
        // with each other.
        if user_name == PICO_SERVICE_USER_NAME {
            if successful_authentication {
                return Verdict::AuthOk;
            } else {
                return Verdict::AuthFail;
            }
        }

        // Switch to admin to access system spaces.
        let admin_guard = session::su(ADMIN_ID).expect("switching to admin should not fail");
        let Some(user) = storage
            .users
            .by_name(&user_name)
            .expect("accessing storage should not fail")
        else {
            // Prevent DOS attacks by first checking whether the user exists.
            // If it doesn't, we shouldn't even bother tracking its attempts.
            // Too many hashmap records will cause a global OOM event.
            debug_assert!(!successful_authentication);
            return Verdict::UnknownUser;
        };
        let max_login_attempts = storage
            .properties
            .max_login_attempts()
            .expect("accessing storage should not fail");
        if storage
            .privileges
            .get(user.id, "universe", 0, "login")
            .expect("storage should not fail")
            .is_none()
        {
            // User does not have login privilege so should not be allowed to connect.
            return Verdict::UserBlocked(NO_LOGIN_PRIVILEGE);
        }
        drop(admin_guard);

        // Borrowing will not panic as there are no yields while it's borrowed
        let mut attempts = storage.login_attempts.borrow_mut();
        match attempts.entry(user_name) {
            Entry::Occupied(e) if *e.get() >= max_login_attempts => {
                // The account is suspended until instance is restarted
                // or user is unlocked with `grant session` operation.
                //
                // See [`crate::storage::global_grant_privilege`]
                Verdict::UserBlocked(MAX_ATTEMPTS_EXCEEDED)
            }
            Entry::Occupied(mut e) => {
                if successful_authentication {
                    // Forget about previous failures
                    e.remove();
                    Verdict::AuthOk
                } else {
                    *e.get_mut() += 1;
                    Verdict::AuthFail
                }
            }
            Entry::Vacant(e) => {
                if successful_authentication {
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
            match compute_auth_verdict(user.clone(), status) {
                Verdict::AuthOk => {
                    // We don't want to spam admins with
                    // unneeded info about internal user
                    if user == PICO_SERVICE_USER_NAME {
                        return;
                    }

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
                Verdict::UserBlocked(err) => {
                    crate::audit!(
                        message: "failed to authenticate user `{user}`",
                        title: "auth_fail",
                        severity: High,
                        user: &user,
                        initiator: &user,
                        verdict: format_args!("{err}; user blocked"),
                    );

                    // Raises an error instead of returning it as a function result.
                    // This is the behavior required by `on_auth` trigger to drop the connection
                    // even if auth was successful. If auth failed the connection will be dropped automatically.
                    //
                    // All the drop implementations are called, no need to clean anything up.
                    tlua::error!(lua, "{}", err);
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
            move |privilege: String,
                  object_type: String, // dummy comment praising rustfmt
                  object: String,
                  _lua: tlua::LuaState| {
                let effective_user = effective_user_id();
                let privilege = privilege.to_lowercase();

                // we do not have box.session.user() equivalent that returns an id straight away
                // so we look up the user by id.
                // Note: since we're in a user context we may not have access to use space.
                let user = session::with_su(ADMIN_ID, || {
                    user_by_id(effective_user).expect("user exists").name
                })
                .expect("must be able to su into admin");

                crate::audit!(
                    message: "{privilege} access denied \
                        on {object_type} `{object}` \
                        for user `{user}`",
                    title: "access_denied",
                    severity: Medium,
                    privilege: &privilege,
                    object: &object,
                    object_type: &object_type,
                    user: &user,
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
    pub fn exec(
        self,
        config: &PicodataConfig,
        to_supervisor: ipc::Sender<IpcMessage>,
    ) -> Result<(), Error> {
        match self {
            Self::StartDiscover => start_discover(config, to_supervisor)?,
            Self::StartBoot => start_boot(config)?,
            Self::StartJoin { leader_address } => start_join(config, leader_address)?,
        }

        Ok(())
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
fn init_common(
    config: &PicodataConfig,
    cfg: &tarantool::Cfg,
) -> Result<(Clusterwide, RaftSpaceAccess), Error> {
    std::fs::create_dir_all(config.instance.data_dir()).unwrap();

    if let Some(log_config) = &cfg.log {
        tlog!(Info, "switching to log configuration: {}", log_config);
    }
    // See doc comments in tlog.rs for explanation.
    tlog::set_core_logger_is_initialized(true);

    if let Err(e) = tarantool::set_cfg(cfg) {
        // Tarantool error is taken separately as in `set_cfg`
        // the needed tarantool error turns into lua error.
        if let Err(tnt_err) = ::tarantool::error::TarantoolError::maybe_last() {
            let err_ty = tnt_err.error_type();
            if err_ty == "XlogError" {
                if let Some(config) = &config.instance.audit {
                    // Init log with stab values for raft_id and gen.
                    // We need to log the 'integrity_violation' event and
                    // there is nowhere to take raft state from at the moment.
                    audit::init(config, 0, 0);
                    crate::audit!(
                        message: "integrity violation detected",
                        title: "integrity_violation",
                        severity: High,
                        error: format!("{err_ty}: {}", tnt_err.message()),
                    )
                }
            }
        }
        tlog::set_core_logger_is_initialized(false);
        return Err(Error::other(format!("core initialization failed: {e}")));
    }

    cbus::init_cbus_endpoint();
    tlog::init_thread_safe_logger();

    if config.instance.shredding() {
        tarantool::xlog_set_remove_file_impl();
    }

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
    // init sbroad statistics tables: they must be created
    // after global tables, so that they don't use their ids.
    let _ = SqlStatTables::get_or_init();
    schema::init_user_pico_service();

    set_login_check(storage.clone());
    set_on_access_denied_audit_trigger();
    let raft_storage =
        RaftSpaceAccess::new().expect("raft storage initialization should never fail");
    Ok((storage.clone(), raft_storage))
}

fn start_discover(
    config: &PicodataConfig,
    to_supervisor: ipc::Sender<IpcMessage>,
) -> Result<(), Error> {
    tlog!(Info, "entering discovery phase");

    luamod::setup();
    assert!(!tarantool::is_box_configured());

    let cfg = tarantool::Cfg::for_discovery(config)?;
    let (storage, raft_storage) = init_common(config, &cfg)?;
    discovery::init_global(config.instance.peers().iter().map(|a| a.to_host_port()));

    if let Some(raft_id) = raft_storage.raft_id()? {
        // This is a restart, go to postjoin immediately.
        tarantool::set_cfg_field("read_only", true)?;
        let instance_id = raft_storage
            .instance_id()?
            .expect("instance_id should be already set");
        postjoin(config, storage, raft_storage)?;
        crate::audit!(
            message: "local database recovered on `{instance_id}`",
            title: "recover_local_db",
            severity: Low,
            instance_id: %instance_id,
            raft_id: %raft_id,
            initiator: "admin",
        );
        crate::audit!(
            message: "local database connected on `{instance_id}`",
            title: "connect_local_db",
            severity: Low,
            instance_id: %instance_id,
            raft_id: %raft_id,
            initiator: "admin",
        );
        return Ok(());
    }

    // Start listenning only after we've checked if this is a restart.
    // Postjoin phase has it's own idea of when to start listenning.
    tarantool::set_cfg_field("listen", config.instance.listen().to_host_port())
        .expect("setting listen port shouldn't fail");

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
    }
}

fn start_boot(config: &PicodataConfig) -> Result<(), Error> {
    tlog!(Info, "entering cluster bootstrap phase");

    let tiers = config.cluster.tiers();
    let my_tier_name = config.instance.tier();
    let Some(tier) = tiers.get(my_tier_name) else {
        return Err(Error::other(format!(
            "invalid configuration: current instance is assigned tier '{my_tier_name}' which is not defined in the configuration file",
        )));
    };

    let instance = Instance::new(
        None,
        config.instance.instance_id.clone(),
        config.instance.replicaset_id.clone(),
        Grade::new(Offline, 0),
        Grade::new(Offline, 0),
        config.instance.failure_domain(),
        my_tier_name,
    );
    let raft_id = instance.raft_id;
    let instance_id = instance.instance_id.clone();

    if !tier.can_vote {
        return Err(Error::invalid_configuration(format!(
            "instance with instance_id '{instance_id}' from tier '{my_tier_name}' with `can_vote = false` \
             cannot be a bootstrap leader"
        )));
    }

    luamod::setup();
    assert!(!tarantool::is_box_configured());

    let cfg = tarantool::Cfg::for_cluster_bootstrap(config, &instance)?;
    let (storage, raft_storage) = init_common(config, &cfg)?;

    let cs = raft::ConfState {
        voters: vec![raft_id],
        ..Default::default()
    };

    let bootstrap_entries = bootstrap_entries::prepare(config, &instance, &tiers);

    let hs = raft::HardState {
        term: traft::INIT_RAFT_TERM,
        commit: bootstrap_entries.len() as _,
        ..Default::default()
    };

    transaction(|| -> Result<(), TntError> {
        raft_storage.persist_raft_id(raft_id).unwrap();
        raft_storage.persist_instance_id(&instance_id).unwrap();
        raft_storage.persist_tier(my_tier_name).unwrap();
        raft_storage
            .persist_cluster_id(config.cluster_id())
            .unwrap();
        raft_storage.persist_entries(&bootstrap_entries).unwrap();
        raft_storage.persist_conf_state(&cs).unwrap();
        raft_storage.persist_hard_state(&hs).unwrap();
        Ok(())
    })
    .unwrap();

    postjoin(config, storage, raft_storage)?;
    // In this case `create_local_db` is logged in postjoin
    crate::audit!(
        message: "local database connected on `{instance_id}`",
        title: "connect_local_db",
        severity: Low,
        instance_id: %instance_id,
        raft_id: %raft_id,
        initiator: "admin",
    );

    Ok(())
}

fn start_join(config: &PicodataConfig, instance_address: String) -> Result<(), Error> {
    tlog!(Info, "joining cluster, peer address: {instance_address}");

    let req = join::Request {
        cluster_id: config.cluster_id().into(),
        instance_id: config.instance.instance_id().map(From::from),
        replicaset_id: config.instance.replicaset_id().map(From::from),
        advertise_address: config.instance.advertise_address().to_host_port(),
        failure_domain: config.instance.failure_domain(),
        tier: config.instance.tier().into(),
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
            Err(TntError::ConnectionClosed(e)) => {
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
                return Err(Error::other(format!(
                    "join request failed: {e}, shutting down..."
                )));
            }
        }
    };

    luamod::setup();
    assert!(!tarantool::is_box_configured());

    let cfg = tarantool::Cfg::for_instance_join(config, &resp)?;
    let (storage, raft_storage) = init_common(config, &cfg)?;

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
        raft_storage
            .persist_cluster_id(config.cluster_id())
            .unwrap();
        raft_storage.persist_tier(config.instance.tier()).unwrap();
        Ok(())
    })
    .unwrap();

    let instance_id = resp.instance.instance_id;
    postjoin(config, storage, raft_storage)?;
    crate::audit!(
        message: "local database created on `{instance_id}`",
        title: "create_local_db",
        severity: Low,
        instance_id: %instance_id,
        raft_id: %raft_id,
        initiator: "admin",
    );
    crate::audit!(
        message: "local database connected on `{instance_id}`",
        title: "connect_local_db",
        severity: Low,
        instance_id: %instance_id,
        raft_id: %raft_id,
        initiator: "admin",
    );
    Ok(())
}

fn postjoin(
    config: &PicodataConfig,
    storage: Clusterwide,
    raft_storage: RaftSpaceAccess,
) -> Result<(), Error> {
    tlog!(Info, "entering post-join phase");

    config.validate_storage(&storage, &raft_storage)?;

    if let Some(config) = &config.instance.audit {
        let raft_id = raft_storage
            .raft_id()
            .expect("failed to get raft_id for audit log")
            .expect("found zero raft_id during audit log init");
        let gen = raft_storage.gen().expect("failed to get gen for audit log");
        audit::init(config, raft_id, gen);
    }

    if let Some(ref plugin_dir) = config.instance.plugin_dir {
        plugin::set_plugin_dir(Path::new(plugin_dir));
    }

    if let Some(addr) = &config.instance.http_listen {
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
    if let Some(ref script) = config.instance.deprecated_script {
        let l = ::tarantool::lua_state();
        l.exec_with(
            "dofile(...)",
            script.to_str().ok_or(Error::other(format!(
                "postjoin script path {} is not encoded in UTF-8",
                script.to_string_lossy()
            )))?,
        )
        .unwrap_or_else(|err| panic!("failed to execute postjoin script: {err}"))
    }

    // Reset the quorum BEFORE initializing the raft node.
    // Otherwise it may stuck on `box.cfg({replication})` call.
    tarantool::set_cfg_field("replication_connect_quorum", 0)
        .expect("changing replication_connect_quorum shouldn't fail");

    let node = traft::node::Node::init(storage.clone(), raft_storage.clone());
    let node = node.expect("failed initializing raft node");
    let raft_id = node.raft_id();

    let cs = raft_storage.conf_state().unwrap();
    if cs.voters == [raft_id] {
        #[rustfmt::skip]
        tlog!(Info, "this is the only voter in cluster, triggering election immediately");

        node.tick_and_yield(1); // apply configuration, if any
        node.campaign_and_yield().ok(); // trigger election immediately
        assert!(node.status().raft_state.is_leader());
    }

    tarantool::set_cfg_field("listen", config.instance.listen().to_host_port())
        .expect("changing listen port shouldn't fail");

    // Start admin console
    let socket_uri = util::validate_and_complete_unix_socket_path(config.instance.admin_socket())?;
    let lua = ::tarantool::lua_state();
    lua.exec_with(r#"require('console').listen(...)"#, &socket_uri)?;

    if let Err(e) = tarantool::on_shutdown(move || fiber::block_on(on_shutdown::callback())) {
        tlog!(Error, "failed setting on_shutdown trigger: {e}");
    }

    // We will shut down, if we don't receive a confirmation of target grade
    // change from leader before this time.
    let activation_deadline = Instant::now().saturating_add(Duration::from_secs(10));

    // This will be doubled on each retry.
    let mut retry_timeout = Duration::from_millis(250);

    // Activates instance
    loop {
        let now = fiber::clock();
        if now > activation_deadline {
            return Err(Error::other(
                "failed to activate myself: timeout, shutting down...",
            ));
        }

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
            .with_failure_domain(config.instance.failure_domain());
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
                return Err(Error::other(format!(
                    "failed to activate myself: {e}, shutting down..."
                )));
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

    let pg_config = &config.instance.pg;
    if pg_config.enabled() {
        pgproto::start(pg_config, config.instance.data_dir())?;
    }

    Ok(())
}
