#![allow(unknown_lints)]
#![allow(non_camel_case_types)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::let_and_return)]
#![allow(clippy::needless_borrow)]
#![allow(clippy::needless_return)]
#![allow(clippy::needless_late_init)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::unwrap_or_default)]
#![allow(clippy::redundant_static_lifetimes)]
#![allow(clippy::redundant_pattern_matching)]
#![allow(clippy::vec_init_then_push)]
#![allow(clippy::unused_io_amount)]
#![allow(clippy::bool_assert_comparison)]
#![allow(clippy::field_reassign_with_default)]
use config::apply_parameter;
use info::PICODATA_VERSION;
use regex::Regex;
use sbroad::frontend::sql::transform_to_regex_pattern;
use serde::{Deserialize, Serialize};
use storage::ToEntryIter;

use ::raft::prelude as raft;
use ::tarantool::error::Error as TntError;
use ::tarantool::error::IntoBoxError;
use ::tarantool::fiber::r#async::timeout::{self, IntoTimeout};
use ::tarantool::time::Instant;
use ::tarantool::tlua;
use ::tarantool::transaction::transaction;
use ::tarantool::{fiber, session};
use std::time::Duration;
use storage::Catalog;
use traft::RaftSpaceAccess;

use crate::access_control::user_by_id;
use crate::address::HttpAddress;
use crate::error_code::ErrorCode;
use crate::instance::Instance;
use crate::instance::StateVariant::*;
use crate::schema::ADMIN_ID;
use crate::schema::PICO_SERVICE_USER_NAME;
use crate::traft::error::Error;
use crate::traft::op;
use crate::traft::Result;
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
pub mod error_code;
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
pub mod topology_cache;
pub mod traft;
pub mod util;
pub mod version;
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

/// Exists to optimize code burden with access to mutable statics.
/// It is pretty much a `addr_of*`/`&raw` wrapper without warnings,
/// so it is developer responsibility to use it correctly, but it
/// is not marked as unsafe in an expansion to prevent silly mistakes
/// and to have an opportunity for reviewers to blame someone easily.
///
/// Expects `mut` or `const` as a second parameter, to state whether
/// you want mutable (exclusive) or constant (shared) reference to
/// the provided mutable static variable as a first parameter.
///
/// NOTE: this macro is not that useful if you need a shared reference
/// to an immutable static, as it is considered safe by default.
#[macro_export]
macro_rules! static_ref {
    ($var:ident mut) => {
        &mut *&raw mut $var
    };
    ($var:ident const) => {
        &*&raw const $var
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

    //add SUBSTRING func to lua
    let _ = lua.exec_with(
        r#"
    if rawget(_G, 'pico') == nil then
           error('pico module must be initialized before regexp_extract')
       end
       if pico.builtins == nil then
           pico.builtins = {}
       end
    pico.builtins.SUBSTRING = ...
    "#,
        tlua::function3(
            |input: Option<String>,
             pattern: Option<String>,
             lua: tlua::LuaState|
             -> Option<String> {
                //input and pattern can be NULL
                let input = input?;
                let pattern = pattern?;
                match Regex::new(&pattern) {
                    Ok(re) => {
                        let caps = re.captures(&input)?;
                        // If there is a capturing group (i.e. len() > 1), return the first group
                        if caps.len() > 1 {
                            let matched = caps.get(1)?;
                            return Some(matched.as_str().to_string());
                        }
                        // Otherwise, return the full match
                        let matched = caps.get(0)?;
                        return Some(matched.as_str().to_string());
                    }
                    Err(err) => {
                        tlua::error!(lua, "Invalid pattern: {}", err);
                    }
                }
            },
        ),
    );

    //add TO_REGEXP func to lua
    let _ = lua.exec_with(
        r#"
    if rawget(_G, 'pico') == nil then
           error('pico module must be initialized before regexp_extract')
       end
       if pico.builtins == nil then
           pico.builtins = {}
       end
    pico.builtins.TO_REGEXP = ...
    "#,
        tlua::function3(
            |pattern: String, escape: String, lua: tlua::LuaState| -> String {
                match transform_to_regex_pattern(&pattern, &escape) {
                    Ok(new_pattern) => return new_pattern,
                    Err(err) => {
                        tlua::error!(lua, "Transformation to REGEX pattern failed: {}", err)
                    }
                };
            },
        ),
    );
}

extern "C-unwind" {
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

fn start_http_server(HttpAddress { host, port, .. }: &HttpAddress) {
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
    .unwrap_or_else(|err| {
        panic!("failed to start http server on {}:{}: {}", host, port, err);
    });
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

    lua.exec_with(
        r#"
        local user_metrics = ...
        pico.httpd:route({method = 'GET', path = 'metrics' }, function()
        local resp = require('metrics.plugins.prometheus').collect_http()
        resp.body = resp.body .. user_metrics()
        return resp
        end)"#,
        tlua::Function::new(crate::plugin::metrics::get_plugin_metrics),
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
                pico.httpd:route({path = '/nodes*path', method = 'GET'}, handler);
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
    plugin::rpc::server::init_handlers();

    rpc::init_static_proc_set();

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

    forbid_unsupported_iproto_requests();
    redirect_iproto_execute_requests();
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

/// Sets a check that will be performed when a user is logging in
/// Checks for user exceeding maximum number of login attempts and if user was blocked.
///
/// Also see [`config::AlterSystemParameters::auth_login_attempt_max`].
fn set_login_check(storage: Catalog) {
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
            .db_config
            .auth_login_attempt_max()
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
        "
        local rust_on_auth = ...
        local function on_auth(user, status)
            if box.session.type() ~= 'console' then
                rust_on_auth(user, status)
            end
        end

        box.session.on_auth(on_auth)",
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

/// Apply all dynamic parameters from `_pico_db_config` via box.cfg
fn reapply_dynamic_parameters(storage: &Catalog, current_tier: &str) -> Result<()> {
    for parameter in storage.db_config.iter()? {
        apply_parameter(parameter, current_tier);
    }

    Ok(())
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Entrypoint {
    StartDiscover,
    StartBoot,
    StartJoin { leader_address: String },
}

/// Runs one of picodata's entry points.
///
/// Returns
/// - `Ok(Some(Entrypoint::StartBoot))` if the cluster is not yet bootstrapped
///   and this instance is chosen as the bootstrap leader.
/// - `Ok(Some(Entrypoint::StartJoin { .. }))` if the cluster is not yet bootstrapped
///   and this instance is a bootstrap follower.
/// - `Ok(None)` if the cluster is already initialized.
///
/// May return an error. Will never return a `StartDiscover` as next entrypoint.
pub fn start(config: &PicodataConfig, entrypoint: Entrypoint) -> Result<Option<Entrypoint>> {
    use Entrypoint::*;

    let mut next_entrypoint = None;
    match entrypoint {
        StartDiscover => {
            next_entrypoint = start_discover(config)?;
        }
        StartBoot => {
            // Cleanup the instance directory with WALs from the previous StartDiscover run
            tarantool::rm_tarantool_files(config.instance.instance_dir())?;
            start_boot(config)?;
        }
        StartJoin { leader_address } => {
            // Cleanup the instance directory with WALs from the previous StartDiscover run
            tarantool::rm_tarantool_files(config.instance.instance_dir())?;
            start_join(config, leader_address)?;
        }
    }

    Ok(next_entrypoint)
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
    shredding: bool,
) -> Result<(Catalog, RaftSpaceAccess), Error> {
    std::fs::create_dir_all(config.instance.instance_dir()).map_err(|err| {
        Error::other(format!(
            "failed creating instance directory {}: {}",
            config.instance.instance_dir().display(),
            err
        ))
    })?;

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
    // There's a problem with cbus channels at the moment, they introduce too
    // much performance penalty when polling them if they're empty,
    // so we only enable the thread-safe logger in debug builds for now
    #[cfg(debug_assertions)]
    tlog::init_thread_safe_logger();

    if shredding {
        tarantool::xlog_set_remove_file_impl();
    }

    set_tarantool_compat_options();

    // Load Lua libraries
    preload_vshard();
    preload_http();
    init_sbroad();

    set_console_prompt();
    redirect_interactive_sql();
    init_handlers();

    let storage = Catalog::try_get(true).expect("storage initialization should never fail");
    schema::init_user_pico_service();

    set_login_check(storage.clone());
    set_on_access_denied_audit_trigger();
    let raft_storage =
        RaftSpaceAccess::new().expect("raft storage initialization should never fail");
    Ok((storage.clone(), raft_storage))
}

fn start_discover(config: &PicodataConfig) -> Result<Option<Entrypoint>, Error> {
    tlog!(Info, "entering discovery phase");

    luamod::setup();
    assert!(!tarantool::is_box_configured());

    let cfg = tarantool::Cfg::for_discovery(config)?;
    let (storage, raft_storage) = init_common(config, &cfg, false)?;
    discovery::init_global(config.instance.peers().iter().map(|a| a.to_host_port()));

    if let Some(raft_id) = raft_storage.raft_id()? {
        // This is a restart, go to postjoin immediately.
        tarantool::set_cfg_field("read_only", true)?;
        let instance_name = raft_storage
            .instance_name()?
            .expect("instance_name should be already set");
        postjoin(config, storage, raft_storage)?;
        crate::audit!(
            message: "local database recovered on `{instance_name}`",
            title: "recover_local_db",
            severity: Low,
            instance_name: %instance_name,
            raft_id: %raft_id,
            initiator: "admin",
        );
        crate::audit!(
            message: "local database connected on `{instance_name}`",
            title: "connect_local_db",
            severity: Low,
            instance_name: %instance_name,
            raft_id: %raft_id,
            initiator: "admin",
        );
        return Ok(None);
    }

    // Start listening only after we've checked if this is a restart.
    // Postjoin phase has its own idea of when to start listening.
    tarantool::set_cfg_field("listen", config.instance.iproto_listen().to_host_port()).map_err(
        |err| {
            Error::other(format!(
                "failed to start listen on iproto {}: {}",
                config.instance.iproto_listen().to_host_port(),
                err
            ))
        },
    )?;

    let role = discovery::wait_global();
    let next_entrypoint = match role {
        discovery::Role::Leader { .. } => Entrypoint::StartBoot,
        discovery::Role::NonLeader { leader } => Entrypoint::StartJoin {
            leader_address: leader,
        },
    };

    Ok(Some(next_entrypoint))
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

    let raft_id = 1;
    let instance_name = config.instance.name();

    let replicaset_number = 1;
    let instances_in_replicaset = 1;
    let instance_name = instance_name.unwrap_or_else(|| {
        instance::InstanceName::from(format!(
            "{my_tier_name}_{replicaset_number}_{instances_in_replicaset}"
        ))
    });
    let replicaset_name = config.instance.replicaset_name();
    let replicaset_name = replicaset_name.unwrap_or_else(|| {
        replicaset::ReplicasetName::from(format!("{my_tier_name}_{replicaset_number}"))
    });

    let instance = Instance {
        raft_id,
        name: instance_name.clone(),
        uuid: uuid::Uuid::new_v4().to_hyphenated().to_string(),
        replicaset_name,
        replicaset_uuid: uuid::Uuid::new_v4().to_hyphenated().to_string(),
        current_state: instance::State::new(Offline, 0),
        target_state: instance::State::new(Offline, 0),
        failure_domain: config.instance.failure_domain().clone(),
        tier: my_tier_name.into(),
        picodata_version: PICODATA_VERSION.to_string(),
    };

    if !tier.can_vote {
        return Err(Error::invalid_configuration(format!(
            "instance with instance_name '{instance_name}' from tier '{my_tier_name}' with `can_vote = false` \
             cannot be a bootstrap leader"
        )));
    }

    luamod::setup();
    assert!(!tarantool::is_box_configured());

    let cfg = tarantool::Cfg::for_cluster_bootstrap(config, &instance)?;
    let (storage, raft_storage) = init_common(config, &cfg, config.cluster.shredding())?;

    let cs = raft::ConfState {
        voters: vec![raft_id],
        ..Default::default()
    };

    let bootstrap_entries = bootstrap_entries::prepare(config, &instance, &tiers, &storage)?;

    let hs = raft::HardState {
        term: traft::INIT_RAFT_TERM,
        commit: bootstrap_entries.len() as _,
        ..Default::default()
    };

    transaction(|| -> Result<(), TntError> {
        raft_storage.persist_raft_id(raft_id).unwrap();
        raft_storage.persist_instance_name(&instance_name).unwrap();
        raft_storage.persist_tier(my_tier_name).unwrap();
        raft_storage
            .persist_cluster_name(config.cluster_name())
            .unwrap();
        raft_storage.persist_entries(&bootstrap_entries).unwrap();
        raft_storage.persist_conf_state(&cs).unwrap();
        raft_storage.persist_hard_state(&hs).unwrap();
        Ok(())
    })
    .unwrap();

    // TODO: log cluster uuid here
    tlog!(Info, "created cluster {}", config.cluster_name());
    tlog!(Info, "raft_id: {}", instance.raft_id);
    tlog!(Info, "instance name: {}", instance.name);
    tlog!(Info, "instance uuid: {}", instance.uuid);
    tlog!(Info, "replicaset name: {}", instance.replicaset_name);
    tlog!(Info, "replicaset uuid: {}", instance.replicaset_uuid);
    tlog!(Info, "tier name: {}", instance.tier);

    postjoin(config, storage, raft_storage)?;
    // In this case `create_local_db` is logged in postjoin
    crate::audit!(
        message: "local database connected on `{instance_name}`",
        title: "connect_local_db",
        severity: Low,
        instance_name: %instance_name,
        raft_id: %raft_id,
        initiator: "admin",
    );

    Ok(())
}

fn start_join(config: &PicodataConfig, instance_address: String) -> Result<(), Error> {
    tlog!(Info, "joining cluster, peer address: {instance_address}");

    #[allow(unused_mut)]
    let mut version = info::PICODATA_VERSION.to_string();
    #[cfg(feature = "error_injection")]
    crate::error_injection!("UPDATE_PICODATA_VERSION" => {
        version = std::env::var("PICODATA_INTERNAL_VERSION_OVERRIDE")
            .unwrap_or_else(|_| info::PICODATA_VERSION.to_string());
    });

    let req = rpc::join::Request {
        cluster_name: config.cluster_name().into(),
        instance_name: config.instance.name(),
        replicaset_name: config.instance.replicaset_name(),
        advertise_address: config.instance.iproto_advertise().to_host_port(),
        pgproto_advertise_address: config.instance.pg.listen().to_host_port(),
        failure_domain: config.instance.failure_domain().clone(),
        tier: config.instance.tier().into(),
        picodata_version: version,
    };

    const INITIAL_DELAY: Duration = Duration::from_millis(100);
    const MAX_DELAY: Duration = Duration::from_secs(60);

    let mut current_delay = INITIAL_DELAY;

    // Arch memo.
    // - There must be no timeouts. Retrying may lead to flooding the
    //   topology with phantom instances. No worry, specifying a
    //   particular `instance_name` for every instance protects from that
    //   flood.
    // - It's fine to retry "connection refused" errors.
    let resp: rpc::join::Response = loop {
        match fiber::block_on(rpc::network_call(
            &instance_address,
            proc_name!(rpc::join::proc_raft_join),
            &req,
        )) {
            Ok(resp) => {
                break resp;
            }
            Err(TntError::ConnectionClosed(e)) => {
                tlog!(
                    Warning,
                    "join request failed: {e}, retrying in {:?}...",
                    current_delay
                );
                fiber::sleep(current_delay);
                current_delay = std::cmp::min(current_delay * 2, MAX_DELAY);
                continue;
            }
            Err(TntError::IO(e)) => {
                tlog!(
                    Warning,
                    "join request failed: {e}, retrying in {:?}...",
                    current_delay
                );
                fiber::sleep(current_delay);
                current_delay = std::cmp::min(current_delay * 2, MAX_DELAY);
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
    let (storage, raft_storage) = init_common(config, &cfg, resp.shredding)?;

    let raft_id = resp.instance.raft_id;
    transaction(|| -> Result<(), TntError> {
        storage.instances.put(&resp.instance).unwrap();
        for traft::PeerAddress {
            raft_id,
            address,
            connection_type,
        } in resp.peer_addresses
        {
            storage
                .peer_addresses
                .put(raft_id, &address, &connection_type)
                .unwrap();
        }
        raft_storage.persist_raft_id(raft_id).unwrap();
        raft_storage
            .persist_instance_name(&resp.instance.name)
            .unwrap();
        raft_storage
            .persist_cluster_name(config.cluster_name())
            .unwrap();
        raft_storage.persist_tier(config.instance.tier()).unwrap();
        Ok(())
    })
    .unwrap();

    // TODO: log cluster uuid here
    tlog!(Info, "joined cluster {}", config.cluster_name());
    tlog!(Info, "raft_id: {}", resp.instance.raft_id);
    tlog!(Info, "instance name: {}", resp.instance.name);
    tlog!(Info, "instance uuid: {}", resp.instance.uuid);
    tlog!(Info, "replicaset name: {}", resp.instance.replicaset_name);
    tlog!(Info, "replicaset uuid: {}", resp.instance.replicaset_uuid);
    tlog!(Info, "tier name: {}", resp.instance.tier);

    let instance_name = resp.instance.name;
    postjoin(config, storage, raft_storage)?;
    crate::audit!(
        message: "local database created on `{instance_name}`",
        title: "create_local_db",
        severity: Low,
        instance_name: %instance_name,
        raft_id: %raft_id,
        initiator: "admin",
    );
    crate::audit!(
        message: "local database connected on `{instance_name}`",
        title: "connect_local_db",
        severity: Low,
        instance_name: %instance_name,
        raft_id: %raft_id,
        initiator: "admin",
    );

    Ok(())
}

fn postjoin(
    config: &PicodataConfig,
    storage: Catalog,
    raft_storage: RaftSpaceAccess,
) -> Result<(), Error> {
    tlog!(Info, "entering post-join phase");

    config.validate_storage(&storage, &raft_storage)?;

    let current_tier_name = raft_storage
        .tier()?
        .expect("tier for instance should exists");

    if let Some(config) = &config.instance.audit {
        let raft_id = raft_storage
            .raft_id()
            .expect("failed to get raft_id for audit log")
            .expect("found zero raft_id during audit log init");
        let gen = raft_storage.gen().expect("failed to get gen for audit log");
        audit::init(config, raft_id, gen);
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

    let node = traft::node::Node::init(storage.clone(), raft_storage.clone(), false);
    let node = node.expect("failed initializing raft node");

    reapply_dynamic_parameters(&storage, &current_tier_name)?;

    let pg_config = &config.instance.pg;
    let instance_dir = config.instance.instance_dir();
    pgproto::init_once(pg_config, instance_dir, &node.storage)?;

    let raft_id = node.raft_id();
    let cs = raft_storage.conf_state().unwrap();
    if cs.voters == [raft_id] {
        #[rustfmt::skip]
        tlog!(Info, "this is the only voter in cluster, triggering election immediately");

        node.tick_and_yield(1); // apply configuration, if any
        node.campaign_and_yield().ok(); // trigger election immediately
        assert!(node.status().raft_state.is_leader());
    }

    tarantool::set_cfg_field("listen", config.instance.iproto_listen().to_host_port())
        .unwrap_or_else(|err| {
            panic!(
                "changing listen address to {} shouldn't fail: {}",
                config.instance.iproto_listen().to_host_port(),
                err
            );
        });

    // Start admin console
    let socket_uri = util::validate_and_complete_unix_socket_path(config.instance.admin_socket())?;
    let lua = ::tarantool::lua_state();
    lua.exec_with(r#"require('console').listen(...)"#, &socket_uri)?;

    let res = on_shutdown::setup_on_shutdown_trigger();
    if let Err(e) = res {
        tlog!(Error, "failed setting on_shutdown trigger: {e}");
    }

    // We will shut down, if we don't receive a confirmation of target state
    // change from leader before this time.
    let activation_deadline = Instant::now_fiber()
        .saturating_add(Duration::from_secs(config.instance.activation_deadline()));

    // This will be doubled on each retry, until max_retry_timeout is reached.
    let mut retry_timeout = Duration::from_millis(250);
    let max_retry_timeout = Duration::from_secs(5);

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

        if has_states!(instance, Expelled -> *) {
            return Err(Error::Expelled);
        }

        let cluster_name = raft_storage
            .cluster_name()
            .expect("storage should never fail");
        // Doesn't have to be leader - can be any online peer
        let leader_id = node.status().leader_id;
        let leader_address = leader_id.and_then(|id| {
            storage
                .peer_addresses
                .try_get(id, &traft::ConnectionType::Iproto)
                .ok()
        });
        let Some(leader_address) = leader_address else {
            // FIXME: don't hard code timeout
            let timeout = Duration::from_millis(250);
            tlog!(
                Debug,
                "leader address is still unknown, retrying in {timeout:?}"
            );
            fiber::sleep(timeout);
            continue;
        };

        #[allow(unused_mut)]
        let mut version = info::PICODATA_VERSION.to_string();
        #[cfg(feature = "error_injection")]
        crate::error_injection!("UPDATE_PICODATA_VERSION" => {
            version = std::env::var("PICODATA_INTERNAL_VERSION_OVERRIDE")
                .unwrap_or_else(|_| info::PICODATA_VERSION.to_string());
        });

        tlog!(
            Info,
            "initiating self-activation of instance {} ({})",
            instance.name,
            instance.uuid
        );
        let req = rpc::update_instance::Request::new(instance.name, cluster_name)
            .with_target_state(Online)
            .with_failure_domain(config.instance.failure_domain().clone())
            .with_picodata_version(version);
        let fut = rpc::network_call(
            &leader_address,
            proc_name!(rpc::update_instance::proc_update_instance),
            &req,
        )
        .timeout(activation_deadline - now);
        let error_message;
        match fiber::block_on(fut) {
            Ok(rpc::update_instance::Response {}) => {
                break;
            }
            Err(timeout::Error::Failed(TntError::Tcp(e))) => {
                error_message = e.to_string();
            }
            Err(timeout::Error::Failed(TntError::IO(e))) => {
                error_message = e.to_string();
            }
            Err(timeout::Error::Failed(e)) if e.error_code() == ErrorCode::NotALeader as u32 => {
                error_message = e.to_string();
            }
            Err(e) => {
                return Err(Error::other(format!(
                    "failed to activate myself: {e}, shutting down..."
                )));
            }
        }

        let timeout = retry_timeout.saturating_sub(now.elapsed());
        retry_timeout = max_retry_timeout.max(retry_timeout * 2);
        #[rustfmt::skip]
        tlog!(Warning, "failed to activate myself: {error_message}, retrying in {timeout:.02?}...");
        fiber::sleep(timeout);
    }

    // Wait for target state to change to Online, so that sentinel doesn't send
    // a redundant update instance request.
    // Otherwise incarnations grow by 2 every time.
    let timeout = Duration::from_secs(10);
    let deadline = fiber::clock().saturating_add(timeout);
    loop {
        if let Ok(instance) = storage.instances.get(&raft_id) {
            if has_states!(instance, * -> Online) {
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

    Ok(())
}

/// Updates to tarantool-sys might bring incompatible changes.
///
/// Most of the time those changes are accompanied by the compatibility
/// tweaks that give user a chance to revert to old behavior
/// by setting a corresponding key to `'old'`.
///
/// Other times, we may want to enable upcoming breaking changes
/// ahead of time using `'new'` where necessary.
///
/// Set the options we need for correct operation.
fn set_tarantool_compat_options() {
    tarantool::exec(
        "require 'compat' {
            c_func_iproto_multireturn = 'new',
        }",
    )
    .unwrap();

    // This setting was introduced in the following commits to tarantool-sys:
    //
    // - base64: add function to caclculate buffer size for decoding
    // - yaml: use standard base64 encoder
    // - lua: add varbinary type
    //
    // Their hashes will most certainly change after a regular
    // rebase of tarantool-sys onto a newer release branch, so
    // we list their names instead.
    //
    // Turns out those commits break traft & network.rs due
    // to the changes around MP_BIN and varbinary.
    //
    // The affected tests include:
    //
    // - picodata::traft::network::tests::multiple_messages
    // - picodata::traft::network::tests::unresponsive_connection
    //
    // TODO: properly support varbinary in crates tlua & tarantool.
    // TODO: https://git.picodata.io/core/tarantool-module/-/issues/239
    tarantool::exec(
        "require 'compat' {
            binary_data_decoding = 'old',
        }",
    )
    .unwrap();
}

::tarantool::define_enum_with_introspection! {
    /// IPROTO request types that are forbidden on picodata side
    #[repr(C)]
    enum ForbiddenIprotoTypes {
        // TODO(kbezuglyi): uncomment when vshard's
        // bucket distribution and discovery stop
        // using IPROTO_SELECT requests
        // SELECT = 1,
        INSERT = 2,
        REPLACE = 3,
        UPDATE = 4,
        DELETE = 5,
        CALL_16 = 6,
        // AUTH = 7, - needed
        // EVAL = 8, - needed
        UPSERT = 9,
        // CALL = 10, - needed
        // EXECUTE = 11, - needed, see [`redirect_iproto_execute_requests`]
        NOP = 12,
        PREPARE = 13,
        BEGIN = 14,
        COMMIT = 15,
        ROLLBACK = 16,
    }
}

/// Automatically forbids all unsupported IPROTO request types.
#[inline]
fn forbid_unsupported_iproto_requests() {
    for iproto_request in ForbiddenIprotoTypes::VARIANTS {
        // SAFETY: function is exported properly and
        // arguments with argument types are correct
        let rc = unsafe {
            tarantool::box_iproto_override(
                *iproto_request as _,
                Some(tarantool::iproto_override_cb_unsupported_requests),
                None,
                // INFO: as long as we are using constant array to represent forbidden IPROTO request
                // types, we are allowed to take a pointer to every type and pass to the `box_iproto_override`
                // as a context so it will not dangle after being passed to a callback
                iproto_request as *const _ as *const () as _,
            )
        };
        assert_eq!(rc, 0);
    }
}

/// Redirects all IPROTO_EXECUTE requests
/// into `sql::sql_dispatch` via FFI
/// `box_iproto_override` function
fn redirect_iproto_execute_requests() {
    let rc = unsafe {
        tarantool::box_iproto_override(
            ::tarantool::network::protocol::codec::IProtoType::Execute as _,
            Some(tarantool::iproto_override_cb_redirect_execute),
            None,
            std::ptr::null_mut(),
        )
    };
    assert_eq!(rc, 0);
}
