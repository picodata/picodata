#![allow(unknown_lints)]
#![allow(non_camel_case_types)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::let_and_return)]
#![allow(clippy::needless_return)]
#![allow(clippy::needless_late_init)]
#![allow(clippy::redundant_static_lifetimes)]
#![allow(clippy::redundant_pattern_matching)]
#![allow(clippy::bool_assert_comparison)]
#![allow(clippy::field_reassign_with_default)]
#![allow(clippy::collapsible_if)]
#![warn(clippy::perf)]
// Prevents ok_or(BoxError::new(...))
#![warn(clippy::or_fun_call)]

use ::raft::Storage;
use ::sql::frontend::sql::transform_to_regex_pattern;
use ::sql::frontend::sql::FUNCTION_NAME_MAPPINGS;
use config::apply_parameter;
use info::PICODATA_VERSION;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use storage::ToEntryIter;

use ::raft::prelude as raft;
use ::tarantool::error::BoxError;
use ::tarantool::error::Error as TntError;
use ::tarantool::error::IntoBoxError;
use ::tarantool::fiber::r#async::timeout::{self, IntoTimeout};
use ::tarantool::time::Instant;
use ::tarantool::tlua;
use ::tarantool::transaction::transaction;
use ::tarantool::{fiber, session};
use std::fs;
use std::time::Duration;
use storage::Catalog;
use traft::RaftSpaceAccess;

use crate::access_control::user_by_id;
use crate::address::HttpAddress;
use crate::error_code::ErrorCode;
use crate::instance::Instance;
use crate::instance::StateVariant::*;
use crate::schema::system_table_definitions;
use crate::schema::TableDef;
use crate::schema::ADMIN_ID;
use crate::schema::PICO_SERVICE_USER_NAME;
use crate::storage::schema::copy_dir_async;
use crate::storage::schema::copy_file_async;
use crate::storage::schema::ddl_meta_space_update_operable;
use crate::storage::PropertyName;
use crate::tarantool::{rm_tarantool_files, ListenConfig};
use crate::traft::error::Error;
use crate::traft::op;
use crate::traft::Result;
use crate::util::effective_user_id;
use backoff::SimpleBackoffManager;
use config::PicodataConfig;
use instance_uuid_file::dump_instance_uuid_file;
use instance_uuid_file::read_instance_uuid_file;
use instance_uuid_file::remove_instance_uuid_file;

mod access_control;
pub mod address;
pub mod audit;
pub mod auth;
pub mod backoff;
mod bootstrap_entries;
pub mod cas;
pub mod catalog;
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
pub mod instance_uuid_file;
pub mod introspection;
pub mod ipc;
mod iproto;
pub mod kvcell;
pub mod r#loop;
mod luamod;
pub mod mailbox;
pub mod metrics;
pub mod on_shutdown;
pub mod pgproto;
mod pico_service;
pub mod plugin;
pub mod reachability;
pub mod replicaset;
pub mod rpc;
mod sasl;
pub mod schema;
mod scram;
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
    (const $var:ident) => {
        &*&raw const $var
    };
    (mut $var:ident) => {
        &mut *&raw mut $var
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
    preload!("vshard.lref", "vshard/storage/ref.lua");
    preload!("vshard.registry", "vshard/registry.lua");
    preload!("vshard.replicaset", "vshard/replicaset.lua");
    preload!("vshard.service_info", "vshard/service_info.lua");
    preload!("vshard.storage.export_log", "vshard/storage/export_log.lua");
    preload!("vshard.storage.exports", "vshard/storage/exports.lua");
    preload!("vshard.storage.schema", "vshard/storage/schema.lua");
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
    preload!("sbroad.builtins", "sql-planner/src/builtins.lua");
    preload!("sbroad.dispatch", "src/sql/dispatch.lua");

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

    lua.exec(r#" require('sbroad.builtins').init() "#).unwrap();
    lua.exec(r#" require('sbroad.dispatch').init() "#).unwrap();

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

fn start_http_server(
    HttpAddress { host, port, .. }: &HttpAddress,
    registry: &'static prometheus::Registry,
) -> Result<(), Error> {
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
    .map_err(|err| {
        Error::other(format!(
            "failed to start http server on {host}:{port}: {err}",
        ))
    })?;

    lua.exec_with(
        r#"
              local handler = ...
              pico.httpd:route({method = 'POST', path = 'api/v1/session' }, function(req)
              local json = require('json')
              local body_string = req.body or ''
              local ok, data = pcall(function() return req:json() end)
              local username = ''
              local password = ''
              if ok and data then
                    username = data.username or ''
                    password = data.password or ''
              end
              return handler(username, password)
        end)"#,
        tlua::Function::new(|login: String, password: String| -> _ {
            http_server::wrap_api_result!(http_server::http_api_login(login, password))
        }),
    )
    .map_err(|err| {
        Error::other(format!(
            "failed to add route POST `/api/v1/session` to http server: {}",
            err
        ))
    })?;

    lua.exec_with(
        r#"
              local handler = ...
              pico.httpd:route({method = 'GET', path = 'api/v1/session' }, function(req)
              local auth_header = req.headers['authorization'] or ''
              return handler(auth_header)
        end)"#,
        tlua::Function::new(|auth_header: String| -> _ {
            http_server::wrap_api_result!(http_server::http_api_refresh_session(auth_header))
        }),
    )
    .map_err(|err| {
        Error::other(format!(
            "failed to add route GET `/api/v1/session` to http server: {}",
            err
        ))
    })?;

    lua.exec_with(
        r#"
              local handler = ...
              pico.httpd:route({method = 'GET', path = 'api/v1/tiers' }, function(req)
              local auth_header = req.headers['authorization'] or ''
              return handler(auth_header)
        end)"#,
        tlua::Function::new(|auth_header: String| -> _ {
            http_server::wrap_api_result!(http_server::http_api_tiers_with_auth(auth_header))
        }),
    )
    .map_err(|err| {
        Error::other(format!(
            "failed to add route `/api/v1/tiers` to http server: {err}",
        ))
    })?;

    lua.exec_with(
        r#"
              local handler = ...
              pico.httpd:route({method = 'GET', path = 'api/v1/cluster' }, function(req)
              local auth_header = req.headers['authorization'] or ''
              return handler(auth_header)
        end)"#,
        tlua::Function::new(|auth_header: String| -> _ {
            http_server::wrap_api_result!(http_server::http_api_cluster_with_auth(auth_header))
        }),
    )
    .map_err(|err| {
        Error::other(format!(
            "failed to add route `/api/v1/cluster` to http server: {err}",
        ))
    })?;

    lua.exec_with(
        r#"
              local handler = ...
              pico.httpd:route({method = 'GET', path = 'api/v1/config' }, function(req)
              return handler()
        end)"#,
        tlua::Function::new(|| -> _ {
            http_server::wrap_api_result!(http_server::http_api_config())
        }),
    )
    .map_err(|err| {
        Error::other(format!(
            "failed to add route `/api/v1/config` to http server: {err}",
        ))
    })?;

    // Initialize all of the metrics here!
    let register_metrics = || {
        metrics::register_metrics(registry)?;
        pgproto::register_metrics(registry)?;
        prometheus::Result::Ok(())
    };
    register_metrics().expect("failed to register metrics");

    lua.exec_with(
        r#"
        local user_metrics, picodata_metrics = ...
        pico.httpd:route({method = 'GET', path = 'metrics' }, function()
        local resp = require('metrics.plugins.prometheus').collect_http()
        resp.body = resp.body .. user_metrics() .. picodata_metrics()
        return resp
        end)"#,
        (
            tlua::Function::new(crate::plugin::metrics::get_plugin_metrics),
            tlua::Function::new(move || crate::metrics::collect_from_registry(registry)),
        ),
    )
    .map_err(|err| Error::other(format!("failed to add route `/metrics`: {err}")))?;

    Ok(())
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
                pico.httpd:route({path = '/login*path', method = 'GET'}, handler);
            end
            pico.httpd:route({path = '/' .. filename, method = 'GET'}, handler);
        end",
        bundle,
    )
    .expect("failed to add Web UI routes")
}

fn stored_procedures_are_initialized(log_non_found: bool) -> Result<bool> {
    let sys_func = ::tarantool::space::SystemSpace::Func.as_space();
    let sys_func_by_name = sys_func
        .index("name")
        .expect("_func should always have index by name");

    let mut failed = false;
    for proc in ::tarantool::proc::all_procs().iter() {
        let proc_name = proc.name();
        let t = sys_func_by_name
            .get(&[format!(".{proc_name}")])
            .expect("reading stored procedure metadata shouldn't fail");
        if t.is_some() {
            continue;
        }

        if !log_non_found {
            return Ok(false);
        }

        tlog!(Warning, "stored procedure .{proc_name} is not defined");
        failed = true;
    }

    Ok(!failed)
}

/// Initializes Tarantool stored procedures.
///
/// Those are used for inter-instance communication
/// (discovery, rpc, public proc api).
fn init_stored_procedures() {
    let lua = ::tarantool::lua_state();
    for proc in ::tarantool::proc::all_procs().iter() {
        let proc_name = proc.name();
        if FUNCTION_NAME_MAPPINGS
            .iter()
            .any(|name| name.rust_procedure == proc_name)
        {
            lua.exec_with(
                "local name, is_public = ...
                local proc_name = '.' .. name
                box.schema.func.create(proc_name, {language = 'C', if_not_exists = true, exports = {'LUA', 'SQL'}, returns = 'any'})
                if is_public then
                    box.schema.role.grant('public', 'execute', 'function', proc_name, {if_not_exists = true})
                end
                ",
                (proc_name, proc.is_public()),
            )
        .expect("this shouldn't fail");
        } else {
            lua.exec_with(
                "local name, is_public = ...
                local proc_name = '.' .. name
                box.schema.func.create(proc_name, {language = 'C', if_not_exists = true})
                if is_public then
                    box.schema.role.grant('public', 'execute', 'function', proc_name, {if_not_exists = true})
                end
                ",
                (proc_name, proc.is_public()),
            )
        .expect("this shouldn't fail");
        }
    }

    lua.exec(
        r#"
        box.schema.func.create('.proc_sql_dispatch', {language = 'C', if_not_exists = true})
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

    lua.exec(
        r#"
        box.schema.func.create('.proc_sql_execute', {language = 'C', if_not_exists = true})
        box.schema.role.grant('public', 'execute', 'function', '.proc_sql_execute', {if_not_exists = true})
        "#,
    )
    .expect(".proc_sql_execute registration should never fail");
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
fn set_login_check() {
    const MAX_ATTEMPTS_EXCEEDED: &str = "Maximum number of login attempts exceeded";
    const NO_LOGIN_PRIVILEGE: &str = "User does not have login privilege";

    enum Verdict {
        AuthOk,
        AuthFail,
        UnknownUser,
        UserBlocked(&'static str),
    }

    // Determines the outcome of an authentication attempt.
    let f = move |user_name: String, successful_authentication: bool, storage: &Catalog| {
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
    let compute_auth_verdict = f;

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
            let storage = match Catalog::try_get(false) {
                Ok(v) => v,
                Err(err) => {
                    tlog!(Error, "failed accessing storage: {err}");
                    // An iproto connection was opened before the global storage
                    // was initialized. This is possible in rare cases in a short
                    // window of time when an instance is booting up.
                    // In this case we simply drop the connection,
                    // because we can't yet determine verify anything.
                    // From the client's perspective the iproto interface is not
                    // yet ready to receive connections.
                    tlua::error!(lua, "{}", err);
                }
            };

            match compute_auth_verdict(user.clone(), status, storage) {
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
        apply_parameter(parameter, current_tier)?;
    }

    Ok(())
}

fn get_shredding_marker_path(config: &PicodataConfig) -> PathBuf {
    config.instance.instance_dir().join("pico_shredding_marker")
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Serialize, Deserialize)]
pub enum Entrypoint {
    StartDiscover,
    StartBoot,
    StartPreJoin {
        leader_address: String,
        instance_uuid: Option<String>,
    },
    StartJoin {
        join_response: rpc::join::Response,
    },
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
        StartPreJoin {
            leader_address,
            instance_uuid,
        } => {
            // Cleanup the instance directory with WALs from the previous StartDiscover run
            tarantool::rm_tarantool_files(config.instance.instance_dir())?;
            next_entrypoint = start_pre_join(config, leader_address, instance_uuid)?;
        }
        StartJoin { ref join_response } => {
            // Cleanup the instance directory with WALs from the previous StartJoin run
            tarantool::rm_tarantool_files(config.instance.instance_dir())?;
            start_join(config, join_response)?;
        }
    }

    Ok(next_entrypoint)
}

fn ensure_marker_file_exists_durable(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    // use `create_new` to skip writing the contents/fsyncing when a marker already exist
    match std::fs::File::create_new(path) {
        Ok(mut file) => {
            // a new marker file was created; write the contents
            file.write_all(contents)?;

            // now try to make sure the written marker file is synchronized to disk
            // sync the file itself
            nix::unistd::fsync(file.as_raw_fd())?;
            drop(file);

            // and sync the parent directory too
            let parent_path = path.parent().expect("marker file path must have a parent");
            let dir = nix::dir::Dir::open(
                parent_path,
                nix::fcntl::OFlag::O_RDONLY,
                nix::sys::stat::Mode::empty(),
            )?;
            nix::unistd::fsync(dir.as_raw_fd())?;
            drop(dir);

            Ok(())
        }
        // do not write to file in case a marker file already exists
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

/// Performs tarantool initialization calling `box.cfg` for the first time.
///
/// This function is called from:
///
/// - `start_restore`
/// - `start_discover`
/// - `start_boot`
/// - `start_pre_join`
/// - `start_join`
///
/// The `shredding` parameter will be used to initialize the tarantool removal function ([`tarantool::xlog_set_remove_file_impl`]).
fn init_common(
    config: &PicodataConfig,
    cfg: &tarantool::Cfg,
    shredding: bool,
) -> Result<(), Error> {
    // Note: we should do this *before* calling `box.cfg {}`.
    crate::auth::register_tarantool_auth_methods();

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

    iproto::tls_init_once(&config.instance.iproto_tls)?;

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

    tarantool::xlog_set_remove_file_impl(shredding);
    if shredding {
        // put down a shredding marker file so that we could determine whether to enable shredding on instance restart.
        // this is required because we can't read `_pico_db_config` before a file removal can take place.
        // note that this marker will not reflect changes made to `_pico_db_config` table.
        let shredding_marker_path = get_shredding_marker_path(config);
        ensure_marker_file_exists_durable(
            &shredding_marker_path,
            b"this file signals picodata to shred all deleted files",
        )
        .map_err(|err| {
            Error::other(format!(
                "failed writing shredding marker {}: {}",
                shredding_marker_path.display(),
                err
            ))
        })?;
    }

    set_tarantool_compat_options();

    // Load Lua libraries
    preload_vshard();
    preload_http();
    init_sbroad();

    set_console_prompt();
    redirect_interactive_sql();

    // Setup plugin RPC handlers
    plugin::rpc::server::init_handlers();
    rpc::init_static_proc_set();

    // Setup IPROTO redirection
    forbid_unsupported_iproto_requests();
    redirect_iproto_execute_requests();

    // Setup access checks
    set_login_check();
    set_on_access_denied_audit_trigger();

    Ok(())
}

/// Initialize a fresh picodata instance storage snapshot. Returns storage
/// access wrapper structs for the newly initialized storage.
///
/// This function is called in the following scenarios:
/// - when a new instance is entering a "discovery phase" it calls this function
///   to initialize the storage because it needs some minimum amount of
///   functionality for the discovery algorithm
/// - when a new instance is bootstrapping a new picodata cluster it calls this
///   function to initialize the storage
/// - when a new instance is joining a cluster as the first replica in a new
///   replicaset it calls this function to initialize the storage
///
/// The storage initialization includes the following:
/// - creating `_pico_*` system tables which are necessary for other parts of
///   the system to function
/// - creating `pico_service` system user account, which is used for all
///   internal RPC communication between cluster instances
/// - creating `.proc_*` stored procedure definitions which are use for all
///   internal RPC communication between cluster instances
fn bootstrap_storage_on_master() -> Result<(Catalog, RaftSpaceAccess)> {
    assert!(!storage::storage_is_initialized());

    tlog!(Debug, "initializing global storage");

    // Create picodata system table definitions in `_space`, `_index`, etc.
    let storage = Catalog::try_get(true).expect("storage initialization should never fail");

    let raft_storage =
        RaftSpaceAccess::new().expect("raft storage initialization should never fail");

    // Create a `pico_service` user record in `_user` tarantool space.
    assert!(!schema::user_pico_service_is_initialized());
    schema::init_user_pico_service();

    // Create a stored procedure records in `_func` tarantool space.
    assert!(!stored_procedures_are_initialized(false)?);
    init_stored_procedures();

    tlog!(Info, "initialized picodata storage");

    // The actual system_catalog_version field in _pico_property will only be
    // populated from the bootstrap_entries once the raft main loop gets going.
    let system_catalog_version = storage::LATEST_SYSTEM_CATALOG_VERSION;
    tlog!(Info, "system catalog version: {system_catalog_version}");

    // Create `_pico_governor_queue` space.
    storage.governor_queue.create_space()?;
    // Create `_pico_user_audit_policy` space.
    storage.users_audit_policies.create_space()?;

    Ok((storage.clone(), raft_storage))
}

/// Get the storage access wrapper structs for the already initialized picodata
/// storage.
///
/// This function is called in the following scenarios:
/// - an instance which has already joined the cluster is restarting after being
///   shut down temporarily calls this function because the storage is restored
///   from disk from a previous execution
/// - a new instance is joining a cluster as a read-only replica in an
///   existing replicaset calls this function because it receives the schema
///   definitions from the master replica via tarantool replication
/// - a new instance is in the "discovery" phase and has been shut down
///   prematurely calls this function to restore the state and proceed with the
///   discovery algorithm
///
/// Note that this function is also called when a newer version of picodata
/// is restarting on older snapshots (with older `system_catalog_version`). In
/// this case this function **does not** upgrade the system schema (no new
/// tables, stored procedures, etc. are created). Upgrading the schema is the
/// responsibility of the [governor].
///
/// **No storage modifications are performed by this function.**
fn get_initialized_storage() -> Result<Option<(Catalog, RaftSpaceAccess)>> {
    // Make sure picodata system table definitions are created in `_space`, `_index`, etc.
    if !storage::storage_is_initialized() {
        return Ok(None);
    }

    // Create the storage wrapper structs to be used throughout the code base.
    let storage = Catalog::try_get(true).expect("storage initialization should never fail");
    let raft_storage =
        RaftSpaceAccess::new().expect("raft storage initialization should never fail");

    // Make sure `pico_service` user record is in `_user` tarantool space.
    assert!(schema::user_pico_service_is_initialized());

    // Make sure stored procedure records are in `_func` tarantool space.
    // FIXME: we want to make this an assertion, but at the moment we can't
    // because it will fail at the moment of upgrade if new stored procedures
    // were added. We need a better system_catalog_version aware validation
    // routine. See also <https://git.picodata.io/core/picodata/-/issues/961>
    stored_procedures_are_initialized(true)?;

    tlog!(Info, "recovered picodata storage");

    let res = storage.properties.system_catalog_version()?;
    if let Some(system_catalog_version) = res {
        tlog!(Info, "system catalog version: {system_catalog_version}");
    } else {
        tlog!(Info, "system catalog version not yet known");
    }

    Ok(Some((storage.clone(), raft_storage)))
}

fn start_discover(config: &PicodataConfig) -> Result<Option<Entrypoint>, Error> {
    tlog!(Info, "entering discovery phase");

    luamod::setup();
    assert!(!tarantool::is_box_configured());

    let cfg = tarantool::Cfg::for_discovery(config)?;

    // when an instance restarts it initially runs in discovery mode.
    // we need to check for `pico_shredding_marker`, which is a proxy for `cluster.shredding` config option.
    // a more proper way would be to read `_pico_db_config` table, but we can't do this without initializing tarantool storage,
    //   which by itself may cause deletions, so it would be too late.
    let enable_shredding = get_shredding_marker_path(config).exists();

    init_common(config, &cfg, enable_shredding)?;
    let can_vote = {
        let tiers = config.cluster.tiers();
        let my_tier_name = config.instance.tier();
        let Some(tier) = tiers.get(my_tier_name) else {
            return Err(Error::other(format!(
                "invalid configuration: current instance is assigned tier '{my_tier_name}' which is not defined in the configuration file",
            )));
        };
        tier.can_vote
    };
    discovery::init_global(
        config.instance.peers().iter().map(|a| a.to_host_port()),
        can_vote,
    );

    if let Some((storage, raft_storage)) = get_initialized_storage()? {
        if let Some(raft_id) = raft_storage.raft_id()? {
            let cluster_uuid = raft_storage.cluster_uuid()?;
            let cluster_uuid =
                ::uuid::Uuid::parse_str(&cluster_uuid).expect("invalid cluster_uuid in storage");
            tarantool::init_cluster_uuid(cluster_uuid);
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
        } else {
            tlog!(Warning, "picodata storage is initialized but the raft node is not, instance likely previously terminated prematurely");
        }

        let join_state = raft_storage.join_state()?;
        if join_state.is_some() {
            let uuid = raft_storage
                .instance_uuid()?
                .expect("instance uuid is always persisted with join_state on rebootstrap");

            let role = discovery::wait_global();
            match role {
                discovery::Role::Leader { .. } => {
                    unreachable!("we cannot be a leader when joining to the cluster after membership inconsistency");
                }
                discovery::Role::NonLeader { leader } => {
                    let next_entrypoint = Entrypoint::StartPreJoin {
                        leader_address: leader,
                        instance_uuid: Some(uuid),
                    };
                    return Ok(Some(next_entrypoint));
                }
            }
        }
    } else if let Some(instance_uuid) = read_instance_uuid_file(config)? {
        // This can happen in a rare case when picodata exits after reboostrap
        // where the snapshot files have already been removed, but before
        // box.cfg() successfully created the new snapshots. In this case after
        // restart we just go to the StartPreJoin entrypoint again with the same
        // instance_uuid which should be properly handled by leader.
        let role = discovery::wait_global();
        match role {
            discovery::Role::Leader { .. } => {
                unreachable!("we cannot be a leader when joining to the cluster after membership inconsistency");
            }
            discovery::Role::NonLeader { leader } => {
                let next_entrypoint = Entrypoint::StartPreJoin {
                    leader_address: leader,
                    instance_uuid: Some(instance_uuid),
                };
                return Ok(Some(next_entrypoint));
            }
        }
    } else {
        // When going into the discovery procedure we don't need the whole storage
        // initialized but it's simpler to just call this instead of picking just
        // the things we need which is `.proc_discover`, the auth trigger and the
        // `_pico_user` table it needs, etc.
        bootstrap_storage_on_master()?;
    }

    // Start listening only after we've checked if this is a restart.
    // Postjoin phase has its own idea of when to start listening.
    let tls_config = &config.instance.iproto_tls;
    let listen_config =
        ListenConfig::new(config.instance.iproto_listen().to_host_port(), tls_config);
    tarantool::set_cfg_field("listen", listen_config).map_err(|err| {
        Error::other(format!(
            "failed to start listening on iproto {}: {}",
            config.instance.iproto_listen().to_host_port(),
            err
        ))
    })?;

    let role = discovery::wait_global();
    let next_entrypoint = match role {
        discovery::Role::Leader { .. } => Entrypoint::StartBoot,
        discovery::Role::NonLeader { leader } => Entrypoint::StartPreJoin {
            leader_address: leader,
            instance_uuid: None,
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

    assert!(
        tier.can_vote,
        "instance with instance_name '{instance_name}' from tier '{my_tier_name}' with `can_vote = false` cannot be a bootstrap leader"
    );

    luamod::setup();
    assert!(!tarantool::is_box_configured());

    let cfg = tarantool::Cfg::for_cluster_bootstrap(config, &instance)?;
    init_common(config, &cfg, config.cluster.shredding())?;
    let (storage, raft_storage) = bootstrap_storage_on_master()?;

    let cs = raft::ConfState {
        voters: vec![raft_id],
        ..Default::default()
    };

    let cluster_uuid = uuid::Uuid::new_v4();
    tarantool::init_cluster_uuid(cluster_uuid);
    let cluster_uuid = cluster_uuid.to_hyphenated().to_string();

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
        raft_storage.persist_cluster_uuid(&cluster_uuid).unwrap();
        raft_storage.persist_entries(&bootstrap_entries).unwrap();
        raft_storage.persist_conf_state(&cs).unwrap();
        raft_storage.persist_hard_state(&hs).unwrap();
        Ok(())
    })
    .expect("transactions should not fail as it introduces unrecoverable state");

    tlog!(Info, "cluster_uuid {}", cluster_uuid);
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

fn restore_from_backup(config: &PicodataConfig, backup_path: &PathBuf) -> Result<(), Error> {
    tlog!(Info, "entering instance restore phase");

    let instance_dir = config.instance.instance_dir();
    let share_dir = config.instance.share_dir();
    let share_dir_name = share_dir
        .file_name()
        .and_then(|n| n.to_str())
        .expect("Share dir should be a valid name");

    if backup_path == instance_dir {
        return Err(Error::other(String::from(
            "Chosen backup directory is currently used instance dir",
        )));
    }

    // Cleanup the instance directory with WALs existing before BACKUP run.
    rm_tarantool_files(instance_dir)?;

    // Move data from backup dir to instance data dir.
    for entry in fs::read_dir(backup_path)? {
        let Ok(entry) = entry else {
            return Err(Error::other(String::from(
                "Unable to operate with entry under backup dir",
            )));
        };
        let path_src = entry.path();
        let entry_name = path_src
            .file_name()
            .and_then(|n| n.to_str())
            .expect("Entry from backup dir should have a file name");

        if entry_name == share_dir_name {
            // Currently it's the only dir of backup which we shouldn't
            // copy to instance_dir.
            copy_dir_async(&path_src, share_dir, false)?;
            continue;
        }

        let path_dest = instance_dir.join(entry_name);

        // We don't have to check extenstion here
        // as we want to move all files from backup dir
        // (including .picodata-cookie and config) into instance_dir.
        if path_src.is_file() {
            // TODO: Am I right that we want to copy files without using
            //       hardlinks when restoring from a backup?
            //       See https://git.picodata.io/core/picodata/-/issues/2184.
            copy_file_async(&path_src, &path_dest)?;
        } else {
            copy_dir_async(&path_src, &path_dest, false)?;
        }
        tlog!(
            Info,
            "Restore moved {} to {}",
            path_src.display(),
            path_dest.display()
        );
    }

    luamod::setup();
    assert!(!tarantool::is_box_configured());

    let cfg = tarantool::Cfg::for_restore(config)?;
    init_common(config, &cfg, config.cluster.shredding())?;
    let (storage, raft_storage) =
        get_initialized_storage()?.expect("Initialized storage should be available on restore");

    // Remove pending_schema_change flag that
    // was set during BACKUP execution (so that
    // BACKUP is not executed again when instance is restarted).
    let properties = &storage.properties;
    properties
        .delete(PropertyName::PendingSchemaChange)
        .expect("storage should not fail");
    properties
        .delete(PropertyName::PendingSchemaVersion)
        .expect("storage should not fail");

    // Decrease NextSchemaVersion that was set on DdlPrepare exectuion.
    let current_schema_version: u64 = properties
        .get(&PropertyName::NextSchemaVersion)
        .expect("storage should not fail")
        .expect("property should exist");
    properties.put(
        PropertyName::NextSchemaVersion,
        &(current_schema_version - 1),
    )?;

    let _ = transaction(|| -> Result<()> {
        // Call log compaction to remove stale DdlPrepare opcode saved during BACKUP execution
        let last_index = raft_storage
            .last_index()
            .expect("last_index should be valid");
        raft_storage
            .compact_log(last_index + 1)
            .expect("log compaction should not fail");
        Ok(())
    });

    // Return tables to "opearable" state.
    //
    // TODO: It's possible that in future we'll use `operable` flag not only for
    //       long-running DDL operations. In such a case execution of `restore`
    //       will switch the flag where it shouldn't.
    //       See https://git.picodata.io/core/picodata/-/issues/2185.
    let system_table_defs: Vec<TableDef> = system_table_definitions()
        .into_iter()
        .map(|(td, _)| td)
        .collect();

    for table_def in storage.pico_table.iter().expect("storage should not fail") {
        if system_table_defs.contains(&table_def) {
            continue;
        }

        ddl_meta_space_update_operable(&storage, table_def.id, true)
            .expect("storage shouldn't fail");
    }

    // Save updated version of .snap.
    let lua = ::tarantool::lua_state();
    lua.exec("box.snapshot()")
        .expect("snapshot shouldn't fail on restore");

    Ok(())
}

fn start_pre_join(
    config: &PicodataConfig,
    instance_address: String,
    instance_uuid_opt: Option<String>,
) -> Result<Option<Entrypoint>, Error> {
    tlog!(
        Info,
        "joining cluster, peer address: {instance_address}, instance_uuid: {instance_uuid_opt:?}"
    );

    let instance_uuid: String;
    if let Some(uuid) = instance_uuid_opt {
        // Instance uuid is already known from a previous attempt to join the
        // cluster. This can happen if the instance unexpectedly exited before
        // receiving and handling the response to proc_raft_join RPC. In this
        // case we simply send another proc_raft_join RPC with the same
        // instance_uuid and everything works correctly.
        instance_uuid = uuid;
        iproto::tls_init_once(&config.instance.iproto_tls)?;
    } else {
        // This is this initial attempt to join the cluster as a new picodata
        // instance. We need to generate a new instance_uuid and persist it to
        // storage before sending the proc_raft_join RPC. This means that we
        // need to initialize the storage. But there's a problem: when
        // initializing we must also initialize the replicaset, but we don't
        // know yet which replicaset this instance is going to be a part of,
        // this decision is made in `proc_raft_join` on leader and we will only
        // know the result when we get the response.
        //
        // The solution is: we bootstrap a throw-away temporary replicaset just
        // so we can initialize storage and persist the instance_uuid. After
        // that once we received the proc_raft_join response from leader and we
        // know which replicaset to join we rebootstrap the storage and go
        // directly to `start_post_join`.

        assert!(!tarantool::is_box_configured());

        // TODO: explain lua setup
        luamod::setup();

        let uuid = uuid::Uuid::new_v4().to_hyphenated().to_string();
        tlog!(Info, "generated instance uuid: {uuid}");

        // Note that we persist instance_uuid to the storage, but that is not
        // enough in some rare cases when picodata crashes in a short period of
        // time after the rebootstrap and before the subsequent box.cfg() call.
        // For that reason we also persist instance_uuid to a regular old file.
        dump_instance_uuid_file(&uuid, config)?;

        let tnt_cfg = tarantool::Cfg::for_instance_pre_join(config, uuid.clone())?;
        // shredding=false because the Tarantool configuration is temporary
        // and we will rebootstrap later, so no user data is persisted
        init_common(config, &tnt_cfg, false)?;

        let (_, raft_storage) = bootstrap_storage_on_master()?;
        transaction(|| -> Result<(), TntError> {
            raft_storage.persist_instance_uuid(&uuid)?;
            raft_storage
                .persist_join_state("prepare".to_owned())
                .unwrap();
            Ok(())
        })
        .expect("transactions should not fail as it introduces unrecoverable state");

        instance_uuid = uuid;
    };

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
        pgproto_advertise_address: config.instance.pgproto_advertise().to_host_port(),
        failure_domain: config.instance.failure_domain().clone(),
        tier: config.instance.tier().into(),
        picodata_version: version,
        uuid: instance_uuid,
    };

    const INITIAL_TIMEOUT: Duration = Duration::from_secs(1);
    const MAX_TIMEOUT: Duration = Duration::from_secs(60);
    let mut backoff = SimpleBackoffManager::new("proc_raft_join RPC", INITIAL_TIMEOUT, MAX_TIMEOUT);

    let resp: rpc::join::Response = loop {
        let current_timeout = backoff.timeout();
        let f = rpc::network_call(
            &instance_address,
            proc_name!(rpc::join::proc_raft_join),
            &req,
        )
        .timeout(current_timeout);
        let res = fiber::block_on(f);
        match res {
            Ok(resp) => {
                crate::error_injection!(exit "EXIT_AFTER_RPC_PROC_RAFT_JOIN");
                break resp;
            }
            Err(timeout::Error::Expired) => {
                tlog!(
                    Warning,
                    "join request timed out after {:?}, retrying...",
                    current_timeout
                );
                backoff.handle_failure();
                continue;
            }
            Err(timeout::Error::Failed(e @ (TntError::ConnectionClosed(_) | TntError::IO(_)))) => {
                tlog!(
                    Warning,
                    "join request failed: {e}, retrying in {:?}...",
                    current_timeout
                );
                fiber::sleep(current_timeout);
                backoff.handle_failure();
                continue;
            }
            Err(e) => {
                return Err(Error::other(format!(
                    "join request failed: {e}, shutting down..."
                )));
            }
        }
    };

    return Ok(Some(Entrypoint::StartJoin {
        join_response: resp,
    }));
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

    // Build a Prometheus registry with const labels (instance_name)
    let registry: &'static prometheus::Registry = {
        let instance_name_for_metrics = raft_storage
            .instance_name()?
            .expect("instance_name should be already set")
            .to_string();
        let mut labels = std::collections::HashMap::new();
        labels.insert("instance_name".to_string(), instance_name_for_metrics);
        let reg = prometheus::Registry::new_custom(None, Some(labels))
            .expect("failed to create Prometheus registry");
        Box::leak(Box::new(reg))
    };

    if let Some(addr) = &config.instance.http_listen {
        start_http_server(addr, registry)?;
        if cfg!(feature = "webui") {
            tlog!(Info, "Web UI is enabled");
        } else {
            tlog!(Info, "Web UI is disabled");
        }
        #[cfg(feature = "webui")]
        start_webui();
    }
    // Set global label for Tarantool metrics: instance_name
    {
        let instance_name_for_metrics = raft_storage
            .instance_name()?
            .expect("instance_name should be already set");
        let lua = ::tarantool::lua_state();
        lua.exec_with(
            r#"
            local name = ...;
            require('metrics').set_global_labels({ instance_name = name })
            "#,
            instance_name_for_metrics.to_string(),
        )?;
    }
    // Execute postjoin script if present
    if let Some(ref script) = config.instance.deprecated_script {
        let l = ::tarantool::lua_state();
        l.exec_with(
            "dofile(...)",
            script.to_str().ok_or_else(|| {
                Error::other(format!(
                    "postjoin script path {} is not encoded in UTF-8",
                    script.to_string_lossy()
                ))
            })?,
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

    let tls_config = &config.instance.iproto_tls;
    let listen_config =
        ListenConfig::new(config.instance.iproto_listen().to_host_port(), tls_config);
    tarantool::set_cfg_field("listen", listen_config).map_err(|err| {
        Error::other(format!(
            "failed to change listen address to {}: {}",
            config.instance.iproto_listen().to_host_port(),
            err
        ))
    })?;

    // Start admin console, set permission mode on socket file to 0660
    let socket_uri = util::validate_and_complete_unix_socket_path(config.instance.admin_socket())?;
    let lua = ::tarantool::lua_state();
    lua.exec_with(
        r#"
    local uri, permissions  = ...;
    require('console').listen(uri, tonumber(permissions, 8))
    "#,
        (&socket_uri, "660"),
    )?;

    let res = on_shutdown::setup_on_shutdown_trigger();
    if let Err(e) = res {
        tlog!(Error, "failed setting on_shutdown trigger: {e}");
    }

    // We will shut down, if we don't receive a confirmation of target state
    // change from leader before this time.
    let boot_timeout =
        Instant::now_fiber().saturating_add(Duration::from_secs(config.instance.boot_timeout()));

    // This will be doubled on each retry, until max_retry_timeout is reached.
    let base_timeout = Duration::from_millis(250);
    let max_timeout = Duration::from_secs(5);
    let mut backoff =
        SimpleBackoffManager::new("proc_update_instance RPC", base_timeout, max_timeout);

    // When the whole cluster is restarting we use a smaller election timeout so
    // that we don't wait too long.
    const BOOTSTRAP_ELECTION_TIMEOUT: Duration = Duration::from_secs(3);
    // Use a random factor so that hopefully everybody doesn't start the
    // election at the same time.
    let random_factor = 1.0 + rand::random::<f64>();
    let election_timeout =
        Duration::from_secs_f64(BOOTSTRAP_ELECTION_TIMEOUT.as_secs_f64() * random_factor);
    let mut next_election_try = fiber::clock().saturating_add(election_timeout);

    // Activates instance
    loop {
        let now = fiber::clock();
        if now > boot_timeout {
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
            return Err(BoxError::new(
                ErrorCode::InstanceExpelled,
                "current instance is expelled from the cluster",
            )
            .into());
        }

        let cluster_name = raft_storage
            .cluster_name()
            .expect("storage should never fail");
        let cluster_uuid = raft_storage
            .cluster_uuid()
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

            // Leader has been unknown for too long
            let election_timeout_expired = fiber::clock() >= next_election_try;

            // When a raft node is initialized the term is set to 0, but it's
            // increased as soon as a raft configuration is applied. So term = 0
            // means that this node has not seen any configuration changes yet
            // and it makes no sense trying to promote to leader (actually
            // it makes sense NOT to promote, as there are some `.unwrap()`
            // calls in raft-rs which will panic in this case).
            let seen_configuration_changes = node.status().term != 0;

            if election_timeout_expired && seen_configuration_changes {
                // Normally we should get here only if the whole cluster of
                // several instances is restarting at the same time, because
                // otherwise the raft leader should be known and the waking up
                // instance should find out about them via raft_main_loop.
                //
                // Note that everybody will not start the election at the same
                // time because of `random_factor` applied to the
                // `election_timeout` above.
                //
                // Also note that even if the raft leader is chosen in a cluster
                // a mulfanctioning instance trying to become the new leader
                // will not affect the healthy portion of the cluster thanks to
                // the pre_vote extension to the raft algorithm which is used in
                // picodata.
                tlog!(Info, "leader not known for too long, trying to promote");
                node.campaign_and_yield().ok();
                next_election_try = fiber::clock().saturating_add(election_timeout);
            }

            fiber::sleep(timeout);
            continue;
        };
        tlog!(Debug, "leader address is known: {leader_address}");

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
        let req = rpc::update_instance::Request::new(instance.name, cluster_name, cluster_uuid)
            .with_target_state(Online)
            .with_failure_domain(config.instance.failure_domain().clone())
            .with_picodata_version(version);
        let fut = rpc::network_call(
            &leader_address,
            proc_name!(rpc::update_instance::proc_update_instance),
            &req,
        )
        .timeout(boot_timeout - now);
        let error_message;
        match fiber::block_on(fut) {
            Ok(rpc::update_instance::Response {}) => {
                break;
            }
            Err(timeout::Error::Failed(TntError::Tcp(e))) => {
                // A network error happened. Try again later.
                error_message = e.to_string();
            }
            Err(timeout::Error::Failed(TntError::IO(e))) => {
                // Hopefully a network error happened? Try again later.
                error_message = e.to_string();
            }
            Err(timeout::Error::Failed(e)) if e.error_code() == ErrorCode::NotALeader as u32 => {
                // Our info about raft leader is outdated, just wait a while for
                // it to update and send a request to hopefully the new leader.
                error_message = e.to_string();
            }
            Err(timeout::Error::Failed(e)) if e.error_code() == ErrorCode::LeaderUnknown as u32 => {
                // The peer no longer knows who the raft leader is. This is
                // possible for example if a leader election is in progress. We
                // should just wait some more and try again later.
                error_message = e.to_string();
            }
            Err(e) => {
                // Other kinds of errors, which can't/shouldn't be fixed by a "try again later" strategy
                return Err(Error::other(format!(
                    "failed to activate myself: {e}, shutting down..."
                )));
            }
        }

        backoff.handle_failure();
        let timeout = backoff.timeout();
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

fn start_join(
    config: &PicodataConfig,
    resp: &rpc::join::Response,
) -> Result<Option<Entrypoint>, Error> {
    luamod::setup();

    let cfg = tarantool::Cfg::for_instance_join(config, resp)?;

    crate::error_injection!(exit "EXIT_AFTER_REBOOTSTRAP_BEFORE_STORAGE_INIT_IN_START_JOIN");

    // XXX: Initialize cluster uuid before opening any iproto connections.
    let uuid = resp
        .cluster_uuid
        .parse()
        .expect("invalid cluster_uuid received from join response");

    tarantool::init_cluster_uuid(uuid);

    init_common(config, &cfg, resp.shredding)?;

    let is_master = !cfg.read_only;
    let (storage, raft_storage) = if is_master {
        bootstrap_storage_on_master()?
    } else {
        // In case of read-only replica the storage is initialized by the master
        // and this instance receives all the data via tarantool replication.
        get_initialized_storage()?
            .expect("read-only replica should receive tarantool snapshot from master")
    };

    let raft_id = resp.instance.raft_id;
    transaction(|| -> Result<(), TntError> {
        storage.instances.put(&resp.instance).unwrap();
        for traft::PeerAddress {
            raft_id,
            address,
            connection_type,
        } in &resp.peer_addresses
        {
            storage
                .peer_addresses
                .put(*raft_id, address, connection_type)
                .unwrap();
        }
        raft_storage.persist_raft_id(raft_id).unwrap();
        raft_storage
            .persist_instance_name(&resp.instance.name)
            .unwrap();
        raft_storage
            .persist_instance_uuid(&resp.instance.uuid)
            .unwrap();
        raft_storage
            .persist_cluster_name(config.cluster_name())
            .unwrap();
        raft_storage
            .persist_cluster_uuid(&resp.cluster_uuid)
            .unwrap();
        raft_storage.persist_tier(config.instance.tier()).unwrap();
        raft_storage
            .persist_join_state("confirm".to_owned())
            .unwrap();
        Ok(())
    })
    .expect("transactions should not fail as it introduces unrecoverable state");

    // At this point it's safe to remove the instance_uuid file, because it's
    // persisted in the final storage and we don't rebootstrap after this point.
    remove_instance_uuid_file(config)?;

    let cluster_uuid = raft_storage
        .cluster_uuid()
        .expect("storage should never fail");

    tlog!(Info, "cluster_uuid {}", cluster_uuid);
    tlog!(Info, "joined cluster {}", config.cluster_name());
    tlog!(Info, "raft_id: {}", resp.instance.raft_id);
    tlog!(Info, "instance name: {}", resp.instance.name);
    tlog!(Info, "instance uuid: {}", resp.instance.uuid);
    tlog!(Info, "replicaset name: {}", resp.instance.replicaset_name);
    tlog!(Info, "replicaset uuid: {}", resp.instance.replicaset_uuid);
    tlog!(Info, "tier name: {}", resp.instance.tier);

    // XXX: We should only do this after we've called tarantool::init_cluster_uuid!
    tlog!(Info, "enabling replication via box.cfg.replication");
    tarantool::set_cfg(&cfg)?;

    let instance_name = resp.instance.name.clone();
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

    Ok(None)
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
