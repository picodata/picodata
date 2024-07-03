use picoplugin::internal::types::{Dml, Op, Predicate};
use picoplugin::log::rs_log;
use picoplugin::plugin::interface::{CallbackResult, DDL};
use picoplugin::plugin::prelude::*;
use picoplugin::sql::types::SqlValue;
use picoplugin::system::tarantool::decimal::Decimal;
use picoplugin::system::tarantool::error::BoxError;
use picoplugin::system::tarantool::index::{IndexOptions, IndexType, Part};
use picoplugin::system::tarantool::space::{Field, SpaceCreateOptions, SpaceType, UpdateOps};
use picoplugin::system::tarantool::tlua::{LuaFunction, LuaRead, LuaThread, PushGuard};
use picoplugin::system::tarantool::tuple::Tuple;
use picoplugin::system::tarantool::util::DisplayAsHexBytes;
use picoplugin::system::tarantool::{fiber, index, tlua};
use picoplugin::transport::context::Context;
use picoplugin::transport::rpc;
use picoplugin::{internal, log, system};
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::fmt::Display;
use std::rc::Rc;
use std::str::FromStr;
use std::sync;
use std::time::Duration;
use time_macros::datetime;

struct ErrInjection;

impl ErrInjection {
    pub fn init(services: &[&str]) {
        let lua = tarantool::lua_state();
        lua.exec("if _G['err_inj'] == nil then _G['err_inj'] = {} end")
            .unwrap();

        for &service in services {
            let exec_code = format!(
                r#"
                    if _G['err_inj']['{service}'] == nil then
                        _G['err_inj']['{service}'] = {{}}
                    end
                "#
            );
            lua.exec(&exec_code).unwrap();
        }
    }

    fn get_err_inj<T>(key: &str, service: &str) -> Option<T>
    where
        T: for<'a> LuaRead<PushGuard<LuaFunction<PushGuard<&'a LuaThread>>>>,
    {
        let lua = tarantool::lua_state();
        let code = format!("return _G['err_inj']['{service}']['{key}']");
        lua.eval(&code).unwrap()
    }

    pub fn err_at_on_start(service: &str) -> Option<bool> {
        Self::get_err_inj("on_start", service)
    }

    pub fn err_at_on_stop(service: &str) -> Option<bool> {
        Self::get_err_inj("on_stop", service)
    }

    pub fn err_at_on_leader_change(service: &str) -> Option<bool> {
        Self::get_err_inj("on_leader_change", service)
    }

    pub fn sleep_at_on_start_sec(service: &str) -> Option<u64> {
        Self::get_err_inj("on_start_sleep_sec", service)
    }

    pub fn err_at_on_cfg_validate(service: &str) -> Option<String> {
        Self::get_err_inj("on_cfg_validate", service)
    }

    pub fn err_at_on_cfg_change(service: &str) -> Option<String> {
        Self::get_err_inj("on_cfg_change", service)
    }
}

fn save_persisted_data(data: &str) {
    static ONCE: sync::Once = sync::Once::new();
    ONCE.call_once(|| {
        let space = tarantool::space::Space::create(
            "persisted_data",
            &SpaceCreateOptions {
                space_type: SpaceType::DataLocal,
                if_not_exists: true,
                format: Some(vec![Field::string("data")]),
                ..Default::default()
            },
        )
        .unwrap();
        let pk_idx = IndexOptions {
            r#type: Some(IndexType::Tree),
            unique: Some(true),
            parts: Some(vec![Part::new("data", index::FieldType::String)]),
            if_not_exists: Some(true),
            ..Default::default()
        };
        space.create_index("pk", &pk_idx).unwrap();
    });

    let space = tarantool::space::Space::find("persisted_data").unwrap();
    space.replace(&(data,)).unwrap();
}

fn init_plugin_state_if_need(lua: &LuaThread, service: &str) {
    let exec_code = format!(
        r#"
            if _G['plugin_state'] == nil then
                _G['plugin_state'] = {{}}
            end
            if _G['plugin_state']['{service}'] == nil then
                _G['plugin_state']['{service}'] = {{}}
            end
            if _G['plugin_state']['data'] == nil then
                _G['plugin_state']['data'] = {{}}
            end
        "#
    );
    lua.exec(&exec_code).unwrap();
}

fn inc_callback_calls(service: &str, callback: &str) {
    let lua = tarantool::lua_state();
    init_plugin_state_if_need(&lua, service);
    let exec_code = format!(
        r#"
            if _G['plugin_state']['{service}']['{callback}'] == nil then
                _G['plugin_state']['{service}']['{callback}'] = 0
            end
            _G['plugin_state']['{service}']['{callback}'] = _G['plugin_state']['{service}']['{callback}'] + 1
        "#
    );
    lua.exec(&exec_code).unwrap();
}

fn update_plugin_cfg(service: &str, cfg: &Service1Cfg) {
    let lua = tarantool::lua_state();
    init_plugin_state_if_need(&lua, service);

    lua.eval_with::<_, ()>(
        format!("_G['plugin_state']['{service}']['current_config'] = ...").as_ref(),
        cfg,
    )
    .unwrap();
}

fn save_last_seen_context(service: &str, ctx: &PicoContext) {
    let lua = tarantool::lua_state();
    init_plugin_state_if_need(&lua, service);

    lua.eval_with::<_, ()>(
        format!("_G['plugin_state']['{service}']['last_seen_ctx'] = {{ is_master = ... }}")
            .as_ref(),
        ctx.is_master(),
    )
    .unwrap();
}

#[derive(Serialize, Deserialize, Debug, tlua::Push)]
struct Service1Cfg {
    foo: bool,
    bar: i32,
    baz: Vec<String>,
}

struct Service1 {
    cfg: Option<Service1Cfg>,
}

impl Service for Service1 {
    type CFG = Service1Cfg;

    fn on_cfg_validate(&self, _configuration: Self::CFG) -> CallbackResult<()> {
        if let Some(err_text) = ErrInjection::err_at_on_cfg_validate("testservice_1") {
            return Err(err_text.into());
        }

        inc_callback_calls("testservice_1", "on_cfg_validate");

        Ok(())
    }

    fn on_config_change(
        &mut self,
        ctx: &PicoContext,
        new_cfg: Self::CFG,
        _old_cfg: Self::CFG,
    ) -> CallbackResult<()> {
        if let Some(err_text) = ErrInjection::err_at_on_cfg_change("testservice_1") {
            return Err(err_text.into());
        }
        save_last_seen_context("testservice_1", ctx);
        inc_callback_calls("testservice_1", "on_config_change");
        update_plugin_cfg("testservice_1", &new_cfg);

        Ok(())
    }

    fn schema(&self) -> Vec<DDL> {
        vec![]
    }

    fn on_start(&mut self, ctx: &PicoContext, cfg: Self::CFG) -> CallbackResult<()> {
        if let Some(sleep_secs) = ErrInjection::sleep_at_on_start_sec("testservice_1") {
            fiber::sleep(Duration::from_secs(sleep_secs));
        }

        if ErrInjection::err_at_on_start("testservice_1").unwrap_or(false) {
            return Err("error at `on_start`".into());
        }

        save_last_seen_context("testservice_1", ctx);
        inc_callback_calls("testservice_1", "on_start");
        update_plugin_cfg("testservice_1", &cfg);

        self.cfg = Some(cfg);
        Ok(())
    }

    fn on_stop(&mut self, ctx: &PicoContext) -> CallbackResult<()> {
        if ErrInjection::err_at_on_stop("testservice_1").unwrap_or(false) {
            return Err("error at `on_stop`".into());
        }

        inc_callback_calls("testservice_1", "on_stop");
        save_last_seen_context("testservice_1", ctx);
        save_persisted_data("testservice_1_stopd");

        Ok(())
    }

    /// Called after replicaset master is changed
    fn on_leader_change(&mut self, ctx: &PicoContext) -> CallbackResult<()> {
        if ErrInjection::err_at_on_leader_change("testservice_1").unwrap_or(false) {
            return Err("error at `on_leader_change`".into());
        }

        save_last_seen_context("testservice_1", ctx);
        inc_callback_calls("testservice_1", "on_leader_change");

        Ok(())
    }
}

impl Service1 {
    pub fn new() -> Self {
        Service1 { cfg: None }
    }
}

struct Service2 {}

impl Service for Service2 {
    type CFG = ();

    fn on_start(&mut self, ctx: &PicoContext, _: Self::CFG) -> CallbackResult<()> {
        if ErrInjection::err_at_on_stop("testservice_2").unwrap_or(false) {
            return Err("error at `on_stop`".into());
        }

        inc_callback_calls("testservice_2", "on_start");
        save_last_seen_context("testservice_2", ctx);

        Ok(())
    }

    fn on_stop(&mut self, ctx: &PicoContext) -> CallbackResult<()> {
        if ErrInjection::err_at_on_stop("testservice_2").unwrap_or(false) {
            return Err("error at `on_stop`".into());
        }

        inc_callback_calls("testservice_2", "on_stop");
        save_last_seen_context("testservice_2", ctx);
        save_persisted_data("testservice_2_stopd");

        Ok(())
    }
}

impl Service2 {
    pub fn new() -> Self {
        Self {}
    }
}

struct Service3;

fn save_in_lua(service: &str, key: &str, value: impl Display) {
    let lua = tarantool::lua_state();
    init_plugin_state_if_need(&lua, service);

    let value = value.to_string();
    lua.exec_with(
        "local key, value = ...
        _G['plugin_state']['data'][key] = value",
        (key, value),
    )
    .unwrap();
}

#[derive(Deserialize)]
struct Service3Cfg {
    test_type: String,
}

impl Service for Service3 {
    type CFG = Service3Cfg;

    fn on_start(&mut self, _: &PicoContext, cfg: Self::CFG) -> CallbackResult<()> {
        match cfg.test_type.as_str() {
            "internal" => {
                let version = internal::picodata_version();
                save_in_lua("testservice_3", "version", version);
                let rpc_version = internal::rpc_version();
                save_in_lua("testservice_3", "rpc_version", rpc_version);

                // get some instance info
                let i_info = internal::instance_info().unwrap();
                save_in_lua("testservice_3", "instance_id", i_info.instance_id());
                save_in_lua("testservice_3", "instance_uuid", i_info.instance_uuid());
                save_in_lua("testservice_3", "replicaset_id", i_info.replicaset_id());
                save_in_lua("testservice_3", "replicaset_uuid", i_info.replicaset_uuid());
                save_in_lua("testservice_3", "cluster_id", i_info.cluster_id());
                save_in_lua("testservice_3", "tier", i_info.tier());

                // get some raft information
                let raft_info = internal::raft_info();
                save_in_lua("testservice_3", "raft_id", raft_info.id());
                save_in_lua("testservice_3", "raft_term", raft_info.term());
                save_in_lua("testservice_3", "raft_index", raft_info.applied());

                let timeout = Duration::from_secs(10);

                // do CAS
                let space = system::tarantool::space::Space::find("AUTHOR")
                    .unwrap()
                    .id();
                let (idx, term) = internal::cas::compare_and_swap(
                    Op::dml(Dml::insert(
                        space,
                        Tuple::new(&(101, "Alexander Pushkin")).unwrap(),
                        1,
                    )),
                    Predicate::new(raft_info.applied(), raft_info.term(), vec![]),
                    timeout,
                )
                .unwrap();
                internal::cas::wait_index(idx, Duration::from_secs(10)).unwrap();

                let mut ops = UpdateOps::new();
                ops.assign(1, "Alexander Blok").unwrap();
                let (idx, _term) = internal::cas::compare_and_swap(
                    Op::dml(Dml::update(space, &(101,), ops, 1).unwrap()),
                    Predicate::new(idx, term, vec![]),
                    timeout,
                )
                .unwrap();
                internal::cas::wait_index(idx, Duration::from_secs(10)).unwrap();
            }
            "sql" => {
                // DO SQL
                picoplugin::sql::query(
                    "INSERT INTO book (id, name, cost, last_buy) VALUES (?, ?, ?, ?)",
                    vec![
                        SqlValue::unsigned(1),
                        SqlValue::string("Ruslan and Ludmila"),
                        SqlValue::decimal(Decimal::from_str("1.1").unwrap()),
                        SqlValue::datetime(datetime!(2023-11-11 2:03:19.35421 -3).into()),
                    ],
                )
                .unwrap();
            }
            "log" => {
                static TARANTOOL_LOGGER: tarantool::log::TarantoolLogger =
                    tarantool::log::TarantoolLogger::new();
                rs_log::set_logger(&TARANTOOL_LOGGER)
                    .map(|()| rs_log::set_max_level(rs_log::LevelFilter::Info))
                    .unwrap();
                log::info!("TEST MESSAGE");
            }
            _ => {
                panic!("invalid test type")
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////
/// ServiceWithRpcTests
////////////////////////////////////////////////////////////////////////////////
struct ServiceWithRpcTests;

fn ping(input: rpc::Request, context: &mut Context) -> Result<rpc::Response, BoxError> {
    let raw_input = input.as_bytes();
    tarantool::say_info!(
        "got rpc request {} {}",
        context.path(),
        DisplayAsHexBytes(raw_input)
    );

    let info = internal::instance_info().unwrap();
    let instance_id = info.instance_id();

    let response = PingResponse {
        pong: "pong".into(),
        instance_id: instance_id.into(),
        raw_input: serde_bytes::ByteBuf::from(raw_input),
    };
    rpc::Response::encode_rmp_unnamed(&response)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PingResponse {
    pong: String,
    instance_id: String,
    raw_input: serde_bytes::ByteBuf,
}

impl Service for ServiceWithRpcTests {
    type CFG = ();

    fn on_start(&mut self, context: &PicoContext, _: ()) -> CallbackResult<()> {
        rpc::RouteBuilder::from(context)
            .path("/ping")
            .register(ping)
            .unwrap();

        // Note: Registerring the same endpoint twice must work, because on_start callbacks must be idempotent
        rpc::RouteBuilder::from(context)
            .path("/ping")
            .register(ping)
            .unwrap();

        rpc::RouteBuilder::from(context)
            .path("/echo-context")
            .register(|_, context| {
                let mut fields = context.get_named_fields()?.clone();
                fields.insert("request_id".into(), context.request_id().to_string().into());
                fields.insert("path".into(), context.path().into());
                fields.insert("plugin_name".into(), context.plugin_name().into());
                fields.insert("service_name".into(), context.service_name().into());
                fields.insert("plugin_version".into(), context.plugin_version().into());

                rpc::Response::encode_rmp(&fields)
            })
            .unwrap();

        rpc::RouteBuilder::from(context)
            .path("/register")
            .register(|input, _| {
                #[derive(serde::Deserialize, Debug)]
                struct Request {
                    path: Option<String>,
                    service_info: (String, String, String),
                }

                let request: Request = input.decode_rmp()?;

                let (plugin, service, version) = &request.service_info;
                let mut builder =
                    unsafe { rpc::RouteBuilder::from_service_info(plugin, service, version) };
                if let Some(path) = &request.path {
                    builder = builder.path(path);
                }

                let was_dropped = Rc::new(Cell::new(false));
                let drop_check = DropCheck(was_dropped.clone());

                let res = builder.register(move |input, context| {
                    // drop_check is owned by this closure and will be dropped with it
                    _ = &drop_check;
                    ping(input, context)
                });

                if res.is_err() {
                    assert!(was_dropped.get());
                }

                res?;

                return Ok(rpc::Response::from_static(b""));

                struct DropCheck(Rc<Cell<bool>>);
                impl Drop for DropCheck {
                    fn drop(&mut self) {
                        self.0.set(true)
                    }
                }
            })
            .unwrap();

        rpc::RouteBuilder::from(context)
            .path("/proxy")
            .register(|input, context| {
                tarantool::say_info!(
                    "got rpc request {} {} {context:#?}",
                    context.path(),
                    DisplayAsHexBytes(input.as_bytes())
                );

                #[derive(serde::Deserialize, Debug)]
                struct Request {
                    path: String,
                    service_info: Option<(String, String, String)>,
                    instance_id: Option<String>,
                    replicaset_id: Option<String>,
                    bucket_id: Option<u64>,
                    to_master: Option<bool>,
                    #[serde(with = "serde_bytes")]
                    input: Vec<u8>,
                }

                let request: Request = input.decode_rmp()?;

                let mut target = rpc::RequestTarget::Any;
                if let Some(instance_id) = &request.instance_id {
                    target = rpc::RequestTarget::InstanceId(instance_id);
                } else if let Some(replicaset_id) = &request.replicaset_id {
                    target = rpc::RequestTarget::ReplicasetId(
                        replicaset_id,
                        request.to_master.unwrap_or(false),
                    );
                } else if let Some(bucket_id) = request.bucket_id {
                    target =
                        rpc::RequestTarget::BucketId(bucket_id, request.to_master.unwrap_or(false));
                }

                let mut builder = rpc::RequestBuilder::new(target);
                if let Some((plugin, service, version)) = &request.service_info {
                    builder = builder
                        .plugin_service(plugin, service)
                        .plugin_version(version)
                } else {
                    builder = builder
                        .plugin_service(context.plugin_name(), context.service_name())
                        .plugin_version(context.plugin_version())
                }

                let mut timeout = Duration::from_secs(10);
                if let Some(t) = context.get("timeout")? {
                    timeout = Duration::from_secs_f64(t.float().unwrap());
                }

                let output = builder
                    .path(&request.path)
                    .input(rpc::Request::from_bytes(&request.input))
                    .timeout(timeout)
                    .send()?;

                tarantool::say_info!("{output:?}");

                Ok(output)
            })
            .unwrap();

        Ok(())
    }
}
#[service_registrar]
pub fn service_registrar(reg: &mut ServiceRegistry) {
    ErrInjection::init(&["testservice_1", "testservice_2"]);

    reg.add("testservice_1", "0.1.0", Service1::new);
    reg.add("testservice_2", "0.1.0", Service2::new);
    reg.add("testservice_3", "0.1.0", || Service3);

    reg.add("testservice_1", "0.2.0", Service1::new);
    reg.add("testservice_2", "0.2.0", Service2::new);
    reg.add("service_with_rpc_tests", "0.1.0", || ServiceWithRpcTests);
}
