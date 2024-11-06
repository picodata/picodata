use picodata_plugin::background::CancellationToken;
use picodata_plugin::internal::types::{Dml, Op, Predicate};
use picodata_plugin::log::rs_log;
use picodata_plugin::plugin::interface::CallbackResult;
use picodata_plugin::plugin::prelude::*;
use picodata_plugin::system::tarantool;
use picodata_plugin::system::tarantool::datetime::Datetime;
use picodata_plugin::system::tarantool::decimal::Decimal;
use picodata_plugin::system::tarantool::error::BoxError;
use picodata_plugin::system::tarantool::index::{IndexOptions, IndexType, Part};
use picodata_plugin::system::tarantool::space::{Field, SpaceCreateOptions, SpaceType, UpdateOps};
use picodata_plugin::system::tarantool::tlua::{
    LuaFunction, LuaRead, LuaState, LuaThread, PushGuard, PushInto,
};
use picodata_plugin::system::tarantool::tuple::Tuple;
use picodata_plugin::system::tarantool::util::DisplayAsHexBytes;
use picodata_plugin::system::tarantool::{fiber, index, tlua};
use picodata_plugin::transport::context::Context;
use picodata_plugin::transport::rpc;
use picodata_plugin::{internal, log, system};
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

    pub fn err_at_on_config_validate(service: &str) -> Option<String> {
        Self::get_err_inj("on_config_validate", service)
    }

    pub fn err_at_on_config_change(service: &str) -> Option<String> {
        Self::get_err_inj("on_config_change", service)
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
    tarantool::say_info!("INSERT INTO persisted_data VALUES ('{data}')");
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

fn update_plugin_config(service: &str, config: impl PushInto<LuaState>) {
    let lua = tarantool::lua_state();
    init_plugin_state_if_need(&lua, service);

    _ = lua.eval_with::<_, ()>(
        format!("_G['plugin_state']['{service}']['current_config'] = ...").as_ref(),
        config,
    );
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

#[derive(Serialize, Deserialize, Debug, tlua::Push, PartialEq)]
struct Service1Config {
    foo: bool,
    bar: i32,
    baz: Vec<String>,
}

struct Service1 {
    config: Option<Service1Config>,
}

impl Service for Service1 {
    type Config = Service1Config;

    fn on_config_change(
        &mut self,
        ctx: &PicoContext,
        new_config: Self::Config,
        old_config: Self::Config,
    ) -> CallbackResult<()> {
        if !ErrInjection::get_err_inj::<bool>("assert_config_changed", "testservice_1")
            .unwrap_or_default()
        {
            assert_ne!(new_config, old_config);
        }
        if let Some(err_text) = ErrInjection::err_at_on_config_change("testservice_1") {
            return Err(err_text.into());
        }
        save_last_seen_context("testservice_1", ctx);
        inc_callback_calls("testservice_1", "on_config_change");
        update_plugin_config("testservice_1", &new_config);

        Ok(())
    }

    fn on_start(&mut self, ctx: &PicoContext, config: Self::Config) -> CallbackResult<()> {
        if let Some(sleep_secs) = ErrInjection::sleep_at_on_start_sec("testservice_1") {
            fiber::sleep(Duration::from_secs(sleep_secs));
        }

        if ErrInjection::err_at_on_start("testservice_1").unwrap_or(false) {
            return Err("error at `on_start`".into());
        }

        save_last_seen_context("testservice_1", ctx);
        inc_callback_calls("testservice_1", "on_start");
        update_plugin_config("testservice_1", &config);

        self.config = Some(config);
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
        Service1 { config: None }
    }
}

#[derive(Serialize, Deserialize, Debug, tlua::Push, PartialEq)]
struct Service2Config {
    foo: i32,
}

struct Service2 {}

impl Service for Service2 {
    type Config = Service2Config;

    fn on_start(&mut self, ctx: &PicoContext, _: Self::Config) -> CallbackResult<()> {
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

    fn on_config_change(
        &mut self,
        _: &PicoContext,
        new_cfg: Self::Config,
        _: Self::Config,
    ) -> CallbackResult<()> {
        inc_callback_calls("testservice_2", "on_config_change");
        update_plugin_config("testservice_2", &new_cfg);
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
struct Service3Config {
    test_type: String,
}

impl Service for Service3 {
    type Config = Service3Config;

    fn on_start(&mut self, ctx: &PicoContext, config: Self::Config) -> CallbackResult<()> {
        match config.test_type.as_str() {
            "internal" => {
                let version = internal::picodata_version();
                save_in_lua("testservice_3", "version", version);
                let rpc_version = internal::rpc_version();
                save_in_lua("testservice_3", "rpc_version", rpc_version);

                // get some instance info
                let i_info = internal::instance_info().unwrap();
                save_in_lua("testservice_3", "name", i_info.name());
                save_in_lua("testservice_3", "uuid", i_info.uuid());
                save_in_lua("testservice_3", "replicaset_name", i_info.replicaset_name());
                save_in_lua("testservice_3", "replicaset_uuid", i_info.replicaset_uuid());
                save_in_lua("testservice_3", "cluster_name", i_info.cluster_name());
                save_in_lua("testservice_3", "tier", i_info.tier());

                // get some raft information
                let raft_info = internal::raft_info();
                save_in_lua("testservice_3", "raft_id", raft_info.id());
                save_in_lua("testservice_3", "raft_term", raft_info.term());
                save_in_lua("testservice_3", "raft_index", raft_info.applied());

                let timeout = Duration::from_secs(10);

                // do CAS
                let space = system::tarantool::space::Space::find("author")
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
                let insert_count = picodata_plugin::sql::query(
                    "INSERT INTO book (id, name, cost, last_buy) VALUES (?, ?, ?, ?)",
                )
                .bind(1)
                .bind("Ruslan and Ludmila")
                .bind(Decimal::from_str("1.1").unwrap())
                .bind(Datetime::from(datetime!(2023-11-11 2:03:19.35421 -3)))
                .execute()
                .unwrap();
                assert_eq!(insert_count, 1);

                #[derive(Deserialize, Debug, PartialEq)]
                struct Book {
                    id: u64,
                    name: String,
                    cost: Decimal,
                    last_buy: Datetime,
                }
                let books = picodata_plugin::sql::query("SELECT * from book")
                    .fetch::<Book>()
                    .unwrap();

                assert_eq!(
                    books,
                    vec![Book {
                        id: 1,
                        name: "Ruslan and Ludmila".to_string(),
                        cost: Decimal::from_str("1.1").unwrap(),
                        last_buy: Datetime::from(datetime!(2023-11-11 2:03:19.35421 -3)),
                    }]
                );
            }
            "log" => {
                static TARANTOOL_LOGGER: tarantool::log::TarantoolLogger =
                    tarantool::log::TarantoolLogger::new();
                rs_log::set_logger(&TARANTOOL_LOGGER)
                    .map(|()| rs_log::set_max_level(rs_log::LevelFilter::Info))
                    .unwrap();
                log::info!("TEST MESSAGE");
            }
            "background" => {
                fn my_job(ct: CancellationToken) {
                    while ct.wait_timeout(Duration::from_millis(100)).is_err() {
                        save_persisted_data("background_job_running");
                    }
                    save_persisted_data("background_job_stopped");
                }

                let wm = ctx.worker_manager();
                wm.register_job(my_job).unwrap();
            }
            "no_test" => {}
            "metrics" => {
                let drop_check = DropCheck;
                let metrics_closure = move || {
                    let _ = &drop_check;
                    "test_metric_1 1\ntest_metric_2 2".into()
                };

                ctx.register_metrics_callback(metrics_closure.clone())
                    .unwrap();
                // This call is idempotent
                ctx.register_metrics_callback(metrics_closure).unwrap();

                // Unless you try registerring a different callback
                let e = ctx
                    .register_metrics_callback(|| "other_metrics 69".into())
                    .unwrap_err();
                assert_eq!(e.to_string(), "FunctionExists: metrics handler for `testplug_sdk.testservice_3:v0.1.0` is already registered with a different handler");

                #[derive(Clone)]
                struct DropCheck;
                impl Drop for DropCheck {
                    fn drop(&mut self) {
                        save_persisted_data("drop was called for metrics closure");
                    }
                }
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

    let info = internal::instance_info()?;
    let instance_name = info.name();

    let response = PingResponse {
        pong: "pong".into(),
        instance_name: instance_name.into(),
        raw_input: serde_bytes::ByteBuf::from(raw_input),
    };
    rpc::Response::encode_rmp_unnamed(&response)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PingResponse {
    pong: String,
    instance_name: String,
    raw_input: serde_bytes::ByteBuf,
}

impl Service for ServiceWithRpcTests {
    type Config = ();

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
                    instance_name: Option<String>,
                    replicaset_name: Option<String>,
                    bucket_id: Option<u64>,
                    tier_and_bucket_id: Option<(String, u64)>,
                    to_master: Option<bool>,
                    #[serde(with = "serde_bytes")]
                    input: Vec<u8>,
                }

                let request: Request = input.decode_rmp()?;

                let mut target = rpc::RequestTarget::Any;
                if let Some(instance_name) = &request.instance_name {
                    target = rpc::RequestTarget::InstanceName(instance_name);
                } else if let Some(replicaset_name) = &request.replicaset_name {
                    target = rpc::RequestTarget::ReplicasetName(
                        replicaset_name,
                        request.to_master.unwrap_or(false),
                    );
                } else if let Some(bucket_id) = &request.bucket_id {
                    target = rpc::RequestTarget::BucketId(
                        *bucket_id,
                        request.to_master.unwrap_or(false),
                    );
                } else if let Some((tier, bucket_id)) = &request.tier_and_bucket_id {
                    target = rpc::RequestTarget::TierAndBucketId(
                        tier,
                        *bucket_id,
                        request.to_master.unwrap_or(false),
                    );
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

#[derive(Debug, Serialize, Deserialize)]
struct StringyConfig {
    stringy_string: String,
}

struct ServiceWithStringConfigValue;

impl Service for ServiceWithStringConfigValue {
    type Config = StringyConfig;
}

// Ensures that macros usage at least compiles.
#[tarantool::proc]
fn example_stored_proc() {}

#[tarantool::test]
fn example_tarantool_test() {}

#[service_registrar]
pub fn service_registrar(reg: &mut ServiceRegistry) {
    ErrInjection::init(&["testservice_1", "testservice_2"]);

    reg.add("testservice_1", "0.1.0", Service1::new);
    reg.add_config_validator::<Service1>("testservice_1", "0.1.0", |_| {
        if let Some(err_text) = ErrInjection::err_at_on_config_validate("testservice_1") {
            return Err(err_text.into());
        }
        inc_callback_calls("testservice_1", "on_config_validate");
        Ok(())
    });

    reg.add("testservice_2", "0.1.0", Service2::new);
    reg.add("testservice_3", "0.1.0", || Service3);

    reg.add("testservice_1", "0.2.0", Service1::new);
    reg.add("testservice_2", "0.2.0", Service2::new);
    reg.add("service_with_rpc_tests", "0.1.0", || ServiceWithRpcTests);

    // 0.2.0 broken version cause inconsistent migration
    reg.add("testservice_2", "0.2.0_broken", Service2::new);

    reg.add("testservice_w_string_conf", "0.1.0", || {
        ServiceWithStringConfigValue
    });
    reg.add("testservice_w_string_conf2", "0.1.0", || {
        ServiceWithStringConfigValue
    });
}
