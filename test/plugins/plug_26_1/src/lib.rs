use picodata_plugin::{
    plugin::{
        interface::{CallbackResult, PicoContext, Service, ServiceRegistry},
        prelude::service_registrar,
    },
    system::tarantool,
};
use serde::Deserialize;

/// Test the absence of `on_cluster_leader_change` callback
#[derive(Default)]
struct NoOnRaftLeaderChangeService {}

#[allow(unused)]
#[derive(Deserialize)]
struct NoOnRaftLeaderChangeServiceConfig {
    dummy_value_to_change: i64,
}

impl Service for NoOnRaftLeaderChangeService {
    type Config = NoOnRaftLeaderChangeServiceConfig;

    fn on_start(&mut self, _: &PicoContext, _: Self::Config) -> CallbackResult<()> {
        let lua = tarantool::lua_state();
        lua.exec("_G['on_start_no_on_cluster_leader_change'] = 'was set'")
            .unwrap();
        Ok(())
    }

    fn on_config_change(
        &mut self,
        _: &PicoContext,
        _: Self::Config,
        _old_config: Self::Config,
    ) -> CallbackResult<()> {
        let lua = tarantool::lua_state();
        lua.exec("_G['on_config_change_no_on_cluster_leader_change'] = 'was set'")
            .unwrap();
        Ok(())
    }

    fn on_leader_change(&mut self, _: &PicoContext) -> CallbackResult<()> {
        let lua = tarantool::lua_state();
        lua.exec("_G['on_leader_change_no_on_cluster_leader_change'] = 'was set'")
            .unwrap();
        Ok(())
    }

    fn on_health_check(&self, _: &PicoContext) -> CallbackResult<()> {
        let lua = tarantool::lua_state();
        lua.exec("_G['on_health_check_no_on_cluster_leader_change'] = 'was set'")
            .unwrap();
        Ok(())
    }

    fn on_stop(&mut self, _: &PicoContext) -> CallbackResult<()> {
        let lua = tarantool::lua_state();
        lua.exec("_G['on_stop_no_on_cluster_leader_change'] = 'was set'")
            .unwrap();
        Ok(())
    }
}

#[service_registrar]
pub fn service_registrar(reg: &mut ServiceRegistry) {
    reg.add(
        "no_on_cluster_leader_change_service",
        "0.1.0",
        NoOnRaftLeaderChangeService::default,
    );
}
