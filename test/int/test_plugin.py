import pytest
import time
import os
import sys
import shutil
from conftest import Cluster, ReturnError, retrying, Instance

_3_SEC = 3
_DEFAULT_CFG = {"foo": True, "bar": 101, "baz": ["one", "two", "three"]}
_NEW_CFG = {"foo": True, "bar": 102, "baz": ["a", "b"]}
_NEW_CFG_2 = {"foo": False, "bar": 102, "baz": ["a", "b"]}

_PLUGIN = "testplug"
_PLUGIN_SERVICES = ["testservice_1", "testservice_2"]
_PLUGIN_SMALL = "testplug_small"
_PLUGIN_SMALL_SERVICES = ["testservice_1"]
_PLUGIN_SMALL_SERVICES_SVC2 = ["testservice_2"]
_DEFAULT_TIER = "default"
_DEFAULT_TIERS = [_DEFAULT_TIER]


# ---------------------------------- Test helper classes {-----------------------------------------


class PluginReflection:
    """PluginReflection used to describe the expected state of the plugin"""

    # plugin name
    name: str
    # list of plugin services
    services: list[str]
    # instances in cluster
    instances: list[Instance]
    # plugin topology
    topology: dict[Instance, list[str]] = {}
    # if True - assert_synced checks that plugin are installed
    installed: bool = False
    # if True - assert_synced checks that plugin are enabled
    enabled: bool = False

    def __init__(self, name: str, services: list[str], *instances):
        """Create reflection with empty topology"""
        self.name = name
        self.services = services
        self.instances = list(instances)
        for i in instances:
            self.topology[i] = []

    @staticmethod
    def default(*instances):
        """Create reflection for default plugin with default topology"""
        topology = {}
        for i in instances:
            topology[i] = _PLUGIN_SERVICES
        return PluginReflection(_PLUGIN, _PLUGIN_SERVICES, *instances).set_topology(
            topology
        )

    def install(self, installed: bool):
        self.installed = installed
        return self

    def enable(self, enabled: bool):
        self.enabled = enabled
        return self

    def set_topology(self, topology: dict[Instance, list[str]]):
        self.topology = topology
        return self

    def add_instance(self, i):
        self.instances.append(i)
        return self

    def assert_synced(self):
        """Assert that plugin reflection and plugin state in cluster are synchronized.
        This means that system tables `_pico_plugin`, `_pico_service` and `_pico_service_route`
        contain necessary plugin information."""
        for i in self.instances:
            plugins = i.eval("return box.space._pico_plugin:select(...)", self.name)
            if self.installed:
                assert len(plugins) == 1
                assert plugins[0][1] == self.enabled
            else:
                assert len(plugins) == 0

            for service in self.services:
                svcs = i.eval(
                    "return box.space._pico_service:select({...})", self.name, service
                )
                if self.installed:
                    assert len(svcs) == 1
                else:
                    assert len(svcs) == 0

        for i in self.topology:
            expected_services = []
            for service in self.topology[i]:
                expected_services.append([i.instance_id, self.name, service, False])

            for neighboring_i in self.topology:
                routes = neighboring_i.eval(
                    'return box.space._pico_service_route:pairs({...}, {iterator="EQ"}):totable()',
                    i.instance_id,
                    self.name,
                )
                assert routes == expected_services

    @staticmethod
    def assert_cb_called(service, callback, called_times, *instances):
        for i in instances:
            cb_calls_number = i.eval(
                f"if _G['plugin_state'] == nil then _G['plugin_state'] = {{}} end "
                f"if _G['plugin_state']['{service}'] == nil then _G['plugin_state']['{service}']"
                f" = {{}} end "
                f"if _G['plugin_state']['{service}']['{callback}'] == nil then _G['plugin_state']"
                f"['{service}']['{callback}'] = 0 end "
                f"return _G['plugin_state']['{service}']['{callback}']"
            )
            assert cb_calls_number == called_times

    @staticmethod
    def assert_persisted_data_exists(data, *instances):
        for i in instances:
            data_exists = i.eval(
                f"return box.space.persisted_data:get({{'{data}'}}) ~= box.NULL"
            )
            assert data_exists

    @staticmethod
    def clear_persisted_data(data, *instances):
        for i in instances:
            i.eval("return box.space.persisted_data:drop()")

    @staticmethod
    def inject_error(service, error, value, instance):
        instance.eval("if _G['err_inj'] == nil then _G['err_inj'] = {} end")
        instance.eval(
            f"if _G['err_inj']['{service}'] == nil then _G['err_inj']['{service}'] "
            "= {{}} end"
        )
        instance.eval(f"_G['err_inj']['{service}']['{error}'] = ...", (value,))

    @staticmethod
    def remove_error(service, error, instance):
        instance.eval("if _G['err_inj'] == nil then _G['err_inj'] = {} end")
        instance.eval(
            f"if _G['err_inj']['{service}'] == nil then _G['err_inj']['{service}'] "
            "= {{}} end"
        )
        instance.eval(f"_G['err_inj']['{service}']['{error}'] = nil")

    @staticmethod
    def assert_last_seen_ctx(service, expected_ctx, *instances):
        for i in instances:
            ctx = i.eval(f"return _G['plugin_state']['{service}']['last_seen_ctx']")
            assert ctx == expected_ctx

    def get_config(self, service, instance):
        return instance.eval(
            f"return box.space._pico_service:get({{'{self.name}', '{service}', "
            f"'0.1.0'}})[5]"
        )

    @staticmethod
    def get_seen_config(service, instance):
        return instance.eval(
            f"return _G['plugin_state']['{service}']['current_config']"
        )

    def assert_config(self, service, expected_cfg, *instances):
        for i in instances:
            cfg_space = self.get_config(service, i)
            assert cfg_space == expected_cfg
            cfg_seen = self.get_seen_config(service, i)
            assert cfg_seen == expected_cfg

    def assert_route_poisoned(self, poison_instance_id, service, poisoned=True):
        for i in self.instances:
            route = i.eval(
                "return box.space._pico_service_route:get({...})",
                poison_instance_id,
                self.name,
                service,
            )
            assert route is not None
            assert route[3] == poisoned


# ---------------------------------- } Test helper classes ----------------------------------------


def test_invalid_manifest_plugin(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    # try to create non-existent plugin
    with pytest.raises(
        ReturnError, match="Error while discovering manifest for plugin"
    ):
        i1.call("pico.install_plugin", "non-existent")
    PluginReflection("non-existent", [], i1, i2).assert_synced()

    # try to use invalid manifest (with undefined plugin name)
    with pytest.raises(ReturnError, match="missing field `name`"):
        i1.call("pico.install_plugin", "testplug_broken_manifest_1")
    PluginReflection(
        "testplug_broken_manifest_1", _PLUGIN_SERVICES, i1, i2
    ).assert_synced()

    # try to use invalid manifest (with invalid default configuration)
    with pytest.raises(ReturnError, match="Error while enable the plugin"):
        i1.call("pico.install_plugin", "testplug_broken_manifest_2")
        i1.call(
            "pico.update_plugin_tiers",
            "testplug_broken_manifest_2",
            "testservice_1",
            _DEFAULT_TIERS,
        )
        i1.call(
            "pico.update_plugin_tiers",
            "testplug_broken_manifest_2",
            "testservice_2",
            _DEFAULT_TIERS,
        )
        i1.call("pico.enable_plugin", "testplug_broken_manifest_2")
    PluginReflection("testplug_broken_manifest_2", _PLUGIN_SERVICES, i1, i2).install(
        True
    ).assert_synced()

    # # try to use invalid manifest (with non-existed extra service)
    with pytest.raises(ReturnError, match="Error while install the plugin"):
        i1.call("pico.install_plugin", "testplug_broken_manifest_3")
    PluginReflection(
        "testplug_broken_manifest_3",
        ["testservice_1", "testservice_2", "testservice_3"],
        i1,
        i2,
    ).assert_synced()
    PluginReflection.assert_cb_called("testservice_1", "on_start", 0, i1, i2)


def install_and_enable_plugin(instance, plugin, services, timeout=_3_SEC):
    instance.call("pico.install_plugin", plugin, timeout=timeout)
    for s in services:
        instance.call("pico.update_plugin_tiers", plugin, s, [_DEFAULT_TIER])
    instance.call("pico.enable_plugin", plugin, timeout=timeout)


def test_plugin_install(cluster: Cluster):
    """
    plugin installation must be full idempotence:
    install non-installed plugin - default behavior
    install already disabled plugin - do nothing
    install already enabled plugin - do nothing
    """

    i1, i2 = cluster.deploy(instance_count=2)
    expected_state = PluginReflection(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    # check default behaviour
    i1.call("pico.install_plugin", _PLUGIN)
    expected_state = expected_state.install(True)
    expected_state.assert_synced()

    # check install already disabled plugin
    i1.call("pico.install_plugin", _PLUGIN)
    expected_state.assert_synced()

    # enable plugin and check installation of already enabled plugin
    i1.call("pico.update_plugin_tiers", _PLUGIN, "testservice_1", [_DEFAULT_TIER])
    i1.call("pico.update_plugin_tiers", _PLUGIN, "testservice_2", [_DEFAULT_TIER])
    i1.call("pico.enable_plugin", _PLUGIN)
    expected_state = expected_state.set_topology(
        {i1: _PLUGIN_SERVICES, i2: _PLUGIN_SERVICES}
    ).enable(True)
    expected_state.assert_synced()

    i1.call("pico.install_plugin", _PLUGIN)
    expected_state.assert_synced()


def test_plugin_enable(cluster: Cluster):
    """
    plugin enabling behaviour:
    enabling of installed and disabled plugin - default behavior
    enabling of already enabled plugin - do nothing
    enabling of non-installed plugin - error occurred
    """

    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    # check default behaviour
    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()
    # assert that on_start callbacks successfully called
    plugin_ref.assert_cb_called("testservice_1", "on_start", 1, i1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_start", 1, i1, i2)

    # check enable already enabled plugin
    i1.call("pico.enable_plugin", _PLUGIN)
    plugin_ref.assert_synced()
    # assert that `on_start` don't call twice
    plugin_ref.assert_cb_called("testservice_1", "on_start", 1, i1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_start", 1, i1, i2)

    # check that enabling of non-installed plugin return error
    with pytest.raises(ReturnError, match="Error while enable the plugin"):
        i1.call("pico.enable_plugin", _PLUGIN_SMALL)


def test_plugin_disable(cluster: Cluster):
    """
    plugin disabling behaviour:
    disabling of enabled plugin - default behavior
    disabling of disabled plugin - do nothing
    disabling of non-installed plugin - error occurred
    """

    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    # check default behaviour
    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    i1.call("pico.disable_plugin", _PLUGIN)

    plugin_ref = plugin_ref.enable(False).set_topology({i1: [], i2: []})
    plugin_ref.assert_synced()

    # retrying, cause routing table update asynchronously
    retrying(lambda: plugin_ref.assert_synced())
    # assert that `on_stop` callbacks successfully called
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 1, i1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_stop", 1, i1, i2)

    # check disabling of already disabled plugin
    i1.call("pico.disable_plugin", _PLUGIN)
    plugin_ref.assert_synced()
    # assert that `on_stop` callbacks don't call twice
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 1, i1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_stop", 1, i1, i2)

    # check that disabling of non-installed plugin return error
    with pytest.raises(
        ReturnError, match="Plugin `testplug_small` not found at instance"
    ):
        i1.call("pico.disable_plugin", _PLUGIN_SMALL)


def test_plugin_remove(cluster: Cluster):
    """
    plugin removing behaviour:
    removing of disabling plugin - default behavior
    removing of non-installed plugin - do nothing
    removing of already enabled plugin - error occurred
    """

    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    # check that removing non-disabled plugin return error
    with pytest.raises(ReturnError, match="Remove of enabled plugin is forbidden"):
        i1.call("pico.remove_plugin", _PLUGIN)
    plugin_ref.assert_synced()

    i1.call("pico._inject_error", "PLUGIN_EXIST_AND_ENABLED", True)
    # same, but error not returned to a client
    i1.call("pico.remove_plugin", _PLUGIN)
    plugin_ref.assert_synced()
    i1.call("pico._inject_error", "PLUGIN_EXIST_AND_ENABLED", False)

    # check default behaviour
    i1.call("pico.disable_plugin", _PLUGIN)
    plugin_ref = plugin_ref.enable(False).set_topology({i1: [], i2: []})

    # retrying, cause routing table update asynchronously
    retrying(lambda: plugin_ref.assert_synced())

    i1.call("pico.remove_plugin", _PLUGIN)
    plugin_ref = plugin_ref.install(False)
    plugin_ref.assert_synced()

    # check removing non-installed plugin
    i1.call("pico.remove_plugin", _PLUGIN)
    plugin_ref.assert_synced()


def test_two_plugin_install_and_enable(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    p1_ref = PluginReflection.default(i1, i2)
    topology = {i1: _PLUGIN_SMALL_SERVICES, i2: _PLUGIN_SMALL_SERVICES}
    p2_ref = PluginReflection(
        _PLUGIN_SMALL, _PLUGIN_SMALL_SERVICES, i1, i2
    ).set_topology(topology)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    p1_ref = p1_ref.install(True).enable(True)
    install_and_enable_plugin(i1, _PLUGIN_SMALL, _PLUGIN_SMALL_SERVICES)
    p2_ref = p2_ref.install(True).enable(True)

    # assert that system tables are filled
    p1_ref.assert_synced()
    p2_ref.assert_synced()

    # assert that on_start callbacks successfully called
    PluginReflection.assert_cb_called("testservice_1", "on_start", 2, i1, i2)
    PluginReflection.assert_cb_called("testservice_2", "on_start", 1, i1, i2)


def test_plugin_install_and_enable_at_new_instance(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    i3 = cluster.add_instance(wait_online=True)
    plugin_ref = plugin_ref.add_instance(i3).set_topology(
        {i1: _PLUGIN_SERVICES, i2: _PLUGIN_SERVICES, i3: _PLUGIN_SERVICES}
    )
    plugin_ref.assert_synced()

    plugin_ref.assert_cb_called("testservice_1", "on_start", 1, i1, i2, i3)
    plugin_ref.assert_cb_called("testservice_2", "on_start", 1, i1, i2, i3)


def test_instance_with_plugin_shutdown(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    i2.restart()
    i2.wait_online()

    PluginReflection.assert_persisted_data_exists("testservice_1_stopd", i2)
    PluginReflection.assert_persisted_data_exists("testservice_2_stopd", i2)
    PluginReflection.clear_persisted_data(i2)


def test_instance_with_plugin_expel(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    cluster.expel(i2)
    # assert that expel doesn't affect plugins
    plugin_ref = PluginReflection.default(i2).install(True).enable(True)
    plugin_ref.assert_synced()


def test_plugin_disable_error_on_stop(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    plugin_ref.inject_error("testservice_1", "on_stop", True, i2)
    plugin_ref.inject_error("testservice_2", "on_stop", True, i2)

    i1.call("pico.disable_plugin", _PLUGIN, timeout=_3_SEC)
    # retrying, cause routing table update asynchronously
    plugin_ref = plugin_ref.enable(False).set_topology({i1: [], i2: []})
    retrying(lambda: plugin_ref.assert_synced())

    i1.call("pico.remove_plugin", _PLUGIN, timeout=_3_SEC)
    plugin_ref = plugin_ref.install(False)
    plugin_ref.assert_synced()

    plugin_ref.assert_cb_called("testservice_1", "on_start", 1, i1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_start", 1, i1, i2)
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 1, i1)
    plugin_ref.assert_cb_called("testservice_2", "on_stop", 1, i1)
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 0, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_stop", 0, i2)


def test_plugin_not_enable_if_error_on_start(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    # inject error into second instance
    plugin_ref.inject_error("testservice_1", "on_start", True, i2)

    # assert that plugin not loaded and on_stop called on both instances
    with pytest.raises(ReturnError, match="Error while enable the plugin"):
        install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)

    # plugin installed but disabled
    plugin_ref = plugin_ref.install(True).set_topology({i1: [], i2: []})
    retrying(lambda: plugin_ref.assert_synced())
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 1, i1, i2)

    # inject error into both instances
    plugin_ref.inject_error("testservice_1", "on_start", True, i1)

    # assert that plugin not loaded and on_stop called on both instances
    with pytest.raises(ReturnError, match="Error while enable the plugin"):
        install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)

    # plugin installed but disabled
    retrying(lambda: plugin_ref.assert_synced())
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 2, i1, i2)

    # remove errors
    plugin_ref.inject_error("testservice_1", "on_start", False, i1)
    plugin_ref.inject_error("testservice_1", "on_start", False, i2)

    # assert plugin loaded now
    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.enable(True).set_topology(
        {i1: _PLUGIN_SERVICES, i2: _PLUGIN_SERVICES}
    )
    plugin_ref.assert_synced()


def test_plugin_not_enable_if_on_start_timeout(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    # inject timeout into second instance
    plugin_ref.inject_error("testservice_1", "on_start_sleep_sec", 3, i2)

    with pytest.raises(ReturnError, match="Error while enable the plugin"):
        i1.call("pico.install_plugin", _PLUGIN)
        i1.call("pico.update_plugin_tiers", _PLUGIN, "testservice_1", [_DEFAULT_TIER])
        i1.call("pico.update_plugin_tiers", _PLUGIN, "testservice_2", [_DEFAULT_TIER])
        i1.call("pico.enable_plugin", _PLUGIN, {"on_start_timeout": 2}, timeout=4)
    # need to wait until sleep at i2 called asynchronously
    time.sleep(2)

    # assert that plugin installed, disabled and on_stop called on both instances
    plugin_ref = plugin_ref.install(True).set_topology({i1: [], i2: []})
    retrying(lambda: plugin_ref.assert_synced())
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 1, i1, i2)

    # inject timeout into both instances
    plugin_ref.inject_error("testservice_1", "on_start_sleep_sec", 3, i1)

    with pytest.raises(ReturnError, match="Error while enable the plugin"):
        i1.call("pico.enable_plugin", _PLUGIN, {"on_start_timeout": 2}, timeout=4)
    # need to wait until sleep at i1 and i2 called asynchronously
    time.sleep(2)

    # assert that plugin installed, disabled and on_stop called on both instances
    retrying(lambda: plugin_ref.assert_synced())
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 2, i1, i2)


def test_config_validation(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    plugin_ref.inject_error("testservice_1", "on_cfg_validate", "test error", i1)
    with pytest.raises(
        ReturnError, match="New configuration validation error:.* test error"
    ):
        i1.eval(
            "return pico.update_plugin_config('testplug', "
            "'testservice_1', {foo = true, bar = 102, baz = {'a', 'b'}})"
        )


def test_on_config_update(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    plugin_ref.assert_config("testservice_1", _DEFAULT_CFG, i1, i2)

    i1.eval(
        "pico.update_plugin_config('testplug', 'testservice_1', {foo = "
        "true, bar = 102, baz = {'a', 'b'}})"
    )
    # retrying, cause new service configuration callback call asynchronously
    retrying(lambda: plugin_ref.assert_config("testservice_1", _NEW_CFG, i1, i2))


def test_plugin_double_config_update(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    i1.eval(
        'pico.update_plugin_config("testplug", "testservice_1", {foo = '
        'true, bar = 102, baz = {"a", "b"}})'
        'return pico.update_plugin_config("testplug", "testservice_1",'
        '{foo = false, bar = 102, baz = {"a", "b"}})'
    )
    # both configs were applied
    # retrying, cause callback call asynchronously
    retrying(
        lambda: plugin_ref.assert_cb_called(
            "testservice_1", "on_config_change", 2, i1, i2
        )
    )
    plugin_ref.assert_config("testservice_1", _NEW_CFG_2, i1, i2)

    i1.eval(
        'pico.update_plugin_config("testplug", "testservice_1", {foo = '
        'true, bar = 102, baz = {"a", "b"}})'
    )
    i2.eval(
        'pico.update_plugin_config("testplug", "testservice_1", {foo = '
        'true, bar = 102, baz = {"a", "b"}})'
    )
    # both configs were applied and result config may be any of applied
    # retrying, cause callback call asynchronously
    retrying(
        lambda: plugin_ref.assert_cb_called(
            "testservice_1", "on_config_change", 4, i1, i2
        )
    )


def test_error_on_config_update(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    plugin_ref.assert_config("testservice_1", _DEFAULT_CFG, i1, i2)

    plugin_ref.inject_error("testservice_1", "on_cfg_change", "test error", i1)

    i1.eval(
        "pico.update_plugin_config('testplug', 'testservice_1', {foo = "
        "true, bar = 102, baz = {'a', 'b'}})"
    )

    # check that at i1 new configuration exists in global space
    # but not really applied to service because error
    cfg_space = plugin_ref.get_config("testservice_1", i1)
    assert cfg_space == _NEW_CFG
    cfg_seen = plugin_ref.get_seen_config("testservice_1", i1)
    assert cfg_seen == _DEFAULT_CFG
    retrying(lambda: plugin_ref.assert_config("testservice_1", _NEW_CFG, i2))

    # assert that the first instance now has a poison service
    # and the second instance is not poisoned
    # retrying, cause routing table update asynchronously
    retrying(lambda: plugin_ref.assert_route_poisoned(i1.instance_id, "testservice_1"))
    retrying(
        lambda: plugin_ref.assert_route_poisoned(
            i2.instance_id, "testservice_1", poisoned=False
        )
    )


def test_instance_service_poison_and_healthy_then(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    plugin_ref.assert_config("testservice_1", _DEFAULT_CFG, i1, i2)
    plugin_ref.inject_error("testservice_1", "on_cfg_change", "test error", i1)

    i1.eval(
        "pico.update_plugin_config('testplug', 'testservice_1', {foo = "
        "true, bar = 102, baz = {'a', 'b'}})"
    )

    # assert that the first instance now has a poison service
    # retrying, cause routing table update asynchronously
    retrying(lambda: plugin_ref.assert_route_poisoned(i1.instance_id, "testservice_1"))

    plugin_ref.remove_error("testservice_1", "on_cfg_change", i1)

    i1.eval(
        "pico.update_plugin_config('testplug', 'testservice_1', {foo = "
        "false, bar = 102, baz = {'a', 'b'}})"
    )

    # retrying, cause routing table update asynchronously
    retrying(
        lambda: plugin_ref.assert_route_poisoned(
            i1.instance_id, "testservice_1", poisoned=False
        )
    )
    plugin_ref.assert_config("testservice_1", _NEW_CFG_2, i1, i2)


def test_on_leader_change(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_id="r1", wait_online=True)
    i2 = cluster.add_instance(replicaset_id="r1", wait_online=True)
    i3 = cluster.add_instance(replicaset_id="r1", wait_online=True)

    plugin_ref = PluginReflection.default(i1, i2, i3)

    masters = [i for i in cluster.instances if not i.eval("return box.info.ro")]
    assert masters[0] == i1

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    plugin_ref.assert_last_seen_ctx("testservice_1", {"is_master": True}, i1)
    plugin_ref.assert_last_seen_ctx("testservice_1", {"is_master": False}, i2, i3)

    index = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r1"],
        ops=[("=", "target_master_id", i2.instance_id)],
    )
    cluster.raft_wait_index(index)

    masters = [i for i in cluster.instances if not i.eval("return box.info.ro")]
    assert masters[0] == i2

    # on_leader_change called at i1 and i2
    # because this is previous and new leader, and not called at i3
    plugin_ref.assert_cb_called("testservice_1", "on_leader_change", 1, i1, i2)
    plugin_ref.assert_cb_called("testservice_1", "on_leader_change", 0, i3)

    # i1 and i3 known that they are not a leader; i2 know that he is a leader
    plugin_ref.assert_last_seen_ctx("testservice_1", {"is_master": False}, i1, i3)
    plugin_ref.assert_last_seen_ctx("testservice_1", {"is_master": True}, i2)


def test_error_on_leader_change(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_id="r1", wait_online=True)
    i2 = cluster.add_instance(replicaset_id="r1", wait_online=True)

    plugin_ref = PluginReflection.default(i1, i2)

    masters = [i for i in cluster.instances if not i.eval("return box.info.ro")]
    assert masters[0] == i1

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    plugin_ref.inject_error("testservice_1", "on_leader_change", True, i1)

    index = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r1"],
        ops=[("=", "target_master_id", i2.instance_id)],
    )
    cluster.raft_wait_index(index)

    masters = [i for i in cluster.instances if not i.eval("return box.info.ro")]
    assert masters[0] == i2

    plugin_ref.assert_last_seen_ctx("testservice_1", {"is_master": True}, i2)

    # assert that the first instance now has a poison service
    # and the second instance is not poisoned
    plugin_ref.assert_route_poisoned(i1.instance_id, "testservice_1")
    plugin_ref.assert_route_poisoned(i2.instance_id, "testservice_1", poisoned=False)


def _test_plugin_lifecycle(cluster: Cluster, compact_raft_log: bool):
    i1, i2, i3, i4 = cluster.deploy(instance_count=4)
    p1_ref = PluginReflection.default(i1, i2, i3, i4)
    p2_ref = PluginReflection(
        _PLUGIN_SMALL, _PLUGIN_SMALL_SERVICES, i1, i2, i3, i4
    ).set_topology(
        {
            i1: _PLUGIN_SMALL_SERVICES,
            i2: _PLUGIN_SMALL_SERVICES,
            i3: _PLUGIN_SMALL_SERVICES,
            i4: _PLUGIN_SMALL_SERVICES,
        }
    )

    # install and enable two plugins
    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    install_and_enable_plugin(i1, _PLUGIN_SMALL, _PLUGIN_SMALL_SERVICES)

    # assert that system tables are filled
    p1_ref = p1_ref.install(True).enable(True)
    p2_ref = p2_ref.install(True).enable(True)
    p1_ref.assert_synced()
    p2_ref.assert_synced()

    # assert that on_start callbacks successfully called
    PluginReflection.assert_cb_called("testservice_1", "on_start", 2, i1, i2, i3, i4)
    p1_ref.assert_cb_called("testservice_2", "on_start", 1, i1, i2, i3, i4)

    i4.terminate()

    p3 = "testplug_small_svc2"
    p3_svc = ["testservice_2"]
    # add third plugin
    p3_ref = PluginReflection(p3, p3_svc, i1, i2, i3, i4).set_topology(
        {i1: p3_svc, i2: p3_svc, i3: p3_svc, i4: p3_svc}
    )
    install_and_enable_plugin(i1, p3, p3_svc)
    p3_ref = p3_ref.install(True).enable(True)

    # update first plugin config
    i1.eval(
        'pico.update_plugin_config("testplug", "testservice_1", {foo = '
        'true, bar = 102, baz = {"a", "b"}})'
    )

    # disable second plugin
    i1.call("pico.disable_plugin", _PLUGIN_SMALL)
    p2_ref = p2_ref.enable(False).set_topology({})
    time.sleep(1)

    if compact_raft_log:
        # Compact raft log to trigger snapshot with an unfinished schema change.
        i1.raft_compact_log()
        i2.raft_compact_log()
        i3.raft_compact_log()

    i4.start()
    i4.wait_online()

    # check that 1st and 3rd plugin enabled at all instances
    p1_ref.assert_synced()
    p3_ref.assert_synced()
    # assert first plugin configuration update at all instances
    p1_ref.assert_config("testservice_1", _NEW_CFG, i1, i2, i3, i4)
    # assert second plugin disabled at all instances
    p2_ref.assert_synced()


def test_four_plugin_install_and_enable(cluster: Cluster):
    _test_plugin_lifecycle(cluster, compact_raft_log=False)


def test_four_plugin_install_and_enable2(cluster: Cluster):
    _test_plugin_lifecycle(cluster, compact_raft_log=True)


# -------------------------- topology tests -------------------------------------


def test_set_topology(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    # set topology to non-existent plugin is forbidden
    with pytest.raises(
        ReturnError,
        match="Service `testservice_1` for plugin `non-existent` not found at instance",
    ):
        i1.call(
            "pico.update_plugin_tiers",
            "non-existent",
            _PLUGIN_SERVICES[0],
            [_DEFAULT_TIER],
        )

    # set topology to non-existent plugin service is forbidden
    with pytest.raises(
        ReturnError,
        match="Service `non-existent` for plugin `testplug` not found at instance",
    ):
        i1.call("pico.update_plugin_tiers", _PLUGIN, "non-existent", [_DEFAULT_TIER])

    # set non-existent tier to first plugin service,
    # and don't set any tier for second plugin service;
    # both services must never be started
    i1.call("pico.install_plugin", _PLUGIN)
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[0], ["non-existent"])
    i1.call("pico.enable_plugin", _PLUGIN)

    plugin_ref = plugin_ref.install(True).enable(True).set_topology({i1: [], i2: []})
    plugin_ref.assert_synced()

    plugin_ref.assert_cb_called("testservice_1", "on_start", 0, i1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_start", 0, i1, i2)


cluster_cfg = """
cluster:
    cluster_id: test
    tiers:
        red:
            replication_factor: 1
        blue:
            replication_factor: 1
        green:
            replication_factor: 1
"""


def test_set_topology_for_single_plugin(cluster: Cluster):
    cluster.set_config_file(yaml=cluster_cfg)

    i1 = cluster.add_instance(wait_online=True, tier="red")
    i2 = cluster.add_instance(wait_online=True, tier="blue")
    i3 = cluster.add_instance(wait_online=True, tier="green")

    plugin_ref = PluginReflection(_PLUGIN, _PLUGIN_SERVICES, i1, i2, i3)

    i1.call("pico.install_plugin", _PLUGIN)
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[0], ["red"])
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[1], ["blue"])
    i1.call("pico.enable_plugin", _PLUGIN)

    plugin_ref = (
        plugin_ref.install(True)
        .enable(True)
        .set_topology({i1: [_PLUGIN_SERVICES[0]], i2: [_PLUGIN_SERVICES[1]], i3: []})
    )
    plugin_ref.assert_synced()

    plugin_ref.assert_cb_called("testservice_1", "on_start", 1, i1)
    plugin_ref.assert_cb_called("testservice_1", "on_start", 0, i2, i3)
    plugin_ref.assert_cb_called("testservice_2", "on_start", 1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_start", 0, i1, i3)


def test_set_topology_for_multiple_plugins(cluster: Cluster):
    cluster.set_config_file(yaml=cluster_cfg)

    i1 = cluster.add_instance(wait_online=True, tier="red")
    i2 = cluster.add_instance(wait_online=True, tier="blue")
    i3 = cluster.add_instance(wait_online=True, tier="green")

    p1_ref = PluginReflection(_PLUGIN, _PLUGIN_SERVICES, i1, i2, i3)
    p2_ref = PluginReflection(_PLUGIN_SMALL, _PLUGIN_SMALL_SERVICES, i1, i2, i3)

    i1.call("pico.install_plugin", _PLUGIN)
    i1.call("pico.install_plugin", _PLUGIN_SMALL)
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[0], ["red"])
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[1], ["red"])
    i1.call(
        "pico.update_plugin_tiers", _PLUGIN_SMALL, _PLUGIN_SMALL_SERVICES[0], ["blue"]
    )
    i1.call("pico.enable_plugin", _PLUGIN)
    i1.call("pico.enable_plugin", _PLUGIN_SMALL)

    p1_ref = (
        p1_ref.install(True)
        .enable(True)
        .set_topology({i1: _PLUGIN_SERVICES, i2: [], i3: []})
    )
    p1_ref.assert_synced()
    p2_ref = (
        p2_ref.install(True)
        .enable(True)
        .set_topology({i1: [], i2: _PLUGIN_SMALL_SERVICES, i3: []})
    )
    p2_ref.assert_synced()

    PluginReflection.assert_cb_called(_PLUGIN_SERVICES[0], "on_start", 1, i1)
    PluginReflection.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 1, i1)
    PluginReflection.assert_cb_called(_PLUGIN_SMALL_SERVICES[0], "on_start", 1, i2)
    PluginReflection.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 0, i2)
    PluginReflection.assert_cb_called(_PLUGIN_SERVICES[0], "on_start", 0, i3)
    PluginReflection.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 0, i3)
    PluginReflection.assert_cb_called(_PLUGIN_SMALL_SERVICES[0], "on_start", 0, i3)


def test_update_topology_1(cluster: Cluster):
    cluster.set_config_file(yaml=cluster_cfg)

    i1 = cluster.add_instance(wait_online=True, tier="red")
    i2 = cluster.add_instance(wait_online=True, tier="blue")
    i3 = cluster.add_instance(wait_online=True, tier="green")

    plugin_ref = PluginReflection(_PLUGIN, _PLUGIN_SERVICES, i1, i2, i3)

    i1.call("pico.install_plugin", _PLUGIN)
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[0], ["red"])
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[1], ["red"])
    i1.call("pico.enable_plugin", _PLUGIN)

    plugin_ref = (
        plugin_ref.install(True)
        .enable(True)
        .set_topology({i1: _PLUGIN_SERVICES, i2: [], i3: []})
    )
    plugin_ref.assert_synced()

    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_start", 1, i1)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 1, i1)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_start", 0, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 0, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_stop", 0, i1, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_stop", 0, i1, i2, i3)

    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[0], ["blue"])

    plugin_ref = plugin_ref.set_topology(
        {i1: [_PLUGIN_SERVICES[1]], i2: [_PLUGIN_SERVICES[0]], i3: []}
    )
    retrying(lambda: plugin_ref.assert_synced())

    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_start", 1, i1, i2)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 1, i1)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_start", 0, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 0, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_stop", 1, i1)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_stop", 0, i1)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_stop", 0, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_stop", 0, i2, i3)


def test_update_topology_2(cluster: Cluster):
    cluster.set_config_file(yaml=cluster_cfg)

    i1 = cluster.add_instance(wait_online=True, tier="red")
    i2 = cluster.add_instance(wait_online=True, tier="blue")
    i3 = cluster.add_instance(wait_online=True, tier="green")

    plugin_ref = PluginReflection(_PLUGIN, _PLUGIN_SERVICES, i1, i2, i3)

    i1.call("pico.install_plugin", _PLUGIN)
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[0], ["red"])
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[1], ["red"])
    i1.call("pico.enable_plugin", _PLUGIN)

    plugin_ref = (
        plugin_ref.install(True)
        .enable(True)
        .set_topology({i1: _PLUGIN_SERVICES, i2: [], i3: []})
    )
    plugin_ref.assert_synced()

    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_start", 1, i1)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 1, i1)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_start", 0, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 0, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_stop", 0, i1, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_stop", 0, i1, i2, i3)

    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[0], ["red", "blue"])

    plugin_ref = plugin_ref.set_topology(
        {i1: _PLUGIN_SERVICES, i2: [_PLUGIN_SERVICES[0]], i3: []}
    )
    retrying(lambda: plugin_ref.assert_synced())

    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_start", 1, i1, i2)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 1, i1)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_start", 0, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 0, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_stop", 0, i1, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_stop", 0, i1, i2, i3)


def test_update_topology_3(cluster: Cluster):
    cluster.set_config_file(yaml=cluster_cfg)

    i1 = cluster.add_instance(wait_online=True, tier="red")
    i2 = cluster.add_instance(wait_online=True, tier="blue")
    i3 = cluster.add_instance(wait_online=True, tier="green")

    plugin_ref = PluginReflection(_PLUGIN, _PLUGIN_SERVICES, i1, i2, i3)

    i1.call("pico.install_plugin", _PLUGIN)
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[0], ["red"])
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[1], ["red"])
    i1.call("pico.enable_plugin", _PLUGIN)

    plugin_ref = (
        plugin_ref.install(True)
        .enable(True)
        .set_topology({i1: _PLUGIN_SERVICES, i2: [], i3: []})
    )
    plugin_ref.assert_synced()

    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_start", 1, i1)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 1, i1)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_start", 0, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 0, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_stop", 0, i1, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_stop", 0, i1, i2, i3)

    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[0], [])

    plugin_ref = plugin_ref.set_topology({i1: [_PLUGIN_SERVICES[1]], i2: [], i3: []})
    retrying(lambda: plugin_ref.assert_synced())

    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_start", 1, i1)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 1, i1)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_start", 0, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_start", 0, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_stop", 1, i1)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[0], "on_stop", 0, i2, i3)
    plugin_ref.assert_cb_called(_PLUGIN_SERVICES[1], "on_stop", 0, i1, i2, i3)


def test_set_topology_after_compaction(cluster: Cluster):
    cluster.set_config_file(yaml=cluster_cfg)

    i1 = cluster.add_instance(wait_online=True, tier="red")
    i2 = cluster.add_instance(wait_online=True, tier="blue")
    i3 = cluster.add_instance(wait_online=True, tier="green")

    p1_ref = PluginReflection(_PLUGIN, _PLUGIN_SERVICES, i1, i2, i3)
    p2_ref = PluginReflection(_PLUGIN_SMALL, _PLUGIN_SMALL_SERVICES, i1, i2, i3)

    i1.call("pico.install_plugin", _PLUGIN)
    i1.call("pico.install_plugin", _PLUGIN_SMALL)
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[0], ["red"])
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[1], ["red", "blue"])
    i1.call(
        "pico.update_plugin_tiers",
        _PLUGIN_SMALL,
        _PLUGIN_SMALL_SERVICES[0],
        ["blue", "green"],
    )
    i1.call("pico.enable_plugin", _PLUGIN)
    i1.call("pico.enable_plugin", _PLUGIN_SMALL)

    p1_ref = (
        p1_ref.install(True)
        .enable(True)
        .set_topology({i1: _PLUGIN_SERVICES, i2: [_PLUGIN_SERVICES[1]], i3: []})
    )
    p1_ref.assert_synced()
    p2_ref = (
        p2_ref.install(True)
        .enable(True)
        .set_topology({i1: [], i2: _PLUGIN_SMALL_SERVICES, i3: _PLUGIN_SMALL_SERVICES})
    )
    p2_ref.assert_synced()

    # terminate i3 and update topology
    i3.terminate()

    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[0], ["red", "green"])
    i1.call(
        "pico.update_plugin_tiers", _PLUGIN_SMALL, _PLUGIN_SMALL_SERVICES[0], ["blue"]
    )

    # compact raft log at online instances
    i1.raft_compact_log()
    i2.raft_compact_log()

    # start i3 and check that new topology set at whole cluster
    i3.start()
    i3.wait_online()

    p1_ref = p1_ref.set_topology(
        {i1: _PLUGIN_SERVICES, i2: [_PLUGIN_SERVICES[1]], i3: [_PLUGIN_SERVICES[0]]}
    )
    p2_ref = p2_ref.set_topology({i1: [], i2: _PLUGIN_SMALL_SERVICES, i3: []})

    retrying(lambda: p1_ref.assert_synced())
    retrying(lambda: p2_ref.assert_synced())


def test_set_topology_with_error_on_start(cluster: Cluster):
    cluster.set_config_file(yaml=cluster_cfg)

    i1 = cluster.add_instance(wait_online=True, tier="red")
    i2 = cluster.add_instance(wait_online=True, tier="blue")

    plugin_ref = PluginReflection(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    i1.call("pico.install_plugin", _PLUGIN)
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[0], ["red"])
    i1.call("pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[1], ["red"])
    i1.call("pico.enable_plugin", _PLUGIN)

    plugin_ref = (
        plugin_ref.install(True)
        .enable(True)
        .set_topology({i1: _PLUGIN_SERVICES, i2: []})
    )
    plugin_ref.assert_synced()

    # inject error into tier "blue"
    plugin_ref.inject_error("testservice_1", "on_start", True, i2)

    with pytest.raises(ReturnError, match="Error while update plugin topology"):
        i1.call(
            "pico.update_plugin_tiers", _PLUGIN, _PLUGIN_SERVICES[0], ["red", "blue"]
        )

    # assert that topology doesnt changes
    plugin_ref.assert_synced()
