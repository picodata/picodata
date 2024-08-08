from dataclasses import dataclass, field
import time
from typing import Any, Dict, List, Optional
import pytest
import uuid
import msgpack  # type: ignore
import os
import hashlib
from conftest import (
    Cluster,
    ReturnError,
    Retriable,
    Instance,
    TarantoolError,
    log_crawler,
)
from decimal import Decimal
import requests  # type: ignore
from conftest import (
    ErrorCode,
)

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
_PLUGIN_WITH_MIGRATION = "testplug_w_migration"
_PLUGIN_WITH_MIGRATION_SERVICES = ["testservice_2"]
_PLUGIN_W_SDK = "testplug_sdk"
_PLUGIN_W_SDK_SERVICES = ["testservice_3"]
SERVICE_W_RPC = "service_with_rpc_tests"

REQUEST_ID = 1
PLUGIN_NAME = 2
SERVICE_NAME = 3
PLUGIN_VERSION = 4

# ---------------------------------- Test helper classes {-----------------------------------------


@dataclass
class PluginReflection:
    """PluginReflection used to describe the expected state of the plugin"""

    # plugin name
    name: str
    # plugin version
    version: str
    # list of plugin services
    services: List[str]
    # instances in cluster
    instances: List[Instance]
    # plugin topology
    topology: Dict[Instance, List[str]] = field(default_factory=dict)
    # if True - assert_synced checks that plugin are installed
    installed: bool = False
    # if True - assert_synced checks that plugin are enabled
    enabled: bool = False
    # plugin data [table -> tuples] map
    data: Dict[str, Optional[List[Any]]] = field(default_factory=dict)

    def __post__init__(self):
        for i in self.instances:
            self.topology[i] = []

    @staticmethod
    def default(*instances):
        """Create reflection for default plugin with default topology"""
        topology = {}
        for i in instances:
            topology[i] = _PLUGIN_SERVICES
        return PluginReflection(
            name=_PLUGIN,
            version="0.1.0",
            services=_PLUGIN_SERVICES,
            instances=list(instances),
        ).set_topology(topology)

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

    def set_data(self, data: dict[str, Optional[list[Any]]]):
        self.data = data
        return self

    def assert_synced(self):
        """Assert that plugin reflection and plugin state in cluster are synchronized.
        This means that system tables `_pico_plugin`, `_pico_service` and `_pico_service_route`
        contain necessary plugin information."""
        for i in self.instances:
            plugins = i.eval(
                "return box.space._pico_plugin:select({...})", self.name, self.version
            )
            if self.installed:
                assert len(plugins) == 1
                assert plugins[0][1] == self.enabled
            else:
                assert len(plugins) == 0

            for service in self.services:
                svcs = i.eval(
                    "return box.space._pico_service:select({...})",
                    [self.name, service, self.version],
                )
                if self.installed:
                    assert len(svcs) == 1
                else:
                    assert len(svcs) == 0

        for i in self.topology:
            expected_services = []
            for service in self.topology[i]:
                expected_services.append(
                    [i.instance_id, self.name, self.version, service, False]
                )

            for neighboring_i in self.topology:
                routes = neighboring_i.eval(
                    'return box.space._pico_service_route:pairs({...}, {iterator="EQ"}):totable()',
                    i.instance_id,
                    self.name,
                    self.version,
                )
                assert routes == expected_services

    def assert_data_synced(self):
        for table in self.data:
            data = []

            for i in self.instances:
                if self.data[table] is None:
                    with pytest.raises(TarantoolError, match="attempt to index field"):
                        i.eval(f"return box.space.{table}:select()")
                else:
                    data += i.eval(f"return box.space.{table}:select()")

            if self.data[table] is not None:
                assert data.sort() == self.data[table].sort()

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
            route_poisoned = i.eval(
                "return box.space._pico_service_route:get({...}).poison",
                poison_instance_id,
                self.name,
                self.version,
                service,
            )
            assert route_poisoned == poisoned

    @staticmethod
    def assert_data_eq(instance, key, expected):
        val = instance.eval(f"return _G['plugin_state']['data']['{key}']")
        assert val == expected

    @staticmethod
    def assert_int_data_le(instance, key, expected):
        val = instance.eval(f"return _G['plugin_state']['data']['{key}']")
        assert int(val) <= expected


# ---------------------------------- } Test helper classes ----------------------------------------


def test_invalid_manifest_plugin(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    # try to create non-existent plugin
    with pytest.raises(
        ReturnError, match="Error while discovering manifest for plugin"
    ):
        i1.call("pico.install_plugin", "non-existent", "0.1.0")
    PluginReflection("non-existent", "0.1.0", [], [i1, i2]).assert_synced()

    # try to use invalid manifest (with undefined plugin name)
    with pytest.raises(ReturnError, match="missing field `name`"):
        i1.call("pico.install_plugin", "testplug_broken_manifest_1", "0.1.0")
    PluginReflection(
        "testplug_broken_manifest_1", "0.1.0", _PLUGIN_SERVICES, [i1, i2]
    ).assert_synced()

    # try to use invalid manifest (with invalid default configuration)
    with pytest.raises(ReturnError, match="Error while enable the plugin"):
        i1.call("pico.install_plugin", "testplug_broken_manifest_2", "0.1.0")
        i1.call(
            "pico.service_append_tier",
            "testplug_broken_manifest_2",
            "0.1.0",
            "testservice_1",
            _DEFAULT_TIER,
        )
        i1.call(
            "pico.service_append_tier",
            "testplug_broken_manifest_2",
            "0.1.0",
            "testservice_2",
            _DEFAULT_TIER,
        )
        i1.call("pico.enable_plugin", "testplug_broken_manifest_2", "0.1.0")
    PluginReflection(
        "testplug_broken_manifest_2", "0.1.0", _PLUGIN_SERVICES, [i1, i2]
    ).install(True).assert_synced()

    # try to use invalid manifest (with non-existed extra service)
    with pytest.raises(ReturnError, match="Error while install the plugin"):
        i1.call("pico.install_plugin", "testplug_broken_manifest_3", "0.1.0")
    PluginReflection(
        "testplug_broken_manifest_3",
        "0.1.0",
        ["testservice_1", "testservice_2", "testservice_3"],
        [i1, i2],
    ).assert_synced()
    PluginReflection.assert_cb_called("testservice_1", "on_start", 0, i1, i2)


def install_and_enable_plugin(
    instance,
    plugin,
    services,
    version="0.1.0",
    migrate=False,
    timeout=_3_SEC,
    default_config=None,
):
    instance.call(
        "pico.install_plugin", plugin, version, {"migrate": migrate}, timeout=timeout
    )
    for s in services:
        if default_config is not None:
            instance.eval(
                f"box.space._pico_service:update"
                f"({{'{plugin}', '{s}', '0.1.0'}}, {{ {{'=', 'configuration', ... }} }})",
                default_config,
            )
        instance.call("pico.service_append_tier", plugin, version, s, _DEFAULT_TIER)
    instance.call("pico.enable_plugin", plugin, version, timeout=timeout)


def test_plugin_install(cluster: Cluster):
    """
    plugin installation must be full idempotence:
    install non-installed plugin - default behavior
    install already disabled plugin - do nothing
    install already enabled plugin - do nothing
    """

    i1, i2 = cluster.deploy(instance_count=2)
    expected_state = PluginReflection(_PLUGIN, "0.1.0", _PLUGIN_SERVICES, [i1, i2])

    # check default behaviour
    i1.call("pico.install_plugin", _PLUGIN, "0.1.0")
    expected_state = expected_state.install(True)
    expected_state.assert_synced()

    # check install already disabled plugin
    i1.call("pico.install_plugin", _PLUGIN, "0.1.0")
    expected_state.assert_synced()

    # enable plugin and check installation of already enabled plugin
    i1.call(
        "pico.service_append_tier", _PLUGIN, "0.1.0", "testservice_1", _DEFAULT_TIER
    )
    i1.call(
        "pico.service_append_tier", _PLUGIN, "0.1.0", "testservice_2", _DEFAULT_TIER
    )
    i1.call("pico.enable_plugin", _PLUGIN, "0.1.0")
    expected_state = expected_state.set_topology(
        {i1: _PLUGIN_SERVICES, i2: _PLUGIN_SERVICES}
    ).enable(True)
    expected_state.assert_synced()

    i1.call("pico.install_plugin", _PLUGIN, "0.1.0")
    expected_state.assert_synced()

    # check that installation of another plugin version is ok
    expected_state_v2 = PluginReflection(
        _PLUGIN, "0.2.0", _PLUGIN_SERVICES, [i1, i2]
    ).install(True)
    i1.call("pico.install_plugin", _PLUGIN, "0.2.0")
    expected_state_v2.assert_synced()


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
    i1.call("pico.enable_plugin", _PLUGIN, "0.1.0")
    plugin_ref.assert_synced()
    # assert that `on_start` don't call twice
    plugin_ref.assert_cb_called("testservice_1", "on_start", 1, i1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_start", 1, i1, i2)

    # check that enabling of non-installed plugin return error
    with pytest.raises(ReturnError, match="Error while enable the plugin"):
        i1.call("pico.enable_plugin", _PLUGIN_SMALL, "0.1.0")

    # check that enabling of plugin with another version return error
    with pytest.raises(ReturnError, match="Error while enable the plugin"):
        i1.call("pico.install_plugin", _PLUGIN, "0.2.0")
        i1.call("pico.enable_plugin", _PLUGIN, "0.2.0")


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

    # check that disabling of a non-enabled version do nothing
    with pytest.raises(
        ReturnError, match="Plugin `testplug:0.2.0` not found at instance"
    ):
        i1.call("pico.disable_plugin", _PLUGIN, "0.2.0")

    i1.call("pico.disable_plugin", _PLUGIN, "0.1.0")
    plugin_ref = plugin_ref.enable(False).set_topology({i1: [], i2: []})
    plugin_ref.assert_synced()

    # retrying, cause routing table update asynchronously
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())
    # assert that `on_stop` callbacks successfully called
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 1, i1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_stop", 1, i1, i2)

    # check disabling of already disabled plugin
    i1.call("pico.disable_plugin", _PLUGIN, "0.1.0")
    plugin_ref.assert_synced()
    # assert that `on_stop` callbacks don't call twice
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 1, i1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_stop", 1, i1, i2)

    # check that disabling of non-installed plugin return error
    with pytest.raises(
        ReturnError, match="Plugin `testplug_small:0.1.0` not found at instance"
    ):
        i1.call("pico.disable_plugin", _PLUGIN_SMALL, "0.1.0")


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
        i1.call("pico.remove_plugin", _PLUGIN, "0.1.0")
    plugin_ref.assert_synced()

    i1.call("pico._inject_error", "PLUGIN_EXIST_AND_ENABLED", True)
    # same, but error not returned to a client
    i1.call("pico.remove_plugin", _PLUGIN, "0.1.0")
    plugin_ref.assert_synced()
    i1.call("pico._inject_error", "PLUGIN_EXIST_AND_ENABLED", False)

    # check default behaviour
    i1.call("pico.disable_plugin", _PLUGIN, "0.1.0")
    plugin_ref = plugin_ref.enable(False).set_topology({i1: [], i2: []})

    # retrying, cause routing table update asynchronously
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())

    # install one more plugin version
    i1.call("pico.install_plugin", _PLUGIN, "0.2.0")
    plugin_ref_v2 = PluginReflection(
        _PLUGIN, "0.2.0", _PLUGIN_SERVICES, [i1, i2]
    ).install(True)

    i1.call("pico.remove_plugin", _PLUGIN, "0.1.0")
    plugin_ref = plugin_ref.install(False)
    plugin_ref.assert_synced()
    plugin_ref_v2.assert_synced()

    # check removing non-installed plugin
    i1.call("pico.remove_plugin", _PLUGIN, "0.1.0")
    plugin_ref.assert_synced()
    plugin_ref_v2.assert_synced()

    # remove last version
    i1.call("pico.remove_plugin", _PLUGIN, "0.2.0")
    plugin_ref = plugin_ref_v2.install(False)
    plugin_ref.assert_synced()
    plugin_ref_v2.assert_synced()


def test_two_plugin_install_and_enable(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    p1_ref = PluginReflection.default(i1, i2)
    p2_ref = PluginReflection(
        _PLUGIN_SMALL,
        "0.1.0",
        _PLUGIN_SMALL_SERVICES,
        [i1, i2],
        topology={i1: _PLUGIN_SMALL_SERVICES, i2: _PLUGIN_SMALL_SERVICES},
    )

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

    i1.call("pico.disable_plugin", _PLUGIN, "0.1.0", timeout=_3_SEC)
    # retrying, cause routing table update asynchronously
    plugin_ref = plugin_ref.enable(False).set_topology({i1: [], i2: []})
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())

    i1.call("pico.remove_plugin", _PLUGIN, "0.1.0", timeout=_3_SEC)
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
    plugin_ref = PluginReflection(_PLUGIN, "0.1.0", _PLUGIN_SERVICES, [i1, i2])

    # inject error into second instance
    plugin_ref.inject_error("testservice_1", "on_start", True, i2)

    # assert that plugin not loaded and on_stop called on both instances
    with pytest.raises(ReturnError, match="Error while enable the plugin"):
        install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)

    # plugin installed but disabled
    plugin_ref = plugin_ref.install(True).set_topology({i1: [], i2: []})
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 1, i1, i2)

    # inject error into both instances
    plugin_ref.inject_error("testservice_1", "on_start", True, i1)

    # assert that plugin not loaded and on_stop called on both instances
    with pytest.raises(ReturnError, match="Error while enable the plugin"):
        install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)

    # plugin installed but disabled
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())
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
    plugin_ref = PluginReflection(_PLUGIN, "0.1.0", _PLUGIN_SERVICES, [i1, i2])

    # inject timeout into second instance
    plugin_ref.inject_error("testservice_1", "on_start_sleep_sec", 3, i2)

    with pytest.raises(ReturnError, match="Error while enable the plugin"):
        i1.call("pico.install_plugin", _PLUGIN, "0.1.0")
        i1.call(
            "pico.service_append_tier", _PLUGIN, "0.1.0", "testservice_1", _DEFAULT_TIER
        )
        i1.call(
            "pico.service_append_tier", _PLUGIN, "0.1.0", "testservice_2", _DEFAULT_TIER
        )
        i1.call(
            "pico.enable_plugin", _PLUGIN, "0.1.0", {"on_start_timeout": 2}, timeout=4
        )
    # need to wait until sleep at i2 called asynchronously
    time.sleep(2)

    # assert that plugin installed, disabled and on_stop called on both instances
    plugin_ref = plugin_ref.install(True).set_topology({i1: [], i2: []})
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 1, i1, i2)

    # inject timeout into both instances
    plugin_ref.inject_error("testservice_1", "on_start_sleep_sec", 3, i1)

    with pytest.raises(ReturnError, match="Error while enable the plugin"):
        i1.call(
            "pico.enable_plugin", _PLUGIN, "0.1.0", {"on_start_timeout": 2}, timeout=4
        )
    # need to wait until sleep at i1 and i2 called asynchronously
    time.sleep(2)

    # assert that plugin installed, disabled and on_stop called on both instances
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 2, i1, i2)


# -------------------------- migration tests -------------------------------------


_DATA_V_0_1_0 = {
    "AUTHOR": [
        [1, "Alexander Pushkin"],
        [2, "Alexander Blok"],
    ],
    "BOOK": [
        [1, "Ruslan and Ludmila"],
        [2, "The Tale of Tsar Saltan"],
        [3, "The Twelve"],
        [4, "The Lady Unknown"],
    ],
}

_DATA_V_0_2_0 = {
    "AUTHOR": [
        [1, "Alexander Pushkin"],
        [2, "Alexander Blok"],
    ],
    "BOOK": [
        [1, "Ruslan and Ludmila"],
        [2, "The Tale of Tsar Saltan"],
        [3, "The Twelve"],
        [4, "The Lady Unknown"],
    ],
    "STORE": [
        [1, "OZON"],
        [2, "Yandex"],
        [2, "Wildberries"],
    ],
    "MANAGER": [
        [1, "Manager 1", 1],
        [2, "Manager 2", 1],
        [3, "Manager 3", 2],
    ],
}

_NO_DATA_V_0_1_0: dict[str, None] = {
    "AUTHOR": None,
    "BOOK": None,
}

_NO_DATA_V_0_2_0: dict[str, None] = {
    "AUTHOR": None,
    "BOOK": None,
    "STORE": None,
    "MANAGER": None,
}


def test_migration_separate_command(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    expected_state = PluginReflection.default()

    i1.call("pico.install_plugin", _PLUGIN_WITH_MIGRATION, "0.1.0", timeout=5)
    i1.call("pico.migration_up", _PLUGIN_WITH_MIGRATION, "0.1.0")
    expected_state = expected_state.set_data(_DATA_V_0_1_0)
    expected_state.assert_data_synced()

    # check migration file checksums are calculated correctly
    rows = i1.sql(""" SELECT "migration_file", "hash" FROM "_pico_plugin_migration" """)
    assert i1.plugin_dir
    plugin_dir = os.path.join(i1.plugin_dir, _PLUGIN_WITH_MIGRATION, "0.1.0")
    for filename, checksum in rows:
        fullpath = os.path.join(plugin_dir, filename)
        with open(fullpath, "rb") as f:
            hash = hashlib.md5(f.read())
        assert checksum == hash.hexdigest(), filename

    # increase a version to v0.2.0
    i1.call("pico.install_plugin", _PLUGIN_WITH_MIGRATION, "0.2.0", timeout=5)
    i1.call("pico.migration_up", _PLUGIN_WITH_MIGRATION, "0.2.0")
    expected_state = expected_state.set_data(_DATA_V_0_2_0)
    expected_state.assert_data_synced()

    # now down from v0.2.0
    i1.call("pico.migration_down", _PLUGIN_WITH_MIGRATION, "0.2.0")
    i1.call("pico.remove_plugin", _PLUGIN_WITH_MIGRATION, "0.2.0", timeout=5)
    expected_state = expected_state.set_data(_NO_DATA_V_0_2_0)
    expected_state.assert_data_synced()


def test_migration_separate_command_apply_err(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    expected_state = PluginReflection.default()

    i1.call("pico.install_plugin", _PLUGIN_WITH_MIGRATION, "0.1.0", timeout=5)
    # migration of v0.1.0 should be ok
    i1.call("pico.migration_up", _PLUGIN_WITH_MIGRATION, "0.1.0")
    expected_state = expected_state.set_data(_DATA_V_0_1_0)
    expected_state.assert_data_synced()

    # second file in a migration list of v0.2.0 applied with error
    i1.call("pico._inject_error", "PLUGIN_MIGRATION_SECOND_FILE_APPLY_ERROR", True)

    # expect that migration of v0.2.0 rollback to v0.1.0
    i1.call("pico.install_plugin", _PLUGIN_WITH_MIGRATION, "0.2.0", timeout=5)
    with pytest.raises(ReturnError, match="Failed to apply `UP` command"):
        i1.call("pico.migration_up", _PLUGIN_WITH_MIGRATION, "0.2.0")
    expected_state.assert_data_synced()


def test_migration_for_changed_migration(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    expected_state = PluginReflection.default()

    i1.call("pico.install_plugin", _PLUGIN_WITH_MIGRATION, "0.1.0", timeout=5)
    i1.call("pico.migration_up", _PLUGIN_WITH_MIGRATION, "0.1.0")
    expected_state = expected_state.set_data(_DATA_V_0_1_0)
    expected_state.assert_data_synced()

    # increase the version to v0.2.0_broken with changed file author.db
    i1.call("pico.install_plugin", _PLUGIN_WITH_MIGRATION, "0.2.0_broken", timeout=5)

    error_regex = "inconsistent with previous version migration list, "
    r"reason: unknown migration files found in manifest migrations "
    r"\(mismatched file meta information for book\.db\)"
    with pytest.raises(ReturnError, match=error_regex):
        i1.call("pico.migration_up", _PLUGIN_WITH_MIGRATION, "0.2.0_broken")


def test_migration_on_plugin_install(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    expected_state = PluginReflection(
        _PLUGIN_WITH_MIGRATION, "0.1.0", _PLUGIN_WITH_MIGRATION_SERVICES, [i1, i2]
    )

    i1.call(
        "pico.install_plugin",
        _PLUGIN_WITH_MIGRATION,
        "0.1.0",
        {"migrate": True},
        timeout=5,
    )
    expected_state = expected_state.install(True).set_data(_DATA_V_0_1_0)
    expected_state.assert_synced()
    expected_state.assert_data_synced()

    i1.call(
        "pico.remove_plugin",
        _PLUGIN_WITH_MIGRATION,
        "0.1.0",
        {"drop_data": True},
        timeout=5,
    )
    expected_state = expected_state.install(False).set_data(_NO_DATA_V_0_1_0)
    expected_state.assert_synced()
    expected_state.assert_data_synced()


def test_migration_on_plugin_next_version_install(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    expected_state = PluginReflection(
        _PLUGIN_WITH_MIGRATION, "0.1.0", _PLUGIN_WITH_MIGRATION_SERVICES, [i1, i2]
    )

    i1.call(
        "pico.install_plugin",
        _PLUGIN_WITH_MIGRATION,
        "0.1.0",
        {"migrate": True},
        timeout=5,
    )
    expected_state = expected_state.install(True).set_data(_DATA_V_0_1_0)
    expected_state.assert_synced()
    expected_state.assert_data_synced()

    i1.call(
        "pico.install_plugin",
        _PLUGIN_WITH_MIGRATION,
        "0.2.0",
        {"migrate": True},
        timeout=5,
    )
    expected_state = (
        PluginReflection(
            _PLUGIN_WITH_MIGRATION, "0.2.0", _PLUGIN_WITH_MIGRATION_SERVICES, [i1, i2]
        )
        .install(True)
        .set_data(_DATA_V_0_2_0)
    )
    expected_state.assert_synced()
    expected_state.assert_data_synced()

    i1.call(
        "pico.remove_plugin",
        _PLUGIN_WITH_MIGRATION,
        "0.2.0",
        {"drop_data": True},
        timeout=5,
    )
    expected_state = expected_state.install(False).set_data(_NO_DATA_V_0_2_0)
    expected_state.assert_synced()
    expected_state.assert_data_synced()


def test_migration_file_invalid_ext(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    # the first file in a migration list has an invalid extension
    i1.call("pico._inject_error", "PLUGIN_MIGRATION_FIRST_FILE_INVALID_EXT", True)

    with pytest.raises(ReturnError, match="invalid extension"):
        i1.call(
            "pico.install_plugin",
            _PLUGIN_WITH_MIGRATION,
            "0.1.0",
            {"migrate": True},
            timeout=5,
        )


def test_migration_apply_err(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    expected_state = PluginReflection(
        _PLUGIN_WITH_MIGRATION, "0.1.0", _PLUGIN_WITH_MIGRATION_SERVICES, [i1, i2]
    )

    # second file in a migration list applied with error
    i1.call("pico._inject_error", "PLUGIN_MIGRATION_SECOND_FILE_APPLY_ERROR", True)

    with pytest.raises(ReturnError, match="Failed to apply `UP` command"):
        i1.call(
            "pico.install_plugin",
            _PLUGIN_WITH_MIGRATION,
            "0.1.0",
            {"migrate": True},
            timeout=5,
        )
    expected_state = expected_state.install(True).set_data(_NO_DATA_V_0_1_0)
    expected_state.assert_synced()
    expected_state.assert_data_synced()


def test_migration_next_version_apply_err(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    # successfully install v0.1.0
    expected_state = PluginReflection(
        _PLUGIN_WITH_MIGRATION, "0.1.0", _PLUGIN_WITH_MIGRATION_SERVICES, [i1, i2]
    )
    i1.call(
        "pico.install_plugin",
        _PLUGIN_WITH_MIGRATION,
        "0.1.0",
        {"migrate": True},
        timeout=5,
    )
    expected_state = expected_state.install(True).set_data(_DATA_V_0_1_0)
    expected_state.assert_synced()
    expected_state.assert_data_synced()

    # second file in a migration list of v0.2.0 applied with error
    i1.call("pico._inject_error", "PLUGIN_MIGRATION_SECOND_FILE_APPLY_ERROR", True)

    # expect rollback to 0.1.0 migrations
    with pytest.raises(ReturnError, match="Failed to apply `UP` command"):
        i1.call(
            "pico.install_plugin",
            _PLUGIN_WITH_MIGRATION,
            "0.2.0",
            {"migrate": True},
            timeout=5,
        )
    expected_state.assert_data_synced()


def test_migration_client_down(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    expected_state = PluginReflection(
        _PLUGIN_WITH_MIGRATION, "0.1.0", _PLUGIN_WITH_MIGRATION_SERVICES, [i1, i2]
    )

    # client down while applied migration
    i1.call("pico._inject_error", "PLUGIN_MIGRATION_CLIENT_DOWN", True)

    i1.call(
        "pico.install_plugin",
        _PLUGIN_WITH_MIGRATION,
        "0.1.0",
        {"migrate": True},
        timeout=5,
    )
    expected_state = expected_state.install(True)
    expected_state.assert_synced()

    with pytest.raises(ReturnError, match="Error while enable the plugin"):
        i1.call("pico.enable_plugin", _PLUGIN_WITH_MIGRATION, "0.1.0")

    i1.call(
        "pico.remove_plugin",
        _PLUGIN_WITH_MIGRATION,
        "0.1.0",
        {"migrate": True},
        timeout=5,
    )
    expected_state = expected_state.install(False).set_data(_NO_DATA_V_0_1_0)
    expected_state.assert_synced()
    expected_state.assert_data_synced()


# -------------------------- configuration tests -------------------------------------


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
            "return pico.update_plugin_config('testplug', '0.1.0', "
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
        "pico.update_plugin_config('testplug', '0.1.0', 'testservice_1', {foo = "
        "true, bar = 102, baz = {'a', 'b'}})"
    )
    # retrying, cause new service configuration callback call asynchronously
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_config("testservice_1", _NEW_CFG, i1, i2)
    )


def test_plugin_double_config_update(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    i1.eval(
        'pico.update_plugin_config("testplug", "0.1.0", "testservice_1", {foo = '
        'true, bar = 102, baz = {"a", "b"}})'
        'return pico.update_plugin_config("testplug", "0.1.0", "testservice_1",'
        '{foo = false, bar = 102, baz = {"a", "b"}})'
    )
    # both configs were applied
    # retrying, cause callback call asynchronously
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_cb_called(
            "testservice_1", "on_config_change", 2, i1, i2
        )
    )
    plugin_ref.assert_config("testservice_1", _NEW_CFG_2, i1, i2)

    i1.eval(
        'pico.update_plugin_config("testplug", "0.1.0", "testservice_1", {foo = '
        'true, bar = 102, baz = {"a", "b"}})'
    )
    i2.eval(
        'pico.update_plugin_config("testplug", "0.1.0", "testservice_1", {foo = '
        'true, bar = 102, baz = {"a", "b"}})'
    )
    # both configs were applied and result config may be any of applied
    # retrying, cause callback call asynchronously
    Retriable(timeout=3, rps=5).call(
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
        "pico.update_plugin_config('testplug', '0.1.0', 'testservice_1', {foo = "
        "true, bar = 102, baz = {'a', 'b'}})"
    )

    # check that at i1 new configuration exists in global space
    # but not really applied to service because error
    cfg_space = plugin_ref.get_config("testservice_1", i1)
    assert cfg_space == _NEW_CFG
    cfg_seen = plugin_ref.get_seen_config("testservice_1", i1)
    assert cfg_seen == _DEFAULT_CFG
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_config("testservice_1", _NEW_CFG, i2)
    )

    # assert that the first instance now has a poison service
    # and the second instance is not poisoned
    # retrying, cause routing table update asynchronously
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_route_poisoned(i1.instance_id, "testservice_1")
    )
    Retriable(timeout=3, rps=5).call(
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
        "pico.update_plugin_config('testplug', '0.1.0', 'testservice_1', {foo = "
        "true, bar = 102, baz = {'a', 'b'}})"
    )

    # assert that the first instance now has a poison service
    # retrying, cause routing table update asynchronously
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_route_poisoned(i1.instance_id, "testservice_1")
    )

    plugin_ref.remove_error("testservice_1", "on_cfg_change", i1)

    i1.eval(
        "pico.update_plugin_config('testplug', '0.1.0', 'testservice_1', {foo = "
        "false, bar = 102, baz = {'a', 'b'}})"
    )

    # retrying, cause routing table update asynchronously
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_route_poisoned(
            i1.instance_id, "testservice_1", poisoned=False
        )
    )
    plugin_ref.assert_config("testservice_1", _NEW_CFG_2, i1, i2)


# -------------------------- leader change test -----------------------------------


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
    assert i1.replicaset_master_id() == i2.instance_id

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
    assert i1.replicaset_master_id() == i2.instance_id

    plugin_ref.assert_last_seen_ctx("testservice_1", {"is_master": True}, i2)

    # assert that the first instance now has a poison service
    # and the second instance is not poisoned
    plugin_ref.assert_route_poisoned(i1.instance_id, "testservice_1")
    plugin_ref.assert_route_poisoned(i2.instance_id, "testservice_1", poisoned=False)


def _test_plugin_lifecycle(cluster: Cluster, compact_raft_log: bool):
    i1, i2, i3, i4 = cluster.deploy(instance_count=4)
    p1_ref = PluginReflection.default(i1, i2, i3, i4)
    p2_ref = PluginReflection(
        _PLUGIN_SMALL,
        "0.1.0",
        _PLUGIN_SMALL_SERVICES,
        instances=[i1, i2, i3, i4],
        topology={
            i1: _PLUGIN_SMALL_SERVICES,
            i2: _PLUGIN_SMALL_SERVICES,
            i3: _PLUGIN_SMALL_SERVICES,
            i4: _PLUGIN_SMALL_SERVICES,
        },
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
    p3_ref = PluginReflection(
        p3,
        "0.1.0",
        p3_svc,
        instances=[i1, i2, i3, i4],
        topology={i1: p3_svc, i2: p3_svc, i3: p3_svc, i4: p3_svc},
    )
    install_and_enable_plugin(i1, p3, p3_svc)
    p3_ref = p3_ref.install(True).enable(True)

    # update first plugin config
    i1.eval(
        'pico.update_plugin_config("testplug", "0.1.0", "testservice_1", {foo = '
        'true, bar = 102, baz = {"a", "b"}})'
    )

    # disable second plugin
    i1.call("pico.disable_plugin", _PLUGIN_SMALL, "0.1.0")
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
    plugin_ref = PluginReflection(_PLUGIN, "0.1.0", _PLUGIN_SERVICES, [i1, i2])

    # set topology to non-existent plugin is forbidden
    with pytest.raises(
        ReturnError,
        match="Service `testservice_1` for plugin `non-existent:0.1.0` not found at instance",
    ):
        i1.call(
            "pico.service_append_tier",
            "non-existent",
            "0.1.0",
            _PLUGIN_SERVICES[0],
            _DEFAULT_TIER,
        )

    # set topology to non-existent plugin service is forbidden
    with pytest.raises(
        ReturnError,
        match="Service `non-existent` for plugin `testplug:0.1.0` not found at instance",
    ):
        i1.call(
            "pico.service_append_tier", _PLUGIN, "0.1.0", "non-existent", _DEFAULT_TIER
        )

    # set non-existent tier to first plugin service,
    # and don't set any tier for second plugin service;
    # both services must never be started
    i1.call("pico.install_plugin", _PLUGIN, "0.1.0")
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        "0.1.0",
        _PLUGIN_SERVICES[0],
        "non-existent",
    )
    i1.call("pico.enable_plugin", _PLUGIN, "0.1.0")

    plugin_ref = plugin_ref.install(True).enable(True).set_topology({i1: [], i2: []})
    plugin_ref.assert_synced()

    plugin_ref.assert_cb_called("testservice_1", "on_start", 0, i1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_start", 0, i1, i2)


cluster_cfg = """
cluster:
    cluster_id: test
    tier:
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

    plugin_ref = PluginReflection(_PLUGIN, "0.1.0", _PLUGIN_SERVICES, [i1, i2, i3])

    i1.call("pico.install_plugin", _PLUGIN, "0.1.0")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[0], "red")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[1], "blue")
    i1.call("pico.enable_plugin", _PLUGIN, "0.1.0")

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

    p1_ref = PluginReflection(_PLUGIN, "0.1.0", _PLUGIN_SERVICES, [i1, i2, i3])
    p2_ref = PluginReflection(
        _PLUGIN_SMALL, "0.1.0", _PLUGIN_SMALL_SERVICES, [i1, i2, i3]
    )

    i1.call("pico.install_plugin", _PLUGIN, "0.1.0")
    i1.call("pico.install_plugin", _PLUGIN_SMALL, "0.1.0")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[0], "red")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[1], "red")
    i1.call(
        "pico.service_append_tier",
        _PLUGIN_SMALL,
        "0.1.0",
        _PLUGIN_SMALL_SERVICES[0],
        "blue",
    )
    i1.call("pico.enable_plugin", _PLUGIN, "0.1.0")
    i1.call("pico.enable_plugin", _PLUGIN_SMALL, "0.1.0")

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

    plugin_ref = PluginReflection(_PLUGIN, "0.1.0", _PLUGIN_SERVICES, [i1, i2, i3])

    i1.call("pico.install_plugin", _PLUGIN, "0.1.0")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[0], "red")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[1], "red")
    i1.call("pico.enable_plugin", _PLUGIN, "0.1.0")

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

    i1.call("pico.service_remove_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[0], "red")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[0], "blue")

    plugin_ref = plugin_ref.set_topology(
        {i1: [_PLUGIN_SERVICES[1]], i2: [_PLUGIN_SERVICES[0]], i3: []}
    )
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())

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

    plugin_ref = PluginReflection(_PLUGIN, "0.1.0", _PLUGIN_SERVICES, [i1, i2, i3])

    i1.call("pico.install_plugin", _PLUGIN, "0.1.0")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[0], "red")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[1], "red")
    i1.call("pico.enable_plugin", _PLUGIN, "0.1.0")

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

    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[0], "blue")

    plugin_ref = plugin_ref.set_topology(
        {i1: _PLUGIN_SERVICES, i2: [_PLUGIN_SERVICES[0]], i3: []}
    )
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())

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

    plugin_ref = PluginReflection(_PLUGIN, "0.1.0", _PLUGIN_SERVICES, [i1, i2, i3])

    i1.call("pico.install_plugin", _PLUGIN, "0.1.0")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[0], "red")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[1], "red")
    i1.call("pico.enable_plugin", _PLUGIN, "0.1.0")

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

    i1.call("pico.service_remove_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[0], "red")

    plugin_ref = plugin_ref.set_topology({i1: [_PLUGIN_SERVICES[1]], i2: [], i3: []})
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())

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

    p1_ref = PluginReflection(_PLUGIN, "0.1.0", _PLUGIN_SERVICES, [i1, i2, i3])
    p2_ref = PluginReflection(
        _PLUGIN_SMALL,
        "0.1.0",
        _PLUGIN_SMALL_SERVICES,
        [i1, i2, i3],
    )

    i1.call("pico.install_plugin", _PLUGIN, "0.1.0")
    i1.call("pico.install_plugin", _PLUGIN_SMALL, "0.1.0")

    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[0], "red")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[1], "red")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[1], "blue")

    i1.call(
        "pico.service_append_tier",
        _PLUGIN_SMALL,
        "0.1.0",
        _PLUGIN_SMALL_SERVICES[0],
        "blue",
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN_SMALL,
        "0.1.0",
        _PLUGIN_SMALL_SERVICES[0],
        "green",
    )

    i1.call("pico.enable_plugin", _PLUGIN, "0.1.0")
    i1.call("pico.enable_plugin", _PLUGIN_SMALL, "0.1.0")

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

    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        "0.1.0",
        _PLUGIN_SERVICES[0],
        "green",
    )
    i1.call(
        "pico.service_remove_tier",
        _PLUGIN_SMALL,
        "0.1.0",
        _PLUGIN_SMALL_SERVICES[0],
        "green",
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

    Retriable(timeout=3, rps=5).call(lambda: p1_ref.assert_synced())
    Retriable(timeout=3, rps=5).call(lambda: p2_ref.assert_synced())


def test_set_topology_with_error_on_start(cluster: Cluster):
    cluster.set_config_file(yaml=cluster_cfg)

    i1 = cluster.add_instance(wait_online=True, tier="red")
    i2 = cluster.add_instance(wait_online=True, tier="blue")

    plugin_ref = PluginReflection(_PLUGIN, "0.1.0", _PLUGIN_SERVICES, [i1, i2])

    i1.call("pico.install_plugin", _PLUGIN, "0.1.0")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[0], "red")
    i1.call("pico.service_append_tier", _PLUGIN, "0.1.0", _PLUGIN_SERVICES[1], "red")
    i1.call("pico.enable_plugin", _PLUGIN, "0.1.0")

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
            "pico.service_append_tier",
            _PLUGIN,
            "0.1.0",
            _PLUGIN_SERVICES[0],
            "blue",
        )

    # assert that topology doesn't change
    plugin_ref.assert_synced()


# -------------------------- RPC SDK tests -------------------------------------


def make_context(override: dict[Any, Any] = {}) -> dict[Any, Any]:
    context = {
        REQUEST_ID: uuid.uuid4(),
        PLUGIN_NAME: _PLUGIN_W_SDK,
        SERVICE_NAME: SERVICE_W_RPC,
        PLUGIN_VERSION: "0.1.0",
        "timeout": 5.0,
    }
    context.update(override)
    return context


def test_plugin_rpc_sdk_basic_errors(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True)

    plugin_name = _PLUGIN_W_SDK
    service_name = SERVICE_W_RPC
    install_and_enable_plugin(i1, plugin_name, [service_name], migrate=True)

    #
    # Check errors in .proc_rpc_dispatch (before handler is handled)
    #
    with pytest.raises(TarantoolError, match="expected 3 arguments"):
        i1.call(".proc_rpc_dispatch")

    with pytest.raises(
        TarantoolError, match=r"first argument \(path\) must be a string"
    ):
        i1.call(".proc_rpc_dispatch", ["not", "string"], b"", {})

    with pytest.raises(
        TarantoolError, match=r"second argument \(input\) must be binary data"
    ):
        i1.call(".proc_rpc_dispatch", "/ping", ["not", "binary"], {})

    with pytest.raises(
        TarantoolError,
        match=r"failed to decode third argument \(context\): expected a map",
    ):
        i1.call(".proc_rpc_dispatch", "/ping", b"", ["not", "map"])

    with pytest.raises(TarantoolError, match="context must contain a request_id"):
        i1.call(".proc_rpc_dispatch", "/ping", b"", {})

    with pytest.raises(TarantoolError, match="no RPC endpoint `[^`]*` is registered"):
        i1.call(".proc_rpc_dispatch", "/unknown-route", b"", make_context())

    # Note: plugin.service is a part of the route, so if service or plugin name
    # is incorrect, the response is there's no handler
    context = make_context({PLUGIN_NAME: "NO_SUCH_PLUGIN"})
    with pytest.raises(TarantoolError, match="no RPC endpoint `[^`]*` is registered"):
        i1.call(".proc_rpc_dispatch", "/ping", b"", context)

    context = make_context({SERVICE_NAME: "NO_SUCH_SERVICE"})
    with pytest.raises(TarantoolError, match="no RPC endpoint `[^`]*` is registered"):
        i1.call(".proc_rpc_dispatch", "/ping", b"", context)

    context = make_context({PLUGIN_VERSION: "0.2.0"})
    with pytest.raises(
        TarantoolError,
        match=r"incompatible version \(requestor: 0.2.0, handler: 0.1.0\)",
    ):
        i1.call(".proc_rpc_dispatch", "/ping", b"", context)


def test_plugin_rpc_sdk_register_endpoint(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True)

    plugin_name = _PLUGIN_W_SDK
    service_name = SERVICE_W_RPC
    install_and_enable_plugin(i1, plugin_name, [service_name], migrate=True)

    input: dict[str, Any]
    #
    # Check errors when registering an RPC endpoint
    #
    with pytest.raises(TarantoolError, match="path must be specified for RPC endpoint"):
        context = make_context()
        input = dict(
            service_info=(plugin_name, service_name, "0.1.0"),
        )
        i1.call(".proc_rpc_dispatch", "/register", msgpack.dumps(input), context)

    with pytest.raises(TarantoolError, match="RPC route path cannot be empty"):
        context = make_context()
        input = dict(
            path="",
            service_info=(plugin_name, service_name, "0.1.0"),
        )
        i1.call(".proc_rpc_dispatch", "/register", msgpack.dumps(input), context)

    with pytest.raises(
        TarantoolError, match="RPC route path must start with '/', got 'bad-path'"
    ):
        context = make_context()
        input = dict(
            path="bad-path",
            service_info=(plugin_name, service_name, "0.1.0"),
        )
        i1.call(".proc_rpc_dispatch", "/register", msgpack.dumps(input), context)

    with pytest.raises(TarantoolError, match="RPC route plugin name cannot be empty"):
        context = make_context()
        input = dict(
            path="/good-path",
            service_info=("", service_name, "0.1.0"),
        )
        i1.call(".proc_rpc_dispatch", "/register", msgpack.dumps(input), context)

    with pytest.raises(
        TarantoolError,
        match="RPC endpoint `[^`]*` is already registered with a different handler",
    ):
        context = make_context()
        input = dict(
            path="/register",
            service_info=(plugin_name, service_name, "0.1.0"),
        )
        i1.call(".proc_rpc_dispatch", "/register", msgpack.dumps(input), context)

    with pytest.raises(
        TarantoolError,
        match="RPC endpoint `[^`]*` is already registered with a different version",
    ):
        context = make_context()
        input = dict(
            path="/register",
            service_info=(plugin_name, service_name, "0.2.0"),
        )
        i1.call(".proc_rpc_dispatch", "/register", msgpack.dumps(input), context)

    # Check all RPC endpoints get unregistered
    i1.call("pico.disable_plugin", plugin_name, "0.1.0")

    with pytest.raises(
        TarantoolError,
        match=f"no RPC endpoint `{plugin_name}.{service_name}/register` is registered",
    ):
        context = make_context()
        i1.call(".proc_rpc_dispatch", "/register", b"", context)

    with pytest.raises(
        TarantoolError,
        match=f"no RPC endpoint `{plugin_name}.{service_name}/ping` is registered",
    ):
        context = make_context()
        i1.call(".proc_rpc_dispatch", "/ping", b"", context)


def test_plugin_rpc_sdk_send_request(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_id="r1", wait_online=False)
    i2 = cluster.add_instance(replicaset_id="r1", wait_online=False)
    i3 = cluster.add_instance(replicaset_id="r2", wait_online=False)
    i4 = cluster.add_instance(replicaset_id="r2", wait_online=False)
    cluster.wait_online()

    def replicaset_master_id(replicaset_id: str) -> str:
        return i1.eval(
            "return box.space._pico_replicaset:get(...).target_master_id", replicaset_id
        )

    def any_bucket_id(instance: Instance) -> str:
        return instance.eval(
            """for _, t in box.space._bucket:pairs() do
                if t.status == 'active' then
                    return t.id
                end
            end"""
        )

    plugin_name = "testplug_sdk"
    service_name = "service_with_rpc_tests"
    install_and_enable_plugin(i1, plugin_name, [service_name], migrate=True)

    # Call simple RPC endpoint, check context is passed correctly
    context = make_context(
        dict(
            foo="bar",
            bool=True,
            int=420,
            array=[1, "two", 3.14],
        )
    )
    output = i1.call(".proc_rpc_dispatch", "/echo-context", b"hello!", context)
    assert msgpack.loads(output) == dict(
        request_id=str(context[REQUEST_ID]),
        plugin_name=context[PLUGIN_NAME],
        service_name=context[SERVICE_NAME],
        plugin_version=context[PLUGIN_VERSION],
        path="/echo-context",
        foo="bar",
        bool=True,
        int=420,
        array=[1, "two", 3.14],
        timeout=5.0,
    )

    input: dict[str, Any]

    # Check calling RPC to a specific instance_id via the plugin SDK
    # Note: /proxy endpoint redirects the request
    context = make_context()
    input = dict(
        path="/ping",
        instance_id=i2.instance_id,
        input="how are you?",
    )
    output = i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    assert msgpack.loads(output) == ["pong", i2.instance_id, b"how are you?"]

    # Check calling RPC to ANY instance via the plugin SDK
    context = make_context()
    input = dict(
        path="/ping",
        input="random-target",
    )
    output = i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    pong, instance_id, echo = msgpack.loads(output)
    assert pong == "pong"
    assert instance_id in [i2.instance_id, i3.instance_id, i4.instance_id]
    assert echo == b"random-target"

    # Check calling RPC to a specific replicaset via the plugin SDK
    context = make_context()
    input = dict(
        path="/ping",
        replicaset_id="r2",
        input="replicaset:any",
    )
    output = i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    pong, instance_id, echo = msgpack.loads(output)
    assert pong == "pong"
    assert instance_id in [i3.instance_id, i4.instance_id]
    assert echo == b"replicaset:any"

    # Check calling RPC to a master of a specific replicaset via the plugin SDK
    context = make_context()
    input = dict(
        path="/ping",
        replicaset_id="r2",
        to_master=True,
        input="replicaset:master",
    )
    output = i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    pong, instance_id, echo = msgpack.loads(output)
    assert pong == "pong"
    assert instance_id == replicaset_master_id("r2")
    assert echo == b"replicaset:master"

    # Make sure buckets are balanced before routing via bucket_id to eliminate
    # flakiness due to bucket rebalancing
    for i in cluster.instances:
        cluster.wait_until_instance_has_this_many_active_buckets(i, 1500)

    # Check calling RPC by bucket_id via the plugin SDK
    context = make_context()
    input = dict(
        path="/ping",
        bucket_id=any_bucket_id(i1),
        input="bucket_id:any",
    )
    output = i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    pong, instance_id, echo = msgpack.loads(output)
    assert pong == "pong"
    assert instance_id == i2.instance_id  # shouldn't call self
    assert echo == b"bucket_id:any"

    # Check calling RPC by bucket_id to master via the plugin SDK
    context = make_context()
    input = dict(
        path="/ping",
        bucket_id=any_bucket_id(i3),
        to_master=True,
        input="bucket_id:master",
    )
    output = i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    pong, instance_id, echo = msgpack.loads(output)
    assert pong == "pong"
    assert instance_id == replicaset_master_id("r2")
    assert echo == b"bucket_id:master"

    # Check calling builtin picodata stored procedures via plugin SDK
    context = make_context()
    input = dict(
        path=".proc_instance_info",
        instance_id="i1",
        input=msgpack.dumps([]),
    )
    output = i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    assert msgpack.loads(output) == i1.call(".proc_instance_info")

    # Check requesting RPC to unknown plugin
    with pytest.raises(
        TarantoolError,
        match=f"service 'NO_SUCH_PLUGIN:0.1.0.{service_name}' is not running on i1",
    ):
        context = make_context()
        input = dict(
            path="/ping",
            instance_id="i1",
            input=msgpack.dumps([]),
            service_info=("NO_SUCH_PLUGIN", service_name, "0.1.0"),
        )
        i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)

    # Check requesting RPC to unknown service
    with pytest.raises(
        TarantoolError,
        match=f"service '{plugin_name}:0.1.0.NO_SUCH_SERVICE' is not running on i1",
    ):
        context = make_context()
        input = dict(
            path="/ping",
            instance_id="i1",
            input=msgpack.dumps([]),
            service_info=(plugin_name, "NO_SUCH_SERVICE", "0.1.0"),
        )
        i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)

    # Check requesting RPC to unknown instance
    with pytest.raises(TarantoolError) as e:
        context = make_context()
        input = dict(
            path="/ping",
            instance_id="NO_SUCH_INSTANCE",
            input=msgpack.dumps([]),
        )
        i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    assert e.value.args[:2] == (
        ErrorCode.NoSuchInstance,
        'instance with instance_id "NO_SUCH_INSTANCE" not found',
    )

    # Check requesting RPC to unknown replicaset
    with pytest.raises(TarantoolError) as e:
        context = make_context()
        input = dict(
            path="/ping",
            replicaset_id="NO_SUCH_REPLICASET",
            input=msgpack.dumps([]),
        )
        i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    assert e.value.args[:2] == (
        ErrorCode.NoSuchReplicaset,
        'replicaset with replicaset_id "NO_SUCH_REPLICASET" not found',
    )

    # Check requesting RPC to unknown bucket id
    with pytest.raises(
        TarantoolError,
        match="Bucket 9999 cannot be found.",
    ):
        context = make_context()
        input = dict(
            path="/ping",
            bucket_id=9999,
            input=msgpack.dumps([]),
        )
        i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)

    # TODO: check calling to poisoned service


def test_sdk_internal(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    install_and_enable_plugin(i1, _PLUGIN_W_SDK, _PLUGIN_W_SDK_SERVICES, migrate=True)

    version_info = i1.call(".proc_version_info")
    PluginReflection.assert_data_eq(i1, "version", version_info["picodata_version"])
    PluginReflection.assert_data_eq(i1, "rpc_version", version_info["rpc_api_version"])

    PluginReflection.assert_data_eq(i1, "instance_id", i1.instance_id)
    PluginReflection.assert_data_eq(i1, "instance_uuid", i1.instance_uuid())
    PluginReflection.assert_data_eq(i1, "replicaset_id", "r1")
    PluginReflection.assert_data_eq(i1, "replicaset_uuid", i1.replicaset_uuid())
    PluginReflection.assert_data_eq(i1, "cluster_id", i1.cluster_id)
    PluginReflection.assert_data_eq(i1, "tier", _DEFAULT_TIER)
    PluginReflection.assert_data_eq(i1, "raft_id", "1")
    PluginReflection.assert_int_data_le(
        i1, "raft_term", i1.eval("return pico.raft_term()")
    )
    PluginReflection.assert_int_data_le(
        i1, "raft_index", i1.eval("return pico.raft_get_index()")
    )

    cas_result_sdk = i1.eval("return box.space.AUTHOR:select()")
    assert cas_result_sdk == [[101, "Alexander Blok"]]


def test_sdk_sql(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    install_and_enable_plugin(
        i1,
        _PLUGIN_W_SDK,
        _PLUGIN_W_SDK_SERVICES,
        migrate=True,
        default_config={"test_type": "sql"},
    )

    sql_result = i1.eval("return box.space.BOOK:select()")
    # remove bucket id and convert datetime to string
    for r in sql_result:
        r.pop(1)
        r[3] = str(r[3])

    assert sql_result == [
        [1, "Ruslan and Ludmila", Decimal("1.1"), "2023-11-11T02:03:19.354210-03:00"]
    ]


def test_sdk_log(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)
    crawler = log_crawler(i1, "TEST MESSAGE")
    install_and_enable_plugin(
        i1,
        _PLUGIN_W_SDK,
        _PLUGIN_W_SDK_SERVICES,
        migrate=True,
        default_config={"test_type": "log"},
    )
    assert crawler.matched


@pytest.mark.webui
def test_sdk_metrics(instance: Instance):
    http_listen = instance.env["PICODATA_HTTP_LISTEN"]
    install_and_enable_plugin(
        instance,
        _PLUGIN_W_SDK,
        _PLUGIN_W_SDK_SERVICES,
        migrate=True,
        default_config={"test_type": "metrics"},
    )

    response = requests.get(f"http://{http_listen}/metrics")
    assert response.ok
    assert "test_metric_1 1" in response.text
    assert "test_metric_2 2" in response.text


def test_sdk_background(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    install_and_enable_plugin(
        i1,
        _PLUGIN_W_SDK,
        _PLUGIN_W_SDK_SERVICES,
        migrate=True,
        default_config={"test_type": "background"},
    )

    # assert that job is working
    Retriable(timeout=5, rps=2).call(
        PluginReflection.assert_persisted_data_exists, "background_job_running", i1
    )

    # assert that job ends after plugin disabled
    i1.call("pico.disable_plugin", _PLUGIN_W_SDK, "0.1.0")

    Retriable(timeout=5, rps=2).call(
        PluginReflection.assert_persisted_data_exists, "background_job_stopped", i1
    )

    # run again
    i1.call("pico.enable_plugin", _PLUGIN_W_SDK, "0.1.0")
    Retriable(timeout=5, rps=2).call(
        PluginReflection.assert_persisted_data_exists, "background_job_running", i1
    )

    # now shutdown 1 and check that job ended
    i1.eval(
        f"pico.update_plugin_config"
        f"('{_PLUGIN_W_SDK}', '0.1.0', '{_PLUGIN_W_SDK_SERVICES[0]}', {{test_type = 'no_test'}})"
    )
    i1.restart()
    i1.wait_online()
    PluginReflection.assert_persisted_data_exists("background_job_stopped", i1)

    PluginReflection.clear_persisted_data(i1)
