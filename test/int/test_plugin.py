from dataclasses import dataclass, field
import time
from typing import Any, Dict, List, Optional
import pytest
import uuid
import msgpack  # type: ignore
import os
import hashlib
from pathlib import Path

from conftest import (
    Cluster,
    ErrorCode,
    ReturnError,
    Retriable,
    Instance,
    ProcessDead,
    TarantoolError,
    log_crawler,
    assert_starts_with,
    copy_plugin_library,
)
from decimal import Decimal
import requests
import signal

_3_SEC = 3
_DEFAULT_CFG = {"foo": True, "bar": 101, "baz": ["one", "two", "three"]}
_NEW_CFG = {"foo": True, "bar": 102, "baz": ["a", "b"]}
_NEW_CFG_2 = {"foo": False, "bar": 102, "baz": ["a", "b"]}

_PLUGIN = "testplug"
_PLUGIN_SERVICES = ["testservice_1", "testservice_2"]
_PLUGIN_SMALL = "testplug_small"
_PLUGIN_SMALL_SERVICES = ["testservice_1"]
_PLUGIN_VERSION_1 = "0.1.0"
_PLUGIN_VERSION_2 = "0.2.0"
_DEFAULT_TIER = "default"
_PLUGIN_WITH_MIGRATION = "testplug_w_migration"
_PLUGIN_WITH_MIGRATION_2 = "testplug_w_migration_2"
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

    # TODO: remove this function. Use `check_plugin_record`,
    # `check_service_records`, etc. instead.
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
            expected_routes = []
            for service in self.topology[i]:
                expected_routes.append(
                    [self.name, self.version, service, i.name, False]
                )

            for neighboring_i in self.topology:
                actual_routes = neighboring_i.sql(
                    """
                    SELECT * FROM "_pico_service_route"
                    WHERE "plugin_name" = ? AND "plugin_version" = ?
                    """,
                    self.name,
                    self.version,
                )
                actual_routes = list(filter(lambda x: x[3] == i.name, actual_routes))
                assert actual_routes == expected_routes

    def assert_data_synced(self):
        for table in self.data:
            data = []

            for i in self.instances:
                if self.data[table] is None:
                    with pytest.raises(TarantoolError, match="attempt to index field"):
                        i.eval(f"return box.space.{table}:select()")
                else:
                    data += i.eval(f"return box.space.{table}:select()")

            def del_bucket_id(row):
                del row[1]
                return row

            data = map(del_bucket_id, data)
            if self.data[table] is not None:
                assert sorted(data) == sorted(self.data[table])

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
        config = dict()
        records = instance.eval(
            "return box.space._pico_plugin_config:select({...})",
            [self.name, self.version, service],
        )
        for record in records:
            config[record[3]] = record[4]
        return config

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

    def assert_in_table_config(self, service, expected_cfg, *instances):
        for i in instances:
            cfg_space = self.get_config(service, i)
            assert cfg_space == expected_cfg

    def assert_route_poisoned(self, poison_instance_name, service, poisoned=True):
        for i in self.instances:
            [[route_poisoned]] = i.sql(
                """
                SELECT "poison" FROM "_pico_service_route"
                WHERE "plugin_name" = ? AND "plugin_version" = ?
                  AND "service_name" = ? AND "instance_name" = ?
                """,
                self.name,
                self.version,
                service,
                poison_instance_name,
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


def check_plugin_record(
    instance: Instance,
    plugin: str,
    version="0.1.0",
    enabled: bool | None = None,
    dropped=False,
):
    assert not (
        dropped and enabled
    ), "plugin cannot be enabled and dropped at the same time"
    rows = instance.sql(
        """
        SELECT enabled FROM _pico_plugin WHERE name = ? AND version = ?
        """,
        plugin,
        version,
    )
    if dropped:
        assert rows == []
    else:
        enabled = enabled or False
        assert rows == [[enabled]]


def check_migration_records(
    instance: Instance,
    plugin: str,
    migrations: list[str],
):
    rows = instance.sql(
        """
        SELECT migration_file FROM _pico_plugin_migration WHERE plugin_name = ?
        """,
        plugin,
    )
    assert set(value for [value] in rows) == set(migrations)


def check_service_records(
    instance: Instance, plugin: str, services: list[str], version="0.1.0"
):
    rows = instance.sql(
        """
        SELECT name FROM _pico_service WHERE plugin_name = ? AND version = ?
        """,
        plugin,
        version,
    )
    assert set(value for [value] in rows) == set(services)


def check_service_route_records(
    instance: Instance,
    plugin: str,
    service: str,
    instance_names: list[str],
    version="0.1.0",
):
    rows = instance.sql(
        """
        SELECT instance_name FROM _pico_service_route
        WHERE plugin_name = ? AND service_name = ? AND plugin_version = ?
        """,
        plugin,
        service,
        version,
    )
    assert set(value for [value] in rows) == set(instance_names)


# ---------------------------------- } Test helper classes ----------------------------------------


def install_and_enable_plugin(
    instance,
    plugin,
    services,
    version="0.1.0",
    migrate=False,
    timeout=3,
    default_config=None,
    if_not_exists=False,
):
    instance.call(
        "pico.install_plugin",
        plugin,
        version,
        {"if_not_exists": if_not_exists},
        timeout=timeout,
    )
    if migrate:
        instance.call(
            "pico.migration_up",
            plugin,
            version,
            timeout=timeout,
        )
    for s in services:
        if default_config is not None:
            for key in default_config:
                # FIXME: this is obviously incorrect and is actually caused by a broken feature
                instance.eval(
                    f"box.space._pico_plugin_config:replace"
                    f"({{'{plugin}', '0.1.0', '{s}', '{key}', ...}})",
                    default_config[key],
                )
        instance.call("pico.service_append_tier", plugin, version, s, _DEFAULT_TIER)
    instance.call("pico.enable_plugin", plugin, version, timeout=timeout)


def init_dummy_plugin(
    cluster: Cluster,
    plugin: str,
    version: str,
    *,
    # NOTE: here we have a default value of a mutable type which could
    # be a source of hard to debug bugs.
    # **DO NOT** modify `services` or `migrations` within this function!
    # See https://florimond.dev/en/posts/2018/08/python-mutable-defaults-are-the-source-of-all-evil  # noqa: E501
    services: list[str] = [],
    migrations: list[str] = [],
    library_name: str = "libtestplug",
) -> Path:
    """Does the following:
    - Setup --share-dir option for the cluster
    - Create plugin directory <share-dir>/<plugin>/<version>
    - Create manifest.yaml in that directory with provided info
    - Copy the <library_name>.so into that directory

    Returns the path to the newly created plugin directory.

    Does *NOT* create migration files, you have to do it yourself.

    """
    cluster.share_dir = cluster.instance_dir
    # Initialize the plugin directory
    plugin_dir = Path(cluster.share_dir) / plugin / version
    os.makedirs(plugin_dir)

    # Copy plugin library
    copy_plugin_library(cluster.binary_path, plugin_dir, library_name)

    # Create manifest
    manifest_path = plugin_dir / "manifest.yaml"
    with open(manifest_path, "w") as f:
        print("description: plugin for test purposes", file=f)
        print(f"name: {plugin}", file=f)
        print(f"version: {version}", file=f)
        print("services:", file=f)
        for service in services:
            print(f"  - name: {service}", file=f)
            print("    description:", file=f)
            print("    default_configuration:", file=f)
        print("migration:", file=f)
        for migration in migrations:
            print(f"  - {migration}", file=f)

    return plugin_dir


def test_invalid_manifest_plugin(cluster: Cluster):
    # This must be set before any instances start up
    cluster.share_dir = cluster.instance_dir

    i1, i2 = cluster.deploy(instance_count=2)

    # try to create non-existent plugin
    with pytest.raises(
        TarantoolError, match="Error while discovering manifest for plugin"
    ):
        i1.sql('CREATE PLUGIN "non-existent" 0.1.0')
    PluginReflection("non-existent", "0.1.0", [], [i1, i2]).assert_synced()

    #
    # Check plugin manifest missing mandatory information
    #
    plugin = "testplug_broken_manifest_1"
    plugin_dir = init_dummy_plugin(cluster, plugin, "0.1.0")
    # Overwrite the manifest with what we want
    (plugin_dir / "manifest.yaml").write_text(
        """
# manifest without a plugin name
description: plugin for test purposes
version: 0.1.0
"""
    )

    with pytest.raises(TarantoolError, match="missing field `name`"):
        i1.sql(f"CREATE PLUGIN {plugin} 0.1.0")
    PluginReflection(plugin, "0.1.0", _PLUGIN_SERVICES, [i1, i2]).assert_synced()

    #
    # Check plugin manifest with invalid default configuration
    #
    plugin = "testplug_broken_manifest_2"
    plugin_dir = init_dummy_plugin(cluster, plugin, "0.1.0")
    # Overwrite the manifest with what we want
    (plugin_dir / "manifest.yaml").write_text(
        f"""
# manifest with invalid `testservice_1` default configuration
description: plugin for test purposes
name: {plugin}
version: 0.1.0
services:
  - name: testservice_1
    description: testservice_1 descr
    default_configuration:
      foo: true
  - name: testservice_2
    description: testservice_2 descr
    default_configuration:
"""
    )
    i1.sql(f"CREATE PLUGIN {plugin} 0.1.0")
    i1.sql(f"ALTER PLUGIN {plugin} 0.1.0 ADD SERVICE testservice_1 TO TIER default")
    i1.sql(f"ALTER PLUGIN {plugin} 0.1.0 ADD SERVICE testservice_2 TO TIER default")
    with pytest.raises(
        TarantoolError, match=f"box error #{ErrorCode.PluginError}: missing field `bar`"
    ):
        i1.sql(f"ALTER PLUGIN {plugin} 0.1.0 ENABLE")
    PluginReflection(
        plugin, "0.1.0", ["testservice_1", "testservice_2"], [i1, i2]
    ).install(True).assert_synced()

    #
    # Check plugin manifest with non-existed extra service
    #
    plugin = "testplug_broken_manifest_3"
    plugin_dir = init_dummy_plugin(cluster, plugin, "0.1.0")
    # Overwrite the manifest with what we want
    (plugin_dir / "manifest.yaml").write_text(
        f"""
# invalid manifest with extra service
description: plugin for test purposes
name: {plugin}
version: 0.1.0
services:
  - name: testservice_1
    description: testservice_1 descr
    default_configuration:
      foo: true
      bar: 101
      baz:
        - "one"
        - "two"
        - "three"
  - name: testservice_2
    description: testservice_2 descr
    default_configuration:
  - name: testservice_0
    description: testservice_0 descr
    default_configuration:
"""
    )
    with pytest.raises(
        TarantoolError,
        match=r'Other: Plugin partial load \(some of services not found: \["testservice_0"\]\)',
    ):
        i1.sql(f"CREATE PLUGIN {plugin} 0.1.0")
    PluginReflection(
        plugin,
        "0.1.0",
        ["testservice_1", "testservice_2", "testservice_3"],
        [i1, i2],
    ).assert_synced()
    PluginReflection.assert_cb_called("testservice_1", "on_start", 0, i1, i2)


def test_plugin_install(cluster: Cluster):
    """
    plugin installation must be full idempotence:
    install non-installed plugin - default behavior
    install already disabled plugin - do nothing
    install already enabled plugin - do nothing
    """

    i1, i2 = cluster.deploy(instance_count=2)
    expected_state = PluginReflection(
        _PLUGIN, _PLUGIN_VERSION_1, _PLUGIN_SERVICES, [i1, i2]
    )

    # check default behaviour
    i1.call("pico.install_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    expected_state = expected_state.install(True)
    expected_state.assert_synced()

    # check install already disabled plugin without if_not_exists opt
    with pytest.raises(ReturnError, match="Plugin `.*` already exists"):
        i1.call("pico.install_plugin", _PLUGIN, "0.1.0")

    # check install already disabled plugin with if_not_exists opt
    i1.call("pico.install_plugin", _PLUGIN, "0.1.0", {"if_not_exists": True})
    expected_state.assert_synced()

    # enable plugin and check installation of already enabled plugin
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        "testservice_1",
        _DEFAULT_TIER,
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        "testservice_2",
        _DEFAULT_TIER,
    )
    i1.call("pico.enable_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    expected_state = expected_state.set_topology(
        {i1: _PLUGIN_SERVICES, i2: _PLUGIN_SERVICES}
    ).enable(True)
    expected_state.assert_synced()

    i1.call("pico.install_plugin", _PLUGIN, "0.1.0", {"if_not_exists": True})
    expected_state.assert_synced()

    # check that installation of another plugin version is ok
    expected_state_v2 = PluginReflection(
        _PLUGIN, _PLUGIN_VERSION_2, _PLUGIN_SERVICES, [i1, i2]
    ).install(True)
    i1.call("pico.install_plugin", _PLUGIN, _PLUGIN_VERSION_2)
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
    with pytest.raises(ReturnError) as e:
        i1.call("pico.enable_plugin", _PLUGIN, "0.1.0")
    assert e.value.args[0] == f"plugin `{_PLUGIN}:0.1.0` is already enabled"

    plugin_ref.assert_synced()
    # assert that `on_start` don't call twice
    plugin_ref.assert_cb_called("testservice_1", "on_start", 1, i1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_start", 1, i1, i2)

    # check that enabling of non-installed plugin return error
    with pytest.raises(ReturnError) as e:
        i1.call("pico.enable_plugin", _PLUGIN_SMALL, "0.1.0")
    assert e.value.args[0] == f"Plugin `{_PLUGIN_SMALL}:0.1.0` not found at instance"

    # check that enabling of plugin with another version return error
    i1.call("pico.install_plugin", _PLUGIN, "0.2.0")
    with pytest.raises(ReturnError) as e:
        i1.call("pico.enable_plugin", _PLUGIN, "0.2.0")
    assert (
        e.value.args[0]
        == f"plugin `{_PLUGIN}:0.2.0` is already enabled with a different version 0.1.0"
    )

    # check that enabling a plugin with unapplied migrations fails
    i1.call("pico.install_plugin", _PLUGIN_WITH_MIGRATION, "0.1.0")
    with pytest.raises(ReturnError) as e:
        i1.call("pico.enable_plugin", _PLUGIN_WITH_MIGRATION, "0.1.0")
    assert (
        e.value.args[0]
        == f"cannot enable plugin `{_PLUGIN_WITH_MIGRATION}:0.1.0`: need to apply migrations first (applied 0/2)"  # noqa: E501
    )


def test_plugin_disable_ok(cluster: Cluster):
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
        i1.call("pico.disable_plugin", _PLUGIN, _PLUGIN_VERSION_2)

    i1.call("pico.disable_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    plugin_ref = plugin_ref.enable(False).set_topology({i1: [], i2: []})
    plugin_ref.assert_synced()

    # retrying, cause routing table update asynchronously
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())
    # assert that `on_stop` callbacks successfully called
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 1, i1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_stop", 1, i1, i2)

    # check disabling of already disabled plugin
    i1.call("pico.disable_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    plugin_ref.assert_synced()
    # assert that `on_stop` callbacks don't call twice
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 1, i1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_stop", 1, i1, i2)

    # check that disabling of non-installed plugin return error
    with pytest.raises(
        ReturnError, match="Plugin `testplug_small:0.1.0` not found at instance"
    ):
        i1.call("pico.disable_plugin", _PLUGIN_SMALL, _PLUGIN_VERSION_1)


def test_drop_plugin_basics(cluster: Cluster):
    """
    plugin removing behaviour:
    removing of disabling plugin - default behavior
    removing of non-installed plugin - do nothing
    removing of already enabled plugin - error occurred
    """

    plugin = _PLUGIN
    [service_1, service_2] = _PLUGIN_SERVICES

    i1, i2 = cluster.deploy(instance_count=2)

    #
    # Create and enable the plugin
    #
    i1.sql(f"CREATE PLUGIN {plugin} 0.1.0")
    i1.sql(f"ALTER PLUGIN {plugin} 0.1.0 ADD SERVICE {service_1} TO TIER default")
    i1.sql(f"ALTER PLUGIN {plugin} 0.1.0 ADD SERVICE {service_2} TO TIER default")
    i1.sql(f"ALTER PLUGIN {plugin} 0.1.0 ENABLE")

    #
    # Check everything worked properly
    #
    check_plugin_record(i1, plugin, enabled=True)
    check_service_records(i1, plugin, services=[service_1, service_2])
    check_service_route_records(i1, plugin, service_1, [i1.name, i2.name])  # type: ignore
    check_service_route_records(i1, plugin, service_2, [i1.name, i2.name])  # type: ignore

    # Check that removing enabled plugin doesn't work
    with pytest.raises(TarantoolError) as e:
        i1.sql(f"DROP PLUGIN {plugin} 0.1.0")
    assert e.value.args[:2] == (
        ErrorCode.PluginError,
        f"attempt to drop an enabled plugin '{plugin}'",
    )

    # check default behaviour
    i1.sql(f"ALTER PLUGIN {plugin} 0.1.0 DISABLE")
    check_plugin_record(i1, plugin, enabled=False)
    check_service_records(i1, plugin, services=[service_1, service_2])
    # retries needed because route records are updated asynchronously
    Retriable().call(lambda: check_service_route_records(i1, plugin, service_1, []))
    Retriable().call(lambda: check_service_route_records(i1, plugin, service_2, []))

    # create one more plugin version
    i1.sql(f"CREATE PLUGIN {plugin} 0.2.0")
    check_plugin_record(i1, plugin, version="0.2.0", enabled=False)
    check_service_records(i1, plugin, version="0.2.0", services=[service_1, service_2])
    # services not running on any instances
    check_service_route_records(i1, plugin, service_1, [], version="0.2.0")
    check_service_route_records(i1, plugin, service_2, [], version="0.2.0")

    # drop old version
    i1.sql(f"DROP PLUGIN {plugin} 0.1.0")
    check_plugin_record(i1, plugin, version="0.1.0", dropped=True)

    # check dropping non existent plugin (previously existed)
    with pytest.raises(TarantoolError) as e:
        i1.sql(f"DROP PLUGIN {plugin} 0.1.0")
    assert e.value.args[:2] == (
        ErrorCode.PluginError,
        f"no such plugin `{plugin}:0.1.0`",
    )

    # check dropping non existent plugin (never existed)
    with pytest.raises(TarantoolError) as e:
        i1.sql(f"DROP PLUGIN {plugin} 0.69.0")
    assert e.value.args[:2] == (
        ErrorCode.PluginError,
        f"no such plugin `{plugin}:0.69.0`",
    )

    # drop last version of plugin
    i1.sql(f"DROP PLUGIN {plugin} 0.2.0")
    check_plugin_record(i1, plugin, version="0.2.0", dropped=True)


def test_drop_plugin_with_or_without_data(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    plugin = _PLUGIN_WITH_MIGRATION

    # Create a plugin
    i1.sql(f"CREATE PLUGIN {plugin} 0.1.0")
    check_plugin_record(i1, plugin, enabled=False)
    # Migrations not applied yet
    check_migration_records(i1, plugin, [])

    # Apply migrations
    i1.sql(f"ALTER PLUGIN {plugin} MIGRATE TO 0.1.0")
    check_migration_records(i1, plugin, ["author.db", "book.db"])
    assert i1.sql("SELECT id FROM _pico_table WHERE name = 'author'") != []
    assert i1.sql("SELECT id FROM _pico_table WHERE name = 'book'") != []

    # Dropping plugin leaving the applied migrations to be
    i1.sql(f"DROP PLUGIN {plugin} 0.1.0")
    # NOTE: we now have references to a plugin in our global storage, but no
    # record of that plugin ever existing, and there's not reference from the
    # actual entities (tables, etc.) created by migrations that the entities are
    # owned by this no-existent plugin. But it is what it is...
    check_plugin_record(i1, plugin, dropped=True)
    check_migration_records(i1, plugin, ["author.db", "book.db"])
    assert i1.sql("SELECT id FROM _pico_table WHERE name = 'author'") != []
    assert i1.sql("SELECT id FROM _pico_table WHERE name = 'book'") != []

    # NOTE: there's no way to drop the plugin's data without dropping the plugin itself

    # Create the plugin back so that we can drop the data
    i1.sql(f"CREATE PLUGIN {plugin} 0.1.0")
    check_plugin_record(i1, plugin, enabled=False)
    i1.sql(f"DROP PLUGIN {plugin} 0.1.0 WITH DATA")
    check_plugin_record(i1, plugin, dropped=True)
    check_migration_records(i1, plugin, [])
    assert i1.sql("SELECT id FROM _pico_table WHERE name = 'author'") == []
    assert i1.sql("SELECT id FROM _pico_table WHERE name = 'book'") == []


def test_two_plugin_install_and_enable(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    p1_ref = PluginReflection.default(i1, i2)
    p2_ref = PluginReflection(
        _PLUGIN_SMALL,
        _PLUGIN_VERSION_1,
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


def test_plugin_disable_error_on_stop(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    plugin_ref.inject_error("testservice_1", "on_stop", True, i2)
    plugin_ref.inject_error("testservice_2", "on_stop", True, i2)

    i1.call("pico.disable_plugin", _PLUGIN, _PLUGIN_VERSION_1, timeout=_3_SEC)
    # retrying, cause routing table update asynchronously
    plugin_ref = plugin_ref.enable(False).set_topology({i1: [], i2: []})
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())

    i1.call("pico.remove_plugin", _PLUGIN, _PLUGIN_VERSION_1, timeout=_3_SEC)
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
    plugin_ref = PluginReflection(
        _PLUGIN, _PLUGIN_VERSION_1, _PLUGIN_SERVICES, [i1, i2]
    )

    # inject error into second instance
    plugin_ref.inject_error("testservice_1", "on_start", True, i2)

    i1.sql(f"CREATE PLUGIN {_PLUGIN} 0.1.0")
    for service in _PLUGIN_SERVICES:
        i1.sql(
            f"ALTER PLUGIN {_PLUGIN} 0.1.0 ADD SERVICE {service} TO TIER {_DEFAULT_TIER}"
        )

    # assert that plugin not loaded and on_stop called on both instances
    with pytest.raises(TarantoolError) as e:
        i1.sql(f"ALTER PLUGIN {_PLUGIN} 0.1.0 ENABLE")
    assert e.value.args[:-1] == (
        ErrorCode.Other,
        f"Failed to enable plugin `{_PLUGIN}:0.1.0`: [instance name:default_2_1] Other: Callback: on_start: box error #{ErrorCode.PluginError}: error at `on_start`",  # noqa: E501
    )

    # plugin installed but disabled
    plugin_ref = plugin_ref.install(True).set_topology({i1: [], i2: []})
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 1, i1, i2)

    # inject error into both instances
    plugin_ref.inject_error("testservice_1", "on_start", True, i1)

    # This is needed to check that raft log compaction respects plugin op finalizers
    i1.sql(""" ALTER SYSTEM SET raft_wal_count_max = 1 """)
    assert i1.call("box.space._raft_log:len") == 0
    index_before = i1.raft_get_index()

    # assert that plugin not loaded and on_stop called on both instances
    with pytest.raises(
        TarantoolError,
        match=f"] Other: Callback: on_start: box error #{ErrorCode.PluginError}: error at `on_start`",  # noqa: E501
    ):
        i1.sql(f"ALTER PLUGIN {_PLUGIN} 0.1.0 ENABLE")

    # Display raft log in case the assertions bellow fail
    print(str.join("\n", i1.call("pico.raft_log", dict(max_width=120))))

    # The ALTER PLUGIN operation added 2 entries
    assert i1.raft_get_index() == index_before + 2
    # And only one of the got compacted, because the other one is the finalizer
    # which is needed to report that error message we just checked above
    assert i1.call("box.space._raft_log:len") == 1

    # plugin installed but disabled
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 2, i1, i2)

    # remove errors
    plugin_ref.inject_error("testservice_1", "on_start", False, i1)
    plugin_ref.inject_error("testservice_1", "on_start", False, i2)

    # assert plugin loaded now
    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES, if_not_exists=True)
    plugin_ref = plugin_ref.enable(True).set_topology(
        {i1: _PLUGIN_SERVICES, i2: _PLUGIN_SERVICES}
    )
    plugin_ref.assert_synced()


def test_plugin_not_enable_if_on_start_timeout(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection(
        _PLUGIN, _PLUGIN_VERSION_1, _PLUGIN_SERVICES, [i1, i2]
    )

    # inject timeout into second instance
    plugin_ref.inject_error("testservice_1", "on_start_sleep_sec", 3, i2)

    i1.call("pico.install_plugin", _PLUGIN, "0.1.0")
    i1.call(
        "pico.service_append_tier", _PLUGIN, "0.1.0", "testservice_1", _DEFAULT_TIER
    )
    i1.call(
        "pico.service_append_tier", _PLUGIN, "0.1.0", "testservice_2", _DEFAULT_TIER
    )
    with pytest.raises(ReturnError, match="] Timeout: no response"):
        i1.call(
            "pico.enable_plugin",
            _PLUGIN,
            _PLUGIN_VERSION_1,
            {"on_start_timeout": 2},
            timeout=4,
        )
    # need to wait until sleep at i2 called asynchronously
    time.sleep(2)

    # assert that plugin installed, disabled and on_stop called on both instances
    plugin_ref = plugin_ref.install(True).set_topology({i1: [], i2: []})
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 1, i1, i2)

    # inject timeout into both instances
    plugin_ref.inject_error("testservice_1", "on_start_sleep_sec", 3, i1)

    with pytest.raises(ReturnError, match="] Timeout: no response"):
        i1.call(
            "pico.enable_plugin",
            _PLUGIN,
            _PLUGIN_VERSION_1,
            {"on_start_timeout": 2},
            timeout=4,
        )
    # need to wait until sleep at i1 and i2 called asynchronously
    time.sleep(2)

    # assert that plugin installed, disabled and on_stop called on both instances
    Retriable(timeout=3, rps=5).call(lambda: plugin_ref.assert_synced())
    plugin_ref.assert_cb_called("testservice_1", "on_stop", 2, i1, i2)


# -------------------------- migration tests -------------------------------------


_DATA_V_0_1_0 = {
    "author": [
        [1, "Alexander Pushkin"],
        [2, "Alexander Blok"],
    ],
    "book": [
        [1, "Ruslan and Ludmila"],
        [2, "The Tale of Tsar Saltan"],
        [3, "The Twelve"],
        [4, "The Lady Unknown"],
    ],
}

_DATA_V_0_2_0 = {
    "author": [
        [1, "Alexander Pushkin"],
        [2, "Alexander Blok"],
    ],
    "book": [
        [1, "Ruslan and Ludmila"],
        [2, "The Tale of Tsar Saltan"],
        [3, "The Twelve"],
        [4, "The Lady Unknown"],
    ],
    "store": [
        [1, "OZON"],
        [2, "Yandex"],
        [3, "Wildberries"],
    ],
    "manager": [
        [1, "Manager 1", 1],
        [2, "Manager 2", 1],
        [3, "Manager 3", 2],
    ],
}

_NO_DATA_V_0_1_0: dict[str, Optional[list[Any]]] = {
    "author": None,
    "book": None,
}

_NO_DATA_V_0_2_0: dict[str, None] = {
    "author": None,
    "book": None,
    "store": None,
    "manager": None,
}


def test_migration_separate_command(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    expected_state = PluginReflection.default(i1, i2)

    i1.call("pico.install_plugin", _PLUGIN_WITH_MIGRATION, _PLUGIN_VERSION_1, timeout=5)
    i1.call("pico.migration_up", _PLUGIN_WITH_MIGRATION, _PLUGIN_VERSION_1)
    expected_state = expected_state.set_data(_DATA_V_0_1_0)
    expected_state.assert_data_synced()

    # check migration file checksums are calculated correctly
    rows = i1.sql(""" SELECT "migration_file", "hash" FROM "_pico_plugin_migration" """)
    assert i1.share_dir
    plugin_dir = os.path.join(i1.share_dir, _PLUGIN_WITH_MIGRATION, "0.1.0")
    for filename, checksum in rows:
        fullpath = os.path.join(plugin_dir, filename)
        with open(fullpath, "rb") as f:
            hash = hashlib.md5(f.read())
        assert checksum == hash.hexdigest(), filename

    # This will do separate checks of applied migrations
    i1.call("pico.enable_plugin", _PLUGIN_WITH_MIGRATION, "0.1.0", timeout=5)

    # increase a version to v0.2.0
    i1.call("pico.install_plugin", _PLUGIN_WITH_MIGRATION, _PLUGIN_VERSION_2, timeout=5)
    i1.call("pico.migration_up", _PLUGIN_WITH_MIGRATION, _PLUGIN_VERSION_2)
    expected_state = expected_state.set_data(_DATA_V_0_2_0)
    expected_state.assert_data_synced()

    # now down from v0.2.0
    i1.call("pico.migration_down", _PLUGIN_WITH_MIGRATION, _PLUGIN_VERSION_2)
    i1.call("pico.remove_plugin", _PLUGIN_WITH_MIGRATION, _PLUGIN_VERSION_2, timeout=5)
    expected_state = expected_state.set_data(_NO_DATA_V_0_2_0)
    expected_state.assert_data_synced()


def test_migration_for_changed_migration(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    expected_state = PluginReflection.default(i1, i2)

    i1.call("pico.install_plugin", _PLUGIN_WITH_MIGRATION, _PLUGIN_VERSION_1, timeout=5)
    i1.call("pico.migration_up", _PLUGIN_WITH_MIGRATION, _PLUGIN_VERSION_1)
    expected_state = expected_state.set_data(_DATA_V_0_1_0)
    expected_state.assert_data_synced()

    # increase the version to v0.2.0_broken with changed file author.db
    i1.call("pico.install_plugin", _PLUGIN_WITH_MIGRATION, "0.2.0_broken", timeout=5)

    error_regex = "inconsistent with previous version migration list, "
    r"reason: unknown migration files found in manifest migrations "
    r"\(mismatched file meta information for book\.db\)"
    with pytest.raises(ReturnError, match=error_regex):
        i1.call("pico.migration_up", _PLUGIN_WITH_MIGRATION, "0.2.0_broken")


def test_migration_apply_err(cluster: Cluster):
    plugin_name = "plugin_for_test_migration_apply_err"

    #
    # Prepare plugin
    #
    plugin_dir = init_dummy_plugin(
        cluster, plugin_name, "0.1.0", migrations=["good.db", "bad.db"]
    )

    (plugin_dir / "good.db").write_text(
        """
-- pico.UP
CREATE TABLE "stuff" (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL) USING memtx DISTRIBUTED BY (id);

-- pico.DOWN
DROP TABLE "stuff";
""",  # noqa: E501
    )

    (plugin_dir / "bad.db").write_text(
        """
-- pico.UP
CREATE DATABASE everything;

-- pico.DOWN
DROP DATABASE everything;
""",
    )

    #
    # Start instance and check
    #
    [i1] = cluster.deploy(instance_count=1)

    i1.call("pico.install_plugin", plugin_name, "0.1.0", timeout=5)
    with pytest.raises(ReturnError) as e:
        i1.call("pico.migration_up", plugin_name, "0.1.0", timeout=5)
    assert_starts_with(
        e.value.args[0],
        "Failed to apply `UP` command (file: bad.db) `CREATE DATABASE everything;`",
    )

    # The good migration was rolled back (good.db:DOWN was applied)
    rows = i1.sql(""" SELECT * FROM "_pico_table" WHERE "name" = 'stuff' """)
    assert rows == []


def test_migration_next_version_apply_err(cluster: Cluster):
    plugin_name = "plugin_for_test_migration_next_version_apply_err"

    # Prepare plugin
    plugin_dir = init_dummy_plugin(
        cluster, plugin_name, "0.1.0", migrations=["../good.db"]
    )
    init_dummy_plugin(
        cluster,
        plugin_name,
        "0.2.0",
        migrations=["../good.db", "../good_v2.db", "../bad.db"],
    )

    (plugin_dir / "../good.db").write_text(
        """
-- pico.UP
CREATE TABLE "stuff" (id INTEGER NOT NULL PRIMARY KEY) USING memtx DISTRIBUTED BY (id);

-- pico.DOWN
DROP TABLE "stuff";
""",  # noqa: E501
    )

    (plugin_dir / "../good_v2.db").write_text(
        """
-- pico.UP
CREATE TABLE "should_not_exist" (id INTEGER NOT NULL PRIMARY KEY) USING memtx DISTRIBUTED BY (id);

-- pico.DOWN
DROP TABLE "should_not_exist";
""",  # noqa: E501
    )

    (plugin_dir / "../bad.db").write_text(
        """
-- pico.UP
CREATE TABLE "also_should_not_exist" (id INTEGER NOT NULL PRIMARY KEY) USING memtx DISTRIBUTED BY (id);
CREATE DATABASE everything;

-- pico.DOWN
DROP TABLE "also_should_not_exist";
DROP DATABASE everything;
""",  # noqa: E501
    )

    #
    # Start instance and check
    #
    [i1] = cluster.deploy(instance_count=1)

    # successfully install v0.1.0
    i1.call("pico.install_plugin", plugin_name, "0.1.0", timeout=5)
    i1.call("pico.migration_up", plugin_name, "0.1.0", timeout=5)

    i1.call("pico.install_plugin", plugin_name, "0.2.0", timeout=5)
    # expect rollback to 0.1.0 migrations
    with pytest.raises(ReturnError) as e:
        i1.call("pico.migration_up", plugin_name, "0.2.0", timeout=5)
    assert_starts_with(
        e.value.args[0],
        "Failed to apply `UP` command (file: ../bad.db) `CREATE DATABASE everything;`",
    )

    # The good migration is still applied, as we rolled back to schema v0.1.0
    rows = i1.sql(""" SELECT "name" FROM "_pico_table" WHERE "name" = 'stuff' """)
    assert rows == [["stuff"]]

    # The good_v2 migration is rolled back
    rows = i1.sql(
        """ SELECT "name" FROM "_pico_table" WHERE "name" = 'should_not_exist' """
    )
    assert rows == []

    # The bad migration is also rolled back
    rows = i1.sql(
        """ SELECT "name" FROM "_pico_table" WHERE "name" = 'also_should_not_exist' """
    )
    assert rows == []


def test_migration_lock(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True)
    i2 = cluster.add_instance(wait_online=False, replicaset_name="storage")
    i3 = cluster.add_instance(wait_online=False, replicaset_name="storage")
    cluster.wait_online()

    # Decrease governor_auto_offline_timeout so that sentinel notices that the instance
    # disappeared quicker
    i1.sql(""" ALTER SYSTEM SET governor_auto_offline_timeout = 1 """)

    # successfully install v0.1.0
    i2.call(
        "pico.install_plugin",
        _PLUGIN_WITH_MIGRATION_2,
        "0.1.0",
        timeout=5,
    )

    i2.call("pico._inject_error", "PLUGIN_MIGRATION_LONG_MIGRATION", True)
    i2.eval(
        """
            local fiber = require('fiber')
            function migrate()
                local res = {pico.migration_up('testplug_w_migration_2', '0.1.0', {timeout = 20})}
                rawset(_G, "migration_up_result", res)
            end
            fiber.create(migrate)
    """
    )
    time.sleep(1)

    with pytest.raises(ReturnError, match="Migration lock is already acquired"):
        i3.call(
            "pico.migration_up",
            _PLUGIN_WITH_MIGRATION_2,
            "0.1.0",
            timeout=10,
        )

    #
    # i2 suddenly stops responding before it has finished applying migrations
    #
    assert i2.process
    os.killpg(i2.process.pid, signal.SIGSTOP)

    def check_instance_is_offline(peer: Instance, instance_name):
        instance_info = peer.call(".proc_instance_info", instance_name)
        assert instance_info["current_state"]["variant"] == "Offline"
        assert instance_info["target_state"]["variant"] == "Offline"

    # sentinel has noticed that i2 is offline and changed it's state
    Retriable(timeout=10).call(check_instance_is_offline, i1, i2.name)

    #
    # i3 can now apply the migrations, because the lock holder is not online
    #
    i3.call("pico.migration_up", _PLUGIN_WITH_MIGRATION_2, "0.1.0", timeout=10)

    #
    # i2 wakes up and attempts to continue with applying the migrations
    #
    os.killpg(i2.process.pid, signal.SIGCONT)
    i2.call("pico._inject_error", "PLUGIN_MIGRATION_LONG_MIGRATION", False)

    def check_migration_up_result(instance: Instance):
        result = instance.eval("return migration_up_result")
        assert result is not None
        return result

    # i2 notices that the lock was forcefully taken away
    ok, err = Retriable(timeout=10).call(check_migration_up_result, i2)
    assert ok is None
    assert err == "Migration lock is already released"


# -------------------------- configuration tests -------------------------------------


def test_config_validation(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    # test custom validator
    plugin_ref.inject_error("testservice_1", "on_config_validate", "test error", i1)
    with pytest.raises(
        TarantoolError, match="New configuration validation error:.* test error"
    ):
        i1.sql(
            f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} SET"
            f"    {_PLUGIN_SERVICES[0]}.foo = 'true',"
            f"    {_PLUGIN_SERVICES[0]}.bar = '102',"
            f'    {_PLUGIN_SERVICES[0]}.baz = \'["a", "b"]\''
        )


def test_on_config_update(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    i1.call("pico.install_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    plugin_ref.assert_in_table_config(_PLUGIN_SERVICES[0], _DEFAULT_CFG, i1, i2)

    # change configuration of non-enabled plugin
    i1.sql(
        f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} SET"
        f"    {_PLUGIN_SERVICES[0]}.foo = 'true',"
        f"    {_PLUGIN_SERVICES[0]}.bar = '102',"
        f'    {_PLUGIN_SERVICES[0]}.baz = \'["a", "b"]\''
    )
    # retrying, cause new service configuration callback call asynchronously
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_in_table_config(_PLUGIN_SERVICES[0], _NEW_CFG, i1, i2)
    )

    # change configuration of enabled plugin
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        _DEFAULT_TIER,
    )
    i1.call("pico.enable_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    i1.sql(
        f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} SET"
        f"    {_PLUGIN_SERVICES[0]}.foo = 'false',"
        f"    {_PLUGIN_SERVICES[0]}.bar = '102',"
        f'    {_PLUGIN_SERVICES[0]}.baz = \'["a", "b"]\''
    )
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_config(_PLUGIN_SERVICES[0], _NEW_CFG_2, i1, i2)
    )


def test_plugin_double_config_update(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    plugin_ref.inject_error(_PLUGIN_SERVICES[0], "assert_config_changed", True, i1)
    plugin_ref.inject_error(_PLUGIN_SERVICES[0], "assert_config_changed", True, i2)

    i1.sql(
        f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} SET"
        f"    {_PLUGIN_SERVICES[0]}.foo = 'true',"
        f"    {_PLUGIN_SERVICES[0]}.bar = '102',"
        f'    {_PLUGIN_SERVICES[0]}.baz = \'["a", "b"]\''
    )
    i1.sql(
        f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} SET"
        f"    {_PLUGIN_SERVICES[0]}.foo = 'false',"
        f"    {_PLUGIN_SERVICES[0]}.bar = '102',"
        f'    {_PLUGIN_SERVICES[0]}.baz = \'["a", "b"]\''
    )
    # both configs were applied
    # retrying, cause callback call asynchronously
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_cb_called(
            _PLUGIN_SERVICES[0], "on_config_change", 2, i1, i2
        )
    )
    plugin_ref.assert_config(_PLUGIN_SERVICES[0], _NEW_CFG_2, i1, i2)

    i1.sql(
        f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} SET"
        f"    {_PLUGIN_SERVICES[0]}.foo = 'true',"
        f"    {_PLUGIN_SERVICES[0]}.bar = '102',"
        f'    {_PLUGIN_SERVICES[0]}.baz = \'["a", "b"]\''
    )
    i2.sql(
        f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} SET"
        f"    {_PLUGIN_SERVICES[0]}.foo = 'true',"
        f"    {_PLUGIN_SERVICES[0]}.bar = '102',"
        f'    {_PLUGIN_SERVICES[0]}.baz = \'["a", "b"]\''
    )

    # both configs were applied and result config may be any of applied
    # retrying, cause callback call asynchronously
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_cb_called(
            _PLUGIN_SERVICES[0], "on_config_change", 4, i1, i2
        )
    )


def test_error_on_config_update(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    plugin_ref.assert_config(_PLUGIN_SERVICES[0], _DEFAULT_CFG, i1, i2)

    plugin_ref.inject_error(_PLUGIN_SERVICES[0], "on_config_change", "test error", i1)

    i1.sql(
        f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} SET"
        f"    {_PLUGIN_SERVICES[0]}.foo = 'true',"
        f"    {_PLUGIN_SERVICES[0]}.bar = '102',"
        f'    {_PLUGIN_SERVICES[0]}.baz = \'["a", "b"]\''
    )

    # check that at i1 new configuration exists in global space
    # but not really applied to service because error
    cfg_space = plugin_ref.get_config(_PLUGIN_SERVICES[0], i1)
    assert cfg_space == _NEW_CFG
    cfg_seen = plugin_ref.get_seen_config(_PLUGIN_SERVICES[0], i1)
    assert cfg_seen == _DEFAULT_CFG
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_config(_PLUGIN_SERVICES[0], _NEW_CFG, i2)
    )

    # assert that the first instance now has a poison service
    # and the second instance is not poisoned
    # retrying, cause routing table update asynchronously
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_route_poisoned(i1.name, _PLUGIN_SERVICES[0])
    )
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_route_poisoned(
            i2.name, _PLUGIN_SERVICES[0], poisoned=False
        )
    )


def test_instance_service_poison_and_healthy_then(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    install_and_enable_plugin(i1, _PLUGIN, _PLUGIN_SERVICES)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    plugin_ref.assert_config(_PLUGIN_SERVICES[0], _DEFAULT_CFG, i1, i2)
    plugin_ref.inject_error(_PLUGIN_SERVICES[0], "on_config_change", "test error", i1)

    i1.sql(
        f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} SET"
        f"    {_PLUGIN_SERVICES[0]}.foo = 'true',"
        f"    {_PLUGIN_SERVICES[0]}.bar = '102',"
        f'    {_PLUGIN_SERVICES[0]}.baz = \'["a", "b"]\''
    )

    # assert that the first instance now has a poison service
    # retrying, cause routing table update asynchronously
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_route_poisoned(i1.name, _PLUGIN_SERVICES[0])
    )

    plugin_ref.remove_error(_PLUGIN_SERVICES[0], "on_config_change", i1)

    i1.sql(
        f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} SET"
        f"    {_PLUGIN_SERVICES[0]}.foo = 'false',"
        f"    {_PLUGIN_SERVICES[0]}.bar = '102',"
        f'    {_PLUGIN_SERVICES[0]}.baz = \'["a", "b"]\''
    )

    # retrying, cause routing table update asynchronously
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_route_poisoned(
            i1.name, _PLUGIN_SERVICES[0], poisoned=False
        )
    )
    plugin_ref.assert_config(_PLUGIN_SERVICES[0], _NEW_CFG_2, i1, i2)


# -------------------------- leader change test -----------------------------------


def test_on_leader_change(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_name="r1", wait_online=True)
    i2 = cluster.add_instance(replicaset_name="r1", wait_online=True)
    i3 = cluster.add_instance(replicaset_name="r1", wait_online=True)

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
        ops=[("=", "target_master_name", i2.name)],
    )
    cluster.raft_wait_index(index)
    i1.wait_governor_status("idle")
    assert i1.replicaset_master_name() == i2.name

    # on_leader_change called at i1 and i2
    # because this is previous and new leader, and not called at i3
    plugin_ref.assert_cb_called("testservice_1", "on_leader_change", 1, i1, i2)
    plugin_ref.assert_cb_called("testservice_1", "on_leader_change", 0, i3)

    # i1 and i3 known that they are not a leader; i2 know that he is a leader
    plugin_ref.assert_last_seen_ctx("testservice_1", {"is_master": False}, i1, i3)
    plugin_ref.assert_last_seen_ctx("testservice_1", {"is_master": True}, i2)


def test_error_on_leader_change(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_name="r1", wait_online=True)
    i2 = cluster.add_instance(replicaset_name="r1", wait_online=True)

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
        ops=[("=", "target_master_name", i2.name)],
    )
    cluster.raft_wait_index(index)
    assert i1.replicaset_master_name() == i2.name

    plugin_ref.assert_last_seen_ctx("testservice_1", {"is_master": True}, i2)

    # assert that the first instance now has a poison service
    # and the second instance is not poisoned
    plugin_ref.assert_route_poisoned(i1.name, "testservice_1")
    plugin_ref.assert_route_poisoned(i2.name, "testservice_1", poisoned=False)


def _test_plugin_install_and_enable_on_catchup(
    cluster: Cluster, compact_raft_log: bool
):
    i1, i2, i3, i4 = cluster.deploy(instance_count=4)
    p1_ref = PluginReflection.default(i1, i2, i3, i4)
    p2_ref = PluginReflection(
        _PLUGIN_SMALL,
        _PLUGIN_VERSION_1,
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
        _PLUGIN_VERSION_1,
        p3_svc,
        instances=[i1, i2, i3, i4],
        topology={i1: p3_svc, i2: p3_svc, i3: p3_svc, i4: p3_svc},
    )
    install_and_enable_plugin(i1, p3, p3_svc)
    p3_ref = p3_ref.install(True).enable(True)

    # update first plugin config
    i1.sql(
        f"ALTER PLUGIN {_PLUGIN} {_PLUGIN_VERSION_1} SET"
        f"    {_PLUGIN_SERVICES[0]}.foo = 'true',"
        f"    {_PLUGIN_SERVICES[0]}.bar = '102',"
        f'    {_PLUGIN_SERVICES[0]}.baz = \'["a", "b"]\''
    )

    # disable second plugin
    i1.call("pico.disable_plugin", _PLUGIN_SMALL, _PLUGIN_VERSION_1)
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


def test_plugin_install_and_enable_on_catchup_by_log(cluster: Cluster):
    _test_plugin_install_and_enable_on_catchup(cluster, compact_raft_log=False)


def test_plugin_install_and_enable_on_catchup_by_snapshot(cluster: Cluster):
    _test_plugin_install_and_enable_on_catchup(cluster, compact_raft_log=True)


# -------------------------- topology tests -------------------------------------


def test_set_topology(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection(
        _PLUGIN, _PLUGIN_VERSION_1, _PLUGIN_SERVICES, [i1, i2]
    )

    i1.call("pico.install_plugin", _PLUGIN, "0.1.0")

    # set topology to non-existent plugin is forbidden
    with pytest.raises(
        ReturnError,
        match="Plugin `non-existent:0.1.0` not found at instance",
    ):
        i1.call(
            "pico.service_append_tier",
            "non-existent",
            _PLUGIN_VERSION_1,
            _PLUGIN_SERVICES[0],
            _DEFAULT_TIER,
        )

    # set topology to non-existent plugin service is forbidden
    with pytest.raises(
        ReturnError,
        match="Service `non-existent` for plugin `testplug:0.1.0` not found at instance",
    ):
        i1.call(
            "pico.service_append_tier",
            _PLUGIN,
            _PLUGIN_VERSION_1,
            "non-existent",
            _DEFAULT_TIER,
        )

    # set non-existent tier to first plugin service,
    # and don't set any tier for second plugin service;
    # both services must never be started
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        "non-existent",
    )
    i1.call("pico.enable_plugin", _PLUGIN, _PLUGIN_VERSION_1)

    plugin_ref = plugin_ref.install(True).enable(True).set_topology({i1: [], i2: []})
    plugin_ref.assert_synced()

    plugin_ref.assert_cb_called("testservice_1", "on_start", 0, i1, i2)
    plugin_ref.assert_cb_called("testservice_2", "on_start", 0, i1, i2)


cluster_cfg = """
    cluster:
        name: test
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

    plugin_ref = PluginReflection(
        _PLUGIN, _PLUGIN_VERSION_1, _PLUGIN_SERVICES, [i1, i2, i3]
    )

    i1.call("pico.install_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        "red",
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[1],
        "blue",
    )
    i1.call("pico.enable_plugin", _PLUGIN, _PLUGIN_VERSION_1)

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

    p1_ref = PluginReflection(
        _PLUGIN, _PLUGIN_VERSION_1, _PLUGIN_SERVICES, [i1, i2, i3]
    )
    p2_ref = PluginReflection(
        _PLUGIN_SMALL, _PLUGIN_VERSION_1, _PLUGIN_SMALL_SERVICES, [i1, i2, i3]
    )

    i1.call("pico.install_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    i1.call("pico.install_plugin", _PLUGIN_SMALL, _PLUGIN_VERSION_1)
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        "red",
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[1],
        "red",
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN_SMALL,
        _PLUGIN_VERSION_1,
        _PLUGIN_SMALL_SERVICES[0],
        "blue",
    )
    i1.call("pico.enable_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    i1.call("pico.enable_plugin", _PLUGIN_SMALL, _PLUGIN_VERSION_1)

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

    plugin_ref = PluginReflection(
        _PLUGIN, _PLUGIN_VERSION_1, _PLUGIN_SERVICES, [i1, i2, i3]
    )

    i1.call("pico.install_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        "red",
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[1],
        "red",
    )
    i1.call("pico.enable_plugin", _PLUGIN, _PLUGIN_VERSION_1)

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

    i1.call(
        "pico.service_remove_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        "red",
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        "blue",
    )

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

    plugin_ref = PluginReflection(
        _PLUGIN, _PLUGIN_VERSION_1, _PLUGIN_SERVICES, [i1, i2, i3]
    )

    i1.call("pico.install_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        "red",
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[1],
        "red",
    )
    i1.call("pico.enable_plugin", _PLUGIN, _PLUGIN_VERSION_1)

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

    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        "blue",
    )

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

    plugin_ref = PluginReflection(
        _PLUGIN, _PLUGIN_VERSION_1, _PLUGIN_SERVICES, [i1, i2, i3]
    )

    i1.call("pico.install_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        "red",
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[1],
        "red",
    )
    i1.call("pico.enable_plugin", _PLUGIN, _PLUGIN_VERSION_1)

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

    i1.call(
        "pico.service_remove_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        "red",
    )

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

    p1_ref = PluginReflection(
        _PLUGIN, _PLUGIN_VERSION_1, _PLUGIN_SERVICES, [i1, i2, i3]
    )
    p2_ref = PluginReflection(
        _PLUGIN_SMALL,
        _PLUGIN_VERSION_1,
        _PLUGIN_SMALL_SERVICES,
        [i1, i2, i3],
    )

    i1.call("pico.install_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    i1.call("pico.install_plugin", _PLUGIN_SMALL, _PLUGIN_VERSION_1)

    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        "red",
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[1],
        "red",
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[1],
        "blue",
    )

    i1.call(
        "pico.service_append_tier",
        _PLUGIN_SMALL,
        _PLUGIN_VERSION_1,
        _PLUGIN_SMALL_SERVICES[0],
        "blue",
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN_SMALL,
        _PLUGIN_VERSION_1,
        _PLUGIN_SMALL_SERVICES[0],
        "green",
    )

    i1.call("pico.enable_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    i1.call("pico.enable_plugin", _PLUGIN_SMALL, _PLUGIN_VERSION_1)

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
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        "green",
    )
    i1.call(
        "pico.service_remove_tier",
        _PLUGIN_SMALL,
        _PLUGIN_VERSION_1,
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

    plugin_ref = PluginReflection(
        _PLUGIN, _PLUGIN_VERSION_1, _PLUGIN_SERVICES, [i1, i2]
    )

    i1.call("pico.install_plugin", _PLUGIN, _PLUGIN_VERSION_1)
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[0],
        "red",
    )
    i1.call(
        "pico.service_append_tier",
        _PLUGIN,
        _PLUGIN_VERSION_1,
        _PLUGIN_SERVICES[1],
        "red",
    )
    i1.call("pico.enable_plugin", _PLUGIN, _PLUGIN_VERSION_1)

    plugin_ref = (
        plugin_ref.install(True)
        .enable(True)
        .set_topology({i1: _PLUGIN_SERVICES, i2: []})
    )
    plugin_ref.assert_synced()

    # inject error into tier "blue"
    plugin_ref.inject_error("testservice_1", "on_start", True, i2)

    with pytest.raises(
        ReturnError,
        match=f"Callback: on_start: box error #{ErrorCode.PluginError}: error at `on_start`",
    ):
        i1.call(
            "pico.service_append_tier",
            _PLUGIN,
            _PLUGIN_VERSION_1,
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
        PLUGIN_VERSION: _PLUGIN_VERSION_1,
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

    context = make_context({PLUGIN_VERSION: _PLUGIN_VERSION_2})
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
            service_info=(plugin_name, service_name, _PLUGIN_VERSION_1),
        )
        i1.call(".proc_rpc_dispatch", "/register", msgpack.dumps(input), context)

    with pytest.raises(TarantoolError, match="RPC route path cannot be empty"):
        context = make_context()
        input = dict(
            path="",
            service_info=(plugin_name, service_name, _PLUGIN_VERSION_1),
        )
        i1.call(".proc_rpc_dispatch", "/register", msgpack.dumps(input), context)

    with pytest.raises(
        TarantoolError, match="RPC route path must start with '/', got 'bad-path'"
    ):
        context = make_context()
        input = dict(
            path="bad-path",
            service_info=(plugin_name, service_name, _PLUGIN_VERSION_1),
        )
        i1.call(".proc_rpc_dispatch", "/register", msgpack.dumps(input), context)

    with pytest.raises(TarantoolError, match="RPC route plugin name cannot be empty"):
        context = make_context()
        input = dict(
            path="/good-path",
            service_info=("", service_name, _PLUGIN_VERSION_1),
        )
        i1.call(".proc_rpc_dispatch", "/register", msgpack.dumps(input), context)

    with pytest.raises(
        TarantoolError,
        match="RPC endpoint `[^`]*` is already registered with a different handler",
    ):
        context = make_context()
        input = dict(
            path="/register",
            service_info=(plugin_name, service_name, _PLUGIN_VERSION_1),
        )
        i1.call(".proc_rpc_dispatch", "/register", msgpack.dumps(input), context)

    with pytest.raises(
        TarantoolError,
        match="RPC endpoint `[^`]*` is already registered with a different version",
    ):
        context = make_context()
        input = dict(
            path="/register",
            service_info=(plugin_name, service_name, _PLUGIN_VERSION_2),
        )
        i1.call(".proc_rpc_dispatch", "/register", msgpack.dumps(input), context)

    # Check all RPC endpoints get unregistered
    i1.call("pico.disable_plugin", plugin_name, _PLUGIN_VERSION_1)

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
    cluster.set_service_password("secrte")
    cluster.set_config_file(
        yaml="""
cluster:
    name: plugin_test
    tier:
        default:
        router:
"""
    )
    i1 = cluster.add_instance(name="i1", replicaset_name="r1", wait_online=False)
    i2 = cluster.add_instance(name="i2", replicaset_name="r1", wait_online=False)
    i3 = cluster.add_instance(name="i3", replicaset_name="r2", wait_online=False)
    i4 = cluster.add_instance(name="i4", replicaset_name="r2", wait_online=False)
    router_instance = cluster.add_instance(
        name="i5", wait_online=False, replicaset_name="r3", tier="router"
    )
    cluster.wait_online()

    def replicaset_master_name(replicaset_name: str) -> str:
        return i1.eval(
            "return box.space._pico_replicaset:get(...).target_master_name",
            replicaset_name,
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

    version = "0.1.0"
    services = [service_name]

    for s in services:
        router_instance.call(
            "pico.service_append_tier", plugin_name, version, s, "router"
        )

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

    # Check calling RPC to a specific instance_name via the plugin SDK
    # Note: /proxy endpoint redirects the request
    context = make_context()
    input = dict(
        path="/ping",
        instance_name=i2.name,
        input="how are you?",
    )
    output = i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    assert msgpack.loads(output) == ["pong", i2.name, b"how are you?"]

    i1.call("pico._inject_error", "RPC_NETWORK_ERROR", True)
    context = make_context()

    # check that rpc call to a non-self instance will fail
    input = dict(
        path="/ping",
        instance_name=i2.name,
        input="how are you?",
    )
    with pytest.raises(TarantoolError, match="injected error"):
        i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)

    # check self-calling RPC (this should not use a network)
    input["instance_name"] = i1.name
    output = i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    assert msgpack.loads(output) == ["pong", i1.name, b"how are you?"]

    i1.call("pico._inject_error", "RPC_NETWORK_ERROR", False)

    # Check calling RPC to ANY instance via the plugin SDK
    context = make_context()
    input = dict(
        path="/ping",
        input="random-target",
    )
    output = i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    pong, instance_name, echo = msgpack.loads(output)
    assert pong == "pong"
    assert instance_name in [
        i2.name,
        i3.name,
        i4.name,
        router_instance.name,
    ]
    assert echo == b"random-target"

    # Check calling RPC to a specific replicaset via the plugin SDK
    context = make_context()
    input = dict(
        path="/ping",
        replicaset_name="r2",
        input="replicaset:any",
    )
    output = i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    pong, instance_name, echo = msgpack.loads(output)
    assert pong == "pong"
    assert instance_name in [i3.name, i4.name]
    assert echo == b"replicaset:any"

    # Check calling RPC to a master of a specific replicaset via the plugin SDK
    context = make_context()
    input = dict(
        path="/ping",
        replicaset_name="r2",
        to_master=True,
        input="replicaset:master",
    )
    output = i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    pong, instance_name, echo = msgpack.loads(output)
    assert pong == "pong"
    assert instance_name == replicaset_master_name("r2")
    assert echo == b"replicaset:master"

    # Make sure buckets are balanced before routing via bucket_id to eliminate
    # flakiness due to bucket rebalancing
    for i in cluster.instances:
        if i.get_tier() != _DEFAULT_TIER:
            continue

        cluster.wait_until_instance_has_this_many_active_buckets(i, 1500)

    cluster.wait_until_instance_has_this_many_active_buckets(router_instance, 3000)

    # Check calling RPC by bucket_id via the plugin SDK
    context = make_context()
    input = dict(
        path="/ping",
        bucket_id=any_bucket_id(i1),
        input="bucket_id:any",
    )
    output = i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    pong, instance_name, echo = msgpack.loads(output)
    assert pong == "pong"
    assert instance_name == i2.name  # shouldn't call self
    assert echo == b"bucket_id:any"

    # Check calling RPC by tier and bucket_id via the plugin SDK
    context = make_context()
    input = dict(
        path="/ping",
        tier_and_bucket_id=("router", any_bucket_id(router_instance)),
        input="bucket_id:any",
    )
    output = i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    pong, instance_name, echo = msgpack.loads(output)
    assert pong == "pong"
    assert instance_name == router_instance.name
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
    pong, instance_name, echo = msgpack.loads(output)
    assert pong == "pong"
    assert instance_name == replicaset_master_name("r2")
    assert echo == b"bucket_id:master"

    # Check calling builtin picodata stored procedures via plugin SDK
    context = make_context()
    input = dict(
        path=".proc_instance_info",
        instance_name="i1",
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
            instance_name="i1",
            input=msgpack.dumps([]),
            service_info=("NO_SUCH_PLUGIN", service_name, _PLUGIN_VERSION_1),
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
            instance_name="i1",
            input=msgpack.dumps([]),
            service_info=(plugin_name, "NO_SUCH_SERVICE", _PLUGIN_VERSION_1),
        )
        i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)

    # Check requesting RPC to unknown instance
    with pytest.raises(TarantoolError) as e:
        context = make_context()
        input = dict(
            path="/ping",
            instance_name="NO_SUCH_INSTANCE",
            input=msgpack.dumps([]),
        )
        i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    assert e.value.args[:2] == (
        ErrorCode.NoSuchInstance,
        'instance with name "NO_SUCH_INSTANCE" not found',
    )

    # Check requesting RPC to unknown replicaset
    with pytest.raises(TarantoolError) as e:
        context = make_context()
        input = dict(
            path="/ping",
            replicaset_name="NO_SUCH_REPLICASET",
            input=msgpack.dumps([]),
        )
        i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    assert e.value.args[:2] == (
        ErrorCode.NoSuchReplicaset,
        'replicaset with name "NO_SUCH_REPLICASET" not found',
    )

    # Check requesting RPC to unknown bucket id
    with pytest.raises(
        TarantoolError,
        match="invalid bucket id: must be within 1..3000, got 9999",
    ):
        context = make_context()
        input = dict(
            path="/ping",
            bucket_id=9999,
            input=msgpack.dumps([]),
        )
        i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)

    # Check requesting RPC to unknown tier
    with pytest.raises(
        TarantoolError,
        match='tier with name "undefined" not found',
    ):
        context = make_context()
        input = dict(
            path="/ping",
            tier_and_bucket_id=("undefined", 1500),
            input=msgpack.dumps([]),
        )
        i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)

    # Check RPC after expel
    counter = i1.governor_step_counter()
    # Start expel and wait until governor performs all necessary steps
    cluster.expel(i3, peer=i1)
    i1.wait_governor_status("idle", old_step_counter=counter)

    # Check RPC directly to expelled instance
    context = make_context()
    input = dict(
        path="/ping",
        instance_name=i3.name,
        input="directly to expelled",
    )
    with pytest.raises(TarantoolError) as e:
        i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    assert e.value.args[:2] == (
        ErrorCode.InstanceExpelled,
        "instance named 'i3' was expelled",
    )

    # Check RPC which could potentially go to expelled instance (same replicaset)
    context = make_context()
    input = dict(
        path="/ping",
        replicaset_name="r2",
        to_master=False,
        input="replicaset-of-expelled",
    )
    # Call from i4 to i4 (also check RPC to self when self is the sole candidate)
    output = i4.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    pong, instance_name, echo = msgpack.loads(output)
    assert pong == "pong"
    # No other candidates
    assert instance_name == i4.name
    assert echo == b"replicaset-of-expelled"

    counter = i1.governor_step_counter()
    # Expell the whole replicaset
    cluster.expel(i4, peer=i1)
    i1.wait_governor_status("idle", old_step_counter=counter, timeout=60)

    [[r2_uuid]] = i1.sql(""" SELECT "uuid" FROM _pico_replicaset WHERE name = 'r2' """)

    # Check RPC to expelled replicaset
    context = make_context()
    input = dict(
        path="/ping",
        replicaset_name="r2",
        to_master=False,
        input="expelled-replicaset",
    )
    with pytest.raises(TarantoolError) as e:
        i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)
    assert e.value.args[:2] == (
        ErrorCode.ReplicasetExpelled,
        f"replicaset with id {r2_uuid} was expelled",
    )

    i2.terminate()
    time.sleep(5)

    with pytest.raises(TarantoolError) as e:
        context = make_context()
        input = dict(
            path="/ping",
            instance_name="i2",
            input=msgpack.dumps([]),
        )
        i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)

    assert e.value.args[:2] == (
        ErrorCode.InstanceUnavaliable,
        "instance with instance_name \"i2\" can't respond due it's state",
    )

    # TODO: check calling to poisoned service


def test_plugin_rpc_sdk_single_instance(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True, init_replication_factor=1)

    plugin_name = _PLUGIN_W_SDK
    service_name = SERVICE_W_RPC
    install_and_enable_plugin(i1, plugin_name, [service_name], migrate=True)

    # Check sending request with `master = false` when there's ony one instance
    context = make_context()
    input = dict(
        service_info=(plugin_name, service_name, _PLUGIN_VERSION_1),
        path="/ping",
        input="by-bucket-id",
        bucket_id=13,
        to_master=False,
    )
    i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)

    # Check sending request with `master = false` when there's ony one instance
    context = make_context()
    input = dict(
        service_info=(plugin_name, service_name, _PLUGIN_VERSION_1),
        path="/ping",
        input="by-replicaset-name",
        replicaset_name=i1.call(".proc_instance_info")["replicaset_name"],
        to_master=False,
    )
    i1.call(".proc_rpc_dispatch", "/proxy", msgpack.dumps(input), context)


def test_panic_in_plugin(cluster: Cluster):
    plugin_name = "plugin_for_test_panic_in_plugin"
    service_name = "test_panic_in_plugin"
    init_dummy_plugin(cluster, plugin_name, "0.1.0", services=[service_name])

    # Set the log configuration
    log_file = Path(cluster.instance_dir) / "test.log"
    cluster.set_config_file(
        yaml=f"""
cluster:
    tier:
        default:
instance:
    cluster_name: test
    tier: default
    log:
        destination: {log_file}
"""
    )

    #
    # Start instance and check
    #
    [i1] = cluster.deploy(instance_count=1)

    i1.sql(f"CREATE PLUGIN {plugin_name} 0.1.0")
    i1.sql(
        f"ALTER PLUGIN {plugin_name} 0.1.0 ADD SERVICE {service_name} TO TIER default"
    )
    with pytest.raises(Exception) as e:
        i1.sql(f"ALTER PLUGIN {plugin_name} 0.1.0 ENABLE")

    # XXX: for some reason this test is very flaky when running in CI.
    # Add more waiting.
    if not isinstance(e, ProcessDead):
        i1.wait_process_stopped()

    log = log_file.read_text()
    assert "this is a unique phrase which makes it safe to use in a test" in log


def test_sdk_internal(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    install_and_enable_plugin(i1, _PLUGIN_W_SDK, _PLUGIN_W_SDK_SERVICES, migrate=True)

    version_info = i1.call(".proc_version_info")
    PluginReflection.assert_data_eq(i1, "version", version_info["picodata_version"])
    PluginReflection.assert_data_eq(i1, "rpc_version", version_info["rpc_api_version"])

    PluginReflection.assert_data_eq(i1, "name", i1.name)
    PluginReflection.assert_data_eq(i1, "uuid", i1.uuid())
    PluginReflection.assert_data_eq(i1, "replicaset_name", "default_1")
    PluginReflection.assert_data_eq(i1, "replicaset_uuid", i1.replicaset_uuid())
    PluginReflection.assert_data_eq(i1, "cluster_name", i1.cluster_name)
    PluginReflection.assert_data_eq(i1, "tier", _DEFAULT_TIER)
    PluginReflection.assert_data_eq(i1, "raft_id", "1")
    PluginReflection.assert_int_data_le(
        i1, "raft_term", i1.eval("return pico.raft_term()")
    )
    PluginReflection.assert_int_data_le(i1, "raft_index", i1.call(".proc_get_index"))

    cas_result_sdk = i1.eval("return box.space.author:select()")
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

    sql_result = i1.eval("return box.space.book:select()")
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
    crawler.wait_matched()


# noinspection HttpUrlsUsage
@pytest.mark.webui
def test_sdk_metrics(instance: Instance):
    plugin = _PLUGIN_W_SDK
    [service] = _PLUGIN_W_SDK_SERVICES

    http_listen = instance.env["PICODATA_HTTP_LISTEN"]

    install_and_enable_plugin(
        instance,
        plugin,
        [service],
        migrate=True,
        default_config={"test_type": "metrics"},
    )

    # Metrics work
    response = requests.get(f"http://{http_listen}/metrics")
    assert response.ok
    assert "test_metric_1 1" in response.text
    assert "test_metric_2 2" in response.text

    instance.sql(f""" ALTER PLUGIN {plugin} 0.1.0 DISABLE """)

    PluginReflection.assert_persisted_data_exists(
        "drop was called for metrics closure", instance
    )

    # Metrics no longer work
    response = requests.get(f"http://{http_listen}/metrics")
    assert response.ok
    assert "test_metric_1 1" not in response.text
    assert "test_metric_2 2" not in response.text


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
    i1.call("pico.disable_plugin", _PLUGIN_W_SDK, _PLUGIN_VERSION_1)

    Retriable(timeout=5, rps=2).call(
        PluginReflection.assert_persisted_data_exists, "background_job_stopped", i1
    )

    # run again
    i1.call("pico.enable_plugin", _PLUGIN_W_SDK, _PLUGIN_VERSION_1)
    Retriable(timeout=5, rps=2).call(
        PluginReflection.assert_persisted_data_exists, "background_job_running", i1
    )

    # now shutdown 1 and check that job ended
    i1.sql(
        f"ALTER PLUGIN {_PLUGIN_W_SDK} {_PLUGIN_VERSION_1} SET"
        f"    {_PLUGIN_W_SDK_SERVICES[0]}.test_type = 'no_test'"
    )

    i1.restart()
    i1.wait_online()
    PluginReflection.assert_persisted_data_exists("background_job_stopped", i1)

    PluginReflection.clear_persisted_data(i1)


def test_sql_interface(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    plugin_ref = PluginReflection(
        _PLUGIN_WITH_MIGRATION, "0.1.0", ["testservice_2"], [i1, i2]
    )

    i1.sql(f'CREATE PLUGIN "{_PLUGIN_WITH_MIGRATION}" 0.1.0')
    i1.sql(f'ALTER PLUGIN "{_PLUGIN_WITH_MIGRATION}" MIGRATE TO 0.1.0')
    plugin_ref = plugin_ref.install(True).set_data(_DATA_V_0_1_0)
    plugin_ref.assert_synced()
    plugin_ref.assert_data_synced()

    i1.sql(
        f'ALTER PLUGIN "{_PLUGIN_WITH_MIGRATION}" 0.1.0 ADD SERVICE "testservice_2" '
        f'TO TIER "{_DEFAULT_TIER}"'
    )
    plugin_ref = plugin_ref.set_topology({i1: ["testservice_2"], i2: ["testservice_2"]})

    i1.sql(f'ALTER PLUGIN "{_PLUGIN_WITH_MIGRATION}" 0.1.0 ENABLE')
    plugin_ref = plugin_ref.enable(True)
    plugin_ref.assert_synced()

    i1.sql(f'ALTER PLUGIN "{_PLUGIN_WITH_MIGRATION}" 0.1.0 DISABLE')
    plugin_ref = plugin_ref.enable(False).set_topology({})
    plugin_ref.assert_synced()

    i1.sql(f'DROP PLUGIN IF EXISTS "{_PLUGIN_WITH_MIGRATION}" 0.1.0 WITH DATA')
    plugin_ref = plugin_ref.set_data(_NO_DATA_V_0_1_0).install(False)
    plugin_ref.assert_synced()
    plugin_ref.assert_data_synced()


def test_sql_interface_update_config(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    plugin_ref = PluginReflection.default(i1, i2)

    plugin = _PLUGIN

    # Error: Wrong plugin name
    with pytest.raises(TarantoolError) as e:
        i1.sql("ALTER PLUGIN no_such_plugin 0.1.0 SET testservice_1.foo = 'false'")
    assert e.value.args[:2] == (
        ErrorCode.PluginError,
        "no such plugin `no_such_plugin:0.1.0`",
    )

    i1.sql(f"CREATE PLUGIN {plugin} 0.1.0")

    # Error: Wrong plugin version
    with pytest.raises(TarantoolError) as e:
        i1.sql(f"ALTER PLUGIN {plugin} 1.2.3 SET testservice_1.foo = 'false'")
    assert e.value.args[:2] == (
        ErrorCode.PluginError,
        f"no such plugin `{plugin}:1.2.3`",
    )

    # Error: Wrong service name
    with pytest.raises(TarantoolError) as e:
        i1.sql(f"ALTER PLUGIN {plugin} 0.1.0 SET no_such_service.foo = 'false'")
    assert e.value.args[:2] == (
        ErrorCode.NoSuchService,
        f"no such service `{plugin}.no_such_service:v0.1.0`",
    )

    i1.sql(
        f"ALTER PLUGIN {plugin} 0.1.0 ADD SERVICE testservice_1 TO TIER {_DEFAULT_TIER}"
    )
    i1.sql(
        f"ALTER PLUGIN {plugin} 0.1.0 ADD SERVICE testservice_2 TO TIER {_DEFAULT_TIER}"
    )
    i1.sql(f"ALTER PLUGIN {plugin} 0.1.0 ENABLE")

    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()
    plugin_ref.assert_config("testservice_1", _DEFAULT_CFG, i1, i2)

    i1.sql(f"ALTER PLUGIN {plugin} 0.1.0 SET testservice_1.foo = 'false'")

    # retrying, cause new service configuration callback call asynchronously
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_config(
            "testservice_1",
            {"foo": False, "bar": 101, "baz": ["one", "two", "three"]},
            i1,
            i2,
        )
    )
    new_cfg = (
        "testservice_1.foo = 'true', testservice_1.bar= '102', "
        "testservice_1.baz = '[\"one\"]', testservice_2.foo = '5'"
    )
    i1.sql(f'ALTER PLUGIN "{plugin}" 0.1.0 SET {new_cfg}')

    # retrying, cause new service configuration callback call asynchronously
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_config(
            "testservice_1",
            {"foo": True, "bar": 102, "baz": ["one"]},
            i1,
            i2,
        )
    )
    Retriable(timeout=3, rps=5).call(
        lambda: plugin_ref.assert_config(
            "testservice_2",
            {"foo": 5},
            i1,
            i2,
        )
    )
    plugin_ref.assert_cb_called("testservice_1", "on_config_change", 2, i1)
    plugin_ref.assert_cb_called("testservice_2", "on_config_change", 1, i1)


def test_sql_interface_inheritance(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    plugin_ref = PluginReflection(_PLUGIN, "0.2.0", _PLUGIN_SERVICES, [i1, i2])

    i1.sql(f'CREATE PLUGIN "{_PLUGIN}" 0.1.0')
    i1.sql(
        f'ALTER PLUGIN "{_PLUGIN}" 0.1.0 ADD SERVICE "testservice_1" TO TIER "{_DEFAULT_TIER}"'
    )
    i1.sql(
        f'ALTER PLUGIN "{_PLUGIN}" 0.1.0 ADD SERVICE "testservice_2" TO TIER "{_DEFAULT_TIER}"'
    )

    # install v0.2.0 with config and topology inherit (currently inheritance are always enabled)
    i1.sql(f'CREATE PLUGIN "{_PLUGIN}" 0.2.0')
    i1.sql(f'ALTER PLUGIN "{_PLUGIN}" 0.2.0 ENABLE')
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref = plugin_ref.set_topology({i1: _PLUGIN_SERVICES, i2: _PLUGIN_SERVICES})
    plugin_ref.assert_synced()
    cfg_space = plugin_ref.get_config("testservice_1", i1)
    assert cfg_space == {"foo": True, "bar": 101, "baz": ["one", "two", "three"]}


def test_plugin_sql_permission_denied(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    user = "alex"
    password = "L0ng enough"

    i1.sql(f"""CREATE USER "{user}" WITH PASSWORD '{password}' using chap-sha1""")
    with pytest.raises(TarantoolError) as e:
        i1.sql(f"CREATE PLUGIN {_PLUGIN} 0.1.0", user=user, password=password)
    assert e.value.args[:2] == (
        "ER_ACCESS_DENIED",
        "Plugin system access is denied for user 'alex'",
    )

    # Access is checked before plugin name validation
    with pytest.raises(TarantoolError) as e:
        i1.sql("CREATE PLUGIN no_such_plugin 0.1.0", user=user, password=password)
    assert e.value.args[:2] == (
        "ER_ACCESS_DENIED",
        "Plugin system access is denied for user 'alex'",
    )

    # Create as superuser to check the rest of commands
    i1.sql(f"CREATE PLUGIN {_PLUGIN} 0.1.0")

    with pytest.raises(TarantoolError) as e:
        i1.sql(
            f'ALTER PLUGIN {_PLUGIN} 0.1.0 ADD SERVICE no_such_service TO TIER "{_DEFAULT_TIER}"',
            user=user,
            password=password,
        )
    assert e.value.args[:2] == (
        "ER_ACCESS_DENIED",
        "Plugin system access is denied for user 'alex'",
    )

    with pytest.raises(TarantoolError) as e:
        i1.sql(f"ALTER PLUGIN {_PLUGIN} 0.1.0 ENABLE", user=user, password=password)
    assert e.value.args[:2] == (
        "ER_ACCESS_DENIED",
        "Plugin system access is denied for user 'alex'",
    )

    with pytest.raises(TarantoolError) as e:
        i1.sql(f"ALTER PLUGIN {_PLUGIN} 0.1.0 DISABLE", user=user, password=password)
    assert e.value.args[:2] == (
        "ER_ACCESS_DENIED",
        "Plugin system access is denied for user 'alex'",
    )

    with pytest.raises(TarantoolError) as e:
        i1.sql(f"DROP PLUGIN {_PLUGIN} 0.1.0", user=user, password=password)
    assert e.value.args[:2] == (
        "ER_ACCESS_DENIED",
        "Plugin system access is denied for user 'alex'",
    )


def test_create_plugin_too_many_versions(cluster: Cluster):
    init_dummy_plugin(cluster, "too_many_versions", "0.1.0")
    init_dummy_plugin(cluster, "too_many_versions", "0.1.1")
    init_dummy_plugin(cluster, "too_many_versions", "0.1.2")
    init_dummy_plugin(cluster, "too_many_versions", "0.1.3")
    init_dummy_plugin(cluster, "another_plugin", "0.1.0")
    init_dummy_plugin(cluster, "another_plugin", "0.1.1")

    instance = cluster.add_instance()

    # We can create two versions of the same plugin
    instance.sql("CREATE PLUGIN too_many_versions 0.1.0")
    instance.sql("CREATE PLUGIN too_many_versions 0.1.1")

    # But creating a 3rd version doesn't work
    with pytest.raises(TarantoolError) as e:
        instance.sql("CREATE PLUGIN too_many_versions 0.1.2")
    assert e.value.args[:2] == (
        ErrorCode.PluginError,
        "too many versions of plugin 'too_many_versions', only 2 versions of the same plugin may exist at the same time",  # noqa: E501
    )

    # Just make sure we can have a couple of versions of another plugin
    instance.sql("CREATE PLUGIN another_plugin 0.1.0")
    instance.sql("CREATE PLUGIN another_plugin 0.1.1")

    # To resolve the problem we must drop one of the old versions
    instance.sql("DROP PLUGIN too_many_versions 0.1.0")

    # Now it's ok to install another version
    instance.sql("CREATE PLUGIN too_many_versions 0.1.2")

    # Try to do the wrong thing again, make sure it still doesn't work
    with pytest.raises(TarantoolError) as e:
        instance.sql("CREATE PLUGIN too_many_versions 0.1.3")
    assert e.value.args[:2] == (
        ErrorCode.PluginError,
        "too many versions of plugin 'too_many_versions', only 2 versions of the same plugin may exist at the same time",  # noqa: E501
    )

    # And the solution still works
    instance.sql("DROP PLUGIN too_many_versions 0.1.1")
    instance.sql("CREATE PLUGIN too_many_versions 0.1.3")

    rows = instance.sql(
        """
        SELECT version FROM _pico_plugin WHERE name = 'too_many_versions' ORDER BY version
        """,
    )
    assert rows == [["0.1.2"], ["0.1.3"]]


def test_picoplugin_version_compatibility_check(cluster: Cluster):
    init_dummy_plugin(
        cluster,
        "plug_wrong_version",
        "0.1.0",
        services=["testservice"],
        library_name="libplug_wrong_version",
    )

    instance = cluster.add_instance()

    with pytest.raises(
        TarantoolError,
        match="Picoplugin version .* used to build a plugin is incompatible with picodata version",
    ):
        instance.sql("CREATE PLUGIN plug_wrong_version 0.1.0")

    # disable compatibility check
    instance.env["PICODATA_UNSAFE_DISABLE_PLUGIN_COMPATIBILITY_CHECK"] = "1"
    instance.restart()
    instance.wait_online()
    instance.sql("CREATE PLUGIN plug_wrong_version 0.1.0")


def test_set_string_values_in_config(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    plugin_ref = PluginReflection(_PLUGIN_W_SDK, "0.1.0", _PLUGIN_W_SDK_SERVICES, [i1])
    install_and_enable_plugin(i1, _PLUGIN_W_SDK, _PLUGIN_W_SDK_SERVICES, migrate=True)
    plugin_ref = plugin_ref.install(True).enable(True)
    plugin_ref.assert_synced()

    def retriable_assert_in_table_config(cfg):
        Retriable(timeout=3, rps=5).call(
            lambda: plugin_ref.assert_in_table_config("testservice_3", cfg, i1)
        )

    def set_service_3_test_type(s: str):
        i1.sql(
            f"ALTER PLUGIN \"{_PLUGIN_W_SDK}\" 0.1.0 SET testservice_3.test_type = '{s}'"
        )

    # valid json string wrapped in quotes
    set_service_3_test_type('"kek"')
    retriable_assert_in_table_config({"test_type": "kek"})

    # invalid json string without quotes
    set_service_3_test_type("kek")
    retriable_assert_in_table_config({"test_type": "kek"})

    # strings with whitespaces
    set_service_3_test_type('" string with whitespaces "')
    retriable_assert_in_table_config({"test_type": " string with whitespaces "})
    set_service_3_test_type(" string with whitespaces ")
    retriable_assert_in_table_config({"test_type": " string with whitespaces "})

    # string with " in the middle (invalid json)
    set_service_3_test_type('" string with " in the middle "')
    retriable_assert_in_table_config({"test_type": '" string with " in the middle "'})
    set_service_3_test_type(' string with " in the middle ')
    retriable_assert_in_table_config({"test_type": ' string with " in the middle '})

    # string with ' in the middle (invalid json)
    with pytest.raises(TarantoolError, match="rule parsing error"):
        set_service_3_test_type('" string with \' in the middle "')
    with pytest.raises(TarantoolError, match="rule parsing error"):
        set_service_3_test_type(" string with ' in the middle ")

    # string with '' in the middle (invalid json)
    set_service_3_test_type("\" string with '' in the middle \"")
    retriable_assert_in_table_config({"test_type": " string with '' in the middle "})
    set_service_3_test_type(" string with '' in the middle ")
    retriable_assert_in_table_config({"test_type": " string with '' in the middle "})

    # awfully invalid json
    with pytest.raises(TarantoolError, match="rule parsing error"):
        set_service_3_test_type(""" "]'['"32+1][[,." """)
    with pytest.raises(TarantoolError, match="rule parsing error"):
        set_service_3_test_type(""" ]'['"32+1][[,. """)

    # a string wrapped with ' (invalid json)
    with pytest.raises(TarantoolError, match="rule parsing error"):
        set_service_3_test_type("' a string wrapped with single quotes '")

    # array of string wrapped with ' (invalid json)
    with pytest.raises(TarantoolError, match="rule parsing error"):
        set_service_3_test_type("\"['1', '2']\"")
    with pytest.raises(TarantoolError, match="rule parsing error"):
        set_service_3_test_type("['1', '2']")


def test_plugin_migration_placeholder_substitution(cluster: Cluster):
    plugin = "testplug_w_migration_in_tier"

    plugin_dir = init_dummy_plugin(
        cluster, plugin, "0.1.0", migrations=["migration.sql"]
    )
    (plugin_dir / "migration.sql").write_text(
        """
-- pico.UP

CREATE TABLE author (id INTEGER NOT NULL, name TEXT NOT NULL, PRIMARY KEY (id))
USING memtx
DISTRIBUTED BY ("id") IN TIER @_plugin_config.stringy_string;

-- pico.DOWN
DROP TABLE author;
"""
    )

    cluster.set_config_file(
        yaml="""
        cluster:
            name: test
            tier:
                default:
                nondefault:
    """
    )

    i1 = cluster.add_instance(tier="nondefault")

    # happy path, valid migration, everything is ok
    i1.sql(f'CREATE PLUGIN "{plugin}" 0.1.0')

    i1.sql(
        f"""
        ALTER PLUGIN "{plugin}" 0.1.0 SET
            migration_context.stringy_string = \'"nondefault"\'
        """
    )

    i1.sql(f'ALTER PLUGIN "{plugin}" MIGRATE TO 0.1.0')

    assert i1.sql("SELECT * FROM author") == []
    distribution = i1.sql("SELECT distribution FROM _pico_table WHERE name = 'author'")[
        0
    ][0]
    tier = distribution["ShardedImplicitly"][2]
    assert tier == "nondefault"

    i1.sql(f'DROP PLUGIN "{plugin}" 0.1.0 WITH DATA')

    with pytest.raises(TarantoolError, match='table with name "author" not found'):
        assert i1.sql("SELECT * FROM author") == []

    # reference missing variable
    (plugin_dir / "migration.sql").write_text(
        """
-- pico.UP

CREATE TABLE author (id INTEGER NOT NULL, name TEXT NOT NULL, PRIMARY KEY (id))
USING memtx
DISTRIBUTED BY ("id") IN TIER @_plugin_config.bubba;

-- pico.DOWN
DROP TABLE author;
"""
    )
    i1.sql(f'CREATE PLUGIN "{plugin}" 0.1.0')

    with pytest.raises(
        TarantoolError, match="no key named bubba found in migration context at line 6"
    ):
        i1.sql(f'ALTER PLUGIN "{plugin}" MIGRATE TO 0.1.0')

    i1.sql(f'DROP PLUGIN "{plugin}" 0.1.0 WITH DATA')
