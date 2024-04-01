import pytest
import time
from conftest import Cluster, ReturnError

_3_SEC = 3
_DEFAULT_CFG = {"foo": True, "bar": 101, "baz": ["one", "two", "three"]}
_NEW_CFG = {"foo": True, "bar": 102, "baz": ["a", "b"]}
_NEW_CFG_2 = {"foo": False, "bar": 102, "baz": ["a", "b"]}

_PLUGIN = "testplug"
_PLUGIN_SERVICES = ["testservice_1", "testservice_2"]
_PLUGIN_SMALL = "testplug_small"
_PLUGIN_SMALL_SERVICES = ["testservice_1"]


def get_route(needle_instance_id, plugin_name, service_name, instance):
    """return route from _pico_service_route space, this information
    using in rpc routing"""
    route = instance.eval(
        "return box.space._pico_service_route:get({...})",
        needle_instance_id,
        plugin_name,
        service_name,
    )
    return route


def assert_plugin_exists(plugin, services, *instances):
    for i in instances:
        plugins = i.eval("return box.space._pico_plugin:select(...)", plugin)
        assert len(plugins) == 1
        for service in services:
            svcs = i.eval(
                "return box.space._pico_service:select({...})", plugin, service
            )
            assert len(svcs) == 1
        # instance have all routing information
        for service in services:
            for neighboring_i in instances:
                route = get_route(neighboring_i.instance_id, plugin, service, i)
                assert route is not None


def assert_plugin_not_exists(plugin, services, *instances):
    for i in instances:
        plugins = i.eval("return box.space._pico_plugin:select(...)", plugin)
        assert len(plugins) == 0
        for service in services:
            svcs = i.eval(
                "return box.space._pico_service:select({...})", plugin, service
            )
            assert len(svcs) == 0

        # instance have all routing information
        for service in services:
            for neighboring_i in instances:
                route = get_route(neighboring_i.instance_id, plugin, service, i)
                assert route is None


def assert_plugin_record(expected, *instances, timeout=_3_SEC):
    plugins = None
    attempts = timeout
    for _ in range(attempts):
        for i in instances:
            plugins = i.eval("return box.space._pico_plugin:select()")
            if plugins == expected:
                return
        time.sleep(1)
    assert plugins == expected


def assert_cb_called(service, callback, called_times, *instances):
    for i in instances:
        cb_calls_number = i.eval(
            f"return _G['plugin_state']['{service}']['{callback}']"
        )
        assert cb_calls_number == called_times


def assert_persisted_data_exists(data, *instances):
    for i in instances:
        data_exists = i.eval(
            f"return box.space.persisted_data:get({{'{data}'}}) ~= box.NULL"
        )
        assert data_exists


def clear_persisted_data(data, *instances):
    for i in instances:
        i.eval("return box.space.persisted_data:drop()")


def inject_error(service, error, value, instance):
    instance.eval("if _G['err_inj'] == nil then _G['err_inj'] = {} end")
    instance.eval(
        f"if _G['err_inj']['{service}'] == nil then _G['err_inj']['{service}'] "
        "= {{}} end"
    )
    instance.eval(f"_G['err_inj']['{service}']['{error}'] = ...", (value,))


def remove_error(service, error, instance):
    instance.eval("if _G['err_inj'] == nil then _G['err_inj'] = {} end")
    instance.eval(
        f"if _G['err_inj']['{service}'] == nil then _G['err_inj']['{service}'] "
        "= {{}} end"
    )
    instance.eval(f"_G['err_inj']['{service}']['{error}'] = nil")


def assert_last_seen_ctx(service, expected_ctx, *instances):
    for i in instances:
        ctx = i.eval(f"return _G['plugin_state']['{service}']['last_seen_ctx']")
        assert ctx == expected_ctx


def test_invalid_manifest_plugin(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)

    # try to create non-existent plugin
    with pytest.raises(
        ReturnError, match="Error while discovering manifest for plugin"
    ):
        i1.call("pico.create_plugin", "non-existent")

    # try to use invalid manifest (with undefined plugin name)
    with pytest.raises(ReturnError, match="missing field `name`"):
        i1.call("pico.create_plugin", "testplug_broken_manifest_1")

    # try to use invalid manifest (with invalid default configuration)
    with pytest.raises(ReturnError, match="Error while load plugin"):
        i1.call("pico.create_plugin", "testplug_broken_manifest_2")

    # try to use invalid manifest (with non-existed extra service)
    with pytest.raises(ReturnError, match="Error while load plugin"):
        i1.call("pico.create_plugin", "testplug_broken_manifest_3")

    assert_plugin_not_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)
    assert_cb_called("testservice_1", "on_start", None, i1, i2)


def test_plugin_load(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)

    # assert that system tables are filled
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    # assert that on_start callbacks successfully called
    assert_cb_called("testservice_1", "on_start", 1, i1, i2)
    assert_cb_called("testservice_2", "on_start", 1, i1, i2)


def test_two_plugin_load(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)
    i1.call("pico.create_plugin", _PLUGIN_SMALL, timeout=_3_SEC)

    # assert that system tables are filled
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)
    assert_plugin_exists(_PLUGIN_SMALL, _PLUGIN_SMALL_SERVICES, i1, i2)

    # assert that on_start callbacks successfully called
    assert_cb_called("testservice_1", "on_start", 2, i1, i2)
    assert_cb_called("testservice_2", "on_start", 1, i1, i2)


def test_plugin_load_at_new_instance(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    i3 = cluster.add_instance(wait_online=True)
    assert_plugin_exists("testplug", _PLUGIN_SERVICES, i1, i2, i3)

    assert_cb_called("testservice_1", "on_start", 1, i1, i2, i3)
    assert_cb_called("testservice_2", "on_start", 1, i1, i2, i3)


def test_plugin_stop(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    i2.restart()
    i2.wait_online()

    assert_persisted_data_exists("testservice_1_stopd", i2)
    assert_persisted_data_exists("testservice_2_stopd", i2)
    clear_persisted_data(i2)


def test_instance_with_plugin_expel(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)
    cluster.expel(i2)
    # assert that expel doesn't affect plugins
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i2)


def test_plugin_remove(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    i1.call("pico.remove_plugin", _PLUGIN, timeout=_3_SEC)
    # sleep cause routing table update asynchronously
    time.sleep(_3_SEC)
    assert_plugin_not_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    assert_cb_called("testservice_1", "on_start", 1, i1, i2)
    assert_cb_called("testservice_2", "on_start", 1, i1, i2)
    assert_cb_called("testservice_1", "on_stop", 1, i1, i2)
    assert_cb_called("testservice_2", "on_stop", 1, i1, i2)


def test_plugin_double_remove(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    i1.eval(
        f'pico.remove_plugin("{_PLUGIN}") return pico.remove_plugin("{_PLUGIN}")',
        timeout=_3_SEC,
    )

    # check that remove plugin is idempotence operation
    assert_plugin_not_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)
    assert_cb_called("testservice_1", "on_stop", 1, i1, i2)
    assert_cb_called("testservice_2", "on_stop", 1, i1, i2)


def test_plugin_remove_error_on_stop(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    inject_error("testservice_1", "on_stop", True, i2)
    inject_error("testservice_2", "on_stop", True, i2)

    i1.call("pico.remove_plugin", _PLUGIN, timeout=_3_SEC)
    # sleep cause routing table update asynchronously
    time.sleep(_3_SEC)
    assert_plugin_not_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    assert_cb_called("testservice_1", "on_start", 1, i1, i2)
    assert_cb_called("testservice_2", "on_start", 1, i1, i2)
    assert_cb_called("testservice_1", "on_stop", 1, i1)
    assert_cb_called("testservice_2", "on_stop", 1, i1)
    assert_cb_called("testservice_1", "on_stop", None, i2)
    assert_cb_called("testservice_2", "on_stop", None, i2)


def test_plugin_not_load_if_error_on_start(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    # inject error into second instance
    inject_error("testservice_1", "on_start", True, i2)

    # assert that plugin not loaded and on_stop called on both instances
    with pytest.raises(ReturnError, match="Error while load plugin"):
        i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)

    assert_plugin_not_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)
    assert_cb_called("testservice_1", "on_stop", 1, i1, i2)

    # inject error into both instances
    inject_error("testservice_1", "on_start", True, i1)

    # assert that plugin not loaded and on_stop called on both instances
    with pytest.raises(ReturnError, match="Error while load plugin"):
        i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)

    assert_plugin_not_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)
    assert_cb_called("testservice_1", "on_stop", 2, i1, i2)

    # remove errors
    inject_error("testservice_1", "on_start", False, i1)
    inject_error("testservice_1", "on_start", False, i2)

    # assert plugin loaded now
    i1.call("pico.create_plugin", "testplug")
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)


def test_plugin_not_load_if_on_start_timeout(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    # inject timeout into second instance
    inject_error("testservice_1", "on_start_sleep_sec", 3, i2)

    with pytest.raises(ReturnError, match="Error while load plugin"):
        i1.call("pico.create_plugin", _PLUGIN, {"on_start_timeout": 2}, timeout=_3_SEC)
    # need to wait until sleep at i2 called asynchronously
    time.sleep(2)

    # assert that plugin not loaded and on_stop called on both instances
    assert_plugin_not_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)
    assert_cb_called("testservice_1", "on_stop", 1, i1, i2)

    # inject timeout into both instances
    inject_error("testservice_1", "on_start_sleep_sec", 3, i1)

    with pytest.raises(ReturnError, match="Error while load plugin"):
        i1.call("pico.create_plugin", _PLUGIN, {"on_start_timeout": 2}, timeout=_3_SEC)
    # need to wait until sleep at i1 and i2 called asynchronously
    time.sleep(2)

    # assert that plugin not loaded and on_stop called on both instances
    assert_plugin_not_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)
    assert_cb_called("testservice_1", "on_stop", 2, i1, i2)


def test_config_validation(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    inject_error("testservice_1", "on_cfg_validate", "test error", i1)
    with pytest.raises(
        ReturnError, match="New configuration validation error: test error"
    ):
        i1.eval(
            "return pico.update_plugin_config('testplug', "
            "'testservice_1', {foo = true, bar = 102, baz = {'a', 'b'}})"
        )


def get_config(plugin, service, instance):
    return instance.eval(
        f"return box.space._pico_service:get({{'{plugin}', '{service}', "
        f"'0.1.0'}})[5]"
    )


def get_seen_config(service, instance):
    return instance.eval(f"return _G['plugin_state']['{service}']['current_config']")


def assert_config(plugin, service, expected_cfg, *instances):
    for i in instances:
        cfg_space = get_config(plugin, service, i)
        assert cfg_space == expected_cfg
        cfg_seen = get_seen_config(service, i)
        assert cfg_seen == expected_cfg


def test_on_config_update(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    assert_config(_PLUGIN, "testservice_1", _DEFAULT_CFG, i1, i2)

    i1.eval(
        "pico.update_plugin_config('testplug', 'testservice_1', {foo = "
        "true, bar = 102, baz = {'a', 'b'}})"
    )
    assert_config(_PLUGIN, "testservice_1", _NEW_CFG, i1, i2)


def test_plugin_double_config_update(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    i1.eval(
        'pico.update_plugin_config("testplug", "testservice_1", {foo = '
        'true, bar = 102, baz = {"a", "b"}})'
        'return pico.update_plugin_config("testplug", "testservice_1",'
        '{foo = false, bar = 102, baz = {"a", "b"}})'
    )
    # both configs were applied
    assert_cb_called("testservice_1", "on_config_change", 2, i1, i2)
    assert_config(_PLUGIN, "testservice_1", _NEW_CFG_2, i1, i2)

    i1.eval(
        'pico.update_plugin_config("testplug", "testservice_1", {foo = '
        'true, bar = 102, baz = {"a", "b"}})'
    )
    i2.eval(
        'pico.update_plugin_config("testplug", "testservice_1", {foo = '
        'true, bar = 102, baz = {"a", "b"}})'
    )
    # both configs were applied and result config may be any of applied
    assert_cb_called("testservice_1", "on_config_change", 4, i1, i2)


def assert_poisoned(poison_instance_id, plugin, service, *instances):
    for i in instances:
        info = get_route(poison_instance_id, plugin, service, i)
        assert info is not None
        assert info[3]


def assert_not_poisoned(poison_instance_id, plugin, service, *instances):
    for i in instances:
        info = get_route(poison_instance_id, plugin, service, i)
        assert info is not None
        assert not info[3]


def test_error_on_config_update(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    assert_config(_PLUGIN, "testservice_1", _DEFAULT_CFG, i1, i2)

    inject_error("testservice_1", "on_cfg_change", "test error", i1)

    i1.eval(
        "pico.update_plugin_config('testplug', 'testservice_1', {foo = "
        "true, bar = 102, baz = {'a', 'b'}})"
    )

    # check that at i1 new configuration exists in global space
    # but not really applied to service because error
    cfg_space = get_config(_PLUGIN, "testservice_1", i1)
    assert cfg_space == _NEW_CFG
    cfg_seen = get_seen_config("testservice_1", i1)
    assert cfg_seen == _DEFAULT_CFG
    assert_config(_PLUGIN, "testservice_1", _NEW_CFG, i2)

    # assert that the first instance now has a poison service
    # and the second instance is not poisoned
    assert_poisoned(i1.instance_id, _PLUGIN, "testservice_1", i1, i2)
    assert_not_poisoned(i2.instance_id, _PLUGIN, "testservice_1", i1, i2)


def test_instance_service_poison_and_healthy_then(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    assert_config(_PLUGIN, "testservice_1", _DEFAULT_CFG, i1, i2)
    inject_error("testservice_1", "on_cfg_change", "test error", i1)

    i1.eval(
        "pico.update_plugin_config('testplug', 'testservice_1', {foo = "
        "true, bar = 102, baz = {'a', 'b'}})"
    )

    # assert that the first instance now has a poison service
    assert_poisoned(i1.instance_id, _PLUGIN, "testservice_1", i1, i2)

    remove_error("testservice_1", "on_cfg_change", i1)

    i1.eval(
        "pico.update_plugin_config('testplug', 'testservice_1', {foo = "
        "false, bar = 102, baz = {'a', 'b'}})"
    )

    assert_not_poisoned(i1.instance_id, _PLUGIN, "testservice_1", i1, i2)
    assert_config(_PLUGIN, "testservice_1", _NEW_CFG_2, i1, i2)


def test_on_leader_change(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_id="r1", wait_online=True)
    i2 = cluster.add_instance(replicaset_id="r1", wait_online=True)
    i3 = cluster.add_instance(replicaset_id="r1", wait_online=True)

    masters = [i for i in cluster.instances if not i.eval("return box.info.ro")]
    assert masters[0] == i1

    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2, i3)

    assert_last_seen_ctx("testservice_1", {"is_master": True}, i1)
    assert_last_seen_ctx("testservice_1", {"is_master": False}, i2, i3)

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
    assert_cb_called("testservice_1", "on_leader_change", 1, i1, i2)
    assert_cb_called("testservice_1", "on_leader_change", None, i3)

    # i1 and i3 known that they are not a leader; i2 know that he is a leader
    assert_last_seen_ctx("testservice_1", {"is_master": False}, i1, i3)
    assert_last_seen_ctx("testservice_1", {"is_master": True}, i2)


def test_error_on_leader_change(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_id="r1", wait_online=True)
    i2 = cluster.add_instance(replicaset_id="r1", wait_online=True)

    masters = [i for i in cluster.instances if not i.eval("return box.info.ro")]
    assert masters[0] == i1

    i1.call("pico.create_plugin", _PLUGIN, timeout=_3_SEC)
    assert_plugin_exists(_PLUGIN, _PLUGIN_SERVICES, i1, i2)

    inject_error("testservice_1", "on_leader_change", True, i1)

    index = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r1"],
        ops=[("=", "target_master_id", i2.instance_id)],
    )
    cluster.raft_wait_index(index)

    masters = [i for i in cluster.instances if not i.eval("return box.info.ro")]
    assert masters[0] == i2

    assert_last_seen_ctx("testservice_1", {"is_master": True}, i2)

    # assert that the first instance now has a poison service
    # and the second instance is not poisoned
    assert_poisoned(i1.instance_id, _PLUGIN, "testservice_1", i1, i2)
    assert_not_poisoned(i2.instance_id, _PLUGIN, "testservice_1", i1, i2)
