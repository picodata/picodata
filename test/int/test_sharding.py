import funcy  # type: ignore
import time
import pytest

from conftest import (
    Cluster,
    Instance,
    Retriable,
    log_crawler,
)


def vshard_router_info(instance: Instance):
    return instance.eval(
        """
    local router = pico.router["default"]
    return router:info()
            """
    )


def vshard_bootstrapped(instance: Instance):
    result = instance.sql("""SELECT "vshard_bootstrapped" FROM "_pico_tier" where "name" = 'default' """)
    return result[0][0]


def test_sharding_reinitializes_on_restart(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    assert vshard_router_info(i1) is not None

    info = i1.call(".proc_instance_info")
    incarnation = info["current_state"]["incarnation"]

    # Instance silently dies without it's state being updated
    i1.kill()

    # Instance restarts, it's incarnation is updated and governor reconfigures
    # all the subsystems
    i1.start()
    i1.wait_online(expected_incarnation=incarnation + 1)

    # Vshard is configured even though the configuration didn't change
    assert vshard_router_info(i1) is not None


def test_bucket_discovery_single(instance: Instance):
    @funcy.retry(tries=30, timeout=0.5)
    def wait_buckets_awailable(i: Instance, expected: int):
        assert vshard_router_info(i)["bucket"]["available_rw"] == expected

    wait_buckets_awailable(instance, 3000)


def test_bucket_discovery_respects_replication_factor(cluster: Cluster):
    i1 = cluster.add_instance(init_replication_factor=2)
    time.sleep(0.5)
    assert i1.eval("return rawget(_G, 'vshard') == nil")
    assert not vshard_bootstrapped(i1)

    i2 = cluster.add_instance(replicaset_name="default_2")
    time.sleep(0.5)
    assert i2.eval("return rawget(_G, 'vshard') == nil")
    assert not vshard_bootstrapped(i2)

    i3 = cluster.add_instance(replicaset_name="default_1")
    time.sleep(0.5)
    assert vshard_bootstrapped(i3)
    cluster.wait_until_instance_has_this_many_active_buckets(i3, 3000)


def test_automatic_bucket_rebalancing(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_name="r1")
    cluster.wait_until_instance_has_this_many_active_buckets(i1, 3000)

    i2 = cluster.add_instance(replicaset_name="r2")
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1500)

    i3 = cluster.add_instance(replicaset_name="r3")
    cluster.wait_until_instance_has_this_many_active_buckets(i3, 1000)

    # Set one of the replicaset's weight to 0, to trigger rebalancing from it.
    i1.sql(
        """
            UPDATE "_pico_replicaset"
            SET "weight" = 0, "weight_origin" = 'user'
            WHERE "name" = 'r1'
        """
    )

    # Note: currently we must bump the target vshard config version explicitly
    # to trigger the actual reconfiguration. This should be done automatically
    # in the user-facing API which will be implemented as part of this issue:
    # https://git.picodata.io/picodata/picodata/picodata/-/issues/787
    i1.sql(
        """
            UPDATE _pico_tier
            SET target_vshard_config_version = current_vshard_config_version + 1
            WHERE name = 'default'
        """
    )

    # This instnace now has no buckets
    cluster.wait_until_instance_has_this_many_active_buckets(i1, 0)

    # Set another replicaset's weight to 0.5, to showcase weights are respected.
    i1.sql(
        """
            UPDATE "_pico_replicaset"
            SET "weight" = 0.5, "weight_origin" = 'user'
            WHERE "name" = 'r2'
        """
    )

    # FIXME: https://git.picodata.io/picodata/picodata/picodata/-/issues/787
    i1.sql(
        """
            UPDATE _pico_tier
            SET target_vshard_config_version = current_vshard_config_version + 1
            WHERE name = 'default'
        """
    )

    # Now i3 has twice as many buckets, because r3.weight == r2.weight * 2
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1000)
    cluster.wait_until_instance_has_this_many_active_buckets(i3, 2000)


def test_bucket_rebalancing_respects_replication_factor(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)

    # wait for buckets to be rebalanced between 2 replicasets 1500 each
    for i in cluster.instances:
        cluster.wait_until_instance_has_this_many_active_buckets(i, 1500)

    # check vshard routes requests to both replicasets
    reached_instances = set()
    for bucket_id in [1, 3000]:
        info = i1.eval(
            f"""
        local router = pico.router["default"]
        return router:callro({bucket_id}, ".proc_instance_info")
                       """
        )
        reached_instances.add(info["name"])
    assert len(reached_instances) == 2

    # add an instance to a new replicaset
    i5 = cluster.add_instance(wait_online=True)

    # buckets do not start rebalancing until new replicaset is full
    cluster.wait_until_instance_has_this_many_active_buckets(i5, 0)
    for i in cluster.instances:
        if i.name != i5.name:
            cluster.wait_until_instance_has_this_many_active_buckets(i, 1500)

    # add another instance to new replicaset, it's now full
    cluster.add_instance(wait_online=True)

    # buckets now must be rebalanced between 3 replicasets 1000 each
    for i in cluster.instances:
        cluster.wait_until_instance_has_this_many_active_buckets(i, 1000)

    # check vshard routes requests to all 3 replicasets
    reached_instances = set()
    for bucket_id in [1, 1500, 3000]:
        info = i1.eval(
            f"""
        local router = pico.router["default"]
        return router:callro({bucket_id}, ".proc_instance_info")
                       """
        )
        reached_instances.add(info["name"])
    assert len(reached_instances) == 3


def get_vshards_opinion_about_replicaset_masters(i: Instance):
    return i.eval(
        """
            local res = {}
            local router = pico.router["default"]
            for _, r in pairs(router.replicasets) do
                res[r.uuid] = r.master.name
            end
            return res
        """
    )


def wait_current_vshard_config_changed(peer: Instance, old_version, timeout=5):
    def impl():
        rows = peer.sql(""" SELECT current_vshard_config_version FROM _pico_tier WHERE name = 'default' """)
        new_version = rows[0][0]
        assert new_version != old_version

    Retriable(timeout=timeout, rps=10).call(impl)


def test_vshard_updates_on_master_change(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_name="r1", wait_online=True)
    i2 = cluster.add_instance(replicaset_name="r1", wait_online=True)
    i3 = cluster.add_instance(replicaset_name="r2", wait_online=True)
    i4 = cluster.add_instance(replicaset_name="r2", wait_online=True)

    r1_uuid = i1.eval("return box.space._pico_replicaset:get('r1').uuid")
    r2_uuid = i1.eval("return box.space._pico_replicaset:get('r2').uuid")

    for i in cluster.instances:
        replicaset_masters = get_vshards_opinion_about_replicaset_masters(i)
        # The first instance in the replicaset becomes it's master
        assert replicaset_masters[r1_uuid] == i1.name
        assert replicaset_masters[r2_uuid] == i3.name

    old_step_counter = i1.governor_step_counter()

    rows = i1.sql(""" SELECT current_vshard_config_version FROM _pico_tier WHERE name = 'default' """)
    old_vshard_config_version = rows[0][0]

    update_target_master = """
        UPDATE _pico_replicaset SET target_master_name = ? WHERE name = ?
    """
    i1.sql(update_target_master, i2.name, "r1")
    i1.sql(update_target_master, i4.name, "r2")

    # Wait until governor performs all the necessary actions
    i1.wait_governor_status("idle", old_step_counter=old_step_counter)

    # Make sure vshard config version changed.
    wait_current_vshard_config_changed(i1, old_vshard_config_version)

    for i in cluster.instances:
        replicaset_masters = get_vshards_opinion_about_replicaset_masters(i)
        # Now the chosen instances are replicaset masters
        assert replicaset_masters[r1_uuid] == i2.name
        assert replicaset_masters[r2_uuid] == i4.name


def test_vshard_bootstrap_timeout(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False, init_replication_factor=1)

    error_injection = "SHARDING_BOOTSTRAP_SPURIOUS_FAILURE"
    injection_log = f"ERROR INJECTION '{error_injection}'"
    lc1 = log_crawler(i1, injection_log)

    i1.env[f"PICODATA_ERROR_INJECTION_{error_injection}"] = "1"
    i1.start()

    # Wait until governor starts trying to bootstrap bucket distribution
    # and triggers the injected error
    lc1.wait_matched()

    assert not vshard_bootstrapped(i1)

    i1.call("pico._inject_error", error_injection, False)

    i1.wait_online()

    lc1.wait_matched()

    assert vshard_bootstrapped(i1)


def test_gitlab_763_no_missing_buckets_after_proc_sharding_failure(cluster: Cluster):
    # Need 3 instances for quorum
    i1, i2, i3 = cluster.deploy(instance_count=3)

    # Wait until buckets are balanced
    for i in cluster.instances:
        cluster.wait_until_instance_has_this_many_active_buckets(i, 1000)

    lc = log_crawler(i1, "ERROR INJECTION 'PROC_SHARDING_SPURIOUS_FAILURE'")

    # Enable error injection so that .proc_sharding fails after configuring the vshard
    i1.call("pico._inject_error", "PROC_SHARDING_SPURIOUS_FAILURE", True)

    # Terminate one of the instances to trigger vshard reconfiguration
    i3.terminate()

    # Wait until governor started trying to reconfigure vshard and triggers the injected error
    lc.wait_matched()

    # At this point the governor is trying to configure vshard but is failing
    # because of the injected error and is infinitely retrying.

    # Restart the previously offline instance.
    i3.start()

    # Instance cannot become online until vshard is reconfigured
    with pytest.raises(AssertionError):
        i3.wait_online()

    # Disable the synthetic failure so that instance can come online
    i1.call("pico._inject_error", "PROC_SHARDING_SPURIOUS_FAILURE", False)

    # Wait until buckets are balanced
    for i in cluster.instances:
        cluster.wait_until_instance_has_this_many_active_buckets(i, 1000)

    def check_available_buckets(i: Instance, count: int):
        info = vshard_router_info(i)
        assert info["bucket"]["available_rw"] == count

    # All buckets are eventually available to the whole cluster
    for i in cluster.instances:
        Retriable(timeout=10, rps=4).call(check_available_buckets, i, 3000)


def get_table_size(instance: Instance, table_name: str):
    table_size = instance.eval(f"return box.space.{table_name}:count()")
    return table_size


def test_is_bucket_rebalancing_means_data_migration(cluster: Cluster):
    i1 = cluster.add_instance()
    cluster.wait_until_instance_has_this_many_active_buckets(i1, 3000)

    ddl = i1.sql(
        """
        CREATE TABLE "sharded_table" ( "id" INTEGER NOT NULL, PRIMARY KEY ("id") )
        DISTRIBUTED BY ("id")
        """
    )
    assert ddl["row_count"] == 1

    table_size = 30000
    batch_size = 1000
    for start in range(1, table_size, batch_size):
        response = i1.sql(
            "INSERT INTO sharded_table VALUES " + (", ".join([f"({i})" for i in range(start, start + batch_size)]))
        )
        assert response["row_count"] == batch_size

    assert get_table_size(i1, "sharded_table") == table_size

    bucket_id_index_in_format = 1
    format = i1.eval("return box.space.sharded_table:format()")
    assert format[bucket_id_index_in_format]["name"] == "bucket_id"

    data = i1.eval("return box.space.sharded_table:select()")
    bucket_ids_of_table = set([tuple[bucket_id_index_in_format] for tuple in data])

    # in picodata amount of buckets fixed and equal to 3000
    all_bucket_ids = set([i + 1 for i in range(3000)])
    assert len(all_bucket_ids - bucket_ids_of_table) == 0

    for _ in range(9):
        cluster.add_instance()

    others = cluster.instances[1:]

    # wait until vshard rebalancing done
    for instance in others:
        cluster.wait_until_instance_has_this_many_active_buckets(instance, 300, max_retries=100)

    for instance in others:
        assert get_table_size(instance, "sharded_table") > 0


def test_expel_blocked_by_bucket_rebalancing(cluster: Cluster):
    # Need 3 instances for quorum
    cluster.set_service_password("secret")
    i1 = cluster.add_instance(wait_online=False, replicaset_name="r1")
    i2 = cluster.add_instance(wait_online=False, replicaset_name="r2")
    i3 = cluster.add_instance(wait_online=False, replicaset_name="r3")
    cluster.wait_online()

    # We have 3 replicasets 1 replica each
    rows = i1.sql(""" SELECT name, weight FROM _pico_replicaset ORDER BY name """)
    assert rows == [
        ["r1", 1.0],
        ["r2", 1.0],
        ["r3", 1.0],
    ]
    cluster.wait_until_instance_has_this_many_active_buckets(i1, 1000)
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1000)
    cluster.wait_until_instance_has_this_many_active_buckets(i3, 1000)

    # Expel one of the instances
    cluster.expel(i3)
    Retriable(timeout=30).call(i3.assert_process_dead)

    # We now have 2 replicasets 1 replica each
    rows = i1.sql(""" SELECT name, weight FROM _pico_replicaset ORDER BY name """)
    assert rows == [
        ["r1", 1.0],
        ["r2", 1.0],
        ["r3", 0.0],
    ]
    cluster.wait_until_instance_has_this_many_active_buckets(i1, 1500)
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1500)


def assert_tier_bucket_count(cluster: Cluster, name: str, bucket_count: int, *instances: Instance):
    assert len(instances) > 0

    i1 = instances[0]
    # default `bucket_count` is 3000
    rows = i1.sql(""" SELECT name, bucket_count FROM _pico_tier WHERE name = ?""", name)
    assert rows == [
        [name, bucket_count],
    ]

    # 3000 bucket counts, 3 replicasets
    for x in instances:
        cluster.wait_until_instance_has_this_many_active_buckets(x, int(bucket_count / len(instances)))


def test_bucket_count_custom_and_default(cluster: Cluster):
    cluster.set_service_password("secret")
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    default_bucket_count: 6000
    tier:
        radix:
            replication_factor: 1
            bucket_count: 16384
        storage:
            replication_factor: 1
"""
    )
    i1 = cluster.add_instance(tier="storage", wait_online=False)
    i2 = cluster.add_instance(tier="storage", wait_online=False)
    i3 = cluster.add_instance(tier="storage", wait_online=False)
    i4 = cluster.add_instance(tier="radix", wait_online=False)
    i5 = cluster.add_instance(tier="radix", wait_online=False)
    cluster.wait_online()

    assert_tier_bucket_count(cluster, "storage", 6000, i1, i2, i3)
    assert_tier_bucket_count(cluster, "radix", 16384, i4, i5)
