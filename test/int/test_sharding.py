import funcy  # type: ignore
import time

from conftest import (
    Cluster,
    Instance,
    Retriable,
    log_crawler,
)


def test_sharding_reinitializes_on_restart(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    assert i1.call("vshard.router.info") is not None

    info = i1.call(".proc_instance_info")
    incarnation = info["current_state"]["incarnation"]

    # Instance silently dies without it's state being updated
    i1.kill()

    # Instance restarts, it's incarnation is updated and governor reconfigures
    # all the subsystems
    i1.start()
    i1.wait_online(expected_incarnation=incarnation + 1)

    # Vshard is configured even though the configuration didn't change
    assert i1.call("vshard.router.info") is not None


def test_bucket_discovery_single(instance: Instance):
    @funcy.retry(tries=30, timeout=0.5)
    def wait_buckets_awailable(i: Instance, expected: int):
        assert i.call("vshard.router.info")["bucket"]["available_rw"] == expected

    wait_buckets_awailable(instance, 3000)


def test_bucket_discovery_respects_replication_factor(cluster: Cluster):
    i1 = cluster.add_instance(init_replication_factor=2)
    time.sleep(0.5)
    assert i1.eval("return rawget(_G, 'vshard') == nil")
    assert i1.call("box.space._pico_property:get", "vshard_bootstrapped") is None

    i2 = cluster.add_instance(replicaset_id="r2")
    time.sleep(0.5)
    assert i2.eval("return rawget(_G, 'vshard') == nil")
    assert i2.call("box.space._pico_property:get", "vshard_bootstrapped") is None

    i3 = cluster.add_instance(replicaset_id="r1")
    time.sleep(0.5)
    assert i3.call("box.space._pico_property:get", "vshard_bootstrapped")[1]
    cluster.wait_until_instance_has_this_many_active_buckets(i3, 3000)


def test_automatic_bucket_rebalancing(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_id="r1")
    cluster.wait_until_instance_has_this_many_active_buckets(i1, 3000)

    i2 = cluster.add_instance(replicaset_id="r2")
    cluster.wait_until_instance_has_this_many_active_buckets(i2, 1500)

    i3 = cluster.add_instance(replicaset_id="r3")
    cluster.wait_until_instance_has_this_many_active_buckets(i3, 1000)

    # Set one of the replicaset's weight to 0, to trigger rebalancing from it.
    index = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r1"],
        ops=[("=", "weight", 0.0), ("=", "weight_origin", "user")],
    )
    cluster.raft_wait_index(index)

    # This instnace now has no buckets
    cluster.wait_until_instance_has_this_many_active_buckets(i1, 0)

    # Set another replicaset's weight to 0.5, to showcase weights are respected.
    index = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r2"],
        ops=[("=", "weight", 0.5), ("=", "weight_origin", "user")],
    )
    cluster.raft_wait_index(index)

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
        info = i1.call("vshard.router.callro", bucket_id, ".proc_instance_info")
        reached_instances.add(info["instance_id"])
    assert len(reached_instances) == 2

    # add an instance to a new replicaset
    i5 = cluster.add_instance(wait_online=True)

    # buckets do not start rebalancing until new replicaset is full
    cluster.wait_until_instance_has_this_many_active_buckets(i5, 0)
    for i in cluster.instances:
        if i.instance_id != i5.instance_id:
            cluster.wait_until_instance_has_this_many_active_buckets(i, 1500)

    # add another instance to new replicaset, it's now full
    cluster.add_instance(wait_online=True)

    # buckets now must be rebalanced between 3 replicasets 1000 each
    for i in cluster.instances:
        cluster.wait_until_instance_has_this_many_active_buckets(i, 1000)

    # check vshard routes requests to all 3 replicasets
    reached_instances = set()
    for bucket_id in [1, 1500, 3000]:
        info = i1.call("vshard.router.callro", bucket_id, ".proc_instance_info")
        reached_instances.add(info["instance_id"])
    assert len(reached_instances) == 3


def get_vshards_opinion_about_replicaset_masters(i: Instance):
    return i.eval(
        """
            local res = {}
            for _, r in pairs(vshard.router.static.replicasets) do
                res[r.uuid] = r.master.name
            end
            return res
        """
    )


def wait_current_vshard_config_changed(
    peer: Instance, old_current_vshard_config, timeout=5
):
    def impl():
        new_current_vshard_config = peer.eval(
            "return box.space._pico_property:get('current_vshard_config').value",
        )
        assert new_current_vshard_config != old_current_vshard_config

    Retriable(timeout=timeout, rps=10).call(impl)


def test_vshard_updates_on_master_change(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_id="r1", wait_online=True)
    i2 = cluster.add_instance(replicaset_id="r1", wait_online=True)
    i3 = cluster.add_instance(replicaset_id="r2", wait_online=True)
    i4 = cluster.add_instance(replicaset_id="r2", wait_online=True)

    r1_uuid = i1.eval("return box.space._pico_replicaset:get('r1').replicaset_uuid")
    r2_uuid = i1.eval("return box.space._pico_replicaset:get('r2').replicaset_uuid")

    for i in cluster.instances:
        replicaset_masters = get_vshards_opinion_about_replicaset_masters(i)
        # The first instance in the replicaset becomes it's master
        assert replicaset_masters[r1_uuid] == i1.instance_id
        assert replicaset_masters[r2_uuid] == i3.instance_id

    old_vshard_config = i1.eval(
        "return box.space._pico_property:get('current_vshard_config').value"
    )

    cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r1"],
        ops=[("=", "target_master_id", i2.instance_id)],
    )
    index = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r2"],
        ops=[("=", "target_master_id", i4.instance_id)],
    )
    cluster.raft_wait_index(index)

    # Wait for governor to change current master and update the vshard config.
    wait_current_vshard_config_changed(i1, old_vshard_config)

    for i in cluster.instances:
        replicaset_masters = get_vshards_opinion_about_replicaset_masters(i)
        # Now the chosen instances are replicaset masters
        assert replicaset_masters[r1_uuid] == i2.instance_id
        assert replicaset_masters[r2_uuid] == i4.instance_id


def test_vshard_bootstrap_timeout(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False, init_replication_factor=1)

    injection_log = "Injection: SHARDING_BOOTSTRAP_SPURIOUS_FAILURE"
    lc1 = log_crawler(i1, injection_log)

    i1.env["PICODATA_ERROR_INJECTION_SHARDING_BOOTSTRAP_SPURIOUS_FAILURE"] = "1"
    i1.start()

    i1.wait_online()

    lc1.wait_matched()

    vshard_bootstrapped = i1.sql(
        """
        select "value" from "_pico_property" where "key" = 'vshard_bootstrapped'
        """
    )[0][0]

    assert vshard_bootstrapped is True


def test_gitlab_763_no_missing_buckets_after_proc_sharding_failure(cluster: Cluster):
    # Need 3 instances for quorum
    i1, i2, i3 = cluster.deploy(instance_count=3, init_replication_factor=1)

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

    # XXX: it's a problem that instance becomes online while the error is happening
    i3.wait_online()

    # Wait until buckets are balanced
    for i in cluster.instances:
        cluster.wait_until_instance_has_this_many_active_buckets(i, 1000)

    def check_available_buckets(i: Instance, count: int):
        info = i.call("vshard.router.info")
        assert info["bucket"]["available_rw"] == count

    # All buckets are eventually available to the whole cluster
    for i in cluster.instances:
        Retriable(timeout=10, rps=4).call(check_available_buckets, i, 3000)
