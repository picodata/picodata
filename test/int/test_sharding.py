import funcy  # type: ignore
import time

from conftest import (
    Cluster,
    Instance,
)


def test_sharding_reinitializes_on_restart(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    assert i1.call("vshard.router.info") is not None

    incarnation = i1.eval("return pico.instance_info().current_grade.incarnation")

    # Instance silently dies without it's grade being updated
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


def wait_has_buckets(c: Cluster, i: Instance, expected_active: int):
    @funcy.retry(tries=30, timeout=0.5)
    def buckets_active(i: Instance):
        return i.call("vshard.storage.info")["bucket"]["active"]

    tries = 4
    previous_active = None
    while True:
        for j in c.instances:
            j.eval(
                """
                if vshard.storage.internal.rebalancer_fiber ~= nil then
                    vshard.storage.rebalancer_wakeup()
                end
            """
            )

        actual_active = buckets_active(i)
        if actual_active == expected_active:
            return

        if actual_active == previous_active:
            if tries > 0:
                tries -= 1
            else:
                print("vshard.storage.info.bucket.active stopped changing")
                assert actual_active == expected_active

        previous_active = actual_active

        time.sleep(0.5)


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
    wait_has_buckets(cluster, i3, 3000)


def test_bucket_rebalancing(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1, init_replication_factor=1)
    wait_has_buckets(cluster, i1, 3000)

    i2 = cluster.add_instance()
    wait_has_buckets(cluster, i2, 1500)

    i3 = cluster.add_instance()
    wait_has_buckets(cluster, i3, 1000)


def test_bucket_rebalancing_respects_replication_factor(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)

    # wait for buckets to be rebalanced between 2 replicasets 1500 each
    for i in cluster.instances:
        wait_has_buckets(cluster, i, 1500)

    # check vshard routes requests to both replicasets
    reached_instances = set()
    for bucket_id in [1, 3000]:
        info = i1.call("vshard.router.callro", bucket_id, "pico.instance_info")
        reached_instances.add(info["instance_id"])
    assert len(reached_instances) == 2

    # add an instance to a new replicaset
    i5 = cluster.add_instance(wait_online=True)

    # buckets do not start rebalancing until new replicaset is full
    wait_has_buckets(cluster, i5, 0)
    for i in cluster.instances:
        if i.instance_id != i5.instance_id:
            wait_has_buckets(cluster, i, 1500)

    # add another instance to new replicaset, it's now full
    cluster.add_instance(wait_online=True)

    # buckets now must be rebalanced between 3 replicasets 1000 each
    for i in cluster.instances:
        wait_has_buckets(cluster, i, 1000)

    # check vshard routes requests to all 3 replicasets
    reached_instances = set()
    for bucket_id in [1, 1500, 3000]:
        info = i1.call("vshard.router.callro", bucket_id, "pico.instance_info")
        reached_instances.add(info["instance_id"])
    assert len(reached_instances) == 3
