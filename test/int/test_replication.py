import funcy  # type: ignore
import pytest
import time

from conftest import (
    Cluster,
    Instance,
)


@pytest.fixture
def cluster1(cluster: Cluster):
    cluster.deploy(instance_count=1, init_replication_factor=2)
    return cluster


@pytest.fixture
def cluster3(cluster: Cluster):
    cluster.deploy(instance_count=3, init_replication_factor=3)
    return cluster


@funcy.retry(tries=30, timeout=0.2)
def wait_repl_master(i: Instance, other_than=None):
    repl_master = i.eval(
        """
        local rid = pico.peer_info(...).replicaset_id
        return box.space.replicasets:get(rid).master_id
    """,
        i.instance_id,
    )
    assert repl_master
    if other_than:
        assert repl_master != other_than
    return repl_master


@funcy.retry(tries=60, timeout=0.2)
def wait_vclock(i: Instance, vclock_expected: dict[int, int]):
    vclock_actual = i.eval("return box.info.vclock")
    del vclock_actual[0]
    for k, v_exp in vclock_expected.items():
        assert (k, v_exp) <= (k, vclock_actual[k])


# fmt: off
def test_2_of_3_writable(cluster3: Cluster):
    i1, i2, i3 = cluster3.instances

    rm = wait_repl_master(i1)
    assert rm == wait_repl_master(i2)
    assert rm == wait_repl_master(i3)

    master, i2, i3 = sorted(
        [i1, i2, i3],
        key=lambda i: rm == i.instance_id,
        reverse=True
    )

    rl_vclock = master.eval("return box.info.vclock")
    del rl_vclock[0]

    wait_vclock(i2, rl_vclock)  # sometimes fails with i2 missing one transaction
    wait_vclock(i3, rl_vclock)  # sometimes fails with i3 missing one transaction

    rl_vclock = master.eval("""
        box.schema.space.create('test_space')
            :create_index('pk')
        box.space.test_space:replace {1}
        return box.info.vclock
    """)
    del rl_vclock[0]

    wait_vclock(i2, rl_vclock)
    assert [[1]] == i2.eval("return box.space.test_space:select()")

    wait_vclock(i3, rl_vclock)
    assert [[1]] == i3.eval("return box.space.test_space:select()")

    master.terminate()

    rm = wait_repl_master(i2, other_than=rm)
    assert rm == wait_repl_master(i3)

    old_leader = master
    master, i3 = sorted(
        [i2, i3],
        key=lambda i: rm == i.eval("return box.info.id"),
        reverse=True
    )

    rl_vclock = master.eval("return box.info.vclock")
    del rl_vclock[0]
    wait_vclock(i3, rl_vclock)

    rl_vclock = master.eval("""
        box.space.test_space:replace {2}
        return box.info.vclock
    """)
    del rl_vclock[0]

    wait_vclock(i3, rl_vclock)
    assert [[1], [2]] == i3.eval("return box.space.test_space:select()")

    print(i3.call("pico.raft_log", dict(return_string=True)))

    print(f"{old_leader=}")
    old_leader.start()
    old_leader.wait_online()
    assert wait_repl_master(old_leader) == rm
    wait_vclock(old_leader, rl_vclock)
    assert [[1], [2]] == old_leader.eval("return box.space.test_space:select()")
# fmt: on


def test_replication_works(cluster1: Cluster):
    i2 = cluster1.add_instance(wait_online=False, replicaset_id="r2")
    i3 = cluster1.add_instance(wait_online=False, replicaset_id="r2")
    i2.start()
    i3.start()
    i2.wait_online()
    i3.wait_online()

    @funcy.retry(tries=10, timeout=0.2)
    def wait_replicas_joined(i: Instance, n: int):
        assert len(i.call("box.info")["replication"]) == n

    wait_replicas_joined(i2, 2)
    wait_replicas_joined(i3, 2)


def test_bucket_discovery_single(instance: Instance):
    @funcy.retry(tries=30, timeout=0.2)
    def wait_buckets_awailable(i: Instance, expected: int):
        assert expected == i.call("vshard.router.info")["bucket"]["available_rw"]

    wait_buckets_awailable(instance, 3000)


@pytest.mark.xfail(
    run=True,
    reason=(
        "currently we bootstrap vshard even before the first replicaset is filled, "
        "but we shouldn't"
    ),
)
def test_bucket_discovery_respects_replication_factor(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1, init_replication_factor=2)
    time.sleep(1)
    assert 0 == i1.call("vshard.router.info")["bucket"]["available_rw"]


@funcy.retry(tries=30, timeout=0.2)
def wait_has_buckets(i: Instance, expected_active: int):
    i.call("vshard.storage.rebalancer_wakeup")
    storage_info = i.call("vshard.storage.info")
    assert expected_active == storage_info["bucket"]["active"]


def test_bucket_rebalancing(cluster: Cluster):
    i1, *_ = cluster.deploy(instance_count=1, init_replication_factor=1)

    i2 = cluster.add_instance()
    wait_has_buckets(i2, 1500)

    i3 = cluster.add_instance()
    wait_has_buckets(i3, 1000)


def test_bucket_rebalancing_respects_replication_factor(cluster: Cluster):
    peer, *_ = cluster.deploy(instance_count=4, init_replication_factor=2)

    # wait for buckets to be rebalanced between 2 replicasets 1500 each
    for i in cluster.instances:
        wait_has_buckets(i, 1500)

    # check vshard routes requests to both replicasets
    reached_instances = set()
    for bucket_id in [1, 3000]:
        info = peer.call("vshard.router.callro", bucket_id, "pico.peer_info")
        reached_instances.add(info["instance_id"])
    assert len(reached_instances) == 2

    # add an instance to a new replicaset
    i5 = cluster.add_instance(wait_online=True)

    # buckets do not start rebalancing until new replicaset is full
    wait_has_buckets(i5, 0)
    for i in cluster.instances:
        if i.instance_id != i5.instance_id:
            wait_has_buckets(i, 1500)

    # add another instance to new replicaset, it's now full
    cluster.add_instance(wait_online=True)

    # buckets now must be rebalanced between 3 replicasets 1000 each
    for i in cluster.instances:
        wait_has_buckets(i, 1000)

    # check vshard routes requests to all 3 replicasets
    reached_instances = set()
    for bucket_id in [1, 1500, 3000]:
        info = peer.call("vshard.router.callro", bucket_id, "pico.peer_info")
        reached_instances.add(info["instance_id"])
    assert len(reached_instances) == 3
