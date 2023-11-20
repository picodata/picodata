import funcy  # type: ignore
import pytest
import time

from conftest import (
    Cluster,
    Instance,
    Retriable,
)


@pytest.fixture
def cluster3(cluster: Cluster):
    cluster.deploy(instance_count=3, init_replication_factor=3)
    return cluster


@funcy.retry(tries=30, timeout=0.2)
def wait_repl_master(i: Instance, other_than=None):
    repl_master = i.eval(
        """
        local rid = pico.instance_info(...).replicaset_id
        return box.space._pico_replicaset:get(rid).current_master_id
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
        assert (k, vclock_actual[k]) >= (k, v_exp)


# fmt: off
def test_2_of_3_writable(cluster3: Cluster):
    i1, i2, i3 = cluster3.instances

    rm = wait_repl_master(i1)
    assert wait_repl_master(i2) == rm
    assert wait_repl_master(i3) == rm

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
    assert i2.eval("return box.space.test_space:select()") == [[1]]

    wait_vclock(i3, rl_vclock)
    assert i3.eval("return box.space.test_space:select()") == [[1]]

    master.terminate()

    rm = wait_repl_master(i2, other_than=rm)
    assert wait_repl_master(i3) == rm

    old_leader = master
    master, i3 = sorted(
        [i2, i3],
        key=lambda i: i.eval("return box.info.id") == rm,
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
    assert i3.eval("return box.space.test_space:select()") == [[1], [2]]

    print(i3.call("pico.raft_log", dict(return_string=True)))

    print(f"{old_leader=}")
    old_leader.start()
    old_leader.wait_online()
    assert wait_repl_master(old_leader) == rm
    wait_vclock(old_leader, rl_vclock)
    assert old_leader.eval("return box.space.test_space:select()") == [[1], [2]]
# fmt: on


def test_replication_works(cluster: Cluster):
    cluster.deploy(instance_count=1, init_replication_factor=2)
    i2 = cluster.add_instance(wait_online=False, replicaset_id="r2")
    i3 = cluster.add_instance(wait_online=False, replicaset_id="r2")
    i2.start()
    i3.start()
    i2.wait_online()
    i3.wait_online()

    @funcy.retry(tries=10, timeout=0.5)
    def wait_replicas_joined(i: Instance, n: int):
        assert len(i.call("box.info")["replication"]) == n

    wait_replicas_joined(i2, 2)
    wait_replicas_joined(i3, 2)


def test_master_auto_switchover(cluster: Cluster):
    # These guys are for quorum.
    i1, i2, i3 = cluster.deploy(instance_count=3)
    # These are being tested.
    i4 = cluster.add_instance(wait_online=True, replicaset_id="r99")
    i5 = cluster.add_instance(wait_online=True, replicaset_id="r99")

    # i4 is master as the first member of the replicaset.
    assert wait_repl_master(i4) == i4.instance_id
    assert wait_repl_master(i5) == i4.instance_id
    assert not i4.eval("return box.info.ro")
    assert i5.eval("return box.info.ro")

    # Terminate master to force switchover.
    i4.terminate()
    assert wait_repl_master(i5) == i5.instance_id
    assert not i5.eval("return box.info.ro")

    # Terminate the last remaining replica, switchover is impossible.
    i5.terminate()
    # FIXME: wait until governor handles all pending events
    time.sleep(0.5)
    assert (
        i1.eval("return box.space._pico_replicaset:get(...).current_master_id", "r99")
        == i5.instance_id
    )

    # Wake the master back up, check it's not read only.
    i5.start()
    i5.wait_online()
    assert wait_repl_master(i5) == i5.instance_id
    assert not i5.eval("return box.info.ro")

    # Terminate it again, to check switchover at catch-up.
    i5.terminate()
    i4.start()
    i4.wait_online()

    # i4 is master again.
    assert wait_repl_master(i4) == i4.instance_id
    assert not i4.eval("return box.info.ro")

    i5.start()
    i5.wait_online()
    # i5 is still read only.
    assert wait_repl_master(i5) == i4.instance_id
    assert i5.eval("return box.info.ro")

    # Manually change master back to i5
    index = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r99"],
        ops=[("=", "target_master_id", i5.instance_id)],
    )
    cluster.raft_wait_index(index)

    assert wait_repl_master(i4) == i5.instance_id
    assert i4.eval("return box.info.ro")
    assert wait_repl_master(i5) == i5.instance_id
    assert not i5.eval("return box.info.ro")


def wait_governor_status(i: Instance, expected_status, timeout=5):
    assert expected_status != "not a leader", "use another function"

    class NotALeader(Exception):
        pass

    def impl():
        actual_status = i.call("pico._governor_loop_status")
        if actual_status == "not a leader":
            raise NotALeader("not a leader")

        assert actual_status == expected_status

    Retriable(timeout=timeout, rps=1, fatal=NotALeader).call(impl)


def get_vclock_without_local(i: Instance):
    vclock = i.eval("return box.info.vclock")
    del vclock[0]

    vclock_array = [0] * (max(vclock.keys()) + 1)
    for k, v in vclock.items():
        vclock_array[k] = v

    return vclock_array


def test_replication_sync_before_master_switchover(cluster: Cluster):
    # These guys are for quorum.
    i1, i2, i3 = cluster.deploy(instance_count=3)
    # These are being tested.
    i4 = cluster.add_instance(wait_online=True, replicaset_id="r99")
    i5 = cluster.add_instance(wait_online=True, replicaset_id="r99")

    # Temporarilly break i5's replication config, so that it's vclock is outdated.
    print("\x1b[31mbreaking i5's replication config\x1b[0m")
    i5.eval(
        """
        replication_before = box.cfg.replication
        box.cfg { replication = {} }
        """
    )

    # Do some storage modifications, which will need to be replicated.
    i4.eval(
        """
        box.schema.space.create('mytable')
        box.space.mytable:create_index('pk')
        box.space.mytable:insert{1, 'hello'}
        box.space.mytable:insert{2, 'there'}
        box.space.mytable:insert{3, 'partner'}
        """
    )

    # Make sure i1 is leader.
    i1.promote_or_fail()

    # Initiate master switchover.
    index = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r99"],
        ops=[("=", "target_master_id", i5.instance_id)],
    )
    cluster.raft_wait_index(index)

    master_vclock = get_vclock_without_local(i4)

    # Wait until governor starts switching the replication leader from i4 to i5.
    # This will block until i5 synchronizes with old master, which it won't
    # until we fix it's replication config.
    time.sleep(1)  # Just in case, nothing really relies on this sleep
    wait_governor_status(i1, "transfer replication leader")

    assert i5.eval("return box.space.mytable") is None
    vclock = get_vclock_without_local(i5)
    assert vclock != master_vclock
    assert (
        i5.eval(
            "return box.space._pico_replicaset:get(...).current_master_id",
            i5.replicaset_id,
        )
        == i4.instance_id
    )

    # Fix i5's replication config, so it's able to continue synching.
    print("\x1b[32mfixing i5's replication config\x1b[0m")
    i5.eval("box.cfg { replication = replication_before }")

    # Wait until governor finishes with all the needed changes.
    wait_governor_status(i1, "idle")

    assert i5.eval("return box.space.mytable.id") is not None
    vclock = get_vclock_without_local(i5)
    assert vclock >= master_vclock
    assert (
        i5.eval(
            "return box.space._pico_replicaset:get(...).current_master_id",
            i5.replicaset_id,
        )
        == i5.instance_id
    )
