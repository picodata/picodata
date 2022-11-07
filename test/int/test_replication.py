import funcy  # type: ignore
import pytest

from conftest import (
    Cluster,
    Instance,
)


@pytest.fixture
def cluster3(cluster: Cluster):
    cluster.deploy(instance_count=3, init_replication_factor=3)
    return cluster


@funcy.retry(tries=30, timeout=0.2)
def wait_repl_master(i: Instance, other_than=None):
    repl_master = i.eval(
        """
        local rid = picolib.peer_info(...).replicaset_id
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

    print(i3.call("picolib.raft_log", dict(return_string=True)))

    print(f"{old_leader=}")
    old_leader.start()
    old_leader.wait_online()
    assert wait_repl_master(old_leader) == rm
    wait_vclock(old_leader, rl_vclock)
    assert [[1], [2]] == old_leader.eval("return box.space.test_space:select()")
# fmt: on
