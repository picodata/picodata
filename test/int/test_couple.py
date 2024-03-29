import pytest
from conftest import Cluster, Instance, Retriable


@pytest.fixture
def cluster2(cluster: Cluster):
    cluster.deploy(instance_count=2)
    return cluster


def test_switchover(cluster2: Cluster):
    i1, i2 = cluster2.instances

    i1.promote_or_fail()
    i1.assert_raft_status("Leader")

    i2.promote_or_fail()
    i2.assert_raft_status("Leader")

    # Check idempotency
    i2.promote_or_fail()


def test_failover(cluster2: Cluster):
    i1, i2 = cluster2.instances
    i1.promote_or_fail()

    Retriable(timeout=2, rps=10).call(
        i2.assert_raft_status, "Follower", leader_id=i1.raft_id
    )

    def do_test():
        i2.eval("pico.raft_tick(20)")
        i1.assert_raft_status("Follower", leader_id=i2.raft_id)
        i2.assert_raft_status("Leader")

    Retriable(timeout=2, rps=10).call(do_test)


def test_restart_follower(cluster2: Cluster):
    # Given a cluster of two instances - i1 (leader) and i2 (follower)
    # When i2 restarts
    # Then it's able to start and remain a follower

    i1, i2 = cluster2.instances
    assert i1.current_grade() == dict(variant="Online", incarnation=1)
    assert i2.current_grade() == dict(variant="Online", incarnation=1)
    i2.restart()
    i2.wait_online()
    assert i2.current_grade() == dict(variant="Online", incarnation=2)
    i1.assert_raft_status("Leader")
    i2.assert_raft_status("Follower")

    i2.restart()
    i2.wait_online()
    assert i2.current_grade() == dict(variant="Online", incarnation=3)


def test_restart_leader(cluster2: Cluster):
    # Given a cluster of two instances - i1 (leader) and i2 (follower)
    # When i1 restarts
    # Then it's able to start and make a proposal.
    # No assuptions about leadership are made though.

    i1, _ = cluster2.instances
    assert i1.current_grade() == dict(variant="Online", incarnation=1)
    i1.restart()
    i1.wait_online()
    assert i1.current_grade() == dict(variant="Online", incarnation=2)

    i1.restart()
    i1.wait_online()
    assert i1.current_grade() == dict(variant="Online", incarnation=3)


def test_restart_both(cluster2: Cluster):
    # Given a cluster of 2 instances - i1, i2
    # When both instances are stopped and then started again
    # Then both can become ready and handle proposals.

    i1, i2 = cluster2.instances
    assert i1.current_grade() == dict(variant="Online", incarnation=1)
    assert i2.current_grade() == dict(variant="Online", incarnation=1)
    i1.terminate()
    i2.terminate()

    def check_alive(instance: Instance):
        assert instance.call(".proc_raft_info")["leader_id"] == 0

    i1.start()
    # This synchronization is necessary for proper test case reproducing.
    # i1 has already initialized raft node but can't win election yet
    # i2 starts discovery and should be able to advance further
    Retriable(timeout=2, rps=10).call(check_alive, i1)

    i2.start()
    Retriable(timeout=2, rps=10).call(check_alive, i2)

    # Speed up elections
    i2.promote_or_fail()

    i1.wait_online()
    assert i1.current_grade() == dict(variant="Online", incarnation=2)
    i2.wait_online()
    assert i1.current_grade() == dict(variant="Online", incarnation=2)

    index = cluster2.cas("insert", "_pico_property", ("check", True))
    i1.raft_wait_index(index)
    i2.raft_wait_index(index)
    assert i1.eval("return box.space._pico_property:get('check')")[1] is True
    assert i2.eval("return box.space._pico_property:get('check')")[1] is True


def test_exit_after_persist_before_commit(cluster2: Cluster):
    [i1, i2] = cluster2.instances

    # Make sure i1 is raft leader
    i1.promote_or_fail()

    # Reduce the maximum interval between heartbeats, so that we don't have to
    # wait for eternity
    index = i1.cas("replace", "_pico_property", ["max_heartbeat_period", 0.5])
    i1.raft_wait_index(index)

    i2.call("pico._inject_error", "EXIT_AFTER_RAFT_PERSISTS_ENTRIES", True)

    index = cluster2.cas("insert", "_pico_property", ["foo", "bar"])

    # The injected error forces the instance to exit right after persisting the
    # raft log entries, but before committing them to raft.
    i2.wait_process_stopped()

    # Instance restarts successfully and the entry is eventually applied.
    i2.start()
    i2.raft_wait_index(index)
    assert i2.eval("return box.space._pico_property:get('foo').value") == "bar"


def test_exit_after_commit_before_apply(cluster2: Cluster):
    [i1, i2] = cluster2.instances

    # Make sure i1 is raft leader
    i1.promote_or_fail()

    i2.call("pico._inject_error", "EXIT_AFTER_RAFT_PERSISTS_HARD_STATE", True)

    index = cluster2.cas("insert", "_pico_property", ["foo", "bar"])

    # The injected error forces the instance to exit right after persisting the
    # hard state, but before applying the entries to the local storage.
    i2.wait_process_stopped()

    # Instance restarts successfully and the entry is eventually applied.
    i2.start()
    i2.raft_wait_index(index)
    assert i2.eval("return box.space._pico_property:get('foo').value") == "bar"


def test_exit_after_apply(cluster2: Cluster):
    [i1, i2] = cluster2.instances

    # Make sure i1 is raft leader
    i1.promote_or_fail()

    i2.call("pico._inject_error", "EXIT_AFTER_RAFT_HANDLES_COMMITTED_ENTRIES", True)

    index = cluster2.cas("insert", "_pico_property", ["foo", "bar"])

    # The injected error forces the instance to exit right after applying the
    # committed entry, but before letting raft-rs know about it.
    i2.wait_process_stopped()

    # Instance restarts successfully and the entry is eventually applied.
    i2.start()
    i2.raft_wait_index(index)
    assert i2.eval("return box.space._pico_property:get('foo').value") == "bar"
