import funcy  # type: ignore
import pytest
from conftest import Cluster, Instance
from time import sleep


@funcy.retry(tries=20, timeout=0.1)
def retry_call(call, *args, **kwargs):
    return call(*args, **kwargs)


@pytest.fixture
def cluster2(cluster: Cluster):
    cluster.deploy(instance_count=2)
    return cluster


def test_follower_proposal(cluster2: Cluster):
    i1, i2 = cluster2.instances
    i1.promote_or_fail()

    i2.assert_raft_status("Follower", leader_id=i1.raft_id)
    i2.raft_propose_eval("rawset(_G, 'check', box.info.listen)")

    assert i1.eval("return check") == i1.listen
    assert i2.eval("return check") == i2.listen


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

    retry_call(i2.assert_raft_status, "Follower", leader_id=i1.raft_id)

    def do_test():
        i2.eval("picolib.raft_tick(20)")
        i1.assert_raft_status("Follower", leader_id=i2.raft_id)
        i2.assert_raft_status("Leader")

    retry_call(do_test)


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
    i1.raft_propose_eval("return")

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

    @funcy.retry(tries=20, timeout=0.1)
    def wait_alive(instance: Instance):
        assert instance._raft_status().leader_id is None

    i1.start()
    # This synchronization is necessary for proper test case reproducing.
    # i1 has already initialized raft node but can't win election yet
    # i2 starts discovery and should be able to advance further
    wait_alive(i1)
    i2.start()

    i1.wait_online()
    assert i1.current_grade() == dict(variant="Online", incarnation=2)
    i2.wait_online()
    assert i1.current_grade() == dict(variant="Online", incarnation=2)

    i1.raft_propose_eval("rawset(_G, 'check', true)")
    assert i1.eval("return check") is True
    assert i2.eval("return check") is True


def test_gl119_panic_in_on_shutdown(cluster2: Cluster):
    i1, i2 = cluster2.instances

    i2.call("picolib.raft_timeout_now", timeout=0.01)
    assert i2.terminate() == 0

    # second instance terminates first, so it becomes a follower
    i2.terminate()
    # terminate the leader, so the follower can't acquire the read barrier
    i1.terminate()

    i2.start()
    # wait for the follower to start acquiring the read barrier
    sleep(1)
    assert i2.terminate() == 0


# it's 2022 and i have to work around a mypy bug reported in 2018
on_shutdown_timed_out: bool


def test_gl127_graceul_shutdown(cluster2: Cluster):
    i1, i2 = cluster2.instances

    # make sure i1 is leader
    i1.promote_or_fail()
    i2.wait_online()

    global on_shutdown_timed_out
    on_shutdown_timed_out = False

    def check_log_line(log):
        if "on_shutdown triggers are timed out" in log:
            global on_shutdown_timed_out
            on_shutdown_timed_out = True

    i1.on_output_line(check_log_line)
    # on_shutdown triggers will timeout after 3sec
    # so we must wait longer than # that
    i1.terminate(kill_after_seconds=10)

    assert not on_shutdown_timed_out
