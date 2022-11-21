import pytest
from conftest import Cluster
from time import sleep


@pytest.fixture
def cluster2(cluster: Cluster):
    cluster.deploy(instance_count=2)
    return cluster


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
