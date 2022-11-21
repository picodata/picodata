import os
import pytest
import signal
from conftest import Cluster


@pytest.fixture
def cluster2(cluster: Cluster):
    cluster.deploy(instance_count=2)
    return cluster


def test_gl119_panic_on_shutdown(cluster2: Cluster):
    i1, i2 = cluster2.instances
    i2.assert_raft_status("Follower", leader_id=i1.raft_id)

    # suspend i1 (leader) and force i2 to start a new term
    assert i1.process is not None
    os.killpg(i1.process.pid, signal.SIGSTOP)
    i2.call("picolib.raft_timeout_now")
    # it can't win the election because there is no quorum
    i2.assert_raft_status("Candidate")

    # stopping i2 in that state still shouldn't be a problem
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
