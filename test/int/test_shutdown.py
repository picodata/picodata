import os
import pytest
import signal
from conftest import Cluster, Instance

ON_SHUTDOWN_TIMEOUT = "on_shutdown triggers failed"


@pytest.fixture
def cluster2(cluster: Cluster):
    cluster.deploy(instance_count=2)
    return cluster


@pytest.fixture
def cluster3(cluster: Cluster):
    cluster.deploy(instance_count=3)
    return cluster


class log_crawler:
    def __init__(self, instance: Instance, search_str: str) -> None:
        self.matched = False
        self.search_str = search_str
        instance.on_output_line(self._cb)

    def _cb(self, line):
        if self.search_str in line:
            self.matched = True


def test_gl119_panic_on_shutdown(cluster2: Cluster):
    i1, i2 = cluster2.instances
    i2.assert_raft_status("Follower", leader_id=i1.raft_id)

    # suspend i1 (leader) and force i2 to start a new term
    assert i1.process is not None
    os.killpg(i1.process.pid, signal.SIGSTOP)
    i2.call("pico.raft_timeout_now")
    # it can't win the election because there is no quorum
    i2.assert_raft_status("Candidate")

    crawler = log_crawler(i2, ON_SHUTDOWN_TIMEOUT)

    # stopping i2 in that state still shouldn't be a problem
    assert i2.terminate() == 0

    # though on_shutdown trigger fails
    assert crawler.matched


def test_single(instance: Instance):
    crawler = log_crawler(instance, ON_SHUTDOWN_TIMEOUT)

    instance.terminate(kill_after_seconds=1)
    assert not crawler.matched


def test_couple_leader_first(cluster2: Cluster):
    i1, i2 = cluster2.instances

    # make sure i1 is leader
    i1.assert_raft_status("Leader")
    i2.assert_raft_status("Follower", leader_id=i1.raft_id)

    c1 = log_crawler(i1, ON_SHUTDOWN_TIMEOUT)
    i1.terminate(kill_after_seconds=1)
    assert not c1.matched

    i2.assert_raft_status("Leader")
    i1_info = i2.call("pico.instance_info", i1.instance_id)
    assert i1_info["target_grade"]["variant"] == "Offline"
    assert i1_info["current_grade"]["variant"] == "Offline"

    c2 = log_crawler(i2, ON_SHUTDOWN_TIMEOUT)
    i2.terminate(kill_after_seconds=1)
    assert not c2.matched


def test_couple_follower_first(cluster2: Cluster):
    i1, i2 = cluster2.instances

    # make sure i1 is leader
    i1.assert_raft_status("Leader")
    i2.assert_raft_status("Follower", leader_id=i1.raft_id)

    c2 = log_crawler(i2, ON_SHUTDOWN_TIMEOUT)
    i2.terminate(kill_after_seconds=1)
    assert not c2.matched

    i2_info = i1.call("pico.instance_info", i2.instance_id)
    assert i2_info["target_grade"]["variant"] == "Offline"
    assert i2_info["current_grade"]["variant"] == "Offline"

    c1 = log_crawler(i1, ON_SHUTDOWN_TIMEOUT)
    i1.terminate(kill_after_seconds=1)
    assert not c1.matched


def test_threesome(cluster3: Cluster):
    i1, i2, i3 = cluster3.instances

    # make sure i1 is leader
    i1.assert_raft_status("Leader")
    i2.assert_raft_status("Follower", leader_id=i1.raft_id)
    i3.assert_raft_status("Follower", leader_id=i1.raft_id)

    c1 = log_crawler(i1, ON_SHUTDOWN_TIMEOUT)
    i1.terminate(kill_after_seconds=1)
    assert not c1.matched

    c2 = log_crawler(i2, ON_SHUTDOWN_TIMEOUT)
    i2.terminate(kill_after_seconds=1)
    assert not c2.matched
