import os
import pytest
import signal
from conftest import Cluster, Instance


@pytest.fixture
def cluster2(cluster: Cluster):
    cluster.deploy(instance_count=2)
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
    i2.call("picolib.raft_timeout_now")
    # it can't win the election because there is no quorum
    i2.assert_raft_status("Candidate")

    crawler = log_crawler(i2, "on_shutdown triggers failed")

    # stopping i2 in that state still shouldn't be a problem
    assert i2.terminate() == 0

    # though on_shutdown trigger fails
    assert crawler.matched


@pytest.mark.xfail
def test_gl127_graceul_shutdown(cluster2: Cluster):
    i1, i2 = cluster2.instances

    # make sure i1 is leader
    i1.promote_or_fail()
    i2.wait_online()

    crawler = log_crawler(i1, "on_shutdown triggers failed")

    # on_shutdown triggers will timeout after 3sec
    # so we must wait longer than that
    i1.terminate(kill_after_seconds=10)

    assert not crawler.matched
