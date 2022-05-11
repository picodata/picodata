import funcy  # type: ignore
import pytest
from conftest import Cluster


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


def test_failover(cluster2: Cluster):
    i1, i2 = cluster2.instances
    i1.promote_or_fail()

    retry_call(i2.assert_raft_status, "Follower", leader_id=i1.raft_id)

    def do_test():
        i2.eval("picolib.raft_tick(20)")
        i1.assert_raft_status("Follower", leader_id=i2.raft_id)
        i2.assert_raft_status("Leader")

    retry_call(do_test)
