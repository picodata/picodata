import pytest
from conftest import Cluster
from util import promote_or_fail, raft_propose_eval, assert_raft_status, retry


@pytest.mark.skip
def test_follower_proposal(run_cluster):
    cluster: Cluster = run_cluster(instance_count=2)

    i1, i2 = cluster.instances

    promote_or_fail(i1)  # Speed up node election

    raft_propose_eval(i1, "rawset(_G, 'check', box.info.listen)")
    assert i1.eval("return check") == i1.listen
    assert i2.eval("return check") == i2.listen


def test_failover(run_cluster):
    cluster: Cluster = run_cluster(instance_count=2)
    i1, i2 = cluster.instances

    promote_or_fail(i1)

    retry(assert_raft_status)(i2, raft_state="Follower", leader_id=1)

    @retry
    def do_test():
        i2.eval("picolib.raft_tick(20)")
        assert_raft_status(i2, raft_state="Leader")
        assert_raft_status(i1, raft_state="Follower", leader_id=2)

    do_test()
