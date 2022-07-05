import pytest

from conftest import (
    Cluster,
    Instance,
    ReturnError,
    retrying,
)


@pytest.fixture
def cluster3(cluster: Cluster):
    cluster.deploy(instance_count=3)
    return cluster


def test_log_rollback(cluster3: Cluster):
    # Scanario: the Leader can't propose without Followers
    #   Given a cluster
    #   When all Followers killed without graceful shutdown
    #   And the Leader proposing changes
    #   Then the proposition failed

    i1, i2, i3 = cluster3.instances
    i1.assert_raft_status("Leader")
    i2.assert_raft_status("Follower")
    i3.assert_raft_status("Follower")

    def propose_state_change(srv: Instance, value):
        code = 'box.space.raft_state:put({"test-timeline", "%s"})' % value
        return srv.raft_propose_eval(code, 0.1)

    propose_state_change(i1, "i1 is a leader")

    # Simulate the network partitioning: i1 can't reach i2 and i3.
    i2.kill()
    i3.kill()

    # No operations can be committed, i1 is alone.
    with pytest.raises(ReturnError, match="timeout"):
        propose_state_change(i1, "i1 lost the quorum")

    # And now i2 + i3 can't reach i1.
    i1.terminate()
    i2.start(peers=[i3])
    i3.start(peers=[i2])
    i2.wait_ready()
    i3.wait_ready()

    # Help i2 to become a new leader
    i2.promote_or_fail()
    retrying(lambda: i3.assert_raft_status("Follower", i2.raft_id))

    propose_state_change(i2, "i2 takes the leadership")

    # Now i1 has an uncommitted, but persisted entry that should be rolled back.
    i1.start(peers=[i2, i3])
    i1.wait_ready()
    retrying(lambda: i1.assert_raft_status("Follower", i2.raft_id))

    propose_state_change(i1, "i1 is alive again")


def test_leader_disruption(cluster3: Cluster):
    # Scenario: Follower reconnection on disconnect from the cluster
    #   Given a cluster
    #   When any Follower lost network connection with all other cluster nodes
    #   And this Follower starts new election
    #   And the network connection was established again
    #   Then the Follower became Follower as it was before

    i1, i2, i3 = cluster3.instances
    i1.assert_raft_status("Leader")
    i2.assert_raft_status("Follower")
    i3.assert_raft_status("Follower")

    # Simulate asymmetric network failure.
    # Node i3 doesn't receive any messages,
    # including the heartbeat from the leader.
    # Then it starts a new election.
    i3.call("box.schema.func.drop", ".raft_interact")

    # Speed up election timeout
    i3.eval(
        """
        while picolib.raft_status().raft_state == 'Follower' do
            picolib.raft_tick(1)
        end
        """
    )
    i3.assert_raft_status("PreCandidate", None)

    # Advance the raft log. It makes i1 and i2 to reject the RequestPreVote.
    i1.raft_propose_eval("return", timeout_seconds=1)

    # Restore normal network operation
    i3.call(
        "box.schema.func.create",
        ".raft_interact",
        {"language": "C", "if_not_exists": True},
    )

    # i3 should become the follower again without disrupting i1
    retrying(lambda: i3.assert_raft_status("Follower", i1.raft_id))
