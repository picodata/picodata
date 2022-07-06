import funcy  # type: ignore
import pytest
from conftest import Cluster, Instance


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
    i2.restart()
    i2.wait_ready()
    i1.assert_raft_status("Leader")
    i2.assert_raft_status("Follower")


def test_restart_leader(cluster2: Cluster):
    # Given a cluster of two instances - i1 (leader) and i2 (follower)
    # When i1 restarts
    # Then it's able to start and make a proposal.
    # No assuptions about leadership are made though.

    i1, _ = cluster2.instances
    i1.restart()
    i1.wait_ready()
    i1.raft_propose_eval("return")


def test_restart_both(cluster2: Cluster):
    # Given a cluster of 2 instances - i1, i2
    # When both instances are stopped and then started again
    # Then both can become ready and handle proposals.

    i1, i2 = cluster2.instances
    i1.terminate()
    i2.terminate()

    @funcy.retry(tries=20, timeout=0.1)
    def wait_alive(instance):
        assert instance._raft_status().is_ready is False

    i1.start()
    # This synchronization is necessary for proper test case reproducing.
    # i1 has already initialized raft node but can't win election yet
    # i2 starts discovery and should be able to advance further
    wait_alive(i1)
    i2.start()

    i1.wait_ready()
    i2.wait_ready()

    i1.raft_propose_eval("rawset(_G, 'check', true)")
    assert i1.eval("return check") is True
    assert i2.eval("return check") is True


def test_deactivation(cluster2: Cluster):
    i1, i2 = cluster2.instances

    def is_voter_is_active(instance: Instance, raft_id):
        return tuple(
            instance.eval(
                """
                    raft_id = ...
                    health = box.space.raft_group.index.raft_id:get(raft_id).health
                    is_active = health == 'Online'
                    voters = box.space.raft_state:get('voters').value
                    for _, voter in pairs(voters) do
                        if voter == raft_id then
                            return { true, is_active }
                        end
                    end
                    return { false, is_active }
                """,
                raft_id,
            )
        )

    assert is_voter_is_active(i1, i1.raft_id) == (True, True)
    assert is_voter_is_active(i2, i2.raft_id) == (True, True)

    i2.terminate()

    assert is_voter_is_active(i1, i1.raft_id) == (True, True)
    assert is_voter_is_active(i1, i2.raft_id) == (False, False)

    i2.start()
    i2.wait_ready()

    assert is_voter_is_active(i1, i1.raft_id) == (True, True)
    assert is_voter_is_active(i2, i2.raft_id) == (True, True)

    i1.terminate()

    assert is_voter_is_active(i2, i1.raft_id) == (False, False)
    assert is_voter_is_active(i2, i2.raft_id) == (True, True)

    # wait until i2 is leader, so it has someone to send the deactivation
    # request to
    i2.promote_or_fail()

    i2.terminate()

    i1.start()
    i2.start()

    i1.wait_ready()
    i2.wait_ready()

    assert is_voter_is_active(i1, i1.raft_id) == (True, True)
    assert is_voter_is_active(i2, i2.raft_id) == (True, True)

    i1.terminate()

    assert is_voter_is_active(i2, i1.raft_id) == (False, False)
    assert is_voter_is_active(i2, i2.raft_id) == (True, True)

    def raft_update_peer(
        host: Instance, target: Instance, is_active: bool
    ) -> list[bool]:
        kind = "Online" if is_active else "Offline"
        return host.call(
            ".raft_update_peer", kind, target.instance_id, target.cluster_id
        )

    # check idempotency
    assert raft_update_peer(i2, target=i1, is_active=False) == [{}]
    assert raft_update_peer(i2, target=i1, is_active=False) == [{}]

    assert raft_update_peer(i2, target=i2, is_active=True) == [{}]
    assert raft_update_peer(i2, target=i2, is_active=True) == [{}]
