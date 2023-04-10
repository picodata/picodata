import pytest
from conftest import Cluster, Instance, retrying, pid_alive


@pytest.fixture
def cluster3(cluster: Cluster):
    cluster.deploy(instance_count=3)
    return cluster


def assert_instance_expelled(expelled_instance: Instance, instance: Instance):
    info = instance.call("pico.instance_info", expelled_instance.instance_id)
    grades = (info["current_grade"]["variant"], info["target_grade"]["variant"])
    assert grades == ("Expelled", "Expelled")


def assert_voters(voters: list[Instance], instance: Instance):
    expected_voters = list(map(lambda i: i.raft_id, voters))
    actual_voters = instance.eval("return pico.space.raft_state:get('voters').value")
    assert actual_voters.sort() == expected_voters.sort()


def assert_pid_down(pid):
    assert not pid_alive(pid)


def test_expel_follower(cluster3: Cluster):
    # Scenario: expel a Follower instance by command to Leader
    #   Given a cluster
    #   When a Follower instance expelled from the cluster
    #   Then the instance marked as expelled in the instances table
    #   And excluded from the voters list

    i1, i2, i3 = cluster3.instances
    i1.promote_or_fail()

    i3.assert_raft_status("Follower", leader_id=i1.raft_id)

    cluster3.expel(i3, i1)

    retrying(lambda: assert_instance_expelled(i3, i1))
    retrying(lambda: assert_voters([i1, i2], i1))

    # assert i3.process
    # retrying(lambda: assert_pid_down(i3.process.pid))


def test_expel_leader(cluster3: Cluster):
    # Scenario: expel a Leader instance by command to itself
    #   Given a cluster
    #   When a Leader instance expelled from the cluster
    #   Then the instance marked as expelled in the instances table
    #   And excluded from the voters list

    i1, i2, i3 = cluster3.instances
    i1.promote_or_fail()

    i1.assert_raft_status("Leader")

    cluster3.expel(i1)

    retrying(lambda: assert_instance_expelled(i1, i2))
    retrying(lambda: assert_voters([i2, i3], i2))

    # assert i1.process
    # retrying(lambda: assert_pid_down(i1.process.pid))


def test_expel_by_follower(cluster3: Cluster):
    # Scenario: expel an instance by command to a Follower
    #   Given a cluster
    #   When instance which is not a Leader receives expel CLI command
    #   Then expelling instance is expelled

    i1, i2, i3 = cluster3.instances
    i1.promote_or_fail()

    i2.assert_raft_status("Follower", leader_id=i1.raft_id)
    i3.assert_raft_status("Follower", leader_id=i1.raft_id)

    cluster3.expel(i3, i2)

    retrying(lambda: assert_instance_expelled(i3, i1))
    retrying(lambda: assert_voters([i1, i2], i1))

    # assert i3.process
    # retrying(lambda: assert_pid_down(i3.process.pid))


def test_raft_id_after_expel(cluster: Cluster):
    # Scenario: join just right after expel should give completely new raft_id for the instance
    #   Given a cluster
    #   When instance with max raft_id expelled
    #   And a new instance joined
    #   Then raft_id of joined instance should be more than raft_id of the expelled instance

    cluster.deploy(instance_count=2)
    i1, _ = cluster.instances
    i3 = cluster.add_instance()
    assert i3.raft_id == 3

    cluster.expel(i3, i1)
    retrying(lambda: assert_instance_expelled(i3, i1))

    i4 = cluster.add_instance()
    assert i4.raft_id == 4
