import pytest
import sys
import pexpect  # type: ignore
from conftest import CLI_TIMEOUT, Cluster, Instance, Retriable, log_crawler


@pytest.fixture
def cluster3(cluster: Cluster):
    cluster.deploy(instance_count=3)
    return cluster


def assert_instance_expelled(expelled_instance: Instance, instance: Instance):
    info = instance.call(".proc_instance_info", expelled_instance.instance_id)
    states = (info["current_state"]["variant"], info["target_state"]["variant"])
    assert states == ("Expelled", "Expelled")


def assert_voters(voters: list[Instance], instance: Instance):
    expected_voters = list(map(lambda i: i.raft_id, voters))
    actual_voters = instance.eval("return box.space._raft_state:get('voters').value")
    assert sorted(actual_voters) == sorted(expected_voters)


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

    Retriable(timeout=10).call(lambda: assert_instance_expelled(i3, i1))
    Retriable(timeout=10).call(lambda: assert_voters([i1, i2], i1))

    # assert i3.process
    Retriable(timeout=10).call(i3.assert_process_dead)

    lc = log_crawler(i3, "current instance is expelled from the cluster")
    i3.fail_to_start()
    assert lc.matched


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

    Retriable(timeout=10).call(lambda: assert_instance_expelled(i1, i2))
    Retriable(timeout=10).call(lambda: assert_voters([i2, i3], i2))

    # assert i1.process
    Retriable(timeout=10).call(i1.assert_process_dead)

    lc = log_crawler(i1, "current instance is expelled from the cluster")
    i1.fail_to_start()
    assert lc.matched


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

    Retriable(timeout=10).call(lambda: assert_instance_expelled(i3, i1))
    Retriable(timeout=10).call(lambda: assert_voters([i1, i2], i1))

    # assert i3.process
    Retriable(timeout=10).call(i3.assert_process_dead)


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

    cluster.expel(i3)
    Retriable(timeout=10).call(lambda: assert_instance_expelled(i3, i1))

    i4 = cluster.add_instance()
    assert i4.raft_id == 4


def test_expel_timeout(cluster: Cluster):
    cluster.deploy(instance_count=1)
    [i1] = cluster.instances

    # If the peer is not resolving, by default we hang on
    # for 5 seconds. We can change it by specifying `timeout`.
    cli = pexpect.spawn(
        cwd=i1.data_dir,
        command=i1.binary_path,
        args=[
            "expel",
            "random_instance_id",
            f"--timeout={CLI_TIMEOUT}",
            "--peer=10001",
        ],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for admin:")
    cli.sendline("wrong_password")

    cli.expect_exact("CRITICAL: connect timeout")
    cli.expect_exact(pexpect.EOF)
