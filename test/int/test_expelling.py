import pytest
import sys
import pexpect  # type: ignore
from conftest import (
    CLI_TIMEOUT,
    Cluster,
    Instance,
    Retriable,
    log_crawler,
    CommandFailed,
)


def assert_instance_expelled(expelled_instance: Instance, instance: Instance):
    info = instance.call(".proc_instance_info", expelled_instance.name)
    states = (info["current_state"]["variant"], info["target_state"]["variant"])
    assert states == ("Expelled", "Expelled")


def assert_voters(voters: list[Instance], instance: Instance):
    expected_voters = list(map(lambda i: i.raft_id, voters))
    actual_voters = instance.eval("return box.space._raft_state:get('voters').value")
    assert sorted(actual_voters) == sorted(expected_voters)


def test_expel_follower(cluster: Cluster):
    # Scenario: expel a Follower instance by command to Leader
    #   Given a cluster
    #   When a Follower instance expelled from the cluster
    #   Then the instance marked as expelled in the instances table
    #   And excluded from the voters list

    i1, i2, i3 = cluster.deploy(instance_count=3, init_replication_factor=3)
    i1.promote_or_fail()

    i3.assert_raft_status("Follower", leader_id=i1.raft_id)

    cluster.expel(i3, i1)

    Retriable(timeout=30).call(lambda: assert_instance_expelled(i3, i1))
    Retriable(timeout=10).call(lambda: assert_voters([i1, i2], i1))

    # assert i3.process
    Retriable(timeout=10).call(i3.assert_process_dead)

    lc = log_crawler(i3, "current instance is expelled from the cluster")
    i3.fail_to_start()
    assert lc.matched


def test_expel_leader(cluster: Cluster):
    # Scenario: expel a Leader instance by command to itself
    #   Given a cluster
    #   When a Leader instance expelled from the cluster
    #   Then the instance marked as expelled in the instances table
    #   And excluded from the voters list

    i1, i2, i3 = cluster.deploy(instance_count=3, init_replication_factor=3)
    i1.promote_or_fail()

    i1.assert_raft_status("Leader")

    cluster.expel(i1)

    Retriable(timeout=30).call(lambda: assert_instance_expelled(i1, i2))
    Retriable(timeout=10).call(lambda: assert_voters([i2, i3], i2))

    # assert i1.process
    Retriable(timeout=10).call(i1.assert_process_dead)

    lc = log_crawler(i1, "current instance is expelled from the cluster")
    i1.fail_to_start()
    assert lc.matched


def test_expel_by_follower(cluster: Cluster):
    # Scenario: expel an instance by command to a Follower
    #   Given a cluster
    #   When instance which is not a Leader receives expel CLI command
    #   Then expelling instance is expelled

    i1, i2, i3 = cluster.deploy(instance_count=3, init_replication_factor=3)
    i1.promote_or_fail()

    i2.assert_raft_status("Follower", leader_id=i1.raft_id)
    i3.assert_raft_status("Follower", leader_id=i1.raft_id)

    cluster.expel(i3, i2)

    Retriable(timeout=30).call(lambda: assert_instance_expelled(i3, i1))
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
    Retriable(timeout=30).call(lambda: assert_instance_expelled(i3, i1))

    i4 = cluster.add_instance()
    assert i4.raft_id == 4


def test_expel_offline_ro_replica(cluster: Cluster):
    cluster.set_service_password("secret")
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        storage:
            replication_factor: 5
"""
    )
    i1 = cluster.add_instance(tier="storage", wait_online=False)
    cluster.add_instance(tier="storage", wait_online=False)
    cluster.add_instance(tier="storage", wait_online=False)
    i4 = cluster.add_instance(tier="storage", wait_online=False)
    i5 = cluster.add_instance(tier="storage", wait_online=False)
    cluster.wait_online()

    counter = i1.wait_governor_status("idle")

    i4.terminate()
    i5.terminate()

    # Synchronization: make sure governor does all it wanted
    i1.wait_governor_status("idle", old_step_counter=counter)

    # Check expelling offline replicas, this should be ok because no data loss
    cluster.expel(i4, peer=i1)
    [i4_state] = i1.sql(
        "SELECT current_state, target_state FROM _pico_instance WHERE name = ?",
        i4.name,
    )
    # ignore incarnation
    i4_state = [variant for [variant, incarnation] in i4_state]
    assert i4_state == ["Expelled", "Expelled"]

    cluster.expel(i5, peer=i1)
    [i5_state] = i1.sql(
        "SELECT current_state, target_state FROM _pico_instance WHERE name = ?",
        i5.name,
    )
    # ignore incarnation
    i5_state = [variant for [variant, incarnation] in i5_state]
    assert i5_state == ["Expelled", "Expelled"]


def test_expel_timeout(cluster: Cluster):
    cluster.deploy(instance_count=1)
    [i1] = cluster.instances

    # If the peer is not resolving, by default we hang on
    # for 5 seconds. We can change it by specifying `timeout`.
    cli = pexpect.spawn(
        cwd=i1.instance_dir,
        command=i1.binary_path,
        args=[
            "expel",
            "random_instance_name",
            f"--timeout={CLI_TIMEOUT}",
            "--peer=10001",
        ],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for admin:")
    cli.sendline("wrong_password")

    if sys.platform == "darwin":
        cli.expect_exact(
            "CRITICAL: failed to connect to address '10001:3301': "
            "No route to host (os error 65)"
        )
    else:
        cli.expect_exact("CRITICAL: connect timeout")

    cli.expect_exact(pexpect.EOF)


def test_join_replicaset_after_expel(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        raft:
            replication_factor: 1
        storage:
            replication_factor: 2
"""
    )
    cluster.set_service_password("secret")

    [leader] = cluster.deploy(instance_count=1, tier="raft")

    # Deploy a cluster with at least one full replicaset
    storage1 = cluster.add_instance(name="storage1", wait_online=True, tier="storage")
    assert storage1.replicaset_name == "r2"
    storage2 = cluster.add_instance(name="storage2", wait_online=True, tier="storage")
    assert storage2.replicaset_name == "r2"
    storage3 = cluster.add_instance(name="storage3", wait_online=True, tier="storage")
    assert storage3.replicaset_name == "r3"

    # Expel one of the replicas in the full replicaset, wait until the change is finalized
    counter = leader.governor_step_counter()
    cluster.expel(storage2, peer=leader)
    leader.wait_governor_status("idle", old_step_counter=counter)

    # Check `picodata expel` idempotency
    cluster.expel(storage2, peer=leader, timeout=1)

    # Add another instance, it should be assigned to the no longer filled replicaset
    storage4 = cluster.add_instance(name="storage4", wait_online=True, tier="storage")
    assert storage4.replicaset_name == "r2"

    # Attempt to expel an offline replicaset
    storage3.terminate()
    with pytest.raises(CommandFailed) as e:
        cluster.expel(storage3, peer=leader, timeout=1)
    assert "Timeout: expel confirmation didn't arrive in time" in e.value.stderr

    # Check `picodata expel` idempotency
    with pytest.raises(CommandFailed) as e:
        cluster.expel(storage3, peer=leader, timeout=1)
    assert "Timeout: expel confirmation didn't arrive in time" in e.value.stderr

    # Offline replicasets aren't allowed to be expelled,
    # so the cluster is blocked attempting to rebalance
    counter = leader.wait_governor_status("transfer buckets from replicaset")

    # The replicaset is in progress of being expelled
    [[r3_state, r3_old_uuid]] = leader.sql(
        """ SELECT state, "uuid" FROM _pico_replicaset WHERE name = 'r3' """
    )
    assert r3_state == "to-be-expelled"

    # Add another instance
    storage5 = cluster.add_instance(name="storage5", tier="storage", wait_online=False)
    storage5.start()

    # NOTE: wait_online doesn't work because bucket rebalancing has higher priortiy
    leader.wait_governor_status(
        "transfer buckets from replicaset", old_step_counter=counter
    )

    # Update the fields on the object
    Retriable().call(storage5.instance_info)
    # Instance is added to a new replicaset because 'r3' is not yet avaliable
    assert storage5.replicaset_name == "r4"

    # Try adding an instance to 'r3' directly, which is not allowed
    storage6 = cluster.add_instance(
        name="storage6", replicaset_name="r3", tier="storage", wait_online=False
    )
    lc = log_crawler(storage6, "cannot join replicaset which is being expelled")
    storage6.fail_to_start()
    lc.wait_matched()

    # Wake up the instance expelled instance so that replicaset is finally expelled
    storage3.start()

    # The buckets are finally able to be rebalanced
    leader.wait_governor_status("idle")

    # The replicaset is finally expelled
    [[r3_state]] = leader.sql(
        """ SELECT state FROM _pico_replicaset WHERE name = 'r3' """
    )
    assert r3_state == "expelled"

    # Now it's ok to reuse the 'r3' replicaset name, but it will be a different replicaset
    cluster.add_instance(replicaset_name="r3", tier="storage", wait_online=True)

    # The new replicaset is created
    [[r3_state, r3_new_uuid]] = leader.sql(
        """ SELECT state, "uuid" FROM _pico_replicaset WHERE name = 'r3' """
    )
    assert r3_state != "expelled"
    assert r3_old_uuid != r3_new_uuid
