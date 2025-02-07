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

    cluster.expel(i3, i1, force=True)

    cluster.wait_has_states(i3, "Expelled", "Expelled")
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

    cluster.expel(i1, peer=i3, force=True)

    cluster.wait_has_states(i1, "Expelled", "Expelled")
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

    cluster.expel(i3, force=True)

    cluster.wait_has_states(i3, "Expelled", "Expelled")
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

    cluster.expel(i3, force=True)
    cluster.wait_has_states(i3, "Expelled", "Expelled")

    i4 = cluster.add_instance()
    assert i4.raft_id == 4


def test_expel_offline_ro_replica(cluster: Cluster):
    cluster.set_service_password("secret")
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        raft:
            can_vote: true
        storage:
            can_vote: false
            replication_factor: 5
"""
    )
    leader = cluster.add_instance(tier="raft", wait_online=True)
    storage_1 = cluster.add_instance(tier="storage", wait_online=True)
    storage_2 = cluster.add_instance(tier="storage", wait_online=False)
    storage_3 = cluster.add_instance(tier="storage", wait_online=False)
    cluster.wait_online()

    counter = leader.wait_governor_status("idle")

    [[current_master, target_master]] = leader.sql(
        "SELECT current_master_name, target_master_name FROM _pico_replicaset WHERE name = ?",
        storage_1.replicaset_name,
    )
    assert current_master == target_master == storage_1.name

    # Expelling an Online instance doesn't work without --force flag
    with pytest.raises(CommandFailed) as e:
        cluster.expel(storage_2, force=False)
    assert (
        f"""attempt to expel instance '{storage_2.name}' which is not Offline, but Online
rerun with --force if you still want to expel the instance"""
        in e.value.stderr
    )

    storage_2.terminate()
    storage_3.terminate()

    # Synchronization: make sure governor does all it wanted
    leader.wait_governor_status("idle", old_step_counter=counter)

    # Check expelling offline replicas, this should be ok because no data loss
    cluster.expel(storage_2, force=False)
    cluster.wait_has_states(storage_2, "Expelled", "Expelled")

    cluster.expel(storage_3, force=False)
    cluster.wait_has_states(storage_3, "Expelled", "Expelled")

    # Terminate the last replica (master)
    storage_1.terminate()

    # Expelling a replicaset master doesn't work without --force flag
    with pytest.raises(CommandFailed) as e:
        cluster.expel(storage_1, force=False)
    assert (
        f"""attempt to expel replicaset master '{storage_1.name}'
rerun with --force if you still want to expel the instance"""
        in e.value.stderr
    )

    # Adding --force makes it so the target_state changes to Expel, but the
    # expel still doesn't work because it is blocked by bucket rebalancing,
    # which will never happen, because instance is Offline.
    with pytest.raises(CommandFailed) as e:
        cluster.expel(storage_1, force=True, timeout=3)
    assert "Timeout: expel confirmation didn't arrive in time" in e.value.stderr

    cluster.wait_has_states(storage_1, "Offline", "Expelled")


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
        cli.expect_exact("CRITICAL: failed to connect to address '10001:3301': No route to host (os error 65)")
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
    storage_1_1 = cluster.add_instance(name="storage_1_1", wait_online=True, tier="storage")
    assert storage_1_1.replicaset_name == "storage_1"
    storage_1_2 = cluster.add_instance(name="storage_1_2", wait_online=True, tier="storage")
    assert storage_1_2.replicaset_name == "storage_1"
    storage_2_1 = cluster.add_instance(name="storage_2_1", wait_online=True, tier="storage")
    assert storage_2_1.replicaset_name == "storage_2"

    # Expel one of the replicas in the full replicaset, wait until the change is finalized
    counter = leader.governor_step_counter()
    cluster.expel(storage_1_2, force=True)
    leader.wait_governor_status("idle", old_step_counter=counter)

    # Check `picodata expel` idempotency
    cluster.expel(storage_1_2, timeout=1)

    # Add another instance, it should be assigned to the no longer filled replicaset
    storage4 = cluster.add_instance(name="storage4", wait_online=True, tier="storage")
    assert storage4.replicaset_name == "storage_1"

    # Attempt to expel an offline replicaset
    storage_2_1.terminate()
    with pytest.raises(CommandFailed) as e:
        cluster.expel(storage_2_1, timeout=1, force=False)
    assert (
        f"""attempt to expel replicaset master '{storage_2_1.name}'
rerun with --force if you still want to expel the instance"""
        in e.value.stderr
    )

    # Only works with `--force` flag
    with pytest.raises(CommandFailed) as e:
        cluster.expel(storage_2_1, timeout=1, force=True)
    assert "Timeout: expel confirmation didn't arrive in time" in e.value.stderr

    # Check `picodata expel` idempotency
    with pytest.raises(CommandFailed) as e:
        cluster.expel(storage_2_1, timeout=1)
    assert "Timeout: expel confirmation didn't arrive in time" in e.value.stderr

    # Offline replicasets aren't allowed to be expelled,
    # so the cluster is blocked attempting to rebalance
    counter = leader.wait_governor_status("transfer buckets from replicaset")

    # The replicaset is in progress of being expelled
    [[storage_2_state, storage_2_old_uuid]] = leader.sql(
        """ SELECT state, "uuid" FROM _pico_replicaset WHERE name = 'storage_2' """
    )
    assert storage_2_state == "to-be-expelled"

    # Add another instance
    storage5 = cluster.add_instance(name="storage5", tier="storage", wait_online=False)
    storage5.start()

    # NOTE: wait_online doesn't work because bucket rebalancing has higher priortiy
    leader.wait_governor_status("transfer buckets from replicaset", old_step_counter=counter)

    # Update the fields on the object
    Retriable().call(storage5.instance_info)
    # Instance is added to a new replicaset because 'storage_2' is not yet avaliable
    assert storage5.replicaset_name == "storage_3"

    # Try adding an instance to 'storage_2' directly, which is not allowed
    storage6 = cluster.add_instance(name="storage6", replicaset_name="storage_2", tier="storage", wait_online=False)
    lc = log_crawler(storage6, "cannot join replicaset which is being expelled")
    storage6.fail_to_start()
    lc.wait_matched()

    # Wake up the instance expelled instance so that replicaset is finally expelled
    storage_2_1.start()

    # The buckets are finally able to be rebalanced
    leader.wait_governor_status("idle")

    # The replicaset is finally expelled
    [[storage_2_state]] = leader.sql(""" SELECT state FROM _pico_replicaset WHERE name = 'storage_2' """)
    assert storage_2_state == "expelled"

    # Now it's ok to reuse the 'r3' replicaset name, but it will be a different replicaset
    cluster.add_instance(replicaset_name="storage_2", tier="storage", wait_online=True)

    # The new replicaset is created
    [[storage_2_state, storage_2_new_uuid]] = leader.sql(
        """ SELECT state, "uuid" FROM _pico_replicaset WHERE name = 'storage_2' """
    )
    assert storage_2_state != "expelled"
    assert storage_2_old_uuid != storage_2_new_uuid
