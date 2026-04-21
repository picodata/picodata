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
    picodata_expel,
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

    cluster.set_service_password("secret")
    i1, i2, i3 = cluster.deploy(instance_count=3, init_replication_factor=3)
    cluster.wait_leader_elected()

    i3.assert_raft_status("Follower", leader_id=i1.raft_id)

    picodata_expel(peer=i1, target=i3, force=True, password_file=cluster.service_password_file)

    cluster.wait_has_states(i3, "Expelled", "Expelled")
    Retriable().call(lambda: assert_voters([i1, i2], i1))

    # assert i3.process
    Retriable().call(i3.assert_process_dead)

    lc = log_crawler(i3, "current instance is expelled from the cluster")
    i3.fail_to_start()
    assert lc.matched


def test_expel_leader(cluster: Cluster):
    # Scenario: expel a Leader instance by command to itself
    #   Given a cluster
    #   When a Leader instance expelled from the cluster
    #   Then the instance marked as expelled in the instances table
    #   And excluded from the voters list

    cluster.set_service_password("secret")
    i1, i2, i3 = cluster.deploy(instance_count=3, init_replication_factor=3)
    cluster.wait_leader_elected()

    i1.assert_raft_status("Leader")

    picodata_expel(peer=i1, target=i1, force=True, timeout=5, password_file=cluster.service_password_file)

    cluster.wait_has_states(i1, "Expelled", "Expelled")
    Retriable().call(lambda: assert_voters([i2, i3], i2))

    # assert i1.process
    Retriable().call(i1.assert_process_dead)

    lc = log_crawler(i1, "current instance is expelled from the cluster")
    i1.fail_to_start()
    assert lc.matched


def test_expel_by_follower(cluster: Cluster):
    # Scenario: expel an instance by command to a Follower
    #   Given a cluster
    #   When instance which is not a Leader receives expel CLI command
    #   Then expelling instance is expelled

    cluster.set_service_password("secret")
    i1, i2, i3 = cluster.deploy(instance_count=3, init_replication_factor=3)
    cluster.wait_leader_elected()

    i2.assert_raft_status("Follower", leader_id=i1.raft_id)
    i3.assert_raft_status("Follower", leader_id=i1.raft_id)

    picodata_expel(peer=i2, target=i3, force=True, password_file=cluster.service_password_file)

    cluster.wait_has_states(i3, "Expelled", "Expelled")
    Retriable().call(lambda: assert_voters([i1, i2], i1))

    # assert i3.process
    Retriable().call(i3.assert_process_dead)


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
    cluster.wait_online()

    # If the peer is not resolving, by default we hang on
    # for 5 seconds. We can change it by specifying `timeout`.
    cli = pexpect.spawn(
        cwd=i1.instance_dir,
        command=i1.executable.command,
        args=[
            "expel",
            "random_instance_name",
            f"--timeout={CLI_TIMEOUT}",
            "--peer=10001:3301",
        ],
        encoding="utf-8",
        timeout=CLI_TIMEOUT,
    )
    cli.logfile = sys.stdout

    cli.expect_exact("Enter password for pico_service:")
    cli.sendline("wrong_password")

    common_error_part = "ERROR: connection failure for address '10001:3301'"
    if sys.platform == "darwin":
        cli.expect_exact(f"{common_error_part}: No route to host (os error 65)")
    else:
        cli.expect_exact(f"{common_error_part}: connect timeout")

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

    # Instance's target state changed to Expelled, but it is still offline
    cluster.wait_has_states(storage_2_1, "Offline", "Expelled")

    # And the replicaset is in progress of being expelled
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

    # Block bucket rebalancing, so that we can check what happens when master of
    # replicaset being expelled has it's state changed from Expelled to something else
    storage_2_1.env["PICODATA_ERROR_INJECTION_TIMEOUT_IN_PROC_WAIT_BUCKET_COUNT"] = "1"
    # Wake up the expelled instance so that replicaset is finally expelled
    storage_2_1.start()

    # Now the instance's target state changed from Expelled to Online.
    # NOTE: that the instance is still getting expelled, but there's no
    # information about it in it's state. Instead this information is save in
    # _pico_replicaset.state
    cluster.wait_has_states(storage_2_1, "Offline", "Online")
    # Replicaset state didn't change yet
    [[storage_2_state]] = leader.sql("SELECT state FROM _pico_replicaset WHERE name = 'storage_2'")
    assert storage_2_state == "to-be-expelled"

    # Unblock bucket rebalancing
    storage_2_1.call("pico._inject_error", "TIMEOUT_IN_PROC_WAIT_BUCKET_COUNT", False)

    # The buckets are finally able to be rebalanced
    leader.wait_governor_status("idle")

    # The instance's state (both current and target) automatically changes to Expelled
    cluster.wait_has_states(storage_2_1, "Expelled", "Expelled")

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


def test_join_replicaset_after_two_expel(cluster: Cluster):
    # test specific scenario with two expels and one join after that
    # test for problem described in https://git.picodata.io/core/picodata/-/issues/1455

    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        raft:
            replication_factor: 1
        storage:
            replication_factor: 3
"""
    )
    cluster.set_service_password("secret")

    [leader] = cluster.deploy(instance_count=1, tier="raft")

    # Deploy a cluster with at least one full replicaset
    storage_1_1 = cluster.add_instance(name="storage_1_1", wait_online=False, tier="storage")
    storage_1_2 = cluster.add_instance(name="storage_1_2", wait_online=False, tier="storage")
    storage_1_3 = cluster.add_instance(name="storage_1_3", wait_online=False, tier="storage")
    cluster.wait_online()
    assert storage_1_1.replicaset_name == "storage_1"
    assert storage_1_2.replicaset_name == "storage_1"
    assert storage_1_3.replicaset_name == "storage_1"

    response = storage_1_3.call("box.execute", 'SELECT "uuid" FROM "_cluster"')
    replicaset_uuids = set(uuid for [uuid] in response["rows"])
    assert replicaset_uuids == set((storage_1_1.uuid(), storage_1_2.uuid(), storage_1_3.uuid()))

    # Expel two of the replicas in the full replicaset, wait until the change is finalized
    counter = leader.governor_step_counter()
    cluster.expel(storage_1_2, force=True)
    cluster.expel(storage_1_1, force=True)
    leader.wait_governor_status("idle", old_step_counter=counter)

    response = storage_1_3.call("box.execute", 'SELECT "uuid" FROM "_cluster"')
    replicaset_uuids = set(uuid for [uuid] in response["rows"])
    assert replicaset_uuids == set([storage_1_3.uuid()])

    # Add another instance, it should be assigned to the no longer filled replicaset
    storage_4 = cluster.add_instance(name="storage_4", wait_online=True, tier="storage")
    assert storage_4.replicaset_name == "storage_1"

    response = storage_1_3.call("box.execute", 'SELECT "uuid" FROM "_cluster"')
    replicaset_uuids = set(uuid for [uuid] in response["rows"])
    assert replicaset_uuids == set((storage_1_3.uuid(), storage_4.uuid()))


def test_expel_zero_bucket_tier(cluster: Cluster):
    """
    Test that instances in a tier with bucket_count=0 (arbiter tier) can be
    freely expelled without hanging on bucket transfer. Such tiers have no
    sharded data and are used only for Raft consensus.
    """
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        arbiter:
            replication_factor: 1
            can_vote: true
            bucket_count: 0
        storage:
            replication_factor: 1
            can_vote: false
"""
    )

    arbiter_1 = cluster.add_instance(name="arbiter_1", tier="arbiter", wait_online=False)
    arbiter_2 = cluster.add_instance(name="arbiter_2", tier="arbiter", wait_online=False)
    cluster.add_instance(name="arbiter_3", tier="arbiter", wait_online=False)
    cluster.wait_online()
    cluster.wait_leader_elected()
    cluster.add_instance(name="storage_1", tier="storage")

    counter = arbiter_1.wait_governor_status("idle")

    # Verify arbiter tier has bucket_count=0
    [[bucket_count]] = arbiter_1.sql("SELECT bucket_count FROM _pico_tier WHERE name = 'arbiter'")
    assert bucket_count == 0

    # Verify vshard is not bootstrapped for the arbiter tier
    [[vshard_bootstrapped]] = arbiter_1.sql("SELECT vshard_bootstrapped FROM _pico_tier WHERE name = 'arbiter'")
    assert vshard_bootstrapped is False

    # Verify vshard is bootstrapped for the storage tier
    [[vshard_bootstrapped]] = arbiter_1.sql("SELECT vshard_bootstrapped FROM _pico_tier WHERE name = 'storage'")
    assert vshard_bootstrapped is True

    arbiter_2.terminate()
    cluster.wait_has_states(arbiter_2, "Offline", "Offline")
    Retriable().call(arbiter_2.assert_process_dead)

    # Expel offline arbiter_2 — since replication_factor=1, it is the sole instance
    # in its replicaset, so this triggers the full replicaset expel flow.
    # Without bucket_count=0 support this would hang waiting for bucket transfer.
    cluster.expel(arbiter_2, force=True)
    cluster.wait_has_states(arbiter_2, "Expelled", "Expelled")

    arbiter_1.wait_governor_status("idle", old_step_counter=counter)

    # Verify the replicaset is expelled
    [[state]] = arbiter_1.sql(
        "SELECT state FROM _pico_replicaset WHERE name = ?",
        arbiter_2.replicaset_name,
    )
    assert state == "expelled"

    # Verify the instance is expelled
    rows = arbiter_1.sql(
        "SELECT current_state FROM _pico_instance WHERE name = ?",
        arbiter_2.name,
    )
    [[state, _incarnation]] = rows[0]
    assert state == "Expelled"


def test_forced_expel_without_substitution(cluster: Cluster):
    cluster.set_service_password("T0psecret")
    password = cluster.service_password_file

    i1, i2, i3 = cluster.deploy(instance_count=3)

    picodata_expel(peer=i1, target=i2, password_file=password, force=True)
    picodata_expel(peer=i1, target=i3, password_file=password, force=True)
    try:
        picodata_expel(peer=i1, target=i1, password_file=password, force=True)
    except CommandFailed as error:
        assert f"attempt to expel instance '{i1.name}' which is the only member in the cluster" in error.stderr
