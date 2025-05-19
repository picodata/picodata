import time
import pytest

from conftest import (
    Cluster,
    Instance,
    Retriable,
    CommandFailed,
    log_crawler,
)


def wait_vclock(i: Instance, vclock_expected: dict[int, int]):
    def check_vclock():
        vclock_actual = i.eval("return box.info.vclock")
        del vclock_actual[0]
        for k, v_exp in vclock_expected.items():
            assert (k, vclock_actual[k]) >= (k, v_exp)

    Retriable(timeout=30).call(check_vclock)


# fmt: off
def test_2_of_3_writable(cluster: Cluster):
    i1, i2, i3 = sorted(cluster.deploy(instance_count=3, init_replication_factor=3),
                        key=lambda i: i.name or "")

    master_name = i1.replicaset_master_name()
    assert i2.replicaset_master_name() == master_name
    assert i3.replicaset_master_name() == master_name

    master, i2, i3 = sorted(
        [i1, i2, i3],
        key=lambda i: master_name == i.name,
        reverse=True
    )

    rl_vclock = master.eval("return box.info.vclock")
    del rl_vclock[0]

    wait_vclock(i2, rl_vclock)  # sometimes fails with i2 missing one transaction
    wait_vclock(i3, rl_vclock)  # sometimes fails with i3 missing one transaction

    rl_vclock = master.eval("""
        box.schema.space.create('test_space')
            :create_index('pk')
        box.space.test_space:replace {1}
        return box.info.vclock
    """)
    del rl_vclock[0]

    wait_vclock(i2, rl_vclock)
    assert i2.eval("return box.space.test_space:select()") == [[1]]

    wait_vclock(i3, rl_vclock)
    assert i3.eval("return box.space.test_space:select()") == [[1]]

    master.terminate()

    old_master_name = master_name

    def check_master_changed():
        nonlocal master_name
        master_name = i2.replicaset_master_name()
        assert master_name != old_master_name

    Retriable(timeout=6, rps=4).call(check_master_changed)
    assert i3.replicaset_master_name() == master_name

    old_leader = master
    master, i3 = sorted(
        [i2, i3],
        key=lambda i: i.eval("return box.info.id") == master_name,
        reverse=True
    )

    rl_vclock = master.eval("return box.info.vclock")
    del rl_vclock[0]
    wait_vclock(i3, rl_vclock)

    rl_vclock = master.eval("""
        box.space.test_space:replace {2}
        return box.info.vclock
    """)
    del rl_vclock[0]

    wait_vclock(i3, rl_vclock)
    assert i3.eval("return box.space.test_space:select()") == [[1], [2]]

    print(i3.call("pico.raft_log", dict(return_string=True)))

    print(f"{old_leader=}")
    old_leader.start()
    old_leader.wait_online()
    assert old_leader.replicaset_master_name() == master_name
    wait_vclock(old_leader, rl_vclock)
    assert old_leader.eval("return box.space.test_space:select()") == [[1], [2]]
# fmt: on


def test_replication_works(cluster: Cluster):
    cluster.deploy(instance_count=1, init_replication_factor=2)
    i2 = cluster.add_instance(wait_online=False, replicaset_name="r2")
    i3 = cluster.add_instance(wait_online=False, replicaset_name="r2")
    i2.start()
    i3.start()
    i2.wait_online()
    i3.wait_online()

    def check_replicas_joined(i: Instance, n: int):
        assert len(i.call("box.info")["replication"]) == n

    Retriable().call(lambda: check_replicas_joined(i2, 2))
    Retriable().call(lambda: check_replicas_joined(i3, 2))


def test_master_auto_switchover(cluster: Cluster):
    # These guys are for quorum.
    i1, i2, i3 = cluster.deploy(instance_count=3)
    # These are being tested.
    i4 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i5 = cluster.add_instance(wait_online=True, replicaset_name="r99")

    # i4 is master as the first member of the replicaset.
    assert i4.replicaset_master_name() == i4.name
    assert i5.replicaset_master_name() == i4.name
    assert not i4.eval("return box.info.ro")
    assert i5.eval("return box.info.ro")

    # Terminate master to force switchover.
    i4.terminate()
    assert i5.replicaset_master_name() == i5.name
    assert not i5.eval("return box.info.ro")

    # Terminate the last remaining replica, switchover is impossible.
    i5.terminate()
    # FIXME: wait until governor handles all pending events
    time.sleep(0.5)
    assert i1.eval("return box.space._pico_replicaset:get(...).current_master_name", "r99") == i5.name

    # Wake the master back up, check it's not read only.
    i5.start()
    i5.wait_online()
    assert i5.replicaset_master_name() == i5.name
    assert not i5.eval("return box.info.ro")

    # Terminate it again, to check switchover at catch-up.
    i5.terminate()
    i4.start()
    i4.wait_online()

    # i4 is master again.
    assert i4.replicaset_master_name() == i4.name
    assert not i4.eval("return box.info.ro")

    i5.start()
    i5.wait_online()
    # i5 is still read only.
    assert i5.replicaset_master_name() == i4.name
    assert i5.eval("return box.info.ro")

    # Manually change master back to i5
    index, _ = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r99"],
        ops=[("=", "target_master_name", i5.name)],
    )
    cluster.raft_wait_index(index)

    assert i4.replicaset_master_name() == i5.name
    assert i4.eval("return box.info.ro")
    assert i5.replicaset_master_name() == i5.name
    assert not i5.eval("return box.info.ro")


def get_vclock_without_local(i: Instance):
    vclock = i.eval("return box.info.vclock")
    del vclock[0]

    vclock_array = [0] * (max(vclock.keys()) + 1)
    for k, v in vclock.items():
        vclock_array[k] = v

    return vclock_array


def test_replication_sync_before_master_switchover(cluster: Cluster):
    # These guys are for quorum.
    i1, i2, i3 = cluster.deploy(instance_count=3)
    # These are being tested.
    i4 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i5 = cluster.add_instance(wait_online=True, replicaset_name="r99")

    # Make sure i5 will not be able to synchronize before promoting
    i5.call("pico._inject_error", "TIMEOUT_WHEN_SYNCHING_BEFORE_PROMOTION_TO_MASTER", True)

    # Do some storage modifications, which will need to be replicated.
    i4.eval(
        """
        box.schema.space.create('mytable')
        box.space.mytable:create_index('pk')
        box.space.mytable:insert{1, 'hello'}
        box.space.mytable:insert{2, 'there'}
        box.space.mytable:insert{3, 'partner'}
        """
    )

    # Make sure i1 is leader.
    i1.promote_or_fail()

    # Initiate master switchover.
    index, _ = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r99"],
        ops=[("=", "target_master_name", i5.name)],
    )
    cluster.raft_wait_index(index)

    master_vclock = get_vclock_without_local(i4)

    # Wait until governor switches the replicaset master from i4 to i5
    # and tries to reconfigure replication between them which will require i5 to synchronize first.
    # This will block until i5 synchronizes with old master, which it won't
    # until the injected error is disabled.
    time.sleep(1)  # Just in case, nothing really relies on this sleep
    i1.wait_governor_status("transfer replication leader")

    # neither old master no new master is writable until the switchover is not finalized
    assert i4.eval("return box.info.ro") is True
    assert i5.eval("return box.info.ro") is True

    # Uninject the error, so it's able to continue synching.
    i5.call("pico._inject_error", "TIMEOUT_WHEN_SYNCHING_BEFORE_PROMOTION_TO_MASTER", False)

    # Wait until governor finishes with all the needed changes.
    i1.wait_governor_status("idle")

    assert i5.eval("return box.space.mytable.id") is not None
    vclock = get_vclock_without_local(i5)
    assert vclock >= master_vclock
    assert i5.eval("return box.info.ro") is False


def test_expel_blocked_by_replicaset_master_switchover_to_online_replica(
    cluster: Cluster,
):
    # These guys are for quorum.
    i1, i2, i3 = cluster.deploy(instance_count=3)
    # These are being tested.
    i4 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i5 = cluster.add_instance(wait_online=True, replicaset_name="r99")

    # Make sure i5 will not be able to synchronize before promoting
    i5.call("pico._inject_error", "TIMEOUT_WHEN_SYNCHING_BEFORE_PROMOTION_TO_MASTER", True)

    # Do some storage modifications, which will need to be replicated.
    i4.sql(""" CREATE TABLE mytable (id UNSIGNED PRIMARY KEY, value STRING) DISTRIBUTED BY (id) """)
    i4.sql(""" INSERT INTO mytable VALUES (0, 'foo'), (1, 'bar'), (2, 'baz') """)

    # Make sure i1 is leader.
    i1.promote_or_fail()

    # Initiate master switchover by expelling i4.
    with pytest.raises(CommandFailed) as e:
        cluster.expel(i4, timeout=1, force=True)
    assert "Timeout: expel confirmation didn't arrive in time" in e.value.stderr

    # Wait until governor switches the replicaset master from i4 to i5
    # and tries to reconfigure replication between them which will require i5 to synchronize first.
    # This will block until i5 synchronizes with old master, which it won't
    # until the injected error is disabled.
    time.sleep(1)  # Just in case, nothing really relies on this sleep
    i1.wait_governor_status("transfer replication leader")

    # i4 does not become expelled until the switchover if finalized
    cluster.wait_has_states(i4, "Online", "Expelled")

    # Uninject the error, so it's able to continue synching.
    i5.call("pico._inject_error", "TIMEOUT_WHEN_SYNCHING_BEFORE_PROMOTION_TO_MASTER", False)

    # Wait until governor finishes with all the needed changes.
    i1.wait_governor_status("idle")

    # Only now the instance gets expelled and shuts down
    i4.assert_process_dead()
    cluster.wait_has_states(i4, "Expelled", "Expelled")

    # i5 is the master
    assert i5.eval("return box.info.ro") is False
    # i5 is also synchronized
    rows = i5.sql(""" SELECT * FROM mytable ORDER BY id """)
    assert rows == [[0, "foo"], [1, "bar"], [2, "baz"]]


def test_expel_blocked_by_replicaset_master_switchover_to_offline_replica(
    cluster: Cluster,
):
    # These guys are for quorum.
    i1, i2, i3 = cluster.deploy(instance_count=3, init_replication_factor=3)
    # These are being tested.
    i4 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i5 = cluster.add_instance(wait_online=True, replicaset_name="r99")

    # i4 is the replicaset master because it was added first
    [[master_name]] = i4.sql(
        """ SELECT current_master_name FROM _pico_replicaset WHERE name = ? """,
        i4.replicaset_name,
    )
    assert master_name == i4.name

    # Shutdown i5 so it's offline and can't become replicaset master.
    i5.terminate()

    # Do some storage modifications, which will need to be replicated.
    i4.sql(
        """ CREATE TABLE mytable (id UNSIGNED PRIMARY KEY, value STRING) DISTRIBUTED BY (id) WAIT APPLIED LOCALLY """
    )
    i4.sql(""" INSERT INTO mytable VALUES (0, 'foo'), (1, 'bar'), (2, 'baz') """)

    # Make sure i1 is leader.
    i1.promote_or_fail()

    # Initiate master switchover by expelling i4.
    with pytest.raises(CommandFailed) as e:
        cluster.expel(i4, timeout=1, force=True)
    assert "Timeout: expel confirmation didn't arrive in time" in e.value.stderr

    # Wait until governor switches the replicaset master from i4 to i5
    # and tries to reconfigure replication between them which will require i5 to synchronize first.
    # This will block until i5 synchronizes with old master, which it won't
    # because it's currently offline.
    i1.wait_governor_status("transfer replication leader")

    # i4 does not become expelled until the switchover if finalized
    cluster.wait_has_states(i4, "Online", "Expelled")

    # Restart i5 so it's able to become the new master.
    i5.start()

    # Wait until governor finishes with all the needed changes.
    i1.wait_governor_status("idle", timeout=30)

    # Only now the instance gets expelled and shuts down
    i4.assert_process_dead()
    cluster.wait_has_states(i4, "Expelled", "Expelled")

    # i5 is the master
    assert i5.eval("return box.info.ro") is False
    # i5 is also synchronized
    rows = i5.sql(""" SELECT * FROM mytable ORDER BY id """)
    assert rows == [[0, "foo"], [1, "bar"], [2, "baz"]]


def test_offline_replicaset(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        raft:
            replication_factor: 1
            can_vote: true
        storage:
            replication_factor: 2
            can_vote: false
"""
    )
    [raft1, raft2, raft3] = cluster.deploy(instance_count=3, tier="raft")
    raft1.promote_or_fail()

    storage1 = cluster.add_instance(wait_online=False, tier="storage")
    storage2 = cluster.add_instance(wait_online=False, tier="storage")
    cluster.wait_online()

    counter = raft1.wait_governor_status("idle")

    assert storage1.replicaset_name == storage2.replicaset_name

    # Terminate the whole replicaset
    # NOTE: order of instance termination is reversed just to win some time
    # because master switchover is skipped, the other way round will also work fine
    storage2.terminate()
    storage1.terminate()

    # Make sure governor is not blocked by an offline replicaset
    raft1.wait_governor_status("idle", old_step_counter=counter)

    # Try adding a new instance
    storage3 = cluster.add_instance(wait_online=False, tier="storage")
    storage3.start()

    # Governor is blocked on the sharding configuration because there's only
    # one replicaset with non-zero weight and it's currently fully offline.
    # This means that all buckets are currently on the offline instances.
    # XXX There's probably something better we could do here, but for now this
    # is what's happening
    raft1.wait_governor_status("update current sharding configuration")

    # The solution at the moment is to wake up one of the instances in that replicaset
    storage1.start()

    # Now adding an instance works fine
    storage4 = cluster.add_instance(wait_online=True, tier="storage")
    storage3.wait_online()
    raft1.wait_governor_status("idle")

    assert storage4.replicaset_name != storage1.replicaset_name
    assert storage4.replicaset_name == storage3.replicaset_name

    # Terminate the another whole replicaset
    storage4.terminate()
    storage3.terminate()

    # This time the instances are added and become online just fine
    storage5 = cluster.add_instance(wait_online=True, tier="storage")
    storage6 = cluster.add_instance(wait_online=True, tier="storage")

    assert storage5.replicaset_name != storage1.replicaset_name
    assert storage5.replicaset_name == storage6.replicaset_name


def test_replication_rpc_protection_from_old_governor(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True)
    i2 = cluster.add_instance(wait_online=True)

    injection_hit = log_crawler(i1, "ERROR INJECTION 'BLOCK_REPLICATION_RPC_ON_CLIENT': BLOCKING")
    different_term_error = log_crawler(
        i1,
        "failed calling proc_replication: server responded with error: "
        "box error #10003: operation request from different term",
    )
    i3_replication_configured = log_crawler(i2, "configured replication with instance, instance_name: default_3_1")

    i1.call("pico._inject_error", "BLOCK_REPLICATION_RPC_ON_CLIENT", True)

    i3 = cluster.add_instance(wait_online=False)
    i3.start()

    # wait till governor starts to configure replication and gets to injected failure
    injection_hit.wait_matched()

    # bump term
    i2.promote_or_fail()

    # remove injected error
    i1.call("pico._inject_error", "BLOCK_REPLICATION_RPC_ON_CLIENT", False)

    # check i1 has expected error in the log
    different_term_error.wait_matched(timeout=2)

    # check i3 has replication configured by i2
    i3_replication_configured.wait_matched(timeout=2)


def test_replication_demote_protection_from_old_governor(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_name="r1", wait_online=True)
    i2 = cluster.add_instance(replicaset_name="r1", wait_online=True)

    assert not i1.eval("return box.info.ro"), "i1 should be master initially"
    assert i2.eval("return box.info.ro"), "i2 should be read-only initially"

    # enable an error to block the demotion of the master instance
    i1.call("pico._inject_error", "BLOCK_REPLICATION_DEMOTE", True)

    injection_hit = log_crawler(i1, "ERROR INJECTION 'BLOCK_REPLICATION_DEMOTE': BLOCKING")

    term_error = log_crawler(
        i1,
        "failed demoting old master and synchronizing new master: server responded with error: "
        "box error #10003: operation request from different term",
    )

    old_step_counter = i1.governor_step_counter()

    # update the replicaset configuration to set i2 as the new target master
    i1.sql("UPDATE _pico_replicaset SET target_master_name = ? WHERE name = 'r1'", i2.name)

    # wait for the error injection to block the demotion process
    injection_hit.wait_matched()

    # promote i2 to master. term is increased, new governor becomes active.
    # starting from this point the cluster should not accept actions from old governors
    i2.promote_or_fail()

    # remove the injected error that blocks the demotion
    i1.call("pico._inject_error", "BLOCK_REPLICATION_DEMOTE", False)

    term_error.wait_matched(timeout=2)

    # wait until governor performs all the necessary actions
    i2.wait_governor_status("idle", old_step_counter=old_step_counter)

    def check_replication():
        # check raft statuses
        i1.assert_raft_status("Follower")
        i2.assert_raft_status("Leader")

        # check that demote indeed changed the master in replicaset
        assert i1.eval("return box.info.ro"), "i1 should become read-only"
        assert not i2.eval("return box.info.ro"), "i2 should become master"

    Retriable(timeout=2).call(check_replication)


def test_stale_governor_replication_requests(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i2 = cluster.add_instance(wait_online=False)

    governor_message = "configuring replication"
    lc = log_crawler(i1, governor_message)

    cluster.wait_online()

    initial_term = i1.raft_term()

    # inject an error to cause a timeout during synchronization before promotion
    i1.call("pico._inject_error", "BLOCK_GOVERNOR_BEFORE_REPLICATION_CALL", True)

    # promote i2 to master
    i2.promote_or_fail()

    new_term = i2.raft_term()
    assert new_term > initial_term, "Term should increase after new leader election"

    # verify that i1 logs the replication configuration attempt
    lc.wait_matched(timeout=10)

    # remove the injected error that caused the synchronization timeout
    i1.call("pico._inject_error", "BLOCK_GOVERNOR_BEFORE_REPLICATION_CALL", False)

    def check_replication():
        i1.assert_raft_status("Follower")
        i2.assert_raft_status("Leader")

    Retriable(timeout=10).call(check_replication)
