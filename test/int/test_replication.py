import re
import time

import pytest
from conftest import (
    Cluster,
    CommandFailed,
    Instance,
    Retriable,
    TarantoolError,
    log_crawler,
)


def wait_vclock(i: Instance, vclock_expected: dict[int, int]):
    def check_vclock():
        vclock_actual = i.eval("return box.info.vclock")
        del vclock_actual[0]
        for k, v_exp in vclock_expected.items():
            assert (k, vclock_actual[k]) >= (k, v_exp)

    Retriable().call(check_vclock)


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

    Retriable().call(check_master_changed)
    assert i3.replicaset_master_name() == master_name

    old_leader = master
    master = i2
    if i2.name != master_name:
        master, i3 = i3, i2

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
    # These are being tested. The replication factor is 1, but the replicaset
    # has 3 members, so its synchro quorum is 2.
    i4 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i5 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i6 = cluster.add_instance(wait_online=True, replicaset_name="r99")

    def check_synchro_quorum(*instances: Instance):
        for i in instances:
            assert i.eval("return box.cfg.replication_synchro_quorum") == 2

    Retriable().call(check_synchro_quorum, i4, i5, i6)

    # A replication reconfiguration may depose the leader for a moment, until
    # it promotes itself again.
    def check_writable(i: Instance):
        assert not i.eval("return box.info.ro")

    # i4 is master as the first member of the replicaset.
    assert i4.replicaset_master_name() == i4.name
    Retriable().call(check_writable, i4)
    assert i5.eval("return box.info.ro")
    assert i6.eval("return box.info.ro")

    # Terminate master to force switchover to one of the replicas.
    i4.terminate()
    new_master_name = i5.replicaset_master_name()
    assert new_master_name in (i5.name, i6.name)
    new_master, last_replica = (i5, i6) if new_master_name == i5.name else (i6, i5)
    Retriable().call(check_writable, new_master)

    # Terminate the new master too. The mastership is retargeted to the last
    # replica, but it's below the quorum, so it can't be elected.
    new_master.terminate()
    cluster.wait_has_states(new_master, "Offline", "Offline")
    cluster.wait_governor_status("idle")
    assert i1.eval("return box.space._pico_replicaset:get(...).current_master_name", "r99") == new_master.name
    assert last_replica.eval("return box.info.ro")

    # Once the new master is back, the quorum is restored. The mastership is
    # retargeted to the last replica, but a candidate only gets the vote of a
    # replica it isn't behind. If the new master has writes the last replica
    # hasn't received, the last replica loses, and the governor retargets the
    # mastership back to the new master.
    new_master.start()
    new_master.wait_online()
    master_name = last_replica.replicaset_master_name()
    assert master_name in (new_master.name, last_replica.name)
    master, replica = (new_master, last_replica) if master_name == new_master.name else (last_replica, new_master)
    Retriable().call(check_writable, master)

    def check_read_only(*instances: Instance):
        for i in instances:
            assert i.eval("return box.info.ro")

    Retriable().call(check_read_only, replica)

    # The first master catches up and stays read only.
    i4.start()
    i4.wait_online()
    Retriable().call(check_synchro_quorum, i4)
    assert i4.replicaset_master_name() == master.name
    assert i4.eval("return box.info.ro")

    # Let the rebalancer stop writing to the master, and i4 catch up.
    # Otherwise i4 may lose the election, and the governor retargets the
    # mastership.
    cluster.wait_until_buckets_balanced()
    master_vclock = master.eval("return box.info.vclock")
    del master_vclock[0]
    wait_vclock(i4, master_vclock)

    # Manually change master back to i4
    index, _ = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r99"],
        ops=[("=", "target_master_name", i4.name)],
    )
    cluster.raft_wait_index(index)

    assert i4.replicaset_master_name() == i4.name
    Retriable().call(check_writable, i4)

    # The others are deposed when they receive i4's PROMOTE, which is
    # replicated after the promotion.
    Retriable().call(check_read_only, i5, i6)


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

    # Make sure i5 will not be able to win the election
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
    cluster.wait_leader_elected()

    # Initiate master switchover.
    index, _ = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r99"],
        ops=[("=", "target_master_name", i5.name)],
    )
    cluster.raft_wait_index(index)

    master_vclock = get_vclock_without_local(i4)

    # Wait until governor tries to synchronize i5 with the old master. This
    # will fail until the injected error is disabled.
    time.sleep(1)  # Just in case, nothing really relies on this sleep
    cluster.wait_governor_status("transfer replication leader")

    # The old master is fenced, so that it doesn't accept writes the new one
    # doesn't have. The new one isn't writable until it wins the election.
    def check_old_master_read_only():
        assert i4.eval("return box.info.ro") is True

    Retriable().call(check_old_master_read_only)
    assert i5.eval("return box.info.ro") is True

    # Uninject the error, so it's able to synchronize and win the election.
    i5.call("pico._inject_error", "TIMEOUT_WHEN_SYNCHING_BEFORE_PROMOTION_TO_MASTER", False)

    # Wait until governor finishes with all the needed changes.
    assert i5.replicaset_master_name() == i5.name
    cluster.wait_governor_status("idle")

    # i5 has everything the old master had.
    assert i5.eval("return box.space.mytable.id") is not None
    vclock = get_vclock_without_local(i5)
    assert vclock >= master_vclock

    def check_new_master_writable():
        assert i5.eval("return box.info.ro") is False

    Retriable().call(check_new_master_writable)

    # The old master stays read only, now deposed by i5's PROMOTE.
    Retriable().call(check_old_master_read_only)


@pytest.mark.skip_asan("Replication leader transfer timing is unreliable under ASan overhead")
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
    cluster.wait_leader_elected()

    # Initiate master switchover by expelling i4.
    with pytest.raises(CommandFailed) as e:
        cluster.expel(i4, timeout=1, force=True)
    assert "Timeout: expel confirmation didn't arrive in time" in e.value.stderr

    # Wait until governor switches the replicaset master from i4 to i5
    # and tries to reconfigure replication between them which will require i5 to synchronize first.
    # This will block until i5 synchronizes with old master, which it won't
    # until the injected error is disabled.
    time.sleep(1)  # Just in case, nothing really relies on this sleep
    cluster.wait_governor_status("transfer replication leader")

    # i4 does not become expelled until the switchover if finalized
    cluster.wait_has_states(i4, "Online", "Expelled")

    # Uninject the error, so it's able to continue synching.
    i5.call("pico._inject_error", "TIMEOUT_WHEN_SYNCHING_BEFORE_PROMOTION_TO_MASTER", False)

    # Wait until governor finishes with all the needed changes.
    cluster.wait_governor_status("idle")

    # Only now the instance gets expelled and shuts down
    cluster.wait_has_states(i4, "Expelled", "Expelled")
    Retriable().call(i4.assert_process_dead)

    # i5 is the master. Once i4 is gone, i5 has no synchro quorum until the
    # governor lowers it to 1, so it may resign for a moment, until it
    # promotes itself again.
    def check_writable():
        assert i5.eval("return box.info.ro") is False

    Retriable().call(check_writable)
    # i5 is also synchronized
    rows = i5.sql(""" SELECT * FROM mytable ORDER BY id """)
    assert rows == [[0, "foo"], [1, "bar"], [2, "baz"]]


def test_expel_blocked_by_replicaset_master_switchover_to_offline_replica(
    cluster: Cluster,
):
    # This test waits on the governor switching a replicaset master over while
    # a replica is offline, so keep the stock governor rpc timeouts instead of
    # the CI-tuned ones.
    cluster.tune_timeouts = False
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
    cluster.wait_leader_elected()

    # Initiate master switchover by expelling i4.
    with pytest.raises(CommandFailed) as e:
        cluster.expel(i4, timeout=1, force=True)
    assert "Timeout: expel confirmation didn't arrive in time" in e.value.stderr

    # The governor retargets the replicaset master from i4 to i5, but it can't
    # promote i5 while it's offline. i4 has writes i5 doesn't, so it's not
    # expelled until then.
    def check_master_retargeted():
        [[target_master_name]] = i1.sql(
            """ SELECT target_master_name FROM _pico_replicaset WHERE name = ? """,
            i4.replicaset_name,
        )
        assert target_master_name == i5.name

    Retriable().call(check_master_retargeted)
    progress = cluster.wait_governor_status("idle")

    # i4 does not become expelled until the switchover if finalized
    cluster.wait_has_states(i4, "Online", "Expelled")

    # Restart i5 so it's able to become the new master.
    i5.start()

    # Wait until governor finishes with all the needed changes. It's idle
    # already, so wait until it makes some progress first.
    cluster.wait_governor_status("idle", old_progress=progress)

    # Only now the instance gets expelled and shuts down
    cluster.wait_has_states(i4, "Expelled", "Expelled")
    Retriable().call(i4.assert_process_dead)

    # i5 is the master. Once i4 is gone, i5 has no synchro quorum until the
    # governor lowers it to 1, so it may resign for a moment, until it
    # promotes itself again.
    def check_writable():
        assert i5.eval("return box.info.ro") is False

    Retriable().call(check_writable)
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
    leader = cluster.add_instance(wait_online=False, tier="raft")
    storage1 = cluster.add_instance(wait_online=False, tier="storage")
    storage2 = cluster.add_instance(wait_online=False, tier="storage")
    cluster.wait_online()

    counter = cluster.wait_governor_status("idle")

    assert storage1.replicaset_name == storage2.replicaset_name

    # Terminate the whole replicaset
    # NOTE: order of instance termination is reversed just to win some time
    # because master switchover is skipped, the other way round will also work fine
    storage2.terminate()
    storage1.terminate()

    # Make sure governor is not blocked by an offline replicaset
    cluster.wait_governor_status("idle", old_progress=counter)

    # Adding a new replicaset
    storage3 = cluster.add_instance(wait_online=False, tier="storage")
    storage4 = cluster.add_instance(wait_online=False, tier="storage")

    storage3.start()
    storage4.start()
    storage3.wait_online()
    storage4.wait_online()

    assert storage4.replicaset_name != storage1.replicaset_name
    assert storage4.replicaset_name == storage3.replicaset_name

    # Terminate another whole replicaset
    storage4.terminate()
    storage3.terminate()

    # Add another replicaset
    storage5 = cluster.add_instance(wait_online=False, tier="storage")
    storage6 = cluster.add_instance(wait_online=False, tier="storage")

    storage5.start()
    storage6.start()
    storage5.wait_online()
    storage6.wait_online()

    assert storage5.replicaset_name != storage1.replicaset_name
    assert storage5.replicaset_name == storage6.replicaset_name

    #
    # Now let's see what happens when an offline replicaset has
    # target_master_name != current_master_name
    #

    cluster.wait_has_states(storage1, "Offline", "Offline")
    cluster.wait_has_states(storage2, "Offline", "Offline")

    [[current_master, target_master]] = leader.sql(
        "SELECT current_master_name, target_master_name FROM _pico_replicaset WHERE name = ?", storage1.replicaset_name
    )
    assert current_master == target_master

    counter = cluster.wait_governor_status("idle")

    # Request a master switchover in an Offline replicaset
    leader.sql(
        """
        UPDATE _pico_replicaset
        SET target_master_name = new_master
        FROM (
            SELECT i.name AS new_master
            FROM _pico_replicaset AS r
            JOIN _pico_instance AS i ON i.replicaset_name = r.name
            WHERE i.name != target_master_name AND r.name = ?
        )
        """,
        storage1.replicaset_name,
    )

    # Bump a vshard config version to force governor do perform a step
    leader.sql("UPDATE _pico_tier SET target_vshard_config_version = target_vshard_config_version + 1")
    cluster.wait_governor_status("idle", old_progress=counter)

    [[current_master, target_master]] = leader.sql(
        "SELECT current_master_name, target_master_name FROM _pico_replicaset WHERE name = ?", storage1.replicaset_name
    )
    assert current_master != target_master


def test_replication_rpc_protection_from_old_governor(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=True)
    i2 = cluster.add_instance(wait_online=True)

    injection_hit = log_crawler(i1, "ERROR INJECTION 'BLOCK_REPLICATION_RPC_ON_CLIENT': BLOCKING")
    different_term_error = log_crawler(
        i1,
        "failed calling proc_replication: server responded with error: "
        "TermMismatch: operation request from different term",
    )
    i3_replication_configured = log_crawler(i2, "configured replication with instance, instance_name: default_3_1")

    i1.call("pico._inject_error", "BLOCK_REPLICATION_RPC_ON_CLIENT", True)

    i3 = cluster.add_instance(wait_online=False)
    i3.start()

    # wait till governor starts to configure replication and gets to injected failure
    injection_hit.wait_matched()

    initial_term = i1.raft_term()

    # bump term by transferring leadership from i1 to i2
    i1.raft_transfer_leadership(i2.raft_id)

    # wait until i3 learns about the new term before letting i1's stale governor RPC through
    # otherwise the stale RPC would pass the term check and the expected error would never be logged
    def i3_learned_new_term():
        assert i3.raft_term() > initial_term

    Retriable().call(i3_learned_new_term)

    # remove injected error
    i1.call("pico._inject_error", "BLOCK_REPLICATION_RPC_ON_CLIENT", False)

    # check i1 has expected error in the log
    different_term_error.wait_matched()

    # check i3 has replication configured by i2
    i3_replication_configured.wait_matched()


def test_replication_promote_protection_from_old_governor(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_name="r1", wait_online=True)
    i2 = cluster.add_instance(replicaset_name="r1", wait_online=True)

    assert not i1.eval("return box.info.ro"), "i1 should be master initially"
    assert i2.eval("return box.info.ro"), "i2 should be read-only initially"

    # enable an error to block the governor right before it promotes the new master
    i1.call("pico._inject_error", "BLOCK_REPLICATION_PROMOTE", True)

    injection_hit = log_crawler(i1, "ERROR INJECTION 'BLOCK_REPLICATION_PROMOTE': BLOCKING")

    term_error = log_crawler(
        i1,
        "failed promoting new master via election: server responded with error: "
        "TermMismatch: operation request from different term",
    )

    # update the replicaset configuration to set i2 as the new target master
    i1.sql("UPDATE _pico_replicaset SET target_master_name = ? WHERE name = 'r1'", i2.name)

    # wait for the error injection to block the promotion
    injection_hit.wait_matched()

    # transfer leadership to i2. term is increased, new governor becomes active.
    # starting from this point the cluster should not accept actions from old governors
    i1.raft_transfer_leadership(i2.raft_id)

    # remove the injected error that blocks the promotion
    i1.call("pico._inject_error", "BLOCK_REPLICATION_PROMOTE", False)

    term_error.wait_matched()

    # wait until governor performs all the necessary actions
    # note that old_progress is not needed because leader is changed
    cluster.wait_governor_status("idle")

    def check_replication():
        # check raft statuses
        i1.assert_raft_status("Follower")
        i2.assert_raft_status("Leader")

        # the new governor has promoted i2 in an election, which deposed i1
        assert i1.eval("return box.info.ro"), "i1 should become read-only"
        assert not i2.eval("return box.info.ro"), "i2 should become master"

    Retriable().call(check_replication)


def test_stale_governor_replication_requests(cluster: Cluster):
    i1 = cluster.add_instance(wait_online=False)
    i2 = cluster.add_instance(wait_online=False)

    governor_message = "configuring replication"
    lc = log_crawler(i1, governor_message)

    cluster.wait_online()

    initial_term = i1.raft_term()

    # inject an error to cause a timeout during synchronization before promotion
    i1.call("pico._inject_error", "BLOCK_GOVERNOR_BEFORE_REPLICATION_CALL", True)

    # transfer leadership to i2
    i1.raft_transfer_leadership(i2.raft_id)

    new_term = i2.raft_term()
    assert new_term > initial_term, "Term should increase after new leader election"

    # verify that i1 logs the replication configuration attempt
    lc.wait_matched()

    # remove the injected error that caused the synchronization timeout
    i1.call("pico._inject_error", "BLOCK_GOVERNOR_BEFORE_REPLICATION_CALL", False)

    def check_replication():
        i1.assert_raft_status("Follower")
        i2.assert_raft_status("Leader")

    Retriable().call(check_replication)


def test_fixing_broken_replication(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_name="default_1", wait_online=True)
    i2 = cluster.add_instance(replicaset_name="default_1", wait_online=True)

    assert not i1.eval("return box.info.ro"), "i1 should be master initially"
    assert i2.eval("return box.info.ro"), "i2 should be read-only initially"

    # break replication
    i2.eval("box.cfg { read_only = false, replication = {} }")
    i1.eval("box.cfg { replication = {} }")

    # The replica can't make a record which would conflict with the master's:
    # it's an election follower, and box.cfg.read_only doesn't change that.
    with pytest.raises(TarantoolError, match="read-only"):
        i2.eval("box.schema.func.create('.my_func')")

    # repair replication
    i1.sql("UPDATE _pico_replicaset SET target_config_version = target_config_version + 1 WHERE name = 'default_1'")
    i2.raft_wait_index(i1.raft_get_index())
    cluster.wait_governor_status("idle")

    # it is OK now
    def check_replication_follows():
        info_replication = i1.eval("return box.info.replication")
        assert info_replication[2]["upstream"]["status"] == "follow"
        assert info_replication[2]["downstream"]["status"] == "follow"

    Retriable().call(check_replication_follows)

    # Without the replica the master lost the quorum and resigned. Now it's
    # the leader again, and the replica still follows it.
    def check_master_writable():
        assert not i1.eval("return box.info.ro")

    Retriable().call(check_master_writable)
    assert i2.eval("return box.info.ro")


def test_applier_cluster_uuid_mismatch(cluster: Cluster, second_cluster: Cluster):
    a = cluster.add_instance(wait_online=True)
    b = second_cluster.add_instance(wait_online=True)

    assert a.cluster_uuid != b.cluster_uuid

    uri_b = b.iproto_listen
    pattern = rf"applier/{re.escape(uri_b)}.*ER_PICO_CLUSTER_UUID_MISMATCH: Picodata cluster UUID mismatch"
    lc = log_crawler(a, pattern, use_regex=True)
    a.eval(f"box.cfg{{replication = {{'{uri_b}'}}}}")

    lc.wait_matched()


def assert_box_replication_follow(cluster: Cluster):
    for instance in cluster.instances:
        info = instance.eval("return box.info")

        # remove our own info from output
        own_id = info["id"]
        info_replication = info["replication"]
        del info_replication[own_id]

        # we have all instances (assumes there is one replicaset in the cluster)
        assert len(info_replication) == len(cluster.instances) - 1

        # ensure status==follow for each one
        for instance_replication_info in info_replication.values():
            assert instance_replication_info["upstream"]["status"] == "follow"
            assert instance_replication_info["downstream"]["status"] == "follow"


def assert_data_replicates(master: Instance, replicas: list[Instance]):
    master_vclock = master.get_vclock()
    del master_vclock[0]

    master.sql("""CREATE TABLE mytable (id UNSIGNED PRIMARY KEY, value STRING) DISTRIBUTED BY (id) """)
    master.sql("""INSERT INTO mytable VALUES (0, 'foo'), (1, 'bar'), (2, 'baz') """)

    new_master_vclock = master.get_vclock()
    del new_master_vclock[0]

    assert new_master_vclock != master_vclock
    master_vclock = new_master_vclock

    for replica in replicas:
        wait_vclock(replica, master_vclock)

    rows_replica: list[list[list[int | str]]] = [[] for _ in replicas]
    rows_master = []
    for i in range(3):
        for j, replica in enumerate(replicas):
            rows_replica[j].append(replica.eval(f"return box.space.mytable:get({i})"))
        rows_master.append(master.eval(f"return box.space.mytable:get({i})"))

    for rows in rows_replica:
        assert rows_master == rows


def test_iproto_tls_with_replication(cluster: Cluster):
    i1 = cluster.add_instance(replicaset_name="r1", wait_online=False)
    i2 = cluster.add_instance(replicaset_name="r1", wait_online=False)
    i3 = cluster.add_instance(replicaset_name="r1", wait_online=False)

    for i in cluster.instances:
        i.prepare_instance_for_iproto_tls()

    cluster.wait_online()

    master_name = i1.replicaset_master_name()

    master, replica1, replica2 = sorted([i1, i2, i3], key=lambda i: master_name == i.name, reverse=True)

    assert_data_replicates(master, [replica1, replica2])

    # The appliers may still be syncing after the last replication
    # reconfiguration: an applier only switches to `follow` once it receives
    # something from its upstream, e.g. a heartbeat.
    Retriable().call(assert_box_replication_follow, cluster)


def test_iproto_tls_enable_after_bootstrap(cluster: Cluster):
    """
    Test case when we setup cluster without tls first and then enable it
    """
    (i1, i2) = cluster.deploy(instance_count=2, init_replication_factor=2)
    cluster.wait_leader_elected()

    assert_box_replication_follow(cluster)

    for i in cluster.instances:
        i.prepare_instance_for_iproto_tls()

    i1.terminate()
    i2.terminate()

    i1.start()
    i2.start()
    cluster.wait_leader_elected()

    cluster.wait_online()

    master_name = i1.replicaset_master_name()

    master, replica = sorted([i1, i2], key=lambda i: master_name == i.name, reverse=True)

    assert_data_replicates(master, [replica])

    # After the restart the replica may still be syncing with the master.
    Retriable().call(assert_box_replication_follow, cluster)
