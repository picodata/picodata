"""
This testsuite does not cover recover process on synchro quorum loss
intentionally. Because the old vshard does not allow to make this process
easy, and we do not want to use workarounds.
We are waiting for the new vshard, then we will adjust the recovery
process in the code and tests.
"""

import pytest

from conftest import (
    Cluster,
    Instance,
    Retriable,
)


def router_discovered_the_split(i: Instance):
    """
    Helper that updates stale router cache.
    """
    replicasets_seen = i.eval("""
        local router = pico.router['sync_tier']
        router:discovery_wakeup()
        local seen = {}
        for bucket_id = 1, router:bucket_count() do
            local rs = router:route(bucket_id)
            if rs ~= nil then seen[rs.uuid] = true end
        end
        local count = 0
        for _ in pairs(seen) do count = count + 1 end
        return count
    """)
    assert replicasets_seen == 2, f"router still maps all buckets to {replicasets_seen} replicaset(s)"


def test_sync_replication_basic(cluster: Cluster):
    """
    Test that creating a sharded table in a sync tier sets is_sync flag on the space.
    """
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        sync_tier:
            replication_factor: 3
            replication_mode: sync
            bucket_count: 30
"""
    )

    i1, i2, i3 = cluster.deploy(instance_count=3, tier="sync_tier")

    # Find the master
    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2, i3] if i.name == master_name)

    # Verify synchro quorum is configured (replication_factor=3, quorum=2)
    quorum = master.eval("return box.cfg.replication_synchro_quorum")
    assert quorum == 2, f"Expected synchro quorum 2, got {quorum}"

    # Verify synchro timeout is configured
    timeout_val = master.eval("return box.cfg.replication_synchro_timeout")
    assert timeout_val == 3153600000, f"Expected synchro timeout 3153600000, got {timeout_val}"

    # Create a sharded table — it should get is_sync=true automatically
    master.sql("CREATE TABLE sync_test (id INT NOT NULL, val TEXT, PRIMARY KEY (id))")

    # Verify the space has is_sync flag
    is_sync = master.eval("return box.space.sync_test.is_sync")
    assert is_sync is True, f"Expected is_sync=true, got {is_sync}"

    # Write data and verify it appears on replicas
    master.sql("INSERT INTO sync_test VALUES (1, 'hello')")
    master.sql("INSERT INTO sync_test VALUES (2, 'world')")

    # Verify data on all replicas
    for instance in [i1, i2, i3]:
        result = instance.eval("return box.space.sync_test:select()")
        assert result == [[1, 14, "hello"], [2, 30, "world"]]

    # Create a global table - it should get is_sync=false automatically
    master.sql("CREATE TABLE global_test (id INT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED GLOBALLY")

    # Verify the space has is_sync=false flag
    is_sync = master.eval("return box.space.global_test.is_sync")
    assert is_sync is False, f"Expected is_sync=false, got {is_sync}"


def test_sync_tier_non_sync_tier_coexist(cluster: Cluster):
    """
    Test that a non-sync tier doesn't set is_sync, while sync tier does.
    """
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        async_tier:
            replication_factor: 1
            bucket_count: 30
        sync_tier:
            replication_factor: 3
            replication_mode: sync
            bucket_count: 30
"""
    )

    async_i = cluster.add_instance(wait_online=False, tier="async_tier")
    sync_i1 = cluster.add_instance(wait_online=False, tier="sync_tier")
    sync_i2 = cluster.add_instance(wait_online=False, tier="sync_tier")
    sync_i3 = cluster.add_instance(wait_online=False, tier="sync_tier")
    cluster.wait_online()

    # Find the sync tier master
    sync_master_name = sync_i1.replicaset_master_name()
    sync_master = next(i for i in [sync_i1, sync_i2, sync_i3] if i.name == sync_master_name)

    # Async tier should NOT have synchro quorum explicitly configured by picodata.
    async_quorum = async_i.eval("return box.cfg.replication_synchro_quorum")
    assert async_quorum != 2, f"Async tier should not have quorum=2, got {async_quorum}"

    # Create a table in async tier - should NOT be is_sync
    async_i.sql(
        'CREATE TABLE async_test (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "async_tier"'
    )

    is_sync = async_i.eval("return box.space.async_test.is_sync")
    assert is_sync is False, f"Expected is_sync=false for async tier, got {is_sync}"

    # Create a table in sync tier - should be is_sync
    sync_master.sql(
        'CREATE TABLE sync_test2 (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"'
    )

    is_sync = sync_master.eval("return box.space.sync_test2.is_sync")
    assert is_sync is True, f"Expected is_sync=true for sync tier, got {is_sync}"


def test_sync_replication_terminate_replicas_rf3(cluster: Cluster):
    """
    Test that a sync replicaset (RF=3) is fenced read-only when it loses the
    synchro quorum (two of three instances down) and recovers when the
    instances return. A separate arbiter tier keeps the raft quorum so the
    governor stays up to fence and later un-fence the replicaset.
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
        sync_tier:
            replication_factor: 3
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()
    i1 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i3 = cluster.add_instance(wait_online=False, tier="sync_tier")
    cluster.wait_online(timeout=60)

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2, i3] if i.name == master_name)
    replicas = [i for i in [i1, i2, i3] if i.name != master_name]

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'initial')")

    # Terminate one replica — the quorum (2) is still met, writes still succeed
    replicas[0].terminate()
    leader.wait_governor_status("idle")
    master.sql("INSERT INTO t VALUES (2, 'still_ok')")

    # Terminate the second replica — the master is now below the synchro quorum
    # and the governor fences the replicaset read-only
    replicas[1].terminate()
    leader.wait_governor_status("idle")

    assert master.eval("return box.info.ro")
    master_id = master.eval("return box.info.id")
    assert master.eval("return box.info.synchro.queue.owner") == master_id
    assert master.eval("return box.info.synchro.queue.len") == 0

    # Writes and DDLs are blocked: the instance is read-only, so they time out
    # without entering the synchro queue
    with pytest.raises(TimeoutError):
        master.sql("INSERT INTO t VALUES (3, 'should_fail')", timeout=1)
    assert master.eval("return box.info.synchro.queue.len") == 0

    with pytest.raises(TimeoutError):
        master.sql(
            'CREATE TABLE t2 (id INT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"', timeout=1
        )
    assert master.eval("return box.info.synchro.queue.len") == 0

    # Restart the replicas — the quorum is restored and the master un-fences
    replicas[0].start()
    replicas[1].start()
    replicas[0].wait_online()
    replicas[1].wait_online()
    leader.wait_governor_status("idle")

    # Writes work again
    master.sql("INSERT INTO t VALUES (4, 'recovered')")

    for i in [i1, i2, i3]:
        # Data is okay
        res = i.eval("return box.space.t:select()")
        assert res == [
            [1, 14, "initial"],
            [2, 30, "still_ok"],
            # This row is here because SQL executor was waiting for RW for a long time,
            # then executed the statement
            # even though the client already had timeout
            # https://git.picodata.io/core/picodata/-/work_items/2989
            [3, 8, "should_fail"],
            [4, 22, "recovered"],
        ]
        # DDL was written in the raft log, so it was retried when the quorum is restored
        assert i.eval("return box.space.t2:select()") == []
        # Check limbo owner and length
        assert i.eval("return box.info.synchro.queue.owner") == master_id
        assert i.eval("return box.info.synchro.queue.len") == 0


def test_sync_replication_terminate_replica_rf2(cluster: Cluster):
    """
    Test quorum loss and recovery in a two-node sync replicaset (RF=2).
    Terminates the only replica so the master cannot meet quorum (quorum=2),
    verifies writes fail with timeout.
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
        sync_tier:
            replication_factor: 2
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()
    i1 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier")
    cluster.wait_online()

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    res = master.sql("INSERT INTO t VALUES (1, 'initial')")

    # Terminate replica
    replica.terminate()
    leader.wait_governor_status("idle")

    # On quorum loss the replicaset is fenced read-only: the master keeps the
    # synchro queue ownership but no longer accepts writes.
    assert master.eval("return box.info.ro")
    master_id = master.eval("return box.info.id")
    assert master.eval("return box.info.synchro.queue.owner") == master_id
    assert master.eval("return box.info.synchro.queue.len") == 0

    # Writes are blocked: the instance is read-only, so the write times out
    # without entering the synchro queue.
    with pytest.raises(TimeoutError):
        master.sql("INSERT INTO t VALUES (2, 'not_ok')", timeout=1)
    assert master.eval("return box.info.synchro.queue.len") == 0

    # DDLs are blocked too
    with pytest.raises(TimeoutError):
        master.sql(
            'CREATE TABLE t2 (id INT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"', timeout=1
        )
    assert master.eval("return box.info.synchro.queue.len") == 0

    replica.start_and_wait()
    leader.wait_governor_status("idle")

    # Writes work again
    master.sql("INSERT INTO t VALUES (3, 'ok')")

    for i in [master, replica]:
        # Data is okay
        res = i.eval("return box.space.t:select()")
        assert res == [
            [1, 14, "initial"],
            # This row is here because SQL executor was waiting for RW for a long time,
            # then executed the statement
            # even though the client already had timeout
            # https://git.picodata.io/core/picodata/-/work_items/2989
            [2, 30, "not_ok"],
            [3, 8, "ok"],
        ]
        # DDL is okay because it was written in the raft log and retried
        assert i.eval("return box.space.t2:select()") == []
        # Limbo owner and length are okay
        assert i.eval("return box.info.synchro.queue.owner") == master_id
        assert i.eval("return box.info.synchro.queue.len") == 0


def test_sync_replication_cascading_failover(cluster: Cluster):
    """
    Test cascading master failover in a sync tier (RF=3).
    Terminates the master, verifies a replica is promoted and can accept writes.
    Then terminates the new master, verifies the last replica is promoted but
    cannot write (quorum=2 unmet). Restarts both terminated instances and
    verifies writes resume.
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
        sync_tier:
            replication_factor: 3
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()
    i1 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i3 = cluster.add_instance(wait_online=False, tier="sync_tier")
    cluster.wait_online(timeout=60)

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2, i3] if i.name == master_name)
    replicas = [i for i in [i1, i2, i3] if i.name != master_name]

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'initial')")

    # Terminate master
    master.terminate()
    # Wait for master switchover
    leader.wait_governor_status("idle")

    new_master_name = replicas[0].replicaset_master_name()
    new_master = next(i for i in replicas if i.name == new_master_name)
    new_master.sql("INSERT INTO t VALUES (2, 'still_ok')")

    # Check RO, limbo owner and length
    assert not new_master.eval("return box.info.ro")
    new_master_id = new_master.eval("return box.info.id")
    assert new_master.eval("return box.info.synchro.queue.owner") == new_master_id
    assert new_master.eval("return box.info.synchro.queue.len") == 0

    # Terminate new master
    new_master.terminate()
    # Wait for master switchover
    leader.wait_governor_status("idle")

    last_replica = next(i for i in replicas if i.name != new_master_name)
    last_replica_id = last_replica.eval("return box.info.id")
    # Only one instance is left, which is below the synchro quorum, so the
    # replicaset is fenced read-only and the failover does not promote it (the
    # synchro queue ownership stays with the previous, now terminated, master).
    assert last_replica.eval("return box.info.ro")
    assert last_replica.eval("return box.info.synchro.queue.owner") == new_master_id
    assert last_replica.eval("return box.info.synchro.queue.len") == 0

    # Writes are blocked: the instance is read-only.
    with pytest.raises(TimeoutError):
        last_replica.sql("INSERT INTO t VALUES (3, 'not_ok')", timeout=1)
    assert last_replica.eval("return box.info.synchro.queue.len") == 0

    # DDLs are blocked too
    with pytest.raises(TimeoutError):
        last_replica.sql(
            'CREATE TABLE t2 (id INT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"',
            timeout=1,
        )
    assert last_replica.eval("return box.info.synchro.queue.len") == 0

    # Return terminated instances. The pending transactions gather quorum and
    # get confirmed, which also unblocks the pending DDL.
    master.start()
    new_master.start()
    master.wait_online()
    new_master.wait_online()
    leader.wait_governor_status("idle")

    # Writes are unblocked
    last_replica.sql("INSERT INTO t VALUES (4, 'final_ok')")

    for i in [i1, i2, i3]:
        # Data is okay
        res = i.eval("return box.space.t:select()")
        assert res == [
            [1, 14, "initial"],
            [2, 30, "still_ok"],
            # This row is here because SQL executor was waiting for RW for a long time,
            # then executed the statement
            # even though the client already had timeout
            # https://git.picodata.io/core/picodata/-/work_items/2989
            [3, 8, "not_ok"],
            [4, 22, "final_ok"],
        ]
        # DDL is okay because it was written in the raft log and retried
        assert i.eval("return box.space.t2:select()") == []
        # Limbo owner and length are okay
        assert i.eval("return box.info.synchro.queue.owner") == last_replica_id
        assert i.eval("return box.info.synchro.queue.len") == 0


def test_sync_replication_master_change_with_non_empty_limbo_rf2(cluster: Cluster):
    """
    Test quorum loss and recovery in a two-node sync replicaset (RF=2).
    Kills the only replica so the master cannot meet quorum (quorum=2),
    verifies writes fail with timeout. Then kills the master too and restarts
    both (replica first), so the master switches over to the replica and the
    in-doubt transactions of the old master get confirmed.
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
        sync_tier:
            replication_factor: 2
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()
    i1 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier")
    cluster.wait_online()

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'initial')")
    leader.wait_governor_status("idle")

    master_id = master.eval("return box.info.id")
    replica_id = replica.eval("return box.info.id")

    # Simulate a transaction which gets replicated but whose CONFIRM is never
    # written: raise the synchro quorum on the master so that the next write
    # cannot gather quorum. This creates the same limbo state as a master
    # dying after relaying a transaction but before relaying its CONFIRM.
    master.eval("box.cfg { replication_synchro_quorum = 3 }")
    with pytest.raises(TimeoutError):
        master.sql("INSERT INTO t VALUES (42, 'in_flight')", timeout=1)
    assert master.eval("return box.info.synchro.queue.len") == 1

    # The transaction has reached the replica and sits in its limbo,
    # unconfirmed, owned by the master.
    def in_flight_txn_reached_replica():
        assert replica.eval("return box.info.synchro.queue.len") == 1

    Retriable().call(in_flight_txn_reached_replica)
    assert replica.eval("return box.info.synchro.queue.owner") == master_id

    # Terminate replica
    replica.terminate()
    leader.wait_governor_status("idle")

    # On quorum loss the replicaset is fenced read-only: the master keeps the
    # synchro queue ownership but no longer accepts writes.
    assert master.eval("return box.info.ro")
    assert master.eval("return box.info.synchro.queue.owner") == master_id
    assert master.eval("return box.info.synchro.queue.len") == 1

    # Writes are blocked: the instance is read-only, so the write times out
    # without entering the synchro queue.
    with pytest.raises(TimeoutError):
        master.sql("INSERT INTO t VALUES (2, 'not_ok')", timeout=1)
    assert master.eval("return box.info.synchro.queue.len") == 1

    # DDLs are blocked too
    with pytest.raises(TimeoutError):
        master.sql(
            'CREATE TABLE t2 (id INT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"', timeout=1
        )
    assert master.eval("return box.info.synchro.queue.len") == 1

    # Kill the master too, then bring the whole replicaset back. While the
    # master is down the replica cannot be promoted (it is below the synchro
    # quorum), so the replicaset stays fenced until both instances return and
    # quorum is restored.
    master.terminate()
    replica.start_and_wait()
    master.start_and_wait()
    leader.wait_governor_status("idle")

    # Writes are unblocked
    master.sql("INSERT INTO t VALUES (3, 'ok')")

    for i in [master, replica]:
        # Data is okay
        assert i.eval("return box.space.t:select()") == [
            [1, 14, "initial"],
            [3, 8, "ok"],
            [42, 26, "in_flight"],
        ]
        assert i.eval("return box.space.t2:select()") == []
        # Limbo is okay
        assert i.eval("return box.info.synchro.queue.owner") == replica_id
        assert i.eval("return box.info.synchro.queue.len") == 0


def test_sync_replication_replica_death_rf2(cluster: Cluster):
    """
    Sync replicaset with RF=2, replica dies abruptly and cannot returns back.
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
        sync_tier:
            replication_factor: 2
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()
    # The replica dies ungracefully, so it never reports going Offline
    # itself. Speed up the automatic failure detection.
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")

    i1 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier")
    cluster.wait_online()

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql(f"INSERT INTO t VALUES {','.join([str((i, 'initial')) for i in range(1, 17)])}")
    leader.wait_governor_status("idle")

    # The replica "segfaults" and cannot be restarted.
    replica.kill()
    cluster.wait_has_states(replica, "Offline", "Offline")
    leader.wait_governor_status("idle")

    # On quorum loss the replicaset is fenced read-only: the master keeps the
    # synchro queue ownership but no longer accepts writes.
    assert master.eval("return box.info.ro")
    master_id = master.eval("return box.info.id")
    assert master.eval("return box.info.synchro.queue.owner") == master_id
    assert master.eval("return box.info.synchro.queue.len") == 0

    # Writes are blocked, because read_only=true
    with pytest.raises(TimeoutError):
        master.sql("INSERT INTO t VALUES (42, 'not_ok')", timeout=1)
    assert master.eval("return box.info.synchro.queue.len") == 0
    assert master.eval("return box.space.t:count()") == 16

    # See this file's header, now we do not support recovery process
    cluster.expel(replica)
    cluster.wait_has_states(replica, "Expelled", "Expelled")
    leader.wait_governor_status("idle")


def test_sync_replication_master_death_rf2(cluster: Cluster):
    """
    Sync replicaset with RF=2, master dies abruptly and cannot return back.
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
        sync_tier:
            replication_factor: 2
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()
    i1 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r1")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r1")
    cluster.wait_online()

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql(f"INSERT INTO t VALUES {','.join([str((i, 'initial')) for i in range(1, 17)])}")
    leader.wait_governor_status("idle")

    master_id = master.eval("return box.info.id")
    # Kill master
    master.kill()
    cluster.wait_has_states(master, "Offline", "Offline")
    leader.wait_governor_status("idle")

    # Check RO, limbo owner and length
    assert replica.eval("return box.info.ro")
    assert replica.eval("return box.info.synchro.queue.owner") == master_id
    assert replica.eval("return box.info.synchro.queue.len") == 0

    # Writes are blocked, because read_only=true
    with pytest.raises(TimeoutError):
        replica.sql("INSERT INTO t VALUES (42, 'not_ok')", timeout=1)
    assert replica.eval("return box.info.synchro.queue.len") == 0

    # DDL is blocked too, but we have record about it in raft log
    with pytest.raises(TimeoutError):
        replica.sql(
            'CREATE TABLE t2 (id INT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"', timeout=1
        )
    assert replica.eval("return box.info.synchro.queue.len") == 0
    assert replica.eval("return box.space.t:count()") == 16

    # Cannot fix bad replicaset for now
    # See this file's header
    cluster.expel(master)
    cluster.wait_has_states(master, "Expelled", "Expelled")
    leader.wait_governor_status("apply clusterwide schema change")


def test_sync_replication_master_death_with_unconfirmed_txns_rf2(cluster: Cluster):
    """
    Test master failover in a sync replicaset (RF=2) when the master dies
    abruptly (e.g. segfaults) while its limbo contains a transaction which has
    been replicated to the replica but never confirmed. The replica's limbo
    thus holds a foreign unconfirmed entry owned by the dead master.
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
        sync_tier:
            replication_factor: 2
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()
    # Speed up the failure detection of the killed master
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")

    i1 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r1")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r1")
    cluster.wait_online()

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql(f"INSERT INTO t VALUES {','.join([str((i, 'confirmed')) for i in range(1, 17)])}")
    leader.wait_governor_status("idle")

    master_id = master.eval("return box.info.id")

    # Simulate a transaction which gets replicated but whose CONFIRM is never
    # written: raise the synchro quorum on the master so that the next write
    # cannot gather quorum. This creates the same limbo state as a master
    # dying after relaying a transaction but before relaying its CONFIRM.
    master.eval("box.cfg { replication_synchro_quorum = 3 }")
    with pytest.raises(TimeoutError):
        master.sql("INSERT INTO t VALUES (42, 'in_flight')", timeout=1)
    assert master.eval("return box.info.synchro.queue.len") == 1

    # The transaction has reached the replica and sits in its limbo,
    # unconfirmed, owned by the master.
    def in_flight_txn_reached_replica():
        assert replica.eval("return box.info.synchro.queue.len") == 1

    Retriable().call(in_flight_txn_reached_replica)
    assert replica.eval("return box.info.synchro.queue.owner") == master_id

    # The master "segfaults"
    master.kill()
    cluster.wait_has_states(master, "Offline", "Offline")
    leader.wait_governor_status("idle")

    assert replica.eval("return box.info.ro")
    assert replica.eval("return box.info.synchro.queue.owner") == master_id
    assert replica.eval("return box.info.synchro.queue.len") == 1
    assert replica.eval("return box.space.t:count()") == 17

    # Writes are blocked, because read_only=true
    with pytest.raises(TimeoutError):
        replica.sql("INSERT INTO t VALUES (43, 'not_ok')", timeout=1)
    assert replica.eval("return box.info.synchro.queue.len") == 1
    assert replica.eval("return box.space.t:count()") == 17

    cluster.expel(master)
    cluster.wait_has_states(master, "Expelled", "Expelled")
    leader.wait_governor_status("idle")


def test_sync_replication_master_death_and_return_rf2(cluster: Cluster):
    """
    Sync replicaset with RF=2, master dies abruptly but returns back.
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
        sync_tier:
            replication_factor: 2
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()
    i1 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r1")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r1")
    cluster.wait_online()

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'initial')")
    leader.wait_governor_status("idle")

    master_id = master.eval("return box.info.id")
    replica_id = replica.eval("return box.info.id")

    # Kill master
    master.kill()
    cluster.wait_has_states(master, "Offline", "Offline")
    leader.wait_governor_status("idle")

    # Check RO, limbo owner and length
    assert replica.eval("return box.info.ro")
    assert replica.eval("return box.info.synchro.queue.owner") == master_id
    assert replica.eval("return box.info.synchro.queue.len") == 0

    # Writes are blocked, because read_only=true
    with pytest.raises(TimeoutError):
        replica.sql("INSERT INTO t VALUES (42, 'not_ok')", timeout=1)
    assert replica.eval("return box.info.synchro.queue.len") == 0

    # DDL is blocked too, but we have record about it in raft log
    with pytest.raises(TimeoutError):
        replica.sql(
            'CREATE TABLE t2 (id INT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"', timeout=1
        )
    assert replica.eval("return box.info.synchro.queue.len") == 0
    assert replica.eval("return box.space.t:count()") == 1

    master.start_and_wait()
    leader.wait_governor_status("idle")

    assert master.eval("return box.info.ro")
    assert not replica.eval("return box.info.ro")

    for i in [master, replica]:
        assert i.eval("return box.space.t:count()") == 2
        assert i.eval("return box.info.synchro.queue.owner") == replica_id
        assert i.eval("return box.info.synchro.queue.len") == 0

    replica.sql("INSERT INTO t VALUES (2, 'ok')")

    for i in [master, replica]:
        assert i.eval("return box.space.t:count()") == 3


def test_sync_replication_expel_master_rf3(cluster: Cluster):
    """
    Test that expelling the sync tier master triggers a switchover.
    Verifies the new master owns the synchro queue, can accept sync writes,
    and that data written before the expel is preserved.
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
        sync_tier:
            replication_factor: 3
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()
    i1 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i3 = cluster.add_instance(wait_online=False, tier="sync_tier")
    cluster.wait_online(timeout=60)

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2, i3] if i.name == master_name)
    replicas = [i for i in [i1, i2, i3] if i.name != master_name]

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'before_expel')")

    # Expel the master — should trigger switchover to a replica
    cluster.expel(master, force=True)
    cluster.wait_has_states(master, "Expelled", "Expelled")
    leader.wait_governor_status("idle")

    # Determine new master
    new_master_name = replicas[0].replicaset_master_name()
    new_master = next(i for i in replicas if i.name == new_master_name)

    # Verify synchro queue ownership transferred
    assert not new_master.eval("return box.info.ro")
    new_master_id = new_master.eval("return box.info.id")
    assert new_master.eval("return box.info.synchro.queue.owner") == new_master_id
    assert new_master.eval("return box.info.synchro.queue.len") == 0
    other_replica = next(i for i in replicas if i.name != new_master_name)
    assert other_replica.eval("return box.info.synchro.queue.owner") == new_master_id

    # Sync writes succeed on new master
    res = new_master.sql("INSERT INTO t VALUES (2, 'after_expel')")
    assert res == {"row_count": 1}

    # Data is okay
    res = other_replica.eval("return box.space.t:select()")
    assert res == [[1, 14, "before_expel"], [2, 30, "after_expel"]]

    # Add new replica
    i4 = cluster.add_instance(tier="sync_tier")
    leader.wait_governor_status("idle")

    i4.eval("return box.space.t:select()")
    assert res == [[1, 14, "before_expel"], [2, 30, "after_expel"]]
    assert not new_master.eval("return box.info.ro")
    assert i4.eval("return box.info.ro")


def test_sync_replication_expel_replicas_rf3(cluster: Cluster):
    """
    Test both replicas are expelled and it loses quorum.
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
        sync_tier:
            replication_factor: 3
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")

    r1 = [cluster.add_instance(wait_online=False, tier="sync_tier") for _ in range(3)]
    cluster.wait_online(timeout=60)

    master_name = r1[0].replicaset_master_name()
    master = next(i for i in r1 if i.name == master_name)
    replicas = [i for i in r1 if i.name != master_name]

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql(f"INSERT INTO t VALUES {','.join([str((i, 'initial')) for i in range(1, 17)])}")
    leader.wait_governor_status("idle")

    # Expel both replicas of r1. The master is now below the synchro quorum, so
    # the replicaset is fenced read-only.
    cluster.expel(replicas[0], force=True)
    cluster.wait_has_states(replicas[0], "Expelled", "Expelled")
    cluster.expel(replicas[1], force=True)
    cluster.wait_has_states(replicas[1], "Expelled", "Expelled")
    leader.wait_governor_status("idle")

    assert master.eval("return box.info.ro")
    master_id = master.eval("return box.info.id")
    assert master.eval("return box.info.synchro.queue.owner") == master_id
    assert master.eval("return box.info.synchro.queue.len") == 0

    # Writes are blocked, because read_only=true
    with pytest.raises(TimeoutError):
        master.sql("INSERT INTO t VALUES (42, 'not_ok')", timeout=1)
    assert master.eval("return box.info.synchro.queue.len") == 0
    assert master.eval("return box.space.t:count()") == 16


def test_sync_replication_expel_master_rf2(cluster: Cluster):
    """
    Test master is expelled and it loses quorum.
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
        sync_tier:
            replication_factor: 2
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")

    i1 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r1")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r1")
    cluster.wait_online()

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql(f"INSERT INTO t VALUES {','.join([str((i, 'initial')) for i in range(1, 17)])}")
    leader.wait_governor_status("idle")

    # Expel the master of r1. The replica is now below the synchro quorum, so
    # the replicaset is fenced read-only.
    cluster.expel(master, force=True)
    cluster.wait_has_states(master, "Expelled", "Expelled")
    leader.wait_governor_status("idle")

    assert replica.eval("return box.info.ro")
    assert replica.eval("return box.info.synchro.queue.len") == 0

    # Writes are blocked, because read_only=true
    with pytest.raises(TimeoutError):
        replica.sql("INSERT INTO t VALUES (42, 'not_ok')", timeout=1)
    assert replica.eval("return box.info.synchro.queue.len") == 0
    assert replica.eval("return box.space.t:count()") == 16


def test_sync_replication_sync_before_promotion(cluster: Cluster):
    """
    Test that the new master synchronizes with the old master before promotion
    in a sync tier. Injects an error to block synchronization, triggers a manual
    switchover, verifies both instances are RO during the transition, then
    unblocks and verifies the new master owns the synchro queue and has all data.
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
        sync_tier:
            replication_factor: 3
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()
    i1 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i3 = cluster.add_instance(wait_online=False, tier="sync_tier")
    cluster.wait_online()

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2, i3] if i.name == master_name)
    replicas = [i for i in [i1, i2, i3] if i.name != master_name]
    new_master = replicas[0]

    # Create table and write data that needs to be replicated before promotion
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'hello')")
    master.sql("INSERT INTO t VALUES (2, 'world')")

    # Block new master from synchronizing before promotion
    new_master.call("pico._inject_error", "TIMEOUT_WHEN_SYNCHING_BEFORE_PROMOTION_TO_MASTER", True)

    # Initiate manual master switchover
    replicaset_name = i1.replicaset_name
    index, _ = cluster.cas(
        "update",
        "_pico_replicaset",
        key=[replicaset_name],
        ops=[("=", "target_master_name", new_master.name)],
    )
    cluster.raft_wait_index(index)

    # Governor should be stuck transferring replication leader: synchronization
    # is blocked, so the "demoting old master and synchronizing new master"
    # substep keeps retrying and the status stays put.
    leader.wait_governor_status("transfer replication leader")

    # Neither old master nor new master is writable during transition.
    def both_read_only():
        assert master.eval("return box.info.ro") is True
        assert new_master.eval("return box.info.ro") is True

    Retriable().call(both_read_only)

    # Unblock synchronization
    new_master.call("pico._inject_error", "TIMEOUT_WHEN_SYNCHING_BEFORE_PROMOTION_TO_MASTER", False)

    # Wait for switchover to complete
    leader.wait_governor_status("idle")

    # New master owns the synchro queue and is writable
    assert not new_master.eval("return box.info.ro")
    new_master_id = new_master.eval("return box.info.id")
    assert new_master.eval("return box.info.synchro.queue.owner") == new_master_id

    # New master has all the data from before switchover
    res = new_master.eval("return box.space.t:select()")
    assert res == [[1, 14, "hello"], [2, 30, "world"]]

    # Sync writes work on the new master
    new_master.sql("INSERT INTO t VALUES (3, 'after_switchover')")
    for i in [new_master, replicas[1], master]:
        res = i.eval("return box.space.t:select()")
        assert res == [[1, 14, "hello"], [2, 30, "world"], [3, 8, "after_switchover"]]


def test_sync_replication_bootstrap(cluster: Cluster):
    """
    Checks that a sharded table in the sync tier cannot accept writes
    until the replicaset is not ready.
    (This is true for writes via SQL for non-sync tier too)
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
        sync_tier:
            replication_factor: 2
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()
    master = cluster.add_instance(tier="sync_tier")
    leader.wait_governor_status("idle")

    assert not master.eval("return box.info.ro")
    master_id = master.eval("return box.info.id")
    assert master.eval("return box.info.synchro.queue.owner") == master_id
    assert master.eval("return box.info.synchro.queue.len") == 0
    assert master.eval("return box.cfg.replication_synchro_quorum") == 2

    master.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')

    assert not master.eval("return box.info.ro")
    assert master.eval("return box.info.synchro.queue.len") == 0

    # Replicaset is not ready yet.
    with pytest.raises(Exception, match="Failed to get replicaset from bucket"):
        master.sql("INSERT INTO t VALUES (1, 'initial')")

    # Once the new instance is online, the replicaset is ready and the writes
    # work.
    replica = cluster.add_instance(tier="sync_tier")
    leader.wait_governor_status("idle")
    master.sql("INSERT INTO t VALUES (1, 'initial')")
    for i in [master, replica]:
        # Data is okay
        assert i.eval("return box.space.t:select()") == [[1, 14, "initial"]]
        # Limbo is okay
        assert master.eval("return box.info.synchro.queue.owner") == master_id
        assert master.eval("return box.info.synchro.queue.len") == 0


def test_sync_replication_quorum_loss_when_leader_in_sync_replicaset(cluster: Cluster):
    """
    Regression test for the governor self-unfence path (`handle_self_read_only`).

    When the sync tier can vote, the raft leader — which runs the governor — may
    itself be the master of a sync replicaset. If that replicaset then loses its
    synchro quorum, the governor must keep it fenced read-only, exactly like the
    regular `proc_replication` quorum check does. It must NOT self-promote the
    master back to writable (which would silently undo the fencing) nor hang
    waiting for a quorum that can no longer be reached.

    Both tiers can vote here, so the cluster keeps raft quorum (3 arbiter voters
    + the surviving sync master = 4 of 6) after two sync replicas die, while the
    sync replicaset itself drops below its synchro quorum.
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
        sync_tier:
            replication_factor: 3
            can_vote: true
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3, tier="arbiter")
    i1 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i3 = cluster.add_instance(wait_online=False, tier="sync_tier")
    cluster.wait_online(timeout=60)

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2, i3] if i.name == master_name)
    replicas = [i for i in [i1, i2, i3] if i.name != master_name]

    # Move raft leadership onto the sync replicaset's master, so the governor
    # runs on the very instance that will lose its synchro quorum.
    master_raft_id = master.instance_info()["raft_id"]
    cluster.leader().raft_transfer_leadership(master_raft_id)

    def master_is_raft_leader():
        assert master.raft_leader_id() == master_raft_id

    Retriable().call(master_is_raft_leader)

    # Create table and write initial data.
    master.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'initial')")
    master_id = master.eval("return box.info.id")

    # Kill two replicas: the sync replicaset drops below its synchro quorum (2),
    # but the cluster keeps raft quorum (arbiter voters + this master), so the
    # master stays the raft leader and keeps running the governor.
    replicas[0].terminate()
    replicas[1].terminate()

    # The governor fences the replicaset: the master (the raft leader itself)
    # becomes read-only and retains synchro queue ownership.
    master.wait_governor_status("idle")
    assert master.raft_leader_id() == master_raft_id, "master should remain the raft leader"
    assert master.eval("return box.info.ro") is True, "fenced master must become read-only"

    # Writes are blocked while fenced: the read-only master times out without
    # entering the synchro queue.
    with pytest.raises(TimeoutError):
        master.sql("INSERT INTO t VALUES (2, 'should_fail')", timeout=1)
    assert master.eval("return box.info.synchro.queue.len") == 0

    # Restart the replicas — the quorum is restored and the master un-fences.
    replicas[0].start()
    replicas[1].start()
    replicas[0].wait_online()
    replicas[1].wait_online()
    master.wait_governor_status("idle")

    assert master.eval("return box.info.ro") is False

    # Writes work again.
    master.sql("INSERT INTO t VALUES (3, 'recovered')")

    for i in [i1, i2, i3]:
        assert i.eval("return box.space.t:select()") == [
            [1, 14, "initial"],
            [2, 30, "should_fail"],
            [3, 8, "recovered"],
        ]
        assert i.eval("return box.info.synchro.queue.owner") == master_id
        assert i.eval("return box.info.synchro.queue.len") == 0


def test_sync_replication_raft_and_synchro_quorum_loss(cluster: Cluster):
    """
    Test that cluster loses both raft quorum and synchro quorum simultaneously
    and restores then.
    """
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        sync_tier:
            replication_factor: 3
            can_vote: true
            replication_mode: sync
            bucket_count: 30
"""
    )

    cluster.deploy(instance_count=3)

    master_name = cluster.instances[0].replicaset_master_name()
    master = next(i for i in cluster.instances if i.name == master_name)
    replicas = [i for i in cluster.instances if i.name != master_name]

    assert cluster.leader() == master
    master_id = master.eval("return box.info.id")

    # Create table and write initial data.
    master.sql("CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id))")
    master.sql("INSERT INTO t VALUES (1, 'initial')")

    replicas[0].kill()
    replicas[1].kill()
    master.wait_governor_status("idle")

    # Writes are blocked, no synchro quorum.
    with pytest.raises(TimeoutError):
        master.sql("INSERT INTO t VALUES (2, 'should_fail')", timeout=1)
    # Fencing does not work, no raft quorum.
    assert not master.eval("return box.info.ro")
    # The transaction is in the limbo.
    assert master.eval("return box.info.synchro.queue.len") == 1

    # Restart the replicas — the quorums are restored.
    replicas[0].start_and_wait()
    replicas[1].start_and_wait()
    cluster.leader().wait_governor_status("idle")

    assert not master.eval("return box.info.ro")

    # Writes work again.
    master.sql("INSERT INTO t VALUES (3, 'recovered')")

    for i in cluster.instances:
        assert i.eval("return box.space.t:select()") == [
            [1, 14, "initial"],
            [2, 30, "should_fail"],
            [3, 8, "recovered"],
        ]
        assert i.eval("return box.info.synchro.queue.owner") == master_id
        assert i.eval("return box.info.synchro.queue.len") == 0


def test_sync_replication_add_new_replicaset_rf2(cluster: Cluster):
    """
    Test synchronous replication with addition of new replicaset.
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
        sync_tier:
            replication_factor: 2
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    arbiters = cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()

    i1 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r1")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r1")
    cluster.wait_online()

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    # Create table and write initial data
    master.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql(f"INSERT INTO t VALUES {','.join([str((i, 'initial')) for i in range(1, 17)])}")

    for i in [master, replica]:
        assert i.eval("return box.space.t:count()") == 16

    i3 = cluster.add_instance(tier="sync_tier", replicaset_name="r2")
    leader.wait_governor_status("idle")

    # DDL is applied successfully.
    assert i3.eval("return box.space.t:select()") == []

    # DML is still mapped to r1, because r2 is not ready yet.
    i3.sql("INSERT INTO t VALUES (17, 'ok')")
    assert i3.eval("return box.space.t:count()") == 0
    for i in [master, replica]:
        assert i.eval("return box.space.t:count()") == 17

    i4 = cluster.add_instance(tier="sync_tier", replicaset_name="r2")
    leader.wait_governor_status("idle")

    # Wait for the rebalancer to spread buckets evenly across the two sync replicasets
    cluster.wait_until_buckets_balanced(exclude=arbiters)

    # Fix stale router cache
    Retriable().call(router_discovered_the_split, master)

    i3.sql("INSERT INTO t VALUES (18, 'ok')")

    def instances_have_data():
        assert master.eval("return box.space.t:count()") == 11
        assert i3.eval("return box.space.t:count()") == 7
        assert replica.eval("return box.space.t:count()") == 11
        assert i4.eval("return box.space.t:count()") == 7

    Retriable().call(instances_have_data)


def test_sync_replication_two_replicasets_rf2(cluster: Cluster):
    """
    Test synchronous replication with two replicasets.
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
        sync_tier:
            replication_factor: 2
            can_vote: false
            replication_mode: sync
            bucket_count: 30
"""
    )

    arbiters = cluster.deploy(instance_count=3, tier="arbiter")
    leader = cluster.leader()
    # The replica dies ungracefully, so it never reports going Offline
    # itself. Speed up the automatic failure detection.
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")

    i1 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r1")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r1")
    cluster.wait_online()
    i3 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r2")
    cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r2")
    cluster.wait_online()

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    leader.wait_governor_status("idle")

    # Wait for the rebalancer to spread buckets evenly across the two sync replicasets
    cluster.wait_until_buckets_balanced(exclude=arbiters)

    # Fix stale router cache
    Retriable().call(router_discovered_the_split, master)

    master.sql(f"INSERT INTO t VALUES {','.join([str((i, 'initial')) for i in range(1, 17)])}")

    assert i1.eval("return box.space.t:count()") == 9
    assert i3.eval("return box.space.t:count()") == 7

    # The replica "segfaults" and cannot be restarted.
    replica.kill()
    cluster.wait_has_states(replica, "Offline", "Offline")
    leader.wait_governor_status("idle")

    # On quorum loss the replicaset is fenced read-only: the master keeps the
    # synchro queue ownership but no longer accepts writes.
    assert master.eval("return box.info.ro")
    master_id = master.eval("return box.info.id")
    assert master.eval("return box.info.synchro.queue.owner") == master_id
    assert master.eval("return box.info.synchro.queue.len") == 0

    # Writes to r2 are okay
    assert master.sql("INSERT INTO t VALUES (18, 'initial')") == {"row_count": 1}
    # Writes to r1 are blocked
    with pytest.raises(TimeoutError):
        master.sql("INSERT INTO t VALUES (17, 'initial')", timeout=1)

    assert master.sql("SELECT id FROM t ORDER BY id") == [[i] for i in range(1, 19) if i != 17]
    assert master.eval("return box.space.t:count()") == 9
    assert i3.eval("return box.space.t:count()") == 8
