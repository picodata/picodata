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
    CommandFailed,
    Instance,
    Retriable,
    TarantoolError,
    log_crawler,
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


def get_master_instance(*instances) -> Instance:
    for i in instances:
        if not i.eval("return box.info.ro"):
            return i
    raise AssertionError("No writable master elected yet")


def sql_insert_retried(instance: Instance, query: str):
    """
    Retries a sharded INSERT while the cluster converges after a failover.
    """

    def do_insert():
        instance.sql(query)

    Retriable().call(do_insert)


def master_saw_bidirectional_replication(master, replica_id):
    assert master.eval(
        """
        local peer_id = ...
        local r = box.info.replication[peer_id]
        return r ~= nil
            and r.upstream ~= nil
            and r.upstream.status == 'follow'
            and r.downstream ~= nil
            and r.downstream.status == 'follow'
        """,
        replica_id,
    )


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
    master = get_master_instance(i1, i2, i3)

    # Verify synchro quorum is configured (replication_factor=3, quorum=2)
    quorum = master.eval("return box.cfg.replication_synchro_quorum")
    assert quorum == 2, f"Expected synchro quorum 2, got {quorum}"

    # Verify synchro timeout is configured
    timeout_val = master.eval("return box.cfg.replication_synchro_timeout")
    assert timeout_val == 3153600000, f"Expected synchro timeout 3153600000, got {timeout_val}"

    assert master.eval("return box.info.election.state") == "leader"
    assert master.eval("return box.info.synchro.quorum") == 2

    # Create a sharded table — it should get is_sync=true automatically
    master.sql("CREATE TABLE sync_test (id INT NOT NULL, val TEXT, PRIMARY KEY (id))")

    # Verify the space has is_sync flag
    is_sync = master.eval("return box.space.sync_test.is_sync")
    assert is_sync is True, f"Expected is_sync=true, got {is_sync}"

    # Write data and verify it appears on replicas
    master.sql("INSERT INTO sync_test VALUES (1, 'hello')")
    master.sql("INSERT INTO sync_test VALUES (2, 'world')")

    # Verify data on all replicas. The synchro quorum is 2 out of 3, so the
    # write is confirmed as soon as one replica besides the master persists
    # it — the remaining replica may still be catching up, hence the retries.
    def check_data_on_instance(instance):
        result = instance.eval("return box.space.sync_test:select()")
        assert result == [[1, 14, "hello"], [2, 30, "world"]]

    for instance in [i1, i2, i3]:
        Retriable().call(check_data_on_instance, instance)

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


def test_sync_replication_ddl_through_sync_follower(cluster: Cluster):
    """
    CREATE TABLE validation does a local dry-run of space creation
    (test_create_space) which requires the instance to be writable. On a sync
    tier box.info.ro is forced to true by tarantool raft elections on
    non-leaders, so the dry-run can't lift
    the read-onlyness and used to fail with ER_READONLY for global tables and
    for tables targeting an async tier whenever the DDL was submitted through
    a sync follower. The dry-run is skipped in clusters with sync tiers.
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

    sync_instances = [sync_i1, sync_i2, sync_i3]

    master = Retriable().call(get_master_instance, *sync_instances)
    follower = next(i for i in sync_instances if i is not master)

    # Follower is effectively read-only
    # because of elections, not because of box.cfg.read_only.
    assert follower.eval("return box.info.ro") is True
    assert follower.eval("return box.info.ro_reason") == "election"
    assert follower.eval("return box.cfg.read_only") is False

    # Global DDL through the sync follower.
    follower.sql("CREATE TABLE global_via_follower (id INT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED GLOBALLY")

    # Sharded DDL targeting the async tier through the sync follower.
    follower.sql(
        'CREATE TABLE async_via_follower (id INT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "async_tier"'
    )

    def space_exists(instance: Instance, name: str):
        assert instance.eval(f"return box.space.{name} ~= nil") is True

    # Global tables materialize on every instance.
    for i in [async_i, *sync_instances]:
        Retriable().call(space_exists, i, "global_via_follower")

    # The async-tier table exists on the async instance.
    Retriable().call(space_exists, async_i, "async_via_follower")

    # Global DML through the follower works too.
    follower.sql("INSERT INTO global_via_follower VALUES (1)")
    assert follower.sql("SELECT * FROM global_via_follower") == [[1]]


def test_sync_replication_terminate_replicas_rf3(cluster: Cluster):
    """
    Test that a sync replicaset (RF=3) stops accepting writes when it loses the
    synchro quorum (two of three instances down) and recovers when the
    instances return.
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

    # Terminate one replica — the quorum (2) is still met, writes still work.
    # Note: the replication reconfig which follows a member's departure can
    # transiently drop the applier health below the quorum, which self-fences
    # the elected leader; the designated master then promotes itself back,
    # which takes a moment. Re-discover the writable master.
    first_victim = replicas[0]
    first_victim_id = first_victim.eval("return box.info.id")
    Retriable().call(master_saw_bidirectional_replication, master, first_victim_id)
    first_victim.terminate()
    cluster.wait_has_states(first_victim, "Offline", "Offline")
    cluster.wait_governor_status("idle")

    def rediscover_master() -> Instance:
        return get_master_instance(*[i for i in [i1, i2, i3] if i is not first_victim])

    master = Retriable().call(rediscover_master)
    sql_insert_retried(master, "INSERT INTO t VALUES (2, 'still_ok')")

    # Terminate the second replica — the master is now below the synchro quorum,
    # so the governor stops advertising a vshard master for the replicaset and
    # writes can no longer be routed to it
    second_victim = next(i for i in replicas if i is not first_victim)
    second_victim_id = second_victim.eval("return box.info.id")
    Retriable().call(master_saw_bidirectional_replication, master, second_victim_id)
    second_victim.terminate()
    cluster.wait_has_states(second_victim, "Offline", "Offline")
    cluster.wait_governor_status("idle")

    master_id = master.eval("return box.info.id")

    def check_master_box_info():
        assert master.eval("return box.info.ro") is True
        assert master.eval("return box.info.ro_reason") == "election"
        assert master.eval("return box.info.election.leader") == 0
        assert master.eval("return box.info.election.state") == "follower"
        assert master.eval("return box.info.synchro.queue.owner") == master_id
        assert master.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(check_master_box_info)

    # Writes are rejected.
    with pytest.raises(TarantoolError):
        master.sql("INSERT INTO t VALUES (3, 'should_fail')", timeout=1)
    assert master.eval("return box.info.synchro.queue.len") == 0

    with pytest.raises(TimeoutError):
        master.sql(
            'CREATE TABLE t2 (id INT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"', timeout=1
        )
    assert master.eval("return box.info.synchro.queue.len") == 0

    # Restart the replicas — the quorum is restored and the designated master
    # (unchanged in the catalog: it stayed alive the whole time) promotes
    # itself back to election leadership, making the replicaset writable
    # again.
    first_victim.start()
    second_victim.start()
    first_victim.wait_online()
    second_victim.wait_online()
    cluster.wait_governor_status("idle")

    new_master = Retriable().call(get_master_instance, i1, i2, i3)
    new_master_id = new_master.eval("return box.info.id")

    # Writes work again
    sql_insert_retried(new_master, "INSERT INTO t VALUES (4, 'recovered')")

    def data_converged_everywhere():
        for i in [i1, i2, i3]:
            rows = i.eval("return box.space.t:select()")
            ids = {row[0] for row in rows}
            # Rows 1 and 2 were written normally, row 4 after the recovery.
            # Row 3 is absent because the quorum gate removed the vshard
            # master and rejected that write before it reached storage.
            assert {1, 2, 4} <= ids
            assert 3 not in ids
            # DDL was written in the raft log, so it was retried when the
            # quorum was restored
            assert i.eval("return box.space.t2:select()") == []
            # Check limbo owner and length
            assert i.eval("return box.info.synchro.queue.owner") == new_master_id
            assert i.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(data_converged_everywhere)


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
    master_id = master.eval("return box.info.id")
    replica_id = replica.eval("return box.info.id")

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'initial')")
    Retriable().call(master_saw_bidirectional_replication, master, replica_id)

    # Terminate replica
    replica.terminate()
    cluster.wait_has_states(replica, "Offline", "Offline")
    cluster.wait_governor_status("idle")

    def check_master_box_info():
        assert master.eval("return box.info.ro") is True
        assert master.eval("return box.info.ro_reason") == "election"
        assert master.eval("return box.info.election.leader") == 0
        assert master.eval("return box.info.election.state") == "follower"
        assert master.eval("return box.info.synchro.queue.owner") == master_id
        assert master.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(check_master_box_info)

    # Writes are rejected.
    with pytest.raises(TarantoolError):
        master.sql("INSERT INTO t VALUES (2, 'not_ok')", timeout=1)
    assert master.eval("return box.info.synchro.queue.len") == 0

    with pytest.raises(TimeoutError):
        master.sql(
            'CREATE TABLE t2 (id INT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"', timeout=1
        )
    assert master.eval("return box.info.synchro.queue.len") == 0

    replica.start_and_wait()
    cluster.wait_governor_status("idle")

    # Writes work again. The master was fenced read-only while below the
    # synchro quorum and promotes itself back only once the returning replica
    # restores it.
    def master_is_writable_again():
        assert not master.eval("return box.info.ro")

    Retriable().call(master_is_writable_again)
    sql_insert_retried(master, "INSERT INTO t VALUES (3, 'ok')")

    # The pending t2 DDL is applied only once the master is writable again, so
    # box.space.t2 may not exist yet on the first attempts.
    def replicaset_converged():
        for i in [master, replica]:
            # Data is okay
            res = i.eval("return box.space.t:select()")
            assert res == [
                [1, 14, "initial"],
                [3, 8, "ok"],
            ]
            # DDL is okay because it was written in the raft log and retried
            assert i.eval("return box.space.t2:select()") == []
            # Limbo owner and length are okay
            assert i.eval("return box.info.synchro.queue.owner") == master_id
            assert i.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(replicaset_converged)


def test_sync_replication_cascading_failover(cluster: Cluster):
    """
    Test cascading master failover in a sync tier (RF=3).
    Terminates the master, verifies a replica is promoted and can accept writes.
    Then terminates the new master, verifies the last replica cannot write
    (quorum=2 unmet). Restarts both terminated instances and
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
    cluster.wait_governor_status("idle")

    # The governor detects the master's death, retargets mastership to an
    # online replica and promotes it in a tarantool raft election
    # (proc_replication_promote). Poll until the promotion completes and the
    # new master becomes writable.
    new_master = Retriable().call(get_master_instance, *replicas)
    new_master.sql("INSERT INTO t VALUES (2, 'still_ok')")

    # Check RO, limbo owner and length
    assert not new_master.eval("return box.info.ro")
    new_master_id = new_master.eval("return box.info.id")
    assert new_master.eval("return box.info.synchro.queue.owner") == new_master_id
    assert new_master.eval("return box.info.synchro.queue.len") == 0

    last_replica = next(i for i in replicas if i != new_master)
    last_replica_id = last_replica.eval("return box.info.id")
    Retriable().call(master_saw_bidirectional_replication, new_master, last_replica_id)

    # Terminate new master
    new_master.terminate()
    cluster.wait_has_states(new_master, "Offline", "Offline")
    cluster.wait_governor_status("idle")

    # Only one instance is left, which is below the synchro quorum. The
    # governor retargets mastership to it but does not attempt the promotion
    # (an election below the quorum can never be won), so the replicaset stays
    # fenced read-only and the synchro queue ownership stays with the
    # previous, now terminated, master.
    def check_replica_box_info():
        assert last_replica.eval("return box.info.ro")
        assert last_replica.eval("return box.info.ro_reason") == "election"
        assert last_replica.eval("return box.info.synchro.queue.owner") == new_master_id
        assert last_replica.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(check_replica_box_info)

    # Writes are rejected: the replicaset is below synchro quorum.
    with pytest.raises(TarantoolError):
        last_replica.sql("INSERT INTO t VALUES (3, 'not_ok')", timeout=1)
    assert last_replica.eval("return box.info.synchro.queue.len") == 0

    # DDLs are blocked too
    with pytest.raises(TimeoutError):
        last_replica.sql(
            'CREATE TABLE t2 (id INT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"',
            timeout=1,
        )
    assert last_replica.eval("return box.info.synchro.queue.len") == 0

    # Return the terminated instances.
    master.start_and_wait()
    new_master.start_and_wait()
    cluster.wait_governor_status("idle")

    # Writes are unblocked
    last_replica.sql("INSERT INTO t VALUES (4, 'final_ok')")

    # After recovery the master designated by the governor is writable; the
    # synchro queue owner is its box id, identical on every replica since the
    # limbo state is replicated.
    new_leader = get_master_instance(i1, i2, i3)
    expected_owner = new_leader.eval("return box.info.id")

    for i in [i1, i2, i3]:
        # Data is okay. Note there is no row (3, 'not_ok'): unlike the
        # governor-driven model, the write on the fenced replicaset was rejected
        # outright (no master to route to) rather than delayed and later applied,
        # so it never reached the storage.
        res = i.eval("return box.space.t:select()")
        assert res == [
            [1, 14, "initial"],
            [2, 30, "still_ok"],
            [4, 22, "final_ok"],
        ]
        # DDL is okay because it was written in the raft log and retried
        assert i.eval("return box.space.t2:select()") == []
        # Limbo owner and length are okay
        assert i.eval("return box.info.synchro.queue.owner") == expected_owner
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

    for i in [i1, i2]:
        i.eval("box.cfg{replication_timeout=0.5}")

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'initial')")
    cluster.wait_governor_status("idle")

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
    Retriable().call(master_saw_bidirectional_replication, master, replica_id)
    replica.terminate()
    cluster.wait_has_states(replica, "Offline", "Offline")
    cluster.wait_governor_status("idle")

    # On quorum loss the master keeps the synchro queue ownership but no longer
    # accepts writes.
    def check_master_box_info():
        assert master.eval("return box.info.ro") is True
        assert master.eval("return box.info.ro_reason") == "election"
        assert master.eval("return box.info.election.leader") == 0
        assert master.eval("return box.info.synchro.queue.owner") == master_id
        assert master.eval("return box.info.synchro.queue.len") == 1

    Retriable().call(check_master_box_info)

    # Writes are rejected before reaching storage: below quorum the generated
    # vshard configuration does not advertise a master for this replicaset.
    with pytest.raises(TarantoolError):
        master.sql("INSERT INTO t VALUES (2, 'not_ok')", timeout=1)
    assert master.eval("return box.info.synchro.queue.len") == 1

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
    cluster.wait_governor_status("idle")

    new_master = get_master_instance(i1, i2)
    new_master_id = new_master.eval("return box.info.id")

    # Writes are unblocked.
    sql_insert_retried(new_master, "INSERT INTO t VALUES (3, 'ok')")

    def replicaset_converged():
        for i in [i1, i2]:
            # Data is okay
            assert i.eval("return box.space.t:select()") == [
                [1, 14, "initial"],
                [3, 8, "ok"],
                [42, 26, "in_flight"],
            ]
            assert i.eval("return box.space.t2:select()") == []
            # Limbo is okay
            assert i.eval("return box.info.synchro.queue.owner") == new_master_id
            assert i.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(replicaset_converged)


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

    i1 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier")
    cluster.wait_online()

    for i in [i1, i2]:
        i.eval("box.cfg{replication_timeout=0.5}")

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql(f"INSERT INTO t VALUES {','.join([str((i, 'initial')) for i in range(1, 17)])}")
    cluster.wait_governor_status("idle")
    replica_id = replica.eval("return box.info.id")
    Retriable().call(master_saw_bidirectional_replication, master, replica_id)

    # The replica dies ungracefully, so it never reports going Offline
    # itself. Speed up the automatic failure detection. Arm the aggressive
    # timeout as late as possible: while in effect it also auto-offlines
    # instances whose applied index transiently lags (routine on slow ASan
    # builds under load), causing spurious offlining and master switchover.
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")

    # The replica "segfaults" and cannot be restarted.
    replica.kill()
    cluster.wait_has_states(replica, "Offline", "Offline")
    cluster.wait_governor_status("idle")

    # On quorum loss the master keeps the synchro queue ownership but no longer
    # accepts writes.
    master_id = master.eval("return box.info.id")

    def check_master_box_info():
        assert master.eval("return box.info.ro") is True
        assert master.eval("return box.info.synchro.queue.owner") == master_id
        assert master.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(check_master_box_info)

    # Writes are rejected before reaching storage because the vshard master is
    # not advertised below quorum.
    with pytest.raises(TarantoolError):
        master.sql("INSERT INTO t VALUES (42, 'not_ok')", timeout=1)
    assert master.eval("return box.info.synchro.queue.len") == 0
    assert master.eval("return box.space.t:count()") == 16

    cluster.expel(replica)
    cluster.wait_has_states(replica, "Expelled", "Expelled")
    cluster.wait_governor_status("idle")


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

    for i in [i1, i2]:
        i.eval("box.cfg{replication_timeout=0.5}")

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql(f"INSERT INTO t VALUES {','.join([str((i, 'initial')) for i in range(1, 17)])}")
    cluster.wait_governor_status("idle")

    master_id = master.eval("return box.info.id")
    # Kill master
    master.kill()
    cluster.wait_has_states(master, "Offline", "Offline")
    cluster.wait_governor_status("idle")

    # Check RO, limbo owner and length
    assert replica.eval("return box.info.ro")
    assert replica.eval("return box.info.synchro.queue.owner") == master_id
    assert replica.eval("return box.info.synchro.queue.len") == 0

    # Writes are blocked, because read_only=true
    with pytest.raises(TarantoolError):
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
    #
    # The dead master is still recorded as the replicaset master: with the
    # replicaset below the synchro quorum no election could move the mastership
    # off the corpse, so the expel requires `force`.
    cluster.expel(master, force=True)
    cluster.wait_has_states(master, "Expelled", "Expelled")
    cluster.wait_governor_status("idle")


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

    i1 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r1")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier", replicaset_name="r1")
    cluster.wait_online()

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql(f"INSERT INTO t VALUES {','.join([str((i, 'confirmed')) for i in range(1, 17)])}")
    cluster.wait_governor_status("idle")

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

    # Speed up the failure detection of the killed master. Arm the aggressive
    # timeout only now: while in effect it also auto-offlines instances whose
    # applied index transiently lags (routine on slow ASan builds under load),
    # and a spurious master switchover would reset the manually raised synchro
    # quorum, confirming the deliberately stuck transaction above.
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")

    # The master "segfaults"
    master.kill()
    cluster.wait_has_states(master, "Offline", "Offline")
    cluster.wait_governor_status("idle")

    assert replica.eval("return box.info.ro")
    assert replica.eval("return box.info.synchro.queue.owner") == master_id
    assert replica.eval("return box.info.synchro.queue.len") == 1
    assert replica.eval("return box.space.t:count()") == 17

    # Writes are rejected: the replicaset is below the synchro quorum.
    with pytest.raises(TarantoolError):
        replica.sql("INSERT INTO t VALUES (43, 'not_ok')", timeout=1)
    assert replica.eval("return box.info.synchro.queue.len") == 1
    assert replica.eval("return box.space.t:count()") == 17

    # The dead master is still recorded as the replicaset master: with the
    # replicaset below the synchro quorum no election could move the mastership
    # off the corpse, so the expel requires `force`.
    cluster.expel(master, force=True)
    cluster.wait_has_states(master, "Expelled", "Expelled")
    cluster.wait_governor_status("idle")


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

    for i in [i1, i2]:
        i.eval("box.cfg{replication_timeout=0.5}")

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'initial')")
    cluster.wait_governor_status("idle")

    master_id = master.eval("return box.info.id")
    replica_id = replica.eval("return box.info.id")

    # Kill master
    master.kill()
    cluster.wait_has_states(master, "Offline", "Offline")

    # Restore the default auto-offline timeout now that the dead master has been
    # detected. The aggressive value above is only needed to notice the master's
    # death quickly; if it stays in effect it also spuriously auto-offlines the
    # newly-promoted replica. The replica transiently lags on its applied index
    # while promoting to master and applying the pending sync DDL without quorum
    # (the old master is dead), which trips the applied-index-lag heuristic. That
    # reverts the failover, so the old master comes back writable and the checks
    # below fail.
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 30")

    cluster.wait_governor_status("idle")

    # Check RO, limbo owner and length
    assert replica.eval("return box.info.ro")
    assert replica.eval("return box.info.synchro.queue.owner") == master_id
    assert replica.eval("return box.info.synchro.queue.len") == 0

    # Writes are blocked, because read_only=true
    with pytest.raises(TarantoolError):
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
    cluster.wait_governor_status("idle")

    # The retargeted replica wins its election in the watcher fiber, which is
    # decoupled from the governor, so an idle
    # governor does not yet imply the switchover is observable. Same for the
    # limbo state, which is replicated after the promotion.
    def switchover_converged():
        assert master.eval("return box.info.ro")
        assert not replica.eval("return box.info.ro")
        for i in [master, replica]:
            assert i.eval("return box.space.t:count()") == 1
            assert i.eval("return box.info.synchro.queue.owner") == replica_id
            assert i.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(switchover_converged)

    sql_insert_retried(replica, "INSERT INTO t VALUES (2, 'ok')")

    # The t2 DDL was stuck in the raft log while the replicaset had no writable
    # master; it is applied after the promotion, so box.space.t2 may not exist
    # yet on the first attempts.
    def write_and_ddl_converged():
        for i in [master, replica]:
            assert i.eval("return box.space.t:count()") == 2
            assert i.eval("return box.space.t2:count()") == 0

    Retriable().call(write_and_ddl_converged)


def test_sync_replication_cluster_death_and_return_rf2(cluster: Cluster):
    """
    Sync replicaset with RF=2, master and replica die abruptly but return back then.
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

    for i in [i1, i2]:
        i.eval("box.cfg{replication_timeout=0.5}")

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'initial')")
    cluster.wait_governor_status("idle")

    # Kill master
    master.kill()
    replica.kill()
    cluster.wait_has_states(master, "Offline", "Offline")
    cluster.wait_has_states(replica, "Offline", "Offline")
    cluster.wait_governor_status("idle")

    # Writes are blocked, because read_only=true
    with pytest.raises(Exception, match="Failed to get replicaset from bucket"):
        leader.sql("INSERT INTO t VALUES (42, 'not_ok')", timeout=1)

    # DDL is blocked too, but we have record about it in raft log
    with pytest.raises(TimeoutError):
        leader.sql(
            'CREATE TABLE t2 (id INT NOT NULL, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"', timeout=1
        )

    master.start()
    replica.start()
    cluster.wait_online()
    cluster.wait_governor_status("idle")

    new_master_name = i1.replicaset_master_name()
    new_master = next(i for i in [i1, i2] if i.name == new_master_name)
    new_replica = next(i for i in [i1, i2] if i.name != new_master_name)
    new_master_id = new_master.eval("return box.info.id")

    def new_master_promoted():
        assert new_replica.eval("return box.info.ro")
        assert not new_master.eval("return box.info.ro")

    Retriable().call(new_master_promoted)

    # The limbo state is replicated after the promotion, so the replica's view
    # of the queue owner converges a moment later.
    def limbo_converged():
        for i in [new_master, new_replica]:
            assert i.eval("return box.space.t:count()") == 1
            assert i.eval("return box.info.synchro.queue.owner") == new_master_id
            assert i.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(limbo_converged)

    sql_insert_retried(new_master, "INSERT INTO t VALUES (2, 'ok')")

    # The t2 DDL was stuck in the raft log while the whole replicaset was down;
    # it is applied after the promotion, so box.space.t2 may not exist yet on
    # the first attempts.
    def write_and_ddl_converged():
        for i in [new_master, new_replica]:
            assert i.eval("return box.space.t:count()") == 2
            assert i.eval("return box.space.t2:count()") == 0

    Retriable().call(write_and_ddl_converged)


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
    cluster.wait_governor_status("idle")

    # Determine new master.
    new_master = Retriable().call(get_master_instance, *replicas)
    new_master_name = new_master.name

    # Verify synchro queue ownership transferred
    new_master_id = new_master.eval("return box.info.id")
    other_replica = next(i for i in replicas if i.name != new_master_name)

    def queue_ownership_transferred():
        assert new_master.eval("return box.info.synchro.queue.owner") == new_master_id
        assert new_master.eval("return box.info.synchro.queue.len") == 0
        assert other_replica.eval("return box.info.synchro.queue.owner") == new_master_id

    Retriable().call(queue_ownership_transferred)

    # Sync writes succeed on new master
    res = new_master.sql("INSERT INTO t VALUES (2, 'after_expel')")
    assert res == {"row_count": 1}

    # Data is okay
    res = other_replica.eval("return box.space.t:select()")
    assert res == [[1, 14, "before_expel"], [2, 30, "after_expel"]]

    # Add new replica
    i4 = cluster.add_instance(tier="sync_tier")
    cluster.wait_governor_status("idle")

    res = i4.eval("return box.space.t:select()")
    assert res == [[1, 14, "before_expel"], [2, 30, "after_expel"]]

    # Joining a new member reconfigures replication on the whole replicaset,
    # which can transiently self-fence the leader; the designated master then
    # promotes itself back. Some member must end up the writable master.
    def replicaset_has_writable_master():
        return get_master_instance(new_master, other_replica, i4)

    Retriable().call(replicaset_has_writable_master)


def test_sync_replication_expel_replicas_rf3(cluster: Cluster):
    """
    Test expelling both replicas of an RF=3 sync replicaset.
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

    r1 = [cluster.add_instance(wait_online=False, tier="sync_tier") for _ in range(3)]
    cluster.wait_online(timeout=60)

    master_name = r1[0].replicaset_master_name()
    master = next(i for i in r1 if i.name == master_name)
    replicas = [i for i in r1 if i.name != master_name]
    replica_0_id = replicas[0].eval("return box.info.id")
    replica_1_id = replicas[1].eval("return box.info.id")

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'initial')")
    cluster.wait_governor_status("idle")

    # Expel both replicas of r1.
    Retriable().call(master_saw_bidirectional_replication, master, replica_0_id)
    cluster.expel(replicas[0], force=True)
    cluster.wait_has_states(replicas[0], "Expelled", "Expelled")
    Retriable().call(master_saw_bidirectional_replication, master, replica_1_id)
    cluster.expel(replicas[1], force=True)
    cluster.wait_has_states(replicas[1], "Expelled", "Expelled")
    cluster.wait_governor_status("idle")

    master_id = master.eval("return box.info.id")

    def check_master_box_info():
        assert master.eval("return box.info.ro")
        assert master.eval("return box.info.synchro.queue.owner") == master_id
        assert master.eval("return box.info.synchro.queue.len") == 0
        assert master.eval("return box.info.synchro.quorum") == 2

    Retriable().call(check_master_box_info)

    # Writes do not work: the master alone does not meet the synchro quorum.
    with pytest.raises(TimeoutError):
        master.sql("INSERT INTO t VALUES (42, 'ok')")
    assert master.eval("return box.space.t:count()") == 1
    assert master.eval("return box.info.synchro.queue.len") == 0


def test_sync_replication_expel_master_rf2(cluster: Cluster):
    """
    Test expelling the master of a two-node sync replicaset.
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

    # Create table and write initial data
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'initial')")
    cluster.wait_governor_status("idle")

    # Expel the master of r1. The governor promotes the replica first (the
    # master is alive, so this is a voluntary switchover via election), then
    # the master is expelled.
    # The expel CLI's connection to the peer can spuriously time out under
    # test load; proc_expel is idempotent, so just retry.
    Retriable().call(lambda: cluster.expel(master, force=True))
    cluster.wait_has_states(master, "Expelled", "Expelled")
    cluster.wait_governor_status("idle")

    replica_id = replica.eval("return box.info.id")

    def check_replica_box_info():
        assert replica.eval("return box.info.ro") is True
        assert replica.eval("return box.info.synchro.queue.owner") == replica_id
        assert replica.eval("return box.info.synchro.queue.len") == 0
        assert replica.eval("return box.info.synchro.quorum") == 2
        assert replica.eval("return box.info.election.leader") == 0

    Retriable().call(check_replica_box_info)

    # Writes do not work: the survivor alone does not meet synchro quorum.
    with pytest.raises(TimeoutError):
        replica.sql("INSERT INTO t VALUES (42, 'ok')", timeout=1)
    assert replica.eval("return box.space.t:count()") == 1
    assert replica.eval("return box.info.synchro.queue.len") == 0


def test_sync_replication_sync_before_promotion(cluster: Cluster):
    """
    Test the voluntary master switchover in a sync tier (RF=3): the master is
    retargeted via `target_master_name` and the governor promotes the target
    through a tarantool raft election (`box.ctl.promote`). The election itself
    provides the "synchronize before promotion" guarantee: votes are
    vclock-gated, so the target cannot win until it has caught up — there is
    no separate synchronization step anymore.

    Injects an error to block the promotion RPC and verifies the governor is
    stuck at the switchover step while the old master remains writable (the
    incumbent is never demoted first, so a stuck switchover causes no write
    outage). Then unblocks and verifies the new master owns the synchro queue
    and has all the data.
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

    # Governor should be stuck transferring replication leader: the promotion
    # RPC is blocked, so the "promoting new master via election" substep keeps
    # retrying and the status stays put.
    cluster.wait_governor_status("transfer replication leader")

    # The old master remains writable while the switchover is pending: the
    # incumbent is never demoted, it is deposed by the election which the
    # target wins. The target is still a read-only follower.
    assert master.eval("return box.info.ro") is False
    assert new_master.eval("return box.info.ro") is True

    # Unblock the promotion
    new_master.call("pico._inject_error", "TIMEOUT_WHEN_SYNCHING_BEFORE_PROMOTION_TO_MASTER", False)

    # Wait for switchover to complete
    cluster.wait_governor_status("idle")

    # New master owns the synchro queue and is writable
    assert not new_master.eval("return box.info.ro")
    new_master_id = new_master.eval("return box.info.id")
    assert new_master.eval("return box.info.synchro.queue.owner") == new_master_id

    # The old master was deposed by the election and is read-only now.
    assert master.eval("return box.info.ro") is True

    # New master has all the data from before switchover
    res = new_master.eval("return box.space.t:select()")
    assert res == [[1, 14, "hello"], [2, 30, "world"]]

    # Sync writes work on the new master
    new_master.sql("INSERT INTO t VALUES (3, 'after_switchover')")

    # The synchro quorum here is 2 of 3, so the write is confirmed as soon as
    # one replica besides the master has persisted it — the third member may
    # still be applying, hence the retries.
    def write_replicated_everywhere():
        for i in [new_master, replicas[1], master]:
            res = i.eval("return box.space.t:select()")
            assert res == [[1, 14, "hello"], [2, 30, "world"], [3, 8, "after_switchover"]]

    Retriable().call(write_replicated_everywhere)


def test_sync_replication_unlogged_tables_truncated_on_demotion(cluster: Cluster):
    """
    Unlogged (non-replicated) tables must be truncated on an instance that
    loses replicaset leadership through a tarantool raft election.
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

    cluster.deploy(instance_count=1, tier="arbiter")
    leader = cluster.leader()
    i1 = cluster.add_instance(wait_online=False, tier="sync_tier")
    i2 = cluster.add_instance(wait_online=False, tier="sync_tier")
    cluster.wait_online()

    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    replica = next(i for i in [i1, i2] if i.name != master_name)

    leader.sql('CREATE UNLOGGED TABLE u (id INT PRIMARY KEY) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO u VALUES (1), (2), (3)")
    [[count]] = master.sql("SELECT COUNT(*) FROM u")
    assert count == 3

    # Voluntary switchover: retarget the master; the governor promotes the
    # target through an election, which deposes the incumbent.
    replicaset_name = i1.replicaset_name
    index, _ = cluster.cas(
        "update",
        "_pico_replicaset",
        key=[replicaset_name],
        ops=[("=", "target_master_name", replica.name)],
    )
    cluster.raft_wait_index(index)
    cluster.wait_governor_status("idle")

    assert replica.eval("return box.info.ro") is False
    assert master.eval("return box.info.ro") is True

    # The unlogged table is not replicated, so the new master has no data.
    [[count]] = replica.sql("SELECT COUNT(*) FROM u")
    assert count == 0

    # The deposed master truncated its unlogged tables on losing leadership.
    # The truncation runs in the election-observer fiber, hence the retry.
    def check_truncated():
        assert master.eval("return box.space.u:bsize()") == 0

    Retriable().call(check_truncated)

    # Switch back: the data written before the first switchover must not
    # resurface on the re-elected original master.
    index, _ = cluster.cas(
        "update",
        "_pico_replicaset",
        key=[replicaset_name],
        ops=[("=", "target_master_name", master.name)],
    )
    cluster.raft_wait_index(index)
    cluster.wait_governor_status("idle")

    assert master.eval("return box.info.ro") is False
    [[count]] = master.sql("SELECT COUNT(*) FROM u")
    assert count == 0


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
    master = cluster.add_instance(tier="sync_tier")
    cluster.wait_governor_status("idle")

    # The governor may become idle before the election worker has promoted the
    # designated first member.
    master = Retriable().call(get_master_instance, master)
    master_id = master.eval("return box.info.id")
    assert master.eval("return box.info.synchro.queue.owner") == master_id
    assert master.eval("return box.info.synchro.queue.len") == 0
    assert master.eval("return box.info.synchro.quorum") == 2
    assert master.eval("return box.info.election.leader") == master_id

    master.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')

    assert not master.eval("return box.info.ro")
    assert master.eval("return box.info.synchro.queue.len") == 0

    # Replicaset is not ready yet.
    with pytest.raises(Exception, match="Failed to get replicaset from bucket"):
        master.sql("INSERT INTO t VALUES (1, 'initial')")

    # Once the new instance is online, the replicaset is ready and the writes
    # work.
    replica = cluster.add_instance(tier="sync_tier")
    cluster.wait_governor_status("idle")

    sql_insert_retried(master, "INSERT INTO t VALUES (1, 'initial')")

    def replicaset_converged():
        for i in [master, replica]:
            # Data is okay
            assert i.eval("return box.space.t:select()") == [[1, 14, "initial"]]
            # box.info is okay
            assert i.eval("return box.info.synchro.queue.owner") == master_id
            assert i.eval("return box.info.synchro.queue.len") == 0
            assert i.eval("return box.info.synchro.quorum") == 2
            assert i.eval("return box.info.election.leader") == master_id

    Retriable().call(replicaset_converged)


def test_sync_replication_quorum_loss_when_leader_in_sync_replicaset(cluster: Cluster):
    """
    Regression test for the governor self-unfence path (`handle_self_read_only`).
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
    replica_0_id = replicas[0].eval("return box.info.id")
    replica_1_id = replicas[1].eval("return box.info.id")

    # Kill two replicas: the sync replicaset drops below its synchro quorum (2),
    # but the cluster keeps raft quorum (arbiter voters + this master), so the
    # master stays the raft leader and keeps running the governor.
    Retriable().call(master_saw_bidirectional_replication, master, replica_0_id)
    replicas[0].terminate()
    Retriable().call(master_saw_bidirectional_replication, master, replica_1_id)
    replicas[1].terminate()

    # Below its synchro quorum the master retains synchro queue ownership and
    # stays the raft leader (the arbiters keep the picodata raft quorum), so the
    # governor must not retarget mastership away from it.
    cluster.wait_governor_status("idle")
    assert master.raft_leader_id() == master_raft_id, "master should remain the raft leader"

    def master_keeps_queue_ownership():
        assert master.eval("return box.info.synchro.queue.owner") == master_id
        assert master.eval("return box.info.ro") is True

    Retriable().call(master_keeps_queue_ownership)

    # Writes are rejected before reaching storage because the vshard master is
    # not advertised below quorum.
    with pytest.raises(TarantoolError):
        master.sql("INSERT INTO t VALUES (2, 'should_fail')", timeout=1)
    assert master.eval("return box.info.synchro.queue.len") == 0

    # Restart the replicas - the quorum is restored and the designated master
    # promotes itself back, making the replicaset writable again.
    # Discover the master instead of assuming, to keep the test robust against
    # a concurrent governor-driven retarget.
    replicas[0].start()
    replicas[1].start()
    replicas[0].wait_online()
    replicas[1].wait_online()
    cluster.wait_governor_status("idle")

    new_master = Retriable().call(get_master_instance, i1, i2, i3)
    new_master_id = new_master.eval("return box.info.id")

    # Writes work again (the master record / vshard config may briefly lag
    # behind the election result, hence the retry).
    sql_insert_retried(new_master, "INSERT INTO t VALUES (3, 'recovered')")

    def data_converged_everywhere():
        for i in [i1, i2, i3]:
            rows = i.eval("return box.space.t:select()")
            ids = {row[0] for row in rows}
            # Row 1 was written normally, row 3 after the recovery. Row 2 may
            # or may not be present: its write timed out client-side while the
            # master was fenced, but the server-side fiber keeps waiting for
            # writability and applies the statement if the old master is
            # re-elected
            # (https://git.picodata.io/core/picodata/-/work_items/2989).
            assert 1 in ids
            assert 3 in ids
            assert i.eval("return box.info.synchro.queue.owner") == new_master_id
            assert i.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(data_converged_everywhere)


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

    # The test kills everything except the replicaset master, so the picodata
    # raft leader (which the surviving cluster needs for recovery) must be the
    # master too. This usually holds after deploy, but not always — make it so.
    master_raft_id = master.instance_info()["raft_id"]
    if cluster.leader() != master:
        cluster.leader().raft_transfer_leadership(master_raft_id)

        def master_is_raft_leader():
            assert master.raft_leader_id() == master_raft_id

        Retriable().call(master_is_raft_leader)

    # While the killed replicas restart, replication on the still-alive master
    # is transiently broken. With the replication-error check enabled (the
    # default) the governor would spuriously auto-offline the master during the
    # recovery. Disable the check.
    # TODO: fix auto-offline strategy for this case
    # (https://git.picodata.io/core/picodata/-/issues/3002)
    master.sql("ALTER SYSTEM SET governor_check_replication_error = false")

    # Create table and write initial data.
    master.sql("CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id))")
    master.sql("INSERT INTO t VALUES (1, 'initial')")

    # Simulate a transaction which gets replicated but never confirmed: raise
    # the synchro quorum on the master so that the next write cannot gather
    # quorum. It reaches the replicas and sits in everyone's limbo. Doing this
    # BEFORE killing the replicas is necessary: killing drops the replication
    # connections instantly, which self-fences the master (tarantool raft steps
    # the leader down) before a write could enter the limbo.
    master.eval("box.cfg { replication_synchro_quorum = 4 }")
    with pytest.raises(TimeoutError):
        master.sql("INSERT INTO t VALUES (2, 'should_fail')", timeout=1)
    assert master.eval("return box.info.synchro.queue.len") == 1

    def in_flight_txn_reached_replicas():
        for r in replicas:
            assert r.eval("return box.info.synchro.queue.len") == 1

    Retriable().call(in_flight_txn_reached_replicas)

    replicas[0].kill()
    replicas[1].kill()

    def check_synchro_queue():
        # The transaction remains in the limbo, unconfirmed: below quorum it can
        # neither be confirmed nor rolled back.
        assert master.eval("return box.info.synchro.queue.len") == 1

    Retriable().call(check_synchro_queue)

    # Restart the replicas — the quorums are restored and the designated
    # master promotes itself back, making the replicaset writable again. All
    # three instances hold the in-limbo transaction (it was replicated before
    # the outage), and whichever instance ends up promoted confirms it with
    # its PROMOTE, so it survives.
    replicas[0].start_and_wait()
    replicas[1].start_and_wait()

    cluster.wait_governor_status("idle")

    new_master = Retriable().call(get_master_instance, *cluster.instances)
    new_master_id = new_master.eval("return box.info.id")

    # Writes work again.
    sql_insert_retried(new_master, "INSERT INTO t VALUES (3, 'recovered')")

    def data_converged_everywhere():
        for i in cluster.instances:
            assert i.eval("return box.space.t:select()") == [
                [1, 14, "initial"],
                [2, 30, "should_fail"],
                [3, 8, "recovered"],
            ]
            assert i.eval("return box.info.synchro.queue.owner") == new_master_id
            assert i.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(data_converged_everywhere)


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
    cluster.wait_governor_status("idle")

    # DDL is applied successfully.
    assert i3.eval("return box.space.t:select()") == []

    # DML is still mapped to r1, because r2 is not ready yet.
    i3.sql("INSERT INTO t VALUES (17, 'ok')")
    assert i3.eval("return box.space.t:count()") == 0
    for i in [master, replica]:
        assert i.eval("return box.space.t:count()") == 17

    i4 = cluster.add_instance(tier="sync_tier", replicaset_name="r2")
    cluster.wait_governor_status("idle")

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
    cluster.wait_governor_status("idle")

    # Wait for the rebalancer to spread buckets evenly across the two sync replicasets
    cluster.wait_until_buckets_balanced(exclude=arbiters)

    # Fix stale router cache
    Retriable().call(router_discovered_the_split, master)

    master.sql(f"INSERT INTO t VALUES {','.join([str((i, 'initial')) for i in range(1, 17)])}")

    assert i1.eval("return box.space.t:count()") == 9
    assert i3.eval("return box.space.t:count()") == 7

    # The replica dies ungracefully, so it never reports going Offline
    # itself. Speed up the automatic failure detection. Arm the aggressive
    # timeout as late as possible: while in effect it also auto-offlines
    # instances whose applied index transiently lags (routine on slow ASan
    # builds under load), causing spurious offlining and master switchover.
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")

    # The replica "segfaults" and cannot be restarted.
    replica.kill()
    cluster.wait_has_states(replica, "Offline", "Offline")
    cluster.wait_governor_status("idle")

    # On quorum loss the master keeps the synchro queue ownership but no longer
    # accepts writes.
    master_id = master.eval("return box.info.id")
    assert master.eval("return box.info.synchro.queue.owner") == master_id
    assert master.eval("return box.info.synchro.queue.len") == 0

    # Writes to r2 are okay
    assert master.sql("INSERT INTO t VALUES (18, 'initial')") == {"row_count": 1}
    # Writes to r1 are rejected before reaching storage because the vshard
    # master is not advertised below quorum.
    with pytest.raises((TimeoutError, TarantoolError)):
        master.sql("INSERT INTO t VALUES (17, 'initial')", timeout=1)

    # A distributed read also refuses to route through the fenced r1
    # replicaset. Verify the storage state locally instead.
    with pytest.raises(TarantoolError):
        master.sql("SELECT id FROM t ORDER BY id")
    assert master.eval("return box.space.t:get{17}") is None
    assert i3.eval("return box.space.t:get{17}") is None
    assert master.eval("return box.space.t:count()") == 9
    assert i3.eval("return box.space.t:count()") == 8


@pytest.mark.skip_asan(
    "relies on the replica syncing within the master's bootstrap stall window, unreliable under ASan overhead"
)
def test_sync_replication_master_online_last_on_bootstrap(cluster: Cluster):
    """
    Regression test for the governor `configure replication` step: the
    replicaset master must always be present in `box.cfg.replication` of its
    replicas, even while the master itself still needs a replication sync.

    Reproduces the race:

      1. The master (first instance of the replicaset, so it becomes the
         replicaset master on join) is held in its initial Offline(0) bootstrap
         state right before it would announce itself Online
         (STALL_BEFORE_UPDATE_OUR_STATE_TO_ONLINE).
      2. The replica joins, goes online and finishes its replication sync while
         the master is still bootstrapping. Because the master is in the initial
         Offline(0) state no master switchover happens (see
         `master_is_bootstrapping`), so the master stays the replicaset master.
      3. The master finally goes online. It still "needs sync" (its
         sync_incarnation lags its target incarnation). The buggy governor
         excluded such a waking-up instance from the replication config it sent
         to everyone -- including when that instance was the master itself. The
         replica thus never subscribed to the master, and for this *sync*
         replicaset (still NotReady, so `proc_replication` keeps is_master=true)
         the master's `box_promote()` could never gather a quorum -> the whole
         replicaset deadlocked: the master never became writable and the
         governor got stuck on "configure replication".

    With the fix the master is always kept in the replication config, the
    replica subscribes to it, `box_promote()` reaches quorum and the replicaset
    comes up normally.
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

    master = cluster.add_instance(name="m1", wait_online=False, tier="sync_tier", replicaset_name="r1")
    replica = cluster.add_instance(name="m2", wait_online=False, tier="sync_tier", replicaset_name="r1")

    # Hold the master in its initial Offline(0) bootstrap state: it joins (and
    # thus becomes r1's master) but stalls right before announcing itself Online.
    injection = "STALL_BEFORE_UPDATE_OUR_STATE_TO_ONLINE"
    stall_started = log_crawler(master, injection)
    master.env[f"PICODATA_ERROR_INJECTION_{injection}"] = "1"
    master.start()
    # Wait until the master has joined and is now stalling before going online.
    stall_started.wait_matched()

    # Sanity: the master is the replicaset master and is still Offline.
    def master_is_replicaset_master():
        [[current_master]] = leader.sql("SELECT current_master_name FROM _pico_replicaset WHERE name = 'r1'")
        assert current_master == master.name

    Retriable().call(master_is_replicaset_master)

    [[[state, _incarnation]]] = leader.sql("SELECT current_state FROM _pico_instance WHERE name = ?", master.name)
    assert state == "Offline"

    # Bring the replica online while the master is still stalled. By the time
    # wait_online returns the replica has already finished its replication sync,
    # because the governor only brings instances Online after the sync step.
    replica.start()
    replica.wait_online()

    # Now let the master proceed (the stall lasts a few seconds). With the bug it
    # gets excluded from the replication config, its box_promote() hangs and it
    # never becomes Online. With the fix it comes up normally.
    master.wait_online()
    cluster.wait_governor_status("idle")

    # The master is writable, owns the synchro queue, and synchronous writes
    # replicate to the replica.
    assert not master.eval("return box.info.ro")
    master_id = master.eval("return box.info.id")
    assert master.eval("return box.info.synchro.queue.owner") == master_id

    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'hello')")
    for i in [master, replica]:
        assert i.eval("return box.space.t:select()") == [[1, 14, "hello"]]
        assert i.eval("return box.info.synchro.queue.owner") == master_id
        assert i.eval("return box.info.synchro.queue.len") == 0


def test_sync_replication_lagging_replica_does_not_become_master_rf3(cluster: Cluster):
    """
    RF=3 sync replicaset: a master, an up-to-date ("fresh") replica and a
    lagging replica. The master dies together with the fresh replica, leaving
    the lagging replica the only survivor.
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
    lagging, fresh = [i for i in [i1, i2, i3] if i.name != master_name]

    # Create table and write initial data.
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'seen_by_all')")

    def initial_row_replicated_to_all():
        for i in [master, lagging, fresh]:
            assert i.eval("return box.space.t:count()") == 1

    Retriable().call(initial_row_replicated_to_all)

    # Disable the replication-error check: with it enabled the governor
    # notices the cut replication below within a fraction of a second and
    # re-pushes the replication config, which restores the lagging replica's
    # appliers and lets it catch up — the lag would evaporate. It would also
    # spuriously auto-offline the fresh replica when it later restarts with
    # the dead master still in its persisted replication config.
    leader.sql("ALTER SYSTEM SET governor_check_replication_error = false")

    # Make the replica lag.
    lagging.eval("box.cfg { replication = {} }")

    # These writes gather the synchro quorum of 2 (master + fresh replica) and
    # get confirmed. The lagging replica does not see them.
    master.sql("INSERT INTO t VALUES (2, 'confirmed_without_lagging')")
    master.sql("INSERT INTO t VALUES (3, 'confirmed_without_lagging')")

    def confirmed_on_fresh_replica():
        assert fresh.eval("return box.space.t:count()") == 3
        assert fresh.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(confirmed_on_fresh_replica)
    assert lagging.eval("return box.space.t:count()") == 1

    # The master dies ungracefully, so it never reports going Offline itself.
    # Speed up the automatic failure detection.
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")
    master.kill()
    # The fresh replica goes down gracefully right after the master's death,
    # making the lagging replica the only survivor. The master is already dead
    # and the fresh replica is going down, so the lagging replica cannot catch
    # up in the meantime.
    fresh.terminate()
    cluster.wait_has_states(master, "Offline", "Offline")
    cluster.wait_has_states(fresh, "Offline", "Offline")
    # Restore the default so the aggressive failure detection doesn't
    # spuriously mark the survivors Offline while the failover is in progress.
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 30")

    # The lagging replica is the only survivor, but it is below the synchro
    # quorum (1 of 3): the governor doesn't attempt to promote it (an election
    # below the quorum can never be won), so it stays read-only and still
    # lagging. The replicaset has no master (the record still points at the
    # dead master, transitioning), so writes cannot be routed.
    assert lagging.eval("return box.space.t:count()") == 1
    assert lagging.eval("return box.info.ro")
    with pytest.raises(TarantoolError):
        lagging.sql("INSERT INTO t VALUES (4, 'not_ok')", timeout=1)

    # The fresh replica returns, restoring the election quorum, and the
    # governor promotes the retargeted survivor. Votes are vclock-gated, so a
    # stale promotion target cannot win the election until it has caught up
    # with the fresh replica's confirmed history - which it does as soon as
    # replication between the survivors is re-established.
    fresh.start()
    fresh.wait_online()

    new_master = Retriable().call(get_master_instance, lagging, fresh)
    new_master_id = new_master.eval("return box.info.id")

    # The winner holds all the confirmed transactions and both survivors
    # converge on the full history - nothing is lost, no split brain, no
    # wedge.
    def survivors_converged():
        for i in [lagging, fresh]:
            assert i.eval("return box.space.t:count()") == 3
            assert i.eval("return box.info.synchro.queue.owner") == new_master_id
            assert i.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(survivors_converged)

    # The master record converges to the promoted leader and writes work again.
    def master_record_converged():
        assert lagging.replicaset_master_name() == new_master.name

    Retriable().call(master_record_converged)

    sql_insert_retried(new_master, "INSERT INTO t VALUES (4, 'after_failover')")

    def write_replicated_to_both():
        for i in [lagging, fresh]:
            assert i.eval("return box.space.t:count()") == 4
            assert i.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(write_replicated_to_both)


def test_sync_replication_master_death_with_lagging_replica_rf3(cluster: Cluster):
    """
    Same scenario as
    test_sync_replication_lagging_replica_does_not_become_master_rf3
    but the up-to-date ("fresh") replica stays alive the whole time: RF=3,
    one replica lags behind, the master is killed, both replicas remain
    Online.
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
    lagging, fresh = [i for i in [i1, i2, i3] if i.name != master_name]

    # Create table and write initial data replicated to everyone
    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'seen_by_all')")

    def initial_row_replicated_to_all():
        for i in [master, lagging, fresh]:
            assert i.eval("return box.space.t:count()") == 1

    Retriable().call(initial_row_replicated_to_all)

    # Make the replica lag.
    lagging.eval("box.cfg { replication = {} }")

    # These writes gather the synchro quorum of 2 (master + fresh replica) and
    # get confirmed. The lagging replica does not see them.
    master.sql("INSERT INTO t VALUES (2, 'confirmed_without_lagging')")
    master.sql("INSERT INTO t VALUES (3, 'confirmed_without_lagging')")

    def confirmed_on_fresh_replica():
        assert fresh.eval("return box.space.t:count()") == 3
        assert fresh.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(confirmed_on_fresh_replica)
    assert lagging.eval("return box.space.t:count()") == 1

    # The master dies ungracefully, so it never reports going Offline itself.
    # Speed up the automatic failure detection.
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")
    master.kill()
    cluster.wait_has_states(master, "Offline", "Offline")
    # Restore the default so the aggressive failure detection doesn't
    # spuriously mark the survivors Offline while the failover is in progress.
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 30")

    # Wait for the switchover. Either replica may win the pick.
    def switchover_happened():
        assert fresh.replicaset_master_name() != master.name

    Retriable().call(switchover_happened)
    new_master_name = fresh.replicaset_master_name()
    assert new_master_name in (lagging.name, fresh.name)
    new_master = lagging if new_master_name == lagging.name else fresh
    replica = fresh if new_master is lagging else lagging

    cluster.wait_governor_status("idle")

    # Whichever replica was promoted, it must own the synchro queue, be
    # writable and have the complete confirmed history: even a lagging pick
    # must catch up from the fresh replica before it can win the promotion
    # election (votes are vclock-gated).
    def new_master_promoted_with_all_data():
        assert not new_master.eval("return box.info.ro")
        new_master_id = new_master.eval("return box.info.id")
        assert new_master.eval("return box.info.synchro.queue.owner") == new_master_id
        assert new_master.eval("return box.space.t:count()") == 3

    Retriable().call(new_master_promoted_with_all_data)

    # The other replica converges too
    new_master_id = new_master.eval("return box.info.id")

    def replica_converged():
        assert replica.eval("return box.info.ro")
        assert replica.eval("return box.info.synchro.queue.owner") == new_master_id
        assert replica.eval("return box.space.t:count()") == 3

    Retriable().call(replica_converged)

    # Sync writes work: the new master and the surviving replica make the
    # synchro quorum of 2.
    new_master.sql("INSERT INTO t VALUES (4, 'after_failover')")
    for i in [new_master, replica]:
        assert i.eval("return box.space.t:count()") == 4
        assert i.eval("return box.info.synchro.queue.len") == 0


def test_sync_replication_promote_lost_election_retargets_to_fallback_rf3(cluster: Cluster):
    """
    RF=3 sync replicaset: the promotion of the target master loses the
    election, so the governor retargets the replicaset to a fallback candidate
    and promotes that one instead. Losing an election is injected rather than
    staged for real.
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

    replicaset_name = i1.replicaset_name
    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2, i3] if i.name == master_name)
    # `loser` is the replica the governor will retarget mastership to once the
    # master dies (it is the only survivor at that point), and its promotion is
    # the one which loses the election. `fallback` is the only candidate left
    # for the governor to retarget to afterwards.
    loser, fallback = [i for i in [i1, i2, i3] if i.name != master_name]

    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'before_failover')")

    def initial_row_replicated_to_all():
        for i in [master, loser, fallback]:
            assert i.eval("return box.space.t:count()") == 1

    Retriable().call(initial_row_replicated_to_all)

    # Disable the replication-error check: it would spuriously auto-offline the
    # restarted replica below, whose persisted replication config still points
    # at the dead master.
    leader.sql("ALTER SYSTEM SET governor_check_replication_error = false")

    # From now on this instance reports a lost election instead of running a
    # real one, so it can never become the master.
    loser.call("pico._inject_error", "LOSE_ELECTION_ON_PROMOTION", True)

    # The replica goes down first, so that when the master dies there is
    # exactly one instance left for the governor to retarget mastership to.
    fallback.terminate()
    cluster.wait_has_states(fallback, "Offline", "Offline")

    # The master dies ungracefully, so it never reports going Offline itself.
    # Speed up the automatic failure detection.
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 1")
    master.kill()
    cluster.wait_has_states(master, "Offline", "Offline")
    # Restore the default so the aggressive failure detection doesn't
    # spuriously mark the survivor Offline while the failover is in progress.
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 30")

    # The only survivor is retargeted, but not promoted: one live member of
    # three is below the synchro quorum, and an election below the quorum can
    # never be won. So it stays a read-only follower and the master record
    # still points at the dead master.
    def retargeted_to_the_only_survivor():
        [[current_master, target_master]] = leader.sql(
            "SELECT current_master_name, target_master_name FROM _pico_replicaset WHERE name = ?",
            replicaset_name,
        )
        assert target_master == loser.name
        assert current_master == master.name

    Retriable().call(retargeted_to_the_only_survivor)
    assert loser.eval("return box.info.ro")

    # The election leader the losing promotion reports back is the dead master
    # (nothing has bumped the term since it died), which is not an eligible
    # candidate anymore, so it doesn't override the rotation order.
    retargeted = log_crawler(
        leader,
        f"promotion of {loser.name} lost election, retargeting replicaset {replicaset_name}",
    )

    # The replica returns, restoring the quorum, so the governor promotes the
    # target it picked earlier. That promotion loses the election, and the
    # governor retargets the replicaset to the only eligible alternative.
    fallback.start()
    fallback.wait_online()

    retargeted.wait_matched()
    cluster.wait_governor_status("idle")

    # The fallback candidate is the new master: the catalog record converged,
    # it won the election and owns the synchro queue, and it has the data
    # written before the failover.
    def fallback_promoted():
        [[current_master, target_master]] = leader.sql(
            "SELECT current_master_name, target_master_name FROM _pico_replicaset WHERE name = ?",
            replicaset_name,
        )
        assert current_master == fallback.name
        assert target_master == fallback.name

        assert not fallback.eval("return box.info.ro")
        fallback_id = fallback.eval("return box.info.id")
        assert fallback.eval("return box.info.synchro.queue.owner") == fallback_id
        assert fallback.eval("return box.space.t:count()") == 1

    Retriable().call(fallback_promoted)

    # The target which lost the election never became writable.
    assert loser.eval("return box.info.ro")

    # Sync writes work again: the new master and the surviving replica make the
    # synchro quorum of 2.
    loser.call("pico._inject_error", "LOSE_ELECTION_ON_PROMOTION", False)
    sql_insert_retried(fallback, "INSERT INTO t VALUES (2, 'after_failover')")

    fallback_id = fallback.eval("return box.info.id")

    def write_replicated_to_both():
        for i in [fallback, loser]:
            assert i.eval("return box.space.t:count()") == 2
            assert i.eval("return box.info.synchro.queue.owner") == fallback_id
            assert i.eval("return box.info.synchro.queue.len") == 0

    Retriable().call(write_replicated_to_both)


def test_sync_replication_promote_lost_election_without_fallback_rf2(cluster: Cluster):
    """
    RF=2 sync replicaset whose master is being expelled: the governor retargets
    mastership to the only replica, and that promotion loses the election. The
    instance which is on its way out is not an eligible master candidate, so
    there is nobody left to retarget to. The governor reports the failure and
    keeps retrying the same target.

    Losing an election is injected rather than staged for real.
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

    replicaset_name = i1.replicaset_name
    master_name = i1.replicaset_master_name()
    master = next(i for i in [i1, i2] if i.name == master_name)
    # The only replica: the governor retargets mastership here once the master
    # is expelled, and this is the promotion which loses the election.
    loser = next(i for i in [i1, i2] if i.name != master_name)

    leader.sql('CREATE TABLE t (id INT NOT NULL, val TEXT, PRIMARY KEY (id)) DISTRIBUTED BY (id) IN TIER "sync_tier"')
    master.sql("INSERT INTO t VALUES (1, 'before_expel')")
    cluster.wait_governor_status("idle")

    def initial_row_replicated_to_both():
        for i in [master, loser]:
            assert i.eval("return box.space.t:count()") == 1

    Retriable().call(initial_row_replicated_to_both)

    # From now on this instance reports a lost election instead of running a
    # real one, so it can never become the master.
    loser.call("pico._inject_error", "LOSE_ELECTION_ON_PROMOTION", True)

    # The election leader the losing promotion reports back is the master being
    # expelled, which is not an eligible candidate, and it is the only other
    # member of the replicaset - so the fallback list is empty.
    no_fallback = log_crawler(
        leader,
        "failed retargeting failed synchronous promotion: promotion lost election and no fallback candidate exists",
    )

    # The master is still alive, so the expel requires `force`. It sets the
    # master's target state to Expelled, which makes the governor retarget
    # mastership to the replica, but the expel itself never completes: it is
    # blocked behind the switchover which can't happen.
    with pytest.raises(CommandFailed) as e:
        cluster.expel(master, force=True, timeout=3)
    assert "Timeout: expel confirmation didn't arrive in time" in e.value.stderr

    cluster.wait_has_states(master, "Online", "Expelled")
    no_fallback.wait_matched()

    # The switchover never happens: the retargeting stands, but the master
    # record still points at the instance being expelled.
    def stuck_mid_switchover():
        [[current_master, target_master]] = leader.sql(
            "SELECT current_master_name, target_master_name FROM _pico_replicaset WHERE name = ?",
            replicaset_name,
        )
        assert current_master == master.name
        assert target_master == loser.name

    Retriable().call(stuck_mid_switchover)

    # The target which keeps losing never becomes writable, and since it never
    # actually wins an election, the instance on its way out is never deposed
    # and remains the writable master.
    assert loser.eval("return box.info.ro")
    assert not master.eval("return box.info.ro")

    # The target can win elections again, so the switchover finally happens and
    # the expel it was blocking goes through.
    loser.call("pico._inject_error", "LOSE_ELECTION_ON_PROMOTION", False)

    cluster.wait_has_states(master, "Expelled", "Expelled")
    cluster.wait_governor_status("idle")

    loser_id = loser.eval("return box.info.id")

    def survivor_is_the_master():
        [[current_master, target_master]] = leader.sql(
            "SELECT current_master_name, target_master_name FROM _pico_replicaset WHERE name = ?",
            replicaset_name,
        )
        assert current_master == loser.name
        assert target_master == loser.name

        # It won the election and claimed the synchro queue, and it kept the
        # data written before the expel.
        assert loser.eval("return box.info.synchro.queue.owner") == loser_id
        assert loser.eval("return box.info.synchro.queue.len") == 0
        assert loser.eval("return box.space.t:count()") == 1

    Retriable().call(survivor_is_the_master)

    # Writes still do not work: the survivor alone does not meet the synchro
    # quorum of the RF=2 tier.
    with pytest.raises(TimeoutError):
        loser.sql("INSERT INTO t VALUES (42, 'ok')", timeout=1)
    assert loser.eval("return box.space.t:count()") == 1
