import time

from conftest import Cluster, log_crawler


_3_SEC = 3


def test_bootstrap_from_snapshot(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    ret, _ = i1.cas("insert", "_pico_property", ["animal", "horse"])
    i1.raft_wait_index(ret, _3_SEC)
    assert i1.raft_read_index(_3_SEC) == ret

    # Compact the log up to insert
    i1.raft_compact_log(ret + 1)

    # Ensure i2 bootstraps from a snapshot
    i2 = cluster.add_instance(wait_online=True)
    # The snapshot was receieved and applied.
    # Several entries related to joining an instance have already been committed
    # on i1 after the compaction, so i2 first index from snapshot will be higher.
    assert i2.raft_first_index() > i1.raft_first_index()

    # Ensure new instance replicates the property
    assert i2.call("box.space._pico_property:get", "animal") == ["animal", "horse"]


def test_revoke_default_privileges_then_bootstrap_from_raft_snapshot(cluster: Cluster):
    user_name = "leeroy"
    impossible_error_on_select = "_pico_privilege cannot be empty"
    select_user_privileges = f"""
    SELECT
        p.*
    FROM
        _pico_privilege p
    JOIN
        _pico_user gr ON p.grantee_id = gr.id
    WHERE
        gr.name = '{user_name}';
    """

    i1, i2 = cluster.deploy(instance_count=2)
    i1.promote_or_fail()
    i1.assert_raft_status("Leader")

    i1.sql(f"CREATE USER {user_name} WITH PASSWORD 'J333333nkins'")
    i1.sql(f"REVOKE ALTER ON USER {user_name} FROM {user_name}")

    before = i1.sql(select_user_privileges)
    assert before, impossible_error_on_select

    i1.raft_compact_log()
    i2.raft_compact_log()

    i3 = cluster.add_instance(wait_online=True)

    after = i3.sql(select_user_privileges)
    assert after, impossible_error_on_select

    error = "reset to default privileges occurred which should not have happened"
    assert before == after, error


def test_catchup_by_snapshot(cluster: Cluster):
    i1, i2, i3 = cluster.deploy(instance_count=3)
    i1.assert_raft_status("Leader")
    ret, _ = i1.cas("insert", "_pico_property", ["animal", "tiger"])

    i3.raft_wait_index(ret, _3_SEC)
    assert i3.call("box.space._pico_property:get", "animal") == ["animal", "tiger"]
    assert i3.raft_first_index() == 1
    i3.terminate()

    i1.cas("delete", "_pico_property", key=["animal"])
    ret, _ = i1.cas("insert", "_pico_property", ["tree", "birch"])

    for i in [i1, i2]:
        i.raft_wait_index(ret, _3_SEC)
        assert i.raft_compact_log() == ret + 1

    # Ensure i3 is able to sync raft using a snapshot
    i3.start()
    i3.wait_online()

    assert i3.call("box.space._pico_property:get", "animal") is None
    assert i3.call("box.space._pico_property:get", "tree") == ["tree", "birch"]

    # Sending a snapshot and handling new DML operation
    # (target_state=Online for i3) are not synchronized.
    # As a result, the operation may sometimes be included
    # in the snapshot and sometimes not. Let's check both cases.
    assert i3.raft_first_index() in (ret + 1, ret + 2)

    # There used to be a problem: catching snapshot used to cause
    # inconsistent raft state so the second restart used to panic.
    i3.terminate()
    i3.start()
    i3.wait_online()


def test_repeated_snapshot_after_repeated_compaction(cluster: Cluster):
    [i1] = cluster.deploy(instance_count=1)

    index, _ = i1.cas("insert", "_pico_property", ["googoo", "gaga"])
    i1.raft_wait_index(index)

    # Compact raft log to trigger creation of snapshot
    i1.raft_compact_log()
    i2 = cluster.add_instance(wait_online=True)

    index, _ = cluster.cas("insert", "_pico_property", ["booboo", "baba"])
    i1.raft_wait_index(index)
    i2.raft_wait_index(index)

    # Compact raft log again
    i1.raft_compact_log()
    i2.raft_compact_log()

    # A new snapshot is generated after the latest compaction
    i3 = cluster.add_instance(wait_online=True)

    # Compact raft log yet again
    i1.raft_compact_log()
    i2.raft_compact_log()
    i3.raft_compact_log()

    # Everything is still ok
    _ = cluster.add_instance(wait_online=True)


def test_snapshot_after_conf_change(cluster: Cluster):
    [i1, i2, i3] = cluster.deploy(instance_count=3)

    i3.terminate()

    index, _ = i1.cas("insert", "_pico_property", ["yoyo", "yaya"])
    i1.raft_wait_index(index)
    i2.raft_wait_index(index)

    # Compact raft log to trigger creation of snapshot
    i1.raft_compact_log()
    i2.raft_compact_log()

    i3.start()
    i3.wait_online()

    # The generated snapshot contains the uptodate conf state, otherwise
    # instance wouldn't've become online.
    _ = cluster.add_instance(wait_online=True)


def test_crash_before_applying_raft_snapshot(cluster: Cluster):
    [i1, i2, i3] = cluster.deploy(instance_count=3)

    i3.terminate()

    index, _ = i1.cas("insert", "_pico_property", ["yoyo", "yaya"])
    i1.raft_wait_index(index)
    i2.raft_wait_index(index)

    # Compact raft log to trigger creation of snapshot
    i1.raft_compact_log()
    i2.raft_compact_log()

    injected_error = "EXIT_BEFORE_APPLYING_RAFT_SNAPSHOT"
    lc = log_crawler(i3, injected_error)
    i3.env[f"PICODATA_ERROR_INJECTION_{injected_error}"] = "1"
    # Instance crashes after receiving a raft snapshot before applying it.
    i3.fail_to_start()
    lc.wait_matched()

    # After restart the Instance receives the snapshot again and applies it successfully.
    del i3.env[f"PICODATA_ERROR_INJECTION_{injected_error}"]
    i3.start()
    i3.wait_online()

    # Another instance also successfully joins (just checking)
    _ = cluster.add_instance(wait_online=True)


def test_crash_learner_before_applying_raft_snapshot(cluster: Cluster):
    # same as test_crash_before_applying_raft_snapshot, but now the crashing instance is a learner
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        voter:
            can_vote: true
            replication_factor: 1
        storage:
            can_vote: false
            replication_factor: 2
"""
    )
    i1 = cluster.add_instance(tier="voter", wait_online=False)
    i2 = cluster.add_instance(tier="voter", wait_online=False)
    i3 = cluster.add_instance(tier="storage", wait_online=False)
    cluster.wait_online()

    i3.terminate()

    index, _ = i1.cas("insert", "_pico_property", ["yoyo", "yaya"])
    i1.raft_wait_index(index)
    i2.raft_wait_index(index)

    # Compact raft log to trigger creation of snapshot
    i1.raft_compact_log()
    i2.raft_compact_log()

    injected_error = "EXIT_BEFORE_APPLYING_RAFT_SNAPSHOT"
    lc = log_crawler(i3, injected_error)
    i3.env[f"PICODATA_ERROR_INJECTION_{injected_error}"] = "1"
    # Instance crashes after receiving a raft snapshot before applying it.
    i3.fail_to_start()
    lc.wait_matched()

    # After restart the Instance receives the snapshot again and applies it successfully.
    del i3.env[f"PICODATA_ERROR_INJECTION_{injected_error}"]
    i3.start()
    i3.wait_online()

    # Another instance also successfully joins (just checking)
    _ = cluster.add_instance(tier="storage", wait_online=True)


def test_restart_learner(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        voter:
            can_vote: true
            replication_factor: 1
        storage:
            can_vote: false
            replication_factor: 2
"""
    )
    _ = cluster.add_instance(tier="voter", wait_online=False)
    _ = cluster.add_instance(tier="voter", wait_online=False)
    i3 = cluster.add_instance(tier="storage", wait_online=False)
    cluster.wait_online()

    time.sleep(11)

    i3.terminate()
    i3.start()
    i3.wait_online()


def test_snapshot_with_stale_schema_version(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        default:
        storage:
            can_vote: false
            replication_factor: 2
"""
    )
    leader = cluster.add_instance(tier="default", wait_online=False)
    storage_1_1 = cluster.add_instance(tier="storage", wait_online=False)
    storage_1_2 = cluster.add_instance(tier="storage", wait_online=False)
    cluster.wait_online()

    # Make sure storage_1_1 is master
    [[master_name]] = leader.sql("SELECT current_master_name FROM _pico_replicaset WHERE tier = 'storage'")
    if storage_1_1.name != master_name:
        storage_1_1, storage_1_2 = storage_1_2, storage_1_1

    # Increase the threshold so that log compaction isn't triggered before we need it
    leader.sql("ALTER SYSTEM SET raft_wal_count_max = 1024")

    # Make it so `storage_1_2` stops handling raft messages (log and snapshot updates).
    injected_error_1 = "IGNORE_ALL_RAFT_MESSAGES"
    storage_1_2.call("pico._inject_error", injected_error_1, True)

    injected_error_2 = "BLOCK_BEFORE_APPLYING_RAFT_SNAPSHOT"
    lc = log_crawler(storage_1_2, injected_error_2)
    storage_1_2.call("pico._inject_error", injected_error_2, True)

    # This is a pretty tricky situation to reproduce as you may notice
    injected_error_3 = "IGNORE_NEWER_SNAPSHOT"
    storage_1_2.call("pico._inject_error", injected_error_3, True)

    # Trigger log compaction now
    # WAIT APPLIED LOCALLY is needed because `storage_1_2`
    # is blocked by the injection
    leader.sql("ALTER SYSTEM SET raft_wal_count_max = 1 WAIT APPLIED LOCALLY")

    # Fix raft message handling, now it will receive a raft snapshot instead of any raft log entries
    storage_1_2.call("pico._inject_error", injected_error_1, False)

    # Make sure the snapshot is received
    # the increased timeout is needed because it takes a while
    #  for the leader to notice that the node comes online after disabling IGNORE_ALL_RAFT_MESSAGES
    #  due to our learner heartbeat throttling
    lc.wait_matched(timeout=20)

    # Make a schema change. WAIT APPLIED LOCALLY is needed because `storage_1_2`
    # is blocked by the injection
    leader.sql("CREATE TABLE life_is_good (i_like_life INTEGER PRIMARY KEY) DISTRIBUTED GLOBALLY WAIT APPLIED LOCALLY")

    # Wait until storage_1_2 synchronizes via tarantool replication stream.
    # This will make it so `_schema.local_schema_version` > `snapshot.schema_version` on it.
    master_vclock = storage_1_1.call(".proc_get_vclock")
    storage_1_2.call(".proc_wait_vclock", master_vclock, 10)

    # Disable the error and make sure the instance successfully catches up eventually
    storage_1_2.call("pico._inject_error", injected_error_2, False)

    leader.sql("INSERT INTO life_is_good VALUES (1), (2), (3)")

    applied = leader.call(".proc_get_index")
    storage_1_2.call(".proc_wait_index", applied, 10)

    rows = storage_1_2.sql("SELECT * FROM life_is_good")
    assert rows == [[1], [2], [3]]


# TODO: test case which would show if we sent snapshot conf state inconsistent
# with snapshot data. The situation should involve a conf change after the
# snapshot data was generated which would be incompatible with conf state at the
# moment the snapshot was requested.
