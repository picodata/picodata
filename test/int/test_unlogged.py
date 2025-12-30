from conftest import (
    Cluster,
)


def test_unlogged_table_basic(cluster: Cluster):
    """
    Test basic interaction with an unlogged table:
    - Create an unlogged table
    - Fill it with data
    - Simulate a failover
    - Check that no data was replicated
    """
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        default:
            can_vote: true
        storage:
            can_vote: false
            replication_factor: 2
"""
    )

    leader = cluster.add_instance(tier="default")
    storage_1_1 = cluster.add_instance(tier="storage")
    storage_1_2 = cluster.add_instance(tier="storage")
    cluster.wait_online()

    # Make it so a crashed instance is detected faster, this is used later
    leader.sql("ALTER SYSTEM SET governor_auto_offline_timeout = 3")

    # Make sure `storage_1_1` is the master
    [[master_name]] = leader.sql("SELECT current_master_name FROM _pico_replicaset WHERE tier = 'storage'")
    if storage_1_1.name != master_name:
        storage_1_1, storage_1_2 = storage_1_2, storage_1_1

    # Create an unlogged table
    storage_1_1.sql("CREATE UNLOGGED TABLE t (a INT PRIMARY KEY) DISTRIBUTED BY (a) IN TIER storage")
    [[opts]] = storage_1_1.sql("SELECT opts FROM _pico_table WHERE name = 't'")
    # Check it has appropriate opts in _pico_table
    assert dict([("unlogged", [True])]) in opts  # type: ignore
    # Fill an unlogged table
    storage_1_1.sql("INSERT INTO t VALUES (1), (2), (1337), (12345), (99), (42)")

    [[prev_count]] = storage_1_1.sql("SELECT COUNT(*) FROM t")
    assert prev_count != 0

    # Kill the master
    storage_1_1.terminate()
    cluster.wait_has_states(storage_1_1, "Offline", "Offline")

    [[master_name]] = leader.sql("SELECT current_master_name FROM _pico_replicaset WHERE tier = 'storage'")
    assert storage_1_2.name == master_name

    # The unlogged space is not replicated, so previous replica should not have any data
    [[new_count]] = storage_1_2.sql("SELECT COUNT(*) FROM t")
    assert new_count == 0


def test_unlogged_table_truncate_on_replica(cluster: Cluster):
    """
    Test that all unlogged tables on a replica get truncated after the failover:
    - Create unlogged tables, fill them with data
    - Switch the master, without stopping the current master
    - Check that no data was replicated
    - Check that the bsizes of unlogged tables are equal to 0
    - Switch the master again
    - Check that the tables are indeed empty
    """
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        default:
            can_vote: true
        storage:
            can_vote: false
            replication_factor: 2
"""
    )

    leader = cluster.add_instance(tier="default")
    storage_1_1 = cluster.add_instance(tier="storage")
    storage_1_2 = cluster.add_instance(tier="storage")
    cluster.wait_online()

    # Make sure `storage_1_1` is the master
    [[master_name]] = leader.sql("SELECT current_master_name FROM _pico_replicaset WHERE tier = 'storage'")
    if storage_1_1.name != master_name:
        storage_1_1, storage_1_2 = storage_1_2, storage_1_1

    # Insert lots of tuples into lots of unlogged tables
    table_count = 15
    batch_count = 50
    batch_size = 1000
    table_name_prefix = "unlogged_table_"
    for t in range(table_count):
        table_name = f"{table_name_prefix}{t}"
        storage_1_1.sql(f"CREATE UNLOGGED TABLE {table_name} (a INT PRIMARY KEY) DISTRIBUTED BY (a) IN TIER storage")
        for i in range(batch_count):
            bulky_insert_sql = f"INSERT INTO {table_name} VALUES "
            for j in range(1, batch_size):
                bulky_insert_sql += f"({i * batch_size * 100 + j}), "
            bulky_insert_sql += f"({i * batch_size * 100 + batch_size})"
            storage_1_1.sql(bulky_insert_sql)

    counter = leader.governor_step_counter()

    # Switch the master, now it should be storage_1_2
    leader.sql(
        """
        UPDATE _pico_replicaset
            SET target_master_name = (
                SELECT i.name FROM _pico_instance AS i
                    JOIN _pico_replicaset AS r ON i.replicaset_name = r.name
                    WHERE i.name != r.current_master_name
                      AND r.name = 'storage_1'
                    LIMIT 1
            )
            WHERE name = 'storage_1'
        """,
    )

    leader.wait_governor_status("idle", old_step_counter=counter)

    [[master_name]] = leader.sql("SELECT current_master_name FROM _pico_replicaset WHERE tier = 'storage'")
    assert storage_1_2.name == master_name

    # Check that tables are not replicated
    for t in range(table_count):
        table_name = f"{table_name_prefix}{t}"
        [[new_count]] = storage_1_2.sql(f"SELECT COUNT(*) FROM {table_name}")
        assert new_count == 0

    # Check that spaces are truncated
    for t in range(table_count):
        table_name = f"{table_name_prefix}{t}"
        size = storage_1_1.eval(f"return box.space.{table_name}:bsize()")
        assert size == 0

    counter = leader.governor_step_counter()

    # Switch the master again, now it should be storage_1_1
    leader.sql(
        """
        UPDATE _pico_replicaset
            SET target_master_name = (
                SELECT i.name FROM _pico_instance AS i
                    JOIN _pico_replicaset AS r ON i.replicaset_name = r.name
                    WHERE i.name != r.current_master_name
                      AND r.name = 'storage_1'
                    LIMIT 1
            )
            WHERE name = 'storage_1'
        """,
    )

    leader.wait_governor_status("idle", old_step_counter=counter)

    [[master_name]] = leader.sql("SELECT current_master_name FROM _pico_replicaset WHERE tier = 'storage'")
    assert storage_1_1.name == master_name

    # Check that spaces are truncated
    for t in range(table_count):
        table_name = f"{table_name_prefix}{t}"
        [[new_count]] = storage_1_1.sql(f"SELECT COUNT(*) FROM {table_name}")
        assert new_count == 0
