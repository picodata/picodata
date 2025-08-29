import pytest
from conftest import Cluster, TarantoolError


def test_set_via_alter_system(cluster: Cluster):
    instance = cluster.add_instance()

    # default values
    box_config = instance.eval("return box.cfg")
    assert box_config["net_msg_max"] == 768
    assert box_config["checkpoint_interval"] == 3600
    assert box_config["checkpoint_count"] == 2
    assert box_config["sql_cache_size"] == 5242880

    # picodata parameters names are slightly different from the tarantools
    instance.sql("ALTER SYSTEM SET iproto_net_msg_max TO 100 FOR ALL TIERS")
    instance.sql("ALTER SYSTEM SET memtx_checkpoint_interval TO 100 FOR ALL TIERS")
    instance.sql("ALTER SYSTEM SET memtx_checkpoint_count TO 100 FOR ALL TIERS")
    instance.sql("ALTER SYSTEM SET sql_storage_cache_size_max TO 100 FOR ALL TIERS")

    # parameters values changed
    box_config = instance.eval("return box.cfg")
    assert box_config["net_msg_max"] == 100
    assert box_config["checkpoint_interval"] == 100
    assert box_config["checkpoint_count"] == 100
    assert box_config["sql_cache_size"] == 100

    # box settings isn't persistent, so it should be reapplied
    instance.restart()
    instance.wait_online()

    # parameters values are correct even after restart
    box_config = instance.eval("return box.cfg")
    assert box_config["net_msg_max"] == 100
    assert box_config["checkpoint_interval"] == 100
    assert box_config["checkpoint_count"] == 100
    assert box_config["sql_cache_size"] == 100

    # bad values for parameters shouldn't pass validation
    # stage before creating DML from ir node
    with pytest.raises(
        TarantoolError,
        match="""invalid value for 'iproto_net_msg_max': expected unsigned, got integer""",
    ):
        instance.sql("ALTER SYSTEM SET iproto_net_msg_max = -1")

    with pytest.raises(
        TarantoolError,
        match="""invalid value for 'memtx_checkpoint_interval': expected unsigned, got decimal""",
    ):
        instance.sql("ALTER SYSTEM SET memtx_checkpoint_interval = -1.0")

    with pytest.raises(
        TarantoolError,
        match="""invalid value for 'memtx_checkpoint_count': value must be between 1 and 2147483647""",
    ):
        instance.sql("ALTER SYSTEM SET memtx_checkpoint_count = -1")

    # can't specify non existent parameter
    with pytest.raises(
        TarantoolError,
        match="""unknown parameter: \'non_existing_name\'""",
    ):
        instance.sql("ALTER SYSTEM SET non_existing_name = -1")

    # can't specify non existent parameter
    with pytest.raises(
        TarantoolError,
        match="""unknown parameter: \'non_existing_name\'""",
    ):
        instance.sql("ALTER SYSTEM SET non_existing_name = -1 FOR TIER non_existent")

    # can't specify non existent parameter
    with pytest.raises(
        TarantoolError,
        match="""unknown parameter: \'non_existing_name\'""",
    ):
        instance.sql("ALTER SYSTEM SET non_existing_name = -1 FOR ALL TIERS")

    # can't specify tier for global parameter
    with pytest.raises(
        TarantoolError,
        match="""parameter with global scope can\'t be configured for tier \'default\'""",
    ):
        instance.sql("ALTER SYSTEM SET raft_wal_count_max = 10 FOR TIER default")

    # can't specify tier for global parameter
    with pytest.raises(
        TarantoolError,
        match="""parameter with global scope can\'t be configured for tier \'default\'""",
    ):
        instance.sql("ALTER SYSTEM SET pg_statement_max = 10 FOR TIER default")

    # but it's ok to specify `for all tiers` for parameter with global scope
    instance.sql("ALTER SYSTEM SET pg_statement_max = 2000 FOR ALL TIERS")

    # but it's ok to specify `for all tiers` for parameter with tier scope
    instance.sql("ALTER SYSTEM SET memtx_checkpoint_count = 200 FOR ALL TIERS")

    # but it's ok not to specify `for all tiers` for parameter with tier scope, this
    # behaviour will be used by default
    instance.sql("ALTER SYSTEM SET memtx_checkpoint_count = 200")

    # can't specify non existent tier
    with pytest.raises(
        TarantoolError,
        match="""specified tier \'non_existent\' doesn\'t exist""",
    ):
        instance.sql("ALTER SYSTEM SET memtx_checkpoint_count = 200 FOR TIER non_existent")

    # reset part

    # can't specify tier for global parameter in reset too
    with pytest.raises(
        TarantoolError,
        match="""parameter with global scope can\'t be configured for tier \'default\'""",
    ):
        instance.sql("ALTER SYSTEM RESET raft_wal_count_max FOR TIER default")

    # but it's ok to use both `FOR ALL TIERS` and nothing
    instance.sql("ALTER SYSTEM RESET raft_wal_count_max FOR ALL TIERS")
    instance.sql("ALTER SYSTEM RESET raft_wal_count_max")

    # any combination valid
    instance.sql("ALTER SYSTEM RESET memtx_checkpoint_interval")
    instance.sql("ALTER SYSTEM RESET memtx_checkpoint_interval FOR ALL TIERS")
    instance.sql("ALTER SYSTEM RESET memtx_checkpoint_interval FOR TIER default")


def test_snapshot_and_dynamic_parameters(cluster: Cluster):
    i1, i2, _ = cluster.deploy(instance_count=3)

    i2.kill()

    i1.sql("ALTER SYSTEM SET iproto_net_msg_max = 100 FOR ALL TIERS")

    # Trigger raft log compaction
    i1.sql("ALTER SYSTEM SET raft_wal_count_max TO 1")

    # Add a new instance and restart `i2`, which catches up by raft snapshot
    i2.start_and_wait()
    i4 = cluster.add_instance()

    for catched_up_by_snapshot in [i2, i4]:
        box_config = catched_up_by_snapshot.eval("return box.cfg")
        assert box_config["net_msg_max"] == 100


def test_set_parameters_with_tier_scope(cluster: Cluster):
    cluster.set_config_file(
        yaml="""
cluster:
    name: test
    tier:
        red:
        blue:
"""
    )

    red_instance = cluster.add_instance(tier="red")
    blue_instance = cluster.add_instance(tier="blue")

    blue_config = blue_instance.eval("return box.cfg")
    assert blue_config["checkpoint_interval"] == 3600
    red_config = red_instance.eval("return box.cfg")
    assert red_config["checkpoint_interval"] == 3600

    red_instance.sql("ALTER SYSTEM SET memtx_checkpoint_interval TO 100 FOR TIER blue")

    blue_config = blue_instance.eval("return box.cfg")
    assert blue_config["checkpoint_interval"] == 100
    red_config = red_instance.eval("return box.cfg")
    assert red_config["checkpoint_interval"] == 3600

    blue_instance.sql("ALTER SYSTEM SET memtx_checkpoint_interval TO 10 FOR TIER red")

    blue_config = blue_instance.eval("return box.cfg")
    assert blue_config["checkpoint_interval"] == 100
    red_config = red_instance.eval("return box.cfg")
    assert red_config["checkpoint_interval"] == 10

    # check that reset works
    red_instance.sql("ALTER SYSTEM RESET memtx_checkpoint_interval FOR TIER blue")
    blue_config = blue_instance.eval("return box.cfg")
    assert blue_config["checkpoint_interval"] == 3600

    # check that reset for all tiers works
    red_instance.sql("ALTER SYSTEM RESET memtx_checkpoint_interval FOR ALL TIERS")
    blue_config = blue_instance.eval("return box.cfg")
    assert blue_config["checkpoint_interval"] == 3600
    red_config = red_instance.eval("return box.cfg")
    assert red_config["checkpoint_interval"] == 3600

    # check that set for all tiers works without 'for all tiers' clause works
    red_instance.sql("ALTER SYSTEM SET memtx_checkpoint_interval TO 3500")
    blue_config = blue_instance.eval("return box.cfg")
    assert blue_config["checkpoint_interval"] == 3500
    red_config = red_instance.eval("return box.cfg")
    assert red_config["checkpoint_interval"] == 3500

    # check that reset all works
    red_instance.sql("ALTER SYSTEM SET memtx_checkpoint_interval TO 10 FOR TIER blue")
    red_instance.sql("ALTER SYSTEM SET memtx_checkpoint_interval TO 20 FOR TIER red")

    red_instance.sql("ALTER SYSTEM RESET ALL")
    blue_config = blue_instance.eval("return box.cfg")
    assert blue_config["checkpoint_interval"] == 3600
    red_config = red_instance.eval("return box.cfg")
    assert red_config["checkpoint_interval"] == 3600


def test_cache_capacity(cluster: Cluster):
    i1 = cluster.add_instance()

    i1.sql("ALTER SYSTEM SET sql_storage_cache_count_max = 1")

    cache_info = i1.eval("return box.info.sql()")
    assert cache_info["cache"]["stmt_count"] == 0

    # random sql that inserts to tarantool cache
    i1.sql("SELECT * FROM _pico_instance")

    cache_info = i1.eval("return box.info.sql()")
    assert cache_info["cache"]["stmt_count"] == 1

    i1.sql("SELECT * FROM _pico_replicaset")

    cache_info = i1.eval("return box.info.sql()")
    assert cache_info["cache"]["stmt_count"] == 1

    i1.sql("ALTER SYSTEM SET sql_storage_cache_count_max = 2")

    i1.sql("SELECT * FROM _pico_replicaset")

    # select from replicaset already cached
    cache_info = i1.eval("return box.info.sql()")
    assert cache_info["cache"]["stmt_count"] == 1

    i1.sql("ALTER SYSTEM SET sql_storage_cache_count_max = 3")

    i1.sql("SELECT * FROM _pico_instance")

    cache_info = i1.eval("return box.info.sql()")
    assert cache_info["cache"]["stmt_count"] == 2

    i1.sql("SELECT * FROM _pico_tier")

    cache_info = i1.eval("return box.info.sql()")
    assert cache_info["cache"]["stmt_count"] == 3

    # cache can shrink
    i1.sql("ALTER SYSTEM SET sql_storage_cache_count_max = 1")

    cache_info = i1.eval("return box.info.sql()")
    assert cache_info["cache"]["size"] == 2393

    i1.sql("SELECT * FROM _pico_tier")

    # if size doesn't changed, then query was in cache, and it's true,
    # because of LRU
    cache_info = i1.eval("return box.info.sql()")
    assert cache_info["cache"]["size"] == 2393


def test_alter_system_iproto_net_msg_max(cluster: Cluster):
    instance = cluster.add_instance()
    iproto_net_msg_max = "iproto_net_msg_max"

    # normal value
    instance.sql(f"ALTER SYSTEM SET {iproto_net_msg_max} = 420")

    bad_value = 0
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{iproto_net_msg_max}': value must be between 2 and 2147483647""",
    ):
        instance.sql(f"ALTER SYSTEM SET {iproto_net_msg_max} = {bad_value}")

    bad_value = 1
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{iproto_net_msg_max}': value must be between 2 and 2147483647""",
    ):
        instance.sql(f"ALTER SYSTEM SET {iproto_net_msg_max} = {bad_value}")

    bad_value = 2147483648
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{iproto_net_msg_max}': value must be between 2 and 2147483647""",
    ):
        instance.sql(f"ALTER SYSTEM SET {iproto_net_msg_max} = {bad_value}")

    bad_value = False
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{iproto_net_msg_max}': expected unsigned, got boolean""",
    ):
        instance.sql(f"ALTER SYSTEM SET {iproto_net_msg_max} = {bad_value}")

    str_value = "str"
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{iproto_net_msg_max}': expected unsigned, got string""",
    ):
        instance.sql(f"ALTER SYSTEM SET {iproto_net_msg_max} = '{str_value}'")


def test_alter_system_memtx_checkpoint_count(cluster: Cluster):
    instance = cluster.add_instance()
    memtx_checkpoint_count = "memtx_checkpoint_count"

    # normal value
    instance.sql(f"ALTER SYSTEM SET {memtx_checkpoint_count} = 4")

    bad_value = -1
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{memtx_checkpoint_count}': value must be between 1 and 2147483647""",
    ):
        instance.sql(f"ALTER SYSTEM SET {memtx_checkpoint_count} = {bad_value}")

    bad_value = 0
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{memtx_checkpoint_count}': value must be between 1 and 2147483647""",
    ):
        instance.sql(f"ALTER SYSTEM SET {memtx_checkpoint_count} = {bad_value}")

    bad_value = 2147483648
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{memtx_checkpoint_count}': value must be between 1 and 2147483647""",
    ):
        instance.sql(f"ALTER SYSTEM SET {memtx_checkpoint_count} = {bad_value}")

    bad_value = True
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{memtx_checkpoint_count}': expected int, got boolean""",
    ):
        instance.sql(f"ALTER SYSTEM SET {memtx_checkpoint_count} = {bad_value}")

    str_value = "str"
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{memtx_checkpoint_count}': expected int, got string""",
    ):
        instance.sql(f"ALTER SYSTEM SET {memtx_checkpoint_count} = '{str_value}'")


def test_alter_system_memtx_checkpoint_interval(cluster: Cluster):
    instance = cluster.add_instance()
    memtx_checkpoint_interval = "memtx_checkpoint_interval"

    # normal value
    instance.sql(f"ALTER SYSTEM SET {memtx_checkpoint_interval} = 3600")

    bad_value = -1
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{memtx_checkpoint_interval}': expected unsigned, got int""",
    ):
        instance.sql(f"ALTER SYSTEM SET {memtx_checkpoint_interval} = {bad_value}")

    bad_value = False
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{memtx_checkpoint_interval}': expected unsigned, got boolean""",
    ):
        instance.sql(f"ALTER SYSTEM SET {memtx_checkpoint_interval} = {bad_value}")


def test_alter_system_sql_storage_cache_size_max(cluster: Cluster):
    instance = cluster.add_instance()
    sql_storage_cache_size_max = "sql_storage_cache_size_max"

    # normal vlaue
    instance.sql(f"ALTER SYSTEM SET {sql_storage_cache_size_max} = 15000")

    bad_value = 0
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{sql_storage_cache_size_max}': value must be between 1 and 2147483647""",
    ):
        instance.sql(f"ALTER SYSTEM SET {sql_storage_cache_size_max} = {bad_value}")

    bad_value = 1
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{sql_storage_cache_size_max}': value must be greater than the current cache size 845""",
    ):
        instance.sql("SELECT 1")
        instance.sql(f"ALTER SYSTEM SET {sql_storage_cache_size_max} = {bad_value}")

    bad_value = -1
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{sql_storage_cache_size_max}': expected unsigned, got integer""",
    ):
        instance.sql(f"ALTER SYSTEM SET {sql_storage_cache_size_max} = {bad_value}")

    bad_value = True
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{sql_storage_cache_size_max}': expected unsigned, got boolean""",
    ):
        instance.sql(f"ALTER SYSTEM SET {sql_storage_cache_size_max} = {bad_value}")


def test_alter_system_sql_storage_cache_count_max(cluster: Cluster):
    instance = cluster.add_instance()
    sql_storage_cache_count_max = "sql_storage_cache_count_max"

    instance.sql(f"ALTER SYSTEM SET {sql_storage_cache_count_max} = 15000")

    bad_value = 0
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{sql_storage_cache_count_max}': value must be between 1 and 18446744073709551615""",
    ):
        instance.sql(f"ALTER SYSTEM SET {sql_storage_cache_count_max} = {bad_value}")

    bad_value = -1
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{sql_storage_cache_count_max}': expected unsigned, got integer""",
    ):
        instance.sql(f"ALTER SYSTEM SET {sql_storage_cache_count_max} = {bad_value}")

    bad_value = False
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{sql_storage_cache_count_max}': expected unsigned, got boolean""",
    ):
        instance.sql(f"ALTER SYSTEM SET {sql_storage_cache_count_max} = {bad_value}")


def test_alter_system_motion_row_param(cluster: Cluster):
    instance = cluster.add_instance(init_replication_factor=2)
    i2 = cluster.add_instance()
    sql_motion_row_max = "sql_motion_row_max"

    db_config = instance.sql(f"""
                SELECT key, value FROM _pico_db_config
                WHERE key='{sql_motion_row_max}'
                """)
    assert db_config == [["sql_motion_row_max", 5000]]

    # limit the parameter
    limit = 25
    instance.sql(f"ALTER SYSTEM SET {sql_motion_row_max} TO {limit}")
    assert instance.sql(f"SELECT key, value FROM _pico_db_config WHERE key = '{sql_motion_row_max}'") == [
        ["sql_motion_row_max", limit]
    ]

    i2.sql("CREATE TABLE t1(id INT PRIMARY KEY) DISTRIBUTED BY (id)")
    i2.sql("CREATE TABLE t2(id INT PRIMARY KEY) DISTRIBUTED BY (id)")
    size = limit * 2
    for x in range(size):
        i2.sql(f"INSERT INTO t1 VALUES ({x})")
        i2.sql(f"INSERT INTO t2 VALUES ({x})")

    with pytest.raises(TarantoolError, match="Exceeded maximum number of rows"):
        instance.sql("SELECT * FROM t1 JOIN t2 ON true")

    # set to 0 to disable limit and verify the same query now succeeds
    limit = 0
    instance.sql(f"ALTER SYSTEM SET {sql_motion_row_max} TO {limit}")
    assert instance.sql(f"SELECT key, value FROM _pico_db_config WHERE key = '{sql_motion_row_max}'") == [
        ["sql_motion_row_max", limit]
    ]

    row_count = instance.sql("SELECT COUNT(*) FROM t1 JOIN t2 ON true")
    # 50 * 50 = 2500 rows total
    assert row_count[0][0] == 2500

    # bad values for parameters shouldn't pass validation
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{sql_motion_row_max}': expected unsigned, got integer""",
    ):
        instance.sql(f"ALTER SYSTEM SET {sql_motion_row_max} = -1")

    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{sql_motion_row_max}': expected unsigned, got string""",
    ):
        instance.sql(f"ALTER SYSTEM SET {sql_motion_row_max} = 'x'")


def test_alter_system_vbde_opcode_param(cluster: Cluster):
    instance = cluster.add_instance(init_replication_factor=2)
    sql_vdbe_opcode_max = "sql_vdbe_opcode_max"

    db_config = instance.sql(f"""
                SELECT key, value FROM _pico_db_config
                WHERE key='{sql_vdbe_opcode_max}'
                """)
    assert db_config == [["sql_vdbe_opcode_max", 45000]]

    # limit the parameter
    limit = 100
    instance.sql(f"ALTER SYSTEM SET {sql_vdbe_opcode_max} TO {limit}")
    assert instance.sql(f"SELECT key, value FROM _pico_db_config WHERE key = '{sql_vdbe_opcode_max}'") == [
        ["sql_vdbe_opcode_max", limit]
    ]

    with pytest.raises(TarantoolError, match=f"Reached a limit on max executed vdbe opcodes. Limit: {limit}"):
        instance.sql("SELECT * FROM _pico_db_config")

    # set 0 to disable limits
    limit = 0
    instance.sql(f"ALTER SYSTEM SET {sql_vdbe_opcode_max} TO {limit}")
    row_count = instance.sql("SELECT * FROM _pico_db_config")
    assert row_count != 0

    # bad values for parameters shouldn't pass validation
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{sql_vdbe_opcode_max}': expected unsigned, got integer""",
    ):
        instance.sql(f"ALTER SYSTEM SET {sql_vdbe_opcode_max} = -1")

    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{sql_vdbe_opcode_max}': expected unsigned, got string""",
    ):
        instance.sql(f"ALTER SYSTEM SET {sql_vdbe_opcode_max} = 'x'")


def test_alter_system_pg_params(cluster: Cluster):
    instance = cluster.add_instance()
    pg_portal_max = "pg_portal_max"
    pg_statement_max = "pg_statement_max"

    # default values
    db_config = instance.sql(f"""
                SELECT key, value FROM _pico_db_config
                WHERE key IN ('{pg_portal_max}', '{pg_statement_max}')
                ORDER BY key
                """)
    assert db_config == [["pg_portal_max", 100000], ["pg_statement_max", 100000]]

    # set 0 to disable limits
    instance.sql(f"ALTER SYSTEM SET {pg_portal_max} TO 0")
    instance.sql(f"ALTER SYSTEM SET {pg_statement_max} TO 0")

    # parameters values changed
    db_config = instance.sql(f"""
                SELECT key, value FROM _pico_db_config
                WHERE key IN ('{pg_portal_max}', '{pg_statement_max}')
                ORDER BY key
                """)
    assert db_config == [["pg_portal_max", 0], ["pg_statement_max", 0]]

    # bad values for parameters shouldn't pass validation
    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{pg_portal_max}': expected unsigned, got integer""",
    ):
        instance.sql(f"ALTER SYSTEM SET {pg_portal_max} = -1")

    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{pg_portal_max}': expected unsigned, got string""",
    ):
        instance.sql(f"ALTER SYSTEM SET {pg_portal_max} = 'x'")

    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{pg_statement_max}': expected unsigned, got integer""",
    ):
        instance.sql(f"ALTER SYSTEM SET {pg_statement_max} = -1")

    with pytest.raises(
        TarantoolError,
        match=f"""invalid value for '{pg_statement_max}': expected unsigned, got string""",
    ):
        instance.sql(f"ALTER SYSTEM SET {pg_statement_max} = 'y'")
