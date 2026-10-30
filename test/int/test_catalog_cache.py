import pytest

from conftest import Cluster, Instance, ProcessDead, Retriable, TarantoolError, log_crawler

NOT_OPERABLE = "cannot be modified now as DDL operation is in progress"


def is_debug_build(instance: Instance) -> bool:
    """`pico._catalog_cache_check` and the cache check after raft entries
    exist only in debug builds."""
    return instance.eval("return pico._catalog_cache_check ~= nil")


def skip_unless_debug_build(instance: Instance):
    if not is_debug_build(instance):
        pytest.skip("needs a debug build")


def skip_unless_debug_tarantool(instance: Instance):
    if not instance.eval("return require('tarantool').build.target").endswith("-Debug"):
        pytest.skip("WAL injections need a build with the debug_tarantool feature")


def table_id(instance: Instance, name: str) -> int:
    return instance.eval("return box.space._pico_table.index._pico_table_name:get(...).id", name)


def test_debug_check_finds_write_bypassing_picodata(instance: Instance):
    # A write from the console bypasses the cache. DDL over another table
    # doesn't touch the row, and the check after applying its entry finds it.
    skip_unless_debug_build(instance)
    instance.sql("CREATE TABLE a (id INT PRIMARY KEY) DISTRIBUTED GLOBALLY")
    id = table_id(instance, "a")
    lc = log_crawler(instance, f"catalog cache differs from _pico_table at key {id}")
    instance.eval("box.space._pico_table:update({...}, {{'=', 'description', 'bypassed'}})", id)
    with pytest.raises((ProcessDead, TarantoolError)):
        instance.sql("CREATE TABLE b (id INT PRIMARY KEY) DISTRIBUTED GLOBALLY")
    instance.wait_process_stopped()
    assert lc.matched


def test_raft_propose_dml_into_catalog(instance: Instance):
    # `pico.raft_propose` bypasses CaS, so DML into `_pico_table` gets into the
    # raft log. Applying it updates the cache like any other write.
    skip_unless_debug_build(instance)
    instance.sql("CREATE TABLE g (id INT PRIMARY KEY) DISTRIBUTED GLOBALLY")
    id = table_id(instance, "g")
    index = instance.eval(
        """
        local msgpack = require('msgpack')
        return pico.raft_propose({
            kind = 'dml',
            op_kind = 'update',
            table = box.space._pico_table.id,
            key = msgpack.encode({...}),
            ops = {msgpack.encode({'=', 'description', 'proposed'})},
            initiator = 1,
        })
        """,
        id,
    )
    instance.raft_wait_index(index)
    assert instance.eval("return box.space._pico_table:get(...).description", id) == "proposed"
    instance.eval("pico._catalog_cache_check()")


def test_sql_truncate_of_system_table_is_denied(instance: Instance):
    for table in ["_pico_table", "_pico_index"]:
        with pytest.raises(TarantoolError, match=f"TRUNCATE on table '{table}' is denied for all users"):
            instance.sql(f"TRUNCATE {table}")
    assert instance.eval("return box.space._pico_index:len()") > 0
    check_cache(instance)


def check_cache(instance: Instance):
    if is_debug_build(instance):
        instance.eval("pico._catalog_cache_check()")


def test_table_found_after_restart(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.sql("CREATE TABLE t (id INT PRIMARY KEY) DISTRIBUTED BY (id)")
    i1.sql("ALTER TABLE t ADD COLUMN v INT")
    i1.sql("INSERT INTO t VALUES " + ", ".join(f"({x}, {x})" for x in range(8)))

    i1.restart()
    i1.wait_online()
    # The router on i1 sends the table version from its cache, the storage on
    # i2 checks it against its own: a mismatch would fail the query.
    assert len(i1.sql("SELECT * FROM t")) == 8


def test_new_replica_finds_tables(cluster: Cluster):
    i1, _ = cluster.deploy(instance_count=2, init_replication_factor=2)
    i1.sql("CREATE TABLE t (id INT PRIMARY KEY, v INT) DISTRIBUTED BY (id)")
    i1.sql("CREATE INDEX t_v ON t (v)")
    i1.sql("INSERT INTO t VALUES (1, 1)")

    replica = cluster.add_instance(replicaset_name=i1.instance_info()["replicaset_name"], wait_online=True)
    assert replica.eval("return box.info.ro")
    assert replica.sql("SELECT * FROM t") == [[1, 1]]
    check_cache(replica)


def test_cache_after_catchup_by_snapshot(cluster: Cluster):
    # i3 is a replica, so DDL doesn't wait for it while it's offline.
    i1, i2, i3 = cluster.deploy(instance_count=3, init_replication_factor=3)
    i1.sql("CREATE TABLE a (id INT PRIMARY KEY) DISTRIBUTED GLOBALLY")
    i3.raft_wait_index(i1.call(".proc_get_index"))
    i3.terminate()

    i1.sql("DROP TABLE a")
    i1.sql("CREATE TABLE b (id INT PRIMARY KEY, v INT) DISTRIBUTED GLOBALLY")
    i1.sql("CREATE INDEX b_v ON b (v)")
    index = i1.call(".proc_get_index")
    for i in [i1, i2]:
        i.raft_wait_index(index)
        i.raft_compact_log()

    i3.start()
    i3.wait_online()
    assert i3.raft_first_index() > index
    with pytest.raises(TarantoolError, match='table with name "a" not found'):
        i3.sql("SELECT * FROM a")
    assert i3.sql("SELECT * FROM b") == []
    check_cache(i3)


def test_plan_not_reused_after_alter(instance: Instance):
    instance.sql("CREATE TABLE t (id INT PRIMARY KEY) DISTRIBUTED BY (id)")
    instance.sql("INSERT INTO t VALUES (1)")
    assert instance.sql("SELECT * FROM t") == [[1]]

    instance.sql("ALTER TABLE t ADD COLUMN v INT")
    assert instance.sql("SELECT * FROM t") == [[1, None]]


def test_ddl_then_dml(instance: Instance):
    # DDL returns once its entry is applied, and the cache changes together
    # with the tables while applying it.
    instance.eval(
        """
        for i = 1, 100 do
            local t = 't' .. i
            assert(pico.sql('CREATE TABLE ' .. t .. ' (id INT PRIMARY KEY) DISTRIBUTED GLOBALLY'))
            local _, err = pico.sql('INSERT INTO ' .. t .. ' VALUES (1)')
            assert(err == nil, tostring(err))
        end
        """,
        timeout=120,
    )


def test_wait_index_then_sql_on_other_instance(cluster: Cluster):
    i1, i2 = cluster.deploy(instance_count=2)
    i1.sql("CREATE TABLE g (id INT PRIMARY KEY) DISTRIBUTED GLOBALLY")
    i2.raft_wait_index(i1.raft_get_index())
    # The table is visible since DdlPrepare, but becomes operable only with
    # DdlCommit, and DML checks that in the cache.
    i2.sql("INSERT INTO g VALUES (1)")
    assert i2.sql("SELECT * FROM g") == [[1]]


def test_console_write_then_restart(instance: Instance):
    # A write bypassing picodata isn't guaranteed to reach the cache before
    # restart. Nothing but DML into other tables gets into the raft log until
    # restart, otherwise the debug check would crash the instance.
    instance.sql("CREATE TABLE g (id INT PRIMARY KEY) DISTRIBUTED GLOBALLY")
    instance.eval("box.space._pico_table:update({...}, {{'=', 'operable', false}})", table_id(instance, "g"))

    instance.restart()
    instance.wait_online()
    with pytest.raises(TarantoolError, match=NOT_OPERABLE):
        instance.sql("INSERT INTO g VALUES (1)")
    check_cache(instance)


def test_unparsable_pico_index_row_crashes_on_start(instance: Instance):
    # Nothing but DML into other tables gets into the raft log until restart,
    # otherwise the debug check would crash the instance with another message.
    instance.eval("box.space._pico_index:insert({20000, 0, 'bad_index', 'bogus', {}, {}, true, 0})")
    instance.terminate()
    lc = log_crawler(instance, "failed to decode _pico_index row .*bad_index", use_regex=True)
    instance.fail_to_start()
    assert lc.matched


def test_change_visible_before_wal(instance: Instance):
    skip_unless_debug_tarantool(instance)

    instance.sql("CREATE TABLE t (id INT PRIMARY KEY) DISTRIBUTED BY (id)")
    applied = instance.raft_get_index()
    instance.call("pico._inject_error", "DELAY_WAL_AFTER_APPLYING_DDL_PREPARE", True)
    instance.eval("require('fiber').new(pico.sql, 'DROP TABLE t')")

    # DdlPrepare is applied, its transaction waits for the WAL.
    def prepare_applied():
        assert instance.raft_get_index() > applied

    Retriable().call(prepare_applied)
    try:
        with pytest.raises(TarantoolError, match=NOT_OPERABLE):
            instance.sql("INSERT INTO t VALUES (1)")
    finally:
        instance.eval(
            """
            pico._inject_error('DELAY_WAL_AFTER_APPLYING_DDL_PREPARE', false)
            box.error.injection.set('ERRINJ_WAL_DELAY', false)
            """
        )


def test_snapshot_visible_before_wal(cluster: Cluster):
    # i3 is a replica, so DDL doesn't wait for it while it's offline.
    i1, i2, i3 = cluster.deploy(instance_count=3, init_replication_factor=3)
    skip_unless_debug_tarantool(i3)
    i1.sql("CREATE TABLE a (id INT PRIMARY KEY) DISTRIBUTED GLOBALLY")
    i3.raft_wait_index(i1.call(".proc_get_index"))
    i3.terminate()

    i1.sql("DROP TABLE a")
    i1.sql("CREATE TABLE b (id INT PRIMARY KEY) DISTRIBUTED GLOBALLY")
    i4 = cluster.add_instance(wait_online=True)
    index = i1.call(".proc_get_index")
    for i in [i1, i2]:
        i.raft_wait_index(index)
        i.raft_compact_log()

    i3.env["PICODATA_ERROR_INJECTION_DELAY_WAL_AFTER_APPLYING_SNAPSHOT"] = "1"
    i3.start()

    # The snapshot is applied, its transaction waits for the WAL.
    def snapshot_applied():
        assert i3.eval("return box.space._pico_table.index._pico_table_name:get('b') ~= nil")

    Retriable().call(snapshot_applied)
    try:
        # Both the table definitions and the topology changed together with
        # the tables.
        assert i3.sql("SELECT * FROM b") == []
        with pytest.raises(TarantoolError, match='table with name "a" not found'):
            i3.sql("SELECT * FROM a")
        # `.proc_instance_name` reads the topology from the cache.
        assert i3.call(".proc_instance_name", i4.uuid()) == i4.name
    finally:
        i3.eval(
            """
            pico._inject_error('DELAY_WAL_AFTER_APPLYING_SNAPSHOT', false)
            box.error.injection.set('ERRINJ_WAL_DELAY', false)
            """
        )

    i3.wait_online()
    assert i3.raft_get_index() >= index
    assert i3.raft_first_index() > index
    i3.eval("pico._catalog_cache_check()")
