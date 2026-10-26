import pytest

from conftest import Cluster, Instance, ProcessDead, Retriable, TarantoolError, log_crawler


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


def test_sql_truncate_of_system_table(instance: Instance):
    instance.sql("TRUNCATE _pico_index")
    assert instance.eval("return box.space._pico_index:len()") == 0
    # In debug builds the check after applying the entry has found the cache
    # empty as well.
    if is_debug_build(instance):
        instance.eval("pico._catalog_cache_check()")
    instance.check_process_alive()


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
        assert i3.eval("return pico.instance_info(...).name", i4.name) == i4.name
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
