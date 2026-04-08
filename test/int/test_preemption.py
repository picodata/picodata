from conftest import Cluster, Retriable, log_crawler
import pytest
import sys
import threading
from typing import Any
from prometheus_client import Metric


def total_samples(families: dict[str, Metric], name: str) -> float:
    found_family = families.get(name)
    if found_family is None:
        return 0.0
    assert len(found_family.samples) == 1, "Metric has {} samples instead of 1".format(len(found_family.samples))
    sample = found_family.samples[0]
    return sample.value


@pytest.mark.flaky(reruns=3)
def test_dql_truncate_concurrent(cluster: Cluster):
    """
    flaky: https://git.picodata.io/core/picodata/-/issues/2770
    """
    cluster.deploy(instance_count=2)
    i1 = cluster.instances[0]
    i2 = cluster.instances[1]

    enable_preemption(i1, True, 0, 1)

    ddl = i1.sql(
        """
        create table t (a int, primary key (a))
        using memtx
        distributed by (a)
        wait applied globally
    """
    )
    assert ddl["row_count"] == 1

    values_to_insert = ""
    for i in range(1000):
        values_to_insert += f"({i}),"
    values_to_insert += "(1000)"

    N = 10

    start_barrier = threading.Barrier(3)
    done_barrier = threading.Barrier(3)

    exc_holder = {"exc": None}
    exc_lock = threading.Lock()

    def fail_now():
        try:
            start_barrier.abort()
        except threading.BrokenBarrierError:
            pass
        try:
            done_barrier.abort()
        except threading.BrokenBarrierError:
            pass

    def record_and_fail():
        with exc_lock:
            if exc_holder["exc"] is None:
                exc_holder["exc"] = sys.exc_info()
        fail_now()

    def run_dql():
        for _ in range(N):
            try:
                start_barrier.wait()
            except Exception:
                record_and_fail()

            try:
                i1.sql("""
                    select *
                    from t t1
                    join t t2 on t1.a = t2.a
                    where t1.a > 1
                    order by t1.a
                    limit 10
                """)
            except Exception as err:
                err_str = str(err)
                if "space cursor is no longer valid" not in err_str:
                    record_and_fail()

            try:
                done_barrier.wait()
            except Exception:
                pass

    def run_truncate():
        try:
            for _ in range(N):
                try:
                    start_barrier.wait()
                except Exception:
                    pass

                i2.sql("truncate t")

                try:
                    done_barrier.wait()
                except Exception:
                    pass

        except Exception:
            record_and_fail()

    t1 = threading.Thread(target=run_dql)
    t2 = threading.Thread(target=run_truncate)
    t1.start()
    t2.start()

    try:
        for _ in range(N):
            if exc_holder["exc"]:
                break

            i2.sql(f"insert into t values {values_to_insert}")

            try:
                start_barrier.wait()
            except Exception:
                pass

            try:
                done_barrier.wait()
            except Exception:
                pass

    except Exception:
        record_and_fail()

    finally:
        fail_now()
        t1.join()
        t2.join()

    if exc_holder["exc"]:
        _, ev, tb = exc_holder["exc"]
        raise ev.with_traceback(tb)


def enable_preemption(instance, preemption, interval_us, opcode_cnt):
    """Enable preemption with aggressive settings to maximize yield frequency."""

    instance.sql(f"alter system set sql_preemption = {preemption}")
    instance.sql(f"alter system set sql_preemption_interval_us = {interval_us}")
    instance.sql(f"alter system set sql_preemption_opcode_max = {opcode_cnt}")
    instance.sql("alter system set sql_motion_row_max = 0")
    instance.sql("alter system set sql_vdbe_opcode_max = 0")

    expected = [
        ["sql_preemption", preemption],
        ["sql_preemption_interval_us", interval_us],
        ["sql_preemption_opcode_max", opcode_cnt],
    ]

    instance.raft_read_index()

    assert instance.sql("select key, value from _pico_db_config where key like '%sql_preemption%';") == expected


def install_sql_cancel_helper(instance):
    instance.eval("""
        rawset(_G, "__pico_sql_cancel_helper", {
            fid = nil,
            fiber = nil,
            joined = false,
            done = false,
            ok = nil,
            err = nil,
            result = nil,
        })
        return true
    """)


def start_sql_cancel_query(instance, sql):
    return instance.eval(
        """
        local fiber = require('fiber')
        local state = rawget(_G, "__pico_sql_cancel_helper")
        assert(state ~= nil, "sql cancel helper is not installed")

        state.done = false
        state.ok = nil
        state.err = nil
        state.result = nil
        state.joined = false

        local f = fiber.new(function(query)
            local ok, res = pcall(function()
                return box.func[".proc_sql_dispatch"]:call({query, {}})
            end)

            state.ok = ok
            if ok then
                state.result = res
            else
                state.err = tostring(res)
            end
            state.done = true
        end, ...)

        f:set_joinable(true)

        state.fid = f:id()
        state.fiber = f
        return state.fid
    """,
        sql,
    )


def cancel_sql_cancel_query(instance):
    return instance.eval("""
        local fiber = require('fiber')
        local state = rawget(_G, "__pico_sql_cancel_helper")
        assert(state ~= nil, "sql cancel helper is not installed")
        assert(state.fid ~= nil, "sql cancel fiber is not started")
        assert(fiber.find(state.fid) ~= nil, "sql cancel fiber does not exist")
        fiber.cancel(state.fid)
        return true
    """)


def get_sql_cancel_state(instance):
    return instance.eval("""
        local state = rawget(_G, "__pico_sql_cancel_helper")
        assert(state ~= nil, "sql cancel helper is not installed")

        return {
            fid = state.fid,
            joined = state.joined,
            done = state.done,
            ok = state.ok,
            err = state.err,
            result = state.result,
            status = state.fiber ~= nil and state.fiber:status() or (state.joined and 'dead' or nil),
        }
    """)


def wait_sql_cancel_query(instance):
    timeout = 120.0
    if instance.cluster is not None and instance.cluster.pytest_timeout is not None:
        timeout = float(instance.cluster.pytest_timeout)

    return instance.eval(
        """
        local state = rawget(_G, "__pico_sql_cancel_helper")
        assert(state ~= nil, "sql cancel helper is not installed")

        if state.fiber ~= nil and not state.joined then
            local f = state.fiber
            local ok, res = f:join()
            state.joined = true
            state.fiber = nil
            assert(ok, tostring(res))
        end

        return {
            fid = state.fid,
            joined = state.joined,
            done = state.done,
            ok = state.ok,
            err = state.err,
            result = state.result,
            status = state.fiber ~= nil and state.fiber:status() or (state.joined and 'dead' or nil),
        }
    """,
        timeout=timeout,
    )


def cleanup_sql_cancel_helper(instance):
    instance.eval("""
        local state = rawget(_G, "__pico_sql_cancel_helper")
        if state ~= nil and state.fiber ~= nil and not state.joined then
            local status_ok, status = pcall(function()
                return state.fiber:status()
            end)
            if status_ok and status == 'dead' then
                pcall(function()
                    state.fiber:join()
                end)
            end
            state.fiber = nil
            state.joined = false
        end
        rawset(_G, "__pico_sql_cancel_helper", nil)
        return true
    """)


def test_master_demotion_during_preemption(cluster: Cluster):
    """
    Master gets demoted while SQL fiber is yielded via preemption.
    Verifies the SQL query terminates gracefully (no crash, no corruption).
    """
    # 3 instances for raft quorum, 2 of them in the same replicaset.
    i1, i2, i3 = cluster.deploy(instance_count=3)
    i4 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i5 = cluster.add_instance(wait_online=True, replicaset_name="r99")

    # i4 is master as the first member of the replicaset.
    assert i4.replicaset_master_name() == i4.name
    assert not i4.eval("return box.info.ro")
    assert i5.eval("return box.info.ro")

    enable_preemption(i1, True, 0, 1)

    ddl = i1.sql("""
        create table t (a int, b int, primary key (a))
        using memtx
        distributed by (a)
        wait applied globally
        option (timeout = 3)
    """)
    assert ddl["row_count"] == 1

    # Insert enough data to make a DQL that yields during preemption.
    values = ",".join(f"({i}, {i})" for i in range(10))
    i4.sql(f"insert into t values {values}")

    # Arm the error injection to block the SQL fiber mid-yield on the master.
    injection_hit = log_crawler(i4, "ERROR INJECTION 'BLOCK_SQL_PREEMPTION_BEFORE_YIELD': BLOCKING")
    i4.call("pico._inject_error", "BLOCK_SQL_PREEMPTION_BEFORE_YIELD", True)

    # Start a long-running DQL on master in a separate thread.
    sql_result = {"error": None}

    def run_dql():
        try:
            i4.sql("select * from t limit 10", timeout=30)
        except Exception as err:
            sql_result["error"] = err

    t = threading.Thread(target=run_dql)
    t.start()

    # Wait for the SQL fiber to hit the injection point (it yielded via preemption).
    injection_hit.wait_matched()

    # Trigger master switchover: demote i4, promote i5.
    index, _ = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r99"],
        ops=[("=", "target_master_name", i5.name)],
    )
    cluster.raft_wait_index(index)
    assert i5.replicaset_master_name() == i5.name

    # Unblock the SQL fiber — let it resume on the now-demoted node.
    injection_hit = log_crawler(i4, "ERROR INJECTION 'BLOCK_SQL_PREEMPTION_BEFORE_YIELD': UNBLOCKING")
    i4.call("pico._inject_error", "BLOCK_SQL_PREEMPTION_BEFORE_YIELD", False)
    # Wait for the SQL fiber to hit the injection point (sql execution is unblocked).
    injection_hit.wait_matched()

    t.join()
    assert not t.is_alive(), "SQL thread should have finished"
    assert sql_result["error"] is None

    i4.check_process_alive()
    i5.check_process_alive()

    # Verify the cluster is still functional after the switchover.
    assert not i5.eval("return box.info.ro"), "i5 should be the new master"
    assert i4.eval("return box.info.ro"), "i4 should be read-only"


def test_master_promotion_during_preemption(cluster: Cluster):
    """
    A replica gets promoted to master while SQL fiber is yielded via preemption.
    Verifies the SQL query terminates gracefully.
    """
    i1, i2, i3 = cluster.deploy(instance_count=3)
    i4 = cluster.add_instance(wait_online=True, replicaset_name="r99")
    i5 = cluster.add_instance(wait_online=True, replicaset_name="r99")

    # i4 is master, i5 is replica.
    assert i4.replicaset_master_name() == i4.name
    assert i5.eval("return box.info.ro")

    enable_preemption(i1, True, 0, 1)

    ddl = i1.sql("""
        create table t (a int, b int, primary key (a))
        using memtx
        distributed by (a)
        wait applied globally
        option (timeout = 3)
    """)
    assert ddl["row_count"] == 1

    values = ",".join(f"({i}, {i})" for i in range(500))
    i4.sql(f"insert into t values {values}")

    # Arm the injection on the replica (i5) to block during preemption yield.
    injection_hit = log_crawler(i5, "ERROR INJECTION 'BLOCK_SQL_PREEMPTION_BEFORE_YIELD': BLOCKING")
    i5.call("pico._inject_error", "BLOCK_SQL_PREEMPTION_BEFORE_YIELD", True)

    # Start a DQL on replica in a separate thread.
    sql_result = {"error": None}

    def run_dql():
        try:
            i5.sql("select * from t limit 10", timeout=30)
        except Exception as err:
            sql_result["error"] = err

    t = threading.Thread(target=run_dql)
    t.start()

    injection_hit.wait_matched()

    # Promote i5 to master while its SQL fiber is blocked at the yield point.
    index, _ = cluster.cas(
        "update",
        "_pico_replicaset",
        key=["r99"],
        ops=[("=", "target_master_name", i5.name)],
    )
    cluster.raft_wait_index(index)
    assert i5.replicaset_master_name() == i5.name

    # Unblock the SQL fiber — let it resume on the now-promoted node.
    injection_hit = log_crawler(i5, "ERROR INJECTION 'BLOCK_SQL_PREEMPTION_BEFORE_YIELD': UNBLOCKING")
    i5.call("pico._inject_error", "BLOCK_SQL_PREEMPTION_BEFORE_YIELD", False)
    # Wait for the SQL fiber to hit the injection point (sql execution is unblocked).
    injection_hit.wait_matched()

    t.join(timeout=5)
    assert not t.is_alive(), "SQL thread should have finished"
    assert sql_result["error"] is None

    # Instance must not crash.
    i4.check_process_alive()
    i5.check_process_alive()

    # Verify the cluster is still functional.
    assert not i5.eval("return box.info.ro"), "i5 should be the new master"
    assert i4.eval("return box.info.ro"), "i4 should be read-only"


def test_sql_fiber_cancel_during_preemption(cluster: Cluster):
    """
    This is the case with single fiber - Router.

    SQL fiber gets cancelled explicitly while blocked in the preemption path.
    Verifies the query aborts with fiber cancellation and the instance remains
    functional.
    """
    (i1,) = cluster.deploy(instance_count=1, init_replication_factor=1, enable_http=True)
    cluster.wait_balanced()

    enable_preemption(i1, True, 0, 1)

    ddl = i1.sql("""
        create table t (a int, b int, primary key (a))
        using memtx
        distributed globally
        wait applied globally
        option (timeout = 3)
    """)
    assert ddl["row_count"] == 1

    values = ",".join(f"({i}, {i})" for i in range(500))
    i1.sql(f"insert into t values {values}")

    install_sql_cancel_helper(i1)

    try:
        # Arm the injection to block the SQL fiber in the preemption path.
        injection_hit = log_crawler(i1, "ERROR INJECTION 'BLOCK_SQL_PREEMPTION_BEFORE_YIELD': BLOCKING")
        i1.call("pico._inject_error", "BLOCK_SQL_PREEMPTION_BEFORE_YIELD", True)

        sql = """
            select * from t
        """
        fid = start_sql_cancel_query(i1, sql)
        assert fid is not None

        # Wait for the SQL router fiber to hit the injection point.
        injection_hit.wait_matched()

        sql_yield_count_before_continuation = int(total_samples(i1.get_metrics(), "pico_sql_yields"))

        state = get_sql_cancel_state(i1)
        assert state["fid"] == fid

        # Cancel the blocked SQL fiber explicitly from a separate control call.
        assert cancel_sql_cancel_query(i1)

        # Wait until the query fiber handles cancellation and finishes.
        state = wait_sql_cancel_query(i1)
        assert state["done"] is True
        assert state["ok"] is False
        assert "fiber is cancelled" in state["err"]

        sql_yield_count_final = int(total_samples(i1.get_metrics(), "pico_sql_yields"))

        # No additional yields should have been recorded for the cancelled fiber.
        # This relies on the instance having no background SQL activity between reads.
        assert sql_yield_count_before_continuation == sql_yield_count_final

    finally:
        i1.call("pico._inject_error", "BLOCK_SQL_PREEMPTION_BEFORE_YIELD", False)
        cleanup_sql_cancel_helper(i1)

    # The instance must not crash.
    i1.check_process_alive()

    # Verify the instance is still functional by running a new query.
    result = i1.sql("select count(*) from t")
    assert result == [[500]]


def test_sql_fiber_cancel_during_preemption_cleans_temp_tables(cluster: Cluster):
    """
    SQL fiber gets cancelled while a motion temp table is being populated on
    storage. Verifies the same cached query still succeeds afterwards and does
    not hit stale temp-table rows.
    """
    (i1,) = cluster.deploy(instance_count=1, init_replication_factor=1, enable_http=True)
    cluster.wait_balanced()

    # Use opcode_max = 1 so every maybe_yield() is a real preemption yield.
    # The dedicated populate_table injection only arms after the first inserted
    # row, so cancellation still happens with partially populated temp-table
    # state.
    enable_preemption(i1, True, 0, 1)

    ddl = i1.sql(""" create table t (a int primary key, b int) """)
    assert ddl["row_count"] == 1

    values = ",".join(f"({i}, {i})" for i in range(100))
    i1.sql(f"insert into t values {values}")

    sql = """ select count(*) from t t1 join t t2 on t1.a = t2.b """

    explain = i1.sql(f"explain {sql}")
    assert any("motion" in line for line in explain), explain

    # Warm the cache first so the cancelled run reuses the same temp tables.
    result = i1.sql(sql)
    assert result == [[100]]

    install_sql_cancel_helper(i1)

    try:
        injection_hit = log_crawler(i1, "ERROR INJECTION 'BLOCK_POPULATE_TABLE_BEFORE_YIELD': BLOCKING")
        i1.call("pico._inject_error", "BLOCK_POPULATE_TABLE_BEFORE_YIELD", True)

        fid = start_sql_cancel_query(i1, sql)
        assert fid is not None

        injection_hit.wait_matched()
        assert cancel_sql_cancel_query(i1)

        state = wait_sql_cancel_query(i1)
        assert state["done"] is True
        assert state["ok"] is False
        assert "fiber is cancelled" in state["err"]

    finally:
        i1.call("pico._inject_error", "BLOCK_POPULATE_TABLE_BEFORE_YIELD", False)
        cleanup_sql_cancel_helper(i1)

    i1.check_process_alive()

    # Re-run the same cached query: temp tables must have been truncated even
    # though the previous execution was cancelled mid-population.
    result = i1.sql(sql)
    assert result == [[100]]


def install_sql_blocker_helper(instance):
    """Install a separate fiber helper for the 'blocker' query (Fiber A)."""
    instance.eval("""
        rawset(_G, "__pico_sql_blocker_helper", {
            fid = nil,
            done = false,
            ok = nil,
            err = nil,
        })
        return true
    """)


def start_sql_blocker_query(instance, sql):
    """Start a SQL query in a new fiber tracked by the blocker helper."""
    return instance.eval(
        """
        local fiber = require('fiber')
        local state = rawget(_G, "__pico_sql_blocker_helper")
        assert(state ~= nil, "sql blocker helper is not installed")

        state.done = false
        state.ok = nil
        state.err = nil

        local f = fiber.new(function(query)
            local ok, res = pcall(function()
                return box.func[".proc_sql_dispatch"]:call({query, {}})
            end)

            state.ok = ok
            if not ok then
                state.err = tostring(res)
            end
            state.done = true
        end, ...)

        state.fid = f:id()
        return state.fid
    """,
        sql,
    )


def get_sql_blocker_state(instance):
    return instance.eval("""
        local state = rawget(_G, "__pico_sql_blocker_helper")
        assert(state ~= nil, "sql blocker helper is not installed")
        return {
            fid = state.fid,
            done = state.done,
            ok = state.ok,
            err = state.err,
        }
    """)


def cleanup_sql_blocker_helper(instance):
    instance.eval("""
        local fiber = require('fiber')
        local state = rawget(_G, "__pico_sql_blocker_helper")
        if state ~= nil and state.fid ~= nil then
            local f = fiber.find(state.fid)
            if f ~= nil and f:status() == 'dead' then
                pcall(function()
                    f:join()
                end)
            end
        end
        rawset(_G, "__pico_sql_blocker_helper", nil)
        return true
    """)


def test_sql_cancelled_owner_cleans_retired_plan(cluster: Cluster):
    """
    A long-running cached query (Q1) is evicted by a different plan (Q2) while
    Q1 still owns the per-plan lock. Q2 must finish without waiting for Q1's
    cleanup, and when the cancelled Q1 owner unwinds it must retire the old
    temp tables before releasing the plan lock.
    """
    (i1,) = cluster.deploy(instance_count=1, init_replication_factor=1, enable_http=True)
    cluster.wait_balanced()

    enable_preemption(i1, True, 0, 1)

    ddl = i1.sql("""
        create table t1 (a int primary key, b int)
    """)
    assert ddl["row_count"] == 1

    ddl = i1.sql("""
        create table t2 (a int primary key, b int)
    """)
    assert ddl["row_count"] == 1

    values = ",".join(f"({i}, {i})" for i in range(100))
    i1.sql(f"insert into t1 values {values}")

    values = ",".join(f"({i}, {i})" for i in range(100))
    i1.sql(f"insert into t2 values {values}")

    i1.sql("alter system set sql_storage_cache_count_max = 1")
    i1.raft_read_index()
    assert [[1]] == i1.sql("select value from _pico_db_config where key = 'sql_storage_cache_count_max';")

    q1 = """ select count(*) from t1 t11 join t1 t12 on t11.a = t12.b """
    q2 = "delete from t2"

    explain = i1.sql(f"explain {q1}")
    assert any("motion" in line for line in explain), explain

    assert i1.sql(q1) == [[100]]

    install_sql_cancel_helper(i1)

    try:
        injection_hit = log_crawler(i1, "ERROR INJECTION 'BLOCK_POPULATE_TABLE_BEFORE_YIELD': BLOCKING")
        i1.call("pico._inject_error", "BLOCK_POPULATE_TABLE_BEFORE_YIELD", True)

        fid = start_sql_cancel_query(i1, q1)
        assert fid is not None
        injection_hit.wait_matched()

        delete_res = i1.sql(q2)
        assert delete_res["row_count"] == 100

        assert cancel_sql_cancel_query(i1)
        state = wait_sql_cancel_query(i1)
        assert state["done"] is True
        assert state["ok"] is False
        assert "fiber is cancelled" in state["err"]

    finally:
        i1.call("pico._inject_error", "BLOCK_POPULATE_TABLE_BEFORE_YIELD", False)
        cleanup_sql_cancel_helper(i1)

    i1.check_process_alive()

    assert i1.sql(q1) == [[100]]
    assert i1.sql("select count(*) from t2") == [[0]]


def test_sql_same_plan_waiter_resumes_after_owner_release(cluster: Cluster):
    """
    Q1 owns the per-plan lock and is evicted by Q2 while blocked in
    populate_table(). Another Q1 request must wait only on the plan lock, then
    continue successfully after the cancelled owner releases it and retires the
    old temp tables.
    """
    (i1,) = cluster.deploy(instance_count=1, init_replication_factor=1, enable_http=True)
    cluster.wait_balanced()

    enable_preemption(i1, True, 0, 1)

    ddl = i1.sql("""
        create table t1 (a int primary key, b int)
    """)
    assert ddl["row_count"] == 1

    ddl = i1.sql("""
        create table t2 (a int primary key, b int)
    """)
    assert ddl["row_count"] == 1

    values = ",".join(f"({i}, {i})" for i in range(100))
    i1.sql(f"insert into t1 values {values}")

    values = ",".join(f"({i}, {i})" for i in range(100))
    i1.sql(f"insert into t2 values {values}")

    i1.sql("alter system set sql_storage_cache_count_max = 1")
    i1.raft_read_index()
    assert [[1]] == i1.sql("select value from _pico_db_config where key = 'sql_storage_cache_count_max';")

    q1 = """ select count(*) from t1 t11 join t1 t12 on t11.a = t12.b """
    q2 = "delete from t2"

    explain = i1.sql(f"explain {q1}")
    assert any("motion" in line for line in explain), explain
    assert i1.sql(q1) == [[100]]

    install_sql_cancel_helper(i1)

    waiter_rows: list[Any] | None = None
    waiter_error: Exception | None = None
    waiter_thread: threading.Thread | None = None

    try:
        injection_hit = log_crawler(i1, "ERROR INJECTION 'BLOCK_POPULATE_TABLE_BEFORE_YIELD': BLOCKING")
        i1.call("pico._inject_error", "BLOCK_POPULATE_TABLE_BEFORE_YIELD", True)

        fid = start_sql_cancel_query(i1, q1)
        assert fid is not None
        injection_hit.wait_matched()

        delete_res = i1.sql(q2)
        assert delete_res["row_count"] == 100

        lock_waits_before = int(total_samples(i1.get_metrics(), "pico_sql_temp_table_lock_waits"))

        def run_waiter_q1() -> None:
            nonlocal waiter_error, waiter_rows
            try:
                waiter_rows = i1.sql(q1, timeout=30)
            except Exception as err:
                waiter_error = err

        waiter_thread = threading.Thread(target=run_waiter_q1)
        waiter_thread.start()

        def check_waiter_blocked_on_temp_lock() -> None:
            waits = int(total_samples(i1.get_metrics(), "pico_sql_temp_table_lock_waits"))
            assert waits > lock_waits_before

        Retriable().call(check_waiter_blocked_on_temp_lock)

        assert cancel_sql_cancel_query(i1)
        i1.call("pico._inject_error", "BLOCK_POPULATE_TABLE_BEFORE_YIELD", False)

        state = wait_sql_cancel_query(i1)
        assert state["done"] is True
        assert state["ok"] is False
        assert "fiber is cancelled" in state["err"]

        waiter_thread.join(timeout=5)
        assert not waiter_thread.is_alive(), "waiter Q1 thread should have finished"
        assert waiter_error is None
        assert waiter_rows is not None
        assert waiter_rows == [[100]]

    finally:
        i1.call("pico._inject_error", "BLOCK_POPULATE_TABLE_BEFORE_YIELD", False)
        if waiter_thread is not None:
            waiter_thread.join(timeout=5)
        cleanup_sql_cancel_helper(i1)

    i1.check_process_alive()
    assert i1.sql(q1) == [[100]]
    assert i1.sql("select count(*) from t2") == [[0]]
