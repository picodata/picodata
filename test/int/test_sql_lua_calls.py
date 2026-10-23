"""
SQL calls into Lua take Lua threads from a pool (`src/sql/lua.rs`).

The pool statistics come from `pico._sql_lua_pool_stats()`, which exists only
in builds with the `error_injection` feature.
"""

import threading
import time
from typing import Any, Callable

import psycopg
import pytest

from conftest import TIMEOUT_SCALE, Instance, Retriable, TarantoolError

PG_USER = "postgres"
PG_PASSWORD = "Passw0rd"


def pool_stats(instance: Instance) -> dict[str, int]:
    return instance.eval("return pico._sql_lua_pool_stats()")


def lua_allocated(instance: Instance) -> int:
    return instance.eval("return misc.getmetrics().gc_allocated")


def insert_rows(instance: Instance, table: str, rows: int, row: Callable[[int], str]) -> None:
    for start in range(1, rows + 1, 1000):
        values = ", ".join(row(i) for i in range(start, min(start + 1000, rows + 1)))
        instance.sql(f"INSERT INTO {table} VALUES {values}")


def set_sql_runtime_concurrency_max(instance: Instance, value: int) -> None:
    # ALTER SYSTEM returns after the new value is applied.
    instance.sql(f"ALTER SYSTEM SET sql_runtime_concurrency_max = {value}", sudo=True)


def create_t(instance: Instance, rows: int) -> None:
    instance.sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    insert_rows(instance, "t", rows, lambda i: f"({i}, 0)")


def run_pg_clients(
    connections: list[psycopg.Connection],
    sql: str,
    params: Callable[[int, int], tuple[Any, ...]],
    per_client: int,
) -> None:
    errors: list[Exception] = []

    def client(n: int, conn: psycopg.Connection) -> None:
        try:
            for i in range(per_client):
                conn.execute(sql, params(n, i), prepare=True)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=client, args=(n, conn)) for n, conn in enumerate(connections)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors, errors


@pytest.mark.timeout(int(300 * TIMEOUT_SCALE))
def test_lua_allocations_per_point_query(instance: Instance):
    accounts = 10_000
    clients = 20
    per_client = 500
    instance.sql("CREATE TABLE pgbench_accounts (aid INT PRIMARY KEY, bid INT, abalance INT, filler VARCHAR(84))")
    insert_rows(instance, "pgbench_accounts", accounts, lambda i: f"({i}, 1, 0, NULL)")
    instance.sql(f"CREATE USER \"{PG_USER}\" WITH PASSWORD '{PG_PASSWORD}'")
    instance.sql(f'GRANT READ TABLE TO "{PG_USER}"', sudo=True)
    instance.sql(f'GRANT WRITE TABLE TO "{PG_USER}"', sudo=True)

    # Connections are opened before the measurement so that it counts queries only.
    connections = [instance.connect_via_pgproto(user=PG_USER, password=PG_PASSWORD) for _ in range(clients)]
    for conn in connections:
        conn.autocommit = True

    def key(n: int, i: int) -> int:
        return (n * per_client + i) % accounts + 1

    queries: list[tuple[str, Callable[[int, int], tuple[Any, ...]]]] = [
        ("SELECT abalance FROM pgbench_accounts WHERE aid = %s", lambda n, i: (key(n, i),)),
        ("UPDATE pgbench_accounts SET abalance = %s WHERE aid = %s", lambda n, i: (i, key(n, i))),
    ]
    try:
        for sql, params in queries:
            # Warm-up: 1 000 queries.
            run_pg_clients(connections, sql, params, 1000 // clients)

            created = pool_stats(instance)["created"]
            start = time.monotonic()
            before = lua_allocated(instance)
            run_pg_clients(connections, sql, params, per_client)
            during_load = lua_allocated(instance) - before
            elapsed = time.monotonic() - start

            # Background fibers allocate in Lua too, so subtract an idle window of the same length.
            before = lua_allocated(instance)
            time.sleep(elapsed)
            background = lua_allocated(instance) - before

            # Threads come from the pool: a few per concurrent client, not one per call.
            assert pool_stats(instance)["created"] - created <= clients

            per_query = (during_load - background) / (clients * per_client)
            assert per_query <= 512, f"{sql}: {per_query:.0f} bytes per query"
    finally:
        for conn in connections:
            conn.close()


def test_waiting_calls_and_pool_capacity(instance: Instance):
    count = 150
    create_t(instance, count)
    # With the default of 50, the rest of the queries would wait for a runtime
    # slot instead of the storage reference.
    set_sql_runtime_concurrency_max(instance, 200)

    # While vshard considers a bucket move in progress, every storage
    # reference waits in `sched.ref_start`.
    instance.eval(
        """
        local fiber = require('fiber')
        local count = ...
        assert(require('vshard.registry').storage_sched.move_start(60))
        local fibers = {}
        for id = 1, count do
            local f = fiber.new(function()
                local _, err = pico.sql('UPDATE t SET v = ? WHERE id = ?', {id * 10, id})
                if err ~= nil then
                    error(err)
                end
                local res, err = pico.sql('SELECT v FROM t WHERE id = ?', {id})
                if err ~= nil then
                    error(err)
                end
                return res.rows[1][1]
            end)
            f:set_joinable(true)
            fibers[id] = f
        end
        rawset(_G, 'sql_lua_calls_fibers', fibers)
        """,
        count,
    )

    def all_wait():
        # One Lua thread per waiting query: none of them waits for a thread, and
        # the thread taken for routing is released before the local UPDATE.
        assert pool_stats(instance)["busy"] == count

    Retriable().call(all_wait)

    values = instance.eval(
        """
        require('vshard.registry').storage_sched.move_end(1)
        local values = {}
        for id, f in ipairs(_G.sql_lua_calls_fibers) do
            local ok, v = f:join()
            if not ok then
                error(v)
            end
            values[id] = v
        end
        rawset(_G, 'sql_lua_calls_fibers', nil)
        return values
        """
    )
    assert values == [id * 10 for id in range(1, count + 1)]

    stats = pool_stats(instance)
    assert stats["busy"] == 0
    # The capacity is min(sql_runtime_concurrency_max, 1024) = 200, so all are kept.
    assert stats["idle"] == count

    # A lowered limit shrinks the pool on the next returned thread.
    set_sql_runtime_concurrency_max(instance, 100)
    assert instance.sql("SELECT v FROM t WHERE id = 1") == [[10]]
    assert pool_stats(instance)["idle"] == 100


def test_refused_ref_keeps_error_text(instance: Instance):
    create_t(instance, 10)
    instance.eval(
        """
        local registry = require('vshard.registry')
        local lerror = require('vshard.error')
        local sched = registry.storage_sched
        rawset(_G, 'sql_lua_calls_sched', sched)
        registry.storage_sched = setmetatable({
            ref_start = function()
                return nil, lerror.vshard(lerror.code.STORAGE_REF_ADD, 'refused by test')
            end,
        }, {__index = sched})
        """
    )

    with pytest.raises(TarantoolError) as error:
        instance.sql("UPDATE t SET v = 1 WHERE id = 1")
    # The prefix comes from Rust, the rest is the vshard error as it was.
    message = str(error.value)
    assert "failed to add local bucket reference: " in message
    assert "vshard error: Can not add a storage ref: refused by test" in message
    assert "name: STORAGE_REF_ADD" in message

    instance.eval(
        """
        require('vshard.registry').storage_sched = _G.sql_lua_calls_sched
        rawset(_G, 'sql_lua_calls_sched', nil)
        """
    )
    assert instance.sql("UPDATE t SET v = 1 WHERE id = 1") == {"row_count": 1}
