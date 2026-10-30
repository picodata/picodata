"""
SQL protects the data of a storage from bucket moves with a vshard storage
reference: a bucket move does not start while the storage is referenced.

When no bucket move runs or waits and all the buckets are writable, a storage
takes the reference without Lua, as a count in the vshard scheduler
(`StorageRef` in `src/sql/dispatch.rs`). Otherwise it calls the vshard ref
module, which waits for the moves and follows the scheduler quotas.

The tests hold a query with error injections: `BLOCK_STORAGE_REF_TAKEN` stops
it after it took its reference and before it runs, `BLOCK_STORAGE_REF_RELEASE`
after it finished and before it releases the reference.
"""

import threading

import pytest

from conftest import Cluster, Instance, Retriable, TarantoolError

# Counts the storage refs taken through Lua: only they call `ref_start` of the
# vshard scheduler. `lref.add` takes the scheduler from the registry on every
# call, so the count works whenever it is set.
COUNT_LUA_REFS = """
local registry = require('vshard.registry')
local sched = registry.storage_sched
rawset(_G, 'lua_refs', 0)
registry.storage_sched = setmetatable({
    ref_start = function(...)
        _G.lua_refs = _G.lua_refs + 1
        return sched.ref_start(...)
    end,
}, {__index = sched})
"""

# Waits until `cond()` is true or 5 seconds pass.
WAIT_FOR = """
local fiber = require('fiber')
local function wait_for(cond)
    local deadline = fiber.clock() + 5
    while not cond() do
        assert(fiber.clock() < deadline, 'timed out')
        fiber.sleep(0.01)
    end
end
"""


def create_t(instance: Instance) -> None:
    instance.sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)")


def reset_bucket_state(instance: Instance) -> None:
    """Make the next storage ref take the Lua path and learn the bucket state.

    A commit to `_bucket` resets the cached state, even with the same tuple.
    """
    all_rw = instance.eval(
        """
        local bucket = box.space._bucket:select({}, {limit = 1})[1]
        box.space._bucket:replace(bucket)
        return require('vshard.registry').storage_sched.all_rw
        """
    )
    assert all_rw == -1


def test_expired_ref_does_not_block_moves(instance: Instance):
    # A router took a ref on the first step of a two-step dispatch and never
    # came back. Once the ref expires, it must not block bucket moves.
    moved, ref_count = instance.eval(
        """
        local fiber = require('fiber')
        local sched = require('vshard.registry').storage_sched
        assert(pico.dispatch.lref.add(1, 'gone-router', 0.1))
        fiber.sleep(0.3)
        local ok = sched.move_start(0.5)
        if ok then
            sched.move_end(1)
        end
        return {ok ~= nil, sched.ref_count}
        """
    )
    assert moved
    assert ref_count == 0


def test_fast_path_skips_lua(instance: Instance):
    create_t(instance)
    instance.eval(COUNT_LUA_REFS)
    reset_bucket_state(instance)
    # The first query learns that all the buckets are writable.
    instance.sql("INSERT INTO t VALUES (0, 0)")
    assert instance.eval("return lua_refs") == 1

    lua_refs, ref_count = instance.eval(
        """
        for id = 1, 100 do
            local _, err = pico.sql('INSERT INTO t VALUES (?, 0)', {id})
            assert(err == nil, err)
        end
        return {lua_refs, require('vshard.registry').storage_sched.ref_count}
        """
    )
    assert lua_refs == 1
    assert ref_count == 0


def test_bucket_change_resets_fast_path(instance: Instance):
    create_t(instance)
    instance.eval(COUNT_LUA_REFS)
    reset_bucket_state(instance)
    instance.sql("INSERT INTO t VALUES (0, 0)")
    assert instance.eval("return lua_refs") == 1

    # The next query after a `_bucket` change takes the Lua path once and learns
    # the bucket state again.
    reset_bucket_state(instance)
    instance.sql("INSERT INTO t VALUES (1, 0)")
    instance.sql("INSERT INTO t VALUES (2, 0)")
    assert instance.eval("return lua_refs") == 2


def test_query_waits_for_sending_bucket(instance: Instance):
    create_t(instance)
    instance.sql("INSERT INTO t VALUES (0, 0)")

    waited, err, in_txn, after_rollback, waited_after_rollback, err_after_rollback = instance.eval(
        """
        local fiber = require('fiber')
        local sched = require('vshard.registry').storage_sched
        local bucket = box.space._bucket:select({}, {limit = 1})[1]
        local sending = {bucket.id, 'sending', 'no-such-replicaset'}

        -- Inserts with the bucket in SENDING, then makes it active again.
        local function insert_while_sending(id)
            local done, err = false, nil
            local insert = fiber.new(function()
                _, err = pico.sql('INSERT INTO t VALUES (?, 0)', {id})
                done = true
            end)
            insert:set_joinable(true)
            fiber.sleep(0.3)
            local waited = not done
            box.space._bucket:replace(bucket)
            insert:join()
            return waited, err
        end

        box.space._bucket:replace(sending)
        local waited, err = insert_while_sending(1)

        -- A transaction makes the bucket active and learns that all the
        -- buckets are writable, then rolls back to SENDING. The rollback must
        -- forget what the transaction learned.
        box.space._bucket:replace(sending)
        box.begin()
        box.space._bucket:replace(bucket)
        assert(require('vshard').storage.internal.bucket_are_all_rw())
        local in_txn = sched.all_rw
        box.rollback()
        local after_rollback = sched.all_rw
        local waited_after_rollback, err_after_rollback = insert_while_sending(2)
        return {
            waited, err ~= nil, in_txn, after_rollback,
            waited_after_rollback, err_after_rollback ~= nil,
        }
        """
    )
    assert waited
    assert not err
    assert in_txn == 1
    assert after_rollback == -1
    assert waited_after_rollback
    assert not err_after_rollback


def test_ref_covers_query_execution(instance: Instance):
    create_t(instance)
    instance.sql("INSERT INTO t VALUES (0, 0)")

    move_waited, row_when_moved = instance.eval(
        WAIT_FOR
        + """
        local sched = require('vshard.registry').storage_sched
        pico._inject_error('BLOCK_STORAGE_REF_TAKEN', true)
        local done = false
        fiber.new(function()
            local _, err = pico.sql('INSERT INTO t VALUES (1, 0)')
            assert(err == nil, err)
            done = true
        end)
        -- The query holds its ref and has not written anything yet.
        wait_for(function() return sched.ref_count == 1 end)

        local moved, row_when_moved = false, nil
        fiber.new(function()
            moved = sched.move_start(5) ~= nil
            row_when_moved = box.space.t:get(1) ~= nil
        end)
        wait_for(function() return sched.move_queue == 1 end)
        fiber.sleep(0.2)
        local move_waited = not moved and box.space.t:get(1) == nil

        pico._inject_error('BLOCK_STORAGE_REF_TAKEN', false)
        wait_for(function() return done and moved end)
        sched.move_end(1)
        return {move_waited, row_when_moved}
        """
    )
    assert move_waited
    # The move started only after the query had committed its row.
    assert row_when_moved


@pytest.mark.parametrize("path", ["fast", "lua"])
@pytest.mark.parametrize("outcome", ["commit", "error"])
def test_ref_lasts_until_query_ends_and_wakes_move(instance: Instance, outcome: str, path: str):
    create_t(instance)
    instance.sql("INSERT INTO t VALUES (0, 0)")
    instance.eval(COUNT_LUA_REFS)
    if path == "lua":
        reset_bucket_state(instance)

    move_waited, err, wait_after_release, lua_refs = instance.eval(
        WAIT_FOR
        + """
        local outcome = ...
        local sched = require('vshard.registry').storage_sched
        pico._inject_error('BLOCK_STORAGE_REF_RELEASE', true)
        -- Key 0 exists already, so the error outcome fails on a duplicate.
        local id = outcome == 'commit' and 1 or 0
        local done, err = false, nil
        fiber.new(function()
            _, err = pico.sql('INSERT INTO t VALUES (?, 0)', {id})
            done = true
        end)
        wait_for(function() return sched.ref_count == 1 end)
        if outcome == 'commit' then
            -- The query has committed, but still holds its ref.
            wait_for(function() return box.space.t:get(1) ~= nil end)
        end

        local moved, started = false, nil
        fiber.new(function()
            moved = sched.move_start(5) ~= nil
            started = fiber.clock()
        end)
        wait_for(function() return sched.move_queue == 1 end)
        fiber.sleep(0.2)
        local move_waited = not moved and not done

        local released = fiber.clock()
        pico._inject_error('BLOCK_STORAGE_REF_RELEASE', false)
        wait_for(function() return done and moved end)
        sched.move_end(1)
        return {move_waited, err ~= nil, started - released, lua_refs}
        """,
        outcome,
    )
    assert move_waited
    assert err == (outcome == "error")
    # The release wakes the move up, it does not wait for its 5 s timeout.
    assert wait_after_release < 1
    assert lua_refs == (1 if path == "lua" else 0)


def test_failed_use_releases_ref(instance: Instance):
    # Rust pins `lref.use` on its first call, so wrap it before any SQL query.
    instance.eval(
        """
        local lerror = require('vshard.error')
        local lref = pico.dispatch.lref
        local use = lref.use
        rawset(_G, 'refuse_use', false)
        lref.use = function(...)
            if _G.refuse_use then
                _G.refuse_use = false
                return nil, lerror.vshard(lerror.code.STORAGE_REF_USE, 'refused by test')
            end
            return use(...)
        end
        """
    )
    create_t(instance)
    reset_bucket_state(instance)
    instance.eval("rawset(_G, 'refuse_use', true)")

    with pytest.raises(TarantoolError) as error:
        instance.sql("INSERT INTO t VALUES (1, 0)")
    message = str(error.value)
    assert "failed to use local bucket reference: " in message
    assert "refused by test" in message
    # The wrapper refused, and the ref added before is released instead of
    # blocking bucket moves until it expires.
    refuse_use, ref_count = instance.eval("return {refuse_use, require('vshard.registry').storage_sched.ref_count}")
    assert refuse_use is False
    assert ref_count == 0


def test_local_refs_do_not_clash_with_router_refs(instance: Instance):
    create_t(instance)

    err, kept, ref_count = instance.eval(
        """
        local lref = pico.dispatch.lref
        local sid = pico.dispatch.session_id()
        -- The refs this instance as a router takes on itself on the first step
        -- of a two-step dispatch.
        for rid = 1, 100 do
            assert(lref.add(rid, sid, 60))
        end
        -- The local query takes the Lua path with a rid of its own.
        local bucket = box.space._bucket:select({}, {limit = 1})[1]
        box.space._bucket:replace(bucket)
        local _, err = pico.sql('INSERT INTO t VALUES (1, 0)')
        local kept = 0
        for rid = 1, 100 do
            if lref.del(rid, sid) then
                kept = kept + 1
            end
        end
        return {err ~= nil, kept, require('vshard.registry').storage_sched.ref_count}
        """
    )
    assert not err
    # The local query neither clashed with the router's refs nor released any.
    assert kept == 100
    assert ref_count == 0


def test_queries_let_queued_move_through(instance: Instance):
    create_t(instance)

    strike, admitted, waiting, waiting_when_moved, rows = instance.eval(
        WAIT_FOR
        + """
        local sched = require('vshard.registry').storage_sched
        local function insert(id)
            local f = fiber.new(function()
                local _, err = pico.sql('INSERT INTO t VALUES (?, 0)', {id})
                assert(err == nil, err)
            end)
            f:set_joinable(true)
            return f
        end
        assert(insert(0):join())
        -- A finished move starts the count of refs in a row from zero.
        assert(sched.move_start(1))
        sched.move_end(1)
        local quota = sched.ref_quota
        sched.cfg({sched_ref_quota = 2})

        pico._inject_error('BLOCK_STORAGE_REF_RELEASE', true)
        local inserts = {insert(1)}
        wait_for(function() return sched.ref_count == 1 end)
        -- The fast path counts its ref in the strike, as sched.ref_start does.
        local strike = sched.ref_strike
        local move = fiber.new(function() return sched.move_start(10) ~= nil end)
        move:set_joinable(true)
        wait_for(function() return sched.move_queue == 1 end)
        for id = 2, 6 do
            table.insert(inserts, insert(id))
        end
        wait_for(function() return sched.ref_count + sched.ref_queue == 6 end)
        local admitted = sched.ref_count - 1
        local waiting = sched.ref_queue

        pico._inject_error('BLOCK_STORAGE_REF_RELEASE', false)
        local _, moved = move:join()
        assert(moved)
        local waiting_when_moved = sched.ref_queue
        sched.move_end(1)
        for _, f in ipairs(inserts) do
            assert(f:join())
        end
        sched.cfg({sched_ref_quota = quota})
        local rows = pico.sql('SELECT count(*) FROM t').rows[1][1]
        return {strike, admitted, waiting, waiting_when_moved, rows}
        """
    )
    assert strike == 1
    # The quota of 2 refs in a row has room for one more query, the other four
    # wait for the move.
    assert admitted == 1
    assert waiting == 4
    # The move started before the queries that waited for it.
    assert waiting_when_moved == waiting
    assert rows == 7


def test_lua_path_as_regular_user(instance: Instance):
    create_t(instance)
    user, password = "alice", "Passw0rd"
    instance.sql(f"CREATE USER \"{user}\" WITH PASSWORD '{password}'")
    instance.sql(f'GRANT WRITE TABLE TO "{user}"', sudo=True)
    conn = instance.connect_via_pgproto(user=user, password=password)

    # A running move sends the query to the Lua path, which switches the user
    # to admin only to add the ref.
    instance.eval("assert(require('vshard.registry').storage_sched.move_start(10))")
    errors: list[Exception] = []

    def insert():
        try:
            conn.execute("INSERT INTO t VALUES (1, 0)")
        except Exception as e:
            errors.append(e)

    thread = threading.Thread(target=insert)
    thread.start()

    def waits():
        assert instance.eval("return require('vshard.registry').storage_sched.ref_queue") == 1

    Retriable().call(waits)
    instance.eval("require('vshard.registry').storage_sched.move_end(1)")
    thread.join()
    assert not errors, errors
    assert instance.sql("SELECT v FROM t WHERE id = 1") == [[0]]


def two_storages(cluster: Cluster) -> tuple[Instance, Instance, int]:
    """Deploy two replicasets and return a router, a storage and a key on it."""
    router, storage = cluster.deploy(instance_count=2)
    create_t(router)
    router.sql("INSERT INTO t VALUES " + ", ".join(f"({i}, 0)" for i in range(1, 51)))
    key = storage.eval("return box.space.t:select({}, {limit = 1})[1].id")
    return router, storage, key


@pytest.mark.parametrize("path", ["fast", "lua"])
def test_router_cleanup_keeps_storage_ref(cluster: Cluster, path: str):
    router, storage, key = two_storages(cluster)
    sid = router.eval("return pico.dispatch.session_id()")
    # A two-step query leaves the router's ref session on the storage, so the
    # cleanup below gets to the refs of that session, not just "no session".
    router.sql("SELECT * FROM t")
    storage.eval(COUNT_LUA_REFS)
    if path == "lua":
        reset_bucket_state(storage)

    storage.eval("pico._inject_error('BLOCK_STORAGE_REF_RELEASE', true)")
    errors: list[Exception] = []

    def update():
        try:
            router.sql("UPDATE t SET v = 1 WHERE id = ?", key)
        except Exception as e:
            errors.append(e)

    thread = threading.Thread(target=update)
    thread.start()

    def holds_ref():
        assert storage.eval("return require('vshard.registry').storage_sched.ref_count") == 1

    Retriable().call(holds_ref)
    assert storage.eval("return lua_refs") == (1 if path == "lua" else 0)

    # After a failed dispatch the router deletes its refs on every storage by
    # its rid and session, even if a storage still executes the plan.
    no_ref, ref_count = storage.eval(
        """
        local sid = ...
        local no_ref = 0
        for rid = 1, 1000 do
            local _, err = pico.dispatch.lref.del(rid, sid)
            if err ~= nil and err.message:find('no ref') then
                no_ref = no_ref + 1
            end
        end
        return {no_ref, require('vshard.registry').storage_sched.ref_count}
        """,
        sid,
    )
    storage.eval("pico._inject_error('BLOCK_STORAGE_REF_RELEASE', false)")
    thread.join()
    assert not errors, errors
    assert no_ref == 1000
    assert ref_count == 1
    assert storage.eval("return require('vshard.registry').storage_sched.ref_count") == 0


def test_refused_remote_ref_keeps_error_text(cluster: Cluster):
    router, storage, key = two_storages(cluster)
    storage.eval(
        """
        local registry = require('vshard.registry')
        local lerror = require('vshard.error')
        local sched = registry.storage_sched
        rawset(_G, 'storage_refs_sched', sched)
        -- A running move sends the query to the Lua path.
        assert(sched.move_start(10))
        registry.storage_sched = setmetatable({
            ref_start = function()
                return nil, lerror.vshard(lerror.code.STORAGE_REF_ADD, 'refused by test')
            end,
        }, {__index = sched})
        """
    )

    with pytest.raises(TarantoolError) as error:
        router.sql("UPDATE t SET v = 1 WHERE id = ?", key)
    message = str(error.value)
    assert "Failed to add a storage reference: " in message
    assert "vshard error: Can not add a storage ref: refused by test" in message

    storage.eval(
        """
        local registry = require('vshard.registry')
        registry.storage_sched = _G.storage_refs_sched
        registry.storage_sched.move_end(1)
        """
    )
