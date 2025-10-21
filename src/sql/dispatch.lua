-- Intermediate layer of communication between Sbroad and vshard.
-- Function, described in this file are called from `vshard` Sbroad module.

local lerror = require('vshard.error')
local vrs = require('vshard.replicaset')
local table = require('table')
local fiber = require('fiber')
local buffer = require('buffer')
local ref_id = 0
local session_id = 0
local SQL_MIN_TIMEOUT = 10

local dispatch = {}

-- Helper function to convert table in form of
-- key-value table to array table.
--
-- Functions from this module are called from rust,
-- rust structs are transformed into lua key-value
-- tables, while our stored procedure expects arguments
-- as msgpack array.
-- FIXME: this should be removed, we should pass tuple
-- from rust without using lua tables at all.
--
local function prepare_args(args, rid, sid)
    local call_args = {}
    table.insert(call_args, rid)
    table.insert(call_args, sid)
    table.insert(call_args, args['required'])
    if args['optional'] then
        table.insert(call_args, args['optional'])
    end

    return call_args
end

local function get_router_for_tier(tier_name)
    return _G.pico.get_router_for_tier(tier_name)
end

local function get_replicasets_from_tier(tier_name)
    local router = get_router_for_tier(tier_name)
    local replicasets = router:routeall()
    return replicasets
end

local function future_wait(cond, timeout)
    local f = function(cond, timeout)
        if timeout and timeout <= 0 then
            error(lerror.make("dql timeout exceeded"))
        end
        local res, err = cond:wait_result(timeout)
        if err then
            error(lerror.make(err))
        end
        return res
    end
    local ok, res = pcall(f, cond, timeout)
    if ok then
        return res
    end
    return nil, res
end

--
-- Helper function to execute SQL request on multiple storages,
-- without buckets being moved between the storages by vhard
-- rebalancer. This function is a modified version of `map_callrw`
-- from vshard router api (see its doc for more details).
--
-- To ensure data does not move between storages during execution,
-- there are two stages: ref and map.
-- 1. Ref stage creates a reference with deadline on each specified
-- replicaset's master. While this reference is alive the master
-- will not receive, nor send data.
-- 2. Map stage - after references were created on each master, request to execute
-- the given function is sent to each master. After function was executed the
-- reference is deleted. If the reference expires, the error will be returned
-- to router.
--
-- NOTE: this function does not work correctly as it does not account for
-- for outdated bucket to replicaset mapping. This will be fixed as soon as
-- https://github.com/tarantool/vshard/pull/442 is merged.
--
-- @param uuid_to_args Mapping between replicaset uuid and function arguments
-- for that replicaset.
-- @param func Name of the function to call.
-- @param opts Table which may have the following options:
--  1. timeout - timeout for the whole function execution.
--  all buckets. If this is true, the error will be returned if router's bucket
--  count != bucket count covered by ref stage. It may mean that router has
--  outdated configuration (some buckets were added/deleted on storages that router
--  does not know about). This option should be used only if you intend to execute
--  the function on all replicasets and want to ensure that all buckets were covered.
--  @param tier Name of the vshard tier to use.
--
-- @return mapping between replicaset uuid and ibuf containing result
--
local function plan_dispatch(uuid_to_args, opts, tier)
    local router = get_router_for_tier(tier)
    local replicasets = router:routeall()
    local timeout
    local res_map = {}
    for uuid, _ in pairs(uuid_to_args) do
        res_map[uuid] = buffer.ibuf()
    end
    if opts then
        timeout = opts.timeout or SQL_MIN_TIMEOUT
    else
        timeout = SQL_MIN_TIMEOUT
    end

    local err, err_uuid, res
    local futures = {}
    local opts_ref = { is_async = true }
    local opts_map = { is_async = true, skip_header = true }
    local rs_count = 0
    local rid = ref_id + 1
    local sid = session_id + 1
    local deadline = fiber.clock() + timeout
    -- Nil checks are done explicitly here (== nil instead of 'not'), because
    -- netbox requests return box.NULL instead of nils.

    -- Wait for all masters to connect.
    vrs.wait_masters_connect(replicasets, timeout)
    timeout = deadline - fiber.clock()


    --
    -- Ref stage: send.
    --
    for uuid, _ in pairs(uuid_to_args) do
        local rs = replicasets[uuid]
        res, err = rs:callrw('pico.dispatch.lref.add',
            { rid, sid, timeout }, opts_ref)
        if res == nil then
            err_uuid = uuid
            goto fail
        end
        futures[uuid] = res
        rs_count = rs_count + 1
    end
    --
    -- Ref stage: collect.
    --
    for uuid, future in pairs(futures) do
        res, err = future_wait(future, timeout)
        -- Handle netbox error first.
        if res == nil then
            err_uuid = uuid
            goto fail
        end
        -- Ref returns nil,err or bucket count.
        res, err = res[1], res[2]
        if res == nil then
            err_uuid = uuid
            goto fail
        end
        timeout = deadline - fiber.clock()
    end

    -- Map stage: send.
    --
    for uuid, rs_args in pairs(uuid_to_args) do
        local rs = replicasets[uuid]
        opts_map['buffer'] = res_map[uuid]
        res, err = rs:callrw('.proc_sql_execute', prepare_args(rs_args, rid, sid), opts_map)
        if res == nil then
            err_uuid = uuid
            goto fail
        end
        futures[uuid] = res
    end
    --
    -- Map stage: collect.
    --
    for uuid, f in pairs(futures) do
        res, err = future_wait(f, timeout)
        if res == nil then
            err_uuid = uuid
            goto fail
        end
        if err ~= nil then
            err_uuid = uuid
            goto fail
        end
        timeout = deadline - fiber.clock()
    end
    do return res_map end

    ::fail::
    for uuid, f in pairs(futures) do
        f:discard()
        -- Best effort to remove the created refs before exiting. Can help if
        -- the timeout was big and the error happened early.
        f = replicasets[uuid]:callrw('pico.dispatch.lref.del',
            { rid, sid }, opts_ref)
        if f ~= nil then
            -- Don't care waiting for a result - no time for this. But it won't
            -- affect the request sending if the connection is still alive.
            f:discard()
        end
    end
    local msg = "Unknown error"
    if err ~= nil and err.message ~= nil then
        msg = err.message
    end
    error(lerror.make("Error on replicaset " .. err_uuid .. ": " .. msg))
end

dispatch.bucket_into_rs = function(bucket_id, tier)
    local router = get_router_for_tier(tier)
    local rs, err = router:route(bucket_id)
    if err ~= nil then
        error(err)
    end
    return rs.uuid
end

dispatch.custom_plan_dispatch = function(uuid_to_args, timeout, tier)
    local opts = { timeout = timeout }

    return plan_dispatch(uuid_to_args, opts, tier)
end

-- Execute an SQL query on the given replicasets UUIDs
-- using the same plan for each replicaset.
--
-- @param args to pass to stored procedure that will be called
-- on each replicaset;
-- @param uuids replicasets UUIDs on which to execute plan;
-- @param timeout timeout in seconds for whole function;
-- @param tier name of the vshard tier to use.
--
-- @return mapping between a replicaset UUID and am ibuf with result.
--
dispatch.single_plan_dispatch = function(args, uuids, timeout, tier)
    if not next(uuids) then
        -- An empty list of UUIDs means execution on all replicasets.
        local uuid_to_rs = get_replicasets_from_tier(tier)
        for uuid, _ in pairs(uuid_to_rs) do
            table.insert(uuids, uuid)
        end
    end

    local uuid_to_args = {}
    for _, uuid in pairs(uuids) do
        uuid_to_args[uuid] = args
    end

    local opts = { timeout = timeout }

    return plan_dispatch(uuid_to_args, opts, tier)
end

local function init()
    if rawget(_G, 'pico') == nil then
        error("dispatch must be initialized after pico module was set!")
    end
    _G.pico.dispatch = dispatch
    _G.pico.dispatch.lref = require('vshard.lref')
end

return {
    init = init
}
