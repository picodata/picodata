-- Intermediate layer of communication between Sbroad and vshard.
-- Function, described in this file are called from `vshard` Sbroad module.

local lerror = require('vshard.error')
local vrs = require('vshard.replicaset')
local helper = require('sbroad.helper')
local table = require('table')
local fiber = require('fiber')
local buffer = require('buffer')
local msgpack = require('msgpack');
local ref_id = 0
local DQL_MIN_TIMEOUT = 10

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
local function prepare_args(args, func_name)
    local call_args = {}
    if func_name then
        table.insert(call_args, func_name)
    end
    table.insert(call_args, args['required'])
    if args['optional'] then
        table.insert(call_args, args['optional'])
    end
    if args['cache_hint'] then
        table.insert(call_args, args['cache_hint'])
    end

    return call_args
end

local function get_router_for_tier(tier_name)
    local get_router, postfix
    if helper.pico_compat() then
        get_router = _G.pico.get_router_for_tier
        postfix = "pico"
    else
        get_router = _G.sbroad.get_router_for_tier
        postfix = "sbroad"
    end
    if get_router == nil then
        local err_msg = string.format(
            "can't get vshard router for tier %s. Please, define function\
'get_router_for_tier' in global table '_G.%s'", tier_name, postfix)
        error(err_msg)
    end

    return get_router(tier_name)
end

local function get_replicasets_from_tier(tier_name)
    local router = get_router_for_tier(tier_name)
    local replicasets = router:routeall()
    return replicasets
end

local function future_wait(cond, timeout)
    local f = function(cond, timeout)
        if timeout and timeout <= 0 then
            return nil, lerror.make("dql timeout exceeded")
        end
        local res, err = cond:wait_result(timeout)
        if err then
            return nil, lerror.make(err)
        end
        return res
    end
    local ok, res = pcall(f, cond, timeout)
    if ok then
        return res
    end
    return nil, lerror.make(res)
end

local ResultHandler = {
    vtable_row_count = 0,
    vtable_max_rows = 0,
}

function ResultHandler:new(o)
    o = o or {}
    setmetatable(o, self)
    self.__index = self
    return o
end

-- Process DQL query result returned from storage.
-- It decodes row count, checks that vtable rows limit
-- is not exceeded and sets Ibuf rpos to the start
-- of tuple storing actual data
--
-- @param res Ibuf storing msgpack that came
-- from storage
-- @param map_format true if result came from
-- multi_storage_dql map stage
--
-- @return error in case of error
-- @return nil in case of success
--
function ResultHandler:process_res(res, map_format)
    local len, data, rows_cnt, _
    if map_format then
        len, data = msgpack.decode_array_header(res.rpos, res:size())
        if len ~= 2 then
            return lerror.make("Unexpected map result len: " .. len)
        end
        local ok
        ok, data = msgpack.decode_unchecked(data)
        if ok ~= true then
            local err
            err, _ = msgpack.decode_unchecked(data)
            return lerror.make(err.message)
        end
    else
        -- data is wrapped into additional tuple...
        data = res.rpos
        len, data = msgpack.decode_array_header(data, res:size())
        if len ~= 1 then
            return lerror.make("Unexpected non-map result len: " .. len)
        end
    end
    len, data = msgpack.decode_array_header(data, res:size())
    -- When we use replicaset:callrw, result maybe wrapped in
    -- additional tuple if multireturn is not supported by this
    -- tarantool version. In particular, this happens in
    -- cartridge integration tests
    if not map_format and not helper.is_iproto_multireturn_supported() then
        len, data = msgpack.decode_array_header(data, res:size())
    end
    if len ~= 3 then
        return lerror.make("Unexpected dql tuple len: " .. len)
    end
    res.rpos = data

    rows_cnt, _ = msgpack.decode_unchecked(data)

    self.vtable_row_count = self.vtable_row_count + rows_cnt
    if self.vtable_max_rows ~= 0 and self.vtable_max_rows < self.vtable_row_count then
        return lerror.make(helper.vtable_limit_exceeded(self.vtable_max_rows, self.vtable_row_count))
    end
    return nil
end

--
-- Helper function to execute read request on multiple storages,
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
-- @param vtable_max_rows Maximum number of rows to receive from storages.
-- If the number is exceeded, the results are discarded and error is returned.
-- @param opts Table which may have the following options:
--  1. timeout - timeout for the whole function execution.
--  2. check_bucket_count - whether or not to check that ref stage has covered
--  all buckets. If this is true, the error will be returned if router's bucket
--  count != bucket count covered by ref stage. It may mean that router has
--  outdated configuration (some buckets were added/deleted on storages that router
--  does not know about). This option should be used only if you intend to execute
--  the function on all replicasets and want to ensure that all buckets were covered.
--
-- @return mapping between replicaset uuid and ibuf containing result
--
local function multi_storage_dql(uuid_to_args, func, handler, opts, tier_name)
    local router = get_router_for_tier(tier_name)
    local replicasets = router:routeall()
    local timeout, check_bucket_count
    local res_map = {}
    for uuid, _ in pairs(uuid_to_args) do
        res_map[uuid] = buffer.ibuf()
    end
    if opts then
        timeout = opts.timeout or DQL_MIN_TIMEOUT
        check_bucket_count = opts.check_bucket_count or false
    else
        timeout = DQL_MIN_TIMEOUT
        check_bucket_count = false
    end

    local err, err_uuid, res
    local futures = {}
    local bucket_count = 0
    local opts_ref = { is_async = true }
    local opts_map = { is_async = true, skip_header = true }
    local rs_count = 0
    local rid = ref_id
    local deadline = fiber.clock() + timeout
    ref_id = rid + 1
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
        res, err = rs:callrw('vshard.storage._call',
            { 'storage_ref', rid, timeout }, opts_ref)
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
        bucket_count = bucket_count + res
        timeout = deadline - fiber.clock()
    end

    -- All refs are done but not all buckets are covered. This is odd and can
    -- mean many things. The most possible ones: 1) outdated configuration on
    -- the router and it does not see another replicaset with more buckets,
    -- 2) some buckets are simply lost or duplicated - could happen as a bug, or
    -- if the user does a maintenance of some kind by creating/deleting buckets.
    -- In both cases can't guarantee all the data would be covered by Map calls.
    if check_bucket_count and bucket_count ~= router:bucket_count() then
        err = string.format("Expected %d buckets in the cluster, got %d",
            router:bucket_count(), bucket_count)
        goto fail
    end

    -- Map stage: send.
    --
    for uuid, rs_args in pairs(uuid_to_args) do
        local rs = replicasets[uuid]
        local args = { 'storage_map', rid, helper.proc_call_fn_name(), prepare_args(rs_args, func) }
        opts_map['buffer'] = res_map[uuid]
        res, err = rs:callrw('vshard.storage._call', args, opts_map)
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
        err = handler:process_res(res_map[uuid], true)
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
        f = replicasets[uuid]:callrw('vshard.storage._call',
            { 'storage_unref', rid }, opts_ref)
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

_G.group_buckets_by_replicasets = function(buckets, tier_name)
    local map = {}
    local router = get_router_for_tier(tier_name)
    for _, bucket_id in pairs(buckets) do
        local rs, err = router:route(bucket_id)
        if err ~= nil then
            error(err)
        end
        local uuid = rs.uuid
        if map[uuid] then
            table.insert(map[uuid], bucket_id)
        else
            map[uuid] = { bucket_id }
        end
    end

    return map
end

_G.get_replicasets_from_buckets = function(buckets, tier_name)
    local map = {}
    local router = get_router_for_tier(tier_name)
    for _, bucket_id in pairs(buckets) do
        local rs, err = router:route(bucket_id)
        if err ~= nil then
            error(err)
        end
        local uuid = rs.uuid
        if not map[uuid] then
            table.insert(map, uuid)
        end
    end

    return map
end

-- Execute dml query on replicasets masters.
--
-- @param uuid_to_args mapping between replicaset uuid and
-- corresponding arguments for stored procedure
-- @param waiting_timeout timeout in seconds for whole
-- function.
--
_G.dispatch_dml = function(uuid_to_args, waiting_timeout, tier_name)
    local result = nil
    local exec_fn = helper.proc_fn_name("proc_sql_execute")
    local futures = {}
    local router = get_router_for_tier(tier_name)

    for rs_uuid, args in pairs(uuid_to_args) do
        local replicasets = router:routeall()
        local replica = replicasets[rs_uuid]
        local required = args["required"]
        local optional = args["optional"]
        local future, err = replica:callrw(
            exec_fn,
            { required, optional, 'cacheable-2' },
            { is_async = true }
        )
        if err ~= nil then
            error(err)
        end
        table.insert(futures, future)
    end

    for _, future in ipairs(futures) do
        local res, err = future:wait_result(waiting_timeout)
        if err ~= nil then
            error(err)
        end

        -- proc_sql_execute returns [{rows_count = ..}] tuple.
        -- But this rust function is wrapped with proc macros that
        -- add an additional layer of array (see ReturnMsgpack).
        local next_result = helper.unwrap_execute_result(res[1][1])
        if result == nil then
            result = next_result
        else
            result.row_count = result.row_count + next_result.row_count
        end
    end

    return box.tuple.new { result }
end

-- Execute DML query on given replicasets
-- using the same plan for each replicaset
--
-- @param args to pass to stored procedure that will be called
-- on each replicaset
-- @param uuids replicasets uuids on which to execute plan
-- @param waiting_timeout timeout in seconds for whole
-- function.
--
_G.dispatch_dml_single_plan = function(args, uuids, waiting_timeout, tier_name)
    if not next(uuids) then
        local uuid_to_rs = get_replicasets_from_tier(tier_name)
        for uuid, _ in pairs(uuid_to_rs) do
            table.insert(uuids, uuid)
        end
    end
    local uuid_to_args = {}
    for _, uuid in pairs(uuids) do
        uuid_to_args[uuid] = args
    end

    return _G.dispatch_dml(uuid_to_args, waiting_timeout, tier_name)
end

-- Execute DQL query on given replicasets and handle results.
-- For each result we check that total number of rows does
-- not exceed given limit.
--
-- If there is a single replicaset, this function just calls
-- stored procedure on that replicaset. Otherwise we use 2 stage
-- protocol to avoid bucket moves between replicasets, see
-- multi_storage_dql
--
-- @param uuid_to_args mapping between replicaset uuid and
-- corresponding arguments for stored procedure
-- @param waiting_timeout timeout in seconds for whole
-- function.
-- @param row_count initial value for the number of rows
-- @param vtable_max_rows maximum number of rows
--
-- @return mapping between replicaset uuid and ibuf containing
-- result
--
_G.dispatch_dql = function(uuid_to_args, waiting_timeout, row_count, vtable_max_rows, check_bucket_count, tier_name)
    local result

    local result_handler = ResultHandler:new { vtable_row_count = row_count, vtable_max_rows = vtable_max_rows }
    local exec_fn = helper.proc_fn_name("proc_sql_execute")
    local router = get_router_for_tier(tier_name)

    if helper.table_size(uuid_to_args) == 1 then
        -- When read request is executed only on one
        -- storage, we don't care about bucket rebalancing.
        local rs_uuid, rs_args = next(uuid_to_args)
        result = {}
        result[rs_uuid] = buffer.ibuf()
        local call_opts = {
            is_async = true,
            buffer = result[rs_uuid],
            skip_header = true
        }
        local replica = router:routeall()[rs_uuid]
        local future, err, _
        local args = prepare_args(rs_args)
        future, err = replica:callbre(
            exec_fn,
            args,
            call_opts
        )
        if err ~= nil then
            error(err)
        end
        _, err = future:wait_result(waiting_timeout)
        if err ~= nil then
            error(err)
        end
        err = result_handler:process_res(result[rs_uuid], false)
        if err ~= nil then
            error(err)
        end
    else
        local err, err_uuid
        local opts = {
            timeout = waiting_timeout,
            check_bucket_count = check_bucket_count or false
        }
        result, err, err_uuid = multi_storage_dql(uuid_to_args, exec_fn, result_handler, opts, tier_name)
        if err ~= nil then
            helper.dql_error(err, err_uuid)
        end
    end

    return result
end

-- Execute DQL query on given replicasets
-- using the same plan for each replicaset
--
-- @param args to pass to stored procedure that will be called
-- on each replicaset
-- @param uuids replicasets uuids on which to execute plan
-- @param waiting_timeout timeout in seconds for whole
-- function.
-- @param row_count initial value for the number of rows
-- @param vtable_max_rows maximum number of rows
--
_G.dispatch_dql_single_plan = function(args, uuids, waiting_timeout, row_count, vtable_max_rows, tier_name)
    local check_bucket_count = false
    if not next(uuids) then
        -- empty list of uuids means execute on all replicasets
        local uuid_to_rs = get_replicasets_from_tier(tier_name)
        for uuid, _ in pairs(uuid_to_rs) do
            table.insert(uuids, uuid)
        end
        check_bucket_count = true
    end

    local uuid_to_args = {}
    for _, uuid in pairs(uuids) do
        uuid_to_args[uuid] = args
    end
    return _G.dispatch_dql(uuid_to_args, waiting_timeout, row_count, vtable_max_rows, check_bucket_count, tier_name)
end
