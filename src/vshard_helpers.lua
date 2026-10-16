local pico = _G.pico
local fiber = require('fiber')
local log = require('log')

function pico.get_router_for_tier(tier_name)
    local router = pico.router[tier_name]
    if router == nil then
        error(string.format("no router found for tier '%s'", tier_name))
    end

    return router
end

local get_router_for_tier = pico.get_router_for_tier

function pico._ddl_map_callrw(tier, timeout, function_name, args)
    local router = get_router_for_tier(tier)

    -- Default timeout is 0.5 seconds.
    local res, err, uuid = router:map_callrw(function_name, args, { timeout = timeout })
    if not res then
        error(err)
    else
        return res
    end
end


function pico._replicaset_priority_list(tier, replicaset_uuid)
    local router = get_router_for_tier(tier)
    local replicaset = router.replicasets[replicaset_uuid]
    if replicaset == nil then
        error(vshard.error.vshard(vshard.error.code.NO_SUCH_REPLICASET, replicaset_uuid))
    end
    local closest_replica = replicaset.replica
    local priority_list = replicaset.priority_list
    local result = {}
    -- Make sure the closest replica which vshard uses for most communication is
    -- at the top of the priority list (this may not be the case if there were
    -- communication failure to the replica with top priority and one with a
    -- lower priority became the closest replica)
    if closest_replica ~= nil then
        table.insert(result, closest_replica.name)
    end
    for _, replica in ipairs(priority_list) do
        if replica ~= closest_replica then
            table.insert(result, replica.name)
        end
    end
    return result
end

function pico._replicaset_uuid_by_bucket_id(tier, bucket_id)
    local router = get_router_for_tier(tier)
    local replicaset, err = router:route(bucket_id)
    if replicaset == nil then
        error(err)
    end
    return replicaset.uuid
end

local lsched = require('vshard.storage.sched')

-- Whether we are holding a place in vshard's move queue. resharding_loop is the
-- only thing which moves buckets here and it wants at most one move at a time,
-- so one flag says it, and it is what makes `pico._sched_move_enqueue`
-- idempotent: the loop re-decides what to do on every iteration and asks for the
-- move again each time, and it must not take a second place in the queue for it.
local move_enqueued = false

function pico._sched_move_enqueue()
    if not move_enqueued then
        lsched.move_enqueue()
        move_enqueued = true
    end
end

-- Wait for our turn, for at most `timeout` seconds. Not getting it is an
-- ordinary outcome and not an error: we keep our place in the queue and the
-- caller comes back to decide whether it still wants the move.
function pico._sched_move_wait(timeout)
    local res = lsched.move_wait(timeout)
    if res ~= nil then
        -- Our place in the queue turned into the move itself.
        move_enqueued = false
    end
    return lsched.move_count
end

function pico._sched_move_dequeue()
    if move_enqueued then
        lsched.move_dequeue()
        move_enqueued = false
    end
    return lsched.move_count
end

function pico._sched_move_end()
    lsched.move_end(1)
    return lsched.move_count
end

-- One of "none", "enqueued", "started".
function pico._sched_move_state()
    -- We're assuming that only one move can be started at a time and that only
    -- we are starting these moves.
    if lsched.move_count > 0 then
        return "started"
    end
    if move_enqueued then
        return "enqueued"
    end
    return "none"
end

local lstorage = require('vshard.storage')
pico._vshard_storage_internal = lstorage.internal

-- Wait for vshard router discovery to complete.
-- This ensures route_map is up-to-date on the current instance and the
-- router's part of rebalancing is done.
function pico.wait_router_discovery_complete(tier_name, timeout)
    local router = get_router_for_tier(tier_name)

    local total = router.total_bucket_count
    if total == 0 then
        -- No buckets configured, nothing to wait for
        return true
    end

    local last_logged_bucket_count = 0
    local deadline = fiber.clock() + timeout

    -- Wait for discovery to complete
    while true do
        if router.known_bucket_count == total then
            break
        end

        if fiber.clock() >= deadline then
            return false
        end

        if router.known_bucket_count ~= last_logged_bucket_count then
            log.verbose("pico.router.%s.known_bucket_count = %d", tier_name, router.known_bucket_count)
            last_logged_bucket_count = router.known_bucket_count
        end

        -- Make sure the discovery fiber is not asleep so the progress is made
        router:discovery_wakeup()

        -- TODO(sharding): we would want to block on some kind of fiber_cond but
        -- there's no appropriate cond in vshard at the moment. This code is
        -- temporary anyway until we move the route_map into rust code
        fiber.sleep(0.5)
    end

    log.verbose("pico.wait_router_discovery_complete: discovered %d/%d buckets for tier %s",
        router.known_bucket_count, total, tier_name)

    return true
end
