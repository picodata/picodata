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

function pico.vshard_bucket_generation_increment()
    pico._vshard_storage_internal.bucket_generation_increment()
end
