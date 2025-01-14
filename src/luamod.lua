local pico = {}
local help = {}
setmetatable(pico, help)

local fiber = require 'fiber'
local log = require 'log'
local utils = require('internal.utils')

local check_param = utils.check_param
local check_param_table = utils.check_param_table

local intro = [[
pico.help([topic])
==================

Show built-in Picodata reference for the given topic.

Full Picodata documentation:

    https://docs.picodata.io/picodata/

Params:

    1. topic (optional string)

Returns:

    (string)
    or
    (nil) if topic not found

Example:

    picodata> pico.help("help")
    -- Shows this message

Topics:

]]

function pico.help(topic)
    if topic == nil or topic == "help" then
        local topics = {"    - help"}
        for k, _ in pairs(help) do
            table.insert(topics, "    - " .. k)
        end
        table.sort(topics)
        return intro .. table.concat(topics, "\n") .. "\n"
    else
        return help[topic]
    end
end

local TIMEOUT_INFINITY = 100 * 365 * 24 * 60 * 60

pico.router = {}

local function get_router_for_tier(tier_name)
    local router = pico.router[tier_name]
    if router == nil then
        error(string.format("no router found for tier '%s'", tier_name))
    end

    return router
end

pico.get_router_for_tier = get_router_for_tier


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

_G.pico = pico
package.loaded.pico = pico
