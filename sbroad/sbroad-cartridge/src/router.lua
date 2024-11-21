require('strict').on()
require('sbroad')
require('sbroad.core-router')
local helper = require('sbroad.helper')

local cartridge = require('cartridge')
local rust = require("sbroad.rust")

_G.get_schema = function()
    return cartridge.get_schema()
end

_G.get_waiting_timeout = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["executor_waiting_timeout"] == nil then
        return 0
    end

    return cfg["executor_waiting_timeout"]
end

_G.get_router_cache_capacity = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["router_cache_capacity"] == nil then
        return 50
    end

    return cfg["router_cache_capacity"]
end

_G.get_sharding_column = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["executor_sharding_column"] == nil then
        return "bucket_id"
    end

    return cfg["executor_sharding_column"]
end

local function execute(query, params)
    local result, err = rust.dispatch_query(query, params)
    if err then
      return nil, err
    end
    return helper.format_result(result[1])
end

local function invalidate_cache(...)
    return rust.invalidate_coordinator_cache(...)
end

return {
    invalidate_cache = invalidate_cache,
    execute = execute,
}
