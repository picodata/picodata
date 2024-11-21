require('strict').on()

local cartridge = require('cartridge')
local rust = require("sbroad.rust")

_G.get_storage_cache_capacity = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["storage_cache_capacity"] == nil then
        return 200
    end

    return cfg["storage_cache_capacity"]
end

_G.get_storage_cache_size_bytes = function()
    local cfg = cartridge.config_get_readonly()

    if cfg["storage_cache_size_bytes"] == nil then
        return 204800
    end

    return cfg["storage_cache_size_bytes"]
end

local function invalidate_cache(...)
    return rust.invalidate_segment_cache(...)
end

return {
    invalidate_cache = invalidate_cache,
}
