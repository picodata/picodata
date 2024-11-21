local checks = require('checks')
local core = require('sbroad.core')
local rust = require("sbroad.rust")
local vshard = require("vshard")

local function get_router_for_tier(tier_name) -- luacheck: no unused args
    return vshard.router.static
end

local function init (is_master)
    if is_master then
        core.init()
    end
    rust.init()

    if rawget(_G, 'sbroad') == nil then
        rawset(_G, 'sbroad', {})
    end
    _G.sbroad.get_router_for_tier = get_router_for_tier
end

local function calculate_bucket_id(values, space_name) -- luacheck: no unused args
    checks('string|table', '?string')

    local has_err, result = pcall(function ()
       if type(values) == 'table' and space_name == nil then
           return false, error("space name is required")
       end

       return true
    end)

    if has_err == false then
        return nil, result
    end

    if space_name then
        return rust.calculate_bucket_id(values, space_name)
    end

    return rust.calculate_bucket_id(values)
end


return {
    init = init,
    calculate_bucket_id = calculate_bucket_id,
}
