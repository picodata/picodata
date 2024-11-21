local sbroad_common = require('sbroad.init')
local sbroad_router = require('sbroad.router')

local function init(opts) -- luacheck: no unused args

    if rawget(_G, 'sbroad') == nil then
        rawset(_G, 'sbroad', {})
    end

    _G.sbroad.calculate_bucket_id = sbroad_common.calculate_bucket_id
    _G.sbroad.execute = sbroad_router.execute

    sbroad_common.init(opts.is_master)

    return true
end

local function apply_config(conf, opts) -- luacheck: no unused args
    return sbroad_router.invalidate_cache()
end

return {
    role_name = 'sbroad-router',
    init = init,
    apply_config = apply_config,
    dependencies = {'cartridge.roles.vshard-router'}
}
