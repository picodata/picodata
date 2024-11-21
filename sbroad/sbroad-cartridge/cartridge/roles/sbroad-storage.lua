local sbroad_common = require('sbroad.init')
local sbroad_storage = require('sbroad.storage')
local sbroad_builtins = require('sbroad.builtins')

local function init(opts) -- luacheck: no unused args
    if rawget(_G, 'sbroad') == nil then
        rawset(_G, 'sbroad', {})
    end

    _G.sbroad.calculate_bucket_id = sbroad_common.calculate_bucket_id

    sbroad_common.init(opts.is_master)
    if opts.is_master then
        sbroad_builtins.init()
    end

    return true
end

local function apply_config(conf, opts) -- luacheck: no unused args
    return sbroad_storage.invalidate_cache()
end

return {
    role_name = 'sbroad-storage',
    init = init,
    apply_config = apply_config,
    dependencies = {
        'cartridge.roles.vshard-storage',
        'cartridge.roles.vshard-router',
    },
}
