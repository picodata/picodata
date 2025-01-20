local ddl = require('ddl')
local vshard = require('vshard')

_G.get_current_schema = nil

local function get_current_schema()
    return ddl.get_schema()
end

local function init(opts) -- luacheck: no unused args
    _G.get_current_schema = get_current_schema
    return true
end

return {
    role_name = 'app.roles.storage',
    init = init,
    apply_config = function(conf, opts) -- luacheck: no unused args
        vshard.consts.CALL_TIMEOUT_MIN = 5
    end,
    dependencies = {
        'cartridge.roles.sbroad-storage',
        'cartridge.roles.crud-storage',
    },
}
