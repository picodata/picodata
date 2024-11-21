local ddl = require('ddl')

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
    dependencies = {
        'cartridge.roles.sbroad-storage',
        'cartridge.roles.crud-storage',
    },
}
