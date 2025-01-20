local cartridge = require('cartridge')
local yaml = require("yaml")
local checks = require('checks')
local vshard = require('vshard')

_G.set_schema = nil

local function set_schema(new_schema)
    checks('table|string')

    local schema_str = new_schema
    if type(new_schema) == 'table' then
        schema_str = yaml.encode(new_schema)
    end
    local _, err = cartridge.set_schema(schema_str)
    if err ~= nil then
        return err
    end

    return nil
end

local function init(opts) -- luacheck: no unused args
    _G.set_schema = set_schema

    return true
end

return {
    role_name = 'app.roles.api',
    init = init,
    apply_config = function(conf, opts) -- luacheck: no unused args
        vshard.consts.CALL_TIMEOUT_MIN = 5
    end,
    dependencies = {
        'cartridge.roles.sbroad-router',
        'cartridge.roles.crud-router',
    },
}
