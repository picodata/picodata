local dt = require('datetime')
local helper = require('sbroad.helper')

-- Builtin sbroad funcs inplemented in LUA
local builtins = {}

builtins.TO_DATE = function (s, fmt)
    local opts = {}
    if fmt and fmt ~= '' then
      opts = { format = fmt }
    end
    -- ignore the second returned value
    local res = dt.parse(s, opts)
    -- we must return only date part
    res:set({hour=0, min=0, sec=0, nsec=0})
    return res
end

builtins.TO_CHAR = function (date, fmt)
  local res
  if fmt then
    res = date:format(fmt)
  else
    res = nil
  end
  return res
end

local function init()
  -- cartridge
  local module = 'sbroad'
  if helper.pico_compat() then
    -- picodata
    module = 'pico'
  end

  if rawget(_G, module) == nil then
    error('buitins must be initialized after app module was set!')
  end

  _G[module].builtins = builtins

  -- We don't want to make builtin functions global,
  -- so we put them in module. But for function
  -- to be callable from sql it should have a simple
  -- name, so for each func we provide a body.
  local body = string.format("function(...) return %s.builtins.TO_DATE(...) end",
  module)
  box.schema.func.create("to_date", {
      language = 'LUA',
      returns = 'datetime',
      body = body,
      param_list = {'string', 'string'},
      exports = {'SQL'},
      is_deterministic = true,
      if_not_exists=true
  })

  body = string.format("function(...) return %s.builtins.TO_CHAR(...) end",
  module)
  box.schema.func.create("to_char", {
      language = 'LUA',
      returns = 'string',
      body = body,
      param_list = {'datetime', 'string'},
      exports = {'SQL'},
      is_deterministic = true,
      if_not_exists=true
  })
end

return {
  init = init,
}
