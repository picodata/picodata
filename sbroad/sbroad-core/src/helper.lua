local compat = require('compat')
local compat_mt = compat ~= nil and getmetatable(compat) or nil

--- Checks if building against picodata - mainly needed because stored procs should be used then.
local function pico_compat()
  return package.loaded["pico"] ~= nil
end

--- Make correct function name to be passed when executed via `proc_call_fn_name()`(see below).
local function proc_fn_name(func_name)
  if pico_compat() then
    return '.' .. func_name
  else
    return "sbroad.procs." .. func_name
  end
end

--- Function that calls other local functions whose name supplied as argument.
--- Picodata relies on stored procs as for now, so it's `box.schema.func.call`,
--- whereas cartridge impl relies on custom caller impl based on `box.lib`.
local function proc_call_fn_name()
    if pico_compat() then
	    return 'box.schema.func.call'
    else
	    return 'sbroad.call_proc'
    end
end

local function format_result(result)
    local formatted = setmetatable(result, { __serialize = nil })
    if formatted['available'] ~= nil then
	formatted.available = setmetatable(formatted.available, { __serialize = nil })
    end
    if formatted['metadata'] ~= nil then
	formatted.metadata = setmetatable(formatted.metadata, { __serialize = nil })
    end
    if formatted['rows'] ~= nil then
	formatted.rows = setmetatable(formatted.rows, { __serialize = nil })
    end

    return formatted
end

local function vtable_limit_exceeded(limit, current_val)
    return string.format("Exceeded maximum number of rows (%d) in virtual table: %d", limit, current_val)
end

local function dql_error(err, rs_uuid)
    if type(err) ~= 'table' and type(err) ~= 'string' then
        io.stderr:write(string.format("expected string or table, got: %s", type(err)))
        error(err)
    end
    if type(err) == 'table' then
        local meta_t = getmetatable(err)
        meta_t.__tostring = function (self)
            return self.message
        end
        err.uuid = rs_uuid
        setmetatable(err, meta_t)
    end
    error(err)
end

-- On older versions of tarantool there was a bug which made all FFI
-- stored procedures' wrap their return values into an additional
-- msgpack array (they call it "multireturn", but you couldn't put
-- multiple values in there, it was hard-coded to be 1 element). But
-- thankfully it was fixed and now we get to rewrite all of our code...
-- Yay!
-- See https://github.com/tarantool/tarantool/issues/4799
local function is_iproto_multireturn_supported()
  if compat_mt == nil then
    return false
  end

  -- We want to just call `compat.c_func_iproto_multireturn:is_new()`,
  -- but it throws an exception when the option is unknown.
  local ok, opt = pcall(compat_mt.__index, compat, 'c_func_iproto_multireturn')
  return ok and opt:is_new()
end

local function unwrap_execute_result(result)
    if is_iproto_multireturn_supported() then
      return result
    else
      return result[1]
    end
end

local function table_size(t)
  local count = 0
  for _, _ in pairs(t) do count = count + 1 end
  return count
end

return {
    is_iproto_multireturn_supported = is_iproto_multireturn_supported,
    pico_compat = pico_compat,
    proc_call_fn_name = proc_call_fn_name,
    proc_fn_name = proc_fn_name,
    vtable_limit_exceeded = vtable_limit_exceeded,
    dql_error = dql_error,
    format_result = format_result,
    unwrap_execute_result = unwrap_execute_result,
    table_size = table_size
}
