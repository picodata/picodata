local errors = require("errors")
local helper = require("sbroad.helper")

local RustError = errors.new_class("RustError")

local rust_funcs = {
	"calculate_bucket_id",
	"invalidate_segment_cache",
	"invalidate_coordinator_cache",
	"dispatch_query",
  "proc_sql_execute",
}

--- Generates convenient wrappers over `box.lib`
local api = {}
--- Those procs wrappers are intended to be called directly via vshard call, not via `storage_map` and `call_proc`.
--- NOTE: they wrap result in an exotic way in order to satisfy IPROTO format.
local iproto_compat = {}

local function call_proc(func_name, ...)
    if not func_name then
      error('"call_proc" expects at least 1 argument - function name')
    end
    func_name = func_name:split(".")
    func_name = func_name[#func_name]
    local result, err = api[func_name](...)
    -- we throw error so `storage_map` will see it, simple return won't work.
    if err then
      error(tostring(err))
    end
    return result
end

---Init rust module
---@return boolean?, table?
api.init = function()
	-- luacheck: ignore
	if type(box.cfg) == "function" then
		error(
			"Rust init must be called after box.cfg. " .. 'Probably you called it on the fresh "validate_config" call'
		)
	end
	local module = box.lib.load("libsbroad")

	for _, func_name in ipairs(rust_funcs) do
		local func = module:load(func_name)
		api[func_name] = function(...)
			local result, err = RustError:pcall(func, ...)
      if err then
        return nil, err.err
      end
      return result
		end
		iproto_compat[func_name] = function(...)
			local result, err = RustError:pcall(func, ...)
      if err then
        error(tostring(err.err))
      end
      if helper.is_iproto_multireturn_supported() then
        return result
      end
      -- We wrap into array, just as expected old behavior of IPROTO.
      -- The thing is that old behavior is only about calling C stored procs, not Lua.
      -- Since we wrap stored procs in Lua functions, we must mimic the same behavior ourselves.
      return {result}
		end
	end

  --- Mimics box.schema.func behavior, intended to be called via `storage_map`.
  --- As we don't use box.schema.func anymore, it is needed as a replacement for box.schema.func.call.
  _G.sbroad.call_proc = call_proc
  _G.sbroad.procs = iproto_compat

	return true, nil
end

--- Rust is only available after box.cfg.
api.is_available = function()
	return type(box.cfg) ~= "function"
end

return api
