require('sbroad.core-router')
require('sbroad.core-storage')
local helper = require('sbroad.helper')
local ffi = require('ffi')

local function check_descriptor(descriptor)
    local desc_type = type(descriptor)
    if desc_type == "number" or desc_type == "cdata" and ffi.sizeof(descriptor) == 8 then
	return true
    end
    return false
end

local function pg_bind(...)
    local n_args = select("#", ...)
    if n_args == 0 or n_args > 3 then
	return nil, "Usage: pg_bind(descriptor, params[, traceable])"
    end
    local descriptor, params, traceable = ...
    if not check_descriptor(descriptor) then
	return nil, "descriptor must be a number"
    end
    if params ~= nil and type(params) ~= "table" then
	return nil, "parameters must be a table"
    end
    if traceable ~= nil and type(traceable) ~= "boolean" then
	return nil, "trace flag must be a boolean"
    end
    if traceable == nil then
	traceable = false
    end

    local ok, err = pcall(
	function()
	    return box.func[".proc_pg_bind"]:call({ descriptor, params, traceable })
	end
    )

    if ok == false then
        return nil, err
    end

    return true
end

local function pg_close(descriptor)
    if not check_descriptor(descriptor) then
	return nil, "descriptor must be a number"
    end
    local ok, err = pcall(
	function()
	    return box.func[".proc_pg_close"]:call({ descriptor })
	end
    )

    if ok == false then
        return nil, err
    end

    return true
end

local function pg_describe(...)
    local n_args = select("#", ...)
    if n_args == 0 or n_args > 2 then
	return nil, "Usage: pg_describe(descriptor[, traceable])"
    end
    local descriptor, traceable = ...
    if not check_descriptor(descriptor) then
	return nil, "descriptor must be a number"
    end
    if traceable ~= nil and type(traceable) ~= "boolean" then
	return nil, "trace flag must be a boolean"
    end
    if traceable == nil then
	traceable = false
    end

    local ok, res = pcall(
	function()
	    return box.func[".proc_pg_describe"]:call({ descriptor, traceable })
	end
    )

    if ok == false then
        return nil, res
    end

    return helper.format_result(res)
end

local function pg_execute(...)
    local n_args = select("#", ...)
    if n_args == 0 or n_args > 2 then
	return nil, "Usage: pg_execute(descriptor[, traceable])"
    end
    local descriptor, traceable = ...
    if not check_descriptor(descriptor) then
	return nil, "descriptor must be a number"
    end
    if traceable ~= nil and type(traceable) ~= "boolean" then
	return nil, "trace flag must be a boolean"
    end
    if traceable == nil then
	traceable = false
    end

    local ok, res = pcall(
	function()
	    return box.func[".proc_pg_execute"]:call({ descriptor, traceable })
	end
    )

    if ok == false then
        return nil, res
    end

    return helper.format_result(res[1])
end

local function pg_parse(...)
    local n_args = select("#", ...)
    if n_args == 0 or n_args > 2 then
	return nil, "Usage: pg_parse(query[, traceable])"
    end
    local query, traceable = ...
    if type(query) ~= "string" then
	return nil, "query pattern must be a string"
    end
    if traceable ~= nil and type(traceable) ~= "boolean" then
	return nil, "trace flag must be a boolean"
    end
    if traceable == nil then
	traceable = false
    end

    local ok, res = pcall(
	function()
	    return box.func[".proc_pg_parse"]:call({ query, traceable })
	end
    )

    if ok == false then
        return nil, res
    end

    return tonumber(res)
end

local function pg_portals()
    local ok, res = pcall(
	function()
	    return box.func[".proc_pg_portals"]:call({})
	end
    )

    if ok == false then
        return nil, res
    end

    return helper.format_result(res)
end

return {
    pg_bind = pg_bind,
    pg_close = pg_close,
    pg_describe = pg_describe,
    pg_execute = pg_execute,
    pg_parse = pg_parse,
    pg_portals = pg_portals,
}
