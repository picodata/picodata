require('sbroad.core-router')
require('sbroad.core-storage')
local helper = require('sbroad.helper')

local function verify_out_formats(formats)
    if type(formats) ~= "table" then
        return false
    end

    for _, format in ipairs(formats) do
        if format ~= 0 and format ~= 1 then
        return false
        end
    end

    return true
end

local function pg_bind(...)
    local n_args = select("#", ...)
    if n_args == 0 or n_args > 6 then
	return nil, "Usage: pg_bind(id, stmt_name, portal_name, params, out_formats[, traceable])"
    end
    local id, stmt_name, portal_name, params, out_formats, traceable = ...
    if type(id) ~= "number" or (type(id) == "number" and id < 0) then
    return nil, "id must be a non-negative number"
    end
    if type(stmt_name) ~= "string" then
    return nil, "name must be a string"
    end
    if type(portal_name) ~= "string" then
    return nil, "name must be a string"
    end
    if params ~= nil and type(params) ~= "table" then
	return nil, "parameters must be a table"
    end
    if not verify_out_formats(out_formats) then
    return nil, "out_formats must be an array of 0 and 1"
    end
    if traceable ~= nil and type(traceable) ~= "boolean" then
	return nil, "trace flag must be a boolean"
    end
    if traceable == nil then
	traceable = false
    end

    local ok, err = pcall(
	function()
	    return box.func[".proc_pg_bind"]:call({
            id, stmt_name, portal_name, params, out_formats, traceable
        })
	end
    )

    if ok == false then
        return nil, err
    end

    return true
end

local function pg_close_stmt(id, name)
    if type(id) ~= "number" or (type(id) == "number" and id < 0) then
    return nil, "id must be a non-negative number"
    end
    if type(name) ~= "string" then
    return nil, "name must be a string"
    end

    local ok, err = pcall(
	function()
	    return box.func[".proc_pg_close_stmt"]:call({ id, name })
	end
    )

    if ok == false then
        return nil, err
    end

    return true
end

local function pg_close_portal(id, name)
    if type(id) ~= "number" or (type(id) == "number" and id < 0) then
    return nil, "id must be a non-negative number"
    end
    if type(name) ~= "string" then
    return nil, "name must be a string"
    end

    local ok, err = pcall(
	function()
	    return box.func[".proc_pg_close_portal"]:call({ id, name })
	end
    )

    if ok == false then
        return nil, err
    end

    return true
end

local function pg_describe_stmt(...)
    local n_args = select("#", ...)
    if n_args == 0 or n_args > 2 then
	return nil, "Usage: pg_describe_stmt(id, name)"
    end
    local id, name = ...
    if type(id) ~= "number" or (type(id) == "number" and id < 0) then
    return nil, "id must be a non-negative number"
    end
    if type(name) ~= "string" then
    return nil, "name must be a string"
    end

    local ok, res = pcall(
	function()
	    return box.func[".proc_pg_describe_stmt"]:call({ id, name })
	end
    )

    if ok == false then
        return nil, res
    end

    return helper.format_result(res)
end

local function pg_describe_portal(...)
    local n_args = select("#", ...)
    if n_args == 0 or n_args > 3 then
	return nil, "Usage: pg_describe_portal(id, name)"
    end
    local id, name = ...
    if type(id) ~= "number" or (type(id) == "number" and id < 0) then
    return nil, "id must be a non-negative number"
    end
    if type(name) ~= "string" then
    return nil, "name must be a string"
    end

    local ok, res = pcall(
	function()
	    return box.func[".proc_pg_describe_portal"]:call({ id, name })
	end
    )

    if ok == false then
        return nil, res
    end

    return helper.format_result(res)
end

local function pg_execute(...)
    local n_args = select("#", ...)
    if n_args == 0 or n_args > 4 then
	return nil, "Usage: pg_execute(id, name, max_rows[, traceable])"
    end
    local id, name, max_rows, traceable = ...
    if type(id) ~= "number" or (type(id) == "number" and id < 0) then
    return nil, "id must be a non-negative number"
    end
    if type(name) ~= "string" then
    return nil, "name must be a string"
    end
    if type(max_rows) ~= "number" then
        return nil, "max_rows must be a number"
    end
    if traceable ~= nil and type(traceable) ~= "boolean" then
	return nil, "trace flag must be a boolean"
    end
    if traceable == nil then
	traceable = false
    end

    local ok, res = pcall(
	function()
	    return box.func[".proc_pg_execute"]:call({ id, name, max_rows, traceable })
	end
    )

    if ok == false then
        return nil, res
    end

    return helper.format_result(res[1])
end

local function verify_param_oids(param_oids)
    if type(param_oids) ~= "table" then
    return false
    end

    for _, oid in ipairs(param_oids) do
        if type(oid) ~= "number" or (type(oid) == "number" and oid < 0) then
        return false
        end
    end

    return true
end

local function pg_parse(...)
    local n_args = select("#", ...)
    if n_args == 0 or n_args > 5 then
	return nil, "Usage: pg_parse(id, name, query, param_oids, [, traceable])"
    end
    local id, name, query, param_oids, traceable = ...
    if type(id) ~= "number" or (type(id) == "number" and id < 0) then
    return nil, "id must be a non-negative number"
    end
    if type(name) ~= "string" then
    return nil, "name must be a string"
    end
    if type(query) ~= "string" then
	return nil, "query pattern must be a string"
    end
    if not verify_param_oids(param_oids) then
    return nil, "param_oids must be a list of non-negative integers"
    end
    if traceable ~= nil and type(traceable) ~= "boolean" then
	return nil, "trace flag must be a boolean"
    end
    if traceable == nil then
	traceable = false
    end

    local ok, err = pcall(
	function()
	    return box.func[".proc_pg_parse"]:call({
            id, name, query, param_oids, traceable
        })
	end
    )

    if ok == false then
        return nil, err
    end

    return true
end

local function pg_close_client_stmts(id)
    if type(id) ~= "number" or (type(id) == "number" and id < 0) then
    return nil, "id must be a non-negative number"
    end
    return pcall(
	function()
	    return box.func[".proc_pg_close_client_stmts"]:call({id})
	end
    )
end

local function pg_close_client_portals(id)
    if type(id) ~= "number" or (type(id) == "number" and id < 0) then
    return nil, "id must be a non-negative number"
    end
    return pcall(
	function()
	    return box.func[".proc_pg_close_client_portals"]:call({id})
	end
    )
end

local function pg_statements(id)
    if type(id) ~= "number" or (type(id) == "number" and id < 0) then
    return nil, "id must be a non-negative number"
    end
    local ok, res = pcall(
	function()
	    return box.func[".proc_pg_statements"]:call({id})
	end
    )

    if ok == false then
        return nil, res
    end

    return helper.format_result(res)
end

local function pg_portals(id)
    if type(id) ~= "number" or (type(id) == "number" and id < 0) then
    return nil, "id must be a non-negative number"
    end
    local ok, res = pcall(
	function()
	    return box.func[".proc_pg_portals"]:call({id})
	end
    )

    if ok == false then
        return nil, res
    end

    return helper.format_result(res)
end

return {
    pg_bind = pg_bind,
    pg_close_stmt = pg_close_stmt,
    pg_close_portal = pg_close_portal,
    pg_describe_stmt = pg_describe_stmt,
    pg_describe_portal = pg_describe_portal,
    pg_execute = pg_execute,
    pg_parse = pg_parse,
    pg_close_client_portals = pg_close_client_portals,
    pg_close_client_stmts = pg_close_client_stmts,
    pg_statements = pg_statements,
    pg_portals = pg_portals
}
