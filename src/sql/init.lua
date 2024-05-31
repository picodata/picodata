require('sbroad.core-router')
require('sbroad.core-storage')
local helper = require('sbroad.helper')
local utils = require('internal.utils')
local check_param_table = utils.check_param_table

local function sql(...)
    local n_args = select("#", ...)
    if n_args == 0 or n_args > 3 then
	return nil, "Usage: sql(query[, params, options])"
    end
    local query, params, options = ...
    if type(query) ~= "string" then
	return nil, "SQL query must be a string"
    end
    if params ~= nil and type(params) ~= "table" then
	return nil, "SQL params must be a table"
    end
    if options ~= nil and type(options) ~= "table" then
	return nil, "SQL options must be a table"
    end
    check_param_table(options, {
        query_id = 'string',
        traceable = 'boolean',
    })

    local query_id = box.NULL
    if options ~= nil then
  query_id = options.query_id or box.NULL
    end

    local tracer = "stat"
    if options ~= nil and options.traceable == true then
	tracer = "test"
    end

    local ok, res = pcall(
        function()
            return box.func[".dispatch_query"]:call({
                query, params, box.NULL, query_id, tracer
	    })
        end
    )

    if ok == false then
        return nil, tostring(res)
    end

    return helper.format_result(res[1])
end

return {
    sql	= sql,
}
