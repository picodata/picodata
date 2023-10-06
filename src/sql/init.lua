require('sbroad.core-router')
require('sbroad.core-storage')
local helper = require('sbroad.helper')

local function sql(...)
    local n_args = select("#", ...)
    if n_args == 0 or n_args > 3 then
	return nil, "Usage: sql(query[, params, traceable])"
    end
    local query, params, traceable = ...
    if type(query) ~= "string" then
	return nil, "SQL query must be a string"
    end
    if params ~= nil and type(params) ~= "table" then
	return nil, "SQL params must be a table"
    end
    if traceable ~= nil and type(traceable) ~= "boolean" then
	return nil, "SQL trace flag must be a boolean"
    end

    local tracer = helper.constants.STAT_TRACER
    if traceable == true then
	tracer = helper.constants.TEST_TRACER
    end

    local ok, res = pcall(
        function()
            return box.func[".dispatch_query"]:call({
                query, params, box.NULL, box.NULL, tracer
	    })
        end
    )

    if ok == false then
        return nil, res
    end

    return helper.format_result(res[1])
end

return {
    sql	= sql,
}
