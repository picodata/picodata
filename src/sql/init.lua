require('sbroad.core-router')
require('sbroad.core-storage')

local function trace(query, params, context, id)
    local has_err, parser_res = pcall(
        function()
            return box.func[".dispatch_query"]:call({ query, params, context, id, true })
        end
    )

    if has_err == false then
        return nil, parser_res
    end

    return parser_res[1]
end

local function sql(...)
    local n_args = select("#", ...)
    if n_args == 0 or n_args > 2 then
	return nil, "Usage: sql(query[, params])"
    end
    local query, params = ...
    if type(query) ~= "string" then
	return nil, "SQL query must be a string"
    end
    if params ~= nil and type(params) ~= "table" then
	return nil, "SQL params must be a table"
    end

    local has_err, parser_res = pcall(
        function()
            return box.func[".dispatch_query"]:call({ query, params, box.NULL, box.NULL, false })
        end
    )

    if has_err == false then
        return nil, parser_res
    end

    return parser_res[1]
end

return {
    sql	= sql,
    trace = trace,
}