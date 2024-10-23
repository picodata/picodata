require('sbroad.core-router')
local helper = require('sbroad.helper')
local utils = require('internal.utils')
local check_param_table = utils.check_param_table

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

    if params == nil then
        params = {}
    end

    local ok, res = pcall(
        function()
            return box.func[".proc_sql_dispatch"]:call({
                query, params
	    })
        end
    )

    if ok == false then
        if res.code == box.error.SQL_SYNTAX_NEAR_TOKEN then
            res = tostring(res)
        end
        return nil, res
    end

    return helper.format_result(res[1])
end

return {
    sql	= sql,
}
