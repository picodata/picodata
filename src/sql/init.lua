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

local function sql(query, params)
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
