local dt = require('datetime')

-- Builtin sbroad funcs implemented in LUA
local builtins = {}

builtins._PICO_BUCKET = function(tier)
    local op = "_pico_bucket"

    if tier == nil then
        return error(("%s: tier name is null"):format(op))
    end

    local info = _G.pico.router[tier]
    if info == nil then
        return error(("%s: tier '%s' doesn't exist"):format(op, tier))
    end

    local max_bucket_id = info:bucket_count()
    local result = {}

    for bucket_id = 1, max_bucket_id do
        local rs, err = info:route(bucket_id)
        if rs == nil then
            return error(("%s: %s"):format(op, err.message))
        end

        local last = result[#result]
        if last
            and rs.uuid == last.current_replicaset_uuid
            and bucket_id == last.bucket_id_end + 1
        then
            -- extend the existing range
            last.bucket_id_end = bucket_id
        else
            -- create a new entry and record it as the last for this uuid
            local entry = {
                tier_name               = tier,
                bucket_id_start         = bucket_id,
                bucket_id_end           = bucket_id,
                state                   = 'active',
                current_replicaset_uuid = rs.uuid,
                target_replicaset_uuid  = rs.uuid,
            }

            result[#result + 1] = entry
        end
    end

    return result
end

builtins.TO_DATE = function(s, fmt)
    if s == nil or fmt == nil then
        return nil
    end
    local opts = {}
    if fmt ~= '' then
        opts = { format = fmt }
    end
    -- ignore the second returned value
    local res = dt.parse(s, opts)
    -- we must return only date part
    res:set({ hour = 0, min = 0, sec = 0, nsec = 0 })
    return res
end

builtins.TO_CHAR = function(date, fmt)
    local res
    if date and fmt then
        res = date:format(fmt)
    else
        res = nil
    end
    return res
end

builtins.SUBSTRING_TO_REGEXP = function(string, pattern, expr)
    -- Check for NULL parameters
    if string == nil or pattern == nil or expr == nil then
        return nil
    end

    -- Call TO_REGEXP and handle its return values
    local new_pattern = builtins.TO_REGEXP(pattern, expr)

    -- Call SUBSTRING with the new pattern
    return builtins.SUBSTRING(string, new_pattern)
end



local function init()
    local module = 'pico'

    if rawget(_G, module) == nil then
        error('buitins must be initialized after app module was set!')
    end

    _G[module].builtins = builtins

    body = string.format("function(...) return %s.builtins._PICO_BUCKET(...) end",
        module)
    box.schema.func.create("_pico_bucket", {
        language = 'LUA',
        returns = 'ARRAY',
        body = body,
        param_list = { 'string' },
        exports = { 'SQL' },
        is_deterministic = false,
        if_not_exists = true
    })

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
        param_list = { 'string', 'string' },
        exports = { 'SQL' },
        is_deterministic = true,
        if_not_exists = true
    })

    body = string.format("function(...) return %s.builtins.TO_CHAR(...) end",
        module)
    box.schema.func.create("to_char", {
        language = 'LUA',
        returns = 'string',
        body = body,
        param_list = { 'datetime', 'string' },
        exports = { 'SQL' },
        is_deterministic = true,
        if_not_exists = true
    })

    body = string.format("function(...) return %s.builtins.SUBSTRING(...) end",
        module)
    box.schema.func.create("substring", {
        language = 'LUA',
        returns = 'string',
        body = body,
        param_list = { 'string', 'string' },
        exports = { 'SQL' },
        is_deterministic = true,
        if_not_exists = true
    })

    body = string.format("function(...) return %s.builtins.SUBSTRING_TO_REGEXP(...) end",
        module)
    box.schema.func.create("substring_to_regexp", {
        language = 'LUA',
        returns = 'string',
        body = body,
        param_list = { 'string', 'string', 'string' },
        exports = { 'SQL' },
        is_deterministic = true,
        if_not_exists = true
    })
end

return {
    init = init,
}
