local function crud_sharding_func(shard_val)
    if type(shard_val) ~= 'table' then
        -- luacheck: ignore sbroad
        return sbroad.calculate_bucket_id(tostring(shard_val))
    end

    local string_value = ''
    for _, v in ipairs(shard_val) do
        string_value = string_value .. tostring(v)
    end

    -- luacheck: ignore sbroad
    return sbroad.calculate_bucket_id(string_value)

end

return {
  crud_sharding_func = crud_sharding_func
}
