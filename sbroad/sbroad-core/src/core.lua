local helper = require('sbroad.helper')

local function init_bucket()
    local exec_fn = helper.proc_fn_name("calculate_bucket_id")
    box.schema.func.create(
        exec_fn,
        { if_not_exists = true, language = 'C' }
    )
end

local function init()
    init_bucket()
end

return {
    init = init,
}
