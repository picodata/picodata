local fio = require("fio")
local yaml = require("yaml")
local errno = require("errno")

local function get_init_config(path)
    local config_path = fio.pathjoin(path, 'test', 'data', 'config.yml')
    if not fio.path.exists(config_path) then
        return nil, error(string.format("No such file: %s", config_path))
    end

    local file, error = fio.open(config_path, "O_RDONLY")

    if error ~= nil then
        return nil, error
    end

    local buf = {}
    while true do
        local val = file:read(1024)
        if val == nil then
            return nil, error(string.format("Failed to read from file %s: %s", config_path, errno.strerror()))
        elseif val == "" then
            break
        end
        table.insert(buf, val)
    end
    file:close()
    local config_yml =  table.concat(buf, "")
    local config = yaml.decode(config_yml)

    return config
end

return {
    get_init_config = get_init_config,
}