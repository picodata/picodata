local M = {}
local help = {}
setmetatable(M, help)

local intro = [[
pico.help([topic])
===================

Show built-in Picodata reference for the given topic.

Full Picodata documentation:

    https://docs.picodata.io/picodata/

Example:

    tarantool> pico.help("help")
    -- Shows this message

Topics:

]]

function M.help(topic)
    if topic == nil or topic == "help" then
        local topics = {"    - help"}
        for k, _ in pairs(help) do
            table.insert(topics, "    - " .. k)
        end
        table.sort(topics)
        return intro .. table.concat(topics, "\n") .. "\n"
    else
        return help[topic]
    end
end

_G.pico = M
package.loaded.pico = M
