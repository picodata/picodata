local pico = {}
local help = {}
setmetatable(pico, help)

local fiber = require 'fiber'
local log = require 'log'
local utils = require('internal.utils')

local check_param = utils.check_param
local check_param_table = utils.check_param_table

local intro = [[
pico.help([topic])
==================

Show built-in Picodata reference for the given topic.

Full Picodata documentation:

    https://docs.picodata.io/picodata/

Params:

    1. topic (optional string)

Returns:

    (string)
    or
    (nil) if topic not found

Example:

    picodata> pico.help("help")
    -- Shows this message

Topics:

]]

function pico.help(topic)
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

local TIMEOUT_INFINITY = 100 * 365 * 24 * 60 * 60

pico.router = {}

_G.pico = pico
package.loaded.pico = pico
