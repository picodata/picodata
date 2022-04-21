local t = require('luatest')
local h = require('test.helper')
local g = t.group()

local ffi = require('ffi')
local fio = require('fio')
local log = require('log')
local popen = require('popen')

local function pgrep_children(pid, result)
    pid = pid or require('tarantool').pid()
    result = result or {}

    local ps = t.assert(popen.shell('exec pgrep -P' .. pid, 'r'))
    for _, child in ipairs(ps:read():strip():split()) do
        table.insert(result, child)
        pgrep_children(child, result)
    end
    return result
end

g.before_test('test_sigkill', function()
    g.data_dir = fio.tempdir()

    g.node = h.Picodata:new({
            name = 'single',
            data_dir = g.data_dir,
            listen = '127.0.0.1:13301',
            peer = {'127.0.0.1:13301'},
    })
    g.node:start()
    g.node:wait_started()

    g.children = pgrep_children()
end)
g.test_sigkill = function()
    t.assert_equals(#g.children, 2, "something wrong with pgrep")

    g.node.process:kill(9)
    log.warn("supervisor killed with SIGKILL")

    h.retrying({}, function()
        for i, pid in ipairs(g.children) do
            t.assert_not(
                t.Process.is_pid_alive(pid),
                string.format("child #%d (pid %s) didn't die", i, pid)
            )
        end
    end)
end
g.after_test('test_sigkill', function()
    for _, pid in ipairs(g.children) do
        t.Process.kill_pid(pid, 9, {quiet = true})
    end
    fio.rmtree(g.data_dir)
end)

