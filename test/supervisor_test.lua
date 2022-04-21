local t = require('luatest')
local h = require('test.helper')
local g = t.group()

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

g.before_each(function()
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
    t.assert_equals(#g.children, 2, "something wrong with pgrep")
end)

g.after_each(function()
    for _, pid in ipairs(g.children) do
        t.Process.kill_pid(pid, 9, {quiet = true})
    end
    fio.rmtree(g.data_dir)
end)

g.test_sigkill = function()
    g.node.process:kill('KILL')
    log.warn("Sent SIGKILL to the supervisor")

    h.retrying({}, function()
        for i, pid in ipairs(g.children) do
            t.assert_not(
                t.Process.is_pid_alive(pid),
                string.format("child #%d (pid %s) didn't die", i, pid)
            )
        end
    end)
end

g.test_sigint = function()
    t.Process.kill_pid(g.children[2], 'STOP')

    -- Signal the supervisor and give it some time to handle one.
    -- Without a sleep the next assertion is useless. Unfortunately,
    -- there're no alternatives to sleep, because the signal
    -- delivery is a mystery of the kernel.
    g.node.process:kill('INT')
    log.warn("Sent SIGINT to the supervisor")
    require('fiber').sleep(0.1)

    -- We've signalled supervisor. It should forward the signal
    -- the child and keep waiting. But the child is stopped now,
    -- and can't handle the forwarded signal.
    -- Supervisor must still be alive.
    t.assert(g.node.process:is_alive(), "supervisor treminated prematurely")

    t.Process.kill_pid(g.children[2], 'CONT')

    h.retrying({}, function()
        for i, pid in ipairs(g.children) do
            t.assert_not(
                t.Process.is_pid_alive(pid),
                string.format("child #%d (pid %s) didn't die", i, pid)
            )
        end
    end)
end
