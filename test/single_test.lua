local t = require('luatest')
local h = require('test.helper')
local g = t.group()

local fio = require('fio')

g.before_all(function()
    g.data_dir = fio.tempdir()

    g.node = h.Picodata:new({
        name = 'single',
        data_dir = g.data_dir,
        listen = '127.0.0.1:13301',
        peer = {'127.0.0.1:13301'},
    })
    g.node:start()
end)

g.after_all(function()
    g.node:stop()
    fio.rmtree(g.data_dir)
end)

g.test = function()
    g.node:assert_raft_status("Follower")

    t.assert_equals(
        g.node:raft_propose_eval(1, 'return'),
        false -- No leader is elected yet
    )

    g.node:try_promote()

    t.assert_equals(
        g.node:raft_propose_eval(0, 'return'),
        false -- Timeout
    )

    t.assert_equals(
        g.node:raft_propose_eval(1, '_G.success = true'),
        true
    )
    t.assert_equals(
        g.node:connect():eval('return success'),
        true
    )
end
