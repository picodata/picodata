local t = require('luatest')
local h = require('test.helper')
local g = t.group()

local fio = require('fio')

g.before_all(function()
    g.data_dir = fio.tempdir()
    local peer = {'127.0.0.1:13301', '127.0.0.1:13302'}

    g.cluster = {
        i1 = h.Picodata:new({
            name = 'i1',
            data_dir = g.data_dir .. '/i1',
            listen = '127.0.0.1:13301',
            peer = peer,
            env = {PICODATA_RAFT_ID = "1"},
        }),
        i2 = h.Picodata:new({
            name = 'i2',
            data_dir = g.data_dir .. '/i2',
            listen = '127.0.0.1:13302',
            peer = peer,
            env = {PICODATA_RAFT_ID = "2"},
        }),
    }

    for _, node in pairs(g.cluster) do
        node:start()
    end
end)

g.after_all(function()
    for _, node in pairs(g.cluster) do
        node:stop()
    end
    fio.rmtree(g.data_dir)
end)

g.test_follower_proposal = function()
    -- Speed up node election
    g.cluster.i1:try_promote()

    t.assert_equals(
        g.cluster.i2:raft_propose_eval(1, '_G.check = box.info.listen'),
        true
    )
    t.assert_equals(
        g.cluster.i1:connect():eval('return check'),
        '127.0.0.1:13301'
    )
    t.assert_equals(
        g.cluster.i2:connect():eval('return check'),
        '127.0.0.1:13302'
    )
end

g.test_failover = function()
    g.cluster.i1:try_promote()
    h.retrying({}, function()
        g.cluster.i2:assert_raft_status("Follower", 1)
    end)

    -- Speed up election timeout
    g.cluster.i2:connect():eval([[
        while picolib.raft_status().raft_state == 'Follower' do
            picolib.raft_tick(1)
        end
    ]])

    h.retrying({}, function()
        g.cluster.i1:assert_raft_status("Follower", 2)
        g.cluster.i2:assert_raft_status("Leader")
    end)
end
