local t = require('luatest')
local h = require('test.helper')
local g = t.group()

local fio = require('fio')

g.before_all(function()
    g.data_dir = fio.tempdir()
    local peer = {
        '127.0.0.1:13301',
        '127.0.0.1:13302',
        '127.0.0.1:13303',
    }

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
        i3 = h.Picodata:new({
            name = 'i3',
            data_dir = g.data_dir .. '/i3',
            listen = '127.0.0.1:13303',
            peer = peer,
            env = {PICODATA_RAFT_ID = "3"},
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

local function propose_state_change(srv, value)
    -- It's just a boilerplate
    local code = string.format(
        'box.space.raft_state:put({"test-timeline", %q})',
        value
    )
    return srv:raft_propose_eval(0.1, code)
end

g.test = function()
    -- Speed up node election
    g.cluster.i1:try_promote()
    h.retrying({}, function()
        g.cluster.i2:assert_raft_status("Follower", 1)
        g.cluster.i3:assert_raft_status("Follower", 1)
    end)

    t.assert_equals(
        propose_state_change(g.cluster.i1, "i1 is a leader"),
        true
    )

    -- Simulate the network partitioning: i1 can't reach i2 and i3.
    g.cluster.i2:stop()
    g.cluster.i3:stop()

    t.assert_equals(
        propose_state_change(g.cluster.i1, "i1 lost the quorum"),
        false
    )

    -- And now i2 + i3 can't reach i1.
    g.cluster.i1:stop()
    g.cluster.i2:start()
    g.cluster.i3:start()

    -- Help I2 to become a new leader.
    g.cluster.i2:try_promote()
    h.retrying({}, function()
        g.cluster.i3:assert_raft_status("Follower", 2)
    end)

    t.assert_equals(
        propose_state_change(g.cluster.i2, "i2 takes the leadership"),
        true
    )

    -- Now i1 has an uncommitted, but persisted entry that should be rolled back.
    g.cluster.i1:start()
    h.retrying({}, function()
        g.cluster.i1:assert_raft_status("Follower", 2)
    end)

    t.assert_equals(
        propose_state_change(g.cluster.i1, "i1 is alive again"),
        true
    )
end
