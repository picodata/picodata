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
        }),
        i2 = h.Picodata:new({
            name = 'i2',
            data_dir = g.data_dir .. '/i2',
            listen = '127.0.0.1:13302',
            peer = peer,
        }),
        i3 = h.Picodata:new({
            name = 'i3',
            data_dir = g.data_dir .. '/i3',
            listen = '127.0.0.1:13303',
            peer = peer,
        }),
    }

    for _, node in pairs(g.cluster) do
        node:start()
    end

    for _, node in pairs(g.cluster) do
        node:wait_started()
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

g.test_log_rollback = function()
    -- TODO
    -- Этот тест стал некорректен с появлением фазы дискавери.
    -- Инстансы i2 и i3 не смогут стартануть, т.к. одним из пиров
    -- является дохлый i1.
    -- Тем не менее, сам тест удалять не следует. Данная
    -- проблема требует пересмотреть лишь подход к созданию причин
    -- для ролбека рафт лога. Но основная задача теста (проверка
    -- поведения пикодаты при ролбеке) остается актуальной.
    t.skip('Fix me')

    -- Speed up node election
    g.cluster.i1:promote_or_fail()
    h.retrying({}, function()
        g.cluster.i2:assert_raft_status("Follower", g.cluster.i1.id)
        g.cluster.i3:assert_raft_status("Follower", g.cluster.i1.id)
    end)

    t.assert(
        propose_state_change(g.cluster.i1, "i1 is a leader")
    )

    -- Simulate the network partitioning: i1 can't reach i2 and i3.
    g.cluster.i2:stop()
    g.cluster.i3:stop()

    -- No operations can be committed, i1 is alone.
    t.assert_equals(
        {propose_state_change(g.cluster.i1, "i1 lost the quorum")},
        {nil, "timeout"}
    )

    -- And now i2 + i3 can't reach i1.
    g.cluster.i1:stop()
    g.cluster.i2:start()
    g.cluster.i3:start()

    -- Help I2 to become a new leader.
    g.cluster.i2:promote_or_fail()
    h.retrying({}, function()
        g.cluster.i3:assert_raft_status("Follower", g.cluster.i2.id)
    end)

    t.assert(
        propose_state_change(g.cluster.i2, "i2 takes the leadership")
    )

    -- Now i1 has an uncommitted, but persisted entry that should be rolled back.
    g.cluster.i1:start()
    h.retrying({}, function()
        g.cluster.i1:assert_raft_status("Follower", g.cluster.i2.id)
    end)

    t.assert(
        propose_state_change(g.cluster.i1, "i1 is alive again")
    )
end

g.test_leader_disruption = function()
    g.cluster.i1:promote_or_fail()
    h.retrying({}, function()
        g.cluster.i2:assert_raft_status("Follower", 1)
        g.cluster.i3:assert_raft_status("Follower", 1)
    end)

    -- Simulate asymmetric network failure.
    -- Node i3 doesn't receive any messages,
    -- including the heartbeat from the leader.
    -- Then it starts a new election.
    g.cluster.i3:connect():call(
        'box.schema.func.drop',
        {'.raft_interact'}
    )

    -- Speed up election timeout
    g.cluster.i3:connect():eval([[
        while picolib.raft_status().raft_state == 'Follower' do
            picolib.raft_tick(1)
        end
    ]])
    g.cluster.i3:assert_raft_status("PreCandidate", 0)

    -- Advance the raft log. It makes i1 and i2 to reject the RequestPreVote.
    g.cluster.i1:raft_propose_eval(1, 'return')

    -- Restore normal network operation
    g.cluster.i3:connect():call(
        'box.schema.func.create',
        {'.raft_interact', {
            language = "C",
            if_not_exists = true
        }}
    )

    -- i3 should become the follower again without disrupting i1
    h.retrying({}, function()
        g.cluster.i3:assert_raft_status("Follower", 1)
    end)
end
