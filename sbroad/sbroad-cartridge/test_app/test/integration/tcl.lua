local t = require('luatest')
local g = t.group('sbroad_with_tcl')

local helper = require('test.helper.cluster_no_replication')

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_transaction_commands = function()
    local r, err
    local api = helper.cluster:server("api-1").net_box

    r, err = api:call("sbroad.execute", { [[BEGIN]] })
    t.assert_equals(err, nil)
    t.assert_equals(r["row_count"], 0)

    r, err = api:call("sbroad.execute", { [[COMMIT]] })
    t.assert_equals(err, nil)
    t.assert_equals(r["row_count"], 0)

    r, err = api:call("sbroad.execute", { [[ROLLBACK]] })
    t.assert_equals(err, nil)
    t.assert_equals(r["row_count"], 0)
end