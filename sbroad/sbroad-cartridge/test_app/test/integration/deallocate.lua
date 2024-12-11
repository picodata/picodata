local t = require('luatest')
local g = t.group('integration_api.deallocate')

local helper = require('test.helper.cluster_no_replication')

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_deallocate = function()
    local r, err
    local api = helper.cluster:server("api-1").net_box

    r, err = api:call("sbroad.execute", { [[DEALLOCATE name]] })
    t.assert_equals(err, nil)
    t.assert_equals(r["row_count"], 0)

    r, err = api:call("sbroad.execute", { [[DEALLOCATE ALL]] })
    t.assert_equals(err, nil)
    t.assert_equals(r["row_count"], 0)

    r, err = api:call("sbroad.execute", { [[DEALLOCATE prepare]] })
    t.assert_equals(err, nil)
    t.assert_equals(r["row_count"], 0)

    r, err = api:call("sbroad.execute", { [[DEALLOCATE PREPARE prepare]] })
    t.assert_equals(err, nil)
    t.assert_equals(r["row_count"], 0)

    r, err = api:call("sbroad.execute", { [[DEALLOCATE PREPARE name]] })
    t.assert_equals(err, nil)
    t.assert_equals(r["row_count"], 0)

    r, err = api:call("sbroad.execute", { [[DEALLOCATE PREPARE ALL]] })
    t.assert_equals(err, nil)
    t.assert_equals(r["row_count"], 0)
end
