local t = require('luatest')
local g = t.group('sbroad_with_substring')

local helper = require('test.helper.cluster_no_replication')

g.before_all(
        function()
            helper.start_test_cluster(helper.cluster_config)
        end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_substring = function ()
    local api = helper.cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[SELECT SUBSTRING(?, ?, ?)]],
        {"abcdefg", 1, 2}
    })
    t.assert_equals(err, nil)
    t.assert_items_equals(r.rows, {{"ab"}})

    local _, err = api:call("sbroad.execute", {
        [[SELECT SUBSTRING(?, ?, ?)]],
        {"abcdefg", "a_c", "#"}
    })
    t.assert_str_contains(err, [[attempt to call field 'TO_REGEXP']])
end