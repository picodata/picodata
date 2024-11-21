local t = require('luatest')
local g = t.group('integration_api.delete')

local helper = require('test.helper.cluster_no_replication')
local cluster = nil


g.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

g.before_each(
    function()
        local api = cluster:server("api-1").net_box

        -- "t" contains:
        -- [1, 2]
        -- [3, 4]
        local r, err = api:call("sbroad.execute", {
            [[insert into "t" ("id", "a") values (?, ?), (?, ?) on conflict do nothing]],
            {1, 2, 3, 4}
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 2})
    end
)

g.after_each(
    function()
        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "t"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "t"]] })
    end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_delete_1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[DELETE FROM "t" WHERE "id" = ?]],
        {3}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    r, err = api:call("sbroad.execute", {
        [[SELECT *, "bucket_id" FROM "t"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "a", type = "number"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            {1, 2, 3940},
        },
    })
end

g.test_delete_2 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[DELETE FROM "t"]],
        {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})

    r, err = api:call("sbroad.execute", {
        [[SELECT *, "bucket_id" FROM "t"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "a", type = "number"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {},
    })
end
