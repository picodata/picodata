local t = require('luatest')
local g = t.group('integration_api.insert_uuid_sql')

local helper = require('test.helper.cluster_no_replication')

local UUID = require("uuid")
local uuid1 = UUID()
local uuid2 = UUID()
local uuid3 = "fb1649a4-d2db-4df4-a24d-2b4e81ee8a41"

g.before_all(
        function()
            helper.start_test_cluster(helper.cluster_config)

            local api = helper.cluster:server("api-1").net_box

            local r, err = api:call("sbroad.execute",
                    {
                        [[
                            INSERT INTO "uuid_t"("id", "name")
                            VALUES (?, ?), (?, ?), (?, ?)
                        ]],
                        { uuid1, "test-1", uuid2, "test-2", uuid3, "test-3" }
                    }
            )
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 3})
        end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_uuid_sql_where_uuid1 = function ()
    local api = helper.cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute",
            {
                [[SELECT "id", "name" FROM "uuid_t" WHERE "id" = ?]],
                {uuid1}
            }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "uuid"},
            {name = "name", type = "string"},
        },
        rows = {
            { uuid1, 'test-1' }
        },
    })
end

g.test_uuid_sql_where_cast_uuid_test1 = function ()
    local api = helper.cluster:server("api-1").net_box

    local r, err = api:call(
            "sbroad.execute",
            {
                [[SELECT CAST("id" as Text) as "id", "name" FROM "uuid_t" WHERE "id" = ? ]],
                { uuid1 }
            }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "string"},
            {name = "name", type = "string"},
        },
        rows = {
            { string.format('%s', uuid1), 'test-1' }
        },
    })
end

g.test_uuid_sql_where_cast_uuid_test2 = function ()
    local api = helper.cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute",
            {
                [[SELECT * FROM "uuid_t" WHERE CAST("id" as Text) IN (?) ]],
                { uuid3 }
            }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "uuid"},
            {name = "name", type = "string"},
        },
        rows = {
            { uuid3, 'test-3' }
        },
    })
end

g.test_uuid_sql_where_cast_uuid_test3 = function ()
    local api = helper.cluster:server("api-1").net_box

    local r, err = api:call(
            "sbroad.execute",
            {
                [[
                    SELECT CAST("id" as Uuid) as "id", "name"
                    FROM (
                        SELECT CAST("id" as Text) as "id", "name"
                        FROM "uuid_t" where "id" = ?
                    )
                ]],
                { uuid2 }
            }
    )

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "uuid"},
            {name = "name", type = "string"},
        },
        rows = {
            { uuid2, 'test-2' }
        },
    })
end
