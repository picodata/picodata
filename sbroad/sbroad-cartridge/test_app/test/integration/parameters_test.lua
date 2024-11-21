local t = require('luatest')
local g = t.group('query_with_parameters')

local helper = require('test.helper.cluster_no_replication')
local cluster = nil

g.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

g.before_each(
    function()
        local api = cluster:server("api-1").net_box

        for i =1,10 do
            local r, err = api:call("sbroad.execute", {
                [[
                    insert into "dtm__marketing__sales_and_stores_history" (
                    "id", "region", "sys_from", "sys_to", "sys_op")
                    values (?, ?, ?, ?, ?)
                ]],
                { i, "region",i + 1, i * 4, i * 3}
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 1})

            r, err = api:call("sbroad.execute", {
                [[
                    insert into "dtm__marketing__sales_and_stores_actual" (
                    "id", "region", "sys_from", "sys_to", "sys_op")
                    values (?, ?, ?, ?, ?)
                ]],
                { i + 2, "region", i + 1, i * 4, i * 3}
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 1})
        end
    end
)

g.after_each(
    function()
        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "dtm__marketing__sales_and_stores_actual"]] })
        storage1:call("box.execute", { [[truncate table "dtm__marketing__sales_and_stores_history"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "dtm__marketing__sales_and_stores_actual"]] })
        storage2:call("box.execute", { [[truncate table "dtm__marketing__sales_and_stores_history"]] })
    end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_parameterized_query = function()
    local api = cluster:server("api-1").net_box

    local query_wo_params = [[
        SELECT
            "id", "region"
        FROM
        "dtm__marketing__sales_and_stores_history"
        WHERE
        "sys_from" <= 3
        AND "sys_to" >= 3
        AND "sys_to" <= 19
        AND ("id", "region") NOT IN (
            SELECT
                "id", "region"
            FROM
                "dtm__marketing__sales_and_stores_history"
            WHERE
                "sys_from" <= 20
                AND "sys_to" >= 20
            UNION ALL
            SELECT
                "id", "region"
            FROM
                "dtm__marketing__sales_and_stores_actual"
            WHERE
                "sys_from" <= 20
        )
    ]]
    local r1, err = api:call("sbroad.execute", { query_wo_params, { } })

    t.assert_equals(err, nil)
    t.assert_not_equals(r1.rows, {})
    local query_w_params = [[
        SELECT
            "id", "region"
        FROM
        "dtm__marketing__sales_and_stores_history"
        WHERE
        "sys_from" <= ?
        AND "sys_to" >= ?
        AND "sys_to" <= ?
        AND ("id", "region") NOT IN (
            SELECT
                "id", "region"
            FROM
                "dtm__marketing__sales_and_stores_history"
            WHERE
                "sys_from" <= ?
                AND "sys_to" >= ?
            UNION ALL
            SELECT
                "id", "region"
            FROM
                "dtm__marketing__sales_and_stores_actual"
            WHERE
                "sys_from" <= ?
        )
    ]]
    -- notice that extra params must be ignored
    local r2, err = api:call("sbroad.execute", { query_w_params, { 3,3,19,20,20,20,1,1,1 } })

    t.assert_equals(err, nil)
    t.assert_items_equals(r1.rows, r2.rows)

    local r3, err = api:call("sbroad.execute", { query_w_params, { 3,3,19,20,20,20 } })

    t.assert_equals(err, nil)
    t.assert_items_equals(r1.rows, r3.rows)
end
