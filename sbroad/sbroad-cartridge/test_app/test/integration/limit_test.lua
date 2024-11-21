local t = require('luatest')
local g = t.group('integration_api.limit')

local helper = require('test.helper.cluster_no_replication')

g.before_all(
        function()
            helper.start_test_cluster(helper.cluster_config)
            local api = helper.cluster:server("api-1").net_box

            local r, err = api:call("sbroad.execute", { [[
                    INSERT INTO "t"("id", "a")
                    VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)
            ]], })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 5})
        end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_limit = function()
    local r, err
    local api = helper.cluster:server("api-1").net_box

    -- select with limit
    r, err = api:call("sbroad.execute", { [[
        SELECT "a" FROM "t" LIMIT 2
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], { {name = "a", type = "number"} })
    t.assert_equals(#r["rows"], 2)

    -- order by with limit
    r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "t" ORDER BY "id" LIMIT 2
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], { {name = "id", type = "integer"} })
    t.assert_items_equals(r["rows"], { {1}, {2} })

    -- aggregate and group by with limit
    r, err = api:call("sbroad.execute", { [[
        SELECT count(*) FROM "t" GROUP BY "id" LIMIT 3
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], { {name = "col_1", type = "integer"} })
    t.assert_equals(#r["rows"], 3)

    -- cte with limit
    r, err = api:call("sbroad.execute", { [[
        WITH cte (b) AS (SELECT "a" FROM "t" ORDER BY "a" LIMIT 2)
        SELECT b FROM cte
        UNION ALL
        SELECT b FROM cte
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], { {name = "b", type = "number"} })
    t.assert_items_equals(r["rows"], { {1}, {2}, {1}, {2} })

    -- cte with limit
    r, err = api:call("sbroad.execute", { [[
        WITH cte (b) AS (SELECT "a" FROM "t" ORDER BY "a" LIMIT 2)
        SELECT b FROM cte
        UNION ALL
        SELECT b FROM cte
        LIMIT 1
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], { {name = "b", type = "number"} })
    t.assert_equals(#r["rows"], 1)

    -- limit in a subquery
    r, err = api:call("sbroad.execute", { [[
        SELECT "a" FROM (SELECT "a" FROM "t" LIMIT 1)
    ]], })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], { {name = "a", type = "number"} })
    t.assert_equals(#r["rows"], 1)
end
