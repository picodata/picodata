local t = require('luatest')
local g = t.group('subquery_as_expression')

local helper = require('test.helper.cluster_no_replication')

local cluster = nil

g.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

g.before_each(
        function()
            local api = cluster:server("api-1").net_box

            local r, err = api:call("sbroad.execute", {
                [[INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
                (?, ?, ?),
                (?, ?, ?),
                (?, ?, ?),
                (?, ?, ?),
                (?, ?, ?),
                (?, ?, ?)
                ]],
                {
                    1, "123", 1,
                    2, "1", 1,
                    3, "1", 1,
                    4, "2", 2,
                    5, "123", 2,
                    6, "2", 4
                }
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 6})

            r, err = api:call("sbroad.execute", {
                [[
                    INSERT INTO "null_t"
                    ("na", "nb", "nc")
                    VALUES (?,?,?),(?,?,?),
                    (?,?,?)
                ]],
                {
                    1, nil, 1,
                    2, nil, nil,
                    3, nil, 3,
                }
            })

            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 3})
        end
)

g.after_each(
        function()
            local storage1 = cluster:server("storage-1-1").net_box
            storage1:call("box.execute", { [[truncate table "testing_space"]] })
            storage1:call("box.execute", { [[truncate table "null_t"]] })

            local storage2 = cluster:server("storage-2-1").net_box
            storage2:call("box.execute", { [[truncate table "testing_space"]] })
            storage2:call("box.execute", { [[truncate table "null_t"]] })
        end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_under_projection = function()
    local api = cluster:server("api-1").net_box

    -- Single value in output.
    local r, err = api:call("sbroad.execute", { [[
        SELECT (VALUES (1)) FROM "testing_space" WHERE "id" = 1
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "unsigned" },
    })
    t.assert_items_equals(r.rows, {
        { 1 }
    })

    -- Several values in output.
    r, err = api:call("sbroad.execute", { [[
        SELECT (VALUES (1)), (VALUES (2)) FROM "testing_space" WHERE "id" = 1
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "unsigned" },
        { name = "col_2", type = "unsigned" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 2 },
    })

    -- Values nested in output.
    r, err = api:call("sbroad.execute", { [[
        SELECT (VALUES ((VALUES (3)))) FROM "testing_space" WHERE "id" = 1
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "unsigned" },
    })
    t.assert_items_equals(r.rows, {
        { 3 },
    })

    -- Subquery returning single value.
    r, err = api:call("sbroad.execute", { [[
        SELECT (SELECT "id" FROM "testing_space" WHERE "id" = 1) + "id" FROM "testing_space" WHERE "id" in (1, 2, 3)
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 2 },
        { 3 },
        { 4 },
    })
end

g.test_under_selection = function()
    local api = cluster:server("api-1").net_box

    -- Single value in selection.
    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "testing_space" WHERE "id" = (VALUES (1))
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "id", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1 }
    })

    -- Arithmetic values in selection.
    r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "testing_space" WHERE "id" = (VALUES (1)) + (VALUES (3)) / (VALUES (2))
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "id", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 2 }
    })
end

g.test_under_group_by = function()
    local api = cluster:server("api-1").net_box

    -- Single value in group by.
    local r, err = api:call("sbroad.execute", { [[
        SELECT count(*) FROM "testing_space" GROUP BY "product_units" + (VALUES (1))
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 1 },
        { 3 },
        { 2 },
    })

    -- Single value in group by and having.
    r, err = api:call("sbroad.execute", { [[
        SELECT sum("id") + 1, count(*)
        FROM "testing_space"
        GROUP BY "product_units" + (VALUES (1))
        HAVING sum("id") + (VALUES (1)) > 7
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
        { name = "col_2", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 10, 2 },
    })

    -- Implicit group by.
    r, err = api:call("sbroad.execute", { [[
        SELECT sum("id" + (VALUES (1))) from "testing_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 27 }
    })
end

g.test_under_order_by = function()
    local api = cluster:server("api-1").net_box

    -- Single value in order by.
    local r, err = api:call("sbroad.execute", { [[
        SELECT "name", "id" FROM "testing_space" ORDER BY "name" || (VALUES ('a'))
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "name", type = "string" },
        { name = "id", type = "integer" },
    })
    -- Note that '123a' is compared with '1a' (the first is smaller).
    t.assert_items_equals(r.rows, {
        { "123", 1 },
        { "123", 5 },
        { "1", 2 },
        { "1", 3 },
        { "2", 4 },
        { "2", 6 },
    })
end

g.test_under_cte = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        WITH "my_cte" ("first") AS (VALUES (cast(1 as string)), ((SELECT "name" FROM "testing_space" WHERE "id" = 1)))
        SELECT "first" FROM "my_cte"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "first", type = "string" },
    })
    t.assert_items_equals(r.rows, {
        { "123" },
        { "1" },
    })
end

g.test_under_join = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "testing_space" JOIN "null_t" ON
        (SELECT true FROM "null_t" WHERE "na" = 1) AND "product_units" = "na" AND "name" != (VALUES ('123'))
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "id", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 2 },
        { 3 },
        { 4 },
    })
end

g.test_under_insert = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[INSERT INTO "testing_space"
          VALUES
          (
            (VALUES (11)),
            (VALUES ('111')) || (VALUES ('222')),
            (SELECT 42 FROM "testing_space" WHERE "id" = 1)
          ),
          (
            (SELECT 42 FROM "testing_space" WHERE "id" = 1),
            ?,
            ?
          )]],
        {"aba", 33}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})
end

g.test_under_update = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    update "testing_space"
    set "name" = (SELECT "name" FROM "testing_space" WHERE "product_units" = 4), "product_units" = (VALUES (42))
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 6})

    r, err = api:call("sbroad.execute", { [[
        SELECT *
        FROM "testing_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "id", type = "integer" },
        { name = "name", type = "string" },
        { name = "product_units", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {1, "2", 42},
        {2, "2", 42},
        {3, "2", 42},
        {4, "2", 42},
        {5, "2", 42},
        {6, "2", 42},
    })
end