local t = require('luatest')
local g = t.group('integration_api.operators')

local helper = require('test.helper.cluster_no_replication')
local cluster = nil

g.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

g.before_each(
    function()
        local api = cluster:server("api-1").net_box

        -- "testing_space" contains:
        -- [1, "123", 1]
        local r, err = api:call("sbroad.execute", {
            [[insert into "testing_space" ("id", "name", "product_units") values (?, ?, ?)]],
            {1, "123", 1}
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 1})

        -- "testing_space_hist" contains:
        -- [1, "123", 5]
        r, err = api:call("sbroad.execute", {
            [[insert into "testing_space_hist" ("id", "name", "product_units") values (?, ?, ?)]],
            {1, "123", 5}
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 1})

        -- "space_simple_shard_key" contains:
        -- [1, "ok", 1]
        -- [10, NULL, 0]
        r, err = api:call("sbroad.execute", {
            [[insert into "space_simple_shard_key" ("id", "name", "sysOp") values (?, ?, ?), (?, ?, ?)]],
            {1, "ok", 1, 10, box.NULL, 0}
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 2})

        -- "space_simple_shard_key_hist" contains:
        -- [1, "ok_hist", 3]
        -- [2, "ok_hist_2", 1]
        r, err = api:call("sbroad.execute", {
            [[insert into "space_simple_shard_key_hist" ("id", "name", "sysOp") values (?, ?, ?), (?, ?, ?)]],
            {1, "ok_hist", 3, 2, "ok_hist_2", 1}
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 2})

        -- "t" contains:
        -- [1, 4.2]
        -- [2, decimal(6.66)]
        r, err = api:call("sbroad.execute", {
            [[insert into "t" ("id", "a") values (?, ?), (?, ?)]],
            {1, 4.2, 2, require('decimal').new(6.66)}
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 2})
    end
)

g.after_each(
    function()
        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "testing_space"]] })
        storage1:call("box.execute", { [[truncate table "testing_space_hist"]] })
        storage1:call("box.execute", { [[truncate table "space_simple_shard_key"]] })
        storage1:call("box.execute", { [[truncate table "space_simple_shard_key_hist"]] })
        storage1:call("box.execute", { [[truncate table "t"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "testing_space"]] })
        storage2:call("box.execute", { [[truncate table "testing_space_hist"]] })
        storage2:call("box.execute", { [[truncate table "space_simple_shard_key"]] })
        storage2:call("box.execute", { [[truncate table "space_simple_shard_key_hist"]] })
        storage2:call("box.execute", { [[truncate table "t"]] })
    end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_operator_1 = function()
    local api = cluster:server("api-1").net_box
    local r, err = api:call(
        "sbroad.execute",
        {
            [[SELECT * FROM "testing_space" where "id" = 1 AND "id" = 2]],
            {}
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"},
        },
        rows = {},
    })
end

g.test_not_eq = function()
    local api = cluster:server("api-1").net_box
    -- id=1 already in space
    local r, err = api:call("sbroad.execute", {
        [[insert into "testing_space" ("id", "name", "product_units") values (?, ?, ?), (?, ?, ?)]],
        {2, "123", 2, 3, "123", 3}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})


    r, err = api:call(
        "sbroad.execute",
        {
            [[SELECT * FROM "testing_space" where "id" <> 1]],
            {}
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "id", type = "integer"},
        {name = "name", type = "string"},
        {name = "product_units", type = "integer"},
    })
    t.assert_items_equals(r.rows, {
        {2, "123", 2}, {3, "123", 3}
    })


    r, err = api:call(
        "sbroad.execute",
        {
            [[SELECT * FROM "testing_space" where "id" <> 1 and "product_units" <> 3]],
            {}
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"},
        },
        rows = {{2, "123", 2}},
    })
end

g.test_not_eq2 = function()
    -- "t" contains:
    -- [1, 4.2]
    -- [2, decimal(6.66)]
    -- [3, 0.0]
    -- [4, 0.0]
    local api = cluster:server("api-1").net_box
    local r, err = api:call("sbroad.execute", {
        [[insert into "t" ("id", "a") values (?, ?), (?, ?)]],
        {3, 0.0, 4, 0.0}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})

    -- make sure table is located on both storages, not only on one storage
    local storage1 = cluster:server("storage-1-1").net_box
    r, err = storage1:call("box.execute", {
        [[select * from "t"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(true, next(r.rows) ~= nil)

    local storage2 = cluster:server("storage-2-1").net_box
    r, err = storage2:call("box.execute", {
        [[select * from "t"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(true, next(r.rows) ~= nil)

    r, err = api:call(
            "sbroad.execute",
            {
                [[
                    SELECT "id", u FROM "t" join
                    (select "id" as u from "t") as q
                    on "t"."id" <> q.u
                ]],
                {}
            }
    )
    t.assert_equals(err, nil)
    t.assert_items_equals(r.metadata, {
        {name = "id", type = "integer"},
        {name = "u", type = "integer"}
    })
    t.assert_items_equals(r.rows, {
        {1, 2}, {1, 3}, {1, 4},
        {2, 1}, {2, 3}, {2, 4},
        {3, 1}, {3, 2}, {3, 4},
        {4, 1}, {4, 2}, {4, 3},
    })
end

g.test_simple_shard_key_union_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT * FROM (
            SELECT "id", "name" FROM "space_simple_shard_key" WHERE "sysOp" < ?
            UNION ALL
            SELECT "id", "name" FROM "space_simple_shard_key_hist" WHERE "sysOp" > ?
        ) as "t1"
        WHERE "id" = ? ]], { 0, 0, 1 } })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
        },
        rows = {
            { 1, "ok_hist" },
        },
    })
end

g.test_complex_shard_key_union_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT * FROM (
            SELECT "id", "name", "product_units" FROM "testing_space" WHERE "product_units" < ?
            UNION ALL
            SELECT "id", "name", "product_units" FROM "testing_space_hist" WHERE "product_units" > ?
        ) as "t1"
        WHERE "id" = ? and "name" = ? ]], { 3, 3, 1, '123' } })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"}
        },
        rows = {
            { 1, "123", 1 },
            { 1, "123", 5 }
        },
    })
end

g.test_compare = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT * FROM "t" where "id" < 2 and "a" > 5]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "a", type = "number"},
        },
        rows = {},
    })
end

g.test_except = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[insert into "t" ("id", "a") values (?, ?), (?, ?), (?, ?)]],
        {
            -- After migration to the virtual tables in the executor
            -- we can't mix types in the same number column (as we don't
            -- cast types while population of the virtual tables).
            3, require('decimal').new(777),
            1000001, require('decimal').new(6.66),
            1000002, require('decimal').new(6.66)
        }
    })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 3})

    r, err = api:call("sbroad.execute", { [[
        SELECT "a" FROM "t" where "id" <= 3
        EXCEPT
        SELECT "a" FROM "t" where "id" > 3
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "a", type = "number"},
    })
    t.assert_items_equals(r.rows, {
        {4.2},
        {777}
    })
end

g.test_is_null = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "space_simple_shard_key" WHERE "name" IS NULL
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
            {10}
        },
    })
end

g.test_is_not_null_1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "space_simple_shard_key" WHERE "name" IS NOT NULL and "id" = 10
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
        },
    })
end

g.test_is_not_null_2 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "space_simple_shard_key" WHERE "name" IS NOT NULL
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
            {1}
        },
    })
end

g.test_in_subquery_select_from_table = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "space_simple_shard_key" WHERE "id" IN (SELECT "id" FROM "testing_space")
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
            {1}
        },
    })
end

g.test_not_in_subquery_select_from_values = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "space_simple_shard_key"
        WHERE "id" NOT IN (SELECT cast("COLUMN_2" as int) FROM (VALUES (1), (3)))
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
            {10}
        },
    })
end

g.test_in_subquery_select_from_values = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "space_simple_shard_key_hist" WHERE "id" IN (SELECT cast("COLUMN_1" as int) FROM (VALUES (1)))
    ]], {1} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
            {1}
        },
    })
end

g.test_exists_subquery_select_from_values = function ()
    local api = cluster:server("api-1").net_box

    -- Exists condition should return true on each row from t
    -- as soon as it's subquery always returns one row.
    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "t" WHERE EXISTS (SELECT 0 FROM (VALUES (1)))
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "id", type = "integer"},
    })
    t.assert_items_equals(r.rows, {
        {1}, {2}
    })
end

g.test_not_exists_subquery_select_from_values = function()
    local api = cluster:server("api-1").net_box

    -- NotExists condition should return false on each row from t
    -- as soon as it's subquery always returns one row.
    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "t" WHERE NOT EXISTS (SELECT cast("COLUMN_1" as int) FROM (VALUES (1)))
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = { },
    })
end

g.test_exists_subquery_with_several_rows = function ()
    local api = cluster:server("api-1").net_box

    -- Exists condition should return true on each row from testing_space
    -- as soon as it's subquery always returns two rows.
    local r, err = api:call("sbroad.execute", { [[
        SELECT * FROM "testing_space" WHERE EXISTS (SELECT 0 FROM "t" WHERE "t"."id" = 1 or "t"."a" = (?))
    ]], {require('decimal').new(6.66)} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"},
        },
        rows = { {1, "123", 1} },
    })
end

g.test_not_exists_subquery_with_several_rows = function()
    local api = cluster:server("api-1").net_box

    -- NotExists condition should return false on each row from testing_space
    -- as soon as it's subquery always returns two rows.
    local r, err = api:call("sbroad.execute", { [[
        SELECT * FROM "testing_space"
        WHERE NOT EXISTS (SELECT 0 FROM "t" WHERE "t"."id" = 1 or "t"."a" = (?))
    ]], {require('decimal').new(6.66)} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"},
        },
        rows = {},
    })
end

g.test_exists_nested = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT * FROM "testing_space" WHERE EXISTS
        (SELECT 0 FROM (VALUES (1)) WHERE EXISTS (SELECT 0 FROM "t" WHERE "t"."id" = 1))
    ]], {1} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"},
        },
        rows = {
            {1, "123", 1}
        },
    })
end

g.test_exists_partitioned_in_selection_condition = function()
    -- make sure table is located on both storages, not only on one storage
    local storage1 = cluster:server("storage-1-1").net_box
    local r, err = storage1:call("box.execute", {
        [[select * from "t"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(true, next(r.rows) ~= nil)

    local storage2 = cluster:server("storage-2-1").net_box
    r, err = storage2:call("box.execute", {
        [[select * from "t"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(true, next(r.rows) ~= nil)


    local api = cluster:server("api-1").net_box

    local r_all, err_all = api:call("sbroad.execute", { [[
        SELECT * FROM "t"
    ]], {} })

    t.assert_equals(err_all, nil)
    t.assert_equals(r_all.metadata, {
        {name = "id", type = "integer"},
        {name = "a", type = "number"},
    })
    t.assert_items_equals(r_all.rows, {
        {1, 4.2}, {2, 6.66}
    })

    local r, err = api:call("sbroad.execute", { [[
        SELECT * FROM "t" WHERE EXISTS (SELECT * FROM "testing_space")
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r_all, r)
end

g.test_exists_partitioned_in_join_filter = function()
    -- make sure table is located on both storages, not only on one storage
    local storage1 = cluster:server("storage-1-1").net_box
    local r, err = storage1:call("box.execute", {
        [[select * from "t"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(true, next(r.rows) ~= nil)

    local storage2 = cluster:server("storage-2-1").net_box
    r, err = storage2:call("box.execute", {
        [[select * from "t"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(true, next(r.rows) ~= nil)


    local api = cluster:server("api-1").net_box

    -- Inner child would be broadcasted and join still will be located
    -- on both storages
    local r_all, err_all = api:call("sbroad.execute", { [[
        SELECT * FROM
            (SELECT "id" as "tid" FROM "t") as "t"
        INNER JOIN
            (SELECT "id" as "sid" FROM "space_simple_shard_key") as "s"
        ON true
    ]], {} })

    t.assert_equals(err_all, nil)
    t.assert_equals(r_all.metadata, {
        {name = "tid", type = "integer"},
        {name = "sid", type = "integer"},
    })
    t.assert_items_equals(r_all.rows, {
        {1, 1}, {1, 10}, {2, 1}, {2, 10}
    })

    local r, err = api:call("sbroad.execute", { [[
        SELECT * FROM
            (SELECT "id" as "tid" FROM "t") as "t"
        INNER JOIN
            (SELECT "id" as "sid" FROM "space_simple_shard_key") as "s"
        ON EXISTS (SELECT * FROM "testing_space")
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r_all, r)
end

g.test_not_with_true_gives_false = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT not true FROM (values (1))
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "col_1", type = "boolean"},
        },
        rows = {
            {false}
        },
    })
end

g.test_double_not_with_true_gives_true = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT not not true FROM (values (1))
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "col_1", type = "boolean"},
        },
        rows = {
            {true}
        },
    })
end

g.test_not_with_parenthesis = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT (not (not true)) FROM (values (1))
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "col_1", type = "boolean"},
        },
        rows = {
            {true}
        },
    })
end

g.test_not_with_and = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT not true and false FROM (values (1))
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "col_1", type = "boolean"},
        },
        rows = {
            {false}
        },
    })
end

g.test_not_with_or = function()
    local api = cluster:server("api-1").net_box

    -- Select must return true as soon as OR operator has greater priority
    -- `SELECT not true or true` = `SELECT (not true) or true`
    local r, err = api:call("sbroad.execute", { [[
        SELECT not true or true FROM (values (1))
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "col_1", type = "boolean"},
        },
        rows = {
            {true}
        },
    })
end

g.test_not_with_in = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        select * from (values (1)) where 1 not in (values(1))
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {{name = "COLUMN_1", type = "unsigned"}},
        rows = {},
    })
end

g.test_not_with_exists = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        select "COLUMN_1" from (values (1)) where not exists (select 1 from (values (1)))
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {{name = "COLUMN_1", type = "unsigned"}},
        rows = {},
    })
end

g.test_not_in_filter = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        select "id" from "testing_space" where not "id" = 2
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {{1}},
    })
end

g.test_not_in_condition = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        select * from
            (select "id" as "tid" from "testing_space")
        inner join
            "t"
        on not "tid" != "id" and "a" = 4.2
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "tid", type = "integer"},
            {name = "id", type = "integer"},
            {name = "a", type = "number"},
        },
        rows = {{1, 1, 4.2}},
    })
end

g.test_not_between = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT 1 not between 2 and 3 FROM (values (1))
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "col_1", type = "boolean"},
        },
        rows = {
            {true}
        },
    })
end

g.test_between1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "space_simple_shard_key" WHERE
        (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "id" = 2) BETWEEN 1 AND 2
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
            {1},
            {10}
        },
    })
end

g.test_between2 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "space_simple_shard_key" WHERE
        "id" BETWEEN 1 AND 2
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
            {1}
        },
    })
end

g.test_join_inner_sq_no_alias = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    select "i" from "testing_space" inner join
    (select "id" as "i", "a" as a from "t")
    on "id" = "i"
]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "i", type = "integer"},
        },
        rows = {
            {1}
        },
    })
end
