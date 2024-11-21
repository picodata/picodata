local t = require('luatest')
local update_queries = t.group('update_queries')
local helper = require('test.helper.cluster_no_replication')
local cluster = nil

update_queries.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

update_queries.before_each(
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
                    INSERT INTO "arithmetic_space"
                    ("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
                    VALUES (?,?,?,?,?,?,?,?,?,?),
                    (?,?,?,?,?,?,?,?,?,?),
                    (?,?,?,?,?,?,?,?,?,?),
                    (?,?,?,?,?,?,?,?,?,?)
                ]],
                {
                    1, 1, 1, 1, 1, 2, 2, true, "a", 3.14,
                    2, 1, 2, 1, 2, 2, 2, true, "a", 2,
                    3, 2, 3, 1, 2, 2, 2, true, "c", 3.14,
                    4, 2, 3, 1, 1, 2, 2, true, "c", 2.14
                }
            })

            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 4})
            r, err = api:call("sbroad.execute", {
                [[
                    INSERT INTO "arithmetic_space2"
                    ("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
                    VALUES (?,?,?,?,?,?,?,?,?,?),
                    (?,?,?,?,?,?,?,?,?,?),
                    (?,?,?,?,?,?,?,?,?,?),
                    (?,?,?,?,?,?,?,?,?,?)
                ]],
                {
                    1, 2, 1, 1, 1, 2, 2, true, "a", 3.1415,
                    2, 2, 2, 1, 3, 2, 2, false, "a", 3.1415,
                    3, 1, 1, 1, 1, 2, 2, false, "b", 2.718,
                    4, 1, 1, 1, 1, 2, 2, true, "b", 2.717,
                }
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 4})

            r, err = api:call("sbroad.execute", {
                [[
                    INSERT INTO "double_t"
                    ("id", "r", "dec")
                    VALUES (?,?,?),
                    (?,?,?)
                ]],
                {
                    1, 1e-1, 0.1,
                    2, 5e-1, 0.5
                }
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 2})

            r, err = api:call("sbroad.execute", {
                [[INSERT INTO "testing_space_bucket_in_the_middle" ("id", "name", "product_units") VALUES
                (?, ?, ?),
                (?, ?, ?),
                (?, ?, ?)
                ]],
                {
                    1, "1", 1,
                    2, "1", 1,
                    3, "1", 1,
                }
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 3})
        end
)

update_queries.after_each(function()
    local storage1 = cluster:server("storage-1-1").net_box
    storage1:call("box.execute", { [[TRUNCATE TABLE "testing_space"]] })
    storage1:call("box.execute", { [[TRUNCATE TABLE "arithmetic_space"]] })
    storage1:call("box.execute", { [[TRUNCATE TABLE "arithmetic_space2"]] })
    storage1:call("box.execute", { [[TRUNCATE TABLE "double_t"]] })
    storage1:call("box.execute", { [[TRUNCATE TABLE "testing_space_bucket_in_the_middle"]] })

    local storage2 = cluster:server("storage-2-1").net_box
    storage2:call("box.execute", { [[TRUNCATE TABLE "testing_space"]] })
    storage2:call("box.execute", { [[TRUNCATE TABLE "arithmetic_space"]] })
    storage2:call("box.execute", { [[TRUNCATE TABLE "arithmetic_space2"]] })
    storage2:call("box.execute", { [[TRUNCATE TABLE "double_t"]] })
    storage2:call("box.execute", { [[TRUNCATE TABLE "testing_space_bucket_in_the_middle"]] })
end)

update_queries.after_all(function()
    helper.stop_test_cluster()
end)


update_queries.test_basic = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    update "testing_space"
    set "name" = 'It works!'
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 6})

    -- check that table was updated
    local r, err = api:call("sbroad.execute", { [[
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
        {1, "It works!", 1},
        {2, "It works!", 1},
        {3, "It works!", 1},
        {4, "It works!", 2},
        {5, "It works!", 2},
        {6, "It works!", 4},
    })
end

update_queries.test_invalid = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.execute", {
        [[ update "testing_space" set "id" = 1 where "name" = 'Den']], {}
    })
    t.assert_str_contains(tostring(err), "invalid query: it is illegal to update primary key column: \"id\"")

    _, err = api:call("sbroad.execute", {
        [[ update "testing_space" set "name" = 'a', "name" = 'b' where "id" = 1]], {}
    })
    t.assert_str_contains(tostring(err), "The same column is specified twice in update list")

    _, err = api:call("sbroad.execute", {
        [[ update "testing_space" set "name" = group_concat("name"), "name" = 'b' where "id" = 1]], {}
    })
    t.assert_str_contains(tostring(err), "aggregate functions are not supported in update expression.")

    _, err = api:call("sbroad.execute", {
        [[ update "testing_space" set "name" = 'a' || group_concat("name"), "name" = 'b' where "id" = 1]], {}
    })
    t.assert_str_contains(tostring(err), "aggregate functions are not supported in update expression.")

    _, err = api:call("sbroad.execute", {
        [[ update "testing_space" set "name" = 'a', "bucket_id" = 1 where "id" = 1]], {}
    })
    t.assert_str_contains(tostring(err), "failed to update column: system column \"bucket_id\" cannot be updated")

    -- subqueries as update expression are not supported
    _, err = api:call("sbroad.execute", {
        [[ update "testing_space" set "product_units" = select sum("id") from "t" where "id" = 1]], {}
    })
    t.assert_str_contains(tostring(err), "Sbroad Error: build query: rule parsing error")

    -- wrong type for column, no conversion is possible
    _, err = api:call("sbroad.execute", {
        [[ update "testing_space" set "product_units" = 'hello']], {}
    })
    t.assert_str_contains(tostring(err), "Failed to cast 'hello' to integer.")

    -- table name can't specified on the left side of update
    _, err = api:call("sbroad.execute", {
        [[ update "testing_space" set "testing_space"."product_units" = 1]], {}
    })
    t.assert_str_contains(tostring(err), "Sbroad Error: build query: rule parsing error")
end

update_queries.test_where = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    update "testing_space"
    set "name" = 'It works!'
    where "product_units" = 1
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 3})

    -- check that table was updated
    local r, err = api:call("sbroad.execute", { [[
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
        {1, "It works!", 1},
        {2, "It works!", 1},
        {3, "It works!", 1},
        {4, "2", 2},
        {5, "123", 2},
        {6, "2", 4}
    })
end

update_queries.test_join = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    update "testing_space" set
    "name" = u."string_col"
    from (select "id" as "i", "string_col" from "arithmetic_space") as u
    where "id" = "i"
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 4})

    -- check that table was updated
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
        {1, "a", 1},
        {2, "a", 1},
        {3, "c", 1},
        {4, "c", 2},
        {5, "123", 2},
        {6, "2", 4}
    })
end

update_queries.test_join_with_multiple_pk_rows = function()
    -- if after join we get multiple records with the same pk
    -- update will use the first row and ignore all others
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    update "testing_space" set
    "name" = "string_col"
    from (select "id" as "i", "string_col" from "arithmetic_space") as u
    where "id" = 1 and "string_col" = 'a'
]], {} })
    t.assert_equals(err, nil)
    -- here join condition produces two rows that will be updated
    t.assert_equals(r, {row_count = 2})

    -- check that table was updated
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
        {1, "a", 1},
        {2, "1", 1},
        {3, "1", 1},
        {4, "2", 2},
        {5, "123", 2},
        {6, "2", 4},
    })
end

update_queries.test_inverse_column_order = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    update "testing_space" set
    "product_units" = "i",
    "name" = "string_col" || 'hello'
    from (select "id" as "i", "string_col" from "arithmetic_space") as u
    where "id" = "i" and "id" = 1
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    -- check that table was updated
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
        {1, "ahello", 1},
        {2, "1", 1},
        {3, "1", 1},
        {4, "2", 2},
        {5, "123", 2},
        {6, "2", 4},
    })
end

update_queries.test_shard_column_updated = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    update "testing_space" set
    "name" = 'some string',
    "product_units" = 1000
    where "id" = 1
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    -- check that table was updated
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
        {1, "some string", 1000},
        {2, "1", 1},
        {3, "1", 1},
        {4, "2", 2},
        {5, "123", 2},
        {6, "2", 4},
    })
    local expected_bucket_id = api:call("sbroad.calculate_bucket_id", {{1, "some string", 1000}, "testing_space"})

    r, err = api:call("sbroad.execute", { [[
    select "bucket_id" from "testing_space"
    where "id" = 1 ]], {}})
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "bucket_id", type = "unsigned" },
    })
    t.assert_equals(#r.rows, 1)
    local new_bucket_id = r.rows[1][1]

    t.assert_equals(new_bucket_id, expected_bucket_id)
end


update_queries.test_local_update = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    update "arithmetic_space" set
    "boolean_col" = false
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 4})

    -- check that table was updated
    r, err = api:call("sbroad.execute", { [[
        SELECT "boolean_col"
        FROM "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "boolean_col", type = "boolean" },
    })
    t.assert_items_equals(r.rows, {
        { false },
        { false },
        { false },
        { false },
    })
end

update_queries.test_local_update_ambiguous_join = function()
    -- Check update execution when join condition allows for one
    -- row of updated table to be joined with multiple rows of another table.
    -- In this case it is not clear which row will be used for update
    -- (current implementation will work as if the last tuple (with the same pk) was used)
    -- but update should succeed and return the number of times `update`
    -- was executed locally.

    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    update "arithmetic_space" set
    "boolean_col" = "boo"
    from (select "boolean_col" as "boo" from "arithmetic_space2")
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 16})

    -- check that table was updated
    r, err = api:call("sbroad.execute", { [[
        SELECT "boolean_col"
        FROM "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "boolean_col", type = "boolean" },
    })
    t.assert_items_equals(r.rows, {
        { true },
        { true },
        { true },
        { true },
    })
end


update_queries.test_subquery_in_selection = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    update "arithmetic_space" set
    "c" = 1000
    where "id" in (select avg("c") from "arithmetic_space2")
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    -- check that table was updated
    r, err = api:call("sbroad.execute", { [[
        SELECT "c"
        FROM "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "c", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1000 },
        { 1 },
        { 1 },
        { 1 },
    })
end

update_queries.test_type_conversion = function()
    local api = cluster:server("api-1").net_box

    -- "r" - is double, 3.14 - decimal literal
    -- "dec" - is decimal, 27e-1 - double literal
    -- check types will be converted
    local r, err = api:call("sbroad.execute", { [[
    update "double_t" set
    "r" = 3.14,
    "dec" = 27e-1
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})

    -- check that table was updated
    r, err = api:call("sbroad.execute", { [[
        SELECT *
        FROM "double_t"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "id", type = "integer" },
        { name = "r", type = "double" },
        { name = "dec", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 3.14, 2.7 },
        { 2, 3.14, 2.7 },
    })
end

update_queries.test_bucket_id_in_the_middle = function()
    local api = cluster:server("api-1").net_box

    -- sharded update
    local r, err = api:call("sbroad.execute", { [[
    update "testing_space_bucket_in_the_middle"
    set "name" = '2'
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 3})

    -- check that table was updated
    r, err = api:call("sbroad.execute", { [[
        SELECT "name"
        FROM "testing_space_bucket_in_the_middle"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "name", type = "string" },
    })
    t.assert_items_equals(r.rows, {
        { "2" },
        { "2" },
        { "2" },
    })

    -- local update
    r, err = api:call("sbroad.execute", { [[
    update "testing_space_bucket_in_the_middle"
    set "product_units" = 42
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 3})

    -- check that table was updated
    r, err = api:call("sbroad.execute", { [[
        SELECT "product_units"
        FROM "testing_space_bucket_in_the_middle"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "product_units", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 42 },
        { 42 },
        { 42 },
    })
end
