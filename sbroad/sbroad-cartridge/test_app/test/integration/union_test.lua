local t = require('luatest')
local union_queries = t.group('union_queries')
local helper = require('test.helper.cluster_no_replication')
local cluster = nil

union_queries.before_all(
        function()
            helper.start_test_cluster(helper.cluster_config)
            cluster = helper.cluster

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
                    INSERT INTO "null_t"
                    ("na", "nb", "nc")
                    VALUES (?,?,?),(?,?,?),
                    (?,?,?),(?,?,?),(?,?,?)
                ]],
                {
                    1, nil, 1,
                    2, nil, nil,
                    3, nil, 3,
                    4, 1, 2,
                    5, nil, 1,
                }
            })

            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 5})
        end
)

union_queries.after_all(function()
    local storage1 = cluster:server("storage-1-1").net_box
    storage1:call("box.execute", { [[TRUNCATE TABLE "testing_space"]] })
    storage1:call("box.execute", { [[TRUNCATE TABLE "arithmetic_space"]] })
    storage1:call("box.execute", { [[TRUNCATE TABLE "arithmetic_space2"]] })

    local storage2 = cluster:server("storage-2-1").net_box
    storage2:call("box.execute", { [[TRUNCATE TABLE "testing_space"]] })
    storage2:call("box.execute", { [[TRUNCATE TABLE "arithmetic_space"]] })
    storage2:call("box.execute", { [[TRUNCATE TABLE "arithmetic_space2"]] })

    helper.stop_test_cluster()
end)


union_queries.after_test('test_union_under_insert', function()
    local storage1 = cluster:server("storage-1-1").net_box
    storage1:call("box.execute", { [[TRUNCATE TABLE t]] })
    local storage2 = cluster:server("storage-2-1").net_box
    storage2:call("box.execute", { [[TRUNCATE TABLE t]] })
end)

union_queries.test_union_under_insert = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        insert into t
        select id, a from arithmetic_space
        union
        select id, a from arithmetic_space
        union
        select id, a from arithmetic_space
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count=4})

    r, err = api:call("sbroad.execute", { [[
        insert into t
        select * from (values (100, 200))
        union
        select * from (values (100, 200), (200, 100))
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count=2})
end


union_queries.test_union_removes_duplicates = function()
    local api = cluster:server("api-1").net_box

    -- with UNION ALL
    local r, err = api:call("sbroad.execute", { [[
    select "name"
    from "testing_space"
    union all
    select null from "testing_space" where false
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "name", type = "string" },
    })
    t.assert_items_equals(r.rows, {
        { "123" },
        { "123" },
        { "1" },
        { "1" },
        { "2" },
        { "2" }
    })

    -- with UNION
    local r, err = api:call("sbroad.execute", { [[
    select "name"
    from "testing_space"
    union
    select null from "testing_space" where false
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "name", type = "string" },
    })
    t.assert_items_equals(r.rows, {
        { "123" },
        { "1" },
        { "2" },
    })
end

union_queries.test_union_seg_vs_single = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    select "a"
    from "arithmetic_space"
    union
    select sum("a") / 3 from "arithmetic_space"
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1 },
        { 2 }
    })
end

union_queries.test_union_seg_vs_any = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    select "a", "b"
    from "arithmetic_space"
    union
    select "a" + 1 - 1, "b" from "arithmetic_space"
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "b", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1 },
        { 1, 2 },
        { 2, 3 }
    })
end

union_queries.test_multi_union = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    select * from (
        select "a"
        from "arithmetic_space"
        union
        select "a" from "arithmetic_space"
    ) union
    select "product_units" from "testing_space"
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1 },
        { 2 },
        { 4 }
    })
end

union_queries.test_union_diff_types = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.execute", { [[
        select "a"
        from "arithmetic_space"
        union
        select "name" from "testing_space"
]], {} })
    t.assert_str_contains(tostring(err), "Failed to cast '1' to integer.")
end

union_queries.test_union_empty_children = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        select "a"
        from "arithmetic_space" where false
        union
        select "id" from "testing_space"
        where false
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
    })
    t.assert_items_equals(r.rows, {})
end
