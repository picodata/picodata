local t = require('luatest')
local option_queries = t.group('query_options')
local helper = require('test.helper.cluster_no_replication')
local cluster = nil

option_queries.before_each(
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
                [[INSERT INTO "cola_accounts_history" ("id", "cola", "colb", "sys_from", "sys_to") VALUES
                (?, ?, ?, ?, ?)
                ]],
                {
                    1, 1, 1, 1, 1
                }
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 1})
        end
)

option_queries.after_each(function()
    local storage1 = cluster:server("storage-1-1").net_box
    storage1:call("box.execute", { [[TRUNCATE TABLE "testing_space"]] })
    storage1:call("box.execute", { [[TRUNCATE TABLE "cola_accounts_history"]] })

    local storage2 = cluster:server("storage-2-1").net_box
    storage2:call("box.execute", { [[TRUNCATE TABLE "testing_space"]] })
    storage2:call("box.execute", { [[TRUNCATE TABLE "cola_accounts_history"]] })

    helper.stop_test_cluster()
end)

option_queries.test_basic = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[select * from "testing_space"]]
    local _, err = api:call("sbroad.execute", {
        query_str .. [[ option(vdbe_max_steps = 5) ]]
    })
    t.assert_str_contains(err, [[Reached a limit on max executed vdbe opcodes. Limit: 5]])

    -- check query works without limit
    local r
    r, err = api:call("sbroad.execute", { query_str })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "id", type = "integer" },
        { name = "name", type = "string" },
        { name = "product_units", type = "integer" },
    })
    t.assert(r.rows ~= nil)
    local no_limit_rows = r.rows

    -- check query executes with an appropriate limit
    r, err = api:call("sbroad.execute", { query_str .. [[ option(vdbe_max_steps = 30) ]] })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "id", type = "integer" },
        { name = "name", type = "string" },
        { name = "product_units", type = "integer" },
    })
    t.assert_equals(r.rows, no_limit_rows)
end

option_queries.test_dml = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[insert into "testing_space" select "id" + 10, "name", "product_units" from "testing_space"]]
    local _, err = api:call("sbroad.execute", {
        query_str .. [[ option(vdbe_max_steps = 10) ]]
    })
    t.assert_str_contains(err, [[Reached a limit on max executed vdbe opcodes. Limit: 10]])

    -- check query works without limit
    local r
    r, err = api:call("sbroad.execute", { query_str .. [[ option(vdbe_max_steps = 0) ]] })
    t.assert_equals(err, nil)
    t.assert_equals(r, { row_count = 6 })
end

option_queries.test_invalid = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[select * from "testing_space"]]
    local _, err = api:call("sbroad.execute", {
        query_str .. [[ option(vdbe_max_steps = 10, vdbe_max_steps = 11) ]]
    })
    t.assert_str_contains(err, [[option vdbe_max_steps specified more than once!]])

    _, err = api:call("sbroad.execute", {
        query_str .. [[ option(vdbe_max_steps = ?) ]], { -1 }
    })
    t.assert_str_contains(err, [[expected option vdbe_max_steps to be unsigned got: Integer(-1)]])
    _, err = api:call("sbroad.execute", {
        query_str .. [[ option(vtable_max_rows = ?) ]], { -1 }
    })
    t.assert_str_contains(err, [[expected option vtable_max_rows to be unsigned got: Integer(-1)]])

    _, err = api:call("sbroad.execute", {
        query_str .. [[ option(bad_option = 1) ]]
    })
    t.assert_str_contains(err, [[Sbroad Error: build query: rule parsing error]])
end

option_queries.test_vtable_max_rows_on_storage = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[insert into "testing_space" select "id" + 10, "name", "product_units" from "testing_space"]]
    local _, err = api:call("sbroad.execute", {
        query_str .. [[ option(vtable_max_rows = 1) ]]
    })
    t.assert_str_contains(err, [[Exceeded maximum number of rows (1) in virtual table: 2]])

    local r
    r, err = api:call("sbroad.execute", { query_str .. [[ option(vtable_max_rows = 6) ]] })
    t.assert_equals(err, nil)
    t.assert_equals(r, { row_count = 6 })
end

option_queries.test_vtable_max_rows_insert_values = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[insert into "cola_accounts_history" values (2, 2, 2, 1, 1), (3, 2, 2, 1, 1), (4, 2, 2, 1, 1)]]
    local _, err = api:call("sbroad.execute", {
        query_str .. [[ option(vtable_max_rows = 1) ]]
    })
    t.assert_str_contains(err, [[Exceeded maximum number of rows (1) in virtual table: 3]])
end

option_queries.test_vtable_max_rows_on_router = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[select "id" from "testing_space" group by "id"]]
    local _, err = api:call("sbroad.execute", {
        query_str .. [[ option(vtable_max_rows = 5) ]]
    })
    -- on storages the result table <= 5 (assuming each storage
    -- has at least one row). On router the VTable will have
    -- 6 rows
    t.assert_str_contains(err, [[Exceeded maximum number of rows (5) in virtual table: 6]])

    local r
    r, err = api:call("sbroad.execute", { query_str .. [[ option(vtable_max_rows = 7) ]] })
    t.assert_equals(err, nil)
    t.assert(r.rows ~= nil)
end
