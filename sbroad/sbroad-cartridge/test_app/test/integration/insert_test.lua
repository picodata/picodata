local t = require('luatest')
local g = t.group('integration_api.insert')

local helper = require('test.helper.cluster_no_replication')
local cluster = nil


g.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

g.before_each(
    function()
        local api = cluster:server("api-1").net_box

        -- "space_simple_shard_key" contains:
        -- [1, "ok", 1]
        -- [10, NULL, 0]
        local r, err = api:call("sbroad.execute", {
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

        r, err = api:call("sbroad.execute", {
            [[insert into "unique_secondary_index" values (?, ?, ?), (?, ?, ?)]],
            {
                1, 1, 1,
                2, 1, 2,
            }
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 2})
    end
)

g.after_each(
    function()
        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "space_simple_shard_key"]] })
        storage1:call("box.execute", { [[truncate table "space_simple_shard_key_hist"]] })
        storage1:call("box.execute", { [[truncate table "t"]] })
        storage1:call("box.execute", { [[truncate table "unique_secondary_index"]] })
        storage1:call("box.execute", { [[truncate table "arithmetic_space"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "space_simple_shard_key"]] })
        storage2:call("box.execute", { [[truncate table "space_simple_shard_key_hist"]] })
        storage2:call("box.execute", { [[truncate table "t"]] })
        storage2:call("box.execute", { [[truncate table "unique_secondary_index"]] })
        storage2:call("box.execute", { [[truncate table "arithmetic_space"]] })
    end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_insert_1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    SELECT * FROM "space_simple_shard_key_hist" WHERE "id" > ?]], { 1 } })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "space_simple_shard_key"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "id", type = "integer"},
        {name = "name", type = "string"},
        {name = "sysOp", type = "integer"},
        {name = "bucket_id", type = "unsigned"},
    })
    t.assert_items_equals(r.rows, {
        {1, "ok", 1, 3940},
        {10, box.NULL, 0, 11520},
        {2, "ok_hist_2", 1, 22072}
    })
end

g.test_insert_2 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("name", "sysOp", "id")
    SELECT 'four', 5, 3 FROM "space_simple_shard_key_hist" WHERE "id" IN (
        SELECT ? FROM "space_simple_shard_key"
    )]], { 1 } })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "space_simple_shard_key"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "id", type = "integer"},
        {name = "name", type = "string"},
        {name = "sysOp", type = "integer"},
        {name = "bucket_id", type = "unsigned"},
    })
    t.assert_items_equals(r.rows, {
        {1, "ok", 1, 3940},
        {10, box.NULL, 0, 11520},
        {3, "four", 5, 21301}
    })
end

g.test_insert_3 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("sysOp", "id") VALUES (?, ?), (?, ?)]], { 5, 4, 6, 5 } })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})

    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "space_simple_shard_key"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "id", type = "integer"},
        {name = "name", type = "string"},
        {name = "sysOp", type = "integer"},
        {name = "bucket_id", type = "unsigned"},
    })
    t.assert_items_equals(r.rows, {
        {1, "ok", 1, 3940},
        {5, box.NULL, 6, 6661},
        {10, box.NULL, 0, 11520},
        {4, box.NULL, 5, 27225},
    })
end

-- check cyrillic consts support
g.test_insert_4 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("sysOp", "id", "name") VALUES (?, ?, 'кириллица'), (?, ?, 'КИРИЛЛИЦА')]], { 5, 4, 6, 5 } })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})

    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "space_simple_shard_key"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "id", type = "integer"},
        {name = "name", type = "string"},
        {name = "sysOp", type = "integer"},
        {name = "bucket_id", type = "unsigned"},
    })
    t.assert_items_equals(r.rows, {
        {1, "ok", 1, 3940},
        {5, "КИРИЛЛИЦА", 6, 6661},
        {10, box.NULL, 0, 11520},
        {4, "кириллица", 5, 27225},
    })
end

-- check cyrillic params support
g.test_insert_5 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("sysOp", "id", "name") VALUES (?, ?, ?), (?, ?, ?)]], { 5, 4, "кириллица", 6, 5, "КИРИЛЛИЦА" } })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})

    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "space_simple_shard_key"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "id", type = "integer"},
        {name = "name", type = "string"},
        {name = "sysOp", type = "integer"},
        {name = "bucket_id", type = "unsigned"},
    })
    t.assert_items_equals(r.rows, {
        {1, "ok", 1, 3940},
        {5, "КИРИЛЛИЦА", 6, 6661},
        {10, box.NULL, 0, 11520},
        {4, "кириллица", 5, 27225},
    })
end

-- check big int
g.test_insert_6 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("sysOp", "id", "name") VALUES (?, ?, ?)]], { 7, -9223372036854775808, "bigint" } })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "space_simple_shard_key"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "id", type = "integer"},
        {name = "name", type = "string"},
        {name = "sysOp", type = "integer"},
        {name = "bucket_id", type = "unsigned"},
    })
    t.assert_items_equals(r.rows, {
        { -9223372036854775808LL, "bigint", 7, 2274 },
        {1, "ok", 1, 3940},
        {10, box.NULL, 0, 11520}
    })
end

-- check empty string
g.test_insert_7 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("sysOp", "id", "name") VALUES (8, 8, '')]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "space_simple_shard_key"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "id", type = "integer"},
        {name = "name", type = "string"},
        {name = "sysOp", type = "integer"},
        {name = "bucket_id", type = "unsigned"},
    })
    t.assert_items_equals(r.rows, {
        {1, "ok", 1, 3940},
        {8, "", 8, 12104},
        {10, box.NULL, 0, 11520}
    })
end

-- TODO(ars): this test fails. What also fails is a simple query like: values (1, 'hello')...
-- check type derivation for null column in the first row of the VALUES operator
g.test_insert_8 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[VALUES (?, ?, ?), (?, ?, ?)]], { 8, 8, box.NULL, 9, 9, 'hello' } })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], {
        {name = "COLUMN_4", type = "scalar"},
        {name = "COLUMN_5", type = "scalar"},
        {name = "COLUMN_6", type = "scalar"},
    })
    t.assert_items_equals(r["rows"], { { 8, 8, box.NULL }, { 9, 9, 'hello' } })

    r, err = api:call(
        "sbroad.execute",
        { [[VALUES (?, ?, ?), (?, ?, ?)]], { 9, 9, 'hello', 8, 8, box.NULL } }
    )
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], {
        {name = "COLUMN_4", type = "scalar"},
        {name = "COLUMN_5", type = "scalar"},
        {name = "COLUMN_6", type = "scalar"},
    })
    t.assert_items_equals(r["rows"], { { 9, 9, 'hello' }, { 8, 8, box.NULL } })

    r, err = api:call("sbroad.execute", { [[VALUES (8, 8, null), (9, 9, 'hello')]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], {
        {name = "COLUMN_4", type = "unsigned"},
        {name = "COLUMN_5", type = "unsigned"},
        {name = "COLUMN_6", type = "string"},
    })
    t.assert_items_equals(r["rows"], { { 8, 8, box.NULL }, { 9, 9, 'hello' } })

    r, err = api:call("sbroad.execute", { [[VALUES (9, 9, 'hello'), (8, 8, null)]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(r["metadata"], {
        {name = "COLUMN_4", type = "unsigned"},
        {name = "COLUMN_5", type = "unsigned"},
        {name = "COLUMN_6", type = "scalar"},
    })
    t.assert_items_equals(r["rows"], { { 9, 9, 'hello' }, { 8, 8, box.NULL } })

    r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("sysOp", "id", "name") VALUES (?, ?, ?), (?, ?, ?)]], { 8, 8, box.NULL, 9, 9, 'hello' } })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})

    r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("sysOp", "id", "name") VALUES (20, 20, null), (21, 21, 'hello')]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})

    r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("sysOp", "id", "name") VALUES (?, ?, ?)]], { 22, 22, box.NULL } })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    ("sysOp", "id", "name") VALUES (23, 23, null)]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    r, err = api:call("sbroad.execute", { [[
            insert into "arithmetic_space"
            ("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
            values (?,?,?,?,?,?,?,?,?,?),
            (?,?,?,?,?,?,?,?,?,?),
            (?,?,?,?,?,?,?,?,?,?),
            (?,?,?,?,?,?,?,?,?,?)
        ]],
        {
            1, 1, 1, 1, 1, 2, 2, true, "a", 3.14,
            2, 1, 2, 1, 2, 2, 2, true, "a", 3,
            3, 2, 3, 1, 2, 2, 2, true, "c", 3.14,
            4, 2, 3, 1, 1, 2, 2, true, "c", 3.1475
        }
    })
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 4})
end

g.test_insert_9 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[INSERT INTO "space_simple_shard_key"
    SELECT * FROM "space_simple_shard_key_hist" WHERE "id" = ? AND "id" = ?]], { 1, 2 } })

    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 0})
end

g.test_insert_on_conflict_do_nothing = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    SELECT * FROM "space_simple_shard_key"
    ]]})
    t.assert_equals(err, nil)
    t.assert(r.rows ~= nil)
    local before_insert_rows = r.rows

    r, err = api:call("sbroad.execute", { [[
    INSERT INTO "space_simple_shard_key" VALUES (1, '1', 1)
    ON CONFLICT DO NOTHING
    ]]})
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 0})

    r, err = api:call("sbroad.execute", { [[
    select * from "space_simple_shard_key"
    ]]})
    t.assert_equals(err, nil)
    t.assert_items_equals(r.rows, before_insert_rows)
end

g.test_insert_select_on_conflict_do_nothing = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    SELECT * FROM "space_simple_shard_key"
    ]]})
    t.assert_equals(err, nil)
    t.assert(r.rows ~= nil)
    local before_insert_rows = r.rows

    r, err = api:call("sbroad.execute", { [[
    INSERT INTO "space_simple_shard_key"
    SELECT * FROM "space_simple_shard_key"
    ON CONFLICT DO NOTHING
    ]]})
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 0})

    r, err = api:call("sbroad.execute", { [[
    select * from "space_simple_shard_key"
    ]]})
    t.assert_equals(err, nil)
    t.assert_items_equals(r.rows, before_insert_rows)
end

g.test_insert_on_conflict_do_replace = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp")
    VALUES (1, '1', 1)
    ON CONFLICT DO REPLACE
    ]]})
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    -- check the row was replaced
    r, err = api:call("sbroad.execute", { [[
    select * from "space_simple_shard_key" ]] })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "id", type = "integer"},
        {name = "name", type = "string"},
        {name = "sysOp", type = "integer"},
    })
    t.assert_items_equals(r.rows, {
        {1, "1", 1},
        {10, nil, 0},
    })
end

g.test_insert_select_on_conflict_do_replace = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    INSERT INTO "space_simple_shard_key" ("id", "name", "sysOp")
    SELECT "id", "name" || '1', "sysOp" + 4
    FROM "space_simple_shard_key"
    ON CONFLICT DO REPLACE
    ]]})
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})

    -- check all rows were replaced
    r, err = api:call("sbroad.execute", { [[
    select * from "space_simple_shard_key" ]] })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "id", type = "integer"},
        {name = "name", type = "string"},
        {name = "sysOp", type = "integer"},
    })
    t.assert_items_equals(r.rows, {
        {1, "ok1", 5},
        {10, nil, 4},
    })
end

g.test_insert_on_conflict_do_replace_fails_for_secondary_unique_index = function()
    local api = cluster:server("api-1").net_box

    -- unique_secondary_index contains on the same storage
    -- 1, 1, 1
    -- 2, 1, 2
    -- It has primary index on the 1-st column, and unique secondary
    -- index on the 3-rd column, so the insert should fail
    local _, err = api:call("sbroad.execute", { [[
    INSERT INTO "unique_secondary_index"
    VALUES (2, 1, 1)
    ON CONFLICT DO REPLACE
    ]]})
    t.assert_str_contains(tostring(err),
            "TupleFound: Duplicate key exists in unique index \\\\\\\"secondary\\\\\\\"")
end

g.test_double_conversion = function()
    local api = cluster:server("api-1").net_box
    local query_id, query = "id1", [[
    INSERT INTO "double_t" values (1, 2.5, 2.5e-1), (1, 2.5e-1, 2.5)
    ON CONFLICT DO REPLACE
    ]]
    local params = { query, {}, nil, query_id }
    local r, err = api:call("sbroad.execute", params)
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 2})
end
