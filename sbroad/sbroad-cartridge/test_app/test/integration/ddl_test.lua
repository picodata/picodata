local t = require('luatest')
local g = t.group('sbroad_with_ddl')

local helper = require('test.helper.cluster_no_replication')
local cluster = nil

g.before_all(
    function()
        helper.start_test_cluster(helper.cluster_config)
        cluster = helper.cluster

        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "broken_hot"]] })
        storage1:call("box.execute", { [[truncate table "BROKEN"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "broken_hot"]] })
        storage2:call("box.execute", { [[truncate table "BROKEN"]] })
    end
)

g.after_all(
    function()
        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "broken_hot"]] })
        storage1:call("box.execute", { [[truncate table "BROKEN"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "broken_hot"]] })
        storage2:call("box.execute", { [[truncate table "BROKEN"]] })
        helper.stop_test_cluster()
    end
)

g.test_insert_after_index = function()
    local api = cluster:server("api-1").net_box
    local storage1 = cluster:server("storage-1-1").net_box
    local storage2 = cluster:server("storage-2-1").net_box

    -- check that at the start sbroad can select from broken_hot
    local r, err = api:call(
        "sbroad.execute",
        { [[SELECT * FROM "broken_hot" ]], {} }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
          { name = "id", type = "number" },
          { name = "reqId", type = "number" },
          { name = "name", type = "string"  },
          { name = "department", type = "string" },
          { name = "manager", type = "string" },
          { name = "salary", type = "number" },
          { name = "sysOp", type = "number" }
        },
        rows = { },
    })

    -- check that at the start BROKEN has two indexes
    local schema_storage_before = storage1:call("get_current_schema", {})
    t.assert_equals(
        schema_storage_before["spaces"]["BROKEN"]["indexes"],
        {
            {
                name = "id",
                parts = {{is_nullable = false, path = "id", type = "number"}},
                type = "TREE",
                unique = true,
            },
            {
                name = "bucket_id",
                parts = {{is_nullable = false, path = "bucket_id", type = "unsigned"}},
                type = "TREE",
                unique = false,
            }
        }
    )
    local schema_broken_hot_before = schema_storage_before["spaces"]["broken_hot"]

    local idx = {
        type = "TREE",
        unique = false,
        parts = {"department", "manager"}
    }

    -- create index on storages
    storage1:call("box.space.BROKEN:create_index", { "test_idx", idx })
    storage2:call("box.space.BROKEN:create_index", { "test_idx", idx })

    local schema_storage_after = storage1:call("get_current_schema", {})
    t.assert_equals(
        schema_storage_after["spaces"]["BROKEN"]["indexes"],
        {
            {
                name = "id",
                parts = {
                    {is_nullable = false, path = "id", type = "number"}
                },
                type = "TREE",
                unique = true,
            },
            {
                name = "bucket_id",
                parts = {
                    {is_nullable = false, path = "bucket_id", type = "unsigned"}
                },
                type = "TREE",
                unique = false,
            },
            {
                name = "test_idx",
                parts = {
                    {is_nullable = false, path = "department", type = "string"},
                    {is_nullable = false, path = "manager", type = "string"}
                },
                type = "TREE",
                unique = false,
            }
        }
    )

    -- create indexes on router
    local _ = api:call("box.space.BROKEN:create_index", { "test_idx", idx })

    -- set new schema with cartridge
    local err_set = api:call("set_schema", { schema_storage_after })
    t.assert_equals(err_set, nil)

    -- check that BROKEN got new index and broken_hot was not changed
    local c = cluster:download_config()
    t.assert_equals(
        c.schema["spaces"]["BROKEN"]["indexes"],
        {
            {
                name = "id",
                parts = {
                    {is_nullable = false, path = "id", type = "number"}
                },
                type = "TREE",
                unique = true,
            },
            {
                name = "bucket_id",
                parts = {{is_nullable = false, path = "bucket_id", type = "unsigned"}},
                type = "TREE",
                unique = false,
            },
            {
                name = "test_idx",
                parts = {
                    {is_nullable = false, path = "department", type = "string"},
                    {is_nullable = false, path = "manager", type = "string"}
                },
                type = "TREE",
                unique = false,
            }
        }
    )
    t.assert_equals(schema_broken_hot_before, c.schema["spaces"]["broken_hot"])

    -- check that sbroad can select from broken_hot still
    r, err = api:call(
        "sbroad.execute",
        { [[SELECT * FROM "broken_hot" ]], {} }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
          { name = "id", type = "number" },
          { name = "reqId", type = "number" },
          { name = "name", type = "string"  },
          { name = "department", type = "string" },
          { name = "manager", type = "string" },
          { name = "salary", type = "number" },
          { name = "sysOp", type = "number" }
        },
        rows = { },
    })

    for i = 1, 4, 1 do
        r, err = api:call("sbroad.execute", {
            [[INSERT INTO "broken_hot" VALUES(?,?,?,?,?,?,?);]],
            {
                i, 1, "123", "123", "123", 100, 0
            }
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 1})
    end

    r, err = api:call(
        "sbroad.execute",
        { [[SELECT * FROM "broken_hot" ]], {} }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
      { name = "id", type = "number" },
      { name = "reqId", type = "number" },
      { name = "name", type = "string"  },
      { name = "department", type = "string" },
      { name = "manager", type = "string" },
      { name = "salary", type = "number" },
      { name = "sysOp", type = "number" }
    })
    t.assert_items_equals(r.rows, {
        { 1, 1, "123", "123", "123", 100, 0 },
        { 2, 1, "123", "123", "123", 100, 0 },
        { 3, 1, "123", "123", "123", 100, 0 },
        { 4, 1, "123", "123", "123", 100, 0 }
    })
end

g.test_create_table = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call(
        "sbroad.execute",
        { [[
            CREATE TABLE t (
                a INT NOT NULL,
                b TEXT NOT NULL,
                PRIMARY KEY (a, b)
            )
            USING memtx
            DISTRIBUTED BY (b, a)
            OPTION (timeout = 1)
        ]], {} }
    )
    t.assert_equals(
        string.format("%s", err),
        [[Sbroad Error: DDL queries are not supported]]
    )
end
