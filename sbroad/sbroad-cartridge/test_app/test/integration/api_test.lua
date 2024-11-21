local t = require('luatest')
local g = t.group('integration_api')
local datetime = require('datetime')
local os = require('os')

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

        -- "datetime_t" contains:
        -- ['20.08.2021 +3', 1]
        -- ['21.08.2021 +0', 1]
        r, err = api:call("sbroad.execute", {
            [[insert into "datetime_t" ("dt", "a") values (?, ?), (?, ?)]],
            { datetime.new({day = 20,month = 8,year = 2021,tzoffset  = 180}), 1,
            datetime.new({day = 21,month = 8,year = 2021,tzoffset  = 180}), 1}
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
        storage1:call("box.execute", { [[truncate table "datetime_t"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "testing_space"]] })
        storage2:call("box.execute", { [[truncate table "testing_space_hist"]] })
        storage2:call("box.execute", { [[truncate table "space_simple_shard_key"]] })
        storage2:call("box.execute", { [[truncate table "space_simple_shard_key_hist"]] })
        storage2:call("box.execute", { [[truncate table "t"]] })
        storage2:call("box.execute", { [[truncate table "datetime_t"]] })
    end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_bucket_id_calculation = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.calculate_bucket_id", { { 1, "123" } })
    t.assert_equals(r, nil)
    t.assert_str_contains(tostring(err), "space name is required")

    r, err = api:call("sbroad.calculate_bucket_id", { "1123" })
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    r, err = api:call(
        "sbroad.calculate_bucket_id",
        {
            { id = 1, name = "123", product_units = 1 },
            "testing_space"
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    r, err = api:call("sbroad.calculate_bucket_id", { box.tuple.new{ 1, "123", 1 }, "testing_space" })
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    r, err = api:call("sbroad.calculate_bucket_id", { { 1, "123", 1 }, "testing_space" })
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

     -- calculate bucket id for space which bucket_id field is located in the middle of format
    r, err = api:call(
        "sbroad.calculate_bucket_id",
        {
            { id = 1, name = "123", product_units = 1 },
            "testing_space_bucket_in_the_middle"
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    r, err = api:call(
        "sbroad.calculate_bucket_id",
        {
            box.tuple.new{ 1, "123", 1 },
            "testing_space_bucket_in_the_middle"
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    r, err = api:call(
        "sbroad.calculate_bucket_id",
        {
            { 1, "123", 1 },
            "testing_space_bucket_in_the_middle"
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    -- incorrect input
    r, err = api:call("sbroad.calculate_bucket_id", { { 1 }, "testing_space" })
    t.assert_equals(r, nil)
    t.assert_str_contains(tostring(err), [[expected to have 3 filed(s), got 1]])

    -- Test with a "bucket_id" field in the tuple.
    r, err = api:call("sbroad.calculate_bucket_id", { { 1, "123", 1, box.NULL }, "testing_space" })
    t.assert_equals(err, nil)
    t.assert_equals(r, 360)

    r, err = api:call("sbroad.calculate_bucket_id", { { 1, "123", 1, 1, 1 }, "testing_space" })
    t.assert_equals(r, nil)
    t.assert_str_contains(tostring(err), [[expected to have 3 filed(s), got 5]])

    -- luacheck: max line length 150
    r, err = api:call("sbroad.calculate_bucket_id", { { id = 1 }, "testing_space" })
    t.assert_equals(r, nil)
    t.assert_equals(
        tostring(err),
        [[Sbroad Error: sharding key (quoted) column "name" in the quoted map {"id": "id"} (original map: {"id": Integer(1)}) not found]]
    )

    r, err = api:call("sbroad.calculate_bucket_id", { { id = 1, "123" }, "testing_space" })
    t.assert_equals(r, nil)
    t.assert_str_contains(
        tostring(err),
        [[expected string, tuple with a space name, or map with a space name as an argument]]
    )

end

g.test_incorrect_query = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.execute", { [[SELECT * FROM "testing_space" INNER JOIN "testing_space"]], {} })
    t.assert_str_contains(tostring(err), "parsing error")
end

g.test_query_errored = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.execute", { [[SELECT * FROM "NotFoundSpace"]], {} })
    t.assert_equals(
        tostring(err),
        "Sbroad Error: build query: table with name \"NotFoundSpace\" not found"
    )

    -- luacheck: max line length 140
    local _, err = api:call("sbroad.execute", { [[SELECT "NotFoundColumn" FROM "testing_space"]], {} })
    t.assert_equals(tostring(err), "Sbroad Error: build query: column with name \"NotFoundColumn\" not found")

    -- check err when params lenght is less then amount of sign `?`
    local _, err = api:call("sbroad.execute", { [[SELECT * FROM "testing_space" where "id" = ?]], {} })
    t.assert_str_contains(
        tostring(err),
        "Expected at least 1 values for parameters. Got 0"
    )
end

g.test_join_query_is_valid = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.execute", { [[SELECT * FROM "testing_space"
            INNER JOIN (SELECT "id" AS "inner_id", "name" AS "inner_name" FROM "testing_space") as t
            ON ("testing_space"."id", "testing_space"."name") = (t."inner_id", t."inner_name")
        WHERE "id" = 5 and "name" = '123']], {} })
    t.assert_equals(err, nil)
end

g.test_simple_shard_key_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT * FROM "space_simple_shard_key" where "id" = ?]], { 5 } })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "sysOp", type = "integer"},
        },
        rows = {},
    })

    r, err = api:call(
        "sbroad.execute",
        {
            [[SELECT *, "bucket_id" FROM "space_simple_shard_key" where "id" = ?]],
            { 1.000 }
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "sysOp", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            { 1, "ok", 1, 3940 }
        },
    })
end

g.test_complex_shard_key_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call(
        "sbroad.execute",
        {
            [[SELECT *, "bucket_id" FROM "testing_space" where "id" = ? and "name" = ?]],
            { 1, '457'}
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {},
    })

    r, err = api:call(
        "sbroad.execute",
        {
            [[SELECT *, "bucket_id" FROM "testing_space" where "id" = 1 and "name" = '123']],
            {}
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "product_units", type = "integer"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            { 1, "123", 1, 360 }
        },
    })

end

g.test_null_col_result = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call(
        "sbroad.execute",
        {
            [[SELECT "id", "name" FROM "space_simple_shard_key" WHERE "id" = 10]],
            {}
        }
    )
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
        },
        rows = {
            { 10, box.NULL },
        },
    })
end

g.test_anonymous_cols_naming = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT * FROM "testing_space"
    WHERE "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" > ?)
        OR "id" in (SELECT "id" FROM "space_simple_shard_key_hist" WHERE "sysOp" > ?)
    ]], { 0, 0 } })

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

g.test_decimal_double = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *, "bucket_id" FROM "t"]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "id", type = "integer"},
        {name = "a", type = "number"},
        {name = "bucket_id", type = "unsigned"},
    })
    t.assert_items_equals(r.rows, {
        {1, 4.2, 3940},
        {2, 6.66, 22072},
    })
end

g.test_bucket_id_in_join = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[SELECT * FROM "space_simple_shard_key" as "t1" JOIN (SELECT "a" FROM "t") as "t2"
        ON "t1"."id" = "t2"."a"]],
        {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "name", type = "string"},
            {name = "sysOp", type = "integer"},
            {name = "a", type = "number"},
        },
        rows = {},
    })
end

g.test_lowercase1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT id FROM "BROKEN"
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "number"},
        },
        rows = {},
    })
end

g.test_lowercase2 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "BROKEN"
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "number"},
        },
        rows = {},
    })
end

g.test_lowercase3 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "a" FROM space_t1
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "a", type = "integer"},
        },
        rows = {},
    })
end

g.test_pg_style_params1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" FROM "t"
        where "id" = $1 or "id" = $1 + 1
    ]], {1} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "id", type = "integer"},
    })
    t.assert_items_equals(r.rows, {
        {1},
        {2}
    })
end

g.test_pg_style_params2 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "id" + $1 as "id" from (
            SELECT "id" FROM "t"
            UNION ALL
            SELECT "id" FROM "space_simple_shard_key"
        )
        group by "id" + $1
        having count("id") > $1
    ]], {1} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
            {2},
        },
    })
end

g.test_datetime_select = function ()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT * from "datetime_t"
    ]]})

    t.assert_equals(err, nil)
    local expected_rows = {
        { datetime.new({day = 20,month = 8,year = 2021,tzoffset = 180}), 1},
        { datetime.new({day = 21,month = 8,year = 2021,tzoffset = 180}), 1},
    }
    t.assert_equals(r.metadata, {
        {name = "dt", type = "datetime"},
        {name = "a", type = "integer"},
    })
    t.assert_items_equals(r.rows, expected_rows)
end

g.test_datetime_insert = function ()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        insert into "datetime_t" select to_date("COLUMN_1", '%c'),
        cast("COLUMN_2" as int) from
        (values ('Thu Jan  1 03:44:00 1970', 100))
    ]]})
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})

    r, err = api:call("sbroad.execute", { [[
        SELECT * from "datetime_t"
        where "a" = 100
    ]]})

    t.assert_equals(err, nil)
    local expected_rows = {
        { datetime.new({day = 1,month = 1,year = 1970}), 100},
    }
    t.assert_equals(r.metadata, {
        {name = "dt", type = "datetime"},
        {name = "a", type = "integer"},
    })
    t.assert_items_equals(r.rows, expected_rows)
end

g.after_test('test_datetime_insert', function ()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        delete from "datetime_t" where "a" = 100
    ]]})
    t.assert_equals(err, nil)
    t.assert_equals(r, {row_count = 1})
end)

g.test_datetime_motion = function ()
    local api = cluster:server("api-1").net_box

    -- inner table for such condition is always
    -- broadcasted to all other nodes
    local r, err = api:call("sbroad.execute", { [[
        select "a" as "u" from (select "id" from "testing_space") join "datetime_t"
        on true
    ]]})

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "u", type = "integer"},
        },
        rows = { {1}, {1} },
    })
end

g.test_to_char = function ()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        select to_char("dt", 'to_char: %Y-%m-%d-%H-%M-%S-%z') from "datetime_t"
    ]]})

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "col_1", type = "string"},
    })
    t.assert_items_equals(r.rows, {
        {"to_char: 2021-08-20-00-00-00-+0300"},
        {"to_char: 2021-08-21-00-00-00-+0300"},
    })

    -- second argument is optional
    -- FIXME: https://git.picodata.io/picodata/picodata/sbroad/-/issues/645
    r, err = api:call("sbroad.execute", { [[
        select to_char(to_date("COLUMN_1", '%Y %d'), null)
        from (values ('2020 20'))
    ]]})

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "col_1", type = "string"},
        },
        rows = {
            {box.NULL},
        },
    })

    -- check we can use expressions inside to_char
    r, err = api:call("sbroad.execute", { [[
        select to_char(to_date("COLUMN_1", '%Y %d'), '%Y-%m-%d' || '-%H-%M-%S-%z')
        from (values ('2020 20'))
    ]]})

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "col_1", type = "string"},
        },
        rows = {
            {"2020-01-20-00-00-00-+0000"},
        },
    })

    -- invalid modifier used
    r, err = api:call("sbroad.execute", { [[
        select to_char(to_date("COLUMN_1", '%Y %d'), '%i-%m-%d')
        from (values ('2020 20'))
    ]]})

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "col_1", type = "string"},
        },
        rows = {
            {"i-01-20"},
        },
    })

    -- invalid argument
    -- FIXME: https://git.picodata.io/picodata/picodata/sbroad/-/issues/644
    -- we need to check function's argument types when building a plan
    local _, error = api:call("sbroad.execute", { [[
        select to_char('%d', '%i-%m-%d')
        from (values ('2020 20'))
    ]]})

    t.assert_str_contains(tostring(error), "bad argument #1 to 'format' (number expected, got string)")
end

g.test_current_date = function ()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        select currEnt_dAte from "datetime_t"
    ]]})

    -- datetime.new() returns local time,
    -- we need utc time
    local os_date = os.date("!*t")
    local current_date = datetime.new{
        day = os_date.day,
        month = os_date.month,
        year = os_date.year
    }
    current_date:set({hour=0, min=0, sec=0, nsec=0, tzoffset=0})
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "col_1", type = "datetime"},
        },
        rows = {
            {current_date},
            {current_date},
        },
    })

    r, err = api:call("sbroad.execute", { [[
        select to_char("col_1", '%Y') from (select to_date("COLUMN_2", '%Y.%m.%d') from (
          values ('2077.1.1'), ('2000.10.10')
        ))
        where "col_1" > CURRENT_DATE
    ]]})
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "col_1", type = "string"},
        },
        rows = {
            {'2077'},
        },
    })
end


g.test_union_operator_works = function ()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        select "id" from "t"
        union
        select "id" from "t"
        union
        select "id" from "t"
        union
        select "id" from "t"
        union
        select "id" from "t"
    ]] })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = { {1}, {2} },
    })
end

g.test_lower_upper = function ()
    local api = cluster:server("api-1").net_box

    local meta = {
        {name = "a", type = "string"},
        {name = "b", type = "string"},
    }
    local r, err = api:call("sbroad.execute", { [[
        select lower("COLUMN_1") as a, upper("COLUMN_1") as b from (values ('Aba'))
    ]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, meta)
    t.assert_equals(r.rows, {
        {'aba', 'ABA'}
    })

    r, err = api:call("sbroad.execute", { [[
        select upper(lower("COLUMN_1")) as a, lower(upper("COLUMN_1")) as b from (values ('Aba'))
    ]] })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, meta)
    t.assert_equals(r.rows, {
        {'ABA', 'aba'}
    })
end

g.test_like_works = function ()
    local api = cluster:server("api-1").net_box

    -- all conditions must evaluate to true
    local r, err = api:call("sbroad.execute", { [[
        select id from testing_space
        where name like '123'
        and name like '1__'
        and name like '%2_'
        and '%_%' like '\%\_\%' escape '\'
        and 'A' || 'a' like '_' || '%'
        and 'A' || '_' like '_' || '\_' escape '' || '\'
        and (values ('a')) like (values ('_'))
        and (values ('_')) like (values ('\_')) escape (values ('\'))
        and (select name from testing_space) like (select name from testing_space)
        and '_' like '\_'
        and '%' like '\%'
        and 'aBa' ilike 'aba'
        and 'B' || 'b' ilike 'b' || 'B'
        and '_' ilike '\_' escape '\'
    ]] })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = { {1} },
    })
end

g.test_select_without_scan = function ()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        select 1 as foo, 2 + 3 as bar
    ]] })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "foo", type = "unsigned"},
            {name = "bar", type = "unsigned"},
        },
        rows = { {1, 5} },
    })

    r, err = api:call("sbroad.execute", { [[
        select (select id from t where id = 1), (select count(*) from t) as bar,
        (values (30))
    ]] })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "col_1", type = "integer"},
            {name = "bar", type = "integer"},
            {name = "col_2", type = "unsigned"},
        },
        rows = { {1, 2, 30} },
    })

    r, err = api:call("sbroad.execute", { [[
        select id, (select 3 as b) from testing_space
        where id in (select 1) and name in (select '123')
    ]] })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
            {name = "col_1", type = "unsigned"},
        },
        rows = { {1, 3} },
    })

    r, err = api:call("sbroad.execute", { [[
        select 1, 2
    ]] })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "col_1", type = "unsigned"},
            {name = "col_2", type = "unsigned"},
        },
        rows = { {1, 2} },
    })

    local _
    _, err = api:call("sbroad.execute", { [[
        select (values (100, 500)) as bar
    ]] })
    t.assert_str_contains(tostring(err), "invalid expression: SubQuery expected to have 1 rows output, but got 2.")

end

