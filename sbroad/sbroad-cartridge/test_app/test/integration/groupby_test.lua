local t = require('luatest')
local groupby_queries = t.group('groupby_queries')
local helper = require('test.helper.cluster_no_replication')
local cluster = nil

groupby_queries.before_all(
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

groupby_queries.after_all(function()
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

groupby_queries.test_grouping = function()
    local api = cluster:server("api-1").net_box

    -- with GROUP BY
    local r, err = api:call("sbroad.execute", { [[
    SELECT "name"
    FROM "testing_space"
    GROUP BY "name"
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "name", type = "string" },
    })
    t.assert_items_equals(r.rows, {
        { "123" },
        { "1" },
        { "2" }
    })

    -- without GROUP BY
    local r, err = api:call("sbroad.execute", { [[
        SELECT "name"
        FROM "testing_space"
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
end

groupby_queries.expr_in_proj = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    SELECT "name" || 'p' AS "name"
    FROM "testing_space"
    GROUP BY "name"
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "name", type = "string" },
    })
    t.assert_items_equals(r.rows, {
        { "123p" },
        { "1p" },
        { "2p" }
    })

    local r, err = api:call("sbroad.execute", { [[
    SELECT "a" + "b" AS e1, "a" / "b" AS e2
    FROM "arithmetic_space"
    GROUP BY "a", "b"
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "E1", type = "integer" },
        { name = "E2", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {3, 0}, {5, 0}, {2, 1}
    })

end

groupby_queries.different_column_types = function()
    local api = cluster:server("api-1").net_box

    -- DECIMAL
    local r, err = api:call("sbroad.execute", { [[
    SELECT *
    FROM (SELECT cast("number_col" AS decimal) AS col FROM "arithmetic_space")
    GROUP BY col
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "COL", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 2 },
        { 2.14 },
        { 3.14 }
    })

    -- integer, boolean, STRING
    r, err = api:call("sbroad.execute", { [[
    SELECT "f", "boolean_col", "string_col"
    FROM "arithmetic_space"
    GROUP BY "f", "boolean_col", "string_col"
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "f", type = "integer"},
        {name = "boolean_col", type = "boolean"},
        {name = "string_col", type = "string"},
    })
    t.assert_items_equals(r.rows, {
        { 2, true, "a" },
        { 2, true, "c" },
    })

    -- SCALAR
    r, err = api:call("sbroad.execute", { [[
    SELECT *
    FROM (
        SELECT CAST("number_col" AS SCALAR) AS u FROM "arithmetic_space"
        UNION ALL
        SELECT * FROM (
            SELECT CAST("boolean_col" AS SCALAR) FROM "arithmetic_space"
            UNION ALL
            SELECT CAST("string_col" AS STRING) FROM "arithmetic_space"
        )
    )
    GROUP BY u
]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "U", type = "scalar"},
    })
    t.assert_items_equals(r.rows, {
        {2}, {true}, {2.14}, {3.14}, {"a"}, {"c"}
    })

    -- double, UNSIGNED
    r, err = api:call("sbroad.execute", { [[
    SELECT d, u
    FROM (
        SELECT CAST("number_col" AS DOUBLE) AS d, CAST("number_col" AS UNSIGNED) AS u FROM "arithmetic_space2"
    )
    GROUP BY d, u
]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "D", type = "double"},
        {name = "U", type = "unsigned"},
    })
    t.assert_items_equals(r.rows, {
        {2.717, 2}, {3.1415, 3}, {2.718, 2},
    })
end

groupby_queries.invalid = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.execute", { [[
        SELECT "id" + "product_units" FROM "testing_space" GROUP BY "id"
    ]], {} })
    t.assert_str_contains(tostring(err), "Invalid projection with GROUP BY clause")

    local _, err = api:call("sbroad.execute", { [[
        SELECT * FROM "testing_space" GROUP BY 1
    ]], {} })
    t.assert_str_contains(tostring(err), "grouping expression must contain at least one column")
    local _, err = api:call("sbroad.execute", { [[
        SELECT * FROM "testing_space" GROUP BY "id" + 1
    ]], {} })
    t.assert_str_contains(tostring(err), "Invalid projection with GROUP BY clause")
    local _, err = api:call("sbroad.execute", { [[
        SELECT ("c"*"b"*"a")*count("c")/(("b"*"a"*"c")*count("c")) as u
        from "arithmetic_space"
        group by "a"*"b"*"c"
    ]], {} })
    t.assert_str_contains(tostring(err), "Invalid projection with GROUP BY clause")

    local _, err = api:call("sbroad.execute", { [[
        SELECT "id" + count("id") FROM "testing_space" GROUP BY "id" + count("id")
    ]], {} })
    t.assert_str_contains(tostring(err), "aggregate functions are not allowed inside grouping expression")
    local _, err = api:call("sbroad.execute", { [[
        SELECT "name", "product_units" FROM "testing_space" GROUP BY "name"
    ]], {} })
    t.assert_str_contains(tostring(err), "Invalid projection with GROUP BY clause")

    local _, err = api:call("sbroad.execute", { [[
        SELECT "name" AS "q" FROM "testing_space" GROUP BY "q"
    ]], {} })
    t.assert_str_contains(tostring(err), "column with name \"q\" not found")

    local _, err = api:call("sbroad.execute", { [[
        SELECT "name", "product_units" FROM "testing_space" GROUP BY "name" "product_units"
    ]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[
        SELECT "product_units" FROM "testing_space" GROUP BY "name"
    ]], {} })
    t.assert_str_contains(tostring(err), "Invalid projection with GROUP BY clause")
end

groupby_queries.test_two_col = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "product_units", "name"
        FROM "testing_space"
        GROUP BY "product_units", "name"
    ]], {} })

    local expected = {
        { 1, "123" },
        { 1, "1" },
        { 2, "2" },
        { 2, "123" },
        { 4, "2" },
    };

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "product_units", type = "integer"},
        {name = "name", type = "string"},
    })
    t.assert_items_equals(r.rows, expected)

    r, err = api:call("sbroad.execute", { [[
        SELECT "product_units", "name"
        FROM "testing_space"
        GROUP BY "name", "product_units"
    ]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "product_units", type = "integer"},
        {name = "name", type = "string"},
    })
    t.assert_items_equals(r.rows, expected)
end

groupby_queries.test_with_selection = function ()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
        SELECT "product_units", "name"
        FROM "testing_space"
        WHERE "product_units" > ?
        GROUP BY "product_units", "name"
    ]], {1} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "product_units", type = "integer"},
        {name = "name", type = "string"},
    })
    t.assert_items_equals(r.rows, {
        { 2, "2" },
        { 2, "123" },
        { 4, "2" },
    })
end

groupby_queries.test_with_join = function ()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[
    SELECT "id", "id2"
    FROM "arithmetic_space"
    INNER JOIN
        (SELECT "id" as "id2", "a" as "a2" from "arithmetic_space2") as t
    ON "arithmetic_space"."id" = t."a2"
    GROUP BY "id", "id2"
]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "id", type = "integer" },
        { name = "id2", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 3 },
        { 1, 4 },
        { 2, 1 },
        { 2, 2 },
    })
end


groupby_queries.test_with_join2 = function ()
    local api = cluster:server("api-1").net_box
    -- with groupBY
    local r, err = api:call("sbroad.execute", { [[
    SELECT "c", q.a1
    FROM "arithmetic_space"
    INNER JOIN
        (SELECT "b" AS b1, "a" AS a1 FROM "arithmetic_space2") AS q
    ON "arithmetic_space"."c" = q.a1
    GROUP BY "c", a1
]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "c", type = "integer" },
        { name = "a1", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 1 },
    })

    -- without groupBY
    r, err = api:call("sbroad.execute", { [[
    SELECT "c", q.a1
    FROM "arithmetic_space"
    INNER JOIN
        (SELECT "b" AS b1, "a" AS a1 FROM "arithmetic_space2") AS q
    ON "arithmetic_space"."c" = q.a1
]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "c", type = "integer" },
        { name = "a1", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}
    })
end


groupby_queries.test_with_join3 = function ()
    local api = cluster:server("api-1").net_box
    local r, err = api:call("sbroad.execute", { [[
    SELECT r."i", q."b"
    FROM (SELECT "a" + "b" AS "i" FROM "arithmetic_space2" GROUP BY "a"+"b") AS r
    INNER JOIN
        (SELECT "c", "b" FROM "arithmetic_space" GROUP BY "c", "b") AS q
    ON r."i" = q."b"
    GROUP BY r."i", q."b"
]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "i", type = "integer" },
        { name = "b", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 2, 2 },
        { 3, 3 },
    })

    -- without GROUP BY
    r, err = api:call("sbroad.execute", { [[
    SELECT r."i", q."b"
    FROM (SELECT "a" AS "i" FROM "arithmetic_space2") AS r
    INNER JOIN
        (SELECT "c", "b" FROM "arithmetic_space") AS q
    ON r."i" = q."b"
]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "i", type = "integer" },
        { name = "b", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        {2, 2}, {2, 2}, {1, 1}, {1, 1}
    })

end


groupby_queries.test_with_union = function ()
    local api = cluster:server("api-1").net_box
    local r, err = api:call("sbroad.execute", {
        [[SELECT "a" FROM "arithmetic_space" GROUP BY "a" UNION ALL SELECT "a" FROM "arithmetic_space2"]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1 },
        { 2 },
        { 2 },
        { 2 },
        { 1 },
        { 1 },
    })

    r, err = api:call("sbroad.execute", {
        [[
            SELECT "a" FROM "arithmetic_space" GROUP BY "a" UNION ALL SELECT "a" FROM "arithmetic_space2" GROUP BY "a"
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1 },
        { 2 },
        { 1 },
        { 2 },
    })


    r, err = api:call("sbroad.execute", {
        [[
        SELECT "a" FROM (
        SELECT "a" FROM "arithmetic_space" GROUP BY "a" UNION ALL SELECT "a" FROM "arithmetic_space2" GROUP BY "a"
        ) GROUP BY "a"]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1 },
        { 2 },
    })
end

groupby_queries.test_with_except = function ()
    local api = cluster:server("api-1").net_box
    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "b" FROM "arithmetic_space" GROUP BY "b"
        EXCEPT
        SELECT "b" FROM "arithmetic_space2"
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "b", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 3 },
    })

    r, err = api:call("sbroad.execute", {
        [[
        SELECT * FROM (
            SELECT "a", "b" FROM "arithmetic_space" GROUP BY "a", "b"
            UNION ALL SELECT * FROM (
            SELECT "c", "d" FROM "arithmetic_space"
            EXCEPT
            SELECT "c", "d" FROM "arithmetic_space2" GROUP BY "c", "d")
        ) GROUP BY "a", "b"
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "b", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 1 },
        { 1, 2 },
        { 2, 3 },
    })

    r, err = api:call("sbroad.execute", {
        [[
        SELECT "b" FROM "arithmetic_space"
        EXCEPT
        SELECT "b" FROM "arithmetic_space2"
        GROUP BY "b"
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "b", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 3 },
    })
end

groupby_queries.test_with_subquery_1 = function ()
    local api = cluster:server("api-1").net_box

    -- with GROUP BY
    local r, err = api:call("sbroad.execute", {
        [[
        SELECT * FROM (
            SELECT "a", "b" FROM "arithmetic_space2"
            GROUP BY "a", "b"
        )
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "b", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 2, 1 },
        { 2, 2 },
        { 1, 1 },
    })

    -- without GROUP BY
    r, err = api:call("sbroad.execute", {
        [[
        SELECT * FROM (
            SELECT "a", "b" FROM "arithmetic_space2"
        )
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "b", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 2, 1 },
        { 2, 2 },
        { 1, 1 },
        { 1, 1 },
    })
end


groupby_queries.test_with_subquery_2 = function ()
    local api = cluster:server("api-1").net_box
    local r, err = api:call("sbroad.execute", {
        [[
            SELECT cast("number_col" AS integer) AS k FROM "arithmetic_space" GROUP BY "number_col"
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "k", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 2 },
        { 2 },
        { 3 },
    })

    r, err = api:call("sbroad.execute", {
        [[
        SELECT "f" FROM "arithmetic_space2"
        WHERE "id" in (SELECT cast("number_col" AS integer) FROM "arithmetic_space" GROUP BY "number_col")
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "f", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 2 },
        { 2 },
    })

    r, err = api:call("sbroad.execute", {
        [[
        SELECT "f" FROM "arithmetic_space2"
        WHERE "id" in (SELECT cast("number_col" AS integer) FROM "arithmetic_space" GROUP BY "number_col")
        GROUP BY "f"
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "f", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 2 },
    })
end

groupby_queries.test_less_cols_in_proj = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
            SELECT "c" FROM "arithmetic_space"
            GROUP BY "c", "d"
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "c", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1 }, { 1 }
    })
end

groupby_queries.test_with_subquery_3 = function ()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "b", "string_col" FROM
        (SELECT "b", "string_col" FROM "arithmetic_space2" GROUP BY "b", "string_col") AS t1
        INNER JOIN
        (SELECT "id" FROM "testing_space" WHERE "id" in (SELECT "a" FROM "arithmetic_space" GROUP BY "a")) AS t2
        on t2."id" = t1."b"
        WHERE "b" in (SELECT "c" FROM "arithmetic_space" GROUP BY "c")
        GROUP BY "b", "string_col"
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "b", type = "integer" },
        { name = "string_col", type = "string" },
    })

    t.assert_items_equals(r.rows, {
        { 1, "a" },
        { 1, "b" },
    })
end

groupby_queries.test_complex_1 = function ()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT * FROM (
            SELECT "b" FROM "arithmetic_space"
            WHERE "a" in
                (SELECT "a" FROM "arithmetic_space2" WHERE "a" in
                    (SELECT "b" FROM "arithmetic_space" GROUP BY "b")
                GROUP BY "a")
            GROUP BY "b"
            UNION ALL SELECT * FROM (
                SELECT "b" FROM "arithmetic_space2"
                EXCEPT
                SELECT "a" FROM "arithmetic_space"
            )
        )
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "b", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1 },
        { 3 },
        { 2 }
    })
end

groupby_queries.test_complex_2 = function ()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT * FROM (
            SELECT "b" FROM "arithmetic_space"
            WHERE "c" in
                (SELECT "id" FROM "arithmetic_space2" WHERE "id" in
                    (SELECT "b" FROM "arithmetic_space" GROUP BY "b")
                GROUP BY "id")
            GROUP BY "b"
            UNION ALL
            SELECT * FROM (
                SELECT "c" FROM "arithmetic_space2"
                WHERE "id" = ? or "b" = ?
                GROUP BY "c"
                EXCEPT
                SELECT "a" FROM "arithmetic_space" GROUP BY "a")
        )
        ]], {2, 1}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "b", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1 },
        { 3 },
        { 2 },
    })
end

groupby_queries.test_count_works = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "d", "e" from "arithmetic_space"
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "d", type = "integer" },
        { name = "e" , type = "integer" }
    })

    -- So if group by "d" in each group there will be two values
    t.assert_items_equals(r.rows, {
        { 1, 2, },
        { 2, 2 },
        { 2, 2 },
        { 1, 2 },
    })
    r, err = api:call("sbroad.execute", {
        [[
        SELECT "d", count("e") from "arithmetic_space"
        group by "d"
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "d", type = "integer" },
        { name = "col_1" , type = "decimal" }
    })

    t.assert_items_equals(r.rows, {
        { 1, 2, },
        { 2, 2 },
    })
end

groupby_queries.test_count = function()
    local api = cluster:server("api-1").net_box
    local r, err = api:call("sbroad.execute", {
        [[
        select cs, count("d") from (
            SELECT "d", count("e") + count("e" + "d") as cs from "arithmetic_space"
            group by "d"
        ) as t
        where t."d" > 1
        group by cs
        ]], {}
    })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "cs", type = "integer" },
        { name = "col_1" , type = "decimal" }
    })

    t.assert_items_equals(r.rows, {
        {4, 1}
    })
end

groupby_queries.test_aggr_invalid = function()
    local api = cluster:server("api-1").net_box

    -- Aggregate function inside aggregate function makes no sense
    local _, err = api:call("sbroad.execute", {
        [[ SELECT "d", count(sum("e")) from "arithmetic_space" group by "d"]], {}
    })
    t.assert_str_contains(tostring(err), "aggregate function inside aggregate function")
end

groupby_queries.test_groupby_arith_expression = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[ SELECT ("a"*"b"*"c")*count("c")/(("a"*"b"*"c")*count("c")) as u from "arithmetic_space"
        group by ("a"*"b"*"c")]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "u", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        {1}, {1}, {1},
    })
end

groupby_queries.test_grouping_by_concat = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "string_col2" || "string_col" as u from
        (select "id" as "i", "string_col" as "string_col2" from "arithmetic_space") as "t1"
        join "arithmetic_space2" on "t1"."i" = "arithmetic_space2"."id"
        group by "string_col2" || "string_col"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "u", type = "string" },
    })
    t.assert_items_equals(r.rows, {
        {"aa"}, {"cb"},
    })
end

groupby_queries.test_groupby_bool_expr = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "b1" and "b1" as "c1", "boolean_col" or "b1" as "c2" from
        (select "id" as "i", "boolean_col" as "b1" from "arithmetic_space") as "t1"
        join "arithmetic_space2" on "t1"."i" = "arithmetic_space2"."id"
        group by "b1" and "b1", "boolean_col" or "b1"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "c1", type = "boolean"},
        {name = "c2", type = "boolean"},
    })
    t.assert_items_equals(r.rows, {
        {true, true},
    })
end

groupby_queries.test_grouping_by_cast_expr = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT cast("number_col" as double) from "arithmetic_space"
        group by cast("number_col" as double)
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "col_1", type = "double"},
    })
    t.assert_items_equals(r.rows, {
        {3.14}, {2}, {2.14}
    })
end


groupby_queries.test_aggr_valid = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[ SELECT sum("e" + "d") from "arithmetic_space"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        { 14 },
    })

    r, err = api:call("sbroad.execute", {
        [[ SELECT "d", count("e" + "d") from "arithmetic_space" group by "d"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "d", type = "integer" },
        { name = "col_1", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        {1, 2},
        {2, 2},
    })
    r, err = api:call("sbroad.execute", {
        [[ SELECT "d", couNT ("e") from "arithmetic_space" group by "d"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "d", type = "integer" },
        { name = "col_1", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        {1, 2},
        {2, 2},
    })

    r, err = api:call("sbroad.execute", {
        [[ SELECT "d", count("e" * 10 + "a") from "arithmetic_space2" group by "d"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "d", type = "integer" },
        { name = "col_1", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        {1, 3},
        {3, 1},
    })

    r, err = api:call("sbroad.execute", {
        [[ SELECT "d", sum("e") = sum("b"), sum("e") != sum("a"), sum("e") > count("a"),
        (sum("e") > count("a")) or (sum("e") = sum("b")) from "arithmetic_space2" group by "d"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "d", type = "integer" },
        { name = "col_1", type = "boolean" },
        { name = "col_2", type = "boolean" },
        { name = "col_3", type = "boolean" },
        { name = "col_4", type = "boolean" },
    })
    t.assert_items_equals(r.rows, {
        {1, false, true, true, true},
        {3, true, false, true, true},
    })

    r, err = api:call("sbroad.execute", {
        [[ SELECT "d", sum(("d" + "c")) from "arithmetic_space2" group by "d"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "d", type = "integer" },
        { name = "col_1", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        {1, 6},
        {3, 4},
    })
    r, err = api:call("sbroad.execute", {
        [[ SELECT "d", count(("d" < "id")) from "arithmetic_space" group by "d"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "d", type = "integer" },
        { name = "col_1", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        {1, 2},
        {2, 2},
    })
    r, err = api:call("sbroad.execute", {
        [[ SELECT "c", count(("b" in ("id"))) as ss from "arithmetic_space2" group by "c"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "c", type = "integer" },
        { name = "ss", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        {1, 4},
    })
end

groupby_queries.test_union_single = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
            SELECT count("e") from (SELECT "e" from "arithmetic_space"
            GROUP BY "e"
            UNION ALL
            select * from (SELECT sum("e" + "d") from "arithmetic_space"
            UNION ALL
            SELECT sum("e") from "arithmetic_space"))
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        { 3 },
    })
end

groupby_queries.test_except_single = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
            SELECT "e" from "arithmetic_space"
            GROUP BY "e"
            EXCEPT
            SELECT * from (
            SELECT sum("e" + "d") from "arithmetic_space"
            EXCEPT
            SELECT sum("e") from "arithmetic_space")
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "e", type = "integer" }
    })
    t.assert_items_equals(r.rows, {
        { 2 },
    })
    r, err = api:call("sbroad.execute", {
        [[
            SELECT "e" from "arithmetic_space"
            GROUP BY "e"
            EXCEPT
            SELECT sum("e" + "d") from "arithmetic_space"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "e", type = "integer" }
    })
    t.assert_items_equals(r.rows, {
        { 2 },
    })
    r, err = api:call("sbroad.execute", {
        [[
            SELECT sum("e" + "d") from "arithmetic_space"
            EXCEPT
            SELECT "e" from "arithmetic_space"
            WHERE "id" > 2
            GROUP BY "e"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        { 14 },
    })
end

-- uncomment when https://git.picodata.io/picodata/tarantool/-/issues/35
-- is fixed.
--[==[
groupby_queries.test_join_single1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[  select o.a, o.b from (select sum("a") as a, sum("b") as b from "arithmetic_space") as o
            inner join (select sum("a") as c, sum("b") as d from "arithmetic_space") as i
            on (o.a, o.b) = (i.c, i.d)
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "O.A", type = "decimal" },
        { name = "O.B", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 6, 9 },
    })
end

groupby_queries.test_join_single2 = function()
    local api = cluster:server("api-1").net_box
    local r, err = api:call("sbroad.execute", {
        [[  select o.a, o.b from (select sum("a") as a, sum("b") as b from "arithmetic_space") as o
            inner join (select sum("a") as c, sum("b") as d from "arithmetic_space") as i
            on o.a = i.c and o.b = i.d or o.a = 1
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "O.A", type = "decimal" },
        { name = "O.B", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 6, 9 },
    })
end

groupby_queries.test_join_single3 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[  select o.a, o.b from (select sum("a") as a, sum("b") as b from "arithmetic_space") as o
            inner join (select sum("a") as c, sum("b") as d from "arithmetic_space") as i
            on o.a = i.c and o.b = i.d and i.c in (6, 10) or o.a = 1
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "O.A", type = "decimal" },
        { name = "O.B", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 6, 9 },
    })
end

groupby_queries.test_having_inside_sq = function()
    local api = cluster:server("api-1").net_box

    -- check we can execute sq-s containing having clause
    local r, err = api:call("sbroad.execute", {
        [[
        SELECT t1.s1, t2.s2 from (
            select sum(distinct "a") as s1 from "arithmetic_space"
            having sum("a") > 3
        ) as t1 inner join (
            select sum(distinct "a") as s2 from "arithmetic_space"
            having sum("a") > 3
        ) as t2 on t1.s1 = t2.s2 and t1.s1 in (
            select sum(distinct "a") as s1 from "arithmetic_space"
            having sum("a") > 3
        )
        where t2.s2 in (
            select sum(distinct "a") as s1 from "arithmetic_space"
            having sum("a") > 3
        )
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "T1.S1", type = "decimal" },
        { name = "T2.S2", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 3, 3 }
    })
end

groupby_queries.test_join_single4 = function()
    local api = cluster:server("api-1").net_box
    local r, err = api:call("sbroad.execute", {
        [[  select o.a, o.b from (select sum("a") as a, sum("b") as b from "arithmetic_space") as o
            inner join (select sum("a") as c, sum("b") as d from "arithmetic_space") as i
            on o.a = i.c or o.b = i.d
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "O.A", type = "decimal" },
        { name = "O.B", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 6, 9 },
    })
end

groupby_queries.test_join_single5 = function()
    local api = cluster:server("api-1").net_box
    local r, err = api:call("sbroad.execute", {
        [[  select o.a, o.b from (select sum("a") as a, sum("b") as b from "arithmetic_space") as o
            inner join (select sum("a") as c, sum("b") as d from "arithmetic_space") as i
            on o.a = 6 and o.b = 9 or i.c = 1 and i.d = 2
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "O.A", type = "decimal" },
        { name = "O.B", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 6, 9 },
    })
end
--]==]

groupby_queries.test_join_single6 = function()
    local api = cluster:server("api-1").net_box

    local expected_metadata = {
        { name = "a", type = "decimal" },
        { name = "b", type = "integer" },
        { name = "c", type = "integer" },
        { name = "d", type = "integer" },
    }
    local expected_rows = {
        { 6, 4, 4, 6 },
        { 6, 4, 4, 6 },
        { 6, 4, 4, 5 },
        { 6, 4, 4, 5 },
    }
    local r, err = api:call("sbroad.execute", {
        [[  select o.a, o.b, i.c, i.d from (select sum("a") as a, count("b") as b from "arithmetic_space") as o
            inner join (select "c" + 3 as c, "d" + 4 as d from "arithmetic_space") as i
            on o.a = i.d or o.b = i.c
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, expected_metadata)
    t.assert_items_equals(r.rows, expected_rows)
    local r, err = api:call("sbroad.execute", {
        [[  select o.a, o.b, i.c, i.d from  (select "c" + 3 as c, "d" + 4 as d from "arithmetic_space") as i
            inner join (select sum("a") as a, count("b") as b from "arithmetic_space") as o
            on o.a = i.d or o.b = i.c and i.c in (select "id" from "arithmetic_space")
            where o.a > 5
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, expected_metadata)
    t.assert_items_equals(r.rows, expected_rows)
end

groupby_queries.test_join_single7 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[  select i.a, o.d from  (select "c" + 3 as c, "d" + 4 as d from "arithmetic_space") as o
            inner join (select sum("a") as a, count("b") as b from "arithmetic_space") as i
            on i.a = cast(o.d as number)
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "decimal" },
        { name = "d", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 6, 6 },
        { 6, 6 },
    })
end

groupby_queries.test_join_single8 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[  select i.a, o.d from  (select "c" + 3 as c, "d" + 4 as d from "arithmetic_space") as o
            inner join (select sum("a") as a, count("b") as b from "arithmetic_space") as i
            on i.a < 10
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "decimal" },
        { name = "d", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 6, 6 },
        { 6, 6 },
        { 6, 5 },
        { 6, 5 },
    })
end

groupby_queries.test_join_single9 = function()
    local api = cluster:server("api-1").net_box
    local r, err = api:call("sbroad.execute", {
        [[  select i.a, o.d from  (select "c" as c, "d" as d from "arithmetic_space" group by "c", "d") as o
            inner join (select sum("a") as a, count("b") as b from "arithmetic_space") as i
            on i.a < o.d + 5
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "decimal" },
        { name = "d", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 6, 2 },
    })
end

groupby_queries.test_join_single10 = function()
    local api = cluster:server("api-1").net_box
    local r, err = api:call("sbroad.execute", {
        [[  select i.a, o.d from  (select "c" as c, "d" as d from "arithmetic_space") as o
            inner join (select sum("a") as a, count("b") as b from "arithmetic_space") as i
            on i.a = o.d + 4 and i.b = o.c + 3
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "decimal" },
        { name = "d", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 6, 2 },
        { 6, 2 },
    })
end

groupby_queries.test_aggr_distinct = function()
    local api = cluster:server("api-1").net_box

    -- "d" has two groups: 1 and 2, each group contains only one value of "e"
    -- "e" + "a" has two unique values in each group of "d"
    local r, err = api:call("sbroad.execute", {
        [[ SELECT "d", count(distinct "e"), count(distinct "e"+"a"), count(distinct "e"+"a") + sum(distinct "d"),
           sum("d") from "arithmetic_space" group by "d" ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "d", type = "integer" },
        { name = "col_1", type = "integer" },
        { name = "col_2", type = "integer" },
        { name = "col_3", type = "decimal" },
        { name = "col_4", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        {1, 1, 2, 3, 2},
        {2, 1, 2, 4, 4},
    })
end

groupby_queries.test_aggr_distinct_without_groupby = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT sum(distinct "d"), count("e"+"a"),
            count(distinct "e"+"a"), count(distinct "e"+"a") + sum(distinct "d"),
            sum("d") from "arithmetic_space"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
        { name = "col_2", type = "decimal" },
        { name = "col_3", type = "integer" },
        { name = "col_4", type = "decimal" },
        { name = "col_5", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        {3, 4, 2, 5, 6},
    })
end

groupby_queries.test_select_distinct = function()
    local api = cluster:server("api-1").net_box


    local distinct_resp, err = api:call("sbroad.execute", {
        [[
        SELECT distinct "a"*2 from "arithmetic_space"
        ]], {}
    })
    t.assert_equals(err, nil)
    local groupby_resp, err = api:call("sbroad.execute", {
        [[
        SELECT "a"*2 from "arithmetic_space"
        group by "a"*2
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_items_equals(distinct_resp.rows, groupby_resp.rows)
end

groupby_queries.test_select_distinct2 = function()
    local api = cluster:server("api-1").net_box


    local r, err = api:call("sbroad.execute", {
        [[
        SELECT distinct * from
        (select "e", "f" from "arithmetic_space")
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "e", type = "integer" },
        { name = "f", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {2, 2}
    })
end

groupby_queries.test_select_distinct3 = function()
    local api = cluster:server("api-1").net_box


    local r, err = api:call("sbroad.execute", {
        [[
        SELECT distinct sum("e") from
        (select "e" from "arithmetic_space")
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 8 }
    })
end

groupby_queries.test_select_distinct4 = function()
    local api = cluster:server("api-1").net_box


    local with_distinct, err = api:call("sbroad.execute", {
        [[
        SELECT distinct sum("e") from
        (select "e", "f" from "arithmetic_space")
        group by "f"
        ]], {}
    })
    t.assert_equals(err, nil)
    local without_distinct, err = api:call("sbroad.execute", {
        [[
        SELECT sum("e") from
        (select "e", "f" from "arithmetic_space")
        group by "f"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_items_equals(with_distinct.rows, without_distinct.rows)
end

groupby_queries.test_count_asterisk = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT count(*) from "arithmetic_space"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        {4}
    })
    local api = cluster:server("api-1").net_box

    -- check on table with nulls
    r, err = api:call("sbroad.execute", {
        [[
        SELECT count(*) from "null_t"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        {5}
    })
end

groupby_queries.test_count_asterisk_with_groupby = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT count(*), "nb" from "null_t" group by "nb"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
        { name = "nb", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {4, nil},
        {1, 1}
    })
end

groupby_queries.test_avg = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT avg("c"), avg(distinct "c"), avg("b"), avg(distinct "b") from "arithmetic_space"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
        { name = "col_2", type = "decimal" },
        { name = "col_3", type = "decimal" },
        { name = "col_4", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        {1, 1, 2.25, 2}
    })
end

groupby_queries.test_avg_with_groupby = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "a", avg("b"), avg(distinct "b") FROM "arithmetic_space"
        GROUP BY "a"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "col_1", type = "decimal" },
        { name = "col_2", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        {1, 1.5, 1.5},
        {2, 3, 3},
    })
end

groupby_queries.test_group_concat = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT group_concat(cast("c" as string), ' '), group_concat(distinct cast("c" as string))
        from "arithmetic_space"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "string" },
        { name = "col_2", type = "string" },
    })
    t.assert_items_equals(r.rows, {
        {"1 1 1 1", "1"}
    })
end

groupby_queries.test_group_concat_with_groupby = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "a", group_concat(cast("e" as string), '|'), group_concat(distinct cast("e" as string))
        FROM "arithmetic_space"
        GROUP BY "a"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "col_1", type = "string" },
        { name = "col_2", type = "string" },
    })
    t.assert_items_equals(r.rows, {
        {1, "2|2", "2"},
        {2, "2|2", "2"},
    })
end

groupby_queries.test_min = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT min("id"), min(distinct "d" / 2) from "arithmetic_space"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "integer" },
        { name = "col_2", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {1, 0}
    })
end

groupby_queries.test_min_with_groupby = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "a", min("b"), min(distinct "b") FROM "arithmetic_space"
        GROUP BY "a"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "col_1", type = "integer" },
        { name = "col_2", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {1, 1, 1},
        {2, 3, 3},
    })
end

groupby_queries.test_max = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT max("id"), max(distinct "d" / 2) from "arithmetic_space"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "integer" },
        { name = "col_2", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {4, 1}
    })
end

groupby_queries.test_max_with_groupby = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "a", max("b"), max(distinct "b") FROM "arithmetic_space"
        GROUP BY "a"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "col_1", type = "integer" },
        { name = "col_2", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {1, 2, 2},
        {2, 3, 3},
    })
end

groupby_queries.test_total = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT total("id"), total(distinct "d" / 2) from "arithmetic_space"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "double" },
        { name = "col_2", type = "double" },
    })
    t.assert_items_equals(r.rows, {
        {10, 1}
    })
end

groupby_queries.test_distinct_asterisk_single_column = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        WITH first  AS (SELECT "id" FROM "arithmetic_space" WHERE "id" = 1),
             second AS (SELECT "id" FROM "arithmetic_space2" WHERE "id" = 2)
        SELECT DISTINCT * FROM first JOIN second ON TRUE
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "id", type = "integer" },
        { name = "id", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {1, 2},
    })
end

groupby_queries.test_distinct_asterisk_several_columns = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        WITH first  AS (SELECT "id", "a" FROM "arithmetic_space"),
             second AS (SELECT "id", "a" FROM "arithmetic_space2")
        SELECT DISTINCT * FROM first JOIN second ON first."id" = second."id"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "id", type = "integer" },
        { name = "a", type = "integer" },
        { name = "id", type = "integer" },
        { name = "a", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {1, 1, 1, 2},
        {2, 1, 2, 2},
        {3, 2, 3, 1},
        {4, 2, 4, 1},
    })
end

groupby_queries.test_total_no_rows = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT total("id") from (
            select * from "arithmetic_space" inner join
            "null_t" on false
        )
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "double" },
    })
    t.assert_items_equals(r.rows, {
        { 0 }
    })
end

groupby_queries.test_total_null_rows = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT total("nb") from (
            select * from "arithmetic_space" left join
            "null_t" on false
        )
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "double" },
    })
    t.assert_items_equals(r.rows, {
        { 0 }
    })
end

groupby_queries.test_sum_no_rows = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT sum("id") from (
            select * from "arithmetic_space" inner join
            "null_t" on false
        )
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { nil }
    })
end

groupby_queries.test_sum_null_rows = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT sum("nb") from (
            select * from "arithmetic_space" left join
            "null_t" on false
        )
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { nil }
    })
end

groupby_queries.test_total_with_groupby = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "a", total("b"), total(distinct "b") FROM "arithmetic_space"
        GROUP BY "a"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "col_1", type = "double" },
        { name = "col_2", type = "double" },
    })
    t.assert_items_equals(r.rows, {
        {1, 3, 3},
        {2, 6, 3},
    })
end

groupby_queries.test_having_with_sq = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[
        SELECT "a", sum(distinct "b") as "sum", count(distinct "b") as "count" from "arithmetic_space"
        group by "a"
        ]];
    -- without having
    local r, err = api:call("sbroad.execute", {
        query_str, {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "sum", type = "decimal" },
        { name = "count", type = "integer" }
    })
    t.assert_items_equals(r.rows, {
        {1, 3, 2},
        {2, 3, 1}
    })
    -- with having
    r, err = api:call("sbroad.execute", {
        query_str ..
        [[ having count(distinct "b") in (select "a" from "arithmetic_space" where "a" = 2)]],
        {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "sum", type = "decimal" },
        { name = "count", type = "integer" }
    })
    t.assert_items_equals(r.rows, {
        {1, 3, 2},
    })
end

groupby_queries.test_having1 = function()
    local api = cluster:server("api-1").net_box

    -- with having
    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "a", sum("b") as "sum" from "arithmetic_space"
        group by "a"
        having sum("b") > 5
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "sum", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        {2, 6}
    })
    -- without having
    r, err = api:call("sbroad.execute", {
        [[
        SELECT "a", sum("b") as "sum" from "arithmetic_space"
        group by "a"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "sum", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        {2, 6},
        {1, 3}
    })
end

groupby_queries.test_having2 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "a", sum("b") as "sum" from "arithmetic_space"
        group by "a"
        having "a" = 1
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "sum", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        {1, 3}
    })
end

groupby_queries.test_having3 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "boolean_col", sum(distinct "f") as "sum" from "arithmetic_space"
        group by "boolean_col"
        having "boolean_col" = true
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "boolean_col", type = "boolean" },
        { name = "sum", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        {true, 2}
    })
end

groupby_queries.test_having_no_groupby = function()
    local api = cluster:server("api-1").net_box

    -- having condition is true
    local r, err = api:call("sbroad.execute", {
        [[
        SELECT sum("a"), sum(distinct "a"), count("a"), count(distinct "a") from "arithmetic_space"
        having count(distinct "a") > 1
        ]], {}
    })
    t.assert_equals(err, nil)
    -- todo: if executed on single tarantool instance, types are: (integer, integer, integer, integer)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
        { name = "col_2", type = "decimal" },
        { name = "col_3", type = "decimal" },
        { name = "col_4", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {6, 3, 4, 2}
    })
    -- having condition is false
    r, err = api:call("sbroad.execute", {
        [[
        SELECT sum("a"), sum(distinct "a"), count("a"), count(distinct "a") from "arithmetic_space"
        having count(distinct "a") > 100
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
        { name = "col_2", type = "decimal" },
        { name = "col_3", type = "decimal" },
        { name = "col_4", type = "integer" },
    })
    t.assert_items_equals(r.rows, {})
end

groupby_queries.test_having_selection = function()
    local api = cluster:server("api-1").net_box

    -- with having
    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "string_col", count(distinct "string_col"), count("string_col")
        from "arithmetic_space"
        where "id" > 2 or "string_col" = 'a'
        group by "string_col"
        having sum(distinct "a") > 1
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "string_col", type = "string" },
        { name = "col_1", type = "integer" },
        { name = "col_2", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        { 'c', 1, 2 }
    })
    -- without having
    r, err = api:call("sbroad.execute", {
        [[
        SELECT "string_col", count(distinct "string_col"), count("string_col")
        from "arithmetic_space"
        where "id" > 2 or "string_col" = 'a'
        group by "string_col"
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "string_col", type = "string" },
        { name = "col_1", type = "integer" },
        { name = "col_2", type = "decimal" }
    })
    t.assert_items_equals(r.rows, {
        { 'a', 1, 2 },
        { 'c', 1, 2 }
    })
end

groupby_queries.test_having_join = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT sum(distinct "a"), "b", s
        from "arithmetic_space" as t1 inner join
        (
            select cast(sum("a") / 6 as integer) as s
            from "arithmetic_space2"
            having count(distinct "a") > 1
        ) as t2
        on t1."c" = t2.s
        group by s, t1."b"
        having sum(distinct "a") in (2, 3) and count(distinct t2.s) = 1
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
        { name = "b", type = "integer" },
        { name = "s", type = "integer" }
    })
    t.assert_items_equals(r.rows, {
        { 2, 3, 1 }
    })
end

groupby_queries.test_having_full_query = function()
    local api = cluster:server("api-1").net_box

    -- data after join + where
    local r, err = api:call("sbroad.execute", {
        [[
        SELECT t1."a", t2.b, t2.s, t1."d"
        from "arithmetic_space" as t1 inner join
        (select "b" as b, "string_col" as s from "arithmetic_space2") as t2
        on t1."a" = t2.b
        where t1."d" + t1."a" > 2
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "b", type = "integer" },
        { name = "s", type = "string" },
        { name = "d", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {1, 1, "a", 2},
        {1, 1, "b", 2},
        {1, 1, "b", 2},
        {2, 2, "a", 2},
        {2, 2, "a", 1},
    })
    r, err = api:call("sbroad.execute", {
        [[
        SELECT t1."a", count(distinct s)
        from "arithmetic_space" as t1 inner join
        (select "b" as b, "string_col" as s from "arithmetic_space2") as t2
        on t1."a" = t2.b
        where t1."d" + t1."a" > 2
        group by "a"
        having count(distinct s) > 1
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "col_1", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 2 }
    })
end

groupby_queries.test_having_inside_union = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
            select sum(distinct "e") from "arithmetic_space"
            having count(distinct "d") > 1
            union all
            select "b" from "arithmetic_space"
            group by "b"
            having count(distinct "d") > 1
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        {2},
        {3}
    })
end

groupby_queries.test_having_inside_union1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
            select "e" from "arithmetic_space"
            union all
            select "b" from "arithmetic_space"
            group by "b"
            having count(distinct "d") > 1
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "e", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {2},
        {2},
        {2},
        {2},
        {3}
    })
end

groupby_queries.test_having_inside_except = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
            select count("e") - 1 from "arithmetic_space"
            having count("e") = 4
            except
            select "b" from "arithmetic_space"
            group by "b"
            having count(distinct "d") > 1
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col_1", type = "integer" },
    })
    t.assert_items_equals(r.rows, {})
end

groupby_queries.test_having_inside_except1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
            select "d" from "arithmetic_space"
            group by "d"
            except
            select sum(distinct "c") from "arithmetic_space"
            having count(distinct "c") = 1
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "d", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {2},
    })
end

groupby_queries.test_sum_on_decimal_col = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.execute", {
        [[
            select sum("number_col") as s from "arithmetic_space"
        ]], {}
    })
    t.assert_str_contains(tostring(err), "can't compute sum on argument with type number")

    _, err = api:call("sbroad.execute", {
        [[
            select total("number_col") as s from "arithmetic_space"
        ]], {}
    })
    t.assert_str_contains(tostring(err), "can't compute total on argument with type number")

    _, err = api:call("sbroad.execute", {
        [[
            select avg("number_col") as s from "arithmetic_space"
        ]], {}
    })
    t.assert_str_contains(tostring(err), "can't compute avg on argument with type number")
end
