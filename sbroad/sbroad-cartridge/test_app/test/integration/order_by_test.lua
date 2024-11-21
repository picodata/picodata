local t = require('luatest')
local g = t.group('order_by_queries')

local helper = require('test.helper.cluster_no_replication')
local cluster = nil

g.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

g.before_each(
        function()
            local api = cluster:server("api-1").net_box

            local null_t_rows_to_insert = {
                {1, 2, 1},
                {2, box.NULL, 3},
                {3, 2, 3},
                {4, 3, 1},
                {5, 1, 5},
                {6, -1, 3},
                {7, 1, 1},
                {8, box.NULL, -1}
            }

            for _, row in pairs(null_t_rows_to_insert) do
                local r, err = api:call("sbroad.execute", {
                    [[
                        insert into "null_t"
                        ("na", "nb", "nc")
                        values (?,?, ?)
                    ]],
                    {row[1], row[2], row[3]},
                })
                t.assert_equals(err, nil)
                t.assert_equals(r, {row_count = 1})
            end
        end
)

g.after_each(
        function()
            local storage1 = cluster:server("storage-1-1").net_box
            storage1:call("box.execute", { [[truncate table "null_t"]] })

            local storage2 = cluster:server("storage-2-1").net_box
            storage2:call("box.execute", { [[truncate table "null_t"]] })
        end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_order_by_query = function()
    -- Make sure table is located on both storages.
    local storage1 = cluster:server("storage-1-1").net_box
    local r, err = storage1:call("box.execute", {
        [[select * from "null_t"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(true, next(r.rows) ~= nil)

    local storage2 = cluster:server("storage-2-1").net_box
    r, err = storage2:call("box.execute", {
        [[select * from "null_t"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(true, next(r.rows) ~= nil)


    local api = cluster:server("api-1").net_box

    local r_init, err = api:call("sbroad.execute", {
        [[select * from "null_t"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r_init.metadata, {
        {name = "na", type = "integer"},
        {name = "nb", type = "integer"},
        {name = "nc", type = "integer"},
    })
    t.assert_items_equals(r_init.rows, {
        {1, 2, 1},
        {5, 1, 5},
        {8, box.NULL, -1},
        {2, box.NULL, 3},
        {3, 2, 3},
        {4, 3, 1},
        {6, -1, 3},
        {7, 1, 1},
    })

    local expected_ordered_by_1_sequence = {
        {1, 2, 1},
        {2, box.NULL, 3},
        {3, 2, 3},
        {4, 3, 1},
        {5, 1, 5},
        {6, -1, 3},
        {7, 1, 1},
        {8, box.NULL, -1},
    }

    -- Order by "na" must return rows in the globally sorted order.
    local r_order_by_na, err = api:call("sbroad.execute", {
        [[select * from "null_t" order by "na"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(expected_ordered_by_1_sequence, r_order_by_na.rows)

    -- Order by 1 must return rows in the same order.
    local r_order_by_1, err = api:call("sbroad.execute", {
        [[select * from "null_t" order by 1]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(expected_ordered_by_1_sequence, r_order_by_1.rows)
    -- Order by parameter is prohibited.
    local _, err = api:call("sbroad.execute", {
        [[select * from "null_t" order by ?]], {2}
    })
    t.assert_str_contains(
            tostring(err),
            "Using parameter as a standalone ORDER BY expression doesn't influence sorting"
    )
    -- Order by "na" asc must return rows in the same order.
    local r_order_by_na_asc, err = api:call("sbroad.execute", {
        [[select * from "null_t" order by "na" asc]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(expected_ordered_by_1_sequence, r_order_by_na_asc.rows)
    -- Order by 1 asc must return rows in the same order.
    local r_order_by_1_asc, err = api:call("sbroad.execute", {
        [[select * from "null_t" order by 1 asc]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(expected_ordered_by_1_sequence, r_order_by_1_asc.rows)
    -- Order by 1, 2 must return rows in the same order.
    local r_order_by_1_asc_2_asc, err = api:call("sbroad.execute", {
        [[select * from "null_t" order by 1, 2]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(expected_ordered_by_1_sequence, r_order_by_1_asc_2_asc.rows)

    local _, err = api:call("sbroad.execute", { [[select * from "null_t" order by 4]], {} })
    t.assert_str_contains(
            tostring(err),
            "invalid expression: Ordering index (4) is bigger than child projection output length (3)"
    )

    local expected_ordered_by_2_sequence = {
        {2, box.NULL, 3},
        {8, box.NULL, -1},
        {6, -1, 3},
        {5, 1, 5},
        {7, 1, 1},
        {1, 2, 1},
        {3, 2, 3},
        {4, 3, 1},
    }

    -- Order by "nb" must keep id sorted ascending.
    local r_order_by_nb, err = api:call("sbroad.execute", {
        [[select * from "null_t" order by "nb", "na"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(expected_ordered_by_2_sequence, r_order_by_nb.rows)
    -- Order by "nb" asc must return rows in the same order.
    local r_order_by_a_asc, err = api:call("sbroad.execute", {
        [[select * from "null_t" order by "nb" asc, "na"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(expected_ordered_by_2_sequence, r_order_by_a_asc.rows)

    -- Order by 2 is equal to order by "nb".
    local r_order_by_2, err = api:call("sbroad.execute", {
        [[select * from "null_t" order by 2, 1]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(expected_ordered_by_2_sequence, r_order_by_2.rows)

    -- Order by "na" desc.
    local r_order_by_na_desc, err = api:call("sbroad.execute", {
        [[select * from "null_t" order by "na" desc]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals({
        {8, box.NULL, -1},
        {7, 1, 1},
        {6, -1, 3},
        {5, 1, 5},
        {4, 3, 1},
        {3, 2, 3},
        {2, box.NULL, 3},
        {1, 2, 1},
    }, r_order_by_na_desc.rows)

    local expected_ordered_by_2_desc_sequence = {
        {4, 3, 1},
        {1, 2, 1},
        {3, 2, 3},
        {5, 1, 5},
        {7, 1, 1},
        {6, -1, 3},
        {2, box.NULL, 3},
        {8, box.NULL, -1},
    }

    local r_order_by_nb_desc, err = api:call("sbroad.execute", {
        [[select * from "null_t" order by "nb" desc, "na"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(expected_ordered_by_2_desc_sequence, r_order_by_nb_desc.rows)

    -- Ordering by expression involving only "nb" column (not changing sign) should give the same order.
    local r_order_by_nb_expr_desc, err = api:call("sbroad.execute", {
        [[select * from "null_t" order by "nb" * 2 + 42 * "nb" desc, "na"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(expected_ordered_by_2_desc_sequence, r_order_by_nb_expr_desc.rows)

    local r_order_by_2_desc_1_desc, err = api:call("sbroad.execute", {
        [[select * from "null_t" order by "nb" desc, "na" desc]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals({
        {4, 3, 1},
        {3, 2, 3},
        {1, 2, 1},
        {7, 1, 1},
        {5, 1, 5},
        {6, -1, 3},
        {8, box.NULL, -1},
        {2, box.NULL, 3},
    }, r_order_by_2_desc_1_desc.rows)

    -- In case we request both ascending and descending ordering of the same column, we should count
    -- only the first one (ordering below is equal to `order by 2 asc, 1 desc`).
    local r_count_only_first_order_type, err = api:call("sbroad.execute", {
        [[select * from "null_t" order by 2 asc, 1 desc, 2 desc, 1 asc]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals({
        {8, box.NULL, -1},
        {2, box.NULL, 3},
        {6, -1, 3},
        {7, 1, 1},
        {5, 1, 5},
        {3, 2, 3},
        {1, 2, 1},
        {4, 3, 1},
    }, r_count_only_first_order_type.rows)
end
