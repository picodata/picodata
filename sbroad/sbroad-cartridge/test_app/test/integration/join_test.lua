local t = require('luatest')
local g = t.group('join_queries')

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
                [[insert into "testing_space" ("id", "name", "product_units")
                  values (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)
                ]],
                {
                    1, "a", 1,
                    2, "a", 1,
                    3, "a", 2,
                    4, "b", 1,
                    5, "b", 2,
                    6, "b", 3,
                    7, "c", 4
                },
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 7})
        end
)

g.after_each(
        function()
            local storage1 = cluster:server("storage-1-1").net_box
            storage1:call("box.execute", { [[truncate table "testing_space"]] })

            local storage2 = cluster:server("storage-2-1").net_box
            storage2:call("box.execute", { [[truncate table "testing_space"]] })
        end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g.test_join_vtable_with_same_column_names = function()
    local api = cluster:server("api-1").net_box

    local res, err = api:call("sbroad.execute", {
        [[select *
          from
            "testing_space"
          join
            (select t1."id" as f, t2."id" as s
             from
                (select "id" from "testing_space") t1
             join
                (select "id" from "testing_space") t2
             on true
            )
          on "id" = f and "id" = s]],
        {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(res.metadata, {
        {name = "id", type = "integer"},
        {name = "name", type = "string"},
        {name = "product_units", type = "integer"},
        {name = "f", type = "integer"},
        {name = "s", type = "integer"},
    })
    t.assert_items_equals(res.rows, {
        {1, "a", 1, 1, 1},
        {2, "a", 1, 2, 2},
        {3, "a", 2, 3, 3},
        {4, "b", 1, 4, 4},
        {6, "b", 3, 6, 6},
        {5, "b", 2, 5, 5},
        {7, "c", 4, 7, 7},
    })
end
