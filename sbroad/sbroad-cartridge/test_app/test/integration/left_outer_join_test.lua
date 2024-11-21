local t = require('luatest')
local d = require('decimal')
local left_join = t.group('left_join')
local helper = require('test.helper.cluster_no_replication')
local cluster = nil

-- luacheck: no max line length
left_join.before_all(
        function()
            helper.start_test_cluster(helper.cluster_config)
            cluster = helper.cluster

            local api = cluster:server("api-1").net_box

            local r, err = api:call("sbroad.execute", {
                [[
                    INSERT INTO "arithmetic_space"(
                        "id", "a", "b", "c", "d", "e", "f",
                        "boolean_col", "string_col", "number_col"
                    )
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
                    INSERT INTO "arithmetic_space2"(
                        "id", "a", "b", "c", "d", "e", "f",
                        "boolean_col", "string_col", "number_col"
                    )
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

            for i =1,6 do
                local values = {
                    i,          -- yearquarter (integer)
                    "a",        -- a_to (string)
                    "a",        -- b_to (string)
                    "a",        -- a_from (string)
                    "a",        -- b_from (string)
                    d.new(i),   -- c_by_ab (decimal)
                    d.new(i),   -- d_by_ab (decimal)
                    d.new(i),   -- d_c_diff (decimal)
                    "a",        -- field1 (string)
                    "a"         -- field2 (string)
                }
                r, err = api:call("sbroad.execute", {
                    [[
                        INSERT INTO "SPACE1"(
                            "yearquarter","a_to","b_to","a_from","b_from",
                            "c_by_ab","d_by_ab","d_c_diff","field1","field2"
                        )
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    ]], values
                })
                t.assert_equals(err, nil)
                t.assert_equals(r, {row_count = 1})
            end
            for i =4,10 do
                local values = {
                    i,          -- id (integer)
                    i,          -- yearquarter (integer)
                    "a",        -- a (string)
                    "a",        -- b (string)
                    "a",        -- name (string)
                    i,          -- field1 (integer)
                    d.new(i),   -- field2 (decimal)
                    "a",        -- field3 (string)
                    i,          -- field4 (integer)
                    "a",        -- field5 (string)
                    d.new(i),   -- field6 (decimal)
                    d.new(i),   -- field7 (decimal)
                    d.new(i),   -- field8 (decimal)
                    i,          -- field9 (integer)
                    i,          -- count_from (integer)
                    i           -- count_to (integer)
                }
                r, err = api:call("sbroad.execute", {
                    [[
                        INSERT INTO "SPACE2"(
                            "id","yearquarter","a","b","name","field1","field2",
                            "field3","field4","field5","field6","field7","field8",
                            "field9","count_from","count_to"
                        )
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ]], values
                })
                t.assert_equals(err, nil)
                t.assert_equals(r, {row_count = 1})
            end
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

left_join.after_all(function()
    local storage1 = cluster:server("storage-1-1").net_box
    storage1:call("box.execute", { [[TRUNCATE TABLE "testing_space"]] })
    storage1:call("box.execute", { [[TRUNCATE TABLE "arithmetic_space"]] })
    storage1:call("box.execute", { [[TRUNCATE TABLE "arithmetic_space2"]] })
    storage1:call("box.execute", { [[TRUNCATE TABLE "null_t"]] })

    local storage2 = cluster:server("storage-2-1").net_box
    storage2:call("box.execute", { [[TRUNCATE TABLE "testing_space"]] })
    storage2:call("box.execute", { [[TRUNCATE TABLE "arithmetic_space"]] })
    storage2:call("box.execute", { [[TRUNCATE TABLE "arithmetic_space2"]] })
    storage2:call("box.execute", { [[TRUNCATE TABLE "null_t"]] })

    helper.stop_test_cluster()
end)

left_join.test_left_join_false_condition = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", {
        [[
        SELECT * from (select "a" as a from "arithmetic_space") as t1
        left join (select sum("f") as b from "arithmetic_space2") as t2
        on t1.a = t2.b
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "b", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 1, nil },
        { 1, nil },
        { 2, nil },
        { 2, nil },
    })
end

left_join.test_left_join_local_execution = function()
    local api = cluster:server("api-1").net_box

    local query = [[
        select * from (select "id" as "A" from "arithmetic_space") as "T1"
        left outer join (select "id" as "B" from "arithmetic_space2") as "T2"
        on "T1"."A" = "T2"."B"
    ]]
    local r, err = api:call("sbroad.execute", {
        query, {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "A", type = "integer" },
        { name = "B", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1 },
        { 2, 2 },
        { 3, 3 },
        { 4, 4 },
    })

    -- check there is really no motion for join in plan
    r, err = api:call("sbroad.execute", {
        [[explain ]] .. query, {}
    })
    t.assert_equals(err, nil)
    t.assert_items_equals(r, {
        "projection (\"T1\".\"A\"::integer -> \"A\", \"T2\".\"B\"::integer -> \"B\")",
        "    left join on ROW(\"T1\".\"A\"::integer) = ROW(\"T2\".\"B\"::integer)",
        "        scan \"T1\"",
        "            projection (\"arithmetic_space\".\"id\"::integer -> \"A\")",
        "                scan \"arithmetic_space\"",
        "        scan \"T2\"",
        "            projection (\"arithmetic_space2\".\"id\"::integer -> \"B\")",
        "                scan \"arithmetic_space2\"",
        "execution options:",
        "    vdbe_max_steps = 45000",
        "    vtable_max_rows = 5000",
        "buckets = [1-30000]"
    })
end

left_join.test_inner_segment_motion = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select * from (select "id" as "A" from "arithmetic_space") as "T1"
        left join (select "a" as "B" from "arithmetic_space2") as "T2"
        on "T1"."A" = "T2"."B"
        ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "A", type = "integer" },
        { name = "B", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1 },
        { 1, 1 },
        { 2, 2 },
        { 2, 2 },
        { 3, nil },
        { 4, nil },
    })

    -- check we have segment motion under inner join child
    r, err = api:call("sbroad.execute", { "explain " .. query_str, {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(r, {
        "projection (\"T1\".\"A\"::integer -> \"A\", \"T2\".\"B\"::integer -> \"B\")",
        "    left join on ROW(\"T1\".\"A\"::integer) = ROW(\"T2\".\"B\"::integer)",
        "        scan \"T1\"",
        "            projection (\"arithmetic_space\".\"id\"::integer -> \"A\")",
        "                scan \"arithmetic_space\"",
        "        motion [policy: segment([ref(\"B\")])]",
        "            scan \"T2\"",
        "                projection (\"arithmetic_space2\".\"a\"::integer -> \"B\")",
        "                    scan \"arithmetic_space2\"",
        "execution options:",
        "    vdbe_max_steps = 45000",
        "    vtable_max_rows = 5000",
        "buckets = unknown"
    })
end

left_join.test_inner_full_motion = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select * from (select "id" as "A" from "arithmetic_space") as "T1"
        left join (select "a" as "B" from "arithmetic_space2") as "T2"
        on "T1"."A" < "T2"."B"
        ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "A", type = "integer" },
        { name = "B", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 2 },
        { 1, 2 },
        { 2, nil },
        { 3, nil },
        { 4, nil },
    })

    -- check we have full motion under inner join child
    r, err = api:call("sbroad.execute", { "explain " .. query_str, {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(r, {
        "projection (\"T1\".\"A\"::integer -> \"A\", \"T2\".\"B\"::integer -> \"B\")",
        "    left join on ROW(\"T1\".\"A\"::integer) < ROW(\"T2\".\"B\"::integer)",
        "        scan \"T1\"",
        "            projection (\"arithmetic_space\".\"id\"::integer -> \"A\")",
        "                scan \"arithmetic_space\"",
        "        motion [policy: full]",
        "            scan \"T2\"",
        "                projection (\"arithmetic_space2\".\"a\"::integer -> \"B\")",
        "                    scan \"arithmetic_space2\"",
        "execution options:",
        "    vdbe_max_steps = 45000",
        "    vtable_max_rows = 5000",
        "buckets = [1-30000]"
    })
end

left_join.test_outer_segment_motion = function()
    -- check we get correct results when we have segment motion under outer child
    -- because sbroad in integration is built in release mode, I can't check
    -- explain here: sum has uuid-generated column name in explain which changes
    -- from launch to launch. The test that plan is correct can be found in sbroad
    -- explain tests.
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select * from (select sum("a") / 3 as a from "arithmetic_space") as t1
        left join (select "id" as b from "arithmetic_space2") as t2
        on t1.a = t2.b
        ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "decimal" },
        { name = "b", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 2, 2 },
    })
end

left_join.test_single_dist_outer = function()
    -- check we get correct results when we have single distribution for left child
    -- plan for such query can be found in sbroad explain tests
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select * from (select sum("a") / 3 as a from "arithmetic_space") as t1
        left join (select "id" as b from "arithmetic_space2") as t2
        on t1.a < t2.b
        ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "decimal" },
        { name = "b", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 2, 3 },
        { 2, 4 },
    })
end

left_join.test_single_dist_both = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select * from (select "id" as a from "arithmetic_space") as t1
        left join (select "id" as b from "arithmetic_space2") as t2
        on t1.a in (select "f" from "arithmetic_space2") or t1.a = 1 and t2.b = 4
        ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "b", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 4 },
        { 3, nil },
        { 4, nil },
        { 2, 1 },
        { 2, 2 },
        { 2, 3 },
        { 2, 4 },
    })
end

left_join.test_sq_with_full_motion = function()
    -- check we get correct results when we have full motion under subquery
    -- explain test can be found in sbroad explain tests
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select * from (select "a" as "A" from "arithmetic_space") as "T1"
        left join (select "id" as "B" from "arithmetic_space2") as "T2"
        on "T1"."A" in (select "a" + 1 from "arithmetic_space")
        ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "A", type = "integer" },
        { name = "B", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, nil },
        { 1, nil },
        { 2, 1 },
        { 2, 2 },
        { 2, 3 },
        { 2, 4 },
        { 2, 1 },
        { 2, 2 },
        { 2, 3 },
        { 2, 4 },
    })

    -- check subquery has Motion(Full)
    r, err = api:call("sbroad.execute", { "explain " .. query_str, {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(r, {
        "projection (\"T1\".\"A\"::integer -> \"A\", \"T2\".\"B\"::integer -> \"B\")",
        "    left join on ROW(\"T1\".\"A\"::integer) in ROW($0)",
        "        scan \"T1\"",
        "            projection (\"arithmetic_space\".\"a\"::integer -> \"A\")",
        "                scan \"arithmetic_space\"",
        "        motion [policy: full]",
        "            scan \"T2\"",
        "                projection (\"arithmetic_space2\".\"id\"::integer -> \"B\")",
        "                    scan \"arithmetic_space2\"",
        "subquery $0:",
        "motion [policy: full]",
        "            scan",
        "                projection (ROW(\"arithmetic_space\".\"a\"::integer) + ROW(1::unsigned) -> \"col_1\")",
        "                    scan \"arithmetic_space\"",
        "execution options:",
        "    vdbe_max_steps = 45000",
        "    vtable_max_rows = 5000",
        "buckets = [1-30000]"
    })
end

left_join.test_sq_with_segment_motion = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select * from (select "id" as "A" from "arithmetic_space") as t1
        left join (select "id" as "B" from "arithmetic_space2") as t2
        on t1."A" in (select "c" from "arithmetic_space")
        ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "A", type = "integer" },
        { name = "B", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1 },
        { 1, 2 },
        { 1, 3 },
        { 1, 4 },
        { 2, nil },
        { 3, nil },
        { 4, nil },
    })

    -- check subquery has Motion::Segment
    r, err = api:call("sbroad.execute", { "explain " .. query_str, {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(r, {
        "projection (\"t1\".\"A\"::integer -> \"A\", \"t2\".\"B\"::integer -> \"B\")",
        "    left join on ROW(\"t1\".\"A\"::integer) in ROW($0)",
        "        scan \"t1\"",
        "            projection (\"arithmetic_space\".\"id\"::integer -> \"A\")",
        "                scan \"arithmetic_space\"",
        "        motion [policy: full]",
        "            scan \"t2\"",
        "                projection (\"arithmetic_space2\".\"id\"::integer -> \"B\")",
        "                    scan \"arithmetic_space2\"",
        "subquery $0:",
        "motion [policy: segment([ref(\"c\")])]",
        "            scan",
        "                projection (\"arithmetic_space\".\"c\"::integer -> \"c\")",
        "                    scan \"arithmetic_space\"",
        "execution options:",
        "    vdbe_max_steps = 45000",
        "    vtable_max_rows = 5000",
        "buckets = unknown"
    })
end

-- uncomment when https://git.picodata.io/picodata/tarantool/-/issues/35
-- is fixed.
--[==[
left_join.test_left_true_condition = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select * from (select sum("id") as a from "arithmetic_space") as t1
        left join (select sum("id") as b from "arithmetic_space2") as t2
        on true
        ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "T1.A", type = "decimal" },
        { name = "T2.B", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 10, 10 },
    })
end

left_join.test_falsy_condition = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select * from (select sum("id") as a from "arithmetic_space") as t1
        left join (select sum("id") as b from "arithmetic_space2") as t2
        on false
        ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "T1.A", type = "decimal" },
        { name = "T2.B", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        { 10, nil },
    })
end
--]==]

left_join.test_table_with_nulls1 = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select * from (select "nb" as a from "null_t") as t1
        left join (select "nc" as b from "null_t") as t2
        on t1.a = t2.b
        ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "b", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { nil, nil },
        { nil, nil },
        { nil, nil },
        { nil, nil },
        { 1, 1 },
        { 1, 1 }
    })
end

left_join.test_table_with_nulls2 = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select * from (select "nb" as a from "null_t") as t1
        left join (select "nc" as b from "null_t") as t2
        on t1.a is not null
    ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "b", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { nil, nil },
        { nil, nil },
        { nil, nil },
        { nil, nil },
        { 1, 3 },
        { 1, 2 },
        { 1, 1 },
        { 1, 1 },
        { 1, nil },
    })
end

left_join.test_empty_left_table = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select * from (select "nb" as a from "null_t" where false) as t1
        left join (select "nc" as b from "null_t") as t2
        on true
    ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "b", type = "integer" },
    })
    t.assert_items_equals(r.rows, {})
end

left_join.test_empty_right_table = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select * from (select "nb" as a from "null_t") as t1
        left join (select "nc" as b from "null_t" where false) as t2
        on true
    ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "b", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {nil, nil},
        {nil, nil},
        {nil, nil},
        {nil, nil},
        {1, nil}
    })
end

left_join.test_groupby_after_join = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select a, count(b) from (select "nb" as a from "null_t") as t1
        left join (select "nc" as b from "null_t" where false) as t2
        on true
        group by a
    ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "col_1", type = "decimal" },
    })
    t.assert_items_equals(r.rows, {
        {nil, 0},
        {1, 0}
    })
end

left_join.test_groupby_under_outer_child = function()
    local api = cluster:server("api-1").net_box
    local query_str = [[
        select * from (select "nb" as a from "null_t" group by "nb") as t1
        left join (select "nc" as b from "null_t") as t2
        on t1.a = t2.b
    ]];

    local r, err = api:call("sbroad.execute", { query_str, {} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "a", type = "integer" },
        { name = "b", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        {nil, nil},
        {1, 1},
        {1, 1},
    })
end

left_join.test_left_join_customer_query = function()
    local api = cluster:server("api-1").net_box
    -- join condition: sp1.a_to = sp2.a and sp1.b_to = sp2.b and sp1.yearquarter = sp2.yearquarter
    -- sp1 have a_to, b_to, yearquarter = i for i in 1..6
    -- sp2 have a,    b,    yearquarter = i for i in 4..10
    -- So only for values 4..6 there will be match from second table,
    -- values 1..3 must be joined to null by `left join`
    local r, err = api:call("sbroad.execute", {
        [[
SELECT
  sp1."yearquarter",
  sp1."a_to" AS "a",
  sp1."b_to" AS "b",
  sp2."total",
  sp2."sp2_id",
  sp2."name"
FROM
  (select "yearquarter", "a_to", "b_to", "d_by_ab", "c_by_ab", "a_from", "b_from"
  from "SPACE1") AS sp1
  LEFT JOIN (
     SELECT
      sp2_1."id" AS "sp2_id",
      sp2_1."yearquarter" AS "sp2_yearquarter",
      sp2_1."a" AS "sp2_a",
      sp2_1."b" AS "sp2_b",
      sp2_1."field7",
      sp2_1."field6",
      0 AS "total",
      sp2_1."field5",
      sp2_1."name",
      sp2_1."field1",
      sp2_1."field2",
      sp2_1."field3",
      sp2_1."field4",
      sp2_1."field8",
      sp2_1."field9",
      sp2_1."count_from",
      sp2_1."count_to"
    FROM
      "SPACE2" AS sp2_1
  ) AS sp2 ON sp1."a_to" = sp2."sp2_a" AND sp1."b_to" = sp2."sp2_b" AND sp1."yearquarter" = sp2."sp2_yearquarter"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "yearquarter", type = "integer"},
        {name = "a", type = "string"},
        {name = "b", type = "string"},
        {name = "total", type = "unsigned"},
        {name = "sp2_id", type = "integer"},
        {name = "name", type = "string"},
    })
    t.assert_items_equals(r.rows, {
        {1, "a", "a", nil, nil, nil},
        {2, "a", "a", nil, nil, nil},
        {3, "a", "a", nil, nil, nil},
        {4, "a", "a", 0, 4, "a"},
        {5, "a", "a", 0, 5, "a"},
        {6, "a", "a", 0, 6, "a"},
    })
end

left_join.test_left_multi_join= function()
    local api = cluster:server("api-1").net_box
    local r, err = api:call("sbroad.execute", {
        [[
        SELECT "SPACE1"."yearquarter", "SPACE2"."name" FROM "SPACE1"
        LEFT JOIN "SPACE2"
        ON "SPACE1"."a_to" = "SPACE2"."a" AND "SPACE1"."b_to" = "SPACE2"."b"
        AND "SPACE1"."yearquarter" = "SPACE2"."yearquarter"
        LEFT JOIN "SPACE2" as space3
        ON "SPACE1"."a_to" = space3."a" AND "SPACE1"."b_to" = space3."b"
        WHERE "SPACE2"."yearquarter" = 4
        ]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        {name = "yearquarter", type = "integer"},
        {name = "name", type = "string"},
    })
    t.assert_items_equals(r.rows, {
        {4, "a"},
        {4, "a"},
        {4, "a"},
        {4, "a"},
        {4, "a"},
        {4, "a"},
        {4, "a"}
    })
end
