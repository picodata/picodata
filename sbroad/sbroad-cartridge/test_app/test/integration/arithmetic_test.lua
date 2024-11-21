local t = require('luatest')
local g = t.group('arithmetic.selection')
local g1 = t.group('arithmetic.join')
local g2 = t.group('arithmetic.projection')
local decimal = require("decimal")
local helper = require('test.helper.cluster_no_replication')

local cluster = nil

g.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

g.before_each(
    function()
        local api = cluster:server("api-1").net_box

        for i = 1, 10 do
            local r, err = api:call("sbroad.execute", {
                [[
                    insert into "arithmetic_space"
                    ("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
                    values (?,?,?,?,?,?,?,?,?,?)
                ]],
                {i, i, i*2, i*3, i, i, i, true, "123", decimal.new('4.6')},
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 1})

            r, err = api:call("sbroad.execute", {
                [[
                    insert into "arithmetic_space2"
                    ("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
                    values (?,?,?,?,?,?,?,?,?,?)
                ]],
                {i, i, i, i, i, i, i, false, "123", decimal.new('4.599999')},
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 1})
        end
    end
)

g.after_each(
    function()
        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "arithmetic_space"]] })
        storage1:call("box.execute", { [[truncate table "arithmetic_space2"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "arithmetic_space"]] })
        storage2:call("box.execute", { [[truncate table "arithmetic_space2"]] })
    end
)

g.after_all(function()
    helper.stop_test_cluster()
end)

g1.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

g1.before_each(
    function()
        local api = cluster:server("api-1").net_box

        for i = 1,10 do
            local r, err = api:call("sbroad.execute", {
                [[
                    insert into "arithmetic_space"
                    ("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
                    values (?,?,?,?,?,?,?,?,?,?)
                ]],
                {i, i, i*2, i*3, i, i, i, true, "123", decimal.new('4.6')},
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 1})

            r, err = api:call("sbroad.execute", {
                [[
                    insert into "arithmetic_space2"
                    ("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
                    values (?,?,?,?,?,?,?,?,?,?)
                ]],
                {i, i, i, i, i, i, i, false, "123", decimal.new('4.599999')},
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 1})
        end
    end
)

g1.after_each(
    function()
        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "arithmetic_space"]] })
        storage1:call("box.execute", { [[truncate table "arithmetic_space2"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "arithmetic_space"]] })
        storage2:call("box.execute", { [[truncate table "arithmetic_space2"]] })
    end
)

g1.after_all(function()
    helper.stop_test_cluster()
end)

g2.before_all(function()
    helper.start_test_cluster(helper.cluster_config)
    cluster = helper.cluster
end)

g2.before_each(
    function()
        local api = cluster:server("api-1").net_box

        for i = 1, 10 do
            local r, err = api:call("sbroad.execute", {
                [[
                    insert into "arithmetic_space"
                    ("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
                    values (?,?,?,?,?,?,?,?,?,?)
                ]],
                {i, i, i*2, i*3, i, i, i, true, "123", decimal.new('4.6')},
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 1})

            r, err = api:call("sbroad.execute", {
                [[
                    insert into "arithmetic_space2"
                    ("id", "a", "b", "c", "d", "e", "f", "boolean_col", "string_col", "number_col")
                    values (?,?,?,?,?,?,?,?,?,?)
                ]],
                {i, i, i, i, i, i, i, false, "123", decimal.new('4.599999')},
            })
            t.assert_equals(err, nil)
            t.assert_equals(r, {row_count = 1})
        end
    end
)

g2.after_each(
    function()
        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "arithmetic_space"]] })
        storage1:call("box.execute", { [[truncate table "arithmetic_space2"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "arithmetic_space"]] })
        storage2:call("box.execute", { [[truncate table "arithmetic_space2"]] })
    end
)

g2.after_all(function()
    helper.stop_test_cluster()
end)

g.test_arithmetic_invalid = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" % 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" ^ 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" ++ 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" ** 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" // 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" ** 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" +- 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" +* 2 > 0]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    -- arithemic operation on boolean col
    local _, err = api:call("sbroad.execute",
        { [[select "id" from "arithmetic_space" where "boolean_col" + "boolean_col" > 0]], {} }
    )
    t.assert_str_contains(
        tostring(err),
        "Type mismatch: can not convert boolean(TRUE) to integer, decimal, double, datetime or interval"
    )

    -- arithemic operation on string col
    local _, err = api:call("sbroad.execute",
        { [[select "id" from "arithmetic_space" where "string_col" + "string_col" > 0]], {} }
    )
    t.assert_str_contains(
        tostring(err),
        "Type mismatch: can not convert string('123') to integer, decimal, double, datetime or interval"
    )

    -- arithemic operation on number col
    local _, err = api:call("sbroad.execute",
    { [[select "id" from "arithmetic_space" where "number_col" + "number_col" > 0]], {} }
)
    t.assert_str_contains(
        tostring(err),
        "Type mismatch: can not convert number(4.6) to integer, decimal, double, datetime or interval"
    )
end

g2.test_arithmetic_invalid = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("sbroad.execute", {
        [[select "id" as "alias1" + "a" as "alias2" from "arithmetic_space"]], {}
    })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", {
        [[select ("id" + "a") as "alias1" + "b" as "alias2" from "arithmetic_space"]], {}
    })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" % 2 from "arithmetic_space"]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" ^ 2 from "arithmetic_space"]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" ++ 2 from "arithmetic_space"]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" ** 2 from "arithmetic_space"]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" // 2 from "arithmetic_space"]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" ** 2 from "arithmetic_space"]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" +- 2 from "arithmetic_space"]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    local _, err = api:call("sbroad.execute", { [[select "id" +* 2 from "arithmetic_space"]], {} })
    t.assert_str_contains(tostring(err), "rule parsing error")

    -- arithemic operation on boolean col
    local _, err = api:call("sbroad.execute",
        { [[select "boolean_col" + "boolean_col" from "arithmetic_space"]], {} }
    )
    t.assert_str_contains(
        tostring(err),
        "invalid expression: types boolean and boolean are not supported for arithmetic expression"
    )

    -- arithemic operation on string col
    local _, err = api:call("sbroad.execute",
        { [[select "string_col" + "string_col" from "arithmetic_space"]], {} }
    )
    t.assert_str_contains(
        tostring(err),
        "invalid expression: types string and string are not supported for arithmetic expression"
    )

    -- arithemic operation on number col
    local _, err = api:call("sbroad.execute",
        { [[select "number_col" + "number_col" from "arithmetic_space"]], {} }
    )
    t.assert_str_contains(
        tostring(err),
        "invalid expression: types number and number are not supported for arithmetic expression"
    )
end

g.test_arithmetic_valid = function()
    local api = cluster:server("api-1").net_box

    local res_all, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space"]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_all.rows, {})

    local r, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where 2 + 2 = 4]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    t.assert_items_equals(
        r.rows,
        res_all.rows
    )

    -- test several identical operators
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where
            "id" + "id" > 0 and "id" + "id" + "id" > 0
            or ("id" * "id" > 0 and "id" * "id" * "id" > 0)
            or ("id" - "id" < 0 and "id" - "id" - "id" < 0)
            or ("id" / "id" > 0 and "id" / "id" / "id" > 0)
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    t.assert_items_equals(
        r.rows,
        res_all.rows
    )

    -- test several operators with different priority
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where
            "id" + "id" * "id" + "id" >= 0
            and "id" - "id" * "id" - "id" <= 0
            and "id" + "id" / "id" + "id" >= 0
            and "id" - "id" / "id" - "id" <= 0
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    t.assert_items_equals(
        r.rows,
        res_all.rows
    )
end

g.test_arithmetic_with_bool = function()
    local api = cluster:server("api-1").net_box

    local res_all, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space"]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_all.rows, {})

    -- test arithmetic_expr [comparison operator] number
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "id" + "a" >= 0
            and "id" + "b" <= 12
            and "id" + "d" > 0
            and "id" + "e" < 8
            and "id" + "d" = 2
            and "id" + "a" != 3
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    t.assert_items_equals(
        r.rows,
        { {1} }
    )

    -- test arithmetic_expr [comparison operator] arithmetic_expr
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "id" + "a" >= "id" * 2
            and "id" + "c" <= "id" * 4
            and "id" + "b" > "id" * "a"
            and "id" + "a" < "id" + 3
            and "id" + "a" = 2
            and "id" + "a" != 4
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    t.assert_items_equals(
        r.rows,
        { {1} }
    )

    -- test arithmetic_expr [comparison operator] row
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "id" + "a" >= "id"
            and "id" + "b" <= "c"
            and "id" + "d" > "e"
            and "id" + "f" < "c"
            and "id" + "a" = "b"
            and "id" + "a" != "c"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    t.assert_items_equals(
        r.rows,
        res_all.rows
    )

    -- test number [comparison operator] arithmetic_expr
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where 12 >= "id" + "a"
            and 4 <= "id" + "d"
            and 12 > "id" + "e"
            and 4 < "id" + "f"
            and 20 = "id" + "c"
            and 9 != "id" + "b"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    t.assert_items_equals(
        r.rows,
        { {5} }
    )

    -- test row [comparison operator] arithmetic_expr
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "c" >= "id" + "b"
            and "b" <= "id" + "c"
            and "c" > "id" + "a"
            and "id" < "a" + "e"
            and "b" = "id" + "f"
            and "c" != "id" + "a"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    t.assert_items_equals(
        r.rows,
        res_all.rows
    )
end

g.test_selection_simple_arithmetic = function()
    local api = cluster:server("api-1").net_box

    local res_all, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space"]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_all.rows, {})

    -- check selection with arithmetic expr and `>` comparison operator
    local r, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space" where "id" + 1 > 8]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    t.assert_items_equals(
        r.rows,
        { {8}, {9}, {10} }
    )

    -- check selection with arithmetic expr and `between` comparison operator
    local r, err = api:call("sbroad.execute",
        { [[select "id" from "arithmetic_space" where "id" between "id" - 1 and "id" * 4]], {} }
    )
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    t.assert_items_equals(
        r.rows,
        res_all.rows
    )

    -- check selection with arithmetic expr and boolean operators
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where ("id" > "a" * 2 or "id" * 2 > 10) and "id" - 6 != 0
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
        r.metadata,
        { {name = "id", type = "integer"} }
    )
    t.assert_items_equals(
        r.rows,
        { {7}, {8}, {9}, {10} }
    )
end

g.test_associativity = function()
    local api = cluster:server("api-1").net_box

    local res_all, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space"]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_all.rows, {})

    -- addition and multiplication are associative
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where "a" + ("b" + "c") = ("a" + "b") + "c"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res.rows, res_all.rows)

    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where "a" * ("b" * "c") = ("a" * "b") * "c"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res.rows, res_all.rows)

    -- subtraction is left-associative
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where ("a" - "b") - "c" = "a" - "b" - "c"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res.rows, res_all.rows)

    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where "a" - ("b" - "c" ) = "a" - "b" - "c"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res.rows, res_all.rows)

    -- division is left-associative
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where
            (cast("a" as decimal) / cast("b" as decimal)) / cast("c" as decimal) =
            cast("a" as decimal) / cast("b" as decimal) / cast("c" as decimal)
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res.rows, res_all.rows)

    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where
            cast("a" as decimal) / (cast("b" as decimal) / cast("c" as decimal)) =
            (cast("a" as decimal) / cast("b" as decimal)) / cast("c" as decimal)
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res.rows, res_all.rows)
end

g.test_commutativity = function()
    local api = cluster:server("api-1").net_box

    local res_all, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space"]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_all.rows, {})

    -- addition and multiplication are commutative
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where "a" + "b" = "b" + "a"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res.rows, res_all.rows)

    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where "a" * "b" = "b" * "a"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res.rows, res_all.rows)

    -- subtraction and division are not commutative
    -- and x [-|/] y = y [-|/] x is true only when
    -- x, y have specific condition
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where "a" - "b" = "b" - "a"
        except
        select "id" from "arithmetic_space" where "a" = "b"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res.rows, {})

    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
            where cast("b" as decimal) / cast("a" as decimal) = cast("a" as decimal) / cast("b" as decimal)
        except
        select "id" from "arithmetic_space"
            where "a" = "b" or "a" = -1 * "b"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res.rows, {})
end

g.test_distributivity = function()
    local api = cluster:server("api-1").net_box

    local res_all, err = api:call("sbroad.execute", { [[select "id" from "arithmetic_space"]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_all.rows, {})

    -- multiplication if left- and right-distributive over addition|subtraction
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where
            "a"  * ("b" + "c") = "a" * "b" + "a" * "c"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res.rows, res_all.rows)

    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where
            ("a" + "b") * "c" = "a" * "c" + "b" * "c"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res.rows, res_all.rows)

    -- division is right-distributive over addition|subtraction
    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where
            (cast("a" as decimal) + cast("b" as decimal)) / cast("c" as decimal) =
            cast("a" as decimal) / cast("c" as decimal) + cast("b" as decimal) / cast("c" as decimal)
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res.rows, res_all.rows)

    local res, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space" where
        cast("a" as decimal) / (cast("b" as decimal) + cast("c" as decimal)) =
        cast("a" as decimal) / cast("b" as decimal) + cast("a" as decimal) / cast("c" as decimal)
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res.rows, res_all.rows)
end

g.test_arithmetic_in_parens = function()
    local api = cluster:server("api-1").net_box

    local without_parens, err = api:call("sbroad.execute", { [[
        select "c" from "arithmetic_space" where "a" + "b" > 1
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
            without_parens.metadata,
            { {name = "c", type = "integer"} }
    )

    local with_parens, err = api:call("sbroad.execute", { [[
        select "c" from "arithmetic_space" where ("a" + "b" > 1)
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
            with_parens.metadata,
            { {name = "c", type = "integer"} }
    )

    t.assert_items_equals(with_parens.rows, without_parens.rows)
end

g.test_arithmetic_in_subquery = function()
    local api = cluster:server("api-1").net_box

    local res_all, err_all = api:call("sbroad.execute", { [[select "id" from "arithmetic_space"]], {} })
    t.assert_equals(err_all, nil)
    t.assert_not_equals(res_all.rows, {})

    -- test arithmetic expressions in subquery projection and condition
    local r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where exists (select (1 + 2) * 3 / 4 from "arithmetic_space" where (1 * 2) / (8 / 4) = "id")
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(res_all.metadata, r.metadata)
    t.assert_items_equals(res_all.rows, r.rows)

    -- test subquery with asterisk and multiplication
    r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where exists (select * from "arithmetic_space" where 1 * 1 = 2)
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {},
    })

    -- test subquery with multiplication in projection
    r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "id" in (select 2 * 3 from "arithmetic_space")
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
            {6}
        },
    })

    -- test nested subquery with arithmetic
    r, err = api:call("sbroad.execute", { [[
        select "id" from "arithmetic_space"
        where "id" in (
            select 1 + 0 from "arithmetic_space" where exists (
                select 1 * (2 + 3) from (select * from (values (1)))
            )
        )
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "integer"},
        },
        rows = {
            {1}
        },
    })
end

g1.test_join_simple_arithmetic = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute",
    { [[
    SELECT "t3"."id", "t3"."a", "t8"."b"
    FROM
        (SELECT "id", "a"
            FROM "arithmetic_space"
            WHERE "c" < 0
        UNION ALL
            SELECT "id", "a"
            FROM "arithmetic_space"
            WHERE "c" > 0) AS "t3"
    INNER JOIN
        (SELECT "id" as "id1", "b"
            FROM "arithmetic_space2"
            WHERE "b" < 0
        UNION ALL
        SELECT "id" as "id1", "b"
            FROM "arithmetic_space2"
            WHERE "b" > 0) AS "t8"
    ON "t3"."id" + "t3"."a" * 2 = "t8"."id1" + "t8"."b"
    WHERE "t3"."id" = 2]], { } })

    t.assert_equals(err, nil)
    t.assert_equals(
        r.metadata,
        {
            {name = "id", type = "integer"},
            {name = "a", type = "integer"},
            {name = "b", type = "integer"},
        }
    )
    t.assert_equals(
        r.rows,
        { { 2, 2, 3 } }
    )

    -- check the same query with params
    local r2, err = api:call("sbroad.execute",
    { [[
    SELECT "t3"."id", "t3"."a", "t8"."b"
    FROM
        (SELECT "id", "a"
            FROM "arithmetic_space"
            WHERE "c" < ?
        UNION ALL
            SELECT "id", "a"
            FROM "arithmetic_space"
            WHERE "c" > ?) AS "t3"
    INNER JOIN
        (SELECT "id" as "id1", "b"
            FROM "arithmetic_space2"
            WHERE "b" < ?
        UNION ALL
        SELECT "id" as "id1", "b"
            FROM "arithmetic_space2"
            WHERE "b" > ?) AS "t8"
    ON "t3"."id" + "t3"."a" * ? = "t8"."id1" + "t8"."b"
    WHERE "t3"."id" = ?]], { 0, 0, 0, 0, 2, 2} })

    t.assert_equals(err, nil)
    t.assert_equals(
        r.metadata,
        {
            {name = "id", type = "integer"},
            {name = "a", type = "integer"},
            {name = "b", type = "integer"},
        }
    )
    t.assert_equals(
        r.rows,
        r2.rows
    )
end

g1.test_projection_selection_join = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute",
    { [[
    SELECT
        "t3"."id",
        "t3"."a",
        "t8"."b",
        "t3"."id" + "t3"."a" + "t8"."b" as "sum"
    FROM
        (SELECT "id", "a"
            FROM "arithmetic_space"
            WHERE "c" < 0
        UNION ALL
            SELECT "id", "a"
            FROM "arithmetic_space"
            WHERE "c" > 0) AS "t3"
    INNER JOIN
        (SELECT "id" as "id1", "b"
            FROM "arithmetic_space2"
            WHERE "b" < 0
        UNION ALL
        SELECT "id" as "id1", "b"
            FROM "arithmetic_space2"
            WHERE "b" > 0) AS "t8"
    ON "t3"."id" + "t3"."a" * 2 = "t8"."id1" + "t8"."b"
    ]], { } })

    t.assert_equals(err, nil)
    t.assert_equals(
        r.metadata,
        {
            {name = "id", type = "integer"},
            {name = "a", type = "integer"},
            {name = "b", type = "integer"},
            {name = "sum", type = "integer"},
        }
    )
    for i=1,table.getn(r.rows) do
        t.assert_equals(
            r.rows[i][1] + r.rows[i][2] + r.rows[i][3],
            r.rows[i][4]
        )
    end

    local r, err = api:call("sbroad.execute",
    { [[
    SELECT "t3"."id", "t3"."a", "t8"."b", "t3"."id" * "t3"."a" * "t8"."b" + 1 as "mul"
    FROM
        (SELECT "id", "a"
            FROM "arithmetic_space"
            WHERE "c" < 0
        UNION ALL
            SELECT "id", "a"
            FROM "arithmetic_space"
            WHERE "c" > 0) AS "t3"
    INNER JOIN
        (SELECT "id" as "id1", "b"
            FROM "arithmetic_space2"
            WHERE "b" < 0
        UNION ALL
        SELECT "id" as "id1", "b"
            FROM "arithmetic_space2"
            WHERE "b" > 0) AS "t8"
    ON "t3"."id" + "t3"."a" * 2 = "t8"."id1" + "t8"."b"
    ]], { } })

    t.assert_equals(err, nil)
    t.assert_equals(
        r.metadata,
        {
            {name = "id", type = "integer"},
            {name = "a", type = "integer"},
            {name = "b", type = "integer"},
            {name = "mul", type = "integer"},

        }
    )
    for i=1,table.getn(r.rows) do
        t.assert_equals(
            r.rows[i][1] * r.rows[i][2] * r.rows[i][3] + 1,
            r.rows[i][4]
        )
    end
end

g1.test_arithmetic_in_parens = function()
    local api = cluster:server("api-1").net_box

    local without_parens, err = api:call("sbroad.execute", { [[
        select t1.a1, t2.a2 from (select "a" as a1 from "arithmetic_space") as t1
        inner join (select "c" as a2 from "arithmetic_space2") as t2
        on t1.a1 = t2.a2 * 2
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
            without_parens.metadata,
            {
                {name = "a1", type = "integer"},
                {name = "a2", type = "integer"},
            }
    )

    local with_parens, err = api:call("sbroad.execute", { [[
        select t1.a1, t2.a2 from (select "a" as a1 from "arithmetic_space") as t1
        inner join (select "c" as a2 from "arithmetic_space2") as t2
        on (t1.a1 = t2.a2 * 2)
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
            with_parens.metadata,
            {
                {name = "a1", type = "integer"},
                {name = "a2", type = "integer"},
            }
    )

    t.assert_items_equals(with_parens.rows, without_parens.rows)
end

g2.test_alias = function()
    local api = cluster:server("api-1").net_box

    local res, err = api:call("sbroad.execute", {
        [[select "id", "id" + "a", "id" * "a" , "a" from "arithmetic_space"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(res.metadata, {
        {name = "id", type = "integer"},
        {name = "col_1", type = "integer"},
        {name = "col_2", type = "integer"},
        {name = "a", type = "integer"},
    })

    local res, err = api:call("sbroad.execute", {
        [[select "id", "id" + "a" as "sum", "id" * "a" as "mul", "a" from "arithmetic_space"]], {}
    })
    t.assert_equals(err, nil)
    t.assert_equals(res.metadata, {
        {name = "id", type = "integer"},
        {name = "sum", type = "integer"},
        {name = "mul", type = "integer"},
        {name = "a", type = "integer"},
    })

end

g2.test_associativity = function()
    local api = cluster:server("api-1").net_box

    -- addition is associative
    local res_add1, err = api:call("sbroad.execute", { [[
        select "id", "a" + ("b" + "c") from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_add1.rows, {})

    local res_add2, err = api:call("sbroad.execute", { [[
        select "id", ("a" + "b") + "c" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res_add2.rows, res_add1.rows)

    -- multiplication is associative
    local res_mul1, err = api:call("sbroad.execute", { [[
        select "id", "a" * ("b" * "c") from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_mul1.rows, {})

    local res_mul2, err = api:call("sbroad.execute", { [[
        select "id", ("a" * "b") * "c" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res_mul2.rows, res_mul1.rows)

    -- subtraction is left-associative
    local res_sub, err = api:call("sbroad.execute", { [[
        select "id", "a" - "b" - "c" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_sub.rows, {})

    local res_sub1, err = api:call("sbroad.execute", { [[
        select "id", ("a" - "b") - "c" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res_sub1.rows, res_sub.rows)

    local res_sub2, err = api:call("sbroad.execute", { [[
        select "id", "a" - ("b" - "c" ) from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_sub2.rows, res_sub.rows)

    -- division is left-associative
    local res_div, err = api:call("sbroad.execute", { [[
        select "id",
            cast("a" as decimal) / cast("b" as decimal) / cast("c" as decimal) from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_div.rows, {})

    local res_div1, err = api:call("sbroad.execute", { [[
        select "id", (cast("a" as decimal) / cast("b" as decimal)) / cast("c" as decimal) from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res_div1.rows, res_div.rows)

    local res_div2, err = api:call("sbroad.execute", { [[
        select "id", cast("a" as decimal) / (cast("b" as decimal) / cast("c" as decimal)) from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_div2.rows, res_div.rows)
end

g2.test_commutativity = function()
    local api = cluster:server("api-1").net_box

    -- addition is commutative
    local res_add1, err = api:call("sbroad.execute", { [[
        select "id", "a" + "b" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_add1.rows, {})

    local res_add2, err = api:call("sbroad.execute", { [[
        select "id", "b" + "a" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res_add2.rows, res_add1.rows)

    -- multiplication is commutative
    local res_mul1, err = api:call("sbroad.execute", { [[
        select "id", "a" * "b" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_mul1.rows, {})

    local res_mul2, err = api:call("sbroad.execute", { [[
        select "id", "b" * "a" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(res_mul2.rows, res_mul1.rows)

    -- subtraction is not commutative
    -- and x [-|/] y = y [-|/] x is true only when
    -- x, y have specific condition
    local res_sub1, err = api:call("sbroad.execute", { [[
        select "id", "a" - "b" from "arithmetic_space" where "a" != "b"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_sub1.rows, {})

    local res_sub2, err = api:call("sbroad.execute", { [[
        select "id", "b" - "a" from "arithmetic_space" where "a" != "b"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_sub2.rows, {})
    t.assert_not_equals(res_sub2.rows, res_sub1.rows)

    -- division is not commutative
    -- and x [-|/] y = y [-|/] x is true only when
    -- x, y have specific condition
    local res_div1, err = api:call("sbroad.execute", { [[
        select "id", cast("b" as decimal) / cast("a" as decimal) from "arithmetic_space"
            where "a" != "b" or "a" != -1 * "b"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_div1.rows, {})

    local res_div2, err = api:call("sbroad.execute", { [[
        select "id", cast("a" as decimal) / cast("b" as decimal) from "arithmetic_space"
            where "a" != "b" or "a" != -1 * "b"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_div2.rows, {})
    t.assert_not_equals(res_div2.rows, res_div1.rows)
end

g2.test_distributivity = function()
    local api = cluster:server("api-1").net_box

    -- multiplication if left- and right-distributive over addition|subtraction
    local res_mul, err = api:call("sbroad.execute", { [[
        select "id", "a" * "b" + "a" * "c" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_mul.rows, {})

    local res_mul1, err = api:call("sbroad.execute", { [[
        select "id", "a" * ("b" + "c") from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_mul.rows, {})
    t.assert_items_equals(res_mul.rows, res_mul1.rows)

    local res_mul2, err = api:call("sbroad.execute", { [[
        select "id", ("b" + "c") * "a" from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_mul2.rows, {})
    t.assert_items_equals(res_mul.rows, res_mul2.rows)

    -- division is right-distributive over addition|subtraction
    local res_div, err = api:call("sbroad.execute", { [[
        select
            "id",
            cast("a" as decimal) / cast("c" as decimal) + cast("b" as decimal) / cast("c" as decimal)
        from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_div.rows, {})

    local res_div1, err = api:call("sbroad.execute", { [[
        select
            "id",
            (cast("a" as decimal) + cast("b" as decimal)) / cast("c" as decimal)
        from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_div1.rows, {})
    t.assert_items_equals(res_div.rows, res_div1.rows)

    local res_div, err = api:call("sbroad.execute", { [[
        select
            "id",
            cast("a" as decimal) / cast("b" as decimal) + cast("a" as decimal) / cast("c" as decimal)
        from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_div.rows, {})

    local res_div2, err = api:call("sbroad.execute", { [[
        select
            "id",
            cast("a" as decimal) / (cast("b" as decimal) + cast("c" as decimal))
        from "arithmetic_space"
    ]], {} })
    t.assert_equals(err, nil)
    t.assert_not_equals(res_div.rows, res_div2.rows)
end

g2.test_arithmetic_in_parens = function()
    local api = cluster:server("api-1").net_box

    local without_parens, err = api:call("sbroad.execute", { [[select "a"+"b" from "arithmetic_space"]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
            without_parens.metadata,
            { {name = "col_1", type = "integer"} }
    )
    local with_parens, err = api:call("sbroad.execute", { [[select ("a"+"b") from "arithmetic_space"]], {} })
    t.assert_equals(err, nil)
    t.assert_items_equals(
            with_parens.metadata,
            { {name = "col_1", type = "integer"} }
    )
    t.assert_items_equals(with_parens.rows, without_parens.rows)
end
