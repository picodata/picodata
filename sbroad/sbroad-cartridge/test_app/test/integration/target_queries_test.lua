local t = require('luatest')
local target_queries = t.group('target_queries')

local helper = require('test.helper.cluster_no_replication')
local cluster = nil

target_queries.before_all(
    function()
        helper.start_test_cluster(helper.cluster_config)
        cluster = helper.cluster

        local api = cluster:server("api-1").net_box

        local r, err = api:call("sbroad.execute", {
            [[insert into "col1_transactions_actual"
            ("col1", "amount", "account_id", "sys_from")
            values (?, ?, ?, ?), (?, ?, ?, ?)]],
            {
                1, 3, 1, 0,
                3, 3, 1, 0
            }
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 2})

        r, err = api:call("sbroad.execute", {
            [[insert into "col1_transactions_history"
            ("id", "col1", "amount", "account_id", "sys_from", "sys_to")
            values (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)]],
            {
                1, 1, 2, 1, 0, 2,
                2, 1, 1, 1, 0, 1
            }
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 2})

        r, err = api:call("sbroad.execute", {
            [[insert into "col1_col2_transactions_actual"
            ("col1", "col2", "amount", "account_id", "sys_from")
            values (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)]],
            {
                1, 2, 3, 1, 0,
                1, 1, 3, 1, 0
            }
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 2})

        r, err = api:call("sbroad.execute", {
            [[insert into "col1_col2_transactions_history"
            ("id", "col1", "col2", "amount", "account_id", "sys_from", "sys_to")
            values (?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?)]],
            {
                1, 1, 2, 2, 1, 0, 2,
                2, 1, 2, 1, 1, 0, 1
            }
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 2})

        r = api:call("sbroad.execute", {
            [[insert into "cola_accounts_actual"
            ("id", "cola", "colb", "sys_from")
            values (?, ?, ?, ?), (?, ?, ?, ?)]],
            {
                1, 1, 3, 0,
                1, 2, 3, 0
            }
        })
        t.assert_equals(r, {row_count = 2})

        r, err = api:call("sbroad.execute", {
            [[insert into "cola_accounts_history"
            ("id", "cola", "colb", "sys_from", "sys_to")
            values (?, ?, ?, ?, ?)]],
            {
                1, 1, 2, 0, 2
            }
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 1})

        r, err = api:call("sbroad.execute", {
            [[insert into "cola_colb_accounts_actual"
            ("id", "cola", "colb", "sys_from")
            values (?, ?, ?, ?)]],
            {
                1, 1, 3, 0
            }
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 1})

        r, err = api:call("sbroad.execute", {
            [[insert into "cola_colb_accounts_history"
            ("id", "cola", "colb", "sys_from", "sys_to")
            values (?, ?, ?, ?, ?)]],
            {
                1, 1, 2, 0, 2
            }
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 1})

        r, err = api:call("sbroad.execute", {
            [[insert into "col1_col2_transactions_num_actual"
            ("col1", "col2", "amount", "account_id", "sys_from")
            values (?, ?, ?, ?, ?)]],
            {
                1, 2, 3, 1, 0
            }
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 1})

        r, err = api:call("sbroad.execute", {
            [[insert into "col1_col2_transactions_num_history"
            ("id", "col1", "col2", "amount", "account_id", "sys_from", "sys_to")
            values (?, ?, ?, ?, ?, ?, ?)]],
            {
                1, 1, 2, 2, 1, 0, 2
            }
        })
        t.assert_equals(err, nil)
        t.assert_equals(r, {row_count = 1})
    end
)

target_queries.after_all(
    function()
        local storage1 = cluster:server("storage-1-1").net_box
        storage1:call("box.execute", { [[truncate table "col1_transactions_actual"]] })
        storage1:call("box.execute", { [[truncate table "col1_transactions_history"]] })
        storage1:call("box.execute", { [[truncate table "col1_col2_transactions_actual"]] })
        storage1:call("box.execute", { [[truncate table "col1_col2_transactions_history"]] })

        local storage2 = cluster:server("storage-2-1").net_box
        storage2:call("box.execute", { [[truncate table "col1_transactions_actual"]] })
        storage2:call("box.execute", { [[truncate table "col1_transactions_history"]] })
        storage1:call("box.execute", { [[truncate table "col1_col2_transactions_actual"]] })
        storage1:call("box.execute", { [[truncate table "col1_col2_transactions_history"]] })

        helper.stop_test_cluster()
    end
)

target_queries.test_type_1 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= ?
            AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" = ?]], { 0, 0, 1} })
    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1, 2 },
        { 1, 1, 1 },
        { 1, 1, 3 }
    })
end

target_queries.test_type_2 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= ?
            AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
WHERE "col1" = ?
        AND "col2" = ?]], { 0, 0, 0, 1, 2} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 2, 1, 2 },
        { 1, 2, 1, 1 },
        { 1, 2, 1, 3 }
    })
end

target_queries.test_type_3 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= ?
            AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
WHERE "col1" = ?
        AND ("col2" = ?
        AND "amount" > ?)]], { 0, 0, 0, 1, 2, 2} })

    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            { name = "col1", type = "integer" },
            { name = "col2", type = "integer" },
            { name = "account_id", type = "integer" },
            { name = "amount", type = "integer" },
        },
        rows = {
            { 1, 2, 1, 3 }
        },
    })
end

target_queries.test_type_4 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= ?
            AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
WHERE "col1" = ? OR "col1" = ?]], { 0, 0, 0, 1, 3} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 3, 1, 3 },
        { 1, 1, 2 },
        { 1, 1, 1 },
        { 1, 1, 3 }
    })
end

target_queries.test_type_5 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= ?
            AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
WHERE "col1" = ?
        AND "col2" = ?
        OR "col1" = ?
        AND "col2" = ?]], { 0, 0, 0, 1, 2, 1, 1} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 2, 1, 2 },
        { 1, 2, 1, 1 },
        { 1, 2, 1, 3 },
        { 1, 1, 1, 3 }
    })
end

target_queries.test_type_6 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= ?
            AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
WHERE "col1" = ?
        OR ("col1" = ?
        OR "col1" = ?)]], { 0, 0, 0, 1, 2, 3 } })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 3, 1, 3 },
        { 1, 1, 2 },
        { 1, 1, 1 },
        { 1, 1, 3 }
    })
end

target_queries.test_type_7 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= ?
            AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
WHERE ("col1" = ?
        OR ("col1" = ?
        OR "col1" = ?))
        AND ("col2" = ?
        OR "col2" = ?)]], { 0, 0, 0, 1, 2, 3, 1, 2 } })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 3 },
        { 1, 2, 1, 2 },
        { 1, 2, 1, 1 },
        { 1, 2, 1, 3 }
    })
end

target_queries.test_type_8 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= ?
            AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
WHERE ("col1" = ?
        OR ("col1" = ?
        OR "col1" = ?))
        AND (("col2" = ?
        OR "col2" = ?)
        AND "amount" > ?)]], { 0, 0, 0, 1, 2, 3, 1, 2, 2 } })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 3 },
        { 1, 2, 1, 3 }
    })
end

target_queries.test_type_9 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= ?
            AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
WHERE "col1" IN
    (SELECT "id"
    FROM
        (SELECT "id", "cola", "colb"
        FROM "cola_accounts_history"
        WHERE "sys_from" <= ? AND "sys_to" >= ?
        UNION ALL
        SELECT "id", "cola", "colb"
        FROM "cola_accounts_actual"
        WHERE "sys_from" <= ?) AS "t8"
    WHERE "cola" = 1)]], { 0, 0, 0, 0, 0, 0} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1, 2 },
        { 1, 1, 1 },
        { 1, 1, 3 }
    })
end

target_queries.test_type_10 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= ? AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
WHERE "col1" IN
    (SELECT "id"
    FROM
        (SELECT "id", "cola", "colb"
        FROM "cola_accounts_history"
        WHERE "sys_from" <= ? AND "sys_to" >= ?
        UNION ALL
        SELECT "id", "cola", "colb"
        FROM "cola_accounts_actual"
        WHERE "sys_from" <= ?) AS "t8"
        WHERE "cola" = ?)
  AND "amount" > ?]], { 0, 0, 0, 0, 0, 0, 1, 0} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1, 2 },
        { 1, 1, 1 },
        { 1, 1, 3 }
    })
end

target_queries.test_type_11 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE ("col1", "col2") IN
    (SELECT "id", "cola"
    FROM
        (SELECT "id", "cola", "colb"
        FROM "cola_accounts_history"
        WHERE "sys_from" <= 0 AND "sys_to" >= 0
        UNION ALL
        SELECT "id", "cola", "colb"
        FROM "cola_accounts_actual"
        WHERE "sys_from" <= 0) AS "t8"
        WHERE "cola" = 1)
    AND "amount" > 0]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })
    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 3 }
    })
end

target_queries.test_type_12 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = 1 AND "t8"."cola" = 1]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 1, 1, 2 },
        { 1, 1, 1, 1, 1, 3 },
        { 1, 1, 2, 1, 1, 2 },
        { 1, 1, 2, 1, 1, 3 },
        { 1, 1, 3, 1, 1, 2 },
        { 1, 1, 3, 1, 1, 3 },
    })
end

target_queries.test_type_13 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= ?
            AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_accounts_history"
    WHERE "sys_from" <= ? AND "sys_to" >= ?
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_accounts_actual"
    WHERE "sys_from" <= ?) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = ? AND ("t8"."cola" = ?
        AND "t3"."amount" > ?)]], { 0, 0, 0, 0, 0, 0, 1, 1, 2 } })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 1, 3, 1, 1, 2 },
        { 1, 1, 3, 1, 1, 3 },
    })
end

target_queries.test_type_14 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= ? AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_colb_accounts_history"
    WHERE "sys_from" <= ? AND "sys_to" >= ?
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_colb_accounts_actual"
    WHERE "sys_from" <= ?) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = ? AND "t3"."col2" = ?
AND ("t8"."cola" = ? AND "t8"."colb" = ?)]], { 0, 0, 0, 0, 0, 0, 1, 2, 1, 2 } })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 2, 1, 1, 1, 1, 2 },
        { 1, 2, 1, 2, 1, 1, 2 },
        { 1, 2, 1, 3, 1, 1, 2 }
    })
end

target_queries.test_type_15 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_colb_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_colb_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = 1 AND "t3"."col2" = 2
AND ("t8"."cola" = 1 AND ("t8"."colb" = 2 AND "t3"."amount" > 0))]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 2, 1, 1, 1, 1, 2 },
        { 1, 2, 1, 2, 1, 1, 2 },
        { 1, 2, 1, 3, 1, 1, 2 }
    })
end

target_queries.test_type_17 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = ? AND "t8"."cola" = ?]], { 1, 2 } })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 1, 2, 3 },
        { 1, 1, 2, 1, 2, 3 },
        { 1, 1, 3, 1, 2, 3 }
    })
end

target_queries.test_type_18 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_num_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_num_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_colb_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_colb_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = ? AND "t3"."col2" = 2 AND ("t8"."cola" = 1 AND "t8"."colb" = ?)]], { 1, 2 } })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "number" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 2, 1, 2, 1, 1, 2 },
        { 1, 2, 1, 3, 1, 1, 2 }
    })
end

target_queries.test_type_19 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."account_id" = "t8"."id"
WHERE "t3"."col1" = 1 AND ("t3"."col2" = 1 AND "t8"."colb" = 2)]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 3, 1, 1, 2 }
    })
end

target_queries.test_type_20 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."col1" = "t8"."cola"
WHERE "t3"."col1" = 1 AND "t3"."col2" = 1]], {} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 3, 1, 1, 2 },
        { 1, 1, 1, 3, 1, 1, 3 }
    })
end

target_queries.test_type_21 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
INNER JOIN
    (SELECT "id", "cola", "colb"
    FROM "cola_accounts_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "id", "cola", "colb"
    FROM "cola_accounts_actual"
    WHERE "sys_from" <= 0) AS "t8"
    ON "t3"."col1" = "t8"."cola"
WHERE "t3"."col1" = 1]], { 0 } })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
        { name = "id", type = "integer" },
        { name = "cola", type = "integer" },
        { name = "colb", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 1, 1, 1, 1, 2 },
        { 1, 1, 1, 1, 1, 3 },
        { 1, 1, 2, 1, 1, 2 },
        { 1, 1, 2, 1, 1, 3 },
        { 1, 1, 3, 1, 1, 2 },
        { 1, 1, 3, 1, 1, 3 },
    })
end

target_queries.test_type_22 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= ? AND "sys_to" >= ?
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= ?) AS "t3"
WHERE "account_id" IN
    (SELECT "id"
    FROM
        (SELECT "id", "cola", "colb"
        FROM "cola_accounts_history"
        WHERE "sys_from" <= ? AND "sys_to" >= ?
        UNION ALL
        SELECT "id", "cola", "colb"
        FROM "cola_accounts_actual"
        WHERE "sys_from" <= ?) AS "t8"
        WHERE "cola" = ?)
    AND ("col1" = ? AND "col2" = ?)]], { 0, 0, 0, 0, 0, 0, 1, 1, 2} })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 2, 1, 2 },
        { 1, 2, 1, 1 },
        { 1, 2, 1, 3 }
    })
end

target_queries.test_type_23 = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("sbroad.execute", { [[SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "account_id" IN
    (SELECT "id"
    FROM
        (SELECT "id", "cola", "colb"
        FROM "cola_colb_accounts_history"
        WHERE "sys_from" <= 0 AND "sys_to" >= 0
        UNION ALL
        SELECT "id", "cola", "colb"
        FROM "cola_colb_accounts_actual"
        WHERE "sys_from" <= 0) AS "t8"
        WHERE "cola" = 1 AND "colb" = 2)
    AND ("col1" = ? AND "col2" = 2)]], { 1 } })

    t.assert_equals(err, nil)
    t.assert_equals(r.metadata, {
        { name = "col1", type = "integer" },
        { name = "col2", type = "integer" },
        { name = "account_id", type = "integer" },
        { name = "amount", type = "integer" },
    })

    t.assert_items_equals(r.rows, {
        { 1, 2, 1, 2 },
        { 1, 2, 1, 1 },
        { 1, 2, 1, 3 }
    })
end
