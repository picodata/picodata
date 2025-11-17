-- TEST: target-queries-1
-- SQL:
DROP TABLE IF EXISTS col1_transactions_actual;
DROP TABLE IF EXISTS col1_transactions_history;
DROP TABLE IF EXISTS col1_col2_transactions_actual;
DROP TABLE IF EXISTS col1_col2_transactions_history;
DROP TABLE IF EXISTS cola_accounts_actual;
DROP TABLE IF EXISTS cola_accounts_history;
DROP TABLE IF EXISTS cola_colb_accounts_actual;
DROP TABLE IF EXISTS cola_colb_accounts_history;
DROP TABLE IF EXISTS col1_col2_transactions_num_actual;
DROP TABLE IF EXISTS col1_col2_transactions_num_history;
CREATE TABLE col1_transactions_actual ("col1" int primary key, "amount" int, "account_id" int, "sys_from" int);
CREATE TABLE col1_transactions_history ("id" int primary key, "col1" int, "amount" int, "account_id" int, "sys_from" int, "sys_to" int);
CREATE TABLE col1_col2_transactions_actual ("col1" int, "col2" int, "amount" int, "account_id" int, "sys_from" int, primary key(col1, col2));
CREATE TABLE col1_col2_transactions_history ("id" int primary key, "col1" int, "col2" int, "amount" int, "account_id" int, "sys_from" int, "sys_to" int);
CREATE TABLE cola_accounts_actual ("id" int, "cola" int, "colb" int, "sys_from" int, primary key(cola, colb));
CREATE TABLE cola_accounts_history ("id" int primary key, "cola" int, "colb" int, "sys_from" int, "sys_to" int);
CREATE TABLE cola_colb_accounts_actual ("id" int, "cola" int primary key, "colb" int, "sys_from" int);
CREATE TABLE cola_colb_accounts_history ("id" int primary key, "cola" int, "colb" int, "sys_from" int, "sys_to" int);
CREATE TABLE col1_col2_transactions_num_actual ("col1" int, "col2" int, "amount" int, "account_id" int, "sys_from" int, primary key(col1, col2));
CREATE TABLE col1_col2_transactions_num_history ("id" int primary key, "col1" int, "col2" int, "amount" int, "account_id" int, "sys_from" int, "sys_to" int);
insert into "col1_transactions_actual"
            ("col1", "amount", "account_id", "sys_from")
            values (1, 3, 1, 0), (3, 3, 1, 0);
insert into "col1_transactions_history"
            ("id", "col1", "amount", "account_id", "sys_from", "sys_to")
            values (1, 1, 2, 1, 0, 2), (2, 1, 1, 1, 0, 1);
insert into "col1_col2_transactions_actual"
            ("col1", "col2", "amount", "account_id", "sys_from")
            values (1, 2, 3, 1, 0), (1, 1, 3, 1, 0);
insert into "col1_col2_transactions_history"
            ("id", "col1", "col2", "amount", "account_id", "sys_from", "sys_to")
            values (1, 1, 2, 2, 1, 0, 2), (2, 1, 2, 1, 1, 0, 1);
insert into "cola_accounts_actual"
            ("id", "cola", "colb", "sys_from")
            values (1, 1, 3, 0), (1, 2, 3, 0);
insert into "cola_accounts_history"
            ("id", "cola", "colb", "sys_from", "sys_to")
            values (1, 1, 2, 0, 2);
insert into "cola_colb_accounts_actual"
            ("id", "cola", "colb", "sys_from")
            values (1, 1, 3, 0);
insert into "cola_colb_accounts_history"
            ("id", "cola", "colb", "sys_from", "sys_to")
            values (1, 1, 2, 0, 2);
insert into "col1_col2_transactions_num_actual"
            ("col1", "col2", "amount", "account_id", "sys_from")
            values (1, 2, 3, 1, 0);
insert into "col1_col2_transactions_num_history"
            ("id", "col1", "col2", "amount", "account_id", "sys_from", "sys_to")
            values (1, 1, 2, 2, 1, 0, 2);

-- TEST: test_type_1
-- SQL:
SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" = 1;
-- EXPECTED:
1, 1, 2, 1, 1, 1, 1, 1, 3

-- TEST: test_type_2
-- SQL:
SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" = 1
        AND "col2" = 2;
-- EXPECTED:
1, 2, 1, 2, 1, 2, 1, 1, 1, 2, 1, 3

-- TEST: test_type_3
-- SQL:
SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" = 1
        AND ("col2" = 2
        AND "amount" > 2);
-- EXPECTED:
1, 2, 1, 3

-- TEST: test_type_4
-- SQL:
SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" = 1 OR "col1" = 3;
-- EXPECTED:
1, 1, 2, 1, 1, 1, 1, 1, 3, 3, 1, 3

-- TEST: test_type_5
-- SQL:
SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" = 1
        AND "col2" = 2
        OR "col1" = 1
        AND "col2" = 1;
-- EXPECTED:
1, 2, 1, 2, 1, 2, 1, 1, 1, 2, 1, 3, 1, 1, 1, 3

-- TEST: test_type_6
-- SQL:
SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" = 1
        OR ("col1" = 2
        OR "col1" = 3);
-- EXPECTED:
1, 1, 2, 1, 1, 1, 1, 1, 3, 3, 1, 3

-- TEST: test_type_7
-- SQL:
SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE ("col1" = 1
        OR ("col1" = 2
        OR "col1" = 3))
        AND ("col2" = 1
        OR "col2" = 2);
-- EXPECTED:
1, 2, 1, 2, 1, 2, 1, 1, 1, 1, 1, 3, 1, 2, 1, 3

-- TEST: test_type_8
-- SQL:
SELECT *
FROM
    (SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "col2", "account_id", "amount"
    FROM "col1_col2_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE ("col1" = 1
        OR ("col1" = 2
        OR "col1" = 3))
        AND (("col2" = 1
        OR "col2" = 2)
        AND "amount" > 2);
-- EXPECTED:
1, 1, 1, 3, 1, 2, 1, 3

-- TEST: test_type_9
-- SQL:
SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0
            AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" IN
    (SELECT "id"
    FROM
        (SELECT "id", "cola", "colb"
        FROM "cola_accounts_history"
        WHERE "sys_from" <= 0 AND "sys_to" >= 0
        UNION ALL
        SELECT "id", "cola", "colb"
        FROM "cola_accounts_actual"
        WHERE "sys_from" <= 0) AS "t8"
    WHERE "cola" = 1);
-- EXPECTED:
1, 1, 2, 1, 1, 1, 1, 1, 3

-- TEST: test_type_10
-- SQL:
SELECT *
FROM
    (SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_history"
    WHERE "sys_from" <= 0 AND "sys_to" >= 0
    UNION ALL
    SELECT "col1", "account_id", "amount"
    FROM "col1_transactions_actual"
    WHERE "sys_from" <= 0) AS "t3"
WHERE "col1" IN
    (SELECT "id"
    FROM
        (SELECT "id", "cola", "colb"
        FROM "cola_accounts_history"
        WHERE "sys_from" <= 0 AND "sys_to" >= 0
        UNION ALL
        SELECT "id", "cola", "colb"
        FROM "cola_accounts_actual"
        WHERE "sys_from" <= 0) AS "t8"
        WHERE "cola" = 1)
  AND "amount" > 0;
-- EXPECTED:
1, 1, 2, 1, 1, 1, 1, 1, 3

-- TEST: test_type_11
-- SQL:
SELECT *
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
    AND "amount" > 0;
-- EXPECTED:
1, 1, 1, 3

-- TEST: test_type_12
-- SQL:
SELECT *
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
WHERE "t3"."col1" = 1 AND "t8"."cola" = 1;
-- EXPECTED:
1, 1, 1, 1, 1, 2,
1, 1, 1, 1, 1, 3,
1, 1, 2, 1, 1, 2,
1, 1, 2, 1, 1, 3,
1, 1, 3, 1, 1, 2,
1, 1, 3, 1, 1, 3

-- TEST: test_type_13
-- SQL:
SELECT *
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
WHERE "t3"."col1" = 1 AND ("t8"."cola" = 1
        AND "t3"."amount" > 2);
-- EXPECTED:
1, 1, 3, 1, 1, 2, 1, 1, 3, 1, 1, 3

-- TEST: test_type_14
-- SQL:
SELECT *
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
AND ("t8"."cola" = 1 AND "t8"."colb" = 2);
-- EXPECTED:
1, 2, 1, 1, 1, 1, 2, 1, 2, 1, 2, 1, 1, 2, 1, 2, 1, 3, 1, 1, 2

-- TEST: test_type_15
-- SQL:
SELECT *
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
AND ("t8"."cola" = 1 AND ("t8"."colb" = 2 AND "t3"."amount" > 0));
-- EXPECTED:
1, 2, 1, 1, 1, 1, 2, 1, 2, 1, 2, 1, 1, 2, 1, 2, 1, 3, 1, 1, 2

-- TEST: test_type_17
-- SQL:
SELECT *
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
WHERE "t3"."col1" = 1 AND "t8"."cola" = 2;
-- EXPECTED:
1, 1, 1, 1, 2, 3, 1, 1, 2, 1, 2, 3, 1, 1, 3, 1, 2, 3

-- TEST: test_type_18
-- SQL:
SELECT *
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
WHERE "t3"."col1" = 1 AND "t3"."col2" = 2 AND ("t8"."cola" = 1 AND "t8"."colb" = 2);
-- EXPECTED:
1, 2, 1, 2, 1, 1, 2, 1, 2, 1, 3, 1, 1, 2

-- TEST: test_type_19
-- SQL:
SELECT *
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
WHERE "t3"."col1" = 1 AND ("t3"."col2" = 1 AND "t8"."colb" = 2);
-- EXPECTED:
1, 1, 1, 3, 1, 1, 2

-- TEST: test_type_20
-- SQL:
SELECT *
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
WHERE "t3"."col1" = 1 AND "t3"."col2" = 1;
-- EXPECTED:
1, 1, 1, 3, 1, 1, 2, 1, 1, 1, 3, 1, 1, 3

-- TEST: test_type_21
-- SQL:
SELECT *
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
    ON "t3"."col1" = "t8"."cola"
WHERE "t3"."col1" = 1;
-- EXPECTED:
1, 1, 1, 1, 1, 2,
1, 1, 1, 1, 1, 3,
1, 1, 2, 1, 1, 2,
1, 1, 2, 1, 1, 3,
1, 1, 3, 1, 1, 2,
1, 1, 3, 1, 1, 3

-- TEST: test_type_22
-- SQL:
SELECT *
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
        FROM "cola_accounts_history"
        WHERE "sys_from" <= 0 AND "sys_to" >= 0
        UNION ALL
        SELECT "id", "cola", "colb"
        FROM "cola_accounts_actual"
        WHERE "sys_from" <= 0) AS "t8"
        WHERE "cola" = 1)
    AND ("col1" = 1 AND "col2" = 2);
-- EXPECTED:
1, 2, 1, 2, 1, 2, 1, 1, 1, 2, 1, 3

-- TEST: test_type_23
-- SQL:
SELECT *
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
    AND ("col1" = 1 AND "col2" = 2);
-- EXPECTED:
1, 2, 1, 2, 1, 2, 1, 1, 1, 2, 1, 3
