-- TEST: delete1
-- SQL:
CREATE TABLE t (id INT PRIMARY KEY, a INT);
INSERT INTO "t" ("id", "a") VALUES (1, 2), (3, 4) ON CONFLICT DO NOTHING;

-- TEST: test_delete1-1
-- SQL:
DELETE FROM "t" WHERE "id" = 3;

-- TEST: test_delete1-2
-- SQL:
SELECT *, "bucket_id" FROM "t";
-- EXPECTED:
1, 2, 1934

-- TEST: test_delete2-1
-- SQL:
DELETE FROM "t";

-- TEST: test_delete2-2
-- SQL:
SELECT *, "bucket_id" FROM "t";
-- EXPECTED:

-- TEST: delete3-1
-- SQL:
CREATE TABLE g (id INT PRIMARY KEY, a INT) DISTRIBUTED GLOBALLY;
INSERT INTO "g" ("id", "a") VALUES (1, 2), (3, 4) ON CONFLICT DO NOTHING;

-- TEST: delete3-2
-- SQL:
DELETE FROM "g";

-- TEST: delete3-3
-- SQL:
SELECT * FROM "g";
-- EXPECTED:


-- TEST: delete4
-- SQL:
explain DELETE FROM "g"
-- EXPECTED:
delete "g"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]
