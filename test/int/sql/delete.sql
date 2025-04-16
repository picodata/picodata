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
