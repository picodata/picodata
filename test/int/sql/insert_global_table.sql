-- TEST: insert-global-table
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t ("a" INT PRIMARY KEY, "b" INT) DISTRIBUTED GLOBALLY;
INSERT INTO "t" VALUES (1, 1), (2, 2);

-- TEST: insert-global-table-1
-- SQL:
INSERT INTO "t" VALUES (1, 2) ON CONFLICT DO REPLACE;

-- TEST: insert-global-table-2
-- SQL:
SELECT * FROM "t";
-- EXPECTED:
1, 2, 2, 2

-- TEST: insert-global-table-3
-- SQL:
INSERT INTO "t" VALUES (1, 3);
-- ERROR:
Duplicate key exists in unique index

-- TEST: insert-global-table-4
-- SQL:
INSERT INTO "t" VALUES (2, 1) ON CONFLICT DO FAIL;
-- ERROR:
Duplicate key exists in unique index

-- TEST: insert-global-table-5
-- SQL:
INSERT INTO "t" VALUES (2, 1) ON CONFLICT DO REPLACE;

-- TEST: insert-global-table-6
-- SQL:
SELECT * FROM "t" ORDER BY a;
-- EXPECTED:
1, 2, 2, 1

-- TEST: insert-global-table-7
-- SQL:
INSERT INTO "t" VALUES (2, 2), (3, 3) ON CONFLICT DO NOTHING;

-- TEST: insert-global-table-8
-- SQL:
SELECT * FROM "t" ORDER BY a;
-- EXPECTED:
1, 2, 2, 1, 3, 3

-- TEST: insert-global-table-unique
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t ("a" INT PRIMARY KEY, "b" INT NOT NULL) DISTRIBUTED GLOBALLY;
CREATE UNIQUE INDEX b_idx ON t ("b");
INSERT INTO "t" VALUES (1, 1), (2, 2);

-- TEST: insert-global-table-unique-1
-- SQL:
INSERT INTO "t" VALUES (3, 2) ON CONFLICT DO NOTHING;

-- TEST: insert-global-table-unique-2
-- SQL:
INSERT INTO "t" VALUES (3, NULL) ON CONFLICT DO NOTHING;
-- ERROR:
expected integer, got nil

-- TEST: insert-global-table-unique-3
-- SQL:
INSERT INTO "t" VALUES (3, 2) ON CONFLICT DO FAIL;
-- ERROR:
Duplicate key exists in unique index

-- TEST: insert-global-table-unique-4
-- SQL:
INSERT INTO "t" VALUES (3, 2) ON CONFLICT DO REPLACE;
-- ERROR:
Duplicate key exists in unique index

-- TEST: insert-global-table-unique-5
-- SQL:
SELECT * FROM "t";
-- EXPECTED:
1, 1, 2, 2
