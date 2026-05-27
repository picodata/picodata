-- TEST: test_option
-- SQL:
DROP TABLE IF EXISTS testing_space;
DROP TABLE IF EXISTS cola_accounts_history;
CREATE TABLE cola_accounts_history ("id" int primary key, "cola" int, "colb" int, "sys_from" int, "sys_to" int);
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);
INSERT INTO "cola_accounts_history" ("id", "cola", "colb", "sys_from", "sys_to") VALUES (1, 1, 1, 1, 1);
CREATE TABLE t (a INT PRIMARY KEY);
INSERT INTO t VALUES (1);

-- TEST: test_basic-1
-- SQL:
select * from "testing_space" option(sql_vdbe_opcode_max = 5);
-- ERROR:
Query 1 from EXPLAIN \(RAW\): Reached a limit on max executed vdbe opcodes. Limit: 5

-- TEST: test_basic-2
-- SQL:
select * from "testing_space" order by 1;
-- EXPECTED:
1, '123', 1, 2, '1', 1, 3, '1', 1, 4, '2', 2, 5, '123', 2, 6, '2', 4

-- TEST: test_basic-3
-- SQL:
select * from "testing_space" option(sql_vdbe_opcode_max = 45);
-- EXPECTED:
1, '123', 1, 2, '1', 1, 3, '1', 1, 4, '2', 2, 5, '123', 2, 6, '2', 4

-- TEST: test_dml-1
-- SQL:
insert into "testing_space" select "id" + 10, "name", "product_units" from "testing_space" option(sql_vdbe_opcode_max = 10);
-- ERROR:
Query 1 from EXPLAIN \(RAW\): Reached a limit on max executed vdbe opcodes. Limit: 10

-- TEST: test_dml-2
-- SQL:
insert into "testing_space" select "id" + 10, "name", "product_units" from "testing_space" option(sql_vdbe_opcode_max = 0);

-- TEST: test_dml-3
-- SQL:
DELETE FROM "testing_space";
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);

-- TEST: test_invalid-1
-- SQL:
select * from "testing_space" option(sql_vdbe_opcode_max = 10, sql_vdbe_opcode_max = 11);
-- ERROR:
Query 1 from EXPLAIN \(RAW\): Reached a limit on max executed vdbe opcodes. Limit: 11

-- TEST: test_invalid-2
-- SQL:
select * from "testing_space" option(sql_vdbe_opcode_max = -1);
-- ERROR:
expected Unsigned or Parameter

-- TEST: test_invalid-3
-- SQL:
select * from "testing_space" option(sql_motion_row_max = -1);
-- ERROR:
expected Unsigned or Parameter

-- TEST: test_invalid-4
-- SQL:
select * from "testing_space" option(bad_option = 1);
-- ERROR:
expected VdbeOpcodeMax, MotionRowMax, ReadPreference, or Forward

-- TEST: test_sql_motion_row_max_on_storage-1
-- SQL:
insert into "testing_space" select "id" + 10, "name", "product_units" from "testing_space" option(sql_motion_row_max = 1);
-- ERROR:
Query 1 from EXPLAIN \(RAW\): Exceeded maximum number of rows \(1\) in virtual table: 6

-- TEST: test_sql_motion_row_max_on_storage-2
-- SQL:
insert into "testing_space" select "id" + 10, "name", "product_units" from "testing_space" option(sql_motion_row_max = 6);

-- TEST: test_sql_motion_row_max_on_storage-3
-- SQL:
DELETE FROM "testing_space";
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
            (1, '123', 1),
            (2, '1', 1),
            (3, '1', 1),
            (4, '2', 2),
            (5, '123', 2),
            (6, '2', 4);

-- TEST: test_sql_motion_row_max_insert_values
-- SQL:
insert into "cola_accounts_history" values (2, 2, 2, 1, 1), (3, 2, 2, 1, 1), (4, 2, 2, 1, 1) option(sql_motion_row_max = 1);
-- ERROR:
Query 1 from EXPLAIN \(RAW\): Exceeded maximum number of rows \(1\) in virtual table: 3

-- TEST: test_sql_motion_row_max_on_router-1
-- SQL:
select "id" from "testing_space" group by "id" option(sql_motion_row_max = 5);
-- ERROR:
Query 1 from EXPLAIN \(RAW\): Exceeded maximum number of rows \(5\) in virtual table: 6

-- TEST: test_sql_motion_row_max_on_router-2
-- SQL:
select "id" from "testing_space" group by "id" option(sql_motion_row_max = 7);
-- EXPECTED:
1, 2, 3, 4, 5, 6

-- TEST: test_sql_motion_row_max_on_router-3
-- SQL:
select "id" from "testing_space" group by "id" option(sql_motion_row_max = 6);
-- EXPECTED:
1, 2, 3, 4, 5, 6

-- TEST: test-sql-vdbe-opcode-max-exceeded-select-first-query
-- SQL:
select * from t order by 1 limit 1000 option (sql_vdbe_opcode_max = 6);
-- ERROR:
Query 1 from EXPLAIN \(RAW\): Reached a limit on max executed vdbe opcodes. Limit: 6

-- TEST: test-sql-vdbe-opcode-max-exceeded-select-second-query
-- SQL:
select * from t order by 1 limit 1000 option (sql_vdbe_opcode_max = 18);
-- ERROR:
Query 2 from EXPLAIN \(RAW\): Reached a limit on max executed vdbe opcodes. Limit: 18

-- TEST: test-sql-vdbe-opcode-max-exceeded-insert-query
-- SQL:
insert into t values (2 + 2) option (sql_vdbe_opcode_max = 1);
-- ERROR:
Query 1 from EXPLAIN \(RAW\): Reached a limit on max executed vdbe opcodes. Limit: 1

-- TEST: test-sql-vdbe-opcode-max-exceeded-insert-select-query
-- SQL:
insert into t select * from (values ((values (1 + 1 - 1 + 1))), (2), (3)) option (sql_vdbe_opcode_max = 1);
-- ERROR:
Query 1 from EXPLAIN \(RAW\): Reached a limit on max executed vdbe opcodes. Limit: 1

-- TEST: test-sql-motion-row-max-exceeded-insert-select-query
-- SQL:
insert into t select * from (values ((values (1 + 1 - 1 + 1))), (2), (3)) option (sql_motion_row_max = 2);
-- ERROR:
Query 2 from EXPLAIN \(RAW\): Exceeded maximum number of rows \(2\) in virtual table: 3

-- TEST: test-sql-motion-row-max-exceeded-insert-values
-- SQL:
insert into t values (1), (2), (3) option (sql_motion_row_max = 2);
-- ERROR:
Query 1 from EXPLAIN \(RAW\): Exceeded maximum number of rows \(2\) in virtual table: 3

-- TEST: test-sql-motion-row-max-exceeded-select-values
-- SQL:
select * from (values (1), (2)) option (sql_motion_row_max = 1);
-- ERROR:
Query 1 from EXPLAIN \(RAW\): Exceeded maximum number of rows \(1\) in virtual table: 2

-- TEST: test-sql-motion-row-max-exceeded-select-dml
-- SQL:
insert into t values (2);

-- TEST: test-sql-motion-row-max-exceeded-select
-- SQL:
select * from t option (sql_motion_row_max = 1);
-- ERROR:
Query 1 from EXPLAIN \(RAW\): Exceeded maximum number of rows \(1\) in virtual table: 2
