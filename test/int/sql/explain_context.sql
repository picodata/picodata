-- TEST: setup
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (a INT PRIMARY KEY, b TEXT);

-- TEST: explain-context-default-options
-- SQL:
EXPLAIN (CONTEXT) SELECT * FROM t;
-- EXPECTED:
sql_vdbe_opcode_max = 45000
sql_motion_row_max = 5000

-- TEST: explain-context-custom-options
-- SQL:
EXPLAIN (CONTEXT) SELECT * FROM t OPTION (sql_vdbe_opcode_max = 3, sql_motion_row_max = 4);
-- EXPECTED:
sql_vdbe_opcode_max = 3
sql_motion_row_max = 4

-- TEST: explain-context-custom-vdbe-opcode
-- SQL:
EXPLAIN (CONTEXT) SELECT * FROM t OPTION (sql_vdbe_opcode_max = 5);
-- EXPECTED:
sql_vdbe_opcode_max = 5
sql_motion_row_max = 5000
