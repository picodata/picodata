-- TEST: invalid-bucket-calculation-gl2640
-- SQL:
CREATE TABLE t (a INT, b DECIMAL, PRIMARY KEY (b));


-- TEST: invalid-bucket-calculation-gl2640.2
-- SQL:
EXPLAIN SELECT * FROM t WHERE b = 1;
-- EXPECTED:
projection ("t"."a"::int -> "a", "t"."b"::decimal -> "b")
    selection "t"."b"::decimal = 1::int
        scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1934]

-- TEST: invalid-bucket-calculation-gl2640.3
-- SQL:
EXPLAIN SELECT * FROM t WHERE b = 1::decimal;
-- EXPECTED:
projection ("t"."a"::int -> "a", "t"."b"::decimal -> "b")
    selection "t"."b"::decimal = 1::decimal
        scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [2135]

-- TEST: invalid-bucket-calculation-gl2640.4
-- SQL:
EXPLAIN SELECT * FROM t WHERE b = 1.0;
-- EXPECTED:
projection ("t"."a"::int -> "a", "t"."b"::decimal -> "b")
    selection "t"."b"::decimal = 1.0::decimal
        scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [712]

-- TEST: invalid-bucket-calculation-gl2640.5
-- SQL:
EXPLAIN INSERT INTO t VALUES (1, 1);
-- EXPECTED:
insert "t" on conflict: fail
    motion [policy: segment([ref("COLUMN_2")]), program: ReshardIfNeeded]
        values
            value row (data=ROW(1::int, 1::int))
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1934]

-- TEST: invalid-bucket-calculation-gl2640.6
-- SQL:
EXPLAIN INSERT INTO t VALUES (1, 1.0);
-- EXPECTED:
insert "t" on conflict: fail
    motion [policy: segment([ref("COLUMN_2")]), program: ReshardIfNeeded]
        values
            value row (data=ROW(1::int, 1.0::decimal))
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [712]
