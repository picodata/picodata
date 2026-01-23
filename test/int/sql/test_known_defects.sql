-- TEST: invalid-bucket-calculation-gl2640
-- SQL:
CREATE TABLE t (a INT, b DOUBLE, PRIMARY KEY (b));


-- TEST: invalid-bucket-calculation-gl2640.2
-- SQL:
EXPLAIN SELECT * FROM t WHERE b = 1;
-- EXPECTED:
projection ("t"."a"::int -> "a", "t"."b"::double -> "b")
    selection "t"."b"::double = 1::int
        scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1934]

-- TEST: invalid-bucket-calculation-gl2640.3
-- SQL:
EXPLAIN SELECT * FROM t WHERE b = 1::double;
-- EXPECTED:
projection ("t"."a"::int -> "a", "t"."b"::double -> "b")
    selection "t"."b"::double = 1::double
        scan "t"
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [2099]
