-- TEST: init
-- SQL:
CREATE TABLE t1 (a INT PRIMARY KEY, b INT ARRAY);

-- TEST: invalid-sharding-key
-- SQL:
CREATE TABLE t (a INT ARRAY PRIMARY KEY);
-- ERROR:
Sharding key column a is not of scalar type.

-- TEST: insert-base
-- SQL:
INSERT INTO t1 VALUES (1, ARRAY[1, 2, 3]);

-- TEST: insert-empty
-- SQL:
INSERT INTO t1 VALUES (2, ARRAY[]);

-- TEST: insert-null
-- SQL:
INSERT INTO t1 VALUES (3, NULL);

-- TEST: insert-null-array
-- SQL:
INSERT INTO t1 VALUES (4, ARRAY[NULL]);

-- TEST: insert-mixed-text
-- SQL:
INSERT INTO t1 VALUES (5, ARRAY['3', 2, 1]);

-- TEST: insert-mixed-decimal
-- SQL:
INSERT INTO t1 VALUES (6, ARRAY[3., 2, 1]);
-- ERROR:
column at position 2 is of type int[], but expression is of type numeric[]

-- TEST: insert-scalar-decimal
-- SQL:
INSERT INTO t1 VALUES (6., NULL);
-- ERROR:
column at position 1 is of type int, but expression is of type numeric

-- TEST: insert-mixed-double
-- SQL:
INSERT INTO t1 VALUES (7, ARRAY[3::double, 2, 1]);
-- ERROR:
column at position 2 is of type int[], but expression is of type double[]

-- TEST: insert-scalar-double
-- SQL:
INSERT INTO t1 VALUES (7::double, NULL);
-- ERROR:
column at position 1 is of type int, but expression is of type double

-- TEST: insert-mixed
-- SQL:
INSERT INTO t1 VALUES (8, ARRAY[3::numeric, '2', 1::double]);
-- ERROR:
column at position 2 is of type int[], but expression is of type numeric[]

-- TEST: insert-text
-- SQL:
INSERT INTO t1 VALUES (9, ARRAY[1, 2, 'hello']);
-- ERROR:
consider using explicit type casts

-- TEST: insert-text-num
-- SQL:
INSERT INTO t1 VALUES (10, ARRAY['0']);

-- TEST: insert-nested-array-rejected
-- SQL:
INSERT INTO t1 VALUES (11, ARRAY[ARRAY[1]]);
-- ERROR:
nested arrays are not supported

-- TEST: insert-bool-array-into-int
-- SQL:
INSERT INTO t1 VALUES (12, ARRAY[true, false]);
-- ERROR:
column at position 2 is of type int[], but expression is of type bool[]

-- TEST: insert-scalar-int-into-array
-- SQL:
INSERT INTO t1 VALUES (13, 1);
-- ERROR:
column at position 2 is of type int\[\], but expression is of type int

-- TEST: insert-array-with-null-element
-- SQL:
INSERT INTO t1 VALUES (14, ARRAY[1, NULL, 3]);

-- TEST: insert-multi-row
-- SQL:
INSERT INTO t1 VALUES (15, ARRAY[1, 2]), (16, ARRAY['3', '4']);

-- TEST: insert-multi-row-mismatch
-- SQL:
INSERT INTO t1 VALUES (17, ARRAY[1, 2]), (18, ARRAY[1.5, 2.5]);
-- ERROR:
column at position 2 is of type int[], but expression is of type decimal[]

-- TEST: select-index-not-null
-- SQL:
SELECT b[b[1]] FROM t1 WHERE b IS NOT NULL;
-- EXPECTED:
1, None, None, 1, None, 1, 1, None

-- TEST: select-index-expression
-- SQL:
SELECT b[1]+4 FROM t1 WHERE a = 5;
-- EXPECTED:
7

-- TEST: init-dbl
-- SQL:
CREATE TABLE t_dbl (a INT PRIMARY KEY, b DOUBLE ARRAY);

-- TEST: dbl-int-literals-coerced
-- SQL:
INSERT INTO t_dbl VALUES (1, ARRAY[1, 2, 3]);

-- TEST: dbl-numeric-literal-coerced
-- SQL:
INSERT INTO t_dbl VALUES (2, ARRAY[1.5, 2.5]);

-- TEST: dbl-mixed-int-numeric
-- SQL:
INSERT INTO t_dbl VALUES (3, ARRAY[1, 2.5, 3]);

-- TEST: dbl-text-coerced
-- SQL:
INSERT INTO t_dbl VALUES (4, ARRAY['1.5', '2.5']);

-- TEST: dbl-text-unparseable
-- SQL:
INSERT INTO t_dbl VALUES (5, ARRAY['hello']);
-- ERROR:
consider using explicit type casts

-- TEST: dbl-bool-rejected
-- SQL:
INSERT INTO t_dbl VALUES (6, ARRAY[true]);
-- ERROR:
column at position 2 is of type double[], but expression is of type bool[]

-- TEST: init-dec
-- SQL:
CREATE TABLE t_dec (a INT PRIMARY KEY, b DECIMAL ARRAY);

-- TEST: dec-int-literals-coerced
-- SQL:
INSERT INTO t_dec VALUES (1, ARRAY[1, 2, 3]);

-- TEST: dec-mixed-int-numeric
-- SQL:
INSERT INTO t_dec VALUES (2, ARRAY[1, 2.5, 3]);

-- TEST: dec-double-rejected
-- SQL:
INSERT INTO t_dec VALUES (3, ARRAY[1::double, 2::double]);

-- TEST: init-txt
-- SQL:
CREATE TABLE t_txt (a INT PRIMARY KEY, b TEXT ARRAY);

-- TEST: txt-base
-- SQL:
INSERT INTO t_txt VALUES (1, ARRAY['hello', 'world']);

-- TEST: txt-int-rejected
-- SQL:
INSERT INTO t_txt VALUES (2, ARRAY[1, 2, 3]);
-- ERROR:
column at position 2 is of type text[], but expression is of type int[]

-- TEST: txt-mixed-text-int-rejected
-- SQL:
INSERT INTO t_txt VALUES (3, ARRAY['hi', 1]);
-- ERROR:
consider using explicit type casts

-- TEST: init-bool-tbl
-- SQL:
CREATE TABLE t_bool (a INT PRIMARY KEY, b BOOL ARRAY);

-- TEST: bool-base
-- SQL:
INSERT INTO t_bool VALUES (1, ARRAY[true, false, true]);

-- TEST: bool-text-coerced
-- SQL:
INSERT INTO t_bool VALUES (2, ARRAY['true', 'false']);

-- TEST: bool-text-unparseable
-- SQL:
INSERT INTO t_bool VALUES (3, ARRAY['maybe']);
-- ERROR:
consider using explicit type casts

-- TEST: bool-int-rejected
-- SQL:
INSERT INTO t_bool VALUES (4, ARRAY[1, 0]);
-- ERROR:
column at position 2 is of type bool[], but expression is of type int[]

-- TEST: init-dt
-- SQL:
CREATE TABLE t_dt (a INT PRIMARY KEY, b DATETIME ARRAY);

-- TEST: dt-text-coerced
-- SQL:
INSERT INTO t_dt VALUES (1, ARRAY['2025-05-27T00:00:00Z']);

-- TEST: dt-text-unparseable
-- SQL:
INSERT INTO t_dt VALUES (2, ARRAY['not a date']);
-- ERROR:
consider using explicit type casts

-- TEST: dt-int-rejected
-- SQL:
INSERT INTO t_dt VALUES (3, ARRAY[1, 2]);
-- ERROR:
column at position 2 is of type datetime[], but expression is of type int[]

-- TEST: init-uu
-- SQL:
CREATE TABLE t_uu (a INT PRIMARY KEY, b UUID ARRAY);

-- TEST: uu-text-coerced
-- SQL:
INSERT INTO t_uu VALUES (1, ARRAY['11111111-1111-1111-1111-111111111111']);

-- TEST: uu-text-unparseable
-- SQL:
INSERT INTO t_uu VALUES (2, ARRAY['not a uuid']);
-- ERROR:
consider using explicit type casts

-- TEST: uu-int-rejected
-- SQL:
INSERT INTO t_uu VALUES (3, ARRAY[1]);
-- ERROR:
column at position 2 is of type uuid[], but expression is of type int[]

-- TEST: uint-array-rejected
-- SQL:
CREATE TABLE t_uint (a INT PRIMARY KEY, b UNSIGNED[]);
-- ERROR:
TypeUnsigned cannot be used as an array element type

-- TEST: empty-into-dbl
-- SQL:
INSERT INTO t_dbl VALUES (90, ARRAY[]);

-- TEST: empty-into-txt
-- SQL:
INSERT INTO t_txt VALUES (90, ARRAY[]);

-- TEST: empty-into-bool
-- SQL:
INSERT INTO t_bool VALUES (90, ARRAY[]);

-- TEST: null-elem-into-bool
-- SQL:
INSERT INTO t_bool VALUES (91, ARRAY[NULL]);

-- TEST: null-elem-into-dt
-- SQL:
INSERT INTO t_dt VALUES (91, ARRAY[NULL]);

-- TEST: index-arith-add
-- SQL:
SELECT b[1]+b[3] FROM t1 WHERE a = 1;
-- EXPECTED:
4

-- TEST: index-arith-sub
-- SQL:
SELECT b[1]-b[2] FROM t1 WHERE a = 5;
-- EXPECTED:
1

-- TEST: index-arith-mul
-- SQL:
SELECT b[2]*b[3] FROM t1 WHERE a = 1;
-- EXPECTED:
6

-- TEST: index-plus-column
-- SQL:
SELECT a+b[1] FROM t1 WHERE a = 16;
-- EXPECTED:
19

-- TEST: index-dynamic-index
-- SQL:
SELECT b[a]+10 FROM t1 WHERE a = 1;
-- EXPECTED:
11

-- TEST: index-null-element
-- SQL:
SELECT b[2]+1 FROM t1 WHERE a = 14;
-- EXPECTED:
None

-- TEST: index-out-of-bounds
-- SQL:
SELECT b[5]+1 FROM t1 WHERE a = 1;
-- EXPECTED:
None

-- TEST: index-literal-empty-arith
-- SQL:
SELECT array[][1] + 100;
-- EXPECTED:
None

-- TEST: index-literal-nonempty-arith
-- SQL:
SELECT array[1, 2][1] + 100;
-- EXPECTED:
101

-- TEST: index-literal-nonempty-arith-2
-- SQL:
SELECT array[1, 2][1] + 100.5;
-- EXPECTED:
101.5

-- TEST: index-init-clean
-- SQL:
CREATE TABLE t_idx (a INT PRIMARY KEY, b INT ARRAY);

-- TEST: index-fill-clean
-- SQL:
INSERT INTO t_idx VALUES (1, ARRAY[10, 20, 30]), (2, ARRAY[5, 15]), (3, ARRAY[7, 8, 9]);

-- TEST: index-filter-eq
-- SQL:
SELECT a FROM t_idx WHERE b[1] = 5 ORDER BY a;
-- EXPECTED:
2

-- TEST: index-filter-cmp
-- SQL:
SELECT a FROM t_idx WHERE b[1] > 6 ORDER BY a;
-- EXPECTED:
1, 3

-- TEST: index-order-by
-- SQL:
SELECT a, b[1] FROM t_idx ORDER BY 2;
-- EXPECTED:
2, 5, 3, 7, 1, 10

-- TEST: index-aggregate-sum
-- SQL:
SELECT sum(b[1]) FROM t1 WHERE a IN (1, 15, 16);
-- EXPECTED:
5

-- TEST: index-double-arith
-- SQL:
SELECT b[1]+b[2] FROM t_dbl WHERE a = 2;
-- EXPECTED:
4.0

-- TEST: index-double-mul
-- SQL:
SELECT b[2]*b[2] FROM t_dbl WHERE a = 2;
-- EXPECTED:
6.25

-- TEST: index-decimal-arith
-- SQL:
SELECT b[2]+b[3] FROM t_dec WHERE a = 1;
-- EXPECTED:
5

-- TEST: index-decimal-frac
-- SQL:
SELECT b[1]+b[2] FROM t_dec WHERE a = 2;
-- EXPECTED:
3.5

-- TEST: index-text-projection
-- SQL:
SELECT b[1] FROM t_txt WHERE a = 1;
-- EXPECTED:
hello

-- TEST: index-text-concat
-- SQL:
SELECT b[1] || b[2] FROM t_txt WHERE a = 1;
-- EXPECTED:
helloworld

-- TEST: cast-literal-text-to-int-arith
-- SQL:
SELECT (ARRAY['1', '2']::int[])[1] + 100;
-- EXPECTED:
101

-- TEST: cast-op-spelling-text-to-int
-- SQL:
SELECT (CAST(ARRAY['5', '6'] AS int[]))[2];
-- EXPECTED:
6

-- TEST: cast-literal-int-to-double-arith
-- SQL:
SELECT (ARRAY[1, 2]::double[])[2] + 0.5;
-- EXPECTED:
2.5

-- TEST: cast-decimal-literal-truncates-pos
-- SQL:
SELECT (ARRAY[1.4, 2.5, -1.5]::int[])[2];
-- EXPECTED:
2

-- TEST: cast-decimal-literal-truncates-neg
-- SQL:
SELECT (ARRAY[1.4, 2.5, -1.5]::int[])[3];
-- EXPECTED:
-1

-- TEST: cast-array-on-reference
-- SQL:
SELECT (b::int[])[2] FROM t1 WHERE a = 1;
-- EXPECTED:
2

-- TEST: cast-array-on-reference-no-parens
-- SQL:
SELECT b::int[][2] FROM t1 WHERE a = 1;
-- EXPECTED:
2

-- TEST: cast-then-index-equals-parenthesized
-- SQL:
SELECT (b::int[])[2] = b::int[][2] FROM t1 WHERE a = 1;
-- EXPECTED:
true

-- TEST: cast-then-index-in-arithmetic
-- SQL:
SELECT b::int[][1] + b::int[][3] FROM t1 WHERE a = 1;
-- EXPECTED:
4

-- TEST: cast-then-index-in-predicate
-- SQL:
SELECT count(*) FROM t1 WHERE a = 1 AND b::int[][1] = 1;
-- EXPECTED:
1

-- TEST: cast-literal-then-index
-- SQL:
SELECT ARRAY[10, 20, 30]::int[][2];
-- EXPECTED:
20

-- TEST: cast-literal-decimal-truncates-then-index
-- SQL:
SELECT ARRAY[1.4, 2.5, -1.5]::int[][3];
-- EXPECTED:
-1

-- TEST: cast-literal-text-coerces-then-index
-- SQL:
SELECT ARRAY['10', '20']::int[][1];
-- EXPECTED:
10

-- TEST: cast-double-then-index-arithmetic
-- SQL:
SELECT ARRAY[1, 2]::double[][2] + 0.5;
-- EXPECTED:
2.5

-- TEST: cast-multidim-rejected
-- SQL:
SELECT b::int[][] FROM t1;
-- ERROR:
rule parsing error

-- TEST: cast-multidim-string-key-error
-- SQL:
SELECT b::int[]['a'] FROM t1;
-- ERROR:
failed to parse 'a' as a value of type int


-- TEST: cast-multidim-deep-rejected
-- SQL:
SELECT b::int[][][][] FROM t1;
-- ERROR:
rule parsing error

-- TEST: cast-decimal-into-int-col
-- SQL:
INSERT INTO t1 VALUES (103, ARRAY[3.7, 4.2]::int[]);

-- TEST: cast-decimal-into-int-col-readback
-- SQL:
SELECT b[1] + b[2] FROM t1 WHERE a = 103;
-- EXPECTED:
7

-- TEST: cast-empty-into-int-col
-- SQL:
INSERT INTO t1 VALUES (100, ARRAY[]::int[]);

-- TEST: cast-empty-into-int-col-readback
-- SQL:
SELECT b[1] FROM t1 WHERE a = 100;
-- EXPECTED:
None

-- TEST: cast-nonempty-into-int-col
-- SQL:
INSERT INTO t1 VALUES (101, ARRAY['10', '20']::int[]);

-- TEST: cast-nonempty-into-int-col-readback
-- SQL:
SELECT b[1] + b[2] FROM t1 WHERE a = 101;
-- EXPECTED:
30

-- TEST: cast-array-on-scalar-reference-rejected
-- SQL:
SELECT a::int[] FROM t1;
-- ERROR:
Failed to cast

-- TEST: cast-array-on-scalar-const-rejected
-- SQL:
SELECT 5::int[];
-- ERROR:
Failed to cast

-- TEST: cast-null-to-array
-- SQL:
SELECT NULL::int[];
-- EXPECTED:
None

-- TEST: cast-null-into-int-col
-- SQL:
INSERT INTO t1 VALUES (102, NULL::int[]);

-- TEST: cast-null-into-int-col-readback
-- SQL:
SELECT a FROM t1 WHERE a = 102 AND b IS NULL;
-- EXPECTED:
102

-- TEST: index-null-cast-literal
-- SQL:
SELECT (null::int[])[1];
-- EXPECTED:
None

-- TEST: index-null-cast-arith
-- SQL:
SELECT (null::int[])[1] + 100;
-- EXPECTED:
None

-- TEST: index-null-column
-- SQL:
SELECT b[1] FROM t1 WHERE a = 102;
-- EXPECTED:
None

-- TEST: ddl-multi-brackets
-- SQL:
CREATE TABLE tt (a INT PRIMARY KEY, b INT[], c INT[3][3], d INT[][5], e INT ARRAY[5]);

-- TEST: explain-logical-array-cast-literal
-- SQL:
EXPLAIN (LOGICAL) SELECT ARRAY[1.1]::int[];
-- EXPECTED:
projection ("_pico_array_cast"(ARRAY[1.1::decimal], 'int') -> col_1)

-- TEST: explain-logical-array-cast-reference
-- SQL:
EXPLAIN (LOGICAL) SELECT b::double[] FROM t1;
-- EXPECTED:
projection ("_pico_array_cast"(t1.b::int[], 'double') -> col_1)
  scan t1

-- TEST: explain-raw-array-cast-literal
-- SQL:
EXPLAIN (RAW) SELECT ARRAY[1.1]::int[];
-- EXPECTED:
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT "_pico_array_cast" ([ CAST($1 AS decimal) ], 'int') as "col_1"
''
plan:
    [0] TRIVIAL

-- TEST: explain-raw-array-cast-reference
-- SQL:
EXPLAIN (RAW) SELECT b::double[] FROM t1;
-- EXPECTED:
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT "_pico_array_cast" ("t1"."b", 'double') as "col_1" FROM "t1"
''
plan:
    [0] SCAN TABLE t1 (~1048576 rows)
