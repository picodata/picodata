-- TEST: trim1
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(id INT PRIMARY KEY, a INT);
INSERT INTO t VALUES(112211, 2211);

-- TEST: test_trim-1.1
-- SQL:
SELECT trim('  aabb  ') as "a" from "t";
-- EXPECTED:
'aabb'

-- TEST: test_trim-1.2
-- SQL:
SELECT trim(trim('  aabb  ')) from "t";
-- EXPECTED:
'aabb'

-- TEST: test_trim-1.3
-- SQL:
SELECT trim('a' from trim('  aabb  ')) from "t";
-- EXPECTED:
'bb'

-- TEST: test_trim-1.4
-- SQL:
SELECT trim(trim(' aabb ') from trim('  aabb  ')) from "t";
-- EXPECTED:
''

-- TEST: test_trim-1.5
-- SQL:
SELECT trim(leading 'a' from trim('aabb  ')) from "t";
-- EXPECTED:
'bb'

-- TEST: test_trim-1.6
-- SQL:
SELECT trim(trailing 'b' from trim('aabb')) from "t";
-- EXPECTED:
'aa'

-- TEST: test_trim-1.7
-- SQL:
SELECT trim(both 'ab' from 'aabb') from "t";
-- EXPECTED:
''

-- TEST: test_trim-1.8
-- SQL:
SELECT trim(  'a' from trim(  '  aabb  '  )  ) from "t";
-- EXPECTED:
'bb'

-- TEST: test_trim-1.9
-- SQL:
SELECT trim(  trim(  ' aabb ') from trim(  '  aabb  '  )  ) from "t";
-- EXPECTED:
''

-- TEST: test_trim-1.10
-- SQL:
SELECT trim(  leading 'a' from trim(  'aabb  '  )  ) from "t";
-- EXPECTED:
'bb'

-- TEST: test_trim-1.11
-- SQL:
SELECT trim(  trailing 'b' from trim(  'aabb'  )) from "t";
-- EXPECTED:
'aa'

-- TEST: test_trim-1.12
-- SQL:
SELECT trim(  both 'ab' from 'aabb'  ) from "t";
-- EXPECTED:
''

-- TEST: like1
-- SQL:
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (c1 TEXT, c2 INT primary key);
INSERT INTO t1 VALUES ('', 1);
INSERT INTO t1 VALUES ('p', 4);

-- TEST: test_like-1.1
-- SQL:
select c2 from t1 where c1 like '';
-- EXPECTED:
1

-- TEST: test_like-1.2
-- SQL:
SELECT (t1.c1 LIKE t1.c1) FROM t1;
-- EXPECTED:
true, true

-- TEST: test_like-1.3
-- SQL:
SELECT (t1.c1 LIKE t1.c1) FROM t1 WHERE ((trim(t1.c1 from t1.c1) LIKE t1.c1));
-- EXPECTED:
true

-- TEST: test-sqlfunc-casts-1.0
-- SQL:
drop table if exists t;
create table t(a int primary key, b int, c int);
INSERT INTO t VALUES (0, 0, 0), (1, 1, 1), (2, 2, 2);

-- TEST: test-sqlfunc-casts-1.1
-- SQL:
WITH
h AS (
    SELECT min(c) e, b i FROM t GROUP BY b
)
SELECT 2 FROM h GROUP BY i HAVING 0 = avg(e);
-- EXPECTED:
2

-- TEST: test-sqlfunc-casts-2.0
-- SQL:
drop table if exists t;
create table t (a int primary key, b int);
insert into t values (1, null), (2, null), (3, 3);

-- TEST: test-sqlfunc-casts-2.1
-- SQL:
select total(coalesce(b,b)) from t;
-- EXPECTED:
3.0

-- TEST: test-sqlfunc-casts-3.0
-- SQL:
drop table if exists t1;
create table t1(a int primary key, b int, c int);
drop table if exists t2;
create table t2 (a int primary key, b int, c int);
INSERT INTO t1(a,b,c) VALUES (1, 1, 1);
INSERT INTO t2(a,b,c) VALUES (10, 10, 10);

-- TEST: test-sqlfunc-casts-3.1
-- SQL:
SELECT d.b, e.b, g.c, d.c
FROM t2 d
    JOIN t2 h
        ON d.b = h.a
    JOIN t1 e
        ON true
    JOIN t1 g
        ON true
    UNION
        SELECT e.c, 0, count(g.c), min(i.a)
        FROM t1 d
            JOIN t2 h
                ON h.a = d.c
            JOIN t2 e
                ON true
            JOIN t1 g
                ON true
            JOIN t1 i
                ON g.a > (
                    SELECT count(a) f FROM t1 HAVING 1 > min(a) ORDER BY f LIMIT 1
                )
        GROUP BY e.c
OPTION(sql_vdbe_opcode_max = 0, sql_motion_row_max = 0);
-- EXPECTED:
10, 1, 1, 10

-- TEST: test-sqlfunc-casts-4.1
-- SQL:
select sum(null + null);
-- EXPECTED:

-- TEST: test-sqlfunc-casts-5.1
-- SQL:
explain select cast(max(cast(1 as int)) as double);
-- EXPECTED:
projection (max((1::double))::int::double -> "col_1")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = any

-- TEST: test-sqlfunc-casts-5.2
-- SQL:
explain select cast(max(cast(1 as double)) as double);
-- EXPECTED:
projection (max((1::double))::double::double -> "col_1")
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = any
