-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: init
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (a INT PRIMARY KEY, q TEXT);
INSERT INTO t VALUES (1, 'select a,b from t where a=1'), (2, NULL);

-- TEST: pico-sql-pretty-short
-- SQL:
SELECT pico_sql_pretty('select   a,b from t   where a=$1 and b = ?');
-- EXPECTED:
'select a, b from t where a = $1 and b = ?'

-- TEST: pico-sql-pretty-long
-- SQL:
SELECT pico_sql_pretty('select t.a, t.b, count(*) from some_long_table_name t join other_table o on t.a = o.a where t.b > 10 group by t.a, t.b order by 3 desc');
-- EXPECTED:
'select
  t.a,
  t.b,
  count(*)
from
  some_long_table_name t
  join other_table o on t.a = o.a
where
  t.b > 10
group by
  t.a,
  t.b
order by
  3 desc'

-- TEST: pico-sql-pretty-not-sql
-- SQL:
SELECT pico_sql_pretty('not sql at all (((');
-- EXPECTED:
'not sql at all ((('

-- TEST: pico-sql-pretty-backslash-is-not-changed
-- SQL:
SELECT pico_sql_pretty($1);
-- PARAMS:
'select  a from t where a like '%\''
-- EXPECTED:
'select  a from t where a like '%\''

-- TEST: pico-sql-pretty-null
-- SQL:
SELECT pico_sql_pretty(NULL);
-- EXPECTED:
None

-- TEST: pico-sql-pretty-columns
-- SQL:
SELECT a, pico_sql_pretty(q) FROM t ORDER BY a;
-- EXPECTED:
1, 'select a, b from t where a = 1',
2, None
