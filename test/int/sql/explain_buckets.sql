-- TEST: explain-setup
-- SQL:
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS tt;
DROP TABLE IF EXISTS g;
CREATE TABLE t (a INT, b DOUBLE, c TEXT, PRIMARY KEY (c, a));
CREATE TABLE tt (d INT PRIMARY KEY);
CREATE TABLE g (a INT PRIMARY KEY, b DOUBLE, c TEXT) DISTRIBUTED GLOBALLY;

-- TEST: buckets-select-any
-- SQL:
explain (buckets) select * from _pico_table;
-- EXPECTED:
buckets = any

-- TEST: buckets-select-filtered
-- SQL:
explain (buckets) select * from t where a = 1 and c = '2';
-- EXPECTED:
buckets = [2997]

-- TEST: buckets-join-filtered
-- SQL:
explain (buckets) select * from (select * from t where a = 5 and c = 'lol') t
join (select * from tt where d = 5) tt on t.a = tt.d;
-- EXPECTED:
buckets <= [219,442]

-- TEST: buckets-join-all
-- SQL:
explain (buckets) select * from t join tt on t.a = tt.d and t.c = '5';
-- EXPECTED:
buckets = [1-3000]

-- TEST: buckets-many-all
-- SQL:
explain (buckets) select * from t join (select * from tt union select * from tt)
tt on t.a = tt.d group by 1, 2, 3, 4 order by 2 limit 3;
-- EXPECTED:
buckets = [1-3000]

-- TEST: buckets-delete-filtered
-- SQL:
explain (buckets) delete from t where a = 1 and c = '2' or a = 3 and c = '4';
-- EXPECTED:
buckets = [2520,2997]

-- TEST: buckets-delete-all
-- SQL:
explain (buckets) delete from t where true;
-- EXPECTED:
buckets = [1-3000]

-- TEST: buckets-update-filtered
-- SQL:
explain (buckets) update t set b = b + 1 where a = 42 and c = 'lol';
-- EXPECTED:
buckets = [739]

-- TEST: buckets-update-all
-- SQL:
explain (buckets) update t set b = d + 1 from tt;
-- EXPECTED:
buckets = [1-3000]

-- TEST: buckets-insert-filtered
-- SQL:
explain (buckets) insert into t values (1, 5, 'kek');
-- EXPECTED:
buckets = [2036]

-- TEST: buckets-insert-unknown
-- SQL:
explain (buckets) insert into t select * from g;
-- EXPECTED:
buckets = unknown

-- TEST: buckets-insert-any
-- SQL:
explain (buckets) insert into g values (1, 1, 'lol');
-- EXPECTED:
buckets = any
