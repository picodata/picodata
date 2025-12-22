-- TEST: test_orderby
-- SQL:
DROP TABLE IF EXISTS null_t;
CREATE TABLE null_t ("na" int primary key, "nb" int, "nc" int);
INSERT INTO "null_t"
("na", "nb", "nc")
VALUES 
    (1, 2, 1),
    (2, null, 3),
    (3, 2, 3),
    (4, 3, 1),
    (5, 1, 5),
    (6, -1, 3),
    (7, 1, 1),
    (8, null, -1);

-- TEST: orderby1
-- SQL:
select * from "null_t" order by "na";
-- EXPECTED:
1, 2, 1,
2, NULL, 3,
3, 2, 3,
4, 3, 1,
5, 1, 5,
6, -1, 3,
7, 1, 1,
8, NULL, -1

-- TEST: orderby2
-- SQL:
select * from "null_t" order by 1;
-- EXPECTED:
1, 2, 1,
2, NULL, 3,
3, 2, 3,
4, 3, 1,
5, 1, 5,
6, -1, 3,
7, 1, 1,
8, NULL, -1

-- TEST: orderby3
-- SQL:
select * from "null_t" order by "na" asc;
-- EXPECTED:
1, 2, 1,
2, NULL, 3,
3, 2, 3,
4, 3, 1,
5, 1, 5,
6, -1, 3,
7, 1, 1,
8, NULL, -1

-- TEST: orderby4
-- SQL:
select * from "null_t" order by 1 asc;
-- EXPECTED:
1, 2, 1,
2, NULL, 3,
3, 2, 3,
4, 3, 1,
5, 1, 5,
6, -1, 3,
7, 1, 1,
8, NULL, -1

-- TEST: orderby5
-- SQL:
select * from "null_t" order by 1, 2;
-- EXPECTED:
1, 2, 1,
2, NULL, 3,
3, 2, 3,
4, 3, 1,
5, 1, 5,
6, -1, 3,
7, 1, 1,
8, NULL, -1

-- TEST: orderby6
-- SQL:
select * from "null_t" order by 4;
-- ERROR:
invalid expression: Ordering index \(4\) is bigger than child projection output length \(3\)

-- TEST: orderby7
-- SQL:
select * from "null_t" order by "nb", "na";
-- EXPECTED:
2, NULL, 3,
8, NULL, -1,
6, -1, 3,
5, 1, 5,
7, 1, 1,
1, 2, 1,
3, 2, 3,
4, 3, 1

-- TEST: orderby8
-- SQL:
select * from "null_t" order by "nb" asc, "na";
-- EXPECTED:
2, NULL, 3,
8, NULL, -1,
6, -1, 3,
5, 1, 5,
7, 1, 1,
1, 2, 1,
3, 2, 3,
4, 3, 1

-- TEST: orderby9
-- SQL:
select * from "null_t" order by 2, 1;
-- EXPECTED:
2, NULL, 3,
8, NULL, -1,
6, -1, 3,
5, 1, 5,
7, 1, 1,
1, 2, 1,
3, 2, 3,
4, 3, 1

-- TEST: orderby10
-- SQL:
select * from "null_t" order by "na" desc;
-- EXPECTED:
8, NULL, -1,
7, 1, 1,
6, -1, 3,
5, 1, 5,
4, 3, 1,
3, 2, 3,
2, NULL, 3,
1, 2, 1

-- TEST: orderby11
-- SQL:
select * from "null_t" order by "nb" desc, "na";
-- EXPECTED:
4, 3, 1,
1, 2, 1,
3, 2, 3,
5, 1, 5,
7, 1, 1,
6, -1, 3,
2, NULL, 3,
8, NULL, -1

-- TEST: orderby12
-- SQL:
select * from "null_t" order by "nb" * 2 + 42 * "nb" desc, "na";
-- EXPECTED:
4, 3, 1,
1, 2, 1,
3, 2, 3,
5, 1, 5,
7, 1, 1,
6, -1, 3,
2, NULL, 3,
8, NULL, -1

-- TEST: orderby13
-- SQL:
select * from "null_t" order by "nb" desc, "na" desc;
-- EXPECTED:
4, 3, 1,
3, 2, 3,
1, 2, 1,
7, 1, 1,
5, 1, 5,
6, -1, 3,
8, NULL, -1,
2, NULL, 3

-- TEST: orderby14
-- SQL:
select * from "null_t" order by 2 asc, 1 desc, 2 desc, 1 asc;
-- EXPECTED:
8, NULL, -1,
2, NULL, 3,
6, -1, 3,
7, 1, 1,
5, 1, 5,
3, 2, 3,
1, 2, 1,
4, 3, 1

-- TEST: test-check-orderby-precedence
-- SQL:
with t(a) as (values (1), (2)) select a from t union all select a from t order by a asc;
-- EXPECTED:
1, 1, 2, 2

-- TEST: test-sq-in-orderby
-- SQL:
with t(a) as (values (1), (2), (3)) select a from t order by (select 1), a desc;
-- EXPECTED:
3, 2, 1

-- TEST: test-orderby-limit-pushdown-1.0
-- SQL:
drop table if exists t1;
create table t1(
    a int primary key,
    b int
);
insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);

-- TEST: test-orderby-limit-pushdown-1.1
-- SQL:
SELECT DISTINCT t0.a AS c0, t0.a AS c1 FROM t1 AS t0 ORDER BY c0 ASC, c1 ASC LIMIT 2;
-- EXPECTED:
1, 1, 2, 2

-- TEST: test-orderby-limit-pushdown-1.2
-- SQL:
SELECT DISTINCT t0.a AS c0, t0.a + 1 AS c1 FROM t1 AS t0 ORDER BY c0 ASC, c1 ASC LIMIT 2;
-- EXPECTED:
1, 2, 2, 3

-- TEST: test-orderby-limit-pushdown-1.3
-- SQL:
SELECT t0.a AS c0, t0.a AS c1, sum(t0.b) as b_sum_0, sum(t0.b) as b_sum_1 FROM t1 AS t0 GROUP BY c0, c1 ORDER BY c0 DESC, c1 ASC LIMIT 1;
-- EXPECTED:
5, 5, 5, 5

-- TEST: test-orderby-limit-pushdown-1.4
-- SQL:
SELECT t0.a AS c0, t0.a AS c1, avg(t0.b) as b_avg_0, avg(t0.b) as b_avg_1 FROM t1 AS t0 GROUP BY c0, c1 ORDER BY c0 ASC, c1 DESC LIMIT 1;
-- EXPECTED:
1, 1, 1, 1

-- TEST: test-orderby-limit-pushdown-1.5
-- SQL:
select distinct t0.a as c0, t0.b as c1, t0.a as c2 from t1 as t0 order by c0 + c2 desc limit 2;
-- EXPECTED:
5, 5, 5, 4, 4, 4

-- TEST: test-orderby-limit-pushdown-1.6
-- SQL:
select t0.a as c0, sum(t0.b) as c1, t0.a as c2 from t1 AS t0 group by c0, c2 order by c0 + c2 desc limit 2;
-- EXPECTED:
5, 5, 5, 4, 4, 4

-- TEST: test-orderby-limit-pushdown-1.7
-- SQL:
select distinct t0.a as c0, t0.b as c1, t0.a as c2 from t1 as t0 order by 3 desc limit 2;
-- EXPECTED:
5, 5, 5, 4, 4, 4

-- TEST: test-orderby-limit-pushdown-1.8
-- SQL:
select t0.a as c0, sum(t0.b) as c1, t0.a as c2 from t1 as t0 group by c0, c2 order by 3 desc limit 2;
-- EXPECTED:
5, 5, 5, 4, 4, 4

-- TEST: test-orderby-limit-pushdown-1.9
-- SQL:
with cte(a) as (select a from t1) select (select count(*) from cte), (select a from cte order by a limit 1);
-- EXPECTED:
5, 1
