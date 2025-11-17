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

