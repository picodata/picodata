-- TEST: test_join
-- SQL:
DROP TABLE IF EXISTS testing_space;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS gt;
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
CREATE TABLE t ("a" INT PRIMARY KEY, "b" INT, "c" INT);
CREATE TABLE gt ("a" INT PRIMARY KEY) DISTRIBUTED GLOBALLY;
insert into "testing_space" ("id", "name", "product_units") values
    (1, 'a', 1),
    (2, 'a', 1),
    (3, 'a', 2),
    (4, 'b', 1),
    (5, 'b', 2),
    (6, 'b', 3),
    (7, 'c', 4);
INSERT INTO t ("a", "b", "c") VALUES
    (1, 2, 3),
    (4, 5, 6),
    (7, 8, 9);
INSERT INTO gt ("a") VALUES (1), (2);

-- TEST: join1
-- SQL:
select *
          from
            "testing_space"
          join
            (select t1."id" as f, t2."id" as s
             from
                (select "id" from "testing_space") t1
             join
                (select "id" from "testing_space") t2
             on true
            )
          on "id" = f and "id" = s order by 1;
-- EXPECTED:
1, 'a', 1, 1, 1,
2, 'a', 1, 2, 2,
3, 'a', 2, 3, 3,
4, 'b', 1, 4, 4,
5, 'b', 2, 5, 5,
6, 'b', 3, 6, 6,
7, 'c', 4, 7, 7

-- TEST: test_join2
-- SQL:
SELECT * FROM t JOIN t AS t1 ON 1 IN (t1.b, t1.a, t1.c) ORDER BY 1;
-- EXPECTED:
1, 2, 3, 1, 2, 3,
4, 5, 6, 1, 2, 3, 
7, 8, 9, 1, 2, 3

-- TEST: test_join3
-- SQL:
SELECT * FROM t JOIN t AS t1 ON t1.a IN (t1.b, t1.a) ORDER BY 1;
-- EXPECTED:
1, 2, 3, 1, 2, 3,
1, 2, 3, 4, 5, 6,
1, 2, 3, 7, 8, 9,
4, 5, 6, 1, 2, 3,
4, 5, 6, 4, 5, 6,
4, 5, 6, 7, 8, 9,
7, 8, 9, 1, 2, 3,
7, 8, 9, 4, 5, 6,
7, 8, 9, 7, 8, 9

-- TEST: test_join4
-- SQL:
SELECT * FROM gt join gt as gt2 ON TRUE JOIN t ON TRUE;
-- EXPECTED:
1,	1,	1,	2,	3,
1,	1,	4,	5,	6,
1,	1,	7,	8,	9,
1,	2,	1,	2,	3,
1,	2,	4,	5,	6,
1,	2,	7,	8,	9,
2,	1,	1,	2,	3,
2,	1,	4,	5,	6,
2,	1,	7,	8,	9,
2,	2,	1,	2,	3,
2,	2,	4,	5,	6,
2,	2,	7,	8,	9


-- TEST: test_join5
-- SQL:
SELECT * FROM t JOIN gt ON true join gt as gt2 ON TRUE;
-- EXPECTED:
1,	2,	3,	1,	1,
1,	2,	3,	1,	2,
1,	2,	3,	2,	1,
1,	2,	3,	2,	2,
4,	5,	6,	1,	1,
4,	5,	6,	1,	2,
4,	5,	6,	2,	1,
4,	5,	6,	2,	2,
7,	8,	9,	1,	1,
7,	8,	9,	1,	2,
7,	8,	9,	2,	1,
7,	8,	9,	2,	2

-- TEST: test-check-condition-types-1
-- SQL:
with t(b) as (select true) select * from t t1 join t t2 on t1.b or false;
-- EXPECTED:
true, true

-- TEST: test-check-condition-types-2
-- SQL:
with t(b) as (select true) select * from t t1 join t t2 on t1.b or 1;
-- ERROR:
could not resolve operator overload for or\(bool, int\)

-- TEST: tets-sq-in-join-condition
-- SQL:
with t(a) as (values (1), (2), (3)) select * from t t1 join t t2 on (select true) and t1.a = t2.a;
-- EXPECTED:
1, 1, 2, 2, 3, 3

