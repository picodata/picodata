-- TEST: unnamed_subquery
-- SQL:
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS gt;
create table t (a int primary key);
create table gt (a int primary key) distributed globally;
INSERT INTO t ("a") VALUES (1);
INSERT INTO gt ("a") VALUES (1), (2);

-- TEST: unnamed_subquery-same-columns-1
-- SQL:
select * from t join (select * from gt) on true
-- EXPECTED:
1, 1,
1, 2,

-- TEST: unnamed_subquery-same-columns-2
-- SQL:
select * from t join (select * from gt join gt as gt2 on true) on true
-- EXPECTED:
1, 1, 1,
1, 1, 2,
1, 2, 1,
1, 2, 2,

-- TEST: unnamed_subquery-same-columns-3
-- SQL:
select * from (select * from gt join gt as gt2 on true) join t on true
-- EXPECTED:
1, 1, 1,
1, 2, 1,
2, 1, 1,
2, 2, 1,
