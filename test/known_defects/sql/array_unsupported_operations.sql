-- TEST: array-compared-in-where-clause
-- SQL:
with cte(a) as (values (array[1]))
select * from cte where cte.a = array[5];
-- ERROR:
sbroad: Query 2 from EXPLAIN \(RAW\): Type mismatch: can not convert array\(\[1\]\) to comparable type

-- TEST: array-compared-union
-- SQL:
with cte(a) as (values (array[1, 2, 3]))
select * from cte union select * from cte;
-- ERROR:
Tuple field 1 \(_COLUMN_0\) type does not match one required by operation: expected scalar, got array

-- TEST: array-compared-left-join
-- SQL:
with cte(a) as (values (array[1, 2, 3]))
select * from cte left join cte t2 on true;
-- ERROR:
Failed to execute SQL statement: field type 'array' is not comparable

-- TEST: array-distribution-key
-- SQL:
create table t (id int primary key, val int[]) distributed by (val);
-- ERROR:
sbroad: invalid column: Sharding key column val is not of scalar type.

-- TEST: array-primary-key
-- SQL:
create table t (id int, val int[] primary key) distributed by (id);
-- ERROR:
field type 'array' is not supported
