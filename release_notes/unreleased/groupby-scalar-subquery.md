## fix/sql

- Scalar subqueries in `GROUP BY` expressions now match their twin in the select 
  list and `HAVING`, e.g. `SELECT a + (SELECT 1) FROM t GROUP BY a + (SELECT 1)` 
  no longer fails with `column "a" is not found in grouping expressions`.
- A scalar subquery that appears both in `GROUP BY` and in the select list or 
  `HAVING` is now planned and executed once instead of once per occurrence.
