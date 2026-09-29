## fix/sql

- An aggregate can now be the argument of a window function in a query without
  `GROUP BY`. `SELECT min(sum(a)) OVER () FROM t` used to fail with `misuse of
  aggregate: SUM()`, while the same query with a `GROUP BY` worked.
