## fix/sql

- A `GROUP BY` that names a select-list alias of a subquery expression now
  plans like the expression itself.
  `EXPLAIN SELECT (SELECT 1) AS s FROM t GROUP BY s` used to fail with
  `sub-query plan subtree ... not found`, and
  `SELECT b IN (SELECT a FROM t) AS s FROM t GROUP BY s` used to fail with
  `column "b" is not found in grouping expressions!`.
