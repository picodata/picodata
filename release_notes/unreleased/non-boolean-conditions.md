## fix/sql

- A non-boolean condition is now reported with its type instead of the obscure
  `filter expression is not a trivalent expression`, e.g.
  `SELECT * FROM (SELECT 1) WHERE 1` fails with
  `argument of WHERE must be type boolean, not type int`.
- `CASE WHEN` conditions and window function `FILTER (WHERE ...)` clauses are
  now checked to be boolean, e.g. `SELECT CASE WHEN 1 THEN 1 END` is rejected
  instead of being silently accepted.
