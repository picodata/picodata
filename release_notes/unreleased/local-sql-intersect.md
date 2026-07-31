## fix/sql

- Executing certain queries with `UNION` and `EXCEPT` no longer returns
  an error `Failed to compile SQL statement: Syntax error at line 1 near '('`,
  e.g. `SELECT 1 UNION SELECT 0 EXCEPT SELECT 1 from t;`.
