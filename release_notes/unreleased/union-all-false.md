## fix/sql

- Queries containing `UNION`/`UNION ALL`, where one of the parts contains a 
  branch with a condition that is always false could return an incorrect (empty) 
  result. Now such queries return the correct result, e.g.,
  `SELECT 1 UNION ALL SELECT a FROM t WHERE false`,
  `SELECT 1 UNION SELECT a FROM t WHERE a = 1 AND a = 2`.
