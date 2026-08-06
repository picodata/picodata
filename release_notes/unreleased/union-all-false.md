## fix/sql

- The query `SELECT 1 UNION ALL SELECT a FROM t WHERE false` now returns the 
  correct result.
