## fix/sql

- Scalar aggregate with empty buckets set now returns `0` instead of `NULL`, 
  e.g. query `SELECT count(*) FROM t WHERE a = 1 AND a = 2`.
