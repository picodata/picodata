## feat/sql

- A statement whose data is on one bucket, including subqueries, `SELECT`
  without `FROM`, window functions, `UNION` and `EXCEPT`, now runs on that
  bucket's storage without moving data through the router. The same parameter
  in several sharding-key filters now selects one bucket, like the same
  literal, e.g. `SELECT * FROM t WHERE id = $1 AND x IN (SELECT x FROM s WHERE id = $1)`.
  Such statements are now allowed in transactional blocks.
