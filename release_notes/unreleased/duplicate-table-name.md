## fix/sql

- Two items of a `FROM` clause can no longer be visible under the same name,
  as in PostgreSQL. `SELECT * FROM t JOIN t ON true` is now rejected with
  `table name "t" specified more than once`; give one of them an alias, e.g.
  `SELECT * FROM t JOIN t AS t2 ON true`. An alias is the name of its item,
  so `FROM t JOIN t1 AS t` is rejected as well.
- The updated table is an item of the `FROM` clause too, so
  `UPDATE t SET d = 1 FROM t` is rejected, while
  `UPDATE t SET d = 1 FROM t AS t2 WHERE t.a = t2.a` is accepted. A nested
  query has a `FROM` clause of its own and is unaffected.
