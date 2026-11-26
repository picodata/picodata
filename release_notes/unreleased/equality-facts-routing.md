## feat/sql

- A sharding key fixed through equalities with other columns now selects
  one bucket too. For example, `SELECT count(*) FROM t WHERE id = x AND x = $1`
  and `SELECT count(*) FROM (SELECT id FROM t WHERE id = $1) s JOIN t ON s.id = t.id`
  now run on that bucket's storage without moving data through the router.
  This works for sharding keys of the `INT`, `TEXT`, `UUID` and `BOOL` types.
  Equal `DECIMAL`, `DOUBLE` and `DATETIME` values can be stored in different
  forms that land in different buckets, so an equality on such a key no longer
  proves that a statement reads one bucket ([!3989]).
- An `INNER JOIN` of a global table with a sharded input that has no rows,
  e.g. `SELECT * FROM g JOIN (SELECT a FROM t WHERE false) s ON g.a = s.a`,
  now reads no bucket: `EXPLAIN (BUCKETS)` shows `buckets = []` for it instead
  of `buckets = any` ([!3989]).
- Conditions of one query block that conflict only together, e.g.
  `SELECT count(*) FROM (SELECT a FROM t WHERE a = 1) l JOIN (SELECT a FROM t WHERE a = 2) r ON l.a = r.a`
  or `SELECT * FROM (SELECT a, b FROM t WHERE b = 1) q WHERE q.b = 2`, now
  prove the result empty: `EXPLAIN (BUCKETS)` shows `buckets = []` or
  `buckets = any` for them, and `count(*)` over such a result runs in one
  stage on one node ([!4050]).
