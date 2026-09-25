## feat/sql

- `EXPLAIN (RAW, BUCKETS)` now shows a `buckets = ...` line for each query of
  a transactional `DO` block. Queries that can run on any node show `buckets = any`.
  Examples of such queries are an `IF` condition with only constants and a
  `LET` with a literal value. The location in a stage header is now the
  location of the query of that stage.
- `EXPLAIN` now accepts transactional blocks that cannot be executed, for
  example blocks with queries on different buckets or queries that move
  data. Use `EXPLAIN` to find the query that causes the problem. If a query
  cannot run in a transaction, `EXPLAIN` shows the reason in place of its
  local SQL. The `BUCKETS` facet shows the union of the buckets of all
  queries. The `RAW` facet shows the buckets of each query. `EXPLAIN` also
  accepts a block with a query on all buckets or on no bucket. For an
  `INSERT` with more than one row, and with rows on different buckets,
  `EXPLAIN` shows each row with its own `bucket_id`.
