## refactor/sql

- `EXPLAIN` no longer prints the `output (...)` column list after a `group by`
  node. It repeated the child's columns verbatim, `bucket_id` and columns the
  query never references included, and doubled the height of every `group by`
  under `EXPLAIN (FMT)`. A grouped plan that used to read
  `group by (t1.a::int) output (t1.a::int -> a, t1.bucket_id::int -> bucket_id, t1.b::int -> b)`
  now reads `group by (t1.a::int)`. Column lists are unchanged on `projection`
  and on a `SELECT` without a scan, the nodes that decide a plan's output.
