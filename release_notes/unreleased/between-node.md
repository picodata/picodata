## fix/sql

- Nested `BETWEEN` no longer doubles the plan and the SQL text on each level. `BETWEEN` was two
  comparisons with a copy of its left operand in each, now it is one expression, and the SQL text
  for a storage writes `x BETWEEN lo AND hi`, so `x` is written once. A column on the left is still
  written as two comparisons, because Tarantool checks `BETWEEN` again when an index takes one of
  its bounds. `EXPLAIN` shows `between`, and `NOT BETWEEN` as `not x between lo and hi`. 18 levels
  with a subquery took 22 MB of SQL and 1 GiB, now about 1.4 KB, and 10-12 levels that failed in the
  parser of a storage now work. A `SELECT` without `FROM` whose subquery in `BETWEEN` reads one
  bucket runs on that bucket.
- A subquery in a join condition that is not an operand of a comparison ran on each storage over the
  rows of that storage, as in `ON t.a + (SELECT ...) > 5` or `ON t.b LIKE (SELECT ...)`. It gets the
  whole result of the subquery now.
- A `LEFT JOIN` of a global table with a sharded one lost matches when a subquery of its condition
  read a sharded table, as in `g LEFT JOIN t ON g.a = t.a AND t.b <= (SELECT ...)`. Such a subquery
  ran on each storage over the rows of that storage, now it gets its whole result.
