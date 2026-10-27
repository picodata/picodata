## feat/sql

- Added the table functions `generate_series(start, stop[, step])` and
  `unnest(array)` in `FROM`. A function gives the column `value`, and
  `WITH ORDINALITY` adds the column `ordinality` with the row numbers.
  `generate_series` gives a series of integers, or of decimals when an
  argument is a decimal. A double argument is an error, as in PostgreSQL.
- `unnest` also expands the untyped arrays of the system tables, such as
  `_pico_table.format`. Their elements have no type, so a comparison
  needs an explicit cast, for example `value::text = 'Online'`.
- The arguments of a table function on the right side of a `JOIN` can
  reference the columns of the sources on its left, for example
  `SELECT * FROM t JOIN unnest(t.arr) u ON true`. The arguments can also
  contain subqueries.
