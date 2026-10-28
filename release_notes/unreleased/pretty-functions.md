## feat/sql

- New scalar function `pico_size_pretty`, as `pg_size_pretty` in PostgreSQL:
  `pico_size_pretty(10485760)` gives `10 MB`. It takes an `int` or a `decimal`
  size in bytes and gives the same text as PostgreSQL.
- New scalar function `pico_num_pretty`, which gives a number as `llvm-cov`
  gives counts: `pico_num_pretty(1234567)` gives `1.23M`. It takes an `int` or
  a `decimal`.
- New scalar function `pico_sql_pretty`, which formats a query as
  `EXPLAIN (RAW, FMT)` shows it. If formatting changes the tokens of the query,
  the function gives the query unchanged.
