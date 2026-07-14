## feat/sql

- `EXPLAIN (FMT)` option is now properly supported for all modes (facets).
  It is now possible to write `explain (fmt)` to get a formatted logical plan
  or `explain (fmt, raw)` to get a formatted raw query plan.
