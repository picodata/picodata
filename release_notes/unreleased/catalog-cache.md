## perf/sql

- SQL keeps table and index definitions in memory instead of reading them from
  `_pico_table` and `_pico_index` on every query. A query with
  `read_preference` other than `leader` no longer slows down as the catalog
  grows: a point `SELECT` with 10 000 tables takes 0.03 ms instead of 2 ms.
  pgbench `INSERT ... ON CONFLICT DO NOTHING` runs about 3% faster ([!3843]).
- A row of `_pico_table` or `_pico_index` that cannot be decoded now stops the
  instance when the instance starts, applies a raft snapshot or applies a raft
  log entry that writes the row. Before, every query that read the row failed
  ([!3843]).
- SQL and CaS do not see a write to `_pico_table` or `_pico_index` that
  bypasses picodata until the instance restarts: a write from the Lua console
  or through `pico.cas` and `pico.raft_propose` into tarantool system tables
  such as `_truncate`. Before, SQL saw such a write at once ([!3843]).
