## fix/sql

- `coalesce` with more than 127 arguments now fails at planning with
  `coalesce takes at most 127 arguments, got 128`, as `unnest` does.
  Before, the storage rejected the call with an error of Tarantool.
