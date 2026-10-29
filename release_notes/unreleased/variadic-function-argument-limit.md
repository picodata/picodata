## fix/sql

- `coalesce` and `json_extract_path` with more than 127 arguments now fail at
  planning with `coalesce takes at most 127 arguments, got 128`, as `unnest`
  does. Before, the storage rejected the call with an error of Tarantool.
