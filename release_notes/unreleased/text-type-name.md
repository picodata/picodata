## feat/sql

- The text type is now spelled `text`, not `string`, in `EXPLAIN` output and
  error messages: `EXPLAIN (LOGICAL)` prints `t.a::text` and `'x'::text`,
  `EXPLAIN (RAW)` shows storage SQL with `CAST('x' AS text)`, and errors read
  e.g. `Failed to cast 1 to text`. Column types in the result metadata of
  iproto, Lua (`pico.sql`) and plugin SQL calls are still reported as `string`.
