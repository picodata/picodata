## fix/sql

- The `||` operator now has PostgreSQL-compatible precedence: a dedicated
  tier below `+`/`-` instead of sharing the `*`/`/` tier. Unparenthesized
  expressions mixing `||` with arithmetic may change meaning ([!3463]).
- `||` now accepts any scalar operand next to `text` (`int`, `numeric`,
  `double`, `bool`, `datetime`, `uuid`). Each argument unconditionally
  is converted with `CAST(... AS string)`, so its rendering follows
  Tarantool (e.g. `TRUE`, ISO 8601 datetimes) rather than PostgreSQL.
- Parameter types for `$1` used in `||` expressions are now reported
  correctly over pgproto: an implicit cast no longer overwrites the
  inferred parameter type.

----

The precedence change can silently alter results of unparenthesized
`||`-with-arithmetic expressions. Consider highlighting it prominently
in the release section.