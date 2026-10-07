## fix/sql

- Fixed truncation of `double` parameters. After a statement was first
  executed with an integral float value such as `3.0`, later executions
  dropped the fractional part: `0.5` was stored as `0.0`.
- A float parameter with an integral value is now a `double`, not an
  `int`. Thus `SELECT $1 / 2` with `3.0` returns `1.5`, and `3.0` is
  rejected for an `int` column. Send an integer value instead.
