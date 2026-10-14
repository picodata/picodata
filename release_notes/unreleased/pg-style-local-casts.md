## refactor/sql

- The SQL a router sends to storages casts with the PostgreSQL-style `::`
  operator instead of `CAST(... AS ...)` ([tarantool!522]). `EXPLAIN (RAW)`
  shows `"t"."a" = 1::int` where it showed `"t"."a" = CAST(1 AS int)`.
