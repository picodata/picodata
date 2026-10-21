## perf/sql

- `to_char` and `to_date` are now built-in C functions of the SQL engine
  instead of Lua functions. They give the same results, but a call no longer
  goes through Lua, and some queries run up to 2 times faster.
