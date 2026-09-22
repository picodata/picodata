## fix/sql

- `SUM` and `AVG` over a `double` in a window now report `double`, which is what Tarantool
  computes, instead of `decimal`.
