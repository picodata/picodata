## fix/sql

- `AVG` and `SUM` on an `int` column of a global table now return `decimal`, the
  type they are declared with, as PostgreSQL does. `AVG` also keeps the fraction:
  the average of 1 and 2 is 3/2, not 1 ([tarantool!507]).
- `SUM` on an `int` column does not fail now when the sum does not fit in 64 bits.
  Tarantool continues such a sum in decimal ([tarantool!507]).
