## fix/sql

- `IS TRUE`, `IS NOT TRUE`, `IS FALSE` and `IS NOT FALSE` no longer return
  `NULL` when the operand is `NULL`: `NULL IS TRUE` is now `false` and
  `NULL IS NOT TRUE` is now `true`. Before, these predicates worked as
  `= true` and `= false`, so filters such as `WHERE b IS NOT TRUE`
  skipped the rows where `b` is `NULL` ([!3897]).
