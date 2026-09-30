## fix/sql

- `COUNT` over a sharded table no longer fails with `Tuple field 1 (_COLUMN_0) type does not
  match one required by operation: expected decimal, got unsigned` when the query sorts or
  de-duplicates on the count, e.g. `SELECT b, count(a) FROM t GROUP BY b ORDER BY 2 LIMIT 8`
  or `SELECT DISTINCT count(a) FROM t GROUP BY b` ([!3720]).
- Function calls now report the type of the overload the type system resolved instead of a
  type guessed from the function name. `coalesce` no longer reports `any`, and `max`/`min`
  no longer report a type inferred before their argument was coerced ([!3720]).
- `SUM` and `AVG` over a `double` in a window now report `double`, which is what Tarantool
  computes, instead of `decimal`.
