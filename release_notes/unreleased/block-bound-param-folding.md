## fix/sql

- A prepared transactional block (`DO $$ ... END $$`) that runs again with other parameter
  values no longer reuses the SQL text and the parameter values of its first execution.
  `UPDATE t SET v = v + 1 WHERE id = $1 AND ($2 OR v = 0)` bound with `$2 = false` and then
  with `$2 = true` now updates the row on the second execution ([!3770]).
- A transactional block no longer folds a bound boolean parameter. A bare boolean parameter
  in an `OR` branch of a filter, such as `WHERE pk = $1 OR $2`, now fails with
  `transaction cannot be executed on all buckets` for every value of `$2`. Before, the block
  ran when `$2` was `false` ([!3770]).
