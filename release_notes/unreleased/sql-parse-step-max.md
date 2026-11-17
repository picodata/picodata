## feat/sql

- Added the `sql_parse_step_max` parameter (default 30 000 000 steps,
  about 0.1 s), which caps the parser steps of an SQL query via
  `ALTER SYSTEM`. `0` removes the cap, but not the budget of 350 steps
  per byte of the query ([!3967]).
