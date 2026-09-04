## fix/sql

- Fixed a `sub-query plan subtree with id XXX not found` error in `EXPLAIN`
  for queries whose filter contains both a `NOT` and a subquery branch
  eliminated by constant folding (e.g.
  `WHERE NOT (a > 0) <= (EXISTS (SELECT 1) OR true)`).
