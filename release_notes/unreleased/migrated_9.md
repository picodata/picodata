## feat/sql

- `EXPLAIN (RAW)` is now more informative and concise due to the new tree-like
  plan representation. The numbers in square brackets can indicate that a group
  of operations is performed on the same relation; in addition, they can be
  used as references to other operations.
