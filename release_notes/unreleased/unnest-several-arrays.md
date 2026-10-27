## feat/sql

- The table function `unnest` takes several arrays, as in PostgreSQL:
  `unnest(ARRAY[1, 2], ARRAY['a'])` gives the columns `value_1` and
  `value_2` with the elements at the same position, and `NULL` for an array
  that has no element there. The rows are as many as the elements of the
  longest array.
