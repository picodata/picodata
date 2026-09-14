## feat/sql

- Introduce the `VERBOSE` option of `EXPLAIN`, which prints the whole list of
  buckets in the `BUCKETS` facet. Without it, only as many buckets as fit into
  a single line are printed, followed by the number of buckets left out, e.g.
  `... (11 more)`. Such a list stays on one line even with the `FMT` option.
