## feat/sql

- Introduce `LOGICAL` facet in `EXPLAIN` statement. Users can now explicitly
  request the logical query plan. This facet can be combined with `RAW`,
  `BUCKETS`, and `FMT` options. The default `EXPLAIN` with no facets specified
  now emits both `LOGICAL` and `BUCKETS`.
