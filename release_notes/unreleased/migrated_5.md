## feat/sql

- Add new `BUCKETS` facet to `EXPLAIN` statement. Users can now inspect query
  buckets without producing a full execution plan. This facet can be combined
  with `RAW` and `FMT` options. When multiple facets are specified, output
  sections are separated by headers.
