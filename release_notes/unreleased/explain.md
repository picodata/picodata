## feat/sql

- Support `LOGICAL`, `BUCKETS`, and `FORWARD` modes of EXPLAIN for transactions ([!3184]).
- Add per query bucket estimation to EXPLAIN (RAW) output when BUCKETS mode is
  specified ([!3280]).
- Provide more accurate info about query execution location in EXPLAIN (RAW)
  output ([!3330]).
- Reflect the planning caveats for queries with UNION of global and sharded
  tables in EXPLAIN(RAW) ([!3357]).
