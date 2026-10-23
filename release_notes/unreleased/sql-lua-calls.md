## perf/sql

- SQL queries no longer create a new Lua thread for every call into Lua.
  Bucket routing and vshard storage references take Lua threads from a pool,
  and the Lua functions they call are looked up once. A point query executed
  locally now allocates about 200 bytes in Lua instead of 2.9 KB, the Lua
  garbage collection on this path almost disappears, and TPC-B on a single
  instance runs about 10% more transactions per second. The pool keeps at most
  `min(sql_runtime_concurrency_max, 1024)` free threads and does not limit
  concurrent queries.
