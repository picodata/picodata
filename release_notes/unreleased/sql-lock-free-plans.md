## perf/sql

- Concurrent executions of the same SQL query on a storage no longer run one
  at a time when the storage part of the query uses no temporary tables (no
  motion). Such queries, for example a key lookup or a range read of one
  table, now overlap their disk reads; on a vinyl dataset at 8 clients this
  gave 20–40% more throughput.
