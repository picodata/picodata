## perf/sql

- A SQL query executed on a storage no longer calls Lua to protect its data
  from bucket rebalancing, as long as no bucket is moving. The vshard storage
  scheduler keeps its counters in memory shared with picodata, and the query
  just increments one. When a bucket move runs or waits, the query takes a
  vshard storage reference as before, with the same waits and errors. On a
  single instance, `INSERT` of one row spends 11% fewer instructions through
  pgproto and 14% fewer through `pico.sql()`, which gives 10% and 22% more
  rows per second.
