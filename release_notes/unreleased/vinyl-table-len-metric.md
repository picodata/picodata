## fix/metrics

- The `pico_table_len` metric now reports an estimate of live tuples for vinyl
  tables, the same as Lua `space:len()` ([tarantool!374]). It used to count
  every statement in the LSM tree, so overwritten rows and deleted rows
  inflated it until compaction. Plugins see the same change through
  `Space::len` in the Rust SDK.
