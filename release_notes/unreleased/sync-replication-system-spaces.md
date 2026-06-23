## feat/replication

- In a tier with `replication_mode = sync` the Tarantool system spaces
  (`_space`, `_index`, `_user`, `_priv`, `_func` and the rest) are now
  synchronous too.
