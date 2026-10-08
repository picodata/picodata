## perf/sql

- The router now caches the plans of transactional blocks (`DO $$ ... END $$`)
  the same way it caches DQL and DML plans. A block sent through iproto, the
  plugin SQL API or a pgproto query without a named prepared statement is no
  longer parsed and optimized on every execution. Such blocks now show up in the
  `pico_router_cache_*` metrics and share the router plan cache with DQL and DML.
