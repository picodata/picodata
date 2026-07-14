## fix

- Fixed the `Storage cache hitrate` panels in the bundled Grafana dashboard.
  They previously referenced non-existent metric names
  (`pico_storage_cache_1st_requests_total` / `…_2nd_requests_total`) and the
  formula folded DML traffic into the storage hitrate, silently inflating it.
  The panels now compute `hits / (hits + misses)` from
  `pico_storage_cache_hits_total` / `pico_storage_cache_misses_total` (DQL-only
  by construction), aggregated across `rpc_type` so the iproto path
  (`1st`, `2nd`) and the local fast-path (`local`) are reflected together.
