## breaking/metrics

- Cache metrics
  `pico_router_cache_{hits,misses,statements_added,statements_evicted}_total`
  and `pico_storage_cache_{statements_added,statements_evicted}_total`
  now carry `tier` and `replicaset` labels (previously unlabelled).
  `pico_storage_cache_{hits,misses}_total`,
  `pico_storage_1st_requests_total`, and `pico_storage_2nd_requests_total`
  gain `tier` and `replicaset` in addition to their existing labels.
  Aggregations across replicasets may need an explicit
  `sum without (tier, replicaset)`.
