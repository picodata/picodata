## fix/sql

- [picodata#2812] Use direct RPC for query metadata on DQL cache miss
  - Replace vshard-based Lua dispatch with ConnectionPool::call_raw for
    the proc_query_metadata callback. This fixes SQL query execution from
    arbiter tier instances (bucket_count=0) where no vshard router exists.
