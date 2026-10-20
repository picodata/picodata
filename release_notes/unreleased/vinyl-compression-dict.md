## feat/sql

- `CREATE TABLE ... USING vinyl WITH (...)` and `CREATE INDEX ... WITH (...)`
  now accept the `compression_dict` option. When it is `true`, Vinyl
  compresses the index data with a ZSTD dictionary that it trains
  automatically during dump and compaction ([tarantool!334]). This can
  improve the compression of structured data, for example JSON documents
  or log entries. The default is `false`. A memtx table or index rejects
  this option. Example:
  `CREATE TABLE logs (id INT PRIMARY KEY, payload TEXT) USING vinyl WITH (compression_dict = true);`
