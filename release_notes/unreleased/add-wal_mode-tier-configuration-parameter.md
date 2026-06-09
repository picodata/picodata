## feat/config

- Added a new parameter `cluster.tier.wal_mode` which controls how the write-ahead log is flushed to disk, letting you trade durability against write performance:
  - `write` (default), the fiber handling a transaction waits for `write(2)` to complete but does not wait for `fsync(2)` before returning control to the user. Data lands in the operating system page cache but is not guaranteed to reach disk, so the most recent records may be lost or corrupted if the power or operating system fails.
  - `fsync`, each `write(2)` is followed by `fsync(2)`, which forces a physical flush to disk and guarantees a consistent database state after a crash. This is the recommended mode when you need reliable recovery from hardware or OS failures. The tradeoff is reduced write performance.
