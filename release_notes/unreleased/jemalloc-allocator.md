## feat/build

- Picodata now uses jemalloc instead of glibc `malloc`. The allocator is
  linked statically and, on Linux, replaces `malloc` for the whole process,
  including Tarantool and plugins. On TPC-B this gives about 10% more
  transactions per second. Build with `--no-default-features` to use the
  system allocator.
