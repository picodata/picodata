## fix/sql

- Fixed comparisons of `pico_raft_leader_uuid()` with `pico_instance_uuid()`
  and `_pico_instance.uuid` failing with an operator overload error. The function
  was registered as `UUID` despite returning a string. Its inferred result type
  is now `TEXT`, matching the returned value.
