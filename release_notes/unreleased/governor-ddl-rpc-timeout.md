## feat/config

- Added `governor_ddl_rpc_timeout` (default 30s). The governor now applies
  global schema changes (`CREATE TABLE`, `CREATE INDEX`, `TRUNCATE`) and backups
  with this dedicated timeout instead of `governor_common_rpc_timeout` (7s). A
  slow apply, such as creating the first vinyl object on a loaded disk, is no
  longer cut short and restarted behind an exponential backoff. The timeout is
  separate from `governor_common_rpc_timeout` so raising it does not affect
  raft-level unreachability detection.

- Raised the default of `governor_common_rpc_timeout` from 3s to 7s to give
  ordinary governor RPCs and raft message delivery more headroom before timing
  out. On upgrade a cluster is moved to 7s only if its stored value is still
  exactly the old 3s default, so any value tuned to something other than 3s is
  kept.
