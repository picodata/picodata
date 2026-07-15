# Release Notes

User-facing changelog of picodata releases, compiled from
per-MR fragments in `release_notes/unreleased/` at release
time. See `doc/dev/generating-changelog.md` for the release
flow.

## [26.2.1-rc1] - 2026-07-15

### Breaking changes

Following a major release of 26.1 we remove some API's deprecated in previous releaes.

- Deprecated `plugin_dir` setting is removed following a major release. Use `share_dir` instead.

- Deprecated `advertise_address` setting is removed following a major release. Use `iproto` section instead.

- `--cluster-name` argument for `picodata expel` is removed following a major release as it is no longer needed.

- Deprecated `ServiceWorkerManager` is removed from `picodata_plugin` following a major release.

- `authenticate` function from `picodata_plugin` is now accessible only through `authentication` module.
  Reexport in `internal` module is removed following a major release.

- Deprecated `RegionBuffer::get` is removed following a major release.

- `CancellationTokenHandle::cancel()` now returns `Rc<OnceEvent>` instead of
  `Channel<()>`. Use `finish_event.is_finished()` to check completion or
  `finish_event.wait_timeout(duration)` to wait with a timeout.

#### api

- Rename fields in `/api/v1/health/status` response: `reasons` to `issues`,
  status level `unhealthy` to `broken`.

#### cli

- `picodata demo` subcommand is now gated behind the `demo` Cargo feature,
  disabled by default. To build with demo, use `CARGO_FLAGS_EXTRA="--features demo"`.

#### metrics

- Cache metrics
  `pico_router_cache_{hits,misses,statements_added,statements_evicted}_total`
  and `pico_storage_cache_{statements_added,statements_evicted}_total`
  now carry `tier` and `replicaset` labels (previously unlabelled).
  `pico_storage_cache_{hits,misses}_total`,
  `pico_storage_1st_requests_total`, and `pico_storage_2nd_requests_total`
  gain `tier` and `replicaset` in addition to their existing labels.
  Aggregations across replicasets may need an explicit
  `sum without (tier, replicaset)`.

### Features

#### sql

- Added `ARRAY` literal support ([!3180](https://git.picodata.io/core/picodata/-/merge_requests/3180)).
- Expanded pgproto support arrays.

- Support `LOGICAL`, `BUCKETS`, and `FORWARD` modes of EXPLAIN for transactions ([!3184](https://git.picodata.io/core/picodata/-/merge_requests/3184)).
- Add per query bucket estimation to EXPLAIN (RAW) output when BUCKETS mode is
  specified ([!3280](https://git.picodata.io/core/picodata/-/merge_requests/3280)).
- Provide more accurate info about query execution location in EXPLAIN (RAW)
  output ([!3330](https://git.picodata.io/core/picodata/-/merge_requests/3330)).
- Reflect the planning caveats for queries with UNION of global and sharded
  tables in EXPLAIN(RAW) ([!3357](https://git.picodata.io/core/picodata/-/merge_requests/3357)).

- Added support for transactional `INSERT ... ON CONFLICT DO UPDATE` statements inside SQL blocks.

- Added an equality-facts analysis pass over the relational plan that derives
  always-true equalities from `WHERE` / `ON` predicates and stores them per
  output slot. Downstream stages (motion planning, bucket determination, JPPD
  transformation) can now answer "is this slot fixed to a constant?" and "do
  these two slots belong to the same equality class?" in O(1) without
  re-parsing predicates. Semantic boundaries (LEFT JOIN nullable side, CTE,
  Motion, set-ops, `LIMIT` / `ORDER BY` / `GROUP BY` / `HAVING`) are respected
  so unrelated scopes never merge, and parameter placeholders are kept
  separate from constants to avoid leaking unsafe bindings into execution-time
  filters.

- Implement the `forward` option for DQL/DML queries that controls how queries
  are routed with respect to bucket ownership:
  - `on`: scatter-gather across replica set leaders;
  - `ro_to_rw`: all buckets on one node, forwarding allowed;
  - `off`: true locality, error if client is not on the correct node.

- [picodata#1596] Added support for `ARRAY` columns in `CREATE TABLE` and
  `ALTER TABLE ADD COLUMN`. Supported syntax: `T[]`, `T[N]`, `T[N][M]`,
  `T[][]`, `T ARRAY`, `T ARRAY[N]`. Declared type and sizes are documentation
  only and do not affect the internal implementation for now.

- Introduce `CONTEXT` facet in `EXPLAIN` statement that shows query execution
  options and remove them from `LOGICAL` facet output.

- Added new `pico_instance_health_status` SQL scalar function to get current
  instance's health status, - SQL wrapper over `/api/v1/health/status`.

- Extended constant folding: AND/OR identities, identity rules for equality with
  true and inequality with false.

- Provide a way to match vdbe opcode or motion row limit error to specific
  storage query in EXPLAIN (RAW).

- [picodata#2764] Transactional blocks now support LET statements.

- [picodata#2765] Transactional blocks now support IF statements.

- `EXPLAIN (FMT)` option is now properly supported for all modes (facets).
  It is now possible to write `explain (fmt)` to get a formatted logical plan
  or `explain (fmt, raw)` to get a formatted raw query plan.

- [picodata#2728] Error generated when `INDEXED BY` is used with non existing index
  now includes the target table name, making it explicit that the index-table
  relationship lookup failed rather than a general index lookup.

- Add new `BUCKETS` facet to `EXPLAIN` statement. Users can now inspect query
  buckets without producing a full execution plan. This facet can be combined
  with `RAW` and `FMT` options. When multiple facets are specified, output
  sections are separated by headers.

- Introduce `LOGICAL` facet in `EXPLAIN` statement. Users can now explicitly
  request the logical query plan. This facet can be combined with `RAW`,
  `BUCKETS`, and `FMT` options. The default `EXPLAIN` with no facets specified
  now emits both `LOGICAL` and `BUCKETS`.

- Support `DELETE` statementes inside transactional `DO` blocks.

- Support `INSERT` statements inside transactional `DO` blocks.

- `EXPLAIN (RAW)` is now more informative and concise due to the new tree-like
  plan representation. The numbers in square brackets can indicate that a group
  of operations is performed on the same relation; in addition, they can be
  used as references to other operations.

- Temporary table names now use the `_tmp_` prefix instead of `TMP_`, aligning them with naming convention for system tables.

#### config

- Added the `instance.memtx.dir` and `instance.vinyl.dir` configuration parameters, which set the directories where memtx snapshot files and vinyl files are stored respectively.

- Added the `instance.wal_dir` configuration parameter, which sets the directory where WAL files are stored [!3294](https://git.picodata.io/core/picodata/-/merge_requests/3294).

- Added a new parameter `cluster.tier.wal_mode` which controls how the write-ahead log is flushed to disk, letting you trade durability against write performance:
  - `write` (default), the fiber handling a transaction waits for `write(2)` to complete but does not wait for `fsync(2)` before returning control to the user. Data lands in the operating system page cache but is not guaranteed to reach disk, so the most recent records may be lost or corrupted if the power or operating system fails.
  - `fsync`, each `write(2)` is followed by `fsync(2)`, which forces a physical flush to disk and guarantees a consistent database state after a crash. This is the recommended mode when you need reliable recovery from hardware or OS failures. The tradeoff is reduced write performance.

- [picodata#760] New configuration parameter `experimental_sharding_implementation` which
  enables the new behavior on the given tier. The parameter must be specified at
  cluster bootstrap via the configuration file and cannot be changed after that
  (in the future this restriction may be lifted).

##### LDAP is now configurable through yaml

LDAP authentication can now be configured with `picodata.yaml` configuration file. This also adds ability to use alternative (non-system-wide) trusted root CAs file when connecting to LDAP server via TLS.

For now, previous configuration method through environment variables (`TT_LDAP_URL`, `TT_LDAP_DN_FMT`, `TT_LDAP_ENABLE_TLS`) is still supported, but considered obsolete. A warning will be printed on start.

Here's an example YAML configuration snippet for configuring LDAP:

```yaml
instance:
  ldap:
    enabled: true                             # `false` by default. LDAP authentication will fail if set to `false`.
    connect: 127.0.0.1:1337                   # Address of the LDAP server to connect to.
    dn_format: "cn=$USER,dc=example,dc=org"   # Defines conversion of picodata username to an LDAP Distinguished Name (DN).
                                              # Must have exactly one occurrence of `$USER` in it.
    tls:
      enabled: true                           # `false` by default. If `true`, TLS will be used to connect to the server
      method: start_tls                       # `implicit` by default. Accepted values are `implicit` and `start_tls`.
                                              # `implicit` means using `ldaps`. `start_tls` means using LDAP over TLS (StartTLS).
      ca_file: /etc/picodata/ldap-root-ca.crt # Path to a file containing alternative trusted root CA certificates, formatted as PEM.
                                              # System trusted certificate store will be ignored, and those certificates will be used instead.
```

###### Migration from legacy environment variables

You need to convert the

1. The old value of `TT_LDAP_DN_FMT` should be put into `instance.ldap.dn_format`. The format strings are fully compatible.

2. For `TT_LDAP_URL` and `TT_LDAP_ENABLE_TLS`:

| `TT_LDAP_URL`               | `TT_LDAP_ENABLE_TLS` | Action                                                                                                                                                |
|-----------------------------|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ldap://[hostname]:[port]`  | not set              | Set `instance.ldap.connect` to `[hostname]:[port]`.<br/>Leave other options as default values.                                                        |
| `ldap://[hostname]:[port]`  | `true`               | Set `instance.ldap.connect` to `[hostname]:[port]`.<br/>Set `instance.ldap.tls.enabled` to `true`.<br/>Set `instance.ldap.tls.method` to `start_tls`. |
| `ldaps://[hostname]:[port]` | not set              | Set `instance.ldap.connect` to `[hostname]:[port]`.<br/>Set `instance.ldap.tls.enabled` to `true`.<br/>Set `instance.ldap.tls.method` to `implicit`.  |
| `ldaps://[hostname]:[port]` | `true`               | This is an invalid configuration, since it requests use of StartTLS while also using LDAPS (implicit TLS). It is unrepresentable with the new config. |

- [picodata#760] Added validation of cluster.tier config file section against
  the persisted system table state upon instance restart.

- [picodata#2952] Added the `instance.wal_dir` configuration parameter, which
  sets the directory where WAL files are stored.

#### join

- When joining a cluster without an explicit `replicaset_name`, an instance
  whose name follows the standard naming scheme `<replicaset_name>_<n>`
  (e.g., `default_6_2`) now prefers joining `<replicaset_name>` (`default_6`)
  over whichever replicaset would otherwise be picked automatically. This
  makes it easier to restore a cluster from a backup of just the master
  instances and rejoin replicas with their expected names.

#### plugin

- Added on_cluster_leader_change callback to plugin API. It is called on two (at max) instances: on the old raft leader and on the new one [!3224](https://git.picodata.io/core/picodata/-/merge_requests/3224).
- Renamed on_leader_change to on_replicaset_leader_change. Both methods are going to be called before the next major release.

#### replication

- Added synchronous replication for tiers via the new `replication_mode`
  tier option (`async`/`sync`, default `async`), set with
  `cluster.tier.<name>.replication_mode`. In `sync` mode Picodata enables
  Tarantool synchronous replication for sharded tables: a write is confirmed
  only after a quorum of the replicaset (`ReplicationFactor / 2 + 1`)
  acknowledges it, protecting against data loss and replication conflicts
  on master failover at the cost of write latency. Picodata automatically
  configures the synchro quorum, manages txn limbo ownership across
  master switchovers, and sets the `is_sync` flag on spaces created
  in the tier ([!2962](https://git.picodata.io/core/picodata/-/merge_requests/2962)).

  Scope and constraints:
  - Applies only to sharded (user) tables. Global and system tables (`_pico*`)
    are replicated globally via Raft and are unaffected. Tarantool system spaces
    are unaffected too.
  - The mode is fixed at cluster bootstrap and cannot be changed for an
    already-deployed cluster.
  - On quorum loss the replicaset becomes read-only and serves only reads
    (DQL); recovery is manual — the quorum must be restored by hand.
  - Replication factor 2 is not recommended with `sync` mode, since the
    quorum is then also 2 and the replicaset loses write availability on any
    single instance failure.


#### webui

- Optimize `/api/v1/tiers` and `/api/v1/cluster` endpoints to reduce RPC calls.
  HTTP addresses are now read from `_pico_peer_address` storage instead of RPC,
  and memory info is only fetched from replicaset leaders. This reduces the
  number of RPC calls from O(N×RF) to O(N) where N is the number of replicasets.
  Offline instances now show their HTTP address (from storage) instead of empty string.

- Added new instance filters: `isVoter` and `isRaftLeader`. Both support
  the standard `Is` / `IsOneOf` / `IsNotOneOf` expressions with a Yes/No
  value set.

- Extended the generic `Filter` component to accept boolean-valued tag
  options in addition to strings. The filter visual style is unchanged.

- The replicaset card now renders placeholders for missing instances
  (instances expected by the replicaset configuration but not currently
  reported by the cluster).

- Added `isRaftLeader` and `isVoter` flags to the instance model. Tier
  and replicaset cards now show indicators when they contain the raft
  leader or a voter instance.

- The replicaset cards now display indicators if they have the not ready status.

- The filter now has the option to select a text search tag without first entering text.

- Added an instance details modal, opened by clicking an instance in the list. 
  The modal includes the Overview, Storage, and Replication tabs. These tabs 
  display the instance's general information, Vinyl and Memtx storage configuration, 
  and replication details, including upstream and downstream information for remote 
  nodes.

#### misc

- Improved accuracy of `space:len()` for Vinyl tables.

### Bug fixes

#### sql

- Queries of the form `(a OR b) AND (c OR d) AND (e OR f)...` could grow 
  exponentially after DNF conversion. This led to a stack overflow. A limit of 
  512 disjunctions has been added; if this limit is exceeded, the DNF is not 
  constructed. You may notice this in the `EXPLAIN` output:
  `Query (WHOLE STORAGE)`. It is recommended to rewrite such queries
  [picodata!3353].

- Fixed a SQL planner panic caused by stale type metadata after clone-based
  rewrites such as `BETWEEN` normalization and `GROUP BY` alias expansion.

- Fixed a bug preventing scalar functions from being used in GROUP BY.

- Fixed a bug with the storage cache that caused an error "Temporary table TMP_ not found".

- Fixed a permission error occured during query planning for non-admin users.

- Fixed the errors `box.cfg.read_only is true` and `Failed to add a storage reference`,
  which occurred when restarting a storage instance and previously required a retry.

- Fixed `ALTER PLUGIN ADD SERVICE TO TIER` accepting nonexistent tier names.
  Now validates that the specified tier exists and returns an error otherwise.

- Fixed the `query for request_id with plan_id not found` error that occurred
  on queries with `UNION` of `CTE` on a cluster consisting of multiple instances.

- Logical `EXPLAIN` (the default mode) now preserves subquery indentation.

- Backup operation will now be automatically aborted if there are offline instances.
  This prevents cluster being locked in a readonly state.

- Fixed the `NO_ROUTE_TO_BUCKET` error that occurred when sending a request to
  the single node of a replicaset during its restart. This error is now retried
  by the router.

- [picodata#2812] Use direct RPC for query metadata on DQL cache miss
  - Replace vshard-based Lua dispatch with ConnectionPool::call_raw for
    the proc_query_metadata callback. This fixes SQL query execution from
    arbiter tier instances (bucket_count=0) where no vshard router exists.

- [picodata#2842] Fixed the `schema version has changed: need to re-compile SQL statement`
  error, which could occur when you execute multiple DQL queries due to
  yield during cache eviction.

- [picodata#2732] Fixed a panic when attempting to inherit privileges via SQL
  (e.g., `GRANT admin TO somebody`). We do not support privilege inheritance
  via `GRANT user1 TO user2`. The system now validates the grantee type and
  returns a proper `NoSuchRole` error instead of panicking.

- Reused CTE bodies are now materialized once per query instead of being
  silently inlined per reference, fixing wrong results for non-deterministic
  CTE bodies and avoiding redundant recomputation.

- Fixed NOT push-down: no longer short-circuits before recursing into operand
  subtrees. The pass now also descends into Cast children to simplify NOTs nested inside.

- [picodata#2775] Dropping the primary index is now rejected with a clear error.

- `_pico_db_config` can no longer be truncated or deleted, as such operations
  may break instances or clusters.

- Fixed crash when disabling a plugin with slow background jobs. Previously,
  if a job didn't finish within the shutdown timeout, the plugin library could
  be unloaded while the job fiber was still running.

- Fix cold restart deadlock where all instances in a replicaset would get empty
  replication configs, preventing synchronization. The governor now includes
  only the master in the fallback replication config, preserving conflict
  isolation while allowing the cluster to recover.

- Fixed plugin unnamed background jobs being marked with wrong source location
  (missing `#[track_caller]` attribute).

- Revoking privileges from `admin` user caused a panic. Now, revoking priviliges
  from `admin` user is forbidden, for same reasons as for the `pico_service` user.

- Fixed sentinel panic on long activation wait.

- Fixed an out-of-bounds panic in plugin RPC client when selecting a random
  candidate instance for `RequestTarget::Any`.

- [picodata#2838] Fixed panic on single-node cluster forced expel
  ("removed all voters"), which now returns an explicit error.

- [picodata#2888] Fixed a bug where cluster would fail to bootstrap if there were no voter
  instances in the initial `--peer` set.

- Fixed the `Storage cache hitrate` panels in the bundled Grafana dashboard.
  They previously referenced non-existent metric names
  (`pico_storage_cache_1st_requests_total` / `…_2nd_requests_total`) and the
  formula folded DML traffic into the storage hitrate, silently inflating it.
  The panels now compute `hits / (hits + misses)` from
  `pico_storage_cache_hits_total` / `pico_storage_cache_misses_total` (DQL-only
  by construction), aggregated across `rpc_type` so the iproto path
  (`1st`, `2nd`) and the local fast-path (`local`) are reflected together.

- All cache panels in `monitoring/dashboard/Picodata.json` now respect the
  dashboard's `$tier` and `$replicaset` template variables.

- Fixed CAS conflict detection for globally distributed tables with secondary
  unique indexes. Previously, stale CAS requests could be appended to the raft
  log with different primary keys but the same secondary unique key, causing
  replicas to fail applying the later entry and preventing raft from advancing.
  Such requests are now rejected with a retriable `CasConflictFound` error.

- [picodata#2926] Reduced log verbosity in vshard for routine replicaset events.

- Implement and export `box_session_user_name` function in tarantool-sys ([tarantool!387](https://git.picodata.io/core/tarantool/-/merge_requests/387)).
- Implement a function `tarantool::session::user_name` and use it in the `on_access_denied` trigger to prevent an implicit multi-statement transaction, which panicked ([!3196](https://git.picodata.io/core/picodata/-/merge_requests/3196)).

#### config

- Fixed a regression in config parsing. `--iproto-listen`, `--iproto-advertise` and
  `--http-listen` were triggering an error instead of overriding corresponding value
  in yaml config when deprecated listen options were used in the config.

#### discovery

- Discovery no longer panics on multi-address nodes ([picodata!3312]).
