# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Calendar Versioning](https://calver.org/#scheme)
with the `YY.MINOR.MICRO` scheme.

<img src="https://img.shields.io/badge/calver-YY.MINOR.MICRO-22bfda.svg">

## [25.5.5] - Unreleased

### Fixes

- Fixed assertion failure in CAS right after raft leader change followed by
  persisted raft log tail truncation.
- Fixed a crash in proc_runtime_info when the last applied raft entry contained
  a unicode string where a 100th byte position was not on a character boundary.


## [25.5.4] - 2026-01-21

### Fixes

- Introduce unnamed_join alias for motions with joins under them to distinguish columns with identical names
- Fix erroneous logic of counting rows returned from replicasets which led to undercovered limit exceedance
  errors.
- Fixed that upgrading between patch versions wouldn't run upgrade scripts.
- Datetime literals should support `yyyy-mm-dd` format, e.g. `select '2026-01-17'::datetime`.
- Fix type inference for the `a BETWEEN b AND c` expression; now types of `a`, `b` and `c` should be
  properly unified, meanining that `select '2026-01-13' between '2026-01-01'::datetime and '2026-01-20'`
  will work as expected.


## [25.5.3] - 2026-01-15

### Features

- Support JSON_EXTRACT_PATH function.
- New column `sync_incarnation` is added to `_pico_instance` system table.
- New ALTER SYSTEM parameter `governor_check_replication_error` (default: true)
  enables the checking if replication is broken on any instance, in which case
  the instance will be automatically made Offline.
- New columns `target_state_reason` & `target_state_change_time` in `_pico_instance` system table

### Fixes

- Fixed that the whole replicaset would be broken if one instance get's a
  replication conflict. (See also https://git.picodata.io/core/picodata/-/issues/2231).
- Fixed that governor would sometimes be blocked in read_only on a DDL operation
  mode not being able to apply any subsequent raft operations.


## [25.5.2] - 2025-12-26

### Features

- Rework SQL execution protocol for DML queries to reduce data transfer.
- Introduce non-blocking SQL execution to prevent fiber starvation.

### Fixes

- Fixed that governor would hang indefinitely if an Offline replicaset had
  target_master_name != current_master_name.
- Fixed that instance would hang indefinitely when trying to join the cluster if
  the cluster becomes too big.
  NOTE: The fix requires modifying the proc_raft_join RPC response format
  which technically breaks compatibility with previous versions of picodata.
  However picodata explicitly doesn't support heterogeneous joins (when version
  of joining instances mismatches version of cluster), so this shouldn't be a
  problem for anybody. NOTE also that this doesn't affect restarting instances
  which already joined the cluster.
- Fixed a crash when SQL request arrives before instance is properly initialized
- Fixed that instances would be made Offline immediately after a raft entry is
  applied if there weren't any entries applied for a long time before that
- Fixed that instances would randomly fail with ER_READONLY during bootstrap
- Improve upgrade flow for creating Lua stored functions (exported to SQL).
- Fixed that governor would send redundant proc_sharding RPCs which would make
  it impossible to deploy huge clusters. Now RPCs from governor are split into
  batches of configurable size (default 200, ALTER SYSTEM parameter `governor_rpc_batch_size`).
- Node construction is now deferred until actually needed, avoiding unnecessary
  work for cached queries on any instance execution
- Fixed a memory leak in SQL API of plugin SDK

## [25.5.1] - 2025-12-19

### Features

- Change the default memtx_checkpoint_count from 2 to 1. If a user needs a checkpoint, they can use SQL BACKUP statement, no need to occupy extra space by default.
- Reduce the frequency of heartbeats sent to raft learners, reducing the cluster traffic.
- Refactor SQL executor to reduce memory allocations.
- Plugins and webUI now support scram-sha256 auth.
- SQL optimizer may now select indices for comparison operators with mixed number
  types (e.g. pk int < decimal) and datetime.
- Vinyl improvements to decrease in-memory page index size.
- Add HTTPS support for metrics and WebUI
- Add the _pico_bucket(tier_name) function to show vshard bucket distribution.
- Rework SQL execution protocol for DQL queries to reduce data transfer.

### Fixes
- Fix compilation error on ARM architecture.
- Fix eliminate erroneous ER_READONLY message in logs when executing TRUNCATE on
  global tables sometimes (See also https://git.picodata.io/core/picodata/-/issues/2274).
- Fix possible crash when using TLS for iproto communication within cluster.
- Specifying `instance.iproto_tls.enable` parameter via configuration file.
- Prevent conflicts when creating tables with renamed table names.
- Fix CREATE TABLE hanging on index conflict.
- Print upper bound of a signed 64-bit integer for ALTERing the `sql_storage_cache_count_max`,
  instead of an unsigned 64-bit integer, after `UNSIGNED` has been deleted internally and range
  has been changed.
- Resolve hang when using TLS (hang in `ssl_iostream_destroy` when fiber is cancelled).
- Resolve hang when using TLS (fix infinite loop in coio_writev_timeout on SSL_ERROR_ZERO_RETURN).
- Fix Service::on_leader_change callbacks not being called after master goes Offline
  due to sentinel auto-offline policy (See also https://git.picodata.io/core/picodata/-/issues/2303).
- Fixed a bug where the raft snapshot would sometimes contain incorrect term
  which would result in an unrecoverable failure of the receiving instance.
- DML to global tables no longer will fail with errors such as "Compacted"
  (See also https://git.picodata.io/core/picodata/-/issues/2273).
- Fix `use-after-poison` in `proc_sql_execute`.
- Fixed a bug which made the effective value of `pg_portal_max` be the same as `pg_statement_max`
- Fix trigger execution and privilege initialization for scram-sha256 auth in pgproto.
- Fixed possible ER_BOOTSTRAP_READONLY failure during instance bootstrap.
- Fixed a possible bootstrap failure when joining multiple instances at the same
  time. NOTE: The fix requires modifying the proc_raft_join RPC response format
  which technically breaks compatibility with previous versions of picodata.
  However picodata explicitly doesn't support heterogeneous joins (when version
  of joining instances mismatches version of cluster), so this shouldn't be a
  problem for anybody. NOTE also that this doesn't affect restarting already
  instances which already joined the cluster.
- Fix pico_instance_state metric to report correct value on all instances
- Fixed a bug where some queries that changed the table schema checked WRITE permissions instead of ALTER permissions.
- Fix `DO NOTHING` conflict policy error on unique indices for globally distributed tables.
- Lua datetime functions `to_char` and `to_date` are now strict (returns `NULL` when at least one argument is `NULL`).
- Fix name resolution in HAVING if projection contains a param.
- Fixed that vshard would consume non-negligible amount of resources when idling
  on big cluster setups.
- Fix single motion policy handling for left join.

### WebUI
- Introduce display of used memory in terms of tier

### ACL
- Support granting CREATE privilege on specific table.

### SQL
- Support indexing arrays, i.e. `a[1][2]` expressions.
- Support indexing maps with string keys, i.e. `m['key']` expressions.
- Add IF NOT EXISTS support for ALTER TABLE ADD COLUMN.
- Support for a new set of volatile scalar functions, providing service information:
  - `pico_instance_name()`: returns a name of the current instance.
  - `pico_replicaset_name()`: returns a name of the replicaset in which current instance is.
  - `pico_tier_name()`: returns a name of the tier in which current replicaset of current instance is.
  - `pico_instance_dir()`: returns an absolute path to the current instance working directory
    (do not confuse with share directory, which is used for plugins).
  - `pico_config_file_path()`: return an absolute path to the current instance configuration file. It
    does not check whether the config exist, it will still return the initial path. If the instance was
    started without specifying an instance config, it will return `null`.
- Support ALTER INDEX RENAME command to rename an existing index.
- Introduced various optimizations reducing memory allocations and
  eliminating unnecessary plan traversals and modifications.
  Some highlights:
  - Eliminated unnecessary parameter clones.
  - Skipped optimizations for plans that cannot benefit from them.
  - Cached results of some transformations.
  - Eliminated allocations in various places.
  - Avoided sending unused vtables for INSERT queries.

  As a result:
  - INSERT performance increased by 20%-70% depending on the size of the rows.
  - TPC-B performance increased by ~10%.
- Support `EXPLAIN (RAW)` for DML queries.

- Add INDEXED BY clause. It is sqlite feature which allows to specify index for
  table lookup.

### Pgproto
- Add support for configuring pgproto TLS certificates via:
  - `instance.pg.ssl` (enable/disable TLS)
  - `instance.pg.cert_file` (client certificate path)
  - `instance.pg.key_file` (private key path)
  - `instance.pg.ca_file` (CA certificate path).

### Observability
- Added metrics `pico_sql_global_dml_query` and `pico_sql_global_dml_query_retries`
  which report respectively the total number of SQL DML operations on global
  tables and number of times these operations had to be retried due to CAS conflicts.

### Configuration
- Changed default value for `raft_wal_count_max` ALTER SYSTEM parameter from 64 to 16384.

## [25.4.4] - 2025-11-12

### Fixes
- Fixed a bug where the raft snapshot would sometimes contain incorrect term
  which would result in an unrecoverable failure of the receiving instance.
- Resolve hang when using TLS (hang in `ssl_iostream_destroy` when fiber is cancelled).
- Resolve hang when using TLS (fix infinite loop in coio_writev_timeout on SSL_ERROR_ZERO_RETURN).
- DML to global tables no longer will fail with errors such as "Compacted"
  (See also https://git.picodata.io/core/picodata/-/issues/2273).
- Fix trigger execution and privilege initialization for scram-sha256 auth in pgproto.

### SQL
- Add IF NOT EXISTS support for ALTER TABLE ADD COLUMN.

### Observability
- Added metrics `pico_sql_global_dml_query` and `pico_sql_global_dml_query_retries`
  which report respectively the total number of SQL DML operations on global
  tables and number of times these operations had to be retried due to CAS conflicts.

## [25.4.3] - 2025-10-15

### Fixes
- Fix possible crash when using TLS for iproto communication within cluster.
- Specifying `instance.iproto_tls.enable` parameter via configuration file.
- Prevent conflicts when creating tables with renamed table names.
- Fix CREATE TABLE hanging on index conflict.
- Fix Service::on_leader_change callbacks not being called after master goes Offline
  due to sentinel auto-offline policy (See also https://git.picodata.io/core/picodata/-/issues/2303).

### Pgproto
- Add support for configuring pgproto TLS certificates via:
  - `instance.pg.ssl` (enable/disable TLS)
  - `instance.pg.cert_file` (client certificate path)
  - `instance.pg.key_file` (private key path)
  - `instance.pg.ca_file` (CA certificate path).

## [25.4.2] - 2025-10-09

### Fixes
- Fix compilation error on ARM architecture.
- Fix eliminate erroneous ER_READONLY message in logs when executing TRUNCATE on
  global tables sometimes (See also https://git.picodata.io/core/picodata/-/issues/2274).

### WebUI
- Add a login form and JWT-based session management. The JWT secret is stored in
  the `_pico_db_config` table as `jwt_secret`. Set `jwt_secret` to an empty string ("")
  to disable authentication. On upgrade, Picodata leaves the existing `jwt_secret`
  unchanged. To enable web authentication on existing clusters, reset `jwt_secret`
  via the `ALTER SYSTEM` API.

## [25.4.1] - 2025-10-02

### Features

- Support unique indexes with non-sharded columns on sharded tables, with sharding key prefix provided
- Introduce the governor script to add `is_default` column to the `_pico_tier` table
- Pgproto now reports more verbose TLS error messages
- Validate cluster UUID during IPROTO handshake and propagate `cluster_uuid` via IPROTO_ID. Connections with a mismatching `cluster_uuid` are rejected, ensuring cross-cluster isolation.
- Instance's which loose ability to apply raft log updates will automatically
  become Offline. (See also https://git.picodata.io/core/picodata/-/issues/2238).
- Support TLS in IPROTO for intra-cluster communication and CLI.
  - Configure via new config section: `instance.iproto_tls`;
  - Run CLI with new arguments: `--tls-cert`, `--tls-key`, `--tls-ca`.
- Support TLS certificate authentication
  - Use the certificate's Common Name (CN) field as the username;
  - For example, use `CN=dbuser@company.com` or `CN=dbuser` in the certificate to authenticate the user "dbuser";
  - This method takes precedence over other authentication mechanisms.
- New ALTER SYSTEM parameter `plugin_check_migration_hash` (default: true)
  allows disabling plugin migration file checksum validation. This allows plugin
  authors to more easily fix mistakes in migration files.
- It is now possible to use `scram-sha256` auth method for old and new users.
  Currently, the method is only compatible with pgproto, which means it
  won't be available for picodata plugins or iproto connections.

### Observability
- `instance_name` is now the primary label for Prometheus metrics.
- Added the metric `pico_info_uptime{instance_name, instance_dir_name, replicaset, tier, cluster_name}`.
- Updated Grafana dashboard: legends now use `{{instance_name}}`.

### SQL
- SQL supports scalar function `abs()`.
- Multiple `OPTIONS` specified in an SQL query no longer result in an error.
  Instead, the rightmost option takes precedence.
- SQL now supports the `AUDIT POLICY` operation to enable or disable audit logging of DML operations for specific users.
  - Use cases:
    - `AUDIT POLICY dml_default BY dbuser` - enables audit logging of DML operations for user "dbuser"
    - `AUDIT POLICY dml_default EXCEPT dbuser` - disables audit logging of DML operations for user "dbuser"
  - Currently, only one policy named `dml_default` is supported.
- Maximum value for `Integer` and `Unsigned` type is ***9223372036854775807***.

### Fixes
- Fixed "instance is already joined" error if picodata crashes during reboostrap
  (see also https://git.picodata.io/core/picodata/-/issues/2077).
- Supported detecting and fixing broken replication.
- Remove unnecessary timeout on local Unix domain sockets in `picodata admin` command.
  Unlike network sockets, local sockets don't silently hang - a server crash
  immediately breaks the connection, making timeouts redundant. This fixes an issue
  where long-running plugin commands could be incorrectly aborted due to the
  artificial timeout.
- Supported backoff strategies when configuring vshard from the governor.
- Fixed restart the whole cluster at once taking too long.
- Fixed an issue where `cluster.shredding` option does not get applied on instance restart.
- `EXPLAIN` queries now support `OPTION` (e.g. `EXPLAIN SELECT 1 OPTION (SQL_VDBE_OPCODE_MAX = 6)`)
- Fixed joining a replica after expel with the same instance name as the
  expelled one (see also https://git.picodata.io/core/picodata/-/issues/2173).
- Fixed an out-of-memory crash when using Response::encode_rmp with large
  collections in plugin API (see also https://git.picodata.io/core/picodata/-/issues/2028).
- Upgraded luajit to fix several issues.
  See https://git.picodata.io/core/tarantool/-/merge_requests/282 for details.
- Fixed bizarre row order produced by ORDER BY <uuid>.
- Fixed restarting the cluster without some of the voters
  (see also https://git.picodata.io/core/picodata/-/issues/2202).
- Fixed potential undefined behavior in plugin RPC handler arguments.
- Picodata now loads all certificates from `ca.crt`, not just the first one.
- Fixed a possible state corruption in case picodata crashes after receiving a
  raft snapshot with a stale schema version.
- Fixed TRUNCATE operation for global tables.

### CLI

- Authentication method flags now use automatic recognition system, instead
  of relying on default values, delivering less error-prone and complex user
  experience.
  See <https://git.picodata.io/core/picodata/-/issues/1973>.

- `picodata status` and `picodata plugin configure` now support execution with
  custom user, determining authentication method automatically on it's own.
  WARNING: If you do not specify an authentication method, it will be found
  out by brute force, which may lead to the user being blocked if the number of
  authorization attempts exceeds the limits.
  See <https://git.picodata.io/core/picodata/issues/1734>.

- `picodata plugin configure` now prints a message on success for better UX.
  See <https://git.picodata.io/core/picodata/-/issues/1904>.

- `picodata plugin configure` now returns a success code, instead of non-zero
  when trying to change plugin service parameters with the same values.
  See <https://git.picodata.io/core/picodata/-/issues/2222>.

### Plugin API

- `internal::authenticate` is now deprecated, and is a re-export of the
  same auth function in a separate module `authentication::authenticate`.
  See <https://git.picodata.io/core/picodata/-/issues/2007>.

### WebUI
 - display expelled state

## [25.3.3] - 2025-09-10

### CLI

- Authentication method flags now use automatic recognition system, instead
  of relying on default values, delivering less error-prone and complex user
  experience.
  See <https://git.picodata.io/core/picodata/-/issues/1973>.

- `picodata status` and `picodata plugin configure` now support execution with
  custom user, determining authentication method automatically on it's own.
  WARNING: If you do not specify an authentication method, it will be found
  out by brute force, which may lead to the user being blocked if the number of
  authorization attempts exceeds the limits.
  See <https://git.picodata.io/core/picodata/issues/1734>.

- `picodata plugin configure` now prints a message on success for better UX.
  See <https://git.picodata.io/core/picodata/-/issues/1904>.

### Features

- Pgproto now reports more verbose TLS error messages

### Fixes

- Record `pico_sql_query_errors_total`, `pico_sql_query_duration` and `pico_sql_query_errors_total` metrics
  for queries executed through pgproto
- Fixed joining a replica after expel with the same instance name as the
  expelled one (see also https://git.picodata.io/core/picodata/-/issues/2173).

## [25.3.2] - 2025-08-04

### Fixes

- Fixed "instance is already joined" error if picodata crashes during reboostrap
  (see also https://git.picodata.io/core/picodata/-/issues/2077).
- Remove unnecessary timeout on local Unix domain sockets in `picodata admin` command.
  Unlike network sockets, local sockets don't silently hang - a server crash
  immediately breaks the connection, making timeouts redundant. This fixes an issue
  where long-running plugin commands could be incorrectly aborted due to the
  artificial timeout.

## [25.3.1] - 2025-07-25

### Features

- Introduce automatic system catalog schema upgrade on new releases
  (automatically executing DDL/DML and creating internal functions).

### ACL
- The maximum number of users and roles that can be created has been increased from 32 to 128.

### SQL
- SQL supports `ORDER BY ... NULLS FIRST/LAST` operation.
- SQL supports `INSERT INTO ... ON CONFLICT DO FAIL/REPLACE/NOTHING` operation for globally distributed tables.
- SQL supports modulo operator `%` for integers.
- SQL supports window function `last_value()`.
- SQL supports new volatile scalar functions `pico_raft_leader_id()` and `pico_raft_leader_uuid()`.
- SQL scalar function `instance_uuid` is now marked as deprecated, consider using `pico_instance_uuid`.
- SQL supports new stable scalar function `version()`, that returns version of the current instance.
- Remove `Unsigned` type from casts and general type operations:
  - The `Unsigned` type can no longer be used in casts (`CAST(x AS unsigned)`)
  - `Unsigned` no longer appears in `EXPLAIN` output
  - The type remains available only for column definitions in DDL (`CREATE TABLE`, `ALTER TABLE`)

- A new `BACKUP` SQL command is now supported for performing consistent, clusterwide backups.
  A new `--backup-dir` configuration parameter has been introduced (available via environment variable and
  config file). By default, `--backup-dir` is set to `<instance-dir>/backup`. When `BACKUP` is executed,
  each instance in the cluster saves its data into a subdirectory under `backup-dir`, named in the
  format YYYYMMDDThhmmss. The following data is included in the backup:
  * `.snap` files
  * the instance configuration file
  * `.picodata-cookie`
  * plugin data from `share-dir`

  **Note**: Backup cleanup is not automated — database administrators are responsible for
  managing and deleting old backups
- A new `picodata restore` CLI command has been added to restore data from previously created backups.
  Use the `--path` parameter to specify the path to the backup directory. The recommended restore
  procedure is:
  * Stop all instances in the cluster
  * Run `picodata restore --path <backup-path>` on each instance, using the same timestamped
    backup folder (e.g., YYYYMMDDThhmmss)
  * Restart all instances

### Pgproto

- Support LDAP with TLS (StartTLS) for LDAP authentication menthod
  - use TT_LDAP_ENABLE_TLS=true environment variable to turn on

- More metrics:
  - pico_pgproto_connections_closed_total
  - pico_pgproto_connections_opened_total
  - pico_pgproto_portals_closed_total
  - pico_pgproto_portals_opened_total
  - pico_pgproto_statements_closed_total
  - pico_pgproto_statements_opened_total
- Record `pico_sql_query_errors_total`, `pico_sql_query_duration` and `pico_sql_query_errors_total` metrics
  for queries executed through pgproto

### Config
- Config supports `instance.pg.advertise` option for pgproto server advertise address,
  which defaults to `instance.pg.listen`. It also can be set in the CLI via `--pg-advertise` option.

### WebUI

- Replicaset state now always display current leader state
- Display cluster ID and instance PG address
- Cluster info and other data refreshes automatically every 10 seconds if the window is focused
- Offline instances are now marked with a contrasting indicator

### Fixes

- Fixed numerous panics when setting invalid values for alter system parameters
- Fixed the modification of `_pico_service_route`; it is now retriable.

- Fixed a performance issue with global table DML which resulted in an up to 95x performance increase!

- Fixed sorting in WebUI - it's now consistent across reloads
- Fixed WebUI dependency on internet access for displaying fonts
- Fixed WebUI copy icons: now they copy related text on-click

- Fixed a bug which broke intra-replicaset replication when upgrading from 25.1.* to 25.2.*.
  Note that clusters broken by this bug require some manual actions in addition
  to this fix, but newer version should upgrade fine. Note also that this fix
  does not include the actual schema upgrade procedure, so the cluster will
  still have a schema of the older version of picodata. For details see
  https://git.picodata.io/core/picodata/-/issues/1946#note_157146

- Preserve parameter types provided by client.
  Previously, we'd completely replace parameter types array with the one
  derived by the Sbroad's type system. As a result, sometimes the type
  could implicitly change from VARCHAR to TEXT, causing various problems
  to client drivers.

- Fixed a bug when parsing RFC 3339 timestamps with a date-time delimiter other than `T`
  resulted in an error.

- Fixes unnecessary vshard router and storage config updates.

- Fixed a bug which could sometimes lead to unconstrained stream of requests
  from a failed instance (implemented expontential backoff for sentinel requests).

- Fixed a bug where cluster-wide settings for options `sql_vdbe_opcode_max` and `sql_motion_row_max`
  were not applied to queries made through pgproto.

- Fixed a panic in sentinel loop because of integer overflow when multiplying.

- Fixed a bug where picodata would sometimes indefinitely block when applying
  DDL operations which could result in full replicaset failure.

- Fixed a bug where picodata would sometimes indefinitely block after a
  restart and/or network failure when receiving a raft snapshot.

### Lua API

- Remove `pico.exit` function as it is no longer used.

### Plugin API

- Allowed to use different patch level versions of SDK (ex. allow to run plugin built with 25.3.1 to run on Picodata 25.3.2)
- Added `cluster_uuid()` API and corresponding `pico_ffi_cluster_uuid`
  FFI binding to retrieve the cluster UUID of the current instance.

## [25.2.3] - 2025-07-07

### Fixes

- Fixed a bug which broke intra-replicaset replication when upgrading from 25.1.* to 25.2.*.
  Note that clusters broken by this bug require some manual actions in addition
  to this fix, but newer version should upgrade fine. Note also that this fix
  does not include the actual schema upgrade procedure, so the cluster will
  still have a schema of the older version of picodata. For details see
  https://git.picodata.io/core/picodata/-/issues/1946#note_157146

- Fixed incorrect validation check for max number of users.

- Preserve parameter types provided by client.
  Previously, we'd completely replace parameter types array with the one
  derived by the Sbroad's type system. As a result, sometimes the type
  could implicitly change from VARCHAR to TEXT, causing various problems
  to client drivers.

- Fixed a bug which could sometimes lead to unconstrained stream of requests
  from a failed instance (implemented expontential backoff for sentinel requests).

- Fixes unnecessary vshard router and storage config updates.

- Fixed a bug when parsing RFC 3339 timestamps with a date-time delimiter other than `T`
  resulted in an error.

## [25.2.2] - 2025-06-25

### Features

- Increase max number of users to 128
- Support LDAP with TLS (StartTLS)

### Fixes

- Fixed a performance issue with global table DML which resulted in an up to 95x performance increase!
- Used to fail to start when raft election was in progress
- row_number marked as non-deterministic function
- Re-enable basic auth for credentials in URL

### Plugin API

- The `authentication` function has been introduced, which determines the authentication method automatically.

## [25.2.1] - 2025-05-26

### CLI
- `picodata admin` in \lua mode no longer requires a delimiter for executing commands.
- allow only the full format for address command-line arguments
  - allow `HOST:PORT`,
  - disallow `HOST` or `:PORT`,
  - this is technically a breaking change.
- provide a more detailed message for address binding errors
- set permissions of admin socket file to 0660 by default.
- `picodata expel` no longer requires the `--cluster-name` parameter; it is now marked as deprecated and will be removed in the future major release (version 26).

### Pgproto

- Do not allow connections without ssl when ssl is enabled on the server.
  - In previous versions `pg.ssl = true` meant "allow clients to connect either plaintext or via SSL",
  - Now it more strict: "only allow clients to connect via SSL".
  - This is technically a breaking change.

- The WAIT APPLIED GLOBALLY option now waits for all instances rather than just replicaset masters.

### Plugin API

- Plugin RPC requests will now be executed locally whenever possible. Previously
  the behavior was inverted - we preferred remote execution, which was counter
  productive.

- Plugin RPC context has new named field "call_was_local" which is set to `true`
  when the call is made locally (without network access). Note that in the
  opposite case "call_was_local" may be unset, so the absence of this fields
  should be interpreted as a non local call.

### ACL

- For all users with role `public` the privileges to `read` system tables `_pico_instance` and `_pico_peer_address` are now granted.

- Forbid the granting of DROP privileges on system tables.

### Type system

- Improved type checking now catches more errors earlier with clearer error messages.
  Valid queries should note no difference.

  Example with `SELECT 1 = false`:

  Old behavior:
  ```sql
  picodata> select 1 = false;
  ---
  - null
  - 'sbroad: failed to create tarantool: Tarantool(BoxError { code: 171, message: Some("Type
    mismatch: can not convert boolean(FALSE) to number"), error_type: Some("ClientError"),
    errno: None, file: Some("./src/box/sql/mem.c"), line: Some(2784), fields: {}, cause:
    None })'
  ...
  ```

  New behavior:
  ```sql
  picodata> select 1 = false;
  ---
  - null
  - 'sbroad: could not resolve operator overload for =(usigned, bool)'
  ...
  ```

  Some queries with parameters or subqueries that previously worked might
  now require explicit type casts. For instance, `select 1 + (select $1)`
  now leads to "could not resolve operator overload for +(unsigned, unknown)"
  error, that can be fixed with explicit type cast: `select 1 + (select $1)::int`

- Sbroad now infers parameter types from query context, allowing to
  prepare statements in pgproto without explicit type specification,
  which is a quite common case.

  For iproto nothing actually changes, because parameter types are
  inferred from the actual parameter values.

  For example, query `SELECT * FROM t WHERE a = $1` used to fail in
  pgproto with "could not determine datatype for parameter $1" error,
  if parameter type wasn't specified by the client. Now the
  parameter type is inferred from the context to the type of column `a`.

  In addition, parameter types can be inferred from the column types
  when inserting values. For instance, in `INSERT INTO t (int_col) VALUES ($1)`
  or `UPDATE t SET int_col = $1` query, parameter type will be inferred to
  the type of the column (int).

  Note that there are 2 methods to fix inference errors:
   1) Explicitly provide parameter type on protocol level.
   2) Explicitly provide parameter type by using CAST.

  Limitations:

   - `SELECT $1`
   Works in PostgreSQL, fails in sbroad, because there is no context
   for type inference. Such queries require parameter type defaulting rules.

   - `SELECT 1 UNION SELECT $1`
   Parameter type could be inferred from the left select statement,
   like PostgreSQL does, but this is not implemented yet.

- SQL type system supports parameter type defaulting to text, allowing
  to handle parameterized queries with context for parameter types
  inference. For instance, `SELECT $1` used to result in
  "could not infer data type of parameter $1" error.
  Now the parameter type will be defaulted to text, making the query valid.

- SQL now can coerce string literals to more suitable type according to
  the context where they are used. For instance, string literal will be
  automatically coerced to a datetime value when it's being inserted in a
  column of type datetime. If a string doesn't represent a valid value of
  the inferred type, the result will be a parsing error.

  Query examples:
   - `SELECT 1 + '1'` is the same as `SELECT 1 + 1`
   - `INSERT INTO t (datetime_col) VALUES ('2023-07-07T12:34:56Z')` is the
     same as `INSERT INTO t (datetime_col) VALUES ('2023-07-07T12:34:56Z'::datetime)`

### Observability
- Added picodata metrics in prometheus format to the `/metrics` endpoint. Metrics allow to monitor SQL, RPC, CAS, Raft, instance and governor states. List of metrics:
  - pico_governor_changes_total
  - pico_sql_query_total
  - pico_sql_query_errors_total
  - pico_sql_query_duration
  - pico_rpc_request_total
  - pico_rpc_request_errors_total
  - pico_rpc_request_duration
  - pico_cas_records_total
  - pico_cas_errors_total
  - pico_cas_ops_duration
  - pico_instance_state
  - pico_raft_applied_index
  - pico_raft_commit_index
  - pico_raft_term
  - pico_raft_state
  - pico_raft_leader_id

### Fixes
- Display correct value for "can_vote" property in webUI

- Disallow DDL creation operations in a heterogeneous cluster. A cluster is considered
  heterogeneous if any instance differs from another by major or minor version.

- Changed `picodata status` output format to more minimalistic and unix-stylished.

- Fixed panic when dropping system user pico_service.

- PostgreSQL protocol initialization now happens at the stage of instance becoming online,
  preventing possible problems in UX.

- User creation or altering in SQL with a password and LDAP authentication method has been forbidden.

- `.proc_before_online`'s PostgreSQL protocol initialization stage will be
  skipped if it was already initialized before, making this RPC idempotent.

- `--pg-listen` parameter is now checked at bootstrap (`postjoin`) stage, so
  it is not possible anymore to bind a busy port when instance is offline.

- Provide the client with a detailed error message when using LDAP for authentication and the LDAP server is unavailable.

- Fixed secondary indexes not working after raft log compaction.

- Fixed a bug where replication master switchover would sometimes fail.

- No longer dumps backtrace files by default. Logs error if dump failed.

- compare_and_swap in plugins now implicitly calls wait_index and validates term of the applied record
  to exclude situations when election caused different entry to become applied at expected index.
  With new behavior users of the API do not need to take that into account.

- Fixed an issue where an instance cannot join after we expelled another instance.

- `ALTER PLUGIN MIGRATE TO` command's timeout option is now handled more accurately.

- Fixed a bug where a timeout during `ALTER PLUGIN MIGRATE` command would make it
  impossible to run migrations on the current instance.

- Fixed a bug where _pico_service_route was incorrectly loaded into the topology cache.

- Fixed a bug where setting `pg_statement_max` or `pg_portal_max` to `0` could cause queries to
  fail with access error to a system table.

### RPC API

- `.proc_before_online` is a successor to `.proc_enable_all_plugins` due to added
  step of initialization of a PostgreSQL protocol at it's call: see "deprecation".

### Deprecation

- `.proc_enable_all_plugins` will be deprecated in the next major release.
- `cluster_name` parameter from `.proc_expel` RPC call will be deprecated in the next major release.

### SQL
- SQL supports `TRUNCATE` operation.
- SQL supports `ALTER TABLE t ADD COLUMN` operation.
- SQL supports `ALTER TABLE old_table_name RENAME TO new_table_name` operation.
- SQL supports `ALTER TABLE t RENAME COLUMN old_column_name TO new_column_name` operation.
- SQL supports volatile scalar functions: `instance_uuid`.
- SQL supports `CURRENT_TIMESTAMP` scalar function now.
- \[breaking\] `LOCALTIMESTAMP` now returns time with correct timezone, instead of always marking it as UTC.

### Configuration

- Minimal supported Rust version has been bumped from `1.76.0` to `1.85`.

- Allow to configure `boot_timeout` parameter per-instance in config file (7200 sec by default) for auto-shutdown.

## [25.1.2] - 2025-05-20

### Fixes

- vshard is bumped to 0.1.30. This fixes a bug lead to out of lua memory

- remove requirement for a delimiter in console when in lua mode

- `picodata status` output is tuned for better readability

- Fixed a bug where _pico_service_route was incorrectly loaded into the topology cache.

## [25.1.1] - 2025-02-21

### Configuration

- New alter system parameters - `sql_vdbe_opcode_max` and `sql_motion_row_max`.

- Default config name is `picodata.yaml`.

- The default names for replicaset and instance are now generated using the following patterns:
  - ReplicaSet Name: `{tier_name}_{replicaset_number_in_this_tier}`
  - Instance Name: `{tier_name}_{replicaset_number_in_tier}_{instance_number_in_replicaset}`

- New parameters for Vinyl configuration: `bloom_fpr`, `max_tuple_size`, `page_size`, `range_size`,
  `run_count_per_size`, `run_size_ratio`, `read_threads`, `write_threads` and `timeout`.

- New parameter for Memtx configuration: `max_tuple_size`.

- `plugin_dir` parameter is renamed to `share_dir` (--plugin-dir -> --share-dir,
  PICODATA_PLUGIN_DIR -> PICODATA_SHARE_DIR, config: instance.plugin_dir ->
  instance.share_dir)

- The following parameters has been moved from configuration file to `_pico_db_config` system table:
  - checkpoint_interval
  - checkpoint_count
  - max_concurrent_messages

- `max_heartbeat_period` removed from `_pico_db_config`

- CLI parameter `service_password_file` removed from arguments. To set a password for connecting to the cluster, create a file named `.picodata-cookie` in the instance's directory and store the password there.

- Major renaming of parameters from _pico_db_config:
 - `max_concurrent_messages` renamed to `iproto_net_msg_max`
 - `password_min_length` renamed to `auth_password_length_min`
 - `password_enforce_uppercase` renamed to `auth_password_enforce_uppercase`
 - `password_enforce_lowercase` renamed to `auth_password_enforce_lowercase`
 - `password_enforce_digits` renamed to `auth_password_enforce_digits`
 - `password_enforce_specialchars` renamed to `auth_password_enforce_specialchars`
 - `max_login_attempts` renamed to `auth_login_attempt_max`
 - `auto_offline_timeout` renamed to `governor_auto_offline_timeout`
 - `max_pg_statements` renamed to `pg_statement_max`
 - `max_pg_portals` renamed to `pg_portal_max`
 - `snapshot_chunk_max_size` renamed to `raft_snapshot_chunk_size_max`
 - `snapshot_read_view_close_timeout` renamed to `raft_snapshot_read_view_close_timeout`
 - `cluster_wal_max_size` renamed to `raft_wal_size_max`
 - `cluster_wal_max_count` renamed to `raft_wal_count_max`

- `listen`, `advertise` parameters are renamed to `iproto_listen`, `iproto_advertise`

- Added scopes to all parameters from `_pico_db_config`. There are two scopesright now - `tier`
and `global`. Parameters with scope `tier`
can be different on different tiers.
For example `ALTER SYSTEM SET parameter_with_scope_tier FOR ALL TIERS` or
`ALTER SYSTEM SET parameter_with_scope_tier FOR TIER default`.
Parameters with scope `global` are the same on each instance.

- `instance.shredding` moved to `cluster` section and is now defined at bootstrap only.

- Added box `sql_cache_size` to _pico_db_config under `sql_storage_cache_size_max` alias.

- Added `sql_storage_cache_count_max` to _pico_db_config.

### CLI

- `picodata expel` takes instance uuid instead of instance name.

- `picodata expel` now doesn't allow expelling Online instances and replicaset
  masters by default.
  New `--force` flag can be used to forcefully expel an Online instance which
  will shutdown once it finds out it got Expelled.

- String cells are now output without double quotes during SELECT.

- `picodata connect` and `picodata admin` return a non-zero exit code for file inputs with errors.

- `picodata --version` now provides verbose output, including the build type (static or dynamic) and the build configuration (release or debug)

- New command `picodata status` which prints all current members of the cluster and their status.

### Pgproto

- Support LDAP authentication method

### Compatibility

- Added unique index on column `uuid` for `_pico_instance` table. IDs
of `_pico_instance_raft_id` and `_pico_instance_replicaset_name` now equals
to 2 and 3.

- New special command `\set delimiter enter` to change the default delimiter to EOL (End Of Line). Introduced a new inner prompt prefix to indicate when input is waiting for a delimiter. EOF is now treated as a delimiter when reading files.

- New field `_pico_property.system_catalog_version` representing version of a system catalog.
  It may not be changed at every release, so this is not autoincrementing value.

- From now on, when joining a cluster, an instance's version must be the same as the cluster's version or one minor version higher. For example, if the cluster's version is 25.1, only instances with versions 25.1 or 25.2 can join.
  - In the `_pico_property` table, there is a new field called `cluster_version`, which shows the global version of the cluster.
  - In the `_pico_instance` table, there is a new field called `picodata_version` that displays the version of the executable running on the instance.
  - The global `cluster_version` is updated by the governor only when every instance in the cluster has been upgraded to the new minor version.

- System table `_pico_peer_address` has a new column `conncetion_type` that indicates the connection type of the peer. It can be `iproto` or `pgproto`.

- Global rename
  - Config File Changes:
    - `data_dir` renamed to `instance_dir`

  - Source Code Changes:
    - `data_dir` renamed to `instance_dir`

  - Environment Variable Changes:
    - `PICODATA_DATA_DIR` renamed to `PICODATA_INSTANCE_DIR`

- PgProto is now enabled by default and listens at `127.0.0.1:4327`.

- Prevented non-admin users with the DROP TABLE privilege from dropping system tables.

- SQL query parameters renamed:
  - `vdbe_max_steps` to `sql_vdbe_opcode_max`
  - `vtable_max_rows` to `sql_motion_row_max`

- `SCALAR` and `NUMBER` data types are not supported anymore.

### RPC API

- `.proc_expel` and `.proc_expel_redirect` takes instance uuid instead of instance name.

### Fixes

- It's no longer possible to execute DML queries for tables that are not operable
- Fixed panic on user/role creation when max user number was exceeded

- `picodata expel` used to finish before the instance got finally expelled.
  Now it will block until the instance is completely expelled, or the timeout
  is exceeded.

- Fixed a bug, where we would allow to create more than 2 versions of the same
  plugin (and panic when compiled in debug mode).

- `DROP PLUGIN` now leaves the plugin's data in the database if `WITH DATA`
  wasn't specified. Previously we would return an error instead.

### SQL

- SQL support `SUBSTRING` function
- SQL support window functions

## [24.6.1] - 2024-10-28

### Configuration

- New feature `tier` - a group of instances with own replication factor.
  Tiers can span multiple failure domains and a single cluster can have
  multiple tiers. Going forward it will be possible to specify which
  tier a table belongs to.

- Default authentication method changed from `CHAP-SHA1` to `MD5` both for user creation and in connect CLI.
  This change affects new user creation and all system users (except the `pico_service` user), as a command-line interface of `picodata connect` and `picodata expel`. Also, default schema version at cluster boot is now `1`, not `0` as it was previously.
  Connection via `Pgproto` no longer requires additional manual step to change the authentication method. However if you use `iproto` the admin will have to manually change the authentication type.

- Support human numbers to configure memtx.memory, vinyl.memory and vinyl.cache parameters.
  Supported suffixes: K, M, G, T, 1K = 1024
  (e.g picodata run --memtx-memory 10G)

- Add `cluster_wal_max_size` and `cluster_wal_max_count` alter system parameters
  which control the new auto-compaction feature.
  Whenever the total size of tuples in raft log system space exceeds `cluster_wal_max_size`
  or the number of tuples exceeds `cluster_wal_max_count` the log will be automatically compacted.

### Plugins

- New ability to write custom plugins for picodata. Plugins are supposed to be written in Rust
  using our official SDK crate: `picodata-plugin`. Plugins are compiled to shared objects which
  are loaded directly into picodata process. Plugins have in-process access to various picodata API's.
  Plugins do not use special sandboxing mechanisms for maximum performance. Thus require special care
  during coding. Make sure you do not install plugins from untrusted sources.
- Plugins are able to use dedicated RPC subsystem for communication inside the cluster.
- Plugins are able to provide migrations written in SQL to define objects they need for operation.
- Plugins are cluster aware, they're deployed on entire cluster. It is possible to specify particular
  `tier` for plugins to run on.
- Plugins are managed with SQL API, i e `CREATE PLUGIN` and such. For details consult with SQL reference.

### CLI

- New `picodata connect` and `picodata expel` argument `--timeout` for specifying
  the timeout for address resolving operation.

- Replace the use of `localhost` with `127.0.0.1` in `picodata run --listen` default
  value and everywhere across documentation and examples to reduce ambiguity.

### RPC API

- New rpc entrypoint: `.proc_get_vshard_config` which returns the vshard configuration of tier.

- Unused types of IPROTO requests have been forbidden for the sake of integrity.
  Here is the list (all start with "IPROTO_" prefix): INSERT, REPLACE,
  UPDATE, DELETE, CALL_16, UPSERT, NOP, PREPARE, BEGIN, COMMIT, ROLLBACK.
  In future verisons, SELECT will also be forbidden.

- New rpc endpoint: `.proc_replication_sync` which waits until replication on
  the instance progresses until the provided vclock value.

- Parameters `traceable` and `id` have been removed from `.proc_sql_dipstach`.

### Compatibility

- The current version is NOT compatible with prior releases. It cannot
  be started with the old snapshots

- Order of columns in `_pico_service_route` table has changed.

- Global rename
  - Config File Changes:
    - `cluster_id` renamed to `name`
    - `instance_id` renamed to `name`
    - `replicaset_id` renamed to `replicaset_name`

  - Source Code Changes:
    - `cluster_id` renamed to `cluster_name`
    - `instance_id` renamed to `instance_name`
    - `instance.instance_uuid` renamed to `instance.uuid`
    - `replicaset.replicaset_uuid` renamed to `replicaset.uuid`
    - `replicaset.replicaset_name` renamed to `replicaset.name`
    - `replicaset_id` renamed to `replicaset_name`
    - corresponding tables' columns and indexes changed accordingly

  - Environment Variable Changes:
    - `PICODATA_CLUSTER_ID` renamed to `PICODATA_CLUSTER_NAME`
    - `PICODATA_INSTANCE_ID` renamed to `PICODATA_INSTANCE_NAME`
    - `PICODATA_REPLICASET_ID` renamed to `PICODATA_REPLICASET_NAME`

- Default delimiter in `picodata connect` and `picodata admin` cli sessions is now `;`

- Paramters of `.proc_sql_dipstach` RPC have been changed.

### Lua API

- Update `pico.LUA_API_VERSION`: `4.0.0` -> `5.0.0`
- The following functions removed in favor of SQL commands and RPC API
  stored procedures:
* `pico.create_table` -> [CREATE TABLE]
* `pico.drop_table` -> [DROP TABLE]
* `pico.raft_read_index` -> [.proc_read_index]
* `pico.raft_wait_index` -> [.proc_wait_index]

[CREATE TABLE]: https://docs.picodata.io/picodata/devel/reference/sql/create_table/
[DROP TABLE]: https://docs.picodata.io/picodata/devel/reference/sql/drop_table/
[.proc_read_index]: https://docs.picodata.io/picodata/devel/architecture/rpc_api/#proc_read_index
[.proc_wait_index]: https://docs.picodata.io/picodata/devel/architecture/rpc_api/#proc_wait_index

### SQL

- SQL supports `LIKE` operator
- SQL supports `ILIKE` operator
- SQL supports `lower` and `upper` string functions
- Execute option `sql_vdbe_max_steps` was renamed to
`vdbe_max_steps`
- SQL supports `SELECT` statements without scans: `select 1`
- `CREATE TABLE`, `CREATE INDEX`, `CREATE PROCEDURE`, `CREATE USER` and `CREATE ROLE` support `IF NOT EXISTS` option
- `DROP TABLE`, `DROP INDEX`, `DROP PROCEDURE`, `DROP USER` and `DROP ROLE` support `IF EXISTS` option
- `CREATE TABLE`, `CREATE INDEX`, `CREATE PROCEDURE`, `DROP TABLE`, `DROP INDEX` and `DROP PROCEDURE`
  support WAIT APPLIED (GLOBALLY | LOCALLY) options, allowing users to wait for operations to be
  committed across all replicasets or only on the current one
- EXPLAIN estimates query buckets
- SQL supports `COALESCE` function
- `CREATE USER` no need to specify `PASSWORD` for LDAP authentification
- SQL supports `STRING_AGG` as an alias for `GROUP_CONCAT` aggregate function
- SQL support `LOCALTIMESTAMP` function
### Pgproto
- "vdbe_max_steps" and "vtable_max_rows" options are supported in connection
  string. These options allow to override the defalt values of the
  corresponding execution options used in sql (VDBE_MAX_STEPS and VTABLE_MAX_ROWS).

  For example, the following connection string sets both options to 42:
  postgres://postgres:Passw0rd@localhost:5432?options=vtable_max_rows%3D42,vdbe_max_steps%3D42

### Fixes

- Fixed bucket rebalancing for sharded tables
- Fixed panic when applying snapshot with the same index
- Fixed a bug when a non-voting instance would sometimes be chosen as
  a bootstrap leader which would always lead to failure.
- Fixed a bug when pgproto expected users to pass parameters from the
  procedure body in CREATE PROCEDURE queries.
- Fixed a bug when pgproto failed to execute CALL queries with the following error:
  `sbroad: invalid node: node is not Relational type: Block(Procedure(Procedure { name: "proc1", values: [NodeId { offset: 0, arena_type: Arena32 }] }))`.
- Fixed a bug when config inheritance during plugin upgrades
  lead to loss of new config keys. Config inheritance was reworked and now we:
  - Inherit keys that present in both configs
  - Keep new keys untouched
  - Preserve missing keys and plugin-level config

## [24.5.1] - 2024-09-04

### SQL

- SQL now infers sharding key from primary key, when
  the former is not specified in `create table` clause
- SQL normalizes unquoted identifiers to lowercase instead of
  uppercase
- SQL supports `LIMIT` clause
- SQL supports `SUBSTR` function
- SQL supports postgres [cast notation]: `expr::type`
- SQL internally uses new protocol for cacheable requests,
which improves perfomance

### Pgproto

- pgproto supports tab-completion for tables names in psql:
  ```sql
  postgres=> select * from _pico_<TAB>
  _pico_index             _pico_plugin            _pico_privilege         _pico_routine           _pico_table
  _pico_instance          _pico_plugin_config     _pico_property          _pico_service           _pico_tier
  _pico_peer_address      _pico_plugin_migration  _pico_replicaset        _pico_service_route     _pico_user
  ```

- pgproto supports explicit parameter type declarations in SQL via casting.
  This is helpful for drivers that do not specify parameters types, such as
  pq and pgx drivers for Go. In such drivers, users need to explicitly cast all
  query parameters.

  If the driver doesn't specify the type and the parameter isn't cast, the query
  will fail. For instance, running `SELECT * FROM t WHERE id = $1` in pgx will
  return "could not determine data type of parameter $1" error. To resolve this,
  users must specify the expected type of the parameter:
  `SELECT * FROM t WHERE id = $1::INT`.

- Mutual TLS authentication for Pgproto.

    1. Set `instance.pg.ssl` configuration parameter to `true`
    1. Put PEM-encoded `ca.crt` file into instance's data directory along with `server.crt` and `server.key`.

  As a result pgproto server will only accept connection if client has presented a certificate
  which was signed by `ca.crt` or it's derivatives.

  If `ca.crt` is absent in instance's data directory, then client certificates are not requested and not validated.

### Configuration

- Set up password for admin with `PICODATA_ADMIN_PASSWORD` environment variable

- Multiline input is available in `picodata admin` and `picodata connect`

- Set delimiter for multiline input with `\set delimiter my-shiny-delimiter`

- Ability to change cluster properties via SQL `ALTER SYSTEM` command

### Fixes

- Fix error "Read access to space '_raft_state' is denied"
  when executing a DML query on global tables

- Fix error "Maximum number of login attempts exceeded" in picodata admin

### Compatibility

- The current version is NOT compatible with prior releases. It cannot
  be started with the old snapshots

- New index for the system table `_pico_replicaset` - `_pico_replicaset_uuid`

- Changed `weight` column type to DOUBLE in `_pico_replicaset`

- Option `picodata run --peer` now defaults to `--advertise` value.
  The previous was `localhost:3301`. This leads to the important behavior change.
  Running `picodata run --listen :3302` without implicit `--peer` specified
  now bootstraps a new cluster. The old behavior was to join `:3301` by default

- DdlAbort raft log entry now contains the error information.

- Add `promotion_vclock` column to `_pico_replicaset` table.

- Add `current_config_version` column to `_pico_replicaset` table.

- Add `target_config_version` column to `_pico_replicaset` table.

- `Replicated` is no longer a valid instance state.

### RPC API

- Removed stored procedure `.proc_replication_promote`.

- New rpc entrypoint: `.proc_get_config` which returns the effective
  picodata configuration

### Lua API

- Update `pico.LUA_API_VERSION`: `3.1.0` -> `4.0.0`
- The following functions removed in favor of SQL commands and RPC API
  stored procedures:
  * `pico.change_password` -> [ALTER USER]
  * `pico.create_role` -> [CREATE ROLE]
  * `pico.create_user` -> [CREATE USER]
  * `pico.drop_role` -> [DROP ROLE]
  * `pico.drop_user` -> [DROP USER]
  * `pico.grant_privilege` -> [GRANT]
  * `pico.raft_get_index` -> [.proc_get_index]
  * `pico.revoke_privilege` -> [REVOKE]

[ALTER USER]: https://docs.picodata.io/picodata/devel/reference/sql/alter_user/
[CREATE ROLE]: https://docs.picodata.io/picodata/devel/reference/sql/create_role/
[CREATE USER]: https://docs.picodata.io/picodata/devel/reference/sql/create_user/
[DROP ROLE]: https://docs.picodata.io/picodata/devel/reference/sql/drop_role/
[DROP USER]: https://docs.picodata.io/picodata/devel/reference/sql/drop_user/
[GRANT]: https://docs.picodata.io/picodata/devel/reference/sql/grant/
[REVOKE]: https://docs.picodata.io/picodata/devel/reference/sql/revoke/
[.proc_get_index]: https://docs.picodata.io/picodata/devel/architecture/rpc_api/#proc_get_index
[cast notation]: https://docs.picodata.io/picodata/devel/reference/sql/cast/

--------------------------------------------------------------------------------
## [24.4.1] - 2024-06-21

### Pgproto

- Allow connecting to the cluster using PostgreSQL protocol, see
  [Tutorial — Connecting — Pgproto]:

  ```
  picodata run --pg-listen localhost:5432
  psql

  CREATE TABLE ...
  INSERT ...
  SELECT ...
  ```

- The feature is currently in beta. It does NOT automatically imply
  complete compatibility with PostgreSQL's extensive features, SQL
  syntax, etc

[Tutorial — Connecting — Pgproto]:
  https://docs.picodata.io/picodata/24.4/tutorial/connecting/#pgproto

### SQL

- New commands [CREATE INDEX] and [DROP INDEX]
- Support `SELECT ... ORDER BY`
- Support `SELECT ... UNION ... SELECT ... UNION`
- Support common table expressions (CTE)
- Support [CASE][sql_case] expression
- New function [TRIM][sql_trim]
- New functions `TO_CHAR`, `TO_DATE`
- Allow `PRIMARY KEY` next to column declaration
- Support `SET ...` and `SET TRANSACTION ...` but they are ignored
- Support inferring not null constraint on primary key columns
- Support `INSERT`, `UPDATE`, `DELETE` in global tables

[CREATE INDEX]: https://docs.picodata.io/picodata/24.4/reference/sql/create_index/
[DROP INDEX]: https://docs.picodata.io/picodata/24.4/reference/sql/drop_index/
[sql_case]: https://docs.picodata.io/picodata/24.4/reference/sql/case/
[sql_trim]: https://docs.picodata.io/picodata/24.4/reference/sql/trim/

### Configuration

- Provide a new way of configuring instances via config file in a yaml
  format, see [Reference — Configuration file]. It extends the variety
  of previously available methods — environment variables and
  command-line arguments

- New option `picodata run --config` provides a path to the config file

- New option `picodata run -c` overrides single parameter using the same
  naming

- New command `picodata config default` generates contents of the
  config file with default parameter values

- New RPC API `.proc_get_config` returns the effective configuration

[Reference — Configuration file]:
  https://docs.picodata.io/picodata/24.4/reference/config/

### Compatibility

- The current version is NOT compatible with prior releases. It cannot
  be started with the old snapshots

- System table `_pico_table` format changed, the field `distribution`
  now is a map, a new field `description` was added (_string_)

- System table `_pico_tier` format changed, a new field `can_vote` was
  added (_boolean_)

- Rename all indexes adding a prefix containing the table name, e.g.
  `name` -> `_pico_table_name`

- System table `_pico_index` format changed, now `parts` are stored by
  field name instead of an index, other fields were rearranged
  significantly, see [Architecture — System tables]

- Rename RPC APIs related to SQL: dispatch_query -> proc_sql_dispatch;
  execute -> proc_sql_execute

[Architecture — System tables]:
  https://docs.picodata.io/picodata/24.4/architecture/system_tables/

--------------------------------------------------------------------------------
## [24.3.3] - 2024-07-03

- Fix invalid socket path error at startup
- Fix insufficient privileges for sbroad's temp tables breaks legit sql queries
- Finalize static analysis patches

--------------------------------------------------------------------------------
## [24.3.2] - 2024-06-10

- New HTTP endpoint `/metrics` exposes instance metrics in prometheus format

--------------------------------------------------------------------------------
## [24.2.4] - 2024-06-03

- Fix invalid socket path error at startup
- Fix insufficient privileges for sbroad's temp tables breaks legit sql queries
- Fix panic in case of reference used in sql query

--------------------------------------------------------------------------------
## [24.2.3] - 2024-05-28

- Fix `picodata admin` 100\% CPU usage when server closes the socket
- Fix `picodata connect` error after granting a role to the user
- Fix `ALTER USER alice WITH NOLOGIN`

--------------------------------------------------------------------------------
## [24.2.2] - 2024-04-03

- Fix panic after `CREATE USER alice; DROP ROLE alice;`

- Fix SQL chain of joins without sub-queries
  `SELECT * FROM ... JOIN ... JOIN ...`

- Fix SQL grammar support for `table.*`

- Refine audit log [events][audit_events] list: remove
 'new_database_created', add 'create_local_db', 'drop_local_db',
 'connect_local_db', 'recover_local_db', 'integrity_violation'

- Revise [picodata expel][cli_expel] command-line arguments and
  [tutorial][tutorial_expel]

[audit_events]: https://docs.picodata.io/picodata/devel/reference/audit_events/
[cli_expel]: https://docs.picodata.io/picodata/24.2/reference/cli/#expel
[tutorial_expel]: https://docs.picodata.io/picodata/24.2/tutorial/deploy/#expel

--------------------------------------------------------------------------------
## [24.2.1] - 2024-03-20

### SQL

- Introduce stored procedures:

  ```sql
  CREATE PROCEDURE my_proc(int, text)
  LANGUAGE SQL
  AS $$
    INSERT INTO my_table VALUES($1, $2)
  $$;

  CALL my_proc(42, 'the answer');

  SELECT * FROM my_table;
  ```

- The following new queries are supported:

  ```
  CREATE PROCEDURE
  DROP PROCEDURE
  CALL PROCEDURE
  ALTER PROCEDURE ... RENAME TO
  GRANT ... ON PROCEDURE
  REVOKE ... ON PROCEDURE

  ALTER USER ... RENAME TO
  ```

### Security

- All inter-instance communications now occur under `pico_service`
  builtin user. The user is secured with a password in a file provided
  in `picodata run --service-password-file` command-line option

- New requirements on password complexity — enforce uppercase,
  lowercase, digits, special symbols

### Implementation details

- Make RPC API the main communication interface, see [Architecture — RPC
  API]. Lua API is deprecated and will be removed soon

### Compatibility

- System table `_pico_role` was deleted

- System table `_pico_user` format changed, a new field `type` was added
  (_string_, `"user" | "role"`)

- The current version is NOT compatible with prior releases. It cannot
  be started with the old snapshots

[Architecture — RPC API]:
  https://docs.picodata.io/picodata/devel/architecture/rpc_api/

--------------------------------------------------------------------------------
## [24.1.1] - 2024-02-09

- Slightly change calendar versioning semantics, it's `YY.MINOR` now
  instead of `YY.0M`.

### CLI

- New `picodata admin` command connects to an instance via unix socket
  under the admin account, see [Tutorial — Connecting — Admin console].

- New `picodata connect` implementation provides a console interface to
  the distributed SQL, see [Tutorial — Connecting — SQL console].

- New option `picodata run --admin-sock` replaces `--console-sock` which
  is removed. The default value is `<data_dir>/admin.sock`.

- New option `picodata run --shredding` enables secure removing of data
  files (snap, xlog).

- New option `picodata run --log` configures the diagnotic log.

- New option `picodata run --memtx-memory` controls the amount of memory
  allocated for the database engine.

[Tutorial — Connecting — Admin console]:
  https://docs.picodata.io/picodata/24.1/tutorial/connecting/#admin_console

[Tutorial — Connecting — SQL console]:
  https://docs.picodata.io/picodata/24.1/tutorial/connecting/#sql_console

### SQL

- Global tables now can be used in the following queries:

  ```
  SELECT
  SELECT ... EXCEPT
  SELECT ... UNION ALL
  SELECT ... WHERE ... IN (SELECT ...)
  SELECT ... JOIN
  SELECT ... GROUP BY
  ```

- `ALTER USER ... WITH LOGIN` can now unblock a user, who was blocked due to exceeding login attempts.

### Fixes

- Revoke excess privileges from `guest`
- Fix panic after `ALTER USER "alice" WITH NOLOGIN`
- Repair `picodata connect --auth-type=ldap`
- Picodata instances will no longer ingore raft entries which failed to apply.
  Instead now the raft loop will keep retrying the operation forever, so that
  admin has an opportunity to fix the error manually. Raft entries should never
  fail to apply, so if this happens please report a bug to us.

### Compatibility

- System table `_pico_replicaset` now has a different format: the field `master_name`
  is replaced with 2 fields `current_master_name` and `target_master_name`.

- All `.proc_*` stored procedures changed their return values. An extra top level
  array of 1 element is removed.

- The current version is NOT compatible with prior releases. It cannot
  be started with the old snapshots.

--------------------------------------------------------------------------------
## [23.12.1] - 2023-12-21

### Fixes

- Correct `picodata -V`
- Web UI appeared to be broken in 23.12.0
- And `picodata connect --unix` too

--------------------------------------------------------------------------------
## [23.12.0] - 2023-12-08

### Features

- Clusterwide SQL is available via `\set language sql` in the
  interactive console with the support of global and sharded tables,
  see [Reference — SQL Queries]. The basic features are:

  ```
  # DDL
  CREATE TABLE ... USING memtx DISTRIBUTED GLOBALLY
  CREATE TABLE ... USING memtx/vinyl DISTRIBUTED BY
  DROP TABLE

  # ACL
  CREATE USER
  CREATE ROLE
  ALTER USER
  DROP USER
  DROP ROLE
  GRANT ... TO
  REVOKE ... FROM

  # DQL
  SELECT
  SELECT ... JOIN
  SELECT ... LEFT JOIN
  SELECT ... WHERE
  SELECT ... WHERE ... IN
  SELECT ... UNION ALL
  SELECT ... GROUP BY

  # DML
  INSERT
  UPDATE
  DELETE

  # other
  EXPLAIN
  ```

- Implement request authorization based on access control lists (ACL),
  see [Tutorial — Access Control].

- Implement security audit log, see [Tutorial — Audit log].

- Implement automatic failover of replicaset leaders,
  see [Architecture — Topology management].

- Introduce Web UI. It includes cluster status panel and replicaset
  list. Current status of Web UI is still beta.

[Reference — SQL Queries]: https://docs.picodata.io/picodata/reference/sql_queries/
[Tutorial — Access Control]: https://docs.picodata.io/picodata/tutorial/access_control/
[Tutorial — Audit log]: https://docs.picodata.io/picodata/tutorial/audit_log/
[Architecture — Topology management]: https://docs.picodata.io/picodata/architecture/topology_management/

### CLI

- Interactive console is disabled by default. Enable it implicitly with
  `picodata run -i`.

- Allow connecting interactive console over a unix socket `picodata run --console-sock`.
  Use `picodata connect --unix` to connect. Unlike connecting to a `--listen` address,
  console communication occurs in plain text and always operates under the admin account.

- New option `--password-file` for `picodata connect' allows supplying
  password in a plain-text file.

- Allow specifying `picodata connect [user@][host][:port]` format. It
  overrides the `--user` option.

- Enable the audit log with `picodata run --audit`.

- Allow connecting interactive console over a unix socket `picodata run --console-sock`.
  Use `picodata connect --unix` to connect. Unlike connecting to a `--listen` address,
  console communication occurs in plain text and always operates under the admin account.

- Block a user after 4 failed login attempts.

### Lua API

- Changes in terminology - all appearances of `space` changed to `table`
- Update `pico.LUA_API_VERSION`: `1.0.0` -> `3.1.0`
- New semantics of `pico.create_table()`. It's idempotent now.
- `pico.create_table()` has new optional parameter: `engine`.
  Note: global spaces can only have memtx engine.
- `pico.whoami()` and `pico.instance_info()` returns new field `tier`
- Add `pico.sql()`
- Add `pico.drop_table()`
- Add `pico.create_user()`, `pico.drop_user()`
- Add `pico.create_role()`, `pico.drop_role()`
- Add `pico.grant_privilege()`, `pico.revoke_privilege()`
- Add `pico.raft_term()`
- Add `pico.change_password()`
- Add `pico.wait_ddl_finalize()`
- Make `pico.cas` follow access control rules
- `pico.cas` now verifies dml operations before applying them
- Change `pico.raft_log()` arguments
- Make `opts.timeout` optional in most functions

### Compatibility

- The current version is NOT compatible with prior releases. It cannot
  be started with the old snapshots.

--------------------------------------------------------------------------------
## [23.06.0] - 2023-06-16

### Features

- Expose the Public Lua API `pico.*`. It's supplemented with a
  built-in reference manual, see `pico.help()`.

- _Compare and Swap_ (CaS) is an algorithm that allows to combine Raft
  operations with a predicate checking. It makes read-write access to
  global spaces serializable. See `pico.help('cas')`.

- _Clusterwide schema_ now allows to create global and sharded spaces.
  The content of global spaces is replicated to every instance in the
  cluster. In sharded spaces the chuncs of data (buckets) are
  distributed across different replicasets. See
  `pico.help('create_space')`.

- _Raft log compaction_ allows stripping old raft log entries in order
  to prevent its infinite growth. See `pico.help('raft_compact_log')`.

- Remove everything related to "migrations". They're superseeded with
  "clusterwide schema" mentioned above.

### CLI

- `picodata run --script` command-line argument sets a path to a Lua
  script executed at startup.

- `picodata run --http-listen` command-line argument sets the [HTTP
  server](https://github.com/tarantool/http) listening address. If no
  value is provided, the server won't be initialized.

- `picodata connect` CLI command provides interactive Lua console.

### Implementation details

- Picodata automatically demotes Raft voters that go offline and
  promotes a replacement. See docs/topology.md for more details.
  Replicaset leadership is switched too.

### Compatibility

- The current version is NOT compatible with prior releases. It cannot
  be started with the old snapshots.

--------------------------------------------------------------------------------
## [22.11.0] - 2022-11-22

### Features

- Brand new algorithm of cluster management based on the _"governor"_
  concept — a centralized actor that maintains cluster topology and
  performs instances configuration.

- Instance states are now called _"grades"_. This new term more clearly
  denotes how an instance is currently perceived by other instances (eg.
  how they are configured in its regard) rather than what it assumes
  about itself.

- Built-in _sharding_ configuration based on the `vshard` library. Once
  a replicaset is up to the given replication factor, Picodata will
  automatically re-balance data across replicasets.

- Clusterwide schema and data _migrations_ are introduced.

- Instances can now be _expelled_ in order to shrink the cluster.

### Compatibility

- The current version is NOT compatible with `22.07.0`. It cannot be
  started with the old snapshots.

--------------------------------------------------------------------------------
## [22.07.0] - 2022-07-08

### Basic functionality

- Command line interface for cluster deployment.
- Dynamic topology configuration, cluster scaling by launching more instances.
- Support for setting the replication factor for the whole cluster.
- Failure domains-aware replicasets composition.
- Two kinds of storages:
  - based on Raft consensus algorithm (clusterwide),
  - based on Tarantool master-master async replication.
- Graceful instance shutdown.
- Automatic Raft group management (voters provision, Raft leader failover).
- Dead instance rebootstrap without data loss.
- Automatic peers discovery during initial cluster configuration.
