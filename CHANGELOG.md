# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Calendar Versioning](https://calver.org/#scheme)
with the `YY.0M.MICRO` scheme.

<img src="https://img.shields.io/badge/calver-YY.0M.MICRO-22bfda.svg">

## Unreleased

### Features

- Transferred replication factor from Properties table to new Tier table. Each tier has his own replication factor.

- New feature `tier` - group of instances with own replication factor.

- New option `--init-cfg` for `picodata run` allows supplying bootstrap configuration in a yaml format file.

- New option `--tier` for `picodata run` allows to specify whether an instance belongs to a tier.

- New option `--password-file` for `picodata connect' allows supplying password in a plain-text file.

- Enable the audit log with `picodata run --audit`.

- Allow connecting interactive console over a unix socket `picodata run --console-sock`.
  Use `picodata connect --unix` to connect. Unlike connecting to a `--listen` address,
  console communication occurs in plain text and always operates under the admin account.

- Restrict the number of login attempts through `picodata connect`. The limit can be set
  through `max_login_attempts` property. The default value is `5`.

- _Clusterwide SQL_ now available via `\set language sql` in interactive console.

- Interactive console is disabled by default. Enable it implicitly with `picodata run -i`.

- Allow specifying `picodata connect [user@][host][:port]` format. It
  overrides the `--user` option.

- Allow creating sharded vinyl tables via `pico.create_table`.

- _Clusterwide SQL_ now uses an internal module called `key_def` to
  determine tuple buckets. In case the tables were sharded using a
  different hash function, executing SQL queries on these tables would
  return inaccurate outcomes. For more examples, refer to
  `pico.help('create_table')`.

- _Clusterwide SQL_ now features Lua documentation. Refer to
  `pico.help('sql')` for more information.

- _Clusterwide SQL_ now enables the creation of sharded tables.
  To learn more, please consult `pico.help('sql')`.

- _Clusterwide SQL_ introduces the capability to delete sharded tables.
  To obtain more details, please consult `pico.help('sql')`.

- _Clusterwide SQL_ now supports simple `on conflict` clause in insert
  to specify behaviour when duplicate error arises. Supported behaviour:
  replace the conflicting tuple (`do replace`), skip the tuple which causes
  error (`do nothing`), return error back to user (`do fail`).

- _Clusterwide SQL_ now supports two execution limits per query:
  max number of rows in virtual table and max number of VDBE opcodes
  for local query execution.

- _Picodata WebUI_ is introduced. It includes cluster status panel and replicaset list.
  Current status of WebUI is still beta.

- _Clusterwide SQL_ introduces the capability to drop users.

- _Clusterwide SQL_ introduces the capability to create and drop roles.

- _Clusterwide SQL_ now allows creation of a global table through
  `create table`.

- _Clusterwide SQL_ introduces the capability to create users.

- _Clusterwide SQL_ introduces the capability to grant and revoke privileges.

- New clusterwide tables now have ids higher than any of the existing ones,
  instead of taking the first available id as it was previously. If there's
  already a table with the highest possible id, then the "wholes" in the id
  ranges start to get filled.

- **BREAKING CHANGE**: pico.trace() function is removed. Now we can trace an
  SQL query via pico.sql().

- **BREAKING CHANGE**: opentelemetry tables __SBROAD_STAT and __SBROAD_QUERY were renamed
  into _sql_stat and _sql_query tables.

### Fixes

- `calculate_bucket_id` now returns values in inclusive range [1, bucket_count] instead of [0, bucket_count - 1] as it was previously.

### Lua API:


- Changes in terminology - all appearances of `space` changed to `table`
- Update `pico.LUA_API_VERSION`: `1.0.0` -> `3.1.0`
- New semantics of `pico.create_table()`. It's idempotent now.
- `pico.create_table()` has new optional parameter: `engine`.
  Note: global spaces can only have memtx engine.
- `pico.whoami()` and `pico.instance_info()` returns new field `tier`
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
