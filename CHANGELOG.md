# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Calendar Versioning](https://calver.org/#scheme)
with the `YY.0M.MICRO` scheme.

<img src="https://img.shields.io/badge/calver-YY.0M.MICRO-22bfda.svg">

## Unreleased

### Features

- New feature `tier` - a group of instances with own replication factor. Tiers can span multiple failure domains and a single cluster can have multiple tiers. Going forward it will be possible to specify which tier a table belongs to.

- New option `--init-cfg` for `picodata run` allows supplying bootstrap
  configuration in a yaml format file.

- New option `--tier` for `picodata run` allows to specify whether an
  instance belongs to a tier.

- Introduce a new _pico_routine system table for the SQL procedures.

- Clusterwide SQL supports procedure creation.

### Compatibility

- System table `_pico_replicaset` now has a different format: the field `master_id`
  is replaced with 2 fields `current_master_id` and `target_master_id`.

- All `.proc_*` stored procedures changed their return values. An extra top level
  array of 1 element is removed.

### CLI

- New command `picodata admin` to connect to picodata instance via unix socket under the admin account.

- SQL by default in `picodata connect`. Lua language is deprecated in `picodata connect`.

- Rename `picodata run --console-sock` option to `--admin-sock` and
  provide a default value `<data_dir>/admin.sock`.

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

- New option `picodata run --shredding` enables secure removing of data files (snap, xlog).

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
