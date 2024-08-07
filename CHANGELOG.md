# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Calendar Versioning](https://calver.org/#scheme)
with the `YY.MINOR.MICRO` scheme.

<img src="https://img.shields.io/badge/calver-YY.MINOR.MICRO-22bfda.svg">

## Unreleased

<!--
- Multiline input is available in `picodata admin` and `picodata connect`.
- Set delimiter for multiline input with `\set delimiter my-shiny-delimiter`.
- New feature `tier` - a group of instances with own replication factor.
  Tiers can span multiple failure domains and a single cluster can have
  multiple tiers. Going forward it will be possible to specify which
  tier a table belongs to.
- New option `picodata run --tier` specifies whether an
  instance belongs to a tier.
- New rpc entrypoint: `.proc_get_config` which returns the effective
  picodata configuration.
- New index for the system table `_pico_replicaset` - `_pico_replicaset_uuid`.
- Fix error "Read access to space '_raft_state' is denied"
  when executing a DML query on global tables
- Changed `weight` column type to DOUBLE  in `_pico_replicaset`.
- Option `picodata run --peer` now defaults to `--advertise` value.
  The previous was `localhost:3301`. This leads to the important behavior change.
  Running `picodata run --listen :3302` without implicit `--peer` specified
  now bootstraps a new cluster. The old behavior was to join `:3301` by default.
-->

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

- System table `_pico_replicaset` now has a different format: the field `master_id`
  is replaced with 2 fields `current_master_id` and `target_master_id`.

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
