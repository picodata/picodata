# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Calendar Versioning](https://calver.org/#scheme)
with the `YY.0M.MICRO` scheme.

<img src="https://img.shields.io/badge/calver-YY.0M.MICRO-22bfda.svg">

## Unreleased

### Features

- _Compare and Swap_ (CaS) is an algorithm that allows to combine Raft
  operations with a predicate checking. Im makes read-write access to
  global spaces serializable.
  <!-- TODO: API docs link. -->

- _Clusterwide schema_ now allows to create global spaces. Their content
  is replicated on every instance in the cluster.
  <!-- TODO: API docs link. -->

- _Raft log compaction_ allows stripping old raft log entries in order
  to prevent its infinite growth.
  <!-- TODO: API docs link. -->

- Remove everything related to "migrations". They're superseeded with
  "clusterwide schema" metioned above.

### CLI

- `picodata run --script` command-line argument sets a path to a Lua
  script executed at startup.

- `picodata run --http-listen` command-line argument sets the [HTTP
  server](https://github.com/tarantool/http) listening address. If no
  value is provided, the server won't be initialized.

- `picodata connect` CLI command provides interactive Lua console.

### Implementation details

- Picodata automatically demotes Raft voters that go offline and
  promotes a replacement. See [docs/topology.md] for more details.
  Replicaset leadership is swiched too.

[docs/topology.md]: https://git.picodata.io/picodata/picodata/picodata/-/blob/310a773f3f/docs/topology.md#динамическое-переключение-голосующих-узлов-в-raft-raft-voter-failover

## Compatibility

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

## Compatibility

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
