# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Calendar Versioning](https://calver.org/#scheme)
with the `YY.0M.MICRO` scheme.

<img src="https://img.shields.io/badge/calver-YY.0M.MICRO-22bfda.svg">

## [22.11.0] - 2022-11-22

### Features

- Brand new algorithm of cluster management based on _"governor"_
  concept — a centralized actor that operates topology and performs
  instances configuration.

- Instances state is denoted by the word _"grade"_ (current and target).
  The current grade of an instance is not what it tells about itself,
  but how other instances are configured with respect to the current
  one.

- Automatically configure _sharding_ (based on `vshard`) and perform
  rebalancing when replication factor is satisfied.

- Apply schema and data _migrations_ clusterwide.

- Shrink cluster by _expelling_ instances.

### Compatibility

- Current version is NOT compatible with `22.07.0`. It cannot be started
  with the old snapshots.

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
