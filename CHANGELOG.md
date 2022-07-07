# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [0.1] - 2022-07-08

### Basic functionality

- Command line interface for deploying a cluster.
- Dynamic topology configuration, scale up the cluster by launching more instances.
- Configure replication factor in the whole cluster.
- Take failure domains into account when composing replicasets.
- Two kinds of storages:
  - based on Raft consensus algorithm (clusterwide),
  - based on Tarantool master-master async replication,
- Shutdown instances gracefully.
- Automatically manage Raft group — provision voters, failover the Raft leader.
- Rebootstrap a dead instance without data loss.
- Automatically discover peers during initial cluster configuration.
