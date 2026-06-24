## feat/plugin

- Added on_cluster_leader_change callback to plugin API. It is called on two (at max) instances: on the old raft leader and on the new one [!3224].
- Renamed on_leader_change to on_replicaset_leader_change. Both methods are going to be called before the next major release.
