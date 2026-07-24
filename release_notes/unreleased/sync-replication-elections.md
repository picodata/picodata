## feat/replication

- Tiers with `replication_mode = sync` now run Tarantool elections in
  `election_mode = "manual"`, and the election leader is the only instance
  which accepts writes. The governor still designates the master in
  `_pico_replicaset`, but a master which lost the majority is fenced by the
  election itself and can no longer accept writes.
- Fixed a bug where a lagging replica could be chosen for promotion during
  failover. The elections now provide the "synchronize before promotion"
  guarantee: a candidate only receives a vote from a peer whose vclock is
  not ahead of its own.
