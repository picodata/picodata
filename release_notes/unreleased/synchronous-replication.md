## feat/replication

- Added synchronous replication for tiers via the new `replication_mode`
  tier option (`async`/`sync`, default `async`), set with
  `cluster.tier.<name>.replication_mode`. In `sync` mode Picodata enables
  Tarantool synchronous replication for sharded tables: a write is confirmed
  only after a quorum of the replicaset (`ReplicationFactor / 2 + 1`)
  acknowledges it, protecting against data loss and replication conflicts
  on master failover at the cost of write latency. Picodata automatically
  configures the synchro quorum, manages txn limbo ownership across
  master switchovers, and sets the `is_sync` flag on spaces created
  in the tier ([!2962]).

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
