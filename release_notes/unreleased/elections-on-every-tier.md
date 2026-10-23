## feat/replication

- Every tier now runs Tarantool elections in `election_mode = "manual"`,
  whatever its `replication_mode` is. The governor still designates the
  master in `_pico_replicaset` and promotes it in an election. A master which
  lost the majority of its replicaset is fenced and can no longer accept
  writes.
- The Tarantool system spaces (`_space`, `_index`, `_user`, `_priv`, `_func`
  and the rest) are now synchronous in every tier: a schema or access change
  is confirmed by the synchro quorum of each replicaset.
- Upgrade from 26.2 is supported. Nothing changes until `cluster_version` is
  bumped, that is until every instance runs the new version.
- The real breakages are behaviour changes:
  - An async replicaset of 3 with 2 members down used to be writable after
    failing over to the survivor. Now it stays read-only until one of them
    comes back. A replicaset of 2 still fails over to the remaining instance.
  - One replicaset below its quorum blocks DDL in the whole cluster,
    including other tiers.
  - DDL in async tiers now waits for the synchro quorum to confirm it, so it
    is slower.
  - An old master which comes back with writes the new master doesn't have
    no longer spreads them to the replicaset: the other instances reject them
    and stop replicating from it. Before, they were merged into the new
    master.
