## fix

- Fix cold restart deadlock where all instances in a replicaset would get empty
  replication configs, preventing synchronization. The governor now includes
  only the master in the fallback replication config, preserving conflict
  isolation while allowing the cluster to recover.
