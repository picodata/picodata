## feat/join

- When joining a cluster without an explicit `replicaset_name`, an instance
  whose name follows the standard naming scheme `<replicaset_name>_<n>`
  (e.g., `default_6_2`) now prefers joining `<replicaset_name>` (`default_6`)
  over whichever replicaset would otherwise be picked automatically. This
  makes it easier to restore a cluster from a backup of just the master
  instances and rejoin replicas with their expected names.
