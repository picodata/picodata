## fix/sql

- Backup operation will now be automatically aborted if there are offline instances.
  This prevents cluster being locked in a readonly state.
