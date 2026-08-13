## fix/backup

- Fixed backup and restore with custom storage directories. SQL `BACKUP` no
  longer crashes when `memtx.dir` or `vinyl.dir` differs from `instance_dir`,
  and `picodata restore` now restores data to the configured `wal_dir`,
  `memtx.dir`, and `vinyl.dir`, including when these directories are nested.
