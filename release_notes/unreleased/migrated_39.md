## fix/sql

- Fixed `ALTER PLUGIN ADD SERVICE TO TIER` accepting nonexistent tier names.
  Now validates that the specified tier exists and returns an error otherwise.
