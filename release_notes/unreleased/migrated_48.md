## fix/sql

- [picodata#2842] Fixed the `schema version has changed: need to re-compile SQL statement`
  error, which could occur when you execute multiple DQL queries due to
  yield during cache eviction.
