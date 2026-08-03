## feat/sql

- Columns in `PRIMARY KEY` and `CREATE INDEX` now accept an optional `ASC`
  or `DESC` sort order modifier. For example, `PRIMARY KEY (a DESC, b ASC)`,
  `id UNSIGNED PRIMARY KEY DESC`, and `CREATE INDEX ... (a ASC, b DESC)`.
  The default order remains `ASC`. Explicit sort order is supported only
  for `TREE` indexes.
- The virtual `bucket_id` column can be the first part of a primary key with
  its own sort order, for example `PRIMARY KEY (bucket_id DESC, id ASC)`.

----

During a rolling upgrade from the previous release, do not use `DESC` until
all instances have been upgraded. After creating a primary key or index with
`DESC`, downgrading instances to the previous release is not supported.
