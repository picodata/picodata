## feat/sql

- Added a native `JSON` data type ([!3428]). A `JSON` column stores a parsed
  document rather than its text, so values are validated on the way in,
  normalized to a canonical form (object keys sorted, duplicate keys dropped,
  integer encodings unified) and compared as documents rather than as strings.
- A `JSON` value is produced by casting text to it, either as `'{"a": 1}'::json`
  or as `CAST('{"a": 1}' AS JSON)`. Invalid text is rejected at planning time.
- `CAST(json AS TEXT)` serializes the whole document, matching PostgreSQL's
  `jsonb::text`. JSON `null` stays a JSON value and is distinct from SQL `NULL`.
- Subscripting reads into a document: `data['key']` for objects and
  `data[0]` for arrays, both zero-based like PostgreSQL's `jsonb`.
  `json_extract_path` accepts the new type as well.
- `DISTINCT`, `GROUP BY`, `count()` and equality work on `JSON` columns.
- Over the PostgreSQL protocol a `JSON` column is advertised as `jsonb`, and
  `jsonb` bound parameters are accepted.
- `JSON` columns cannot take part in a primary key, a sharding key or an index.
