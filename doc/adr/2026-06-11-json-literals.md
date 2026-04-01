status: proposed

decision-makers: @kostja, @funbringer, @d.rodionov

--------------------------------

# JSON Literals Support in Picodata SQL

## Problem Statement

Picodata has partial JSON support today: tables can be created with `json`
columns, data can be inserted via Lua, and `JSON_EXTRACT_PATH()` reads values.
But SQL-based JSON literals do not work. The cast `'{"a":1}'::json` is not
wired up, and the current storage mapping (`DomainType::Json` to
`FieldType::Map`) rejects everything that is not an object, so arrays, scalars,
and JSON `null` cannot be stored.

This ADR enables JSON literals in Picodata SQL so that inserting JSON into a
`json` column works:

```sql
INSERT INTO t VALUES (1, '{"a":1}'::json);          -- object, shorthand cast
INSERT INTO t VALUES (2, CAST('[1,2,3]' AS json));  -- array, standard cast
INSERT INTO t VALUES (3, 'true'::json);             -- boolean scalar
INSERT INTO t VALUES (4, '42'::json);               -- integer scalar
INSERT INTO t VALUES (5, '"hello"'::json);          -- string scalar
INSERT INTO t VALUES (6, 'null'::json);             -- JSON null, distinct from SQL NULL
```

Both cast forms are supported and equivalent: the PostgreSQL shorthand
`'...'::json` and the SQL standard `CAST('...' AS json)`.

## Decision

Build on Tarantool's native JSON type rather than reimplementing JSON semantics
on top of `FieldType::Any`. Picodata stays thin:

1. The router parses the JSON literal into MessagePack and frames it as the
   Tarantool JSON extension (`MP_JSON`).
2. The value travels to storage as a bound MessagePack parameter, rendered as
   `CAST($n AS json)`, exactly like `decimal`, `uuid`, and `datetime` values do
   today.
3. Tarantool owns normalization, the SQL `NULL` versus JSON `null` distinction,
   ordering, and comparison.

This deletes most of the machinery an `Any`-based design would require (a
custom null marker, key sorting, comparison and ordering code) and avoids
shipping a JSON implementation that would have to be migrated again once the
native type lands.

### Considered and rejected

- **Store JSON in `FieldType::Any` and implement everything in Picodata.**
  Requires a homegrown null marker, router-side normalization, and comparison
  and ordering code. It diverges from the native representation (different
  extension subtype, and an ordering that the original draft got wrong), and a
  second migration would be needed once the native type is adopted.
- **Tarantool-style native literals** (`{'a': 1}`, `[1, 2, 3]`). Single-quoted
  keys are not PostgreSQL compatible, the grammar is ambiguous with array
  indexing and other brace uses, and the only capability cast syntax does not
  already cover is expression interpolation, which requires per-row JSON
  construction on storage. See Future Work.

## Dependency: Tarantool Native JSON Type

This design depends on the Tarantool native JSON type (see References). What it
provides:

| Capability | Behavior |
|------------|----------|
| Field type | A first-class `json` field type (`FIELD_TYPE_JSON`) |
| Wire and storage format | A MessagePack extension, subtype `MP_JSON = 7`; the type tag travels with the bytes through tuples, indexes, replication, and Lua, the same way `DECIMAL`, `UUID`, `DATETIME`, and `INTERVAL` do |
| Null distinction | SQL `NULL` and JSON `null` are kept distinct |
| Normalization | On insert through Tarantool SQL: keys sorted, duplicate keys resolved last-wins |
| Ordering | JSONB-compatible: `null < string < number < bool < array < object` |
| Indexing | JSON values are not indexable: a JSON column cannot be a primary key, carry a unique constraint, or back an index |

The vendored `tarantool-sys` does not yet expose this type, and the Rust
bindings' `FieldType` enum stops at `Map`. Landing this ADR's Phase 1 requires
the native type to be merged, vendored into `tarantool-sys`, and a
`FieldType::Json` variant added to the bindings (see Verification Items).

## Architecture

### Where parsing happens

Tarantool's `CAST(text AS json)` is text-wrapping and deliberately limited: it
would turn `'{"a":1}'` into a JSON string scalar, not an object. So Picodata
cannot delegate document parsing to storage by sending a string. Instead the
router parses the literal:

```
'{"a":1}'::json
      |
      v  (router, at planning time)
serde_json parse -> rmpv content -> Value::Json
      |
      v  (bound MessagePack parameter, framed as MP_JSON ext)
storage SQL: ... CAST($1 AS json) ...
      |
      v  (Tarantool SQL INSERT)
normalize, validate, store as native json
```

Normalization (key sorting, duplicate resolution) is performed by Tarantool
when the INSERT executes through its SQL layer. The router does not normalize.

### Why a bound parameter and `CAST($n AS json)`

Picodata already renders every typed constant as `CAST($n AS <type>)` and binds
the value as a MessagePack parameter (`sql-planner/src/backend/sql/ir.rs`). The
`CAST` is a type annotation for Tarantool's SQL typer, not a data conversion.
The value itself carries its type tag in MessagePack.

The closest precedents are the other MessagePack extension types. A
`'...'::decimal` constant already travels as a decimal-ext parameter rendered as
`CAST($1 AS decimal)`, and Tarantool treats a cast of an already-ext-typed
value to its own type as identity. JSON (`MP_JSON`) has the same shape, so
`CAST($1 AS json)` over an `MP_JSON` parameter follows the existing path. The
text-wrapping limitation above applies only to text input, which is a different
parameter type than an `MP_JSON` value.

## Type System Changes

JSON must be a distinct type in all four type lattices. Today it is conflated
with `Map`, and there is a latent bug.

`sql-planner/src/ir/types.rs`:

- `DomainType::Json` maps to `FieldType::Json` and `SpaceFieldType::Json`
  (currently `Map`), and to `UnrestrictedType::Json` (currently `Map`).
- Add `UnrestrictedType::Json`, displayed as `"json"`, mapping to
  `FieldType::Json`. Keep `UnrestrictedType::Map`: system spaces such as
  `_pico_table.distribution` are `Field::map` and rehydrate as
  `UnrestrictedType::Map`. Add `"json"` to `UnrestrictedType::new`.
- `CastType::Json` displays as `"json"` (currently `"map"`) and maps to
  `UnrestrictedType::Json` and to the new `Type::Json` (currently `Map`).
- Fix `TryFrom<Rule> for UnrestrictedType`: `Rule::TypeJSON` currently maps to
  `Double`. It must map to `Json`.
- Add the `Rule::TypeJSON` arm to `TryFrom<&Rule> for CastType` (missing today,
  which is why `'...'::json` does not parse).

`sql-type-system/src/expr.rs`:

- Add `Type::Json`. Wire `CastType::Json` to it. This keeps JSON distinct from
  `Map` for overload resolution and yields clear type errors for operations
  JSON does not support.

`sql-protocol/src/dql_encoder.rs`:

- Add `ColumnType::Json`, appended at the end of the enum to keep wire
  discriminants stable. Add the `Json` arm to `From<ColumnType> for
  DerivedType` (`sql-planner/src/ir/types.rs`).

The `CAST($n AS json)` rendering is driven by `UnrestrictedType`'s `Display`
through `DerivedType`, so once `UnrestrictedType::Json` displays as `"json"`,
the storage SQL is correct.

## Value Representation and JSON null

Add a `Value::Json` variant holding the decoded JSON content. The `MP_JSON`
framing (extension subtype 7) is applied only at the storage-encode boundary
and stripped on decode, which keeps the runtime value usable for output and any
future comparison.

```rust
// sql-planner/src/ir/value.rs
pub enum Value {
    // ... existing variants
    Json(Box<rmpv::Value>), // decoded content; boxed to keep Value at 32 bytes
}
```

The SQL `NULL` versus JSON `null` distinction falls out of two facts, with no
homegrown marker:

- `Value::Json` is a distinct variant from `Value::Null`, so a JSON value is
  never SQL `NULL`.
- At the encode boundary, JSON content (including a content `Nil`) is framed as
  `MP_JSON`, whereas SQL `NULL` encodes as a bare MessagePack nil.

| Literal | Router value | Encoded form | `IS NULL` |
|---------|--------------|--------------|-----------|
| `'null'::json` | `Value::Json(Nil)` | `MP_JSON` null | false |
| `NULL::json` / `NULL` | `Value::Null` | bare nil | true |

`IS NULL` is not evaluated on the router for table queries; the predicate is
pushed to storage and Tarantool's native type enforces the distinction. The
cast of SQL `NULL` to `json` stays `Value::Null`, so `NULL::json IS NULL` is
true.

A new `Value` variant forces arms in the exhaustive matches (`eq`,
`partial_cmp`, `as_key_def_part`, `Encode`, `Display`, `cast`, serialization,
Lua push). For Phase 1, where JSON is an opaque payload:

- `eq(Json, non-Json)` is `False`; `eq(Json, Null)` is `Unknown`.
- `partial_cmp` involving `Json` is `None`. Ordering is deferred (see Future
  Work).
- `eq(Json, Json)` is not relied upon: equality against a literal is pushed to
  storage, and the router does not normalize, so it cannot byte-compare two
  JSON values reliably.
- `as_key_def_part(Json)` is unreachable because JSON is never a key; provide a
  defensive arm and rely on planner-level rejection.

### Encode and decode

```rust
// Encoding: Value::Json content -> MP_JSON ext bytes.
const MP_JSON: i8 = 7;

Value::Json(content) => {
    // Encode the content, then frame it as the JSON extension.
    let mut payload = Vec::new();
    rmpv::encode::write_value(&mut payload, content)?;
    write_ext(w, MP_JSON, &payload)?;
}

// Decoding uses the column type context: a json column yields Value::Json
// holding the unframed content.
```

The exact `MP_JSON` payload layout must match Tarantool's own encoder
byte-for-byte (see Verification Items).

## Intermediate Results (motions and `populate_table`)

Distributed queries materialize intermediate results into temporary spaces and
fill them with `populate_table` (`src/sql/execute.rs`). JSON flows through this
path as an opaque payload:

- Add `ColumnType::Json` mapping to `Field::json` in `table_create`.
- Temporary spaces get a synthetic appended `unsigned` primary key, and the
  only index is built on that column. JSON data columns are never indexed, so
  the non-indexability rule does not bite here.
- The `MP_JSON` tag travels with the bytes through materialize, `populate_table`,
  and the local INSERT, so JSON typing survives the round trip untouched.

## Read and Output Paths

- **pgproto.** `src/pgproto/value.rs` already has `struct Json(rmpv::Value)`
  with `serde_json` text rendering, `FromSql`/`ToSql`, the `PgValue::Json`
  variant, and JSON OID handling on input and output. The one missing piece is
  the input conversion into sbroad, which today returns "cannot represent json
  in sbroad". Replace it with `PgValue::Json(j) => Value::Json(Box::new(j.0))`.
  Because `rmpv::Value::Map` preserves order, the text output reflects the
  stored (Tarantool-sorted) key order, so normalization is observable here.
- **iproto.** Results are streamed as raw storage MessagePack, so an `MP_JSON`
  value reaches the client as extension subtype 7, the same way `decimal`,
  `uuid`, and `datetime` are returned. The value is passed through, not
  unwrapped, which preserves the null distinction on the wire and matches
  existing extension handling. The client connector decodes subtype 7.
- **`CAST(json AS text)`.** Tarantool's container-to-text rendering is
  deliberately limited, so this SQL-level cast is not cleanly pushable to
  storage. It is deferred (see Future Work). Normalization is verified through
  pgproto text output instead.

## JSON_EXTRACT_PATH

`JSON_EXTRACT_PATH` is a Lua stored function created during catalog upgrade
(`src/governor/upgrade_operations.rs`), type-checked in
`sql-type-system/src/type_system.rs` with first argument `Type::Map`.

- Update the overload signature to accept `Type::Json` as the first argument so
  resolution succeeds for JSON columns.
- The Lua body must accept `MP_JSON` input. The native type is described as
  self-describing across Lua, so the function may receive a decoded table
  unchanged; this is a Verification Item.

## DDL Constraints

JSON is not indexable. The sharding key is already protected: non-scalar
columns are rejected (`sql-planner/src/frontend/sql.rs`), and
`DomainType::Json.is_scalar()` is false. Add an explicit router-side check that
rejects JSON columns in `PRIMARY KEY`, `UNIQUE`, and `CREATE INDEX` with a
clear "JSON columns are not indexable" message, so the failure is reported at
planning rather than at DDL apply time.

## Migration of Legacy json Columns

`_pico_table.format` persists each column's field type string, and that string
is authoritative when the space is built. Existing `json` columns were
persisted as `field_type = "map"` and stay that way, so they are not broken by
the mapping change: on load they rehydrate as `UnrestrictedType::Map` and keep
behaving as object-only, exactly as today. The new mapping affects only new
`CREATE TABLE`. Because Picodata has no user-facing `map` type, a `map`-typed
user column is by definition a legacy `json` column.

No automatic data migration is performed. Converting a legacy column in place
would mean rewriting every stored value to `MP_JSON` and altering the field
type, which is costly and risky. Instead:

- During catalog upgrade, mark affected tables by annotating `description` in
  `_pico_table`:

  ```
  [JSON MIGRATION REQUIRED] This table has JSON columns using legacy 'map'
  storage which cannot store arrays or scalar values. Recreate the table to
  enable full JSON support.
  ```

- Document a detection query and the manual recreate steps in release notes,
  and emit a one-time upgrade log warning:

  ```sql
  SELECT name FROM _pico_table WHERE description LIKE '%JSON MIGRATION REQUIRED%';
  ```

  ```sql
  CREATE TABLE t_new (id INT PRIMARY KEY, data JSON);
  INSERT INTO t_new SELECT * FROM t_old;     -- existing objects are compatible
  DROP TABLE t_old;
  ALTER TABLE t_new RENAME TO t;
  ```

A JSON literal INSERT into a legacy `map` column fails with the natural cast
mismatch error; the message points at the migration guidance.

## Scope

In scope (Phase 1): JSON literal INSERT into a `json` column via cast syntax,
the supporting type system and value representation, pgproto parameters and
output, `JSON_EXTRACT_PATH` compatibility, DDL constraints, and legacy table
marking.

Rejected in Phase 1 with a clear error, because they would require router-side
hashing, comparison, or ordering of JSON values: `GROUP BY`, `DISTINCT`,
`ORDER BY`, and `UNION` over a JSON column, and JSON as a join key. `WHERE data
= '...'::json` against a sharded table is pushed to storage and handled by
Tarantool, so it needs no router-side comparison.

Deferred (see Future Work): the SQL `CAST(json AS text)` cast, and the query
operations listed above.

## Implementation Tasks

| Task | Description | Dependency |
|------|-------------|------------|
| 1 | Type system: add `UnrestrictedType::Json`, `Type::Json`, `ColumnType::Json`; rewire `DomainType::Json` and `CastType::Json` to `FieldType::Json` and `"json"`; fix the `Rule::TypeJSON` mapping bug; add the `CastType` grammar arm | Native type bindings |
| 2 | Value: add `Value::Json(Box<rmpv::Value>)` and fill the exhaustive match arms | Task 1 |
| 3 | Cast: implement `TEXT -> JSON` in `Value::cast` (serde_json to rmpv content); keep `NULL -> JSON` as `Value::Null` | Task 2 |
| 4 | Encode and decode: frame content as `MP_JSON` on encode, unframe on decode using column type context | Task 2 |
| 5 | Storage rendering: `CAST($n AS json)` for JSON parameters | Task 1 |
| 6 | Intermediate results: `ColumnType::Json` to `Field::json` in `table_create` | Task 1 |
| 7 | pgproto: convert `PgValue::Json` to `Value::Json` | Task 2 |
| 8 | `JSON_EXTRACT_PATH`: accept `Type::Json`; adapt the Lua body for `MP_JSON` | Task 1 |
| 9 | DDL: reject JSON in primary key, unique, and index | Task 1 |
| 10 | Migration: mark legacy `map` columns; document detection and recreate; log warning | None |
| 11 | Integration tests | All above |

## Acceptance Criteria

1. `INSERT INTO t VALUES (1, '{"a":1}'::json)` works (object, shorthand cast).
2. `INSERT INTO t VALUES (2, CAST('[1,2,3]' AS json))` works (array, standard cast).
3. `INSERT INTO t VALUES (3, '42'::json)` works (integer scalar).
4. `INSERT INTO t VALUES (4, '"hello"'::json)` works (string scalar).
5. `INSERT INTO t VALUES (5, 'true'::json)` works (boolean scalar).
6. `INSERT INTO t VALUES (6, 'null'::json)` works (JSON null).
7. `'null'::json IS NULL` returns false.
8. `NULL::json IS NULL` returns true.
9. The two cast forms are equivalent.
10. Object keys are normalized (sorted) on insert, observable through pgproto text output.
11. Duplicate keys are resolved last-wins.
12. `JSON_EXTRACT_PATH()` works on a `json` column.
13. Prepared statements with JSON parameters work via pgproto.
14. JSON in primary key, unique, index, or sharding key is rejected at planning.
15. `GROUP BY`, `DISTINCT`, `ORDER BY`, `UNION`, and join keys over JSON are rejected with a clear error.
16. Legacy `map`-backed `json` columns keep working as object-only and are marked for manual migration.

## Verification Items

These must be confirmed against the native JSON type before or during
implementation:

1. The native type is merged, vendored into `tarantool-sys`, and a
   `FieldType::Json` variant is added to the Rust bindings. Neither exists in
   the vendored tree today.
2. Tarantool normalizes a bound `MP_JSON` parameter on SQL INSERT, not only
   JSON it parses from literal text.
3. `CAST(<MP_JSON> AS json)` is identity, not text-wrapping or an error, as
   `CAST(<decimal-ext> AS decimal)` is.
4. The `MP_JSON` payload layout produced by Picodata is byte-identical to
   Tarantool's own encoder.
5. Tarantool hands `MP_JSON` to a Lua function as a decoded table, so
   `JSON_EXTRACT_PATH` works, or its body needs adapting.

## Future Work

Captured in a companion document so this ADR stays scoped to literals:

- **Query support for JSON columns.** `GROUP BY`, `DISTINCT`, `ORDER BY`,
  `UNION`, and join keys. These need router-side `MP_JSON` hashing and
  comparison aligned to the JSONB ordering `null < string < number < bool <
  array < object`.
- **`CAST(json AS text)`** at the SQL level.
- **PostgreSQL construction functions** for ergonomic and interpolated JSON:
  `json_build_object`, `json_build_array`, `to_json`. Function arguments are
  expressions, so interpolation is natural and the syntax stays PostgreSQL
  compatible. The per-row case needs storage-side JSON construction (a Tarantool
  function or pushdown), which is the part this ADR's rejected native-literal
  option also depended on.

## References

- Tarantool native JSON type: https://git.picodata.io/core/tarantool/-/merge_requests/408
- PostgreSQL JSON Types: https://www.postgresql.org/docs/current/datatype-json.html
- PostgreSQL JSON Functions: https://www.postgresql.org/docs/current/functions-json.html
- MessagePack Extension Types: https://github.com/msgpack/msgpack/blob/master/spec.md#extension-types
