## feat/sql

- A system catalog migration can declare columns of type `ANY` and `ANY[]` in
  `CREATE TABLE` and `ALTER TABLE ADD COLUMN`. A column of this type accepts a
  value of every type, so it has no type safety: it is for the tables of
  system migrations. Any other statement, admin included, gets
  `the type ANY is available only to system migrations`.
- A column of type `ANY` cannot be `NOT NULL` (it accepts NULL whatever the
  declaration says) and cannot be part of a primary key, as an `ARRAY` column
  cannot.
- An expression of type `ANY` needs an explicit cast where the type decides,
  as in `count(val::int)`. Rows of different value types in one `VALUES` list
  need a cast or one `INSERT` per row.
