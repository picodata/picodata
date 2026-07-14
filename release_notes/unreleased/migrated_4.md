## feat/sql

- [picodata#2728] Error generated when `INDEXED BY` is used with non existing index
  now includes the target table name, making it explicit that the index-table
  relationship lookup failed rather than a general index lookup.
