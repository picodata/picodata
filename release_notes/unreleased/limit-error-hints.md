## feat/sql

- The `sql_vdbe_opcode_max` and `sql_motion_row_max` limit errors now tell how
  to adjust the limit: with `OPTION (<name> = <new_limit>)` in the query or
  globally via `ALTER SYSTEM`.
