## breaking/pgproto

- Removed the `pico.pg_*` Lua functions and the `.proc_pg_*` procedures.
  They gave access to the PostgreSQL protocol statements and portals and
  were used only by tests. Use a PostgreSQL client instead.
