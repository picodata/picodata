## fix/sql

- [picodata#2732] Fixed a panic when attempting to inherit privileges via SQL
  (e.g., `GRANT admin TO somebody`). We do not support privilege inheritance
  via `GRANT user1 TO user2`. The system now validates the grantee type and
  returns a proper `NoSuchRole` error instead of panicking.
