## feat/sql

- `IF` statements in transactional `DO` blocks now accept an optional `ELSE`
  branch: `IF <expr> THEN ... ELSE ... END IF;`. The branch runs when the
  condition is false or `NULL`, holds the same statements a `THEN` body does
  (including nested `IF`s, which is how a cascade of conditions is written),
  and may be empty. Each branch is a scope of its own, so a `LET` declared in
  one is invisible in the other. The reads-before-writes rule keeps applying to
  the block as it is written, so a `LET` or `RETURN QUERY` in an `ELSE` branch
  may not follow a DML statement of the `THEN` body.
