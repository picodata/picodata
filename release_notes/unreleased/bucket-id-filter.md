## feat/sql

- The `WHERE bucket_id = <CONST>` filter now avoids `Motion(Full)` insertion and 
  routes the query to a single storage. This optimization also helps execute 
  transactions locally and queries with `OPTION(forward = off)`.
