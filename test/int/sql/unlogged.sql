-- TEST: globally-distributed-unlogged-table
-- SQL:
create unlogged table t (a int primary key) distributed globally;
-- ERROR:
Global tables can't be unlogged.

-- TEST: unlogged-table-on-vinyl
-- SQL:
create unlogged table t (a int primary key) using vinyl;
-- ERROR:
Unlogged tables can use only memtx engine.
