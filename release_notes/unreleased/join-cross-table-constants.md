## fix/sql

- A join which pins constants on the sharding keys of both children no longer 
  returns an empty result, e.g. `SELECT * FROM t1 JOIN t2 ON a = 1 AND b = 2` 
  where `t1` is sharded by `a` and `t2` by `b`.
