## fix/sql

- A join no longer loses rows when its `ON` condition compares a column of the
  right side with a subquery and the join key is not the sharding key of the
  right side, as in `t1 JOIN t2 ON t1.k = t2.k AND t2.id IN (SELECT id FROM t3)`
  or the same `LEFT JOIN`. The right side was copied to every node or resharded
  by the join key, but its subquery was placed as if the right side had stayed
  in place, so the matching rows of the subquery could be on another node.
