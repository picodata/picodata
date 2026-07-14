## feat/sql

- Added an equality-facts analysis pass over the relational plan that derives
  always-true equalities from `WHERE` / `ON` predicates and stores them per
  output slot. Downstream stages (motion planning, bucket determination, JPPD
  transformation) can now answer "is this slot fixed to a constant?" and "do
  these two slots belong to the same equality class?" in O(1) without
  re-parsing predicates. Semantic boundaries (LEFT JOIN nullable side, CTE,
  Motion, set-ops, `LIMIT` / `ORDER BY` / `GROUP BY` / `HAVING`) are respected
  so unrelated scopes never merge, and parameter placeholders are kept
  separate from constants to avoid leaking unsafe bindings into execution-time
  filters.
