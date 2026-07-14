## fix/sql

- Reused CTE bodies are now materialized once per query instead of being
  silently inlined per reference, fixing wrong results for non-deterministic
  CTE bodies and avoiding redundant recomputation.
