## fix/sql

- Fixed NOT push-down: no longer short-circuits before recursing into operand
  subtrees. The pass now also descends into Cast children to simplify NOTs nested inside.
