## fix/sql

- A condition no longer loses part of a conjunction with an `OR` in it on the way to the
  storage. It happened to a conjunction of three or more terms where the planner does not
  bring the condition to the disjunctive normal form: in an operand of a comparison, as in
  `((a = 1 OR a = 2) AND a > 0 AND a < 5) = false`, and under an `OR` in a condition too
  large for that form (more than about 500 disjunctions in it). The storage got only some of
  the terms, so `SELECT` returned wrong rows, and `DELETE` and `UPDATE` changed rows that
  did not match the condition.
