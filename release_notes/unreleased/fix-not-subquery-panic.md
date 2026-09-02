## fix/sql NOT <subquery> panic

- Fixed a panic in queries containing negated subquery expressions (e.g. `WHERE NOT (SELECT FALSE)`).
