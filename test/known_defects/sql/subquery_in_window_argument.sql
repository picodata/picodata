-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- TEST: subquery-in-window-argument-setup
-- SQL:
CREATE TABLE v1 (c0 INT, c1 INT, c2 INT, PRIMARY KEY (c0));
INSERT INTO v1 VALUES (1, 2, 3);

-- TEST: subquery-in-window-argument-setup.2
-- SQL:
CREATE TABLE v2 (c0 INT, PRIMARY KEY (c0)) DISTRIBUTED BY (c0);
INSERT INTO v2 VALUES (10), (20);

-- TEST: subquery-in-window-argument
-- SQL:
SELECT MAX(EXISTS (SELECT * FROM v1)) OVER () FROM v2;
-- ERROR:
sbroad: invalid expression: column at position 2 is not among the 2 columns of the relational child output

-- TEST: subquery-in-window-argument-explicit-columns
-- SQL:
SELECT MAX(EXISTS (SELECT v1.c0, v1.c1, v1.c2 FROM v1)) OVER () FROM v2;
-- ERROR:
sbroad: invalid expression: column at position 2 is not among the 2 columns of the relational child output
