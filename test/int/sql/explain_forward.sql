-- TEST: setup
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t (a INT PRIMARY KEY, b TEXT);

-- TEST: test-explain-forward-insert-query
-- SQL:
EXPLAIN (FORWARD) INSERT INTO t (a) VALUES (1), (2), (3);
-- EXPECTED:
forward analysis (on > ro_to_rw > off):
  forward = off

-- TEST: test-forward-insert-query
-- SQL:
INSERT INTO t (a) VALUES (1), (2), (3)
OPTION (FORWARD = OFF);

-- TEST: test-explain-forward-delete-with-filter
-- SQL:
EXPLAIN (FORWARD) DELETE FROM t WHERE a in (1, 2, 3, 4) and b = 'kek';
-- EXPECTED:
forward analysis (on > ro_to_rw > off):
  forward = off

-- TEST: test-forward-delete-with-filter
-- SQL:
DELETE FROM t WHERE a in (1, 2, 3, 4) and b = 'kek'
OPTION (FORWARD = OFF);

-- TEST: test-explain-forward-update-with-filter
-- SQL:
EXPLAIN (FORWARD) UPDATE t SET b = 'lol' WHERE a = 2;
-- EXPECTED:
forward analysis (on > ro_to_rw > off):
  forward = off

-- TEST: test-forward-update-with-filter
-- SQL:
UPDATE t SET b = 'lol' WHERE a = 2
OPTION (FORWARD = OFF);

-- TEST: test-explain-forward-update-many-buckets
-- SQL:
EXPLAIN (FORWARD) UPDATE t SET b = 'lol' WHERE a = 2 or a = 3;
-- EXPECTED:
forward analysis (on > ro_to_rw > off):
  forward = off

-- TEST: test-forward-update-all-buckets
-- SQL:
UPDATE t SET b = 'lol' WHERE a = 2 or a = 3
OPTION (FORWARD = OFF);

-- TEST: test-explain-forward-single-value
-- SQL:
EXPLAIN (FORWARD) SELECT * FROM t WHERE a = 1;
-- EXPECTED:
forward analysis (on > ro_to_rw > off):
  forward = off

-- TEST: test-forward-single-value
-- SQL:
SELECT * FROM t WHERE a = 1
OPTION (FORWARD = OFF);
-- EXPECTED:
1, None

-- TEST: test-explain-forward-many-and-ors
-- SQL:
EXPLAIN (FORWARD) SELECT * FROM t WHERE a = 1 and b = 'pek' or a = 6 and b =
'kek' or a = 90 or a = 2;
-- EXPECTED:
forward analysis (on > ro_to_rw > off):
  forward = off

-- TEST: test-forward-many-and-ors
-- SQL:
SELECT * FROM t WHERE (a = 1 and b = 'pek') 
                      or (a = 6 and b = 'kek')
                      or a = 90
                      or a = 2
OPTION (FORWARD = OFF);
-- EXPECTED:
2, 'lol'

-- TEST: test-explain-forward-join-sharding-key
-- SQL:
EXPLAIN (FORWARD) SELECT * FROM t tt JOIN t ttt on tt.a = ttt.a WHERE tt.a = 2;
-- EXPECTED:
forward analysis (on > ro_to_rw > off):
  forward = off

-- TEST: test-forward-join-sharding-key
-- SQL:
SELECT * FROM t tt JOIN t ttt on tt.a = ttt.a WHERE tt.a = 2
OPTION (FORWARD = OFF);
-- EXPECTED:
2, 'lol', 2, 'lol'

-- TEST: test-explain-forward-not-sharding-key
-- SQL:
EXPLAIN (FORWARD)
SELECT * FROM t WHERE b = 'lol';
-- EXPECTED:
forward analysis (on > ro_to_rw > off):
  forward = on

-- TEST: test-forward-not-sharding-key-off-error
-- SQL:
SELECT * FROM t WHERE b = 'lol'
OPTION (FORWARD = OFF);
-- ERROR:
sbroad: invalid option: cannot satisfy "forward = off": buckets span multiple nodes and are not present on the current node

-- TEST: test-explain-forward-off-block-query
-- SQL:
EXPLAIN (FORWARD) DO $$ BEGIN
    RETURN QUERY SELECT b FROM t WHERE a = 2;
    UPDATE t SET b = 'lol' WHERE a = 2;
END $$;
-- EXPECTED:
forward analysis (on > ro_to_rw > off):
  forward = off

-- TEST: test-forward-off-block-query
-- SQL:
DO $$ BEGIN
    RETURN QUERY SELECT b FROM t WHERE a = 2;
    UPDATE t SET b = 'lol' WHERE a = 2;
END $$
OPTION (FORWARD = OFF);
-- EXPECTED:
'lol'

-- TEST: test-forward-ro-to-rw-block-query
-- SQL:
DO $$ BEGIN
    RETURN QUERY SELECT b FROM t WHERE a = 3;
    UPDATE t SET b = 'kek' WHERE a = 3;
END $$
OPTION (FORWARD = RO_TO_RW);
-- EXPECTED:
'lol'

-- TEST: test-explain-forward-on-block-query
-- SQL:
EXPLAIN (FORWARD)
DO $$ BEGIN
    LET var = (VALUES (123));
    
    RETURN QUERY SELECT b::int FROM t WHERE a = 1;
    RETURN QUERY SELECT a FROM t WHERE a = 1;

    IF var = 456 THEN
        UPDATE t SET b = 'kek' WHERE a = 1;
    END IF;
END $$;
-- EXPECTED:
forward analysis (on > ro_to_rw > off):
  forward = off

-- TEST: test-forward-on-block-query
-- SQL:
DO $$ BEGIN
    LET var = (VALUES (123));
    
    RETURN QUERY SELECT b::int FROM t WHERE a = 1;
    RETURN QUERY SELECT a FROM t WHERE a = 1;

    IF var = 456 THEN
        UPDATE t SET b = 'kek' WHERE a = 1;
    END IF;
END $$
OPTION (FORWARD = ON);
-- EXPECTED:
None, 1
