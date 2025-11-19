\set aid0 (random(1, 100000 * :scale))
\set aid :aid0
\set delta random(-5000, 5000)

DO $$
BEGIN 
  RETURN QUERY SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
  UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
  UPDATE pgbench_accounts SET abalance = abalance - :delta WHERE aid = :aid;
END $$

