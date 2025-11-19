\set aid0 (random(1, 100000 * :scale))
\set aid :aid0

DO $$
BEGIN
    RETURN QUERY SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
END $$
