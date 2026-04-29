\set aid (random(1, 100000 * :scale))
\set tid (random(1, 10 * :scale))
\set bid (random(1, 1 * :scale))
\set delta random(-5000, 5000)

DO $$
BEGIN
    INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
        VALUES (:tid, :bid, :aid, :delta, NULL);
END $$
