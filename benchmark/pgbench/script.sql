\set aid (random(1, 100000 * :scale))
\set tid (random(1, 10 * :scale))
\set bid (random(1, 1 * :scale))
\set delta random(-5000, 5000)
UPDATE pgbench_accounts SET abalance = cast(:delta as int) WHERE aid = cast(:aid as int);
SELECT abalance FROM pgbench_accounts WHERE aid = cast(:aid as int);
UPDATE pgbench_tellers SET tbalance = cast(:delta as int) WHERE tid = cast(:tid as int);
UPDATE pgbench_branches SET bbalance = cast(:delta as int) WHERE bid = cast(:bid as int);
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (cast(:tid as int), cast(:bid as int), cast(:aid as int), cast(:delta as int), NULL);
