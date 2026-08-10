\set id     (random(1, 2000000000))
\set uid    (random(1, 100000 * :scale))
\set amount (random(-5000, 5000))

DO $$
BEGIN
  INSERT INTO ledger (id, user_id, amount) VALUES (:id, :uid, :amount) ON CONFLICT DO NOTHING;
END $$
