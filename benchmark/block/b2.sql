\set uid (random(1, 100000 * :scale))

DO $$
BEGIN
  RETURN QUERY SELECT balance FROM checking WHERE user_id = :uid;
END $$
