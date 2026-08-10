\set uid (random(1, 100000 * :scale))

SELECT balance FROM checking WHERE user_id = :uid;
