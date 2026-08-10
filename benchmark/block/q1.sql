\set uid    (random(1, 100000 * :scale))
\set amount (random(1, 100))

SELECT balance FROM checking WHERE user_id = :uid;
UPDATE checking SET balance = balance - :amount WHERE user_id = :uid;
UPDATE savings  SET balance = balance + :amount WHERE user_id = :uid;
