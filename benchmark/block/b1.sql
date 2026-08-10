\set uid    (random(1, 100000 * :scale))
\set amount (random(1, 100))

DO $$
BEGIN
  LET bal = (SELECT balance FROM checking WHERE user_id = :uid);
  IF bal >= :amount THEN
    UPDATE checking SET balance = balance - :amount WHERE user_id = :uid;
    UPDATE savings  SET balance = balance + :amount WHERE user_id = :uid;
  END IF;
END $$
