\set id 1

DO $$
BEGIN
  INSERT INTO counter (id, value) VALUES (:id, 0) ON CONFLICT (id) DO UPDATE SET value = value + 1;
END $$
