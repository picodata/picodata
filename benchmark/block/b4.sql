\set id 1

DO $$
BEGIN
    INSERT INTO pgbench_block_conflict_counter VALUES (:id, 0)
    ON CONFLICT("bucket_id", "id") DO UPDATE
    SET "counter" = "counter" + 1;
END $$
