-- TEST: null
-- SQL:
DROP TABLE IF EXISTS testing_space;
CREATE TABLE testing_space ("id" int primary key, "name" string, "product_units" int);
INSERT INTO "testing_space" ("id", "name", "product_units") VALUES
    (1, 'some', NULL),
    (2, NULL, NULL),
    (3, NULL, NULL),
    (4, NULL, 10);

-- TEST: transfer_nulls_at_the_end
-- SQL:
SELECT id, name, product_units, NULL FROM "testing_space";
-- UNORDERED:
1, 'some', NULL, NULL,
3, NULL, NULL, NULL,
2, NULL, NULL, NULL,
4, NULL, 10, NULL
