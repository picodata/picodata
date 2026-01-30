-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(a INT, PRIMARY KEY (bucket_id, a)) DISTRIBUTED BY (a);
DROP TABLE IF EXISTS t;

-- TEST: incorrect_primary_key_wrong_order
-- SQL:
CREATE TABLE t(a INT, PRIMARY KEY (a, bucket_id));
-- ERROR:
invalid column: Primary key column bucket_id not found.

-- TEST: incorrect_primary_key_only_bucket_id
-- SQL:
CREATE TABLE t(a INT, PRIMARY KEY (bucket_id));
-- ERROR:
invalid primary key: Primary key must include at least one column in addition to bucket_id.
