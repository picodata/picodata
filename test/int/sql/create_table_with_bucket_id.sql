-- TEST: initialization
-- SQL:
DROP TABLE IF EXISTS t;
CREATE TABLE t(a INT, PRIMARY KEY (bucket_id, a)) DISTRIBUTED BY (a);
DROP TABLE IF EXISTS t;

-- TEST: incorrect_primary_key_wrong_order
-- SQL:
CREATE TABLE t(a INT, PRIMARY KEY (a, bucket_id));
-- ERROR:
invalid primary key: Primary key must include bucket_id as first column.

-- TEST: incorrect_primary_key_only_bucket_id
-- SQL:
CREATE TABLE t(a INT, PRIMARY KEY (bucket_id));
-- ERROR:
invalid primary key: Primary key must include at least one column in addition to bucket_id.

-- TEST: incorrect_column_name
-- SQL:
CREATE TABLE t(a INT, bucket_id INT, PRIMARY KEY (bucket_id, a));
-- ERROR:
invalid column: bucket_id is reserved for system use in sharded tables. Choose another name.

-- TEST: incorrect_primary_key_global_table
-- SQL:
CREATE TABLE t(a INT, PRIMARY KEY (bucket_id, a)) DISTRIBUTED GLOBALLY;
-- ERROR:
invalid column: Primary key column bucket_id not found.
