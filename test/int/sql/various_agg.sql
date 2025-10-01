-- TEST: init
-- SQL:
create table t2( a text primary key, b text);
insert into t2 values('1', '3');
insert into t2 values('2', '2');
insert into t2 values('3', '1');

-- TEST: string_agg_without_scan
-- SQL:
SELECT STRING_AGG('str', ',');
-- EXPECTED:
str

-- TEST: group_concat_without_scan
-- SQL:
SELECT GROUP_CONCAT('str', ',');
-- EXPECTED:
str

-- TEST: string_agg_with_window
-- SQL:
SELECT STRING_AGG('str', ',') OVER ();
-- EXPECTED:
str

-- TEST: group_concat_with_scan_1
-- SQL:
select group_concat(a, '.') from t2;
-- EXPECTED:
1.2.3

-- TEST: group_concat_with_scan_2
-- SQL:
select group_concat(b, '.') from t2;
-- EXPECTED:
3.2.1

-- TEST: group_concat_with_scan_3
-- SQL:
select group_concat(a, 1) from t2;
-- ERROR:
could not resolve function overload for group_concat\(text, int\)

-- TEST: group_concat_with_scan_4
-- SQL:
select group_concat(a, b) from t2;
-- ERROR:
GROUP_CONCAT aggregate function second argument must be a string literal
