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
