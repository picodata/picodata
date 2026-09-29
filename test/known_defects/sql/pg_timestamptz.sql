-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Cases of PostgreSQL's src/test/regress/sql/timestamptz.sql where Picodata
-- disagrees with PostgreSQL.
--
-- Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
--
-- Portions Copyright (c) 1994, The Regents of the University of California
--
-- Permission to use, copy, modify, and distribute this software and its
-- documentation for any purpose, without fee, and without a written agreement
-- is hereby granted, provided that the above copyright notice and this
-- paragraph and the following two paragraphs appear in all copies.
--
-- IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
-- DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
-- LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
-- DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
-- POSSIBILITY OF SUCH DAMAGE.
--
-- THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
-- INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
-- AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
-- ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
-- PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

-- TEST: timestamptz-5-setup
-- SQL:
CREATE TABLE timestamptz_tbl (id INT PRIMARY KEY, d1 DATETIME);

-- TEST: timestamptz-15
-- SQL:
INSERT INTO timestamptz_tbl VALUES (15, 'today');
-- ERROR:
failed to parse 'today' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-16
-- SQL:
INSERT INTO timestamptz_tbl VALUES (16, 'yesterday');
-- ERROR:
failed to parse 'yesterday' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-17
-- SQL:
INSERT INTO timestamptz_tbl VALUES (17, 'tomorrow');
-- ERROR:
failed to parse 'tomorrow' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-18
-- SQL:
INSERT INTO timestamptz_tbl VALUES (18, 'tomorrow EST');
-- ERROR:
failed to parse 'tomorrow EST' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-19
-- SQL:
INSERT INTO timestamptz_tbl VALUES (19, 'tomorrow zulu');
-- ERROR:
failed to parse 'tomorrow zulu' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-21
-- SKIP_FOR: iproto
-- SQL:
SELECT count(*) AS one FROM timestamptz_tbl WHERE d1 = datetime 'today';
-- ERROR:
Type mismatch: can not convert string\('today'\) to datetime

-- TEST: timestamptz-22
-- SKIP_FOR: iproto
-- SQL:
SELECT count(*) AS one FROM timestamptz_tbl WHERE d1 = datetime 'tomorrow';
-- ERROR:
Type mismatch: can not convert string\('tomorrow'\) to datetime

-- TEST: timestamptz-23
-- SKIP_FOR: iproto
-- SQL:
SELECT count(*) AS one FROM timestamptz_tbl WHERE d1 = datetime 'yesterday';
-- ERROR:
Type mismatch: can not convert string\('yesterday'\) to datetime

-- TEST: timestamptz-24
-- SKIP_FOR: iproto
-- SQL:
SELECT count(*) AS one FROM timestamptz_tbl WHERE d1 = datetime 'tomorrow EST';
-- ERROR:
Type mismatch: can not convert string\('tomorrow EST'\) to datetime

-- TEST: timestamptz-25
-- SKIP_FOR: iproto
-- SQL:
SELECT count(*) AS one FROM timestamptz_tbl WHERE d1 = datetime 'tomorrow zulu';
-- ERROR:
Type mismatch: can not convert string\('tomorrow zulu'\) to datetime

-- TEST: timestamptz-34
-- SQL:
INSERT INTO timestamptz_tbl VALUES (34, 'now');
-- ERROR:
failed to parse 'now' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-49
-- SQL:
INSERT INTO timestamptz_tbl VALUES (49, '-infinity');
-- ERROR:
failed to parse '-infinity' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-50
-- SQL:
INSERT INTO timestamptz_tbl VALUES (50, 'infinity');
-- ERROR:
failed to parse 'infinity' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-53
-- SQL:
SELECT datetime 'infinity' = datetime '+infinity' AS t;
-- ERROR:
Type mismatch: can not convert string\('infinity'\) to datetime

-- TEST: timestamptz-109
-- SQL:
SELECT '205000-07-10 17:32:01 Europe/Helsinki'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('205000-07-10 17:32:01 Europe/Helsinki'\) to datetime

-- TEST: timestamptz-110
-- SQL:
SELECT '205000-01-10 17:32:01 Europe/Helsinki'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('205000-01-10 17:32:01 Europe/Helsinki'\) to datetime

-- TEST: timestamptz-130
-- SQL:
SELECT 'now'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('now'\) to datetime

-- TEST: timestamptz-192
-- SQL:
SELECT '294276-12-31 23:59:59+00'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('294276-12-31 23:59:59\+00'\) to datetime

-- TEST: timestamptz-193
-- SQL:
SELECT '294276-12-31 15:59:59-08'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('294276-12-31 15:59:59-08'\) to datetime

-- TEST: timestamptz-430-setup
-- SQL:
CREATE TABLE timestamptz_tst (id INT PRIMARY KEY, a INT, b DATETIME);

-- TEST: timestamptz-434
-- SQL:
INSERT INTO timestamptz_tst VALUES (434, 2, 'Sat Mar 12 23:58:48 10000 IST');
-- ERROR:
failed to parse 'Sat Mar 12 23:58:48 10000 IST' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-435
-- SQL:
INSERT INTO timestamptz_tst VALUES (435, 3, 'Sat Mar 12 23:58:48 100000 IST');
-- ERROR:
failed to parse 'Sat Mar 12 23:58:48 100000 IST' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-436
-- SQL:
INSERT INTO timestamptz_tst VALUES (436, 3, '10000 Mar 12 23:58:48 IST');
-- ERROR:
failed to parse '10000 Mar 12 23:58:48 IST' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-437
-- SQL:
INSERT INTO timestamptz_tst VALUES (437, 4, '100000312 23:58:48 IST');
-- ERROR:
failed to parse '100000312 23:58:48 IST' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-438
-- SQL:
INSERT INTO timestamptz_tst VALUES (438, 4, '1000000312 23:58:48 IST');
-- ERROR:
failed to parse '1000000312 23:58:48 IST' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-442-setup
-- SQL:
DROP TABLE timestamptz_tst;

-- TEST: timestamptz-678-setup
-- SQL:
CREATE TABLE tmptz (f1 DATETIME PRIMARY KEY);

-- TEST: timestamptz-679-setup
-- SQL:
INSERT INTO tmptz VALUES ('2017-01-18 00:00+00');

-- TEST: timestamptz-682-other-offset
-- SKIP_FOR: pgproto-1rsX1
-- SQL:
SELECT count(*) FROM tmptz WHERE f1 = '2017-01-18 03:00+03';
-- EXPECTED:
0

-- TEST: timestamptz-682-duplicate-key
-- SKIP_FOR: pgproto-1rsX1
-- SQL:
INSERT INTO tmptz VALUES ('2017-01-18 03:00+03');

-- TEST: timestamptz-682-count
-- SKIP_FOR: pgproto-1rsX1
-- SQL:
SELECT count(f1), count(DISTINCT f1) FROM tmptz;
-- EXPECTED:
2, 1
