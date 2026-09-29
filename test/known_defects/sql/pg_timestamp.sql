-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Cases of PostgreSQL's src/test/regress/sql/timestamp.sql where Picodata
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

-- TEST: timestamp-5-setup
-- SQL:
CREATE TABLE timestamp_tbl (id INT PRIMARY KEY, d1 DATETIME);

-- TEST: timestamp-15
-- SQL:
INSERT INTO timestamp_tbl VALUES (15, 'today');
-- ERROR:
failed to parse 'today' as a value of type datetime, consider using explicit type casts

-- TEST: timestamp-16
-- SQL:
INSERT INTO timestamp_tbl VALUES (16, 'yesterday');
-- ERROR:
failed to parse 'yesterday' as a value of type datetime, consider using explicit type casts

-- TEST: timestamp-17
-- SQL:
INSERT INTO timestamp_tbl VALUES (17, 'tomorrow');
-- ERROR:
failed to parse 'tomorrow' as a value of type datetime, consider using explicit type casts

-- TEST: timestamp-19
-- SQL:
INSERT INTO timestamp_tbl VALUES (19, 'tomorrow EST');
-- ERROR:
failed to parse 'tomorrow EST' as a value of type datetime, consider using explicit type casts

-- TEST: timestamp-20
-- SQL:
INSERT INTO timestamp_tbl VALUES (20, 'tomorrow zulu');
-- ERROR:
failed to parse 'tomorrow zulu' as a value of type datetime, consider using explicit type casts

-- TEST: timestamp-22
-- SKIP_FOR: iproto
-- SQL:
SELECT count(*) AS one FROM timestamp_tbl WHERE d1 = datetime 'today';
-- ERROR:
Type mismatch: can not convert string\('today'\) to datetime

-- TEST: timestamp-23
-- SKIP_FOR: iproto
-- SQL:
SELECT count(*) AS one FROM timestamp_tbl WHERE d1 = datetime 'tomorrow';
-- ERROR:
Type mismatch: can not convert string\('tomorrow'\) to datetime

-- TEST: timestamp-24
-- SKIP_FOR: iproto
-- SQL:
SELECT count(*) AS one FROM timestamp_tbl WHERE d1 = datetime 'yesterday';
-- ERROR:
Type mismatch: can not convert string\('yesterday'\) to datetime

-- TEST: timestamp-33
-- SQL:
INSERT INTO timestamp_tbl VALUES (33, 'now');
-- ERROR:
failed to parse 'now' as a value of type datetime, consider using explicit type casts

-- TEST: timestamp-48
-- SQL:
INSERT INTO timestamp_tbl VALUES (48, '-infinity');
-- ERROR:
failed to parse '-infinity' as a value of type datetime, consider using explicit type casts

-- TEST: timestamp-49
-- SQL:
INSERT INTO timestamp_tbl VALUES (49, 'infinity');
-- ERROR:
failed to parse 'infinity' as a value of type datetime, consider using explicit type casts

-- TEST: timestamp-52
-- SQL:
SELECT datetime 'infinity' = datetime '+infinity' AS t;
-- ERROR:
Type mismatch: can not convert string\('infinity'\) to datetime

-- TEST: timestamp-100
-- SQL:
SELECT 'now'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('now'\) to datetime

-- TEST: timestamp-152
-- SQL:
SELECT '294276-12-31 23:59:59'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('294276-12-31 23:59:59'\) to datetime
