-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Port of PostgreSQL's src/test/regress/sql/date.sql.
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

-- TEST: date-5-setup
-- SQL:
CREATE TABLE date_tbl (id INT PRIMARY KEY, f1 DATETIME);

-- TEST: date-7
-- SQL:
INSERT INTO date_tbl VALUES (7, '1957-04-09');

-- TEST: date-8
-- SQL:
INSERT INTO date_tbl VALUES (8, '1957-06-13');

-- TEST: date-9
-- SQL:
INSERT INTO date_tbl VALUES (9, '1996-02-28');

-- TEST: date-10
-- SQL:
INSERT INTO date_tbl VALUES (10, '1996-02-29');

-- TEST: date-11
-- SQL:
INSERT INTO date_tbl VALUES (11, '1996-03-01');

-- TEST: date-12
-- SQL:
INSERT INTO date_tbl VALUES (12, '1996-03-02');

-- TEST: date-13
-- SQL:
INSERT INTO date_tbl VALUES (13, '1997-02-28');

-- TEST: date-14
-- SQL:
INSERT INTO date_tbl VALUES (14, '1997-02-29');
-- ERROR:
failed to parse '1997-02-29' as a value of type datetime, consider using explicit type casts

-- TEST: date-15
-- SQL:
INSERT INTO date_tbl VALUES (15, '1997-03-01');

-- TEST: date-16
-- SQL:
INSERT INTO date_tbl VALUES (16, '1997-03-02');

-- TEST: date-17
-- SQL:
INSERT INTO date_tbl VALUES (17, '2000-04-01');

-- TEST: date-18
-- SQL:
INSERT INTO date_tbl VALUES (18, '2000-04-02');

-- TEST: date-19
-- SQL:
INSERT INTO date_tbl VALUES (19, '2000-04-03');

-- TEST: date-20
-- SQL:
INSERT INTO date_tbl VALUES (20, '2038-04-08');

-- TEST: date-21
-- SQL:
INSERT INTO date_tbl VALUES (21, '2039-04-09');

-- TEST: date-22
-- SQL:
INSERT INTO date_tbl VALUES (22, '2040-04-10');

-- TEST: date-23
-- SQL:
INSERT INTO date_tbl VALUES (23, '2040-04-10 BC');

-- TEST: date-25
-- SQL:
SELECT f1::text FROM date_tbl;
-- UNORDERED:
'1957-04-09T00:00:00Z',
'1957-06-13T00:00:00Z',
'1996-02-28T00:00:00Z',
'1996-02-29T00:00:00Z',
'1996-03-01T00:00:00Z',
'1996-03-02T00:00:00Z',
'1997-02-28T00:00:00Z',
'1997-03-01T00:00:00Z',
'1997-03-02T00:00:00Z',
'2000-04-01T00:00:00Z',
'2000-04-02T00:00:00Z',
'2000-04-03T00:00:00Z',
'2038-04-08T00:00:00Z',
'2039-04-09T00:00:00Z',
'2040-04-10T00:00:00Z',
'-2039-04-10T00:00:00Z'

-- TEST: date-27
-- SQL:
SELECT f1::text FROM date_tbl WHERE f1 < '2000-01-01';
-- UNORDERED:
'1957-04-09T00:00:00Z',
'1957-06-13T00:00:00Z',
'1996-02-28T00:00:00Z',
'1996-02-29T00:00:00Z',
'1996-03-01T00:00:00Z',
'1996-03-02T00:00:00Z',
'1997-02-28T00:00:00Z',
'1997-03-01T00:00:00Z',
'1997-03-02T00:00:00Z',
'-2039-04-10T00:00:00Z'

-- TEST: date-29
-- SQL:
SELECT f1::text FROM date_tbl
  WHERE f1 BETWEEN '2000-01-01' AND '2001-01-01';
-- UNORDERED:
'2000-04-01T00:00:00Z',
'2000-04-02T00:00:00Z',
'2000-04-03T00:00:00Z'

-- TEST: date-141
-- SQL:
SELECT datetime 'January 8, 1999'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-142
-- SQL:
SELECT datetime '1999-01-08'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-143
-- SQL:
SELECT datetime '1999-01-18'::text;
-- EXPECTED:
'1999-01-18T00:00:00Z'

-- TEST: date-144
-- SQL:
SELECT datetime '1/8/1999'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-145
-- SQL:
SELECT datetime '1/18/1999'::text;
-- EXPECTED:
'1999-01-18T00:00:00Z'

-- TEST: date-146
-- SQL:
SELECT datetime '18/1/1999'::text;
-- ERROR:
Type mismatch: can not convert string\('18/1/1999'\) to datetime

-- TEST: date-147
-- SQL:
SELECT datetime '01/02/03'::text;
-- EXPECTED:
'2003-01-02T00:00:00Z'

-- TEST: date-148
-- SQL:
SELECT datetime '19990108'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-149
-- SQL:
SELECT datetime '990108'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-150
-- SQL:
SELECT datetime '1999.008'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-151
-- SQL:
SELECT datetime 'J2451187'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-152
-- SQL:
SELECT datetime 'January 8, 99 BC'::text;
-- EXPECTED:
'-098-01-08T00:00:00Z'

-- TEST: date-154
-- SQL:
SELECT datetime '99-Jan-08'::text;
-- ERROR:
Type mismatch: can not convert string\('99-Jan-08'\) to datetime

-- TEST: date-155
-- SQL:
SELECT datetime '1999-Jan-08'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-156
-- SQL:
SELECT datetime '08-Jan-99'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-157
-- SQL:
SELECT datetime '08-Jan-1999'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-158
-- SQL:
SELECT datetime 'Jan-08-99'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-159
-- SQL:
SELECT datetime 'Jan-08-1999'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-160
-- SQL:
SELECT datetime '99-08-Jan'::text;
-- ERROR:
Type mismatch: can not convert string\('99-08-Jan'\) to datetime

-- TEST: date-161
-- SQL:
SELECT datetime '1999-08-Jan'::text;
-- ERROR:
Type mismatch: can not convert string\('1999-08-Jan'\) to datetime

-- TEST: date-163
-- SQL:
SELECT datetime '99 Jan 08'::text;
-- ERROR:
Type mismatch: can not convert string\('99 Jan 08'\) to datetime

-- TEST: date-164
-- SQL:
SELECT datetime '1999 Jan 08'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-165
-- SQL:
SELECT datetime '08 Jan 99'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-166
-- SQL:
SELECT datetime '08 Jan 1999'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-167
-- SQL:
SELECT datetime 'Jan 08 99'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-168
-- SQL:
SELECT datetime 'Jan 08 1999'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-169
-- SQL:
SELECT datetime '99 08 Jan'::text;
-- ERROR:
Type mismatch: can not convert string\('99 08 Jan'\) to datetime

-- TEST: date-170
-- SQL:
SELECT datetime '1999 08 Jan'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-172
-- SQL:
SELECT datetime '99-01-08'::text;
-- ERROR:
Type mismatch: can not convert string\('99-01-08'\) to datetime

-- TEST: date-173
-- SQL:
SELECT datetime '1999-01-08'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-174
-- SQL:
SELECT datetime '08-01-99'::text;
-- EXPECTED:
'1999-08-01T00:00:00Z'

-- TEST: date-175
-- SQL:
SELECT datetime '08-01-1999'::text;
-- EXPECTED:
'1999-08-01T00:00:00Z'

-- TEST: date-176
-- SQL:
SELECT datetime '01-08-99'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-177
-- SQL:
SELECT datetime '01-08-1999'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-178
-- SQL:
SELECT datetime '99-08-01'::text;
-- ERROR:
Type mismatch: can not convert string\('99-08-01'\) to datetime

-- TEST: date-179
-- SQL:
SELECT datetime '1999-08-01'::text;
-- EXPECTED:
'1999-08-01T00:00:00Z'

-- TEST: date-181
-- SQL:
SELECT datetime '99 01 08'::text;
-- ERROR:
Type mismatch: can not convert string\('99 01 08'\) to datetime

-- TEST: date-182
-- SQL:
SELECT datetime '1999 01 08'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-183
-- SQL:
SELECT datetime '08 01 99'::text;
-- EXPECTED:
'1999-08-01T00:00:00Z'

-- TEST: date-184
-- SQL:
SELECT datetime '08 01 1999'::text;
-- EXPECTED:
'1999-08-01T00:00:00Z'

-- TEST: date-185
-- SQL:
SELECT datetime '01 08 99'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-186
-- SQL:
SELECT datetime '01 08 1999'::text;
-- EXPECTED:
'1999-01-08T00:00:00Z'

-- TEST: date-187
-- SQL:
SELECT datetime '99 08 01'::text;
-- ERROR:
Type mismatch: can not convert string\('99 08 01'\) to datetime

-- TEST: date-188
-- SQL:
SELECT datetime '1999 08 01'::text;
-- EXPECTED:
'1999-08-01T00:00:00Z'

-- TEST: date-191
-- SQL:
SELECT datetime '4714-11-24 BC'::text;
-- EXPECTED:
'-4713-11-24T00:00:00Z'

-- TEST: date-192
-- SQL:
SELECT datetime '4714-11-23 BC'::text;
-- ERROR:
Type mismatch: can not convert string\('4714-11-23 BC'\) to datetime

-- TEST: date-194
-- SQL:
SELECT datetime '5874898-01-01'::text;
-- ERROR:
Type mismatch: can not convert string\('5874898-01-01'\) to datetime

-- TEST: date-198
-- SQL:
SELECT 'garbage'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('garbage'\) to datetime

-- TEST: date-199
-- SQL:
SELECT '6874898-01-01'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('6874898-01-01'\) to datetime
