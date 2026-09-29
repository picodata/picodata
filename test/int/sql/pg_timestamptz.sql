-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Port of PostgreSQL's src/test/regress/sql/timestamptz.sql.
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

-- TEST: timestamptz-51
-- SQL:
INSERT INTO timestamptz_tbl VALUES (51, 'epoch');

-- TEST: timestamptz-56
-- SQL:
INSERT INTO timestamptz_tbl VALUES (56, 'Mon Feb 10 17:32:01 1997 PST');

-- TEST: timestamptz-59
-- SQL:
INSERT INTO timestamptz_tbl VALUES (59, 'Mon Feb 10 17:32:01.000001 1997 PST');

-- TEST: timestamptz-60
-- SQL:
INSERT INTO timestamptz_tbl VALUES (60, 'Mon Feb 10 17:32:01.999999 1997 PST');

-- TEST: timestamptz-61
-- SQL:
INSERT INTO timestamptz_tbl VALUES (61, 'Mon Feb 10 17:32:01.4 1997 PST');

-- TEST: timestamptz-62
-- SQL:
INSERT INTO timestamptz_tbl VALUES (62, 'Mon Feb 10 17:32:01.5 1997 PST');

-- TEST: timestamptz-63
-- SQL:
INSERT INTO timestamptz_tbl VALUES (63, 'Mon Feb 10 17:32:01.6 1997 PST');

-- TEST: timestamptz-66
-- SQL:
INSERT INTO timestamptz_tbl VALUES (66, '1997-01-02');

-- TEST: timestamptz-67
-- SQL:
INSERT INTO timestamptz_tbl VALUES (67, '1997-01-02 03:04:05');

-- TEST: timestamptz-68
-- SQL:
INSERT INTO timestamptz_tbl VALUES (68, '1997-02-10 17:32:01-08');

-- TEST: timestamptz-69
-- SQL:
INSERT INTO timestamptz_tbl VALUES (69, '1997-02-10 17:32:01-0800');

-- TEST: timestamptz-70
-- SQL:
INSERT INTO timestamptz_tbl VALUES (70, '1997-02-10 17:32:01 -08:00');

-- TEST: timestamptz-71
-- SQL:
INSERT INTO timestamptz_tbl VALUES (71, '19970210 173201 -0800');

-- TEST: timestamptz-72
-- SQL:
INSERT INTO timestamptz_tbl VALUES (72, '1997-06-10 17:32:01 -07:00');

-- TEST: timestamptz-73
-- SQL:
INSERT INTO timestamptz_tbl VALUES (73, '2001-09-22T18:19:20');

-- TEST: timestamptz-76
-- SQL:
INSERT INTO timestamptz_tbl VALUES (76, '2000-03-15 08:14:01 GMT+8');

-- TEST: timestamptz-77
-- SQL:
INSERT INTO timestamptz_tbl VALUES (77, '2000-03-15 13:14:02 GMT-1');

-- TEST: timestamptz-78
-- SQL:
INSERT INTO timestamptz_tbl VALUES (78, '2000-03-15 12:14:03 GMT-2');

-- TEST: timestamptz-79
-- SQL:
INSERT INTO timestamptz_tbl VALUES (79, '2000-03-15 03:14:04 PST+8');

-- TEST: timestamptz-80
-- SQL:
INSERT INTO timestamptz_tbl VALUES (80, '2000-03-15 02:14:05 MST+7:00');

-- TEST: timestamptz-83
-- SQL:
INSERT INTO timestamptz_tbl VALUES (83, 'Feb 10 17:32:01 1997 -0800');

-- TEST: timestamptz-84
-- SQL:
INSERT INTO timestamptz_tbl VALUES (84, 'Feb 10 17:32:01 1997');

-- TEST: timestamptz-85
-- SQL:
INSERT INTO timestamptz_tbl VALUES (85, 'Feb 10 5:32PM 1997');

-- TEST: timestamptz-86
-- SQL:
INSERT INTO timestamptz_tbl VALUES (86, '1997/02/10 17:32:01-0800');

-- TEST: timestamptz-87
-- SQL:
INSERT INTO timestamptz_tbl VALUES (87, '1997-02-10 17:32:01 PST');

-- TEST: timestamptz-88
-- SQL:
INSERT INTO timestamptz_tbl VALUES (88, 'Feb-10-1997 17:32:01 PST');

-- TEST: timestamptz-89
-- SQL:
INSERT INTO timestamptz_tbl VALUES (89, '02-10-1997 17:32:01 PST');

-- TEST: timestamptz-90
-- SQL:
INSERT INTO timestamptz_tbl VALUES (90, '19970210 173201 PST');

-- TEST: timestamptz-92
-- SQL:
INSERT INTO timestamptz_tbl VALUES (92, '97FEB10 5:32:01PM UTC');
-- ERROR:
failed to parse '97FEB10 5:32:01PM UTC' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-93
-- SQL:
INSERT INTO timestamptz_tbl VALUES (93, '97/02/10 17:32:01 UTC');
-- ERROR:
failed to parse '97/02/10 17:32:01 UTC' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-95
-- SQL:
INSERT INTO timestamptz_tbl VALUES (95, '1997.041 17:32:01 UTC');

-- TEST: timestamptz-98
-- SQL:
INSERT INTO timestamptz_tbl VALUES (98, '19970210 173201 America/New_York');

-- TEST: timestamptz-100
-- SQL:
INSERT INTO timestamptz_tbl VALUES (100, '19970710 173201 America/New_York');

-- TEST: timestamptz-102
-- SQL:
INSERT INTO timestamptz_tbl VALUES (102, '19970710 173201 America/Does_not_exist');
-- ERROR:
failed to parse '19970710 173201 America/Does_not_exist' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-106
-- SQL:
SELECT '20500710 173201 Europe/Helsinki'::datetime::text;
-- EXPECTED:
'2050-07-10T17:32:01+0300'

-- TEST: timestamptz-107
-- SQL:
SELECT '20500110 173201 Europe/Helsinki'::datetime::text;
-- EXPECTED:
'2050-01-10T17:32:01+0200'

-- TEST: timestamptz-113
-- SQL:
SELECT 'Jan 01 00:00:00 1000 LMT'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('Jan 01 00:00:00 1000 LMT'\) to datetime

-- TEST: timestamptz-114
-- SQL:
SELECT 'Jan 01 00:00:00 2024 LMT'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('Jan 01 00:00:00 2024 LMT'\) to datetime

-- TEST: timestamptz-122
-- SQL:
SELECT '1912-01-01 00:00 MMT'::datetime::text;
-- EXPECTED:
'1912-01-01T00:00:00+0630'

-- TEST: timestamptz-131
-- SQL:
SELECT 'garbage'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('garbage'\) to datetime

-- TEST: timestamptz-132
-- SQL:
SELECT '2001-01-01 00:00 Nehwon/Lankhmar'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('2001-01-01 00:00 Nehwon/Lankhmar'\) to datetime

-- TEST: timestamptz-137
-- SQL:
INSERT INTO timestamptz_tbl VALUES (137, '1997-06-10 18:32:01 PDT');

-- TEST: timestamptz-139
-- SQL:
INSERT INTO timestamptz_tbl VALUES (139, 'Feb 10 17:32:01 1997');

-- TEST: timestamptz-140
-- SQL:
INSERT INTO timestamptz_tbl VALUES (140, 'Feb 11 17:32:01 1997');

-- TEST: timestamptz-141
-- SQL:
INSERT INTO timestamptz_tbl VALUES (141, 'Feb 12 17:32:01 1997');

-- TEST: timestamptz-142
-- SQL:
INSERT INTO timestamptz_tbl VALUES (142, 'Feb 13 17:32:01 1997');

-- TEST: timestamptz-143
-- SQL:
INSERT INTO timestamptz_tbl VALUES (143, 'Feb 14 17:32:01 1997');

-- TEST: timestamptz-144
-- SQL:
INSERT INTO timestamptz_tbl VALUES (144, 'Feb 15 17:32:01 1997');

-- TEST: timestamptz-145
-- SQL:
INSERT INTO timestamptz_tbl VALUES (145, 'Feb 16 17:32:01 1997');

-- TEST: timestamptz-147
-- SQL:
INSERT INTO timestamptz_tbl VALUES (147, 'Feb 16 17:32:01 0097 BC');

-- TEST: timestamptz-148
-- SQL:
INSERT INTO timestamptz_tbl VALUES (148, 'Feb 16 17:32:01 0097');

-- TEST: timestamptz-149
-- SQL:
INSERT INTO timestamptz_tbl VALUES (149, 'Feb 16 17:32:01 0597');

-- TEST: timestamptz-150
-- SQL:
INSERT INTO timestamptz_tbl VALUES (150, 'Feb 16 17:32:01 1097');

-- TEST: timestamptz-151
-- SQL:
INSERT INTO timestamptz_tbl VALUES (151, 'Feb 16 17:32:01 1697');

-- TEST: timestamptz-152
-- SQL:
INSERT INTO timestamptz_tbl VALUES (152, 'Feb 16 17:32:01 1797');

-- TEST: timestamptz-153
-- SQL:
INSERT INTO timestamptz_tbl VALUES (153, 'Feb 16 17:32:01 1897');

-- TEST: timestamptz-154
-- SQL:
INSERT INTO timestamptz_tbl VALUES (154, 'Feb 16 17:32:01 1997');

-- TEST: timestamptz-155
-- SQL:
INSERT INTO timestamptz_tbl VALUES (155, 'Feb 16 17:32:01 2097');

-- TEST: timestamptz-157
-- SQL:
INSERT INTO timestamptz_tbl VALUES (157, 'Feb 28 17:32:01 1996');

-- TEST: timestamptz-158
-- SQL:
INSERT INTO timestamptz_tbl VALUES (158, 'Feb 29 17:32:01 1996');

-- TEST: timestamptz-159
-- SQL:
INSERT INTO timestamptz_tbl VALUES (159, 'Mar 01 17:32:01 1996');

-- TEST: timestamptz-160
-- SQL:
INSERT INTO timestamptz_tbl VALUES (160, 'Dec 30 17:32:01 1996');

-- TEST: timestamptz-161
-- SQL:
INSERT INTO timestamptz_tbl VALUES (161, 'Dec 31 17:32:01 1996');

-- TEST: timestamptz-162
-- SQL:
INSERT INTO timestamptz_tbl VALUES (162, 'Jan 01 17:32:01 1997');

-- TEST: timestamptz-163
-- SQL:
INSERT INTO timestamptz_tbl VALUES (163, 'Feb 28 17:32:01 1997');

-- TEST: timestamptz-164
-- SQL:
INSERT INTO timestamptz_tbl VALUES (164, 'Feb 29 17:32:01 1997');
-- ERROR:
failed to parse 'Feb 29 17:32:01 1997' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-165
-- SQL:
INSERT INTO timestamptz_tbl VALUES (165, 'Mar 01 17:32:01 1997');

-- TEST: timestamptz-166
-- SQL:
INSERT INTO timestamptz_tbl VALUES (166, 'Dec 30 17:32:01 1997');

-- TEST: timestamptz-167
-- SQL:
INSERT INTO timestamptz_tbl VALUES (167, 'Dec 31 17:32:01 1997');

-- TEST: timestamptz-168
-- SQL:
INSERT INTO timestamptz_tbl VALUES (168, 'Dec 31 17:32:01 1999');

-- TEST: timestamptz-169
-- SQL:
INSERT INTO timestamptz_tbl VALUES (169, 'Jan 01 17:32:01 2000');

-- TEST: timestamptz-170
-- SQL:
INSERT INTO timestamptz_tbl VALUES (170, 'Dec 31 17:32:01 2000');

-- TEST: timestamptz-171
-- SQL:
INSERT INTO timestamptz_tbl VALUES (171, 'Jan 01 17:32:01 2001');

-- TEST: timestamptz-174
-- SQL:
INSERT INTO timestamptz_tbl VALUES (174, 'Feb 16 17:32:01 -0097');
-- ERROR:
failed to parse 'Feb 16 17:32:01 -0097' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-175
-- SQL:
INSERT INTO timestamptz_tbl VALUES (175, 'Feb 16 17:32:01 5097 BC');
-- ERROR:
failed to parse 'Feb 16 17:32:01 5097 BC' as a value of type datetime, consider using explicit type casts

-- TEST: timestamptz-179
-- SQL:
SELECT 'Wed Jul 11 10:51:14 America/New_York 2001'::datetime::text;
-- EXPECTED:
'2001-07-11T10:51:14-0400'

-- TEST: timestamptz-180
-- SQL:
SELECT 'Wed Jul 11 10:51:14 GMT-4 2001'::datetime::text;
-- EXPECTED:
'2001-07-11T10:51:14+0400'

-- TEST: timestamptz-181
-- SQL:
SELECT 'Wed Jul 11 10:51:14 GMT+4 2001'::datetime::text;
-- EXPECTED:
'2001-07-11T10:51:14-0400'

-- TEST: timestamptz-182
-- SQL:
SELECT 'Wed Jul 11 10:51:14 PST-03:00 2001'::datetime::text;
-- EXPECTED:
'2001-07-11T10:51:14+0300'

-- TEST: timestamptz-183
-- SQL:
SELECT 'Wed Jul 11 10:51:14 PST+03:00 2001'::datetime::text;
-- EXPECTED:
'2001-07-11T10:51:14-0300'

-- TEST: timestamptz-185
-- SQL:
SELECT d1::text FROM timestamptz_tbl;
-- UNORDERED:
'1970-01-01T00:00:00Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01.000001-0800',
'1997-02-10T17:32:01.999999-0800',
'1997-02-10T17:32:01.400-0800',
'1997-02-10T17:32:01.500-0800',
'1997-02-10T17:32:01.600-0800',
'1997-01-02T00:00:00Z',
'1997-01-02T03:04:05Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-06-10T17:32:01-0700',
'2001-09-22T18:19:20Z',
'2000-03-15T08:14:01-0800',
'2000-03-15T13:14:02+0100',
'2000-03-15T12:14:03+0200',
'2000-03-15T03:14:04-0800',
'2000-03-15T02:14:05-0700',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01Z',
'1997-02-10T17:32:00Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01Z',
'1997-02-10T17:32:01-0500',
'1997-07-10T17:32:01-0400',
'1997-06-10T18:32:01-0700',
'1997-02-10T17:32:01Z',
'1997-02-11T17:32:01Z',
'1997-02-12T17:32:01Z',
'1997-02-13T17:32:01Z',
'1997-02-14T17:32:01Z',
'1997-02-15T17:32:01Z',
'1997-02-16T17:32:01Z',
'-096-02-16T17:32:01Z',
'0097-02-16T17:32:01Z',
'0597-02-16T17:32:01Z',
'1097-02-16T17:32:01Z',
'1697-02-16T17:32:01Z',
'1797-02-16T17:32:01Z',
'1897-02-16T17:32:01Z',
'1997-02-16T17:32:01Z',
'2097-02-16T17:32:01Z',
'1996-02-28T17:32:01Z',
'1996-02-29T17:32:01Z',
'1996-03-01T17:32:01Z',
'1996-12-30T17:32:01Z',
'1996-12-31T17:32:01Z',
'1997-01-01T17:32:01Z',
'1997-02-28T17:32:01Z',
'1997-03-01T17:32:01Z',
'1997-12-30T17:32:01Z',
'1997-12-31T17:32:01Z',
'1999-12-31T17:32:01Z',
'2000-01-01T17:32:01Z',
'2000-12-31T17:32:01Z',
'2001-01-01T17:32:01Z'

-- TEST: timestamptz-185-distinct
-- SQL:
SELECT count(d1), count(DISTINCT d1) FROM timestamptz_tbl;
-- EXPECTED:
62, 49

-- TEST: timestamptz-185-same-instant
-- SQL:
SELECT count(*) FROM timestamptz_tbl WHERE d1 = '1997-02-11 01:32:01+00';
-- EXPECTED:
11

-- TEST: timestamptz-188
-- SQL:
SELECT '4714-11-24 00:00:00+00 BC'::datetime::text;
-- EXPECTED:
'-4713-11-24T00:00:00Z'

-- TEST: timestamptz-189
-- SQL:
SELECT '4714-11-23 16:00:00-08 BC'::datetime::text;
-- EXPECTED:
'-4713-11-23T16:00:00-0800'

-- TEST: timestamptz-190
-- SQL:
SELECT 'Sun Nov 23 16:00:00 4714 PST BC'::datetime::text;
-- EXPECTED:
'-4713-11-23T16:00:00-0800'

-- TEST: timestamptz-191
-- SQL:
SELECT '4714-11-23 23:59:59+00 BC'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('4714-11-23 23:59:59\+00 BC'\) to datetime

-- TEST: timestamptz-194
-- SQL:
SELECT '294277-01-01 00:00:00+00'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('294277-01-01 00:00:00\+00'\) to datetime

-- TEST: timestamptz-195
-- SQL:
SELECT '294277-12-31 16:00:00-08'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('294277-12-31 16:00:00-08'\) to datetime

-- TEST: timestamptz-198
-- SQL:
SELECT d1::text FROM timestamptz_tbl
   WHERE d1 > datetime '1997-01-02';
-- UNORDERED:
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01.000001-0800',
'1997-02-10T17:32:01.999999-0800',
'1997-02-10T17:32:01.400-0800',
'1997-02-10T17:32:01.500-0800',
'1997-02-10T17:32:01.600-0800',
'1997-01-02T03:04:05Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-06-10T17:32:01-0700',
'2001-09-22T18:19:20Z',
'2000-03-15T08:14:01-0800',
'2000-03-15T13:14:02+0100',
'2000-03-15T12:14:03+0200',
'2000-03-15T03:14:04-0800',
'2000-03-15T02:14:05-0700',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01Z',
'1997-02-10T17:32:00Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01Z',
'1997-02-10T17:32:01-0500',
'1997-07-10T17:32:01-0400',
'1997-06-10T18:32:01-0700',
'1997-02-10T17:32:01Z',
'1997-02-11T17:32:01Z',
'1997-02-12T17:32:01Z',
'1997-02-13T17:32:01Z',
'1997-02-14T17:32:01Z',
'1997-02-15T17:32:01Z',
'1997-02-16T17:32:01Z',
'1997-02-16T17:32:01Z',
'2097-02-16T17:32:01Z',
'1997-02-28T17:32:01Z',
'1997-03-01T17:32:01Z',
'1997-12-30T17:32:01Z',
'1997-12-31T17:32:01Z',
'1999-12-31T17:32:01Z',
'2000-01-01T17:32:01Z',
'2000-12-31T17:32:01Z',
'2001-01-01T17:32:01Z'

-- TEST: timestamptz-201
-- SQL:
SELECT d1::text FROM timestamptz_tbl
   WHERE d1 < datetime '1997-01-02';
-- UNORDERED:
'1970-01-01T00:00:00Z',
'-096-02-16T17:32:01Z',
'0097-02-16T17:32:01Z',
'0597-02-16T17:32:01Z',
'1097-02-16T17:32:01Z',
'1697-02-16T17:32:01Z',
'1797-02-16T17:32:01Z',
'1897-02-16T17:32:01Z',
'1996-02-28T17:32:01Z',
'1996-02-29T17:32:01Z',
'1996-03-01T17:32:01Z',
'1996-12-30T17:32:01Z',
'1996-12-31T17:32:01Z',
'1997-01-01T17:32:01Z'

-- TEST: timestamptz-204
-- SQL:
SELECT d1::text FROM timestamptz_tbl
   WHERE d1 = datetime '1997-01-02';
-- EXPECTED:
'1997-01-02T00:00:00Z'

-- TEST: timestamptz-207
-- SQL:
SELECT d1::text FROM timestamptz_tbl
   WHERE d1 != datetime '1997-01-02';
-- UNORDERED:
'1970-01-01T00:00:00Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01.000001-0800',
'1997-02-10T17:32:01.999999-0800',
'1997-02-10T17:32:01.400-0800',
'1997-02-10T17:32:01.500-0800',
'1997-02-10T17:32:01.600-0800',
'1997-01-02T03:04:05Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-06-10T17:32:01-0700',
'2001-09-22T18:19:20Z',
'2000-03-15T08:14:01-0800',
'2000-03-15T13:14:02+0100',
'2000-03-15T12:14:03+0200',
'2000-03-15T03:14:04-0800',
'2000-03-15T02:14:05-0700',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01Z',
'1997-02-10T17:32:00Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01Z',
'1997-02-10T17:32:01-0500',
'1997-07-10T17:32:01-0400',
'1997-06-10T18:32:01-0700',
'1997-02-10T17:32:01Z',
'1997-02-11T17:32:01Z',
'1997-02-12T17:32:01Z',
'1997-02-13T17:32:01Z',
'1997-02-14T17:32:01Z',
'1997-02-15T17:32:01Z',
'1997-02-16T17:32:01Z',
'-096-02-16T17:32:01Z',
'0097-02-16T17:32:01Z',
'0597-02-16T17:32:01Z',
'1097-02-16T17:32:01Z',
'1697-02-16T17:32:01Z',
'1797-02-16T17:32:01Z',
'1897-02-16T17:32:01Z',
'1997-02-16T17:32:01Z',
'2097-02-16T17:32:01Z',
'1996-02-28T17:32:01Z',
'1996-02-29T17:32:01Z',
'1996-03-01T17:32:01Z',
'1996-12-30T17:32:01Z',
'1996-12-31T17:32:01Z',
'1997-01-01T17:32:01Z',
'1997-02-28T17:32:01Z',
'1997-03-01T17:32:01Z',
'1997-12-30T17:32:01Z',
'1997-12-31T17:32:01Z',
'1999-12-31T17:32:01Z',
'2000-01-01T17:32:01Z',
'2000-12-31T17:32:01Z',
'2001-01-01T17:32:01Z'

-- TEST: timestamptz-210
-- SQL:
SELECT d1::text FROM timestamptz_tbl
   WHERE d1 <= datetime '1997-01-02';
-- UNORDERED:
'1970-01-01T00:00:00Z',
'1997-01-02T00:00:00Z',
'-096-02-16T17:32:01Z',
'0097-02-16T17:32:01Z',
'0597-02-16T17:32:01Z',
'1097-02-16T17:32:01Z',
'1697-02-16T17:32:01Z',
'1797-02-16T17:32:01Z',
'1897-02-16T17:32:01Z',
'1996-02-28T17:32:01Z',
'1996-02-29T17:32:01Z',
'1996-03-01T17:32:01Z',
'1996-12-30T17:32:01Z',
'1996-12-31T17:32:01Z',
'1997-01-01T17:32:01Z'

-- TEST: timestamptz-213
-- SQL:
SELECT d1::text FROM timestamptz_tbl
   WHERE d1 >= datetime '1997-01-02';
-- UNORDERED:
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01.000001-0800',
'1997-02-10T17:32:01.999999-0800',
'1997-02-10T17:32:01.400-0800',
'1997-02-10T17:32:01.500-0800',
'1997-02-10T17:32:01.600-0800',
'1997-01-02T00:00:00Z',
'1997-01-02T03:04:05Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-06-10T17:32:01-0700',
'2001-09-22T18:19:20Z',
'2000-03-15T08:14:01-0800',
'2000-03-15T13:14:02+0100',
'2000-03-15T12:14:03+0200',
'2000-03-15T03:14:04-0800',
'2000-03-15T02:14:05-0700',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01Z',
'1997-02-10T17:32:00Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01Z',
'1997-02-10T17:32:01-0500',
'1997-07-10T17:32:01-0400',
'1997-06-10T18:32:01-0700',
'1997-02-10T17:32:01Z',
'1997-02-11T17:32:01Z',
'1997-02-12T17:32:01Z',
'1997-02-13T17:32:01Z',
'1997-02-14T17:32:01Z',
'1997-02-15T17:32:01Z',
'1997-02-16T17:32:01Z',
'1997-02-16T17:32:01Z',
'2097-02-16T17:32:01Z',
'1997-02-28T17:32:01Z',
'1997-03-01T17:32:01Z',
'1997-12-30T17:32:01Z',
'1997-12-31T17:32:01Z',
'1999-12-31T17:32:01Z',
'2000-01-01T17:32:01Z',
'2000-12-31T17:32:01Z',
'2001-01-01T17:32:01Z'

-- TEST: timestamptz-216
-- SQL:
SELECT d1::text
   FROM timestamptz_tbl WHERE d1 BETWEEN '1902-01-01' AND '2038-01-01';
-- UNORDERED:
'1970-01-01T00:00:00Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01.000001-0800',
'1997-02-10T17:32:01.999999-0800',
'1997-02-10T17:32:01.400-0800',
'1997-02-10T17:32:01.500-0800',
'1997-02-10T17:32:01.600-0800',
'1997-01-02T00:00:00Z',
'1997-01-02T03:04:05Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-06-10T17:32:01-0700',
'2001-09-22T18:19:20Z',
'2000-03-15T08:14:01-0800',
'2000-03-15T13:14:02+0100',
'2000-03-15T12:14:03+0200',
'2000-03-15T03:14:04-0800',
'2000-03-15T02:14:05-0700',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01Z',
'1997-02-10T17:32:00Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01Z',
'1997-02-10T17:32:01-0500',
'1997-07-10T17:32:01-0400',
'1997-06-10T18:32:01-0700',
'1997-02-10T17:32:01Z',
'1997-02-11T17:32:01Z',
'1997-02-12T17:32:01Z',
'1997-02-13T17:32:01Z',
'1997-02-14T17:32:01Z',
'1997-02-15T17:32:01Z',
'1997-02-16T17:32:01Z',
'1997-02-16T17:32:01Z',
'1996-02-28T17:32:01Z',
'1996-02-29T17:32:01Z',
'1996-03-01T17:32:01Z',
'1996-12-30T17:32:01Z',
'1996-12-31T17:32:01Z',
'1997-01-01T17:32:01Z',
'1997-02-28T17:32:01Z',
'1997-03-01T17:32:01Z',
'1997-12-30T17:32:01Z',
'1997-12-31T17:32:01Z',
'1999-12-31T17:32:01Z',
'2000-01-01T17:32:01Z',
'2000-12-31T17:32:01Z',
'2001-01-01T17:32:01Z'

-- TEST: timestamptz-289
-- SQL:
SELECT d1::text
  FROM timestamptz_tbl
  WHERE d1 BETWEEN datetime '1902-01-01' AND datetime '2038-01-01';
-- UNORDERED:
'1970-01-01T00:00:00Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01.000001-0800',
'1997-02-10T17:32:01.999999-0800',
'1997-02-10T17:32:01.400-0800',
'1997-02-10T17:32:01.500-0800',
'1997-02-10T17:32:01.600-0800',
'1997-01-02T00:00:00Z',
'1997-01-02T03:04:05Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-06-10T17:32:01-0700',
'2001-09-22T18:19:20Z',
'2000-03-15T08:14:01-0800',
'2000-03-15T13:14:02+0100',
'2000-03-15T12:14:03+0200',
'2000-03-15T03:14:04-0800',
'2000-03-15T02:14:05-0700',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01Z',
'1997-02-10T17:32:00Z',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01-0800',
'1997-02-10T17:32:01Z',
'1997-02-10T17:32:01-0500',
'1997-07-10T17:32:01-0400',
'1997-06-10T18:32:01-0700',
'1997-02-10T17:32:01Z',
'1997-02-11T17:32:01Z',
'1997-02-12T17:32:01Z',
'1997-02-13T17:32:01Z',
'1997-02-14T17:32:01Z',
'1997-02-15T17:32:01Z',
'1997-02-16T17:32:01Z',
'1997-02-16T17:32:01Z',
'1996-02-28T17:32:01Z',
'1996-02-29T17:32:01Z',
'1996-03-01T17:32:01Z',
'1996-12-30T17:32:01Z',
'1996-12-31T17:32:01Z',
'1997-01-01T17:32:01Z',
'1997-02-28T17:32:01Z',
'1997-03-01T17:32:01Z',
'1997-12-30T17:32:01Z',
'1997-12-31T17:32:01Z',
'1999-12-31T17:32:01Z',
'2000-01-01T17:32:01Z',
'2000-12-31T17:32:01Z',
'2001-01-01T17:32:01Z'

-- TEST: timestamptz-379
-- SQL:
SELECT "COLUMN_1"::text
   FROM (VALUES
       ('2018-11-02 12:34:56'::datetime),
       ('2018-11-02 12:34:56.78'),
       ('2018-11-02 12:34:56.78901'),
       ('2018-11-02 12:34:56.78901234')
   );
-- UNORDERED:
'2018-11-02T12:34:56Z',
'2018-11-02T12:34:56.780Z',
'2018-11-02T12:34:56.789010Z',
'2018-11-02T12:34:56.789012Z'

-- TEST: timestamptz-430-setup
-- SQL:
CREATE TABLE timestamptz_tst (id INT PRIMARY KEY, a INT, b DATETIME);

-- TEST: timestamptz-433
-- SQL:
INSERT INTO timestamptz_tst VALUES (433, 1, 'Sat Mar 12 23:58:48 1000 IST');

-- TEST: timestamptz-440
-- SQL:
SELECT a, b::text FROM timestamptz_tst ORDER BY a;
-- EXPECTED:
1, '1000-03-12T23:58:48+0200'

-- TEST: timestamptz-442-setup
-- SQL:
DROP TABLE timestamptz_tst;

-- TEST: timestamptz-527
-- SQL:
SELECT '2011-03-27 00:00:00 Europe/Moscow'::datetime::text;
-- EXPECTED:
'2011-03-27T00:00:00+0300'

-- TEST: timestamptz-528
-- SQL:
SELECT '2011-03-27 01:00:00 Europe/Moscow'::datetime::text;
-- EXPECTED:
'2011-03-27T01:00:00+0300'

-- TEST: timestamptz-529
-- SQL:
SELECT '2011-03-27 01:59:59 Europe/Moscow'::datetime::text;
-- EXPECTED:
'2011-03-27T01:59:59+0300'

-- TEST: timestamptz-530
-- SQL:
SELECT '2011-03-27 02:00:00 Europe/Moscow'::datetime::text;
-- EXPECTED:
'2011-03-27T02:00:00+0300'

-- TEST: timestamptz-531
-- SQL:
SELECT '2011-03-27 02:00:01 Europe/Moscow'::datetime::text;
-- EXPECTED:
'2011-03-27T02:00:01+0300'

-- TEST: timestamptz-532
-- SQL:
SELECT '2011-03-27 02:59:59 Europe/Moscow'::datetime::text;
-- EXPECTED:
'2011-03-27T02:59:59+0300'

-- TEST: timestamptz-533
-- SQL:
SELECT '2011-03-27 03:00:00 Europe/Moscow'::datetime::text;
-- EXPECTED:
'2011-03-27T03:00:00+0400'

-- TEST: timestamptz-534
-- SQL:
SELECT '2011-03-27 03:00:01 Europe/Moscow'::datetime::text;
-- EXPECTED:
'2011-03-27T03:00:01+0400'

-- TEST: timestamptz-535
-- SQL:
SELECT '2011-03-27 04:00:00 Europe/Moscow'::datetime::text;
-- EXPECTED:
'2011-03-27T04:00:00+0400'

-- TEST: timestamptz-537
-- SQL:
SELECT '2011-03-27 00:00:00 MSK'::datetime::text;
-- EXPECTED:
'2011-03-27T00:00:00+0300'

-- TEST: timestamptz-538
-- SQL:
SELECT '2011-03-27 01:00:00 MSK'::datetime::text;
-- EXPECTED:
'2011-03-27T01:00:00+0300'

-- TEST: timestamptz-539
-- SQL:
SELECT '2011-03-27 01:59:59 MSK'::datetime::text;
-- EXPECTED:
'2011-03-27T01:59:59+0300'

-- TEST: timestamptz-540
-- SQL:
SELECT '2011-03-27 02:00:00 MSK'::datetime::text;
-- EXPECTED:
'2011-03-27T02:00:00+0400'

-- TEST: timestamptz-541
-- SQL:
SELECT '2011-03-27 02:00:01 MSK'::datetime::text;
-- EXPECTED:
'2011-03-27T02:00:01+0400'

-- TEST: timestamptz-542
-- SQL:
SELECT '2011-03-27 02:59:59 MSK'::datetime::text;
-- EXPECTED:
'2011-03-27T02:59:59+0400'

-- TEST: timestamptz-543
-- SQL:
SELECT '2011-03-27 03:00:00 MSK'::datetime::text;
-- EXPECTED:
'2011-03-27T03:00:00+0400'

-- TEST: timestamptz-544
-- SQL:
SELECT '2011-03-27 03:00:01 MSK'::datetime::text;
-- EXPECTED:
'2011-03-27T03:00:01+0400'

-- TEST: timestamptz-545
-- SQL:
SELECT '2011-03-27 04:00:00 MSK'::datetime::text;
-- EXPECTED:
'2011-03-27T04:00:00+0400'

-- TEST: timestamptz-547
-- SQL:
SELECT '2014-10-26 00:00:00 Europe/Moscow'::datetime::text;
-- EXPECTED:
'2014-10-26T00:00:00+0400'

-- TEST: timestamptz-548
-- SQL:
SELECT '2014-10-26 00:59:59 Europe/Moscow'::datetime::text;
-- EXPECTED:
'2014-10-26T00:59:59+0400'

-- TEST: timestamptz-549
-- SQL:
SELECT '2014-10-26 01:00:00 Europe/Moscow'::datetime::text;
-- EXPECTED:
'2014-10-26T01:00:00+0300'

-- TEST: timestamptz-550
-- SQL:
SELECT '2014-10-26 01:00:01 Europe/Moscow'::datetime::text;
-- EXPECTED:
'2014-10-26T01:00:01+0300'

-- TEST: timestamptz-551
-- SQL:
SELECT '2014-10-26 02:00:00 Europe/Moscow'::datetime::text;
-- EXPECTED:
'2014-10-26T02:00:00+0300'

-- TEST: timestamptz-553
-- SQL:
SELECT '2014-10-26 00:00:00 MSK'::datetime::text;
-- EXPECTED:
'2014-10-26T00:00:00+0400'

-- TEST: timestamptz-554
-- SQL:
SELECT '2014-10-26 00:59:59 MSK'::datetime::text;
-- EXPECTED:
'2014-10-26T00:59:59+0400'

-- TEST: timestamptz-555
-- SQL:
SELECT '2014-10-26 01:00:00 MSK'::datetime::text;
-- EXPECTED:
'2014-10-26T01:00:00+0300'

-- TEST: timestamptz-556
-- SQL:
SELECT '2014-10-26 01:00:01 MSK'::datetime::text;
-- EXPECTED:
'2014-10-26T01:00:01+0300'

-- TEST: timestamptz-557
-- SQL:
SELECT '2014-10-26 02:00:00 MSK'::datetime::text;
-- EXPECTED:
'2014-10-26T02:00:00+0300'

-- TEST: timestamptz-608
-- SQL:
SELECT '2011-03-26 21:00:00 UTC'::datetime
     = '2011-03-27 00:00:00 Europe/Moscow'::datetime AS t;
-- EXPECTED:
true

-- TEST: timestamptz-609
-- SQL:
SELECT '2011-03-26 22:00:00 UTC'::datetime
     = '2011-03-27 01:00:00 Europe/Moscow'::datetime AS t;
-- EXPECTED:
true

-- TEST: timestamptz-610
-- SQL:
SELECT '2011-03-26 22:59:59 UTC'::datetime
     = '2011-03-27 01:59:59 Europe/Moscow'::datetime AS t;
-- EXPECTED:
true

-- TEST: timestamptz-611
-- SQL:
SELECT '2011-03-26 23:00:00 UTC'::datetime
     = '2011-03-27 03:00:00 Europe/Moscow'::datetime AS t;
-- EXPECTED:
true

-- TEST: timestamptz-612
-- SQL:
SELECT '2011-03-26 23:00:01 UTC'::datetime
     = '2011-03-27 03:00:01 Europe/Moscow'::datetime AS t;
-- EXPECTED:
true

-- TEST: timestamptz-613
-- SQL:
SELECT '2011-03-26 23:59:59 UTC'::datetime
     = '2011-03-27 03:59:59 Europe/Moscow'::datetime AS t;
-- EXPECTED:
true

-- TEST: timestamptz-614
-- SQL:
SELECT '2011-03-27 00:00:00 UTC'::datetime
     = '2011-03-27 04:00:00 Europe/Moscow'::datetime AS t;
-- EXPECTED:
true

-- TEST: timestamptz-616
-- SQL:
SELECT '2014-10-25 21:00:00 UTC'::datetime
     = '2014-10-26 01:00:00+04'::datetime AS t;
-- EXPECTED:
true

-- TEST: timestamptz-617
-- SQL:
SELECT '2014-10-25 21:59:59 UTC'::datetime
     = '2014-10-26 01:59:59+04'::datetime AS t;
-- EXPECTED:
true

-- TEST: timestamptz-618
-- SQL:
SELECT '2014-10-25 22:00:00 UTC'::datetime
     = '2014-10-26 01:00:00 Europe/Moscow'::datetime AS t;
-- EXPECTED:
true

-- TEST: timestamptz-619
-- SQL:
SELECT '2014-10-25 22:00:01 UTC'::datetime
     = '2014-10-26 01:00:01 Europe/Moscow'::datetime AS t;
-- EXPECTED:
true

-- TEST: timestamptz-620
-- SQL:
SELECT '2014-10-25 23:00:00 UTC'::datetime
     = '2014-10-26 02:00:00 Europe/Moscow'::datetime AS t;
-- EXPECTED:
true

-- TEST: timestamptz-657
-- SQL:
SELECT CAST('1978-07-07 19:38 America/New_York' AS datetime)::text;
-- EXPECTED:
'1978-07-07T19:38:00-0400'

-- TEST: timestamptz-678-setup
-- SQL:
CREATE TABLE tmptz (f1 DATETIME PRIMARY KEY);

-- TEST: timestamptz-679-setup
-- SQL:
INSERT INTO tmptz VALUES ('2017-01-18 00:00+00');

-- TEST: timestamptz-682
-- SQL:
SELECT f1::text FROM tmptz WHERE f1 = '2017-01-18 00:00';
-- EXPECTED:
'2017-01-18T00:00:00Z'

-- TEST: timestamptz-701
-- SQL:
SELECT datetime '1999-12-31 24:00:00'::text;
-- EXPECTED:
'2000-01-01T00:00:00Z'
