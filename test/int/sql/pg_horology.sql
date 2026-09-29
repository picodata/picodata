-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Port of PostgreSQL's src/test/regress/sql/horology.sql.
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

-- TEST: horology-timestamp_tbl-setup
-- SQL:
CREATE TABLE timestamp_tbl (id INT PRIMARY KEY, d1 DATETIME);

-- TEST: horology-timestamp-50
-- SQL:
INSERT INTO timestamp_tbl VALUES (50, 'epoch');

-- TEST: horology-timestamp-55
-- SQL:
INSERT INTO timestamp_tbl VALUES (55, 'Mon Feb 10 17:32:01 1997 PST');

-- TEST: horology-timestamp-58
-- SQL:
INSERT INTO timestamp_tbl VALUES (58, 'Mon Feb 10 17:32:01.000001 1997 PST');

-- TEST: horology-timestamp-59
-- SQL:
INSERT INTO timestamp_tbl VALUES (59, 'Mon Feb 10 17:32:01.999999 1997 PST');

-- TEST: horology-timestamp-60
-- SQL:
INSERT INTO timestamp_tbl VALUES (60, 'Mon Feb 10 17:32:01.4 1997 PST');

-- TEST: horology-timestamp-61
-- SQL:
INSERT INTO timestamp_tbl VALUES (61, 'Mon Feb 10 17:32:01.5 1997 PST');

-- TEST: horology-timestamp-62
-- SQL:
INSERT INTO timestamp_tbl VALUES (62, 'Mon Feb 10 17:32:01.6 1997 PST');

-- TEST: horology-timestamp-65
-- SQL:
INSERT INTO timestamp_tbl VALUES (65, '1997-01-02');

-- TEST: horology-timestamp-66
-- SQL:
INSERT INTO timestamp_tbl VALUES (66, '1997-01-02 03:04:05');

-- TEST: horology-timestamp-67
-- SQL:
INSERT INTO timestamp_tbl VALUES (67, '1997-02-10 17:32:01-08');

-- TEST: horology-timestamp-68
-- SQL:
INSERT INTO timestamp_tbl VALUES (68, '1997-02-10 17:32:01-0800');

-- TEST: horology-timestamp-69
-- SQL:
INSERT INTO timestamp_tbl VALUES (69, '1997-02-10 17:32:01 -08:00');

-- TEST: horology-timestamp-70
-- SQL:
INSERT INTO timestamp_tbl VALUES (70, '19970210 173201 -0800');

-- TEST: horology-timestamp-71
-- SQL:
INSERT INTO timestamp_tbl VALUES (71, '1997-06-10 17:32:01 -07:00');

-- TEST: horology-timestamp-72
-- SQL:
INSERT INTO timestamp_tbl VALUES (72, '2001-09-22T18:19:20');

-- TEST: horology-timestamp-75
-- SQL:
INSERT INTO timestamp_tbl VALUES (75, '2000-03-15 08:14:01 GMT+8');

-- TEST: horology-timestamp-76
-- SQL:
INSERT INTO timestamp_tbl VALUES (76, '2000-03-15 13:14:02 GMT-1');

-- TEST: horology-timestamp-77
-- SQL:
INSERT INTO timestamp_tbl VALUES (77, '2000-03-15 12:14:03 GMT-2');

-- TEST: horology-timestamp-78
-- SQL:
INSERT INTO timestamp_tbl VALUES (78, '2000-03-15 03:14:04 PST+8');

-- TEST: horology-timestamp-79
-- SQL:
INSERT INTO timestamp_tbl VALUES (79, '2000-03-15 02:14:05 MST+7:00');

-- TEST: horology-timestamp-82
-- SQL:
INSERT INTO timestamp_tbl VALUES (82, 'Feb 10 17:32:01 1997 -0800');

-- TEST: horology-timestamp-83
-- SQL:
INSERT INTO timestamp_tbl VALUES (83, 'Feb 10 17:32:01 1997');

-- TEST: horology-timestamp-84
-- SQL:
INSERT INTO timestamp_tbl VALUES (84, 'Feb 10 5:32PM 1997');

-- TEST: horology-timestamp-85
-- SQL:
INSERT INTO timestamp_tbl VALUES (85, '1997/02/10 17:32:01-0800');

-- TEST: horology-timestamp-86
-- SQL:
INSERT INTO timestamp_tbl VALUES (86, '1997-02-10 17:32:01 PST');

-- TEST: horology-timestamp-87
-- SQL:
INSERT INTO timestamp_tbl VALUES (87, 'Feb-10-1997 17:32:01 PST');

-- TEST: horology-timestamp-88
-- SQL:
INSERT INTO timestamp_tbl VALUES (88, '02-10-1997 17:32:01 PST');

-- TEST: horology-timestamp-89
-- SQL:
INSERT INTO timestamp_tbl VALUES (89, '19970210 173201 PST');

-- TEST: horology-timestamp-91
-- SQL:
INSERT INTO timestamp_tbl VALUES (91, '97FEB10 5:32:01PM UTC');
-- ERROR:
failed to parse '97FEB10 5:32:01PM UTC' as a value of type datetime, consider using explicit type casts

-- TEST: horology-timestamp-92
-- SQL:
INSERT INTO timestamp_tbl VALUES (92, '97/02/10 17:32:01 UTC');
-- ERROR:
failed to parse '97/02/10 17:32:01 UTC' as a value of type datetime, consider using explicit type casts

-- TEST: horology-timestamp-94
-- SQL:
INSERT INTO timestamp_tbl VALUES (94, '1997.041 17:32:01 UTC');

-- TEST: horology-timestamp-95
-- SQL:
INSERT INTO timestamp_tbl VALUES (95, '19970210 173201 America/New_York');

-- TEST: horology-timestamp-97
-- SQL:
INSERT INTO timestamp_tbl VALUES (97, '19970710 173201 America/Does_not_exist');
-- ERROR:
failed to parse '19970710 173201 America/Does_not_exist' as a value of type datetime, consider using explicit type casts

-- TEST: horology-timestamp-107
-- SQL:
INSERT INTO timestamp_tbl VALUES (107, '1997-06-10 18:32:01 PDT');

-- TEST: horology-timestamp-109
-- SQL:
INSERT INTO timestamp_tbl VALUES (109, 'Feb 10 17:32:01 1997');

-- TEST: horology-timestamp-110
-- SQL:
INSERT INTO timestamp_tbl VALUES (110, 'Feb 11 17:32:01 1997');

-- TEST: horology-timestamp-111
-- SQL:
INSERT INTO timestamp_tbl VALUES (111, 'Feb 12 17:32:01 1997');

-- TEST: horology-timestamp-112
-- SQL:
INSERT INTO timestamp_tbl VALUES (112, 'Feb 13 17:32:01 1997');

-- TEST: horology-timestamp-113
-- SQL:
INSERT INTO timestamp_tbl VALUES (113, 'Feb 14 17:32:01 1997');

-- TEST: horology-timestamp-114
-- SQL:
INSERT INTO timestamp_tbl VALUES (114, 'Feb 15 17:32:01 1997');

-- TEST: horology-timestamp-115
-- SQL:
INSERT INTO timestamp_tbl VALUES (115, 'Feb 16 17:32:01 1997');

-- TEST: horology-timestamp-117
-- SQL:
INSERT INTO timestamp_tbl VALUES (117, 'Feb 16 17:32:01 0097 BC');

-- TEST: horology-timestamp-118
-- SQL:
INSERT INTO timestamp_tbl VALUES (118, 'Feb 16 17:32:01 0097');

-- TEST: horology-timestamp-119
-- SQL:
INSERT INTO timestamp_tbl VALUES (119, 'Feb 16 17:32:01 0597');

-- TEST: horology-timestamp-120
-- SQL:
INSERT INTO timestamp_tbl VALUES (120, 'Feb 16 17:32:01 1097');

-- TEST: horology-timestamp-121
-- SQL:
INSERT INTO timestamp_tbl VALUES (121, 'Feb 16 17:32:01 1697');

-- TEST: horology-timestamp-122
-- SQL:
INSERT INTO timestamp_tbl VALUES (122, 'Feb 16 17:32:01 1797');

-- TEST: horology-timestamp-123
-- SQL:
INSERT INTO timestamp_tbl VALUES (123, 'Feb 16 17:32:01 1897');

-- TEST: horology-timestamp-124
-- SQL:
INSERT INTO timestamp_tbl VALUES (124, 'Feb 16 17:32:01 1997');

-- TEST: horology-timestamp-125
-- SQL:
INSERT INTO timestamp_tbl VALUES (125, 'Feb 16 17:32:01 2097');

-- TEST: horology-timestamp-127
-- SQL:
INSERT INTO timestamp_tbl VALUES (127, 'Feb 28 17:32:01 1996');

-- TEST: horology-timestamp-128
-- SQL:
INSERT INTO timestamp_tbl VALUES (128, 'Feb 29 17:32:01 1996');

-- TEST: horology-timestamp-129
-- SQL:
INSERT INTO timestamp_tbl VALUES (129, 'Mar 01 17:32:01 1996');

-- TEST: horology-timestamp-130
-- SQL:
INSERT INTO timestamp_tbl VALUES (130, 'Dec 30 17:32:01 1996');

-- TEST: horology-timestamp-131
-- SQL:
INSERT INTO timestamp_tbl VALUES (131, 'Dec 31 17:32:01 1996');

-- TEST: horology-timestamp-132
-- SQL:
INSERT INTO timestamp_tbl VALUES (132, 'Jan 01 17:32:01 1997');

-- TEST: horology-timestamp-133
-- SQL:
INSERT INTO timestamp_tbl VALUES (133, 'Feb 28 17:32:01 1997');

-- TEST: horology-timestamp-134
-- SQL:
INSERT INTO timestamp_tbl VALUES (134, 'Feb 29 17:32:01 1997');
-- ERROR:
failed to parse 'Feb 29 17:32:01 1997' as a value of type datetime, consider using explicit type casts

-- TEST: horology-timestamp-135
-- SQL:
INSERT INTO timestamp_tbl VALUES (135, 'Mar 01 17:32:01 1997');

-- TEST: horology-timestamp-136
-- SQL:
INSERT INTO timestamp_tbl VALUES (136, 'Dec 30 17:32:01 1997');

-- TEST: horology-timestamp-137
-- SQL:
INSERT INTO timestamp_tbl VALUES (137, 'Dec 31 17:32:01 1997');

-- TEST: horology-timestamp-138
-- SQL:
INSERT INTO timestamp_tbl VALUES (138, 'Dec 31 17:32:01 1999');

-- TEST: horology-timestamp-139
-- SQL:
INSERT INTO timestamp_tbl VALUES (139, 'Jan 01 17:32:01 2000');

-- TEST: horology-timestamp-140
-- SQL:
INSERT INTO timestamp_tbl VALUES (140, 'Dec 31 17:32:01 2000');

-- TEST: horology-timestamp-141
-- SQL:
INSERT INTO timestamp_tbl VALUES (141, 'Jan 01 17:32:01 2001');

-- TEST: horology-timestamp-144
-- SQL:
INSERT INTO timestamp_tbl VALUES (144, 'Feb 16 17:32:01 -0097');
-- ERROR:
failed to parse 'Feb 16 17:32:01 -0097' as a value of type datetime, consider using explicit type casts

-- TEST: horology-timestamp-145
-- SQL:
INSERT INTO timestamp_tbl VALUES (145, 'Feb 16 17:32:01 5097 BC');
-- ERROR:
failed to parse 'Feb 16 17:32:01 5097 BC' as a value of type datetime, consider using explicit type casts

-- TEST: horology-date_tbl-setup
-- SQL:
CREATE TABLE date_tbl (id INT PRIMARY KEY, f1 DATETIME);

-- TEST: horology-date-7
-- SQL:
INSERT INTO date_tbl VALUES (7, '1957-04-09');

-- TEST: horology-date-8
-- SQL:
INSERT INTO date_tbl VALUES (8, '1957-06-13');

-- TEST: horology-date-9
-- SQL:
INSERT INTO date_tbl VALUES (9, '1996-02-28');

-- TEST: horology-date-10
-- SQL:
INSERT INTO date_tbl VALUES (10, '1996-02-29');

-- TEST: horology-date-11
-- SQL:
INSERT INTO date_tbl VALUES (11, '1996-03-01');

-- TEST: horology-date-12
-- SQL:
INSERT INTO date_tbl VALUES (12, '1996-03-02');

-- TEST: horology-date-13
-- SQL:
INSERT INTO date_tbl VALUES (13, '1997-02-28');

-- TEST: horology-date-14
-- SQL:
INSERT INTO date_tbl VALUES (14, '1997-02-29');
-- ERROR:
failed to parse '1997-02-29' as a value of type datetime, consider using explicit type casts

-- TEST: horology-date-15
-- SQL:
INSERT INTO date_tbl VALUES (15, '1997-03-01');

-- TEST: horology-date-16
-- SQL:
INSERT INTO date_tbl VALUES (16, '1997-03-02');

-- TEST: horology-date-17
-- SQL:
INSERT INTO date_tbl VALUES (17, '2000-04-01');

-- TEST: horology-date-18
-- SQL:
INSERT INTO date_tbl VALUES (18, '2000-04-02');

-- TEST: horology-date-19
-- SQL:
INSERT INTO date_tbl VALUES (19, '2000-04-03');

-- TEST: horology-date-20
-- SQL:
INSERT INTO date_tbl VALUES (20, '2038-04-08');

-- TEST: horology-date-21
-- SQL:
INSERT INTO date_tbl VALUES (21, '2039-04-09');

-- TEST: horology-date-22
-- SQL:
INSERT INTO date_tbl VALUES (22, '2040-04-10');

-- TEST: horology-date-23
-- SQL:
INSERT INTO date_tbl VALUES (23, '2040-04-10 BC');

-- TEST: horology-11
-- SQL:
SELECT datetime '20011227 040506+08'::text;
-- EXPECTED:
'2001-12-27T04:05:06+0800'

-- TEST: horology-12
-- SQL:
SELECT datetime '20011227 040506-08'::text;
-- EXPECTED:
'2001-12-27T04:05:06-0800'

-- TEST: horology-13
-- SQL:
SELECT datetime '20011227 040506.789+08'::text;
-- EXPECTED:
'2001-12-27T04:05:06.789+0800'

-- TEST: horology-14
-- SQL:
SELECT datetime '20011227 040506.789-08'::text;
-- EXPECTED:
'2001-12-27T04:05:06.789-0800'

-- TEST: horology-15
-- SQL:
SELECT datetime '20011227T040506+08'::text;
-- EXPECTED:
'2001-12-27T04:05:06+0800'

-- TEST: horology-16
-- SQL:
SELECT datetime '20011227T040506-08'::text;
-- EXPECTED:
'2001-12-27T04:05:06-0800'

-- TEST: horology-17
-- SQL:
SELECT datetime '20011227T040506.789+08'::text;
-- EXPECTED:
'2001-12-27T04:05:06.789+0800'

-- TEST: horology-18
-- SQL:
SELECT datetime '20011227T040506.789-08'::text;
-- EXPECTED:
'2001-12-27T04:05:06.789-0800'

-- TEST: horology-19
-- SQL:
SELECT datetime '2001-12-27 04:05:06.789-08'::text;
-- EXPECTED:
'2001-12-27T04:05:06.789-0800'

-- TEST: horology-20
-- SQL:
SELECT datetime '2001.12.27 04:05:06.789-08'::text;
-- EXPECTED:
'2001-12-27T04:05:06.789-0800'

-- TEST: horology-21
-- SQL:
SELECT datetime '2001/12/27 04:05:06.789-08'::text;
-- EXPECTED:
'2001-12-27T04:05:06.789-0800'

-- TEST: horology-22
-- SQL:
SELECT datetime '12/27/2001 04:05:06.789-08'::text;
-- EXPECTED:
'2001-12-27T04:05:06.789-0800'

-- TEST: horology-23
-- SQL:
SELECT datetime '2001-12-27 04:05:06.789 MET DST'::text;
-- EXPECTED:
'2001-12-27T04:05:06.789+0200'

-- TEST: horology-24
-- SQL:
SELECT datetime '2001-12-27 allballs'::text;
-- EXPECTED:
'2001-12-27T00:00:00Z'

-- TEST: horology-26
-- SQL:
SELECT datetime '27/12/2001 04:05:06.789-08'::text;
-- ERROR:
Type mismatch: can not convert string\('27/12/2001 04:05:06\.789-08'\) to datetime

-- TEST: horology-30
-- SQL:
SELECT datetime 'J2452271+08'::text;
-- EXPECTED:
'2001-12-27T00:00:00+0800'

-- TEST: horology-31
-- SQL:
SELECT datetime 'J2452271-08'::text;
-- EXPECTED:
'2001-12-27T00:00:00-0800'

-- TEST: horology-32
-- SQL:
SELECT datetime 'J2452271.5+08'::text;
-- EXPECTED:
'2001-12-27T12:00:00+0800'

-- TEST: horology-33
-- SQL:
SELECT datetime 'J2452271.5-08'::text;
-- EXPECTED:
'2001-12-27T12:00:00-0800'

-- TEST: horology-34
-- SQL:
SELECT datetime 'J2452271 04:05:06+08'::text;
-- EXPECTED:
'2001-12-27T04:05:06+0800'

-- TEST: horology-35
-- SQL:
SELECT datetime 'J2452271 04:05:06-08'::text;
-- EXPECTED:
'2001-12-27T04:05:06-0800'

-- TEST: horology-36
-- SQL:
SELECT datetime 'J2452271T040506+08'::text;
-- EXPECTED:
'2001-12-27T04:05:06+0800'

-- TEST: horology-37
-- SQL:
SELECT datetime 'J2452271T040506-08'::text;
-- EXPECTED:
'2001-12-27T04:05:06-0800'

-- TEST: horology-38
-- SQL:
SELECT datetime 'J2452271T040506.789+08'::text;
-- EXPECTED:
'2001-12-27T04:05:06.789+0800'

-- TEST: horology-39
-- SQL:
SELECT datetime 'J2452271T040506.789-08'::text;
-- EXPECTED:
'2001-12-27T04:05:06.789-0800'

-- TEST: horology-41
-- SQL:
SELECT datetime '12.27.2001 04:05:06.789+08'::text;
-- EXPECTED:
'2001-12-27T04:05:06.789+0800'

-- TEST: horology-42
-- SQL:
SELECT datetime '12.27.2001 04:05:06.789-08'::text;
-- EXPECTED:
'2001-12-27T04:05:06.789-0800'

-- TEST: horology-93
-- SQL:
SELECT datetime 'J1520447'::text;
-- EXPECTED:
'-550-09-28T00:00:00Z'

-- TEST: horology-94
-- SQL:
SELECT datetime 'J0'::text;
-- EXPECTED:
'-4713-11-24T00:00:00Z'

-- TEST: horology-97
-- SQL:
SELECT datetime '1995-08-06  J J J'::text;
-- ERROR:
Type mismatch: can not convert string\('1995-08-06  J J J'\) to datetime

-- TEST: horology-98
-- SQL:
SELECT datetime 'J J 1520447'::text;
-- ERROR:
Type mismatch: can not convert string\('J J 1520447'\) to datetime

-- TEST: horology-102
-- SQL:
SELECT datetime 'Y2001M12D27H04M05S06.789+08'::text;
-- ERROR:
Type mismatch: can not convert string\('Y2001M12D27H04M05S06\.789\+08'\) to datetime

-- TEST: horology-103
-- SQL:
SELECT datetime 'Y2001M12D27H04MM05S06.789-08'::text;
-- ERROR:
Type mismatch: can not convert string\('Y2001M12D27H04MM05S06\.789-08'\) to datetime

-- TEST: horology-106
-- SQL:
SELECT datetime 'J2452271 T X03456-08'::text;
-- ERROR:
Type mismatch: can not convert string\('J2452271 T X03456-08'\) to datetime

-- TEST: horology-107
-- SQL:
SELECT datetime 'J2452271 T X03456.001e6-08'::text;
-- ERROR:
Type mismatch: can not convert string\('J2452271 T X03456\.001e6-08'\) to datetime

-- TEST: horology-110
-- SQL:
SELECT datetime '1995-08-06 epoch'::text;
-- ERROR:
Type mismatch: can not convert string\('1995-08-06 epoch'\) to datetime

-- TEST: horology-111
-- SQL:
SELECT datetime '1995-08-06 infinity'::text;
-- ERROR:
Type mismatch: can not convert string\('1995-08-06 infinity'\) to datetime

-- TEST: horology-112
-- SQL:
SELECT datetime '1995-08-06 -infinity'::text;
-- ERROR:
Type mismatch: can not convert string\('1995-08-06 -infinity'\) to datetime

-- TEST: horology-113
-- SQL:
SELECT datetime 'today infinity'::text;
-- ERROR:
Type mismatch: can not convert string\('today infinity'\) to datetime

-- TEST: horology-114
-- SQL:
SELECT datetime '-infinity infinity'::text;
-- ERROR:
Type mismatch: can not convert string\('-infinity infinity'\) to datetime

-- TEST: horology-115
-- SQL:
SELECT datetime '1995-08-06 epoch'::text;
-- ERROR:
Type mismatch: can not convert string\('1995-08-06 epoch'\) to datetime

-- TEST: horology-116
-- SQL:
SELECT datetime '1995-08-06 infinity'::text;
-- ERROR:
Type mismatch: can not convert string\('1995-08-06 infinity'\) to datetime

-- TEST: horology-117
-- SQL:
SELECT datetime '1995-08-06 -infinity'::text;
-- ERROR:
Type mismatch: can not convert string\('1995-08-06 -infinity'\) to datetime

-- TEST: horology-118
-- SQL:
SELECT datetime 'epoch 01:01:01'::text;
-- ERROR:
Type mismatch: can not convert string\('epoch 01:01:01'\) to datetime

-- TEST: horology-119
-- SQL:
SELECT datetime 'infinity 01:01:01'::text;
-- ERROR:
Type mismatch: can not convert string\('infinity 01:01:01'\) to datetime

-- TEST: horology-120
-- SQL:
SELECT datetime '-infinity 01:01:01'::text;
-- ERROR:
Type mismatch: can not convert string\('-infinity 01:01:01'\) to datetime

-- TEST: horology-121
-- SQL:
SELECT datetime 'now epoch'::text;
-- ERROR:
Type mismatch: can not convert string\('now epoch'\) to datetime

-- TEST: horology-122
-- SQL:
SELECT datetime '-infinity infinity'::text;
-- ERROR:
Type mismatch: can not convert string\('-infinity infinity'\) to datetime

-- TEST: horology-123
-- SQL:
SELECT datetime '1995-08-06 epoch'::text;
-- ERROR:
Type mismatch: can not convert string\('1995-08-06 epoch'\) to datetime

-- TEST: horology-124
-- SQL:
SELECT datetime '1995-08-06 infinity'::text;
-- ERROR:
Type mismatch: can not convert string\('1995-08-06 infinity'\) to datetime

-- TEST: horology-125
-- SQL:
SELECT datetime '1995-08-06 -infinity'::text;
-- ERROR:
Type mismatch: can not convert string\('1995-08-06 -infinity'\) to datetime

-- TEST: horology-126
-- SQL:
SELECT datetime 'epoch 01:01:01'::text;
-- ERROR:
Type mismatch: can not convert string\('epoch 01:01:01'\) to datetime

-- TEST: horology-127
-- SQL:
SELECT datetime 'infinity 01:01:01'::text;
-- ERROR:
Type mismatch: can not convert string\('infinity 01:01:01'\) to datetime

-- TEST: horology-128
-- SQL:
SELECT datetime '-infinity 01:01:01'::text;
-- ERROR:
Type mismatch: can not convert string\('-infinity 01:01:01'\) to datetime

-- TEST: horology-129
-- SQL:
SELECT datetime 'now epoch'::text;
-- ERROR:
Type mismatch: can not convert string\('now epoch'\) to datetime

-- TEST: horology-130
-- SQL:
SELECT datetime '-infinity infinity'::text;
-- ERROR:
Type mismatch: can not convert string\('-infinity infinity'\) to datetime

-- TEST: horology-316-setup
-- SQL:
CREATE TABLE temp_timestamp (id INT PRIMARY KEY, f1 DATETIME);

-- TEST: horology-320-setup
-- SQL:
INSERT INTO temp_timestamp
  SELECT id, d1 FROM timestamp_tbl
  WHERE d1 BETWEEN '13-jun-1957' AND '1-jan-1997'
   OR d1 BETWEEN '1-jan-1999' AND '1-jan-2010';

-- TEST: horology-325
-- SQL:
SELECT row_number() OVER (ORDER BY f1) AS n, f1::text AS "timestamp"
  FROM temp_timestamp
  ORDER BY n;
-- EXPECTED:
1, '1970-01-01T00:00:00Z',
2, '1996-02-28T17:32:01Z',
3, '1996-02-29T17:32:01Z',
4, '1996-03-01T17:32:01Z',
5, '1996-12-30T17:32:01Z',
6, '1996-12-31T17:32:01Z',
7, '1999-12-31T17:32:01Z',
8, '2000-01-01T17:32:01Z',
9, '2000-03-15T02:14:05-0700',
10, '2000-03-15T12:14:03+0200',
11, '2000-03-15T03:14:04-0800',
12, '2000-03-15T13:14:02+0100',
13, '2000-03-15T08:14:01-0800',
14, '2000-12-31T17:32:01Z',
15, '2001-01-01T17:32:01Z',
16, '2001-09-22T18:19:20Z'

-- TEST: horology-356-setup
-- SQL:
DROP TABLE temp_timestamp;

-- TEST: horology-362
-- SQL:
SELECT '2202020-10-05'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('2202020-10-05'\) to datetime

-- TEST: horology-366
-- SQL:
SELECT '2202020-10-05'::datetime::text;
-- ERROR:
Type mismatch: can not convert string\('2202020-10-05'\) to datetime

-- TEST: horology-371
-- SQL:
SELECT '4714-11-24 BC'::datetime::text;
-- EXPECTED:
'-4713-11-24T00:00:00Z'

-- TEST: horology-375
-- SQL:
SELECT '4714-11-24 BC'::datetime < '2020-10-05'::datetime AS t;
-- EXPECTED:
true

-- TEST: horology-376
-- SQL:
SELECT '2020-10-05'::datetime >= '4714-11-24 BC'::datetime AS t;
-- EXPECTED:
true

-- TEST: horology-378
-- SQL:
SELECT '4714-11-24 BC'::datetime < '2020-10-05'::datetime AS t;
-- EXPECTED:
true

-- TEST: horology-379
-- SQL:
SELECT '2020-10-05'::datetime >= '4714-11-24 BC'::datetime AS t;
-- EXPECTED:
true

-- TEST: horology-390
-- SQL:
SELECT count(*) FROM date_tbl
  WHERE f1 BETWEEN '1997-01-01' AND '1998-01-01';
-- EXPECTED:
3

-- TEST: horology-396
-- SQL:
SELECT count(*) FROM date_tbl
  WHERE f1 NOT BETWEEN '1997-01-01' AND '1998-01-01';
-- EXPECTED:
13

-- TEST: horology-691
-- SQL:
SELECT '2012-12-12 12:00'::datetime::text;
-- EXPECTED:
'2012-12-12T12:00:00Z'

-- TEST: horology-692
-- SQL:
SELECT '2012-12-12 12:00 America/New_York'::datetime::text;
-- EXPECTED:
'2012-12-12T12:00:00-0500'
