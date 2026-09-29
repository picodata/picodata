-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Cases of PostgreSQL's src/test/regress/sql/horology.sql where Picodata
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

-- TEST: horology-363
-- SQL:
SELECT '2202020-10-05'::datetime > '2020-10-05'::datetime AS t;
-- ERROR:
Type mismatch: can not convert string\('2202020-10-05'\) to datetime

-- TEST: horology-364
-- SQL:
SELECT '2020-10-05'::datetime > '2202020-10-05'::datetime AS f;
-- ERROR:
Type mismatch: can not convert string\('2202020-10-05'\) to datetime

-- TEST: horology-367
-- SQL:
SELECT '2202020-10-05'::datetime > '2020-10-05'::datetime AS t;
-- ERROR:
Type mismatch: can not convert string\('2202020-10-05'\) to datetime

-- TEST: horology-368
-- SQL:
SELECT '2020-10-05'::datetime > '2202020-10-05'::datetime AS f;
-- ERROR:
Type mismatch: can not convert string\('2202020-10-05'\) to datetime
