-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Queries and schema are copied from benchmark/tpch.

-- TEST: tpch-schema
-- SQL:
CREATE TABLE customer (
    c_custkey BIGINT not null PRIMARY KEY,
    c_name VARCHAR(25) not null,
    c_address VARCHAR(40) not null,
    c_nationkey INTEGER not null,
    c_phone VARCHAR(15) not null,
    c_acctbal DOUBLE not null,
    c_mktsegment VARCHAR(10) not null,
    c_comment VARCHAR(117) not null
);
CREATE TABLE orders (
    o_orderkey BIGINT not null PRIMARY KEY,
    o_custkey BIGINT not null,
    o_orderstatus VARCHAR(1) not null,
    o_totalprice DOUBLE not null,
    o_orderdate DATETIME not null,
    o_orderpriority VARCHAR(15) not null,
    o_clerk VARCHAR(15) not null,
    o_shippriority INTEGER not null,
    o_comment VARCHAR(79) not null
);
CREATE TABLE lineitem (
    l_orderkey BIGINT not null,
    l_partkey BIGINT not null,
    l_suppkey BIGINT not null,
    l_linenumber BIGINT not null,
    l_quantity DOUBLE not null,
    l_extendedprice DOUBLE not null,
    l_discount DOUBLE not null,
    l_tax DOUBLE not null,
    l_returnflag VARCHAR(1) not null,
    l_linestatus VARCHAR(1) not null,
    l_shipdate DATETIME not null,
    l_commitdate DATETIME not null,
    l_receiptdate DATETIME not null,
    l_shipinstruct VARCHAR(25) not null,
    l_shipmode VARCHAR(10) not null,
    l_comment VARCHAR(44) not null,
    PRIMARY KEY (l_orderkey, l_linenumber)
);
INSERT INTO customer VALUES
    (1, 'Customer#000000001', 'address 1', 1, '11-555-000-0001', 100.5, 'BUILDING', 'customer 1'),
    (2, 'Customer#000000002', 'address 2', 2, '12-555-000-0002', 201.0, 'AUTOMOBILE', 'customer 2'),
    (3, 'Customer#000000003', 'address 3', 3, '13-555-000-0003', 301.5, 'BUILDING', 'customer 3'),
    (4, 'Customer#000000004', 'address 4', 4, '14-555-000-0004', 402.0, 'MACHINERY', 'customer 4'),
    (5, 'Customer#000000005', 'address 5', 0, '15-555-000-0005', 502.5, 'BUILDING', 'customer 5'),
    (6, 'Customer#000000006', 'address 6', 1, '16-555-000-0006', 603.0, 'HOUSEHOLD', 'customer 6'),
    (7, 'Customer#000000007', 'address 7', 2, '17-555-000-0007', 703.5, 'FURNITURE', 'customer 7'),
    (8, 'Customer#000000008', 'address 8', 3, '18-555-000-0008', 804.0, 'BUILDING', 'customer 8');
INSERT INTO orders VALUES
    (1, 1, 'F', 40504.8828125, '1995-03-01', '2-HIGH', 'Clerk#000000007', 0, 'order 1'),
    (2, 3, 'F', 33644.53125, '1995-02-20', '3-MEDIUM', 'Clerk#000000014', 0, 'order 2'),
    (3, 5, 'F', 27257.03125, '1995-03-10', '4-NOT SPECIFIED', 'Clerk#000000021', 0, 'order 3'),
    (4, 8, 'F', 28883.59375, '1995-01-15', '5-LOW', 'Clerk#000000028', 0, 'order 4'),
    (5, 1, 'F', 35156.25, '1995-03-14', '1-URGENT', 'Clerk#000000035', 0, 'order 5'),
    (6, 3, 'F', 24750.0, '1995-02-28', '2-HIGH', 'Clerk#000000042', 0, 'order 6'),
    (7, 8, 'F', 32087.5, '1995-01-05', '3-MEDIUM', 'Clerk#000000049', 0, 'order 7'),
    (8, 5, 'F', 76062.5, '1994-12-30', '4-NOT SPECIFIED', 'Clerk#000000056', 0, 'order 8'),
    (9, 8, 'F', 3296.77734375, '1995-03-12', '5-LOW', 'Clerk#000000063', 0, 'order 9'),
    (10, 1, 'F', 36311.71875, '1995-02-14', '1-URGENT', 'Clerk#000000070', 0, 'order 10'),
    (11, 3, 'F', 74109.375, '1995-01-20', '2-HIGH', 'Clerk#000000077', 0, 'order 11'),
    (12, 5, 'F', 10757.8125, '1995-03-02', '3-MEDIUM', 'Clerk#000000084', 0, 'order 12'),
    (13, 8, 'F', 32093.359375, '1995-02-01', '4-NOT SPECIFIED', 'Clerk#000000091', 0, 'order 13'),
    (14, 2, 'F', 65000.0, '1995-03-01', '5-LOW', 'Clerk#000000098', 0, 'order 14'),
    (15, 1, 'F', 70000.0, '1995-03-15', '1-URGENT', 'Clerk#000000005', 0, 'order 15'),
    (16, 5, 'F', 75000.0, '1995-04-01', '2-HIGH', 'Clerk#000000012', 0, 'order 16'),
    (17, 4, 'F', 64713.8671875, '1992-02-10', '3-MEDIUM', 'Clerk#000000019', 0, 'order 17'),
    (18, 6, 'F', 67741.40625, '1993-10-01', '4-NOT SPECIFIED', 'Clerk#000000026', 0, 'order 18'),
    (19, 7, 'P', 134319.140625, '1995-06-01', '5-LOW', 'Clerk#000000033', 0, 'order 19'),
    (20, 2, 'O', 47767.96875, '1996-11-11', '1-URGENT', 'Clerk#000000040', 0, 'order 20'),
    (21, 4, 'O', 96835.15625, '1998-07-20', '2-HIGH', 'Clerk#000000047', 0, 'order 21'),
    (22, 6, 'O', 12181.640625, '1998-08-01', '3-MEDIUM', 'Clerk#000000054', 0, 'order 22');
INSERT INTO lineitem VALUES
    (1, 1, 2, 1, 10.0, 10000.0, 0.03125, 0.0, 'A', 'F', '1995-03-20', '1995-03-10', '1995-03-25', 'NONE', 'REG AIR', 'lineitem 1-1'),
    (1, 2, 3, 2, 5.0, 5500.0, 0.0625, 0.03125, 'R', 'F', '1995-03-25', '1995-03-15', '1995-03-30', 'TAKE BACK RETURN', 'AIR', 'lineitem 1-2'),
    (1, 3, 1, 3, 20.0, 24000.0, 0.0, 0.0625, 'A', 'F', '1995-03-10', '1995-02-28', '1995-03-15', 'DELIVER IN PERSON', 'FOB', 'lineitem 1-3'),
    (2, 3, 1, 1, 30.0, 36000.0, 0.09375, 0.03125, 'R', 'F', '1995-03-16', '1995-03-06', '1995-03-21', 'TAKE BACK RETURN', 'AIR', 'lineitem 2-1'),
    (3, 4, 2, 1, 12.0, 15600.0, 0.03125, 0.0625, 'A', 'F', '1995-03-18', '1995-03-08', '1995-03-23', 'DELIVER IN PERSON', 'FOB', 'lineitem 3-1'),
    (3, 5, 3, 2, 8.0, 11200.0, 0.0, 0.0, 'R', 'F', '1995-04-02', '1995-03-23', '1995-04-07', 'COLLECT COD', 'RAIL', 'lineitem 3-2'),
    (4, 2, 3, 1, 11.0, 12100.0, 0.0, 0.0, 'R', 'F', '1995-03-15', '1995-03-05', '1995-03-20', 'COLLECT COD', 'RAIL', 'lineitem 4-1'),
    (4, 3, 1, 2, 14.0, 16800.0, 0.03125, 0.03125, 'A', 'F', '1995-02-01', '1995-01-22', '1995-02-06', 'NONE', 'SHIP', 'lineitem 4-2'),
    (5, 6, 1, 1, 25.0, 37500.0, 0.0625, 0.0, 'A', 'F', '1995-04-10', '1995-03-31', '1995-04-15', 'NONE', 'SHIP', 'lineitem 5-1'),
    (6, 7, 2, 1, 16.0, 25600.0, 0.0625, 0.03125, 'R', 'F', '1995-03-30', '1995-03-20', '1995-04-04', 'TAKE BACK RETURN', 'TRUCK', 'lineitem 6-1'),
    (7, 7, 2, 1, 16.0, 25600.0, 0.0625, 0.0625, 'A', 'F', '1995-03-17', '1995-03-07', '1995-03-22', 'DELIVER IN PERSON', 'MAIL', 'lineitem 7-1'),
    (7, 8, 3, 2, 4.0, 6800.0, 0.03125, 0.0, 'R', 'F', '1995-03-01', '1995-02-19', '1995-03-06', 'COLLECT COD', 'REG AIR', 'lineitem 7-2'),
    (8, 9, 1, 1, 40.0, 72000.0, 0.0, 0.03125, 'R', 'F', '1995-03-19', '1995-03-09', '1995-03-24', 'COLLECT COD', 'REG AIR', 'lineitem 8-1'),
    (8, 1, 2, 2, 2.0, 2000.0, 0.09375, 0.0, 'A', 'F', '1995-05-01', '1995-04-21', '1995-05-06', 'NONE', 'AIR', 'lineitem 8-2'),
    (9, 2, 3, 1, 3.0, 3300.0, 0.03125, 0.03125, 'A', 'F', '1995-03-21', '1995-03-11', '1995-03-26', 'NONE', 'AIR', 'lineitem 9-1'),
    (10, 5, 3, 1, 18.0, 25200.0, 0.09375, 0.0625, 'R', 'F', '1995-03-22', '1995-03-12', '1995-03-27', 'TAKE BACK RETURN', 'FOB', 'lineitem 10-1'),
    (10, 6, 1, 2, 7.0, 10500.0, 0.0, 0.03125, 'A', 'F', '1995-03-23', '1995-03-13', '1995-03-28', 'DELIVER IN PERSON', 'RAIL', 'lineitem 10-2'),
    (10, 4, 2, 3, 1.0, 1300.0, 0.0625, 0.0, 'R', 'F', '1995-03-24', '1995-03-14', '1995-03-29', 'COLLECT COD', 'SHIP', 'lineitem 10-3'),
    (11, 8, 3, 1, 45.0, 76500.0, 0.03125, 0.0, 'A', 'F', '1995-06-01', '1995-05-22', '1995-06-06', 'DELIVER IN PERSON', 'RAIL', 'lineitem 11-1'),
    (12, 9, 1, 1, 6.0, 10800.0, 0.0625, 0.0625, 'R', 'F', '1995-03-26', '1995-03-16', '1995-03-31', 'COLLECT COD', 'SHIP', 'lineitem 12-1'),
    (13, 1, 2, 1, 22.0, 22000.0, 0.0, 0.0, 'A', 'F', '1995-03-27', '1995-03-17', '1995-04-01', 'NONE', 'TRUCK', 'lineitem 13-1'),
    (13, 3, 1, 2, 9.0, 10800.0, 0.09375, 0.03125, 'R', 'F', '1995-03-28', '1995-03-18', '1995-04-02', 'TAKE BACK RETURN', 'MAIL', 'lineitem 13-2'),
    (14, 4, 2, 1, 50.0, 65000.0, 0.0, 0.0, 'R', 'F', '1995-03-20', '1995-03-10', '1995-03-25', 'TAKE BACK RETURN', 'MAIL', 'lineitem 14-1'),
    (15, 5, 3, 1, 50.0, 70000.0, 0.0, 0.0, 'A', 'F', '1995-03-20', '1995-03-10', '1995-03-25', 'DELIVER IN PERSON', 'REG AIR', 'lineitem 15-1'),
    (16, 6, 1, 1, 50.0, 75000.0, 0.0, 0.0, 'R', 'F', '1995-04-05', '1995-03-26', '1995-04-10', 'COLLECT COD', 'AIR', 'lineitem 16-1'),
    (17, 1, 2, 1, 17.0, 17000.0, 0.03125, 0.03125, 'A', 'F', '1992-03-13', '1992-03-03', '1992-03-18', 'NONE', 'FOB', 'lineitem 17-1'),
    (17, 2, 3, 2, 36.0, 39600.0, 0.09375, 0.0625, 'R', 'F', '1992-04-12', '1992-04-02', '1992-04-17', 'TAKE BACK RETURN', 'RAIL', 'lineitem 17-2'),
    (17, 3, 1, 3, 8.0, 9600.0, 0.0, 0.0, 'A', 'F', '1992-05-01', '1992-04-21', '1992-05-06', 'DELIVER IN PERSON', 'SHIP', 'lineitem 17-3'),
    (18, 4, 2, 1, 28.0, 36400.0, 0.0625, 0.03125, 'R', 'F', '1993-11-05', '1993-10-26', '1993-11-10', 'TAKE BACK RETURN', 'RAIL', 'lineitem 18-1'),
    (18, 5, 3, 2, 24.0, 33600.0, 0.03125, 0.0, 'A', 'F', '1994-01-20', '1994-01-10', '1994-01-25', 'DELIVER IN PERSON', 'SHIP', 'lineitem 18-2'),
    (19, 6, 1, 1, 32.0, 48000.0, 0.09375, 0.0625, 'N', 'F', '1995-06-14', '1995-06-04', '1995-06-19', 'DELIVER IN PERSON', 'SHIP', 'lineitem 19-1'),
    (19, 7, 2, 2, 38.0, 60800.0, 0.0, 0.03125, 'N', 'F', '1995-06-16', '1995-06-06', '1995-06-21', 'COLLECT COD', 'TRUCK', 'lineitem 19-2'),
    (19, 8, 3, 3, 15.0, 25500.0, 0.0625, 0.0625, 'N', 'O', '1995-06-20', '1995-06-10', '1995-06-25', 'NONE', 'MAIL', 'lineitem 19-3'),
    (20, 9, 1, 1, 2.0, 3600.0, 0.03125, 0.0625, 'N', 'O', '1996-12-01', '1996-11-21', '1996-12-06', 'COLLECT COD', 'TRUCK', 'lineitem 20-1'),
    (20, 1, 2, 2, 47.0, 47000.0, 0.0625, 0.0, 'N', 'O', '1997-02-02', '1997-01-23', '1997-02-07', 'NONE', 'MAIL', 'lineitem 20-2'),
    (21, 2, 3, 1, 21.0, 23100.0, 0.0, 0.03125, 'N', 'O', '1998-08-30', '1998-08-20', '1998-09-04', 'NONE', 'MAIL', 'lineitem 21-1'),
    (21, 3, 1, 2, 13.0, 15600.0, 0.09375, 0.0, 'N', 'O', '1998-09-02', '1998-08-23', '1998-09-07', 'TAKE BACK RETURN', 'REG AIR', 'lineitem 21-2'),
    (21, 4, 2, 3, 44.0, 57200.0, 0.03125, 0.0625, 'N', 'O', '1998-09-03', '1998-08-24', '1998-09-08', 'DELIVER IN PERSON', 'AIR', 'lineitem 21-3'),
    (22, 5, 3, 1, 9.0, 12600.0, 0.0625, 0.03125, 'N', 'O', '1998-11-15', '1998-11-05', '1998-11-20', 'TAKE BACK RETURN', 'REG AIR', 'lineitem 22-1');

-- TEST: tpch-q1
-- SQL:
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= to_date('1998-09-02', '%Y-%m-%d')
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;
-- EXPECTED:
'A', 'F', 275.0, 374000.0, 364468.75, 369864.55078125, 18.333333333333332, 24933.333333333332, 0.029166666666666667, 15,
'N', 'F', 70.0, 108800.0, 104300.0, 108918.75, 35.0, 54400.0, 0.046875, 2,
'N', 'O', 98.0, 114800.0, 108693.75, 111127.734375, 19.6, 22960.0, 0.05, 5,
'R', 'F', 312.0, 433300.0, 417650.0, 427506.0546875, 20.8, 28886.666666666668, 0.04791666666666667, 15

-- TEST: tpch-q3
-- SQL:
SELECT
  l_orderkey,
  SUM(l_extendedprice * (1 - l_discount)) AS revenue,
  o_orderdate,
  o_shippriority
FROM
  customer
  JOIN orders ON TRUE
  JOIN lineitem ON TRUE
WHERE
  c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < datetime '1995-03-15'
  AND l_shipdate > datetime '1995-03-15'
GROUP BY
  l_orderkey,
  o_orderdate,
  o_shippriority
ORDER BY
  revenue DESC,
  o_orderdate
LIMIT
  10;
-- EXPECTED:
11, 74109.375, Datetime('1995-01-20T00:00:00Z'), 0,
8, 73812.5, Datetime('1994-12-30T00:00:00Z'), 0,
5, 35156.25, Datetime('1995-03-14T00:00:00Z'), 0,
10, 34556.25, Datetime('1995-02-14T00:00:00Z'), 0,
2, 32625.0, Datetime('1995-02-20T00:00:00Z'), 0,
13, 31787.5, Datetime('1995-02-01T00:00:00Z'), 0,
3, 26312.5, Datetime('1995-03-10T00:00:00Z'), 0,
7, 24000.0, Datetime('1995-01-05T00:00:00Z'), 0,
6, 24000.0, Datetime('1995-02-28T00:00:00Z'), 0,
1, 14843.75, Datetime('1995-03-01T00:00:00Z'), 0
