-- TEST-MATRIX: pgproto-1rsX1, pgproto-2rsX1, iproto-2rsX1

-- Queries and schema are copied from benchmark/tpch.

-- TEST: tpch-schema
-- SQL:
CREATE TABLE nation (
    n_nationkey INTEGER not null PRIMARY KEY,
    n_name VARCHAR(25) not null,
    n_regionkey INTEGER not null,
    n_comment VARCHAR(152)
);
CREATE TABLE region (
    r_regionkey INTEGER not null PRIMARY KEY,
    r_name VARCHAR(25) not null,
    r_comment VARCHAR(152)
);
CREATE TABLE part (
    p_partkey BIGINT not null PRIMARY KEY,
    p_name VARCHAR(55) not null,
    p_mfgr VARCHAR(25) not null,
    p_brand VARCHAR(10) not null,
    p_type VARCHAR(25) not null,
    p_size INTEGER not null,
    p_container VARCHAR(10) not null,
    p_retailprice DOUBLE not null,
    p_comment VARCHAR(23) not null
);
CREATE TABLE supplier (
    s_suppkey BIGINT not null PRIMARY KEY,
    s_name VARCHAR(25) not null,
    s_address VARCHAR(40) not null,
    s_nationkey INTEGER not null,
    s_phone VARCHAR(15) not null,
    s_acctbal DOUBLE not null,
    s_comment VARCHAR(101) not null
);
CREATE TABLE partsupp (
    ps_partkey BIGINT not null,
    ps_suppkey BIGINT not null,
    ps_availqty BIGINT not null,
    ps_supplycost DOUBLE not null,
    ps_comment VARCHAR(199) not null,
    PRIMARY KEY (ps_partkey, ps_suppkey)
);
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
INSERT INTO region VALUES
    (0, 'AFRICA', 'region 0'),
    (1, 'AMERICA', 'region 1'),
    (2, 'ASIA', 'region 2'),
    (3, 'EUROPE', 'region 3'),
    (4, 'MIDDLE EAST', 'region 4');
INSERT INTO nation VALUES
    (0, 'ALGERIA', 0, 'nation 0'),
    (1, 'ARGENTINA', 1, 'nation 1'),
    (2, 'BRAZIL', 1, 'nation 2'),
    (3, 'CANADA', 1, 'nation 3'),
    (4, 'EGYPT', 4, 'nation 4'),
    (5, 'ETHIOPIA', 0, 'nation 5'),
    (6, 'FRANCE', 3, 'nation 6'),
    (7, 'GERMANY', 3, 'nation 7'),
    (8, 'INDIA', 2, 'nation 8'),
    (9, 'INDONESIA', 2, 'nation 9'),
    (10, 'IRAN', 4, 'nation 10'),
    (11, 'IRAQ', 4, 'nation 11'),
    (12, 'JAPAN', 2, 'nation 12'),
    (13, 'JORDAN', 4, 'nation 13'),
    (14, 'KENYA', 0, 'nation 14'),
    (15, 'MOROCCO', 0, 'nation 15'),
    (16, 'MOZAMBIQUE', 0, 'nation 16'),
    (17, 'PERU', 1, 'nation 17'),
    (18, 'CHINA', 2, 'nation 18'),
    (19, 'ROMANIA', 3, 'nation 19'),
    (20, 'SAUDI ARABIA', 4, 'nation 20'),
    (21, 'VIETNAM', 2, 'nation 21'),
    (22, 'RUSSIA', 3, 'nation 22'),
    (23, 'UNITED KINGDOM', 3, 'nation 23'),
    (24, 'UNITED STATES', 1, 'nation 24');
INSERT INTO part VALUES
    (1, 'part 1', 'Manufacturer#1', 'Brand#12', 'PROMO BURNISHED COPPER', 3, 'SM CASE', 1000.0, 'part 1'),
    (2, 'part 2', 'Manufacturer#2', 'Brand#23', 'ECONOMY ANODIZED STEEL', 9, 'MED BAG', 1100.0, 'part 2'),
    (3, 'part 3', 'Manufacturer#3', 'Brand#34', 'LARGE BRUSHED BRASS', 14, 'LG BOX', 1200.0, 'part 3'),
    (4, 'part 4', 'Manufacturer#4', 'Brand#45', 'MEDIUM POLISHED TIN', 49, 'JUMBO JAR', 1300.0, 'part 4'),
    (5, 'part 5', 'Manufacturer#1', 'Brand#13', 'MEDIUM POLISHED NICKEL', 23, 'WRAP CASE', 1400.0, 'part 5'),
    (6, 'part 6', 'Manufacturer#1', 'Brand#13', 'ECONOMY ANODIZED STEEL', 23, 'SM PKG', 1500.0, 'part 6'),
    (7, 'part 7', 'Manufacturer#2', 'Brand#22', 'PROMO PLATED STEEL', 45, 'LG BAG', 1600.0, 'part 7'),
    (8, 'part 8', 'Manufacturer#2', 'Brand#22', 'STANDARD BURNISHED TIN', 7, 'MED BOX', 1700.0, 'part 8'),
    (9, 'part 9', 'Manufacturer#1', 'Brand#12', 'PROMO BURNISHED COPPER', 3, 'SM BOX', 1800.0, 'part 9');
INSERT INTO supplier VALUES
    (1, 'Supplier#000000001', 'address 1', 7, '17-555-100-0001', 250.5, 'supplier 1'),
    (2, 'Supplier#000000002', 'address 2', 6, '16-555-100-0002', 500.5, 'supplier 2'),
    (3, 'Supplier#000000003', 'address 3', 2, '12-555-100-0003', 750.5, 'supplier 3'),
    (4, 'Supplier#000000004', 'address 4', 8, '18-555-100-0004', 1000.5, 'supplier 4'),
    (5, 'Supplier#000000005', 'address 5', 12, '22-555-100-0005', 1250.5, 'supplier 5'),
    (6, 'Supplier#000000006', 'address 6', 7, '17-555-100-0006', 1500.5, 'Customer slow Complaints'),
    (7, 'Supplier#000000007', 'address 7', 3, '13-555-100-0007', 1750.5, 'supplier 7');
INSERT INTO partsupp VALUES
    (1, 1, 100, 1.5, 'partsupp 1-1'),
    (1, 2, 200, 2.25, 'partsupp 1-2'),
    (1, 6, 50, 4.0, 'partsupp 1-6'),
    (2, 1, 300, 10.5, 'partsupp 2-1'),
    (2, 3, 150, 3.0, 'partsupp 2-3'),
    (3, 2, 400, 0.75, 'partsupp 3-2'),
    (3, 6, 10, 8.0, 'partsupp 3-6'),
    (4, 1, 20, 0.5, 'partsupp 4-1'),
    (4, 4, 250, 5.0, 'partsupp 4-4'),
    (5, 1, 1, 0.25, 'partsupp 5-1'),
    (5, 5, 60, 2.5, 'partsupp 5-5'),
    (6, 3, 70, 1.25, 'partsupp 6-3'),
    (6, 7, 90, 6.5, 'partsupp 6-7'),
    (7, 1, 1000, 0.125, 'partsupp 7-1'),
    (7, 6, 30, 2.0, 'partsupp 7-6'),
    (8, 2, 80, 3.5, 'partsupp 8-2'),
    (9, 1, 100, 1.5, 'partsupp 9-1'),
    (9, 4, 40, 9.0, 'partsupp 9-4');
INSERT INTO customer VALUES
    (1, 'Customer#000000001', 'address 1', 6, '11-555-000-0001', 100.5, 'BUILDING', 'customer 1'),
    (2, 'Customer#000000002', 'address 2', 7, '12-555-000-0002', 201.0, 'AUTOMOBILE', 'customer 2'),
    (3, 'Customer#000000003', 'address 3', 2, '13-555-000-0003', 301.5, 'BUILDING', 'customer 3'),
    (4, 'Customer#000000004', 'address 4', 1, '14-555-000-0004', 402.0, 'MACHINERY', 'customer 4'),
    (5, 'Customer#000000005', 'address 5', 8, '15-555-000-0005', 502.5, 'BUILDING', 'customer 5'),
    (6, 'Customer#000000006', 'address 6', 3, '16-555-000-0006', 603.0, 'HOUSEHOLD', 'customer 6'),
    (7, 'Customer#000000007', 'address 7', 12, '17-555-000-0007', 703.5, 'FURNITURE', 'customer 7'),
    (8, 'Customer#000000008', 'address 8', 24, '18-555-000-0008', 804.0, 'BUILDING', 'customer 8'),
    (9, 'Customer#000000009', 'address 9', 12, '19-555-000-0009', 904.5, 'AUTOMOBILE', 'customer 9'),
    (10, 'Customer#000000010', 'address 10', 9, '20-555-000-0010', 1005.0, 'MACHINERY', 'customer 10');
INSERT INTO orders VALUES
    (1, 1, 'F', 40504.8828125, '1995-03-01', '2-HIGH', 'Clerk#000000007', 0, 'order 1'),
    (2, 3, 'F', 33644.53125, '1995-02-20', '3-MEDIUM', 'Clerk#000000014', 0, 'special handling requests'),
    (3, 5, 'F', 27257.03125, '1995-03-10', '4-NOT SPECIFIED', 'Clerk#000000021', 0, 'order 3'),
    (4, 8, 'F', 28883.59375, '1995-01-15', '5-LOW', 'Clerk#000000028', 0, 'order 4'),
    (5, 1, 'F', 35156.25, '1995-03-14', '1-URGENT', 'Clerk#000000035', 0, 'order 5'),
    (6, 3, 'F', 24750.0, '1995-02-28', '2-HIGH', 'Clerk#000000042', 0, 'special packing requests'),
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
    (22, 6, 'O', 12181.640625, '1998-08-01', '3-MEDIUM', 'Clerk#000000054', 0, 'order 22'),
    (23, 7, 'F', 20000.0, '1994-05-10', '1-URGENT', 'Clerk#000000061', 0, 'order 23'),
    (24, 2, 'O', 8000.0, '1996-02-01', '3-MEDIUM', 'Clerk#000000068', 0, 'order 24'),
    (25, 1, 'O', 12000.0, '1996-04-15', '5-LOW', 'Clerk#000000075', 0, 'order 25'),
    (26, 3, 'O', 30000.0, '1996-07-01', '2-HIGH', 'Clerk#000000082', 0, 'order 26'),
    (27, 4, 'F', 15000.0, '1993-11-15', '4-NOT SPECIFIED', 'Clerk#000000089', 0, 'order 27'),
    (28, 6, 'F', 9000.0, '1995-08-20', '5-LOW', 'Clerk#000000096', 0, 'order 28'),
    (29, 2, 'O', 330000.0, '1998-08-10', '1-URGENT', 'Clerk#000000003', 0, 'order 29');
INSERT INTO lineitem VALUES
    (1, 1, 2, 1, 10.0, 10000.0, 0.03125, 0.0, 'A', 'F', '1995-03-20', '1995-03-10', '1995-03-25', 'NONE', 'REG AIR', 'lineitem 1-1'),
    (1, 2, 3, 2, 5.0, 5500.0, 0.0625, 0.03125, 'R', 'F', '1995-03-25', '1995-03-15', '1995-03-30', 'TAKE BACK RETURN', 'AIR', 'lineitem 1-2'),
    (1, 3, 1, 3, 20.0, 24000.0, 0.0, 0.0625, 'A', 'F', '1995-03-10', '1995-02-28', '1995-03-15', 'DELIVER IN PERSON', 'AIR', 'lineitem 1-3'),
    (2, 3, 1, 1, 30.0, 36000.0, 0.09375, 0.03125, 'R', 'F', '1995-03-16', '1995-03-06', '1995-03-21', 'TAKE BACK RETURN', 'AIR', 'lineitem 2-1'),
    (3, 4, 2, 1, 12.0, 15600.0, 0.03125, 0.0625, 'A', 'F', '1995-03-18', '1995-03-08', '1995-03-23', 'DELIVER IN PERSON', 'FOB', 'lineitem 3-1'),
    (3, 5, 3, 2, 8.0, 11200.0, 0.0, 0.0, 'R', 'F', '1995-04-02', '1995-03-23', '1995-04-07', 'COLLECT COD', 'RAIL', 'lineitem 3-2'),
    (4, 2, 3, 1, 11.0, 12100.0, 0.0, 0.0, 'R', 'F', '1995-03-15', '1995-03-05', '1995-03-20', 'DELIVER IN PERSON', 'AIR REG', 'lineitem 4-1'),
    (4, 3, 1, 2, 14.0, 16800.0, 0.03125, 0.03125, 'A', 'F', '1995-02-01', '1995-01-22', '1995-02-06', 'NONE', 'SHIP', 'lineitem 4-2'),
    (5, 6, 1, 1, 25.0, 37500.0, 0.0625, 0.0, 'A', 'F', '1995-04-10', '1995-03-31', '1995-04-15', 'NONE', 'SHIP', 'lineitem 5-1'),
    (6, 7, 2, 1, 16.0, 25600.0, 0.0625, 0.03125, 'R', 'F', '1995-03-30', '1995-03-20', '1995-04-04', 'TAKE BACK RETURN', 'TRUCK', 'lineitem 6-1'),
    (7, 7, 2, 1, 16.0, 25600.0, 0.0625, 0.0625, 'A', 'F', '1995-03-17', '1995-03-07', '1995-03-22', 'DELIVER IN PERSON', 'MAIL', 'lineitem 7-1'),
    (7, 8, 3, 2, 4.0, 6800.0, 0.03125, 0.0, 'R', 'F', '1995-03-01', '1995-02-19', '1995-03-06', 'COLLECT COD', 'REG AIR', 'lineitem 7-2'),
    (8, 9, 4, 1, 40.0, 72000.0, 0.0, 0.03125, 'R', 'F', '1995-03-19', '1995-03-09', '1995-03-24', 'COLLECT COD', 'REG AIR', 'lineitem 8-1'),
    (8, 1, 2, 2, 2.0, 2000.0, 0.09375, 0.0, 'A', 'F', '1995-05-01', '1995-04-21', '1995-05-06', 'DELIVER IN PERSON', 'AIR', 'lineitem 8-2'),
    (9, 2, 2, 1, 3.0, 3300.0, 0.03125, 0.03125, 'A', 'F', '1995-03-21', '1995-03-11', '1995-03-26', 'NONE', 'AIR', 'lineitem 9-1'),
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
    (17, 3, 1, 3, 8.0, 9600.0, 0.0, 0.0, 'A', 'F', '1992-05-01', '1992-04-21', '1992-05-06', 'DELIVER IN PERSON', 'AIR', 'lineitem 17-3'),
    (18, 4, 2, 1, 28.0, 36400.0, 0.0625, 0.03125, 'R', 'F', '1993-11-05', '1993-10-26', '1993-11-10', 'TAKE BACK RETURN', 'RAIL', 'lineitem 18-1'),
    (18, 5, 3, 2, 24.0, 33600.0, 0.03125, 0.0, 'A', 'F', '1994-01-20', '1994-01-22', '1994-01-25', 'DELIVER IN PERSON', 'SHIP', 'lineitem 18-2'),
    (19, 6, 1, 1, 32.0, 48000.0, 0.09375, 0.0625, 'N', 'F', '1995-06-14', '1995-06-04', '1995-06-19', 'DELIVER IN PERSON', 'SHIP', 'lineitem 19-1'),
    (19, 7, 2, 2, 38.0, 60800.0, 0.0, 0.03125, 'N', 'F', '1995-06-16', '1995-06-06', '1995-06-21', 'COLLECT COD', 'TRUCK', 'lineitem 19-2'),
    (19, 8, 3, 3, 15.0, 25500.0, 0.0625, 0.0625, 'N', 'O', '1995-06-20', '1995-06-10', '1995-06-25', 'NONE', 'MAIL', 'lineitem 19-3'),
    (20, 9, 1, 1, 2.0, 3600.0, 0.03125, 0.0625, 'N', 'O', '1996-12-01', '1996-11-21', '1996-12-06', 'COLLECT COD', 'TRUCK', 'lineitem 20-1'),
    (20, 1, 2, 2, 47.0, 47000.0, 0.0625, 0.0, 'N', 'O', '1997-02-02', '1997-01-23', '1997-02-07', 'NONE', 'MAIL', 'lineitem 20-2'),
    (21, 2, 3, 1, 21.0, 23100.0, 0.0, 0.03125, 'N', 'O', '1998-08-30', '1998-08-20', '1998-09-04', 'NONE', 'MAIL', 'lineitem 21-1'),
    (21, 3, 1, 2, 13.0, 15600.0, 0.09375, 0.0, 'N', 'O', '1998-09-02', '1998-08-23', '1998-09-07', 'TAKE BACK RETURN', 'REG AIR', 'lineitem 21-2'),
    (21, 4, 2, 3, 44.0, 57200.0, 0.03125, 0.0625, 'N', 'O', '1998-09-03', '1998-08-24', '1998-09-08', 'DELIVER IN PERSON', 'AIR', 'lineitem 21-3'),
    (22, 5, 3, 1, 9.0, 12600.0, 0.0625, 0.03125, 'N', 'O', '1998-11-15', '1998-11-05', '1998-11-20', 'TAKE BACK RETURN', 'REG AIR', 'lineitem 22-1'),
    (23, 3, 5, 1, 10.0, 12000.0, 0.0625, 0.0, 'A', 'F', '1994-05-20', '1994-05-25', '1994-05-30', 'NONE', 'MAIL', 'lineitem 23-1'),
    (23, 6, 4, 2, 5.0, 7500.0, 0.0, 0.0, 'R', 'F', '1994-05-22', '1994-05-20', '1994-06-01', 'COLLECT COD', 'MAIL', 'lineitem 23-2'),
    (24, 1, 2, 1, 8.0, 8000.0, 0.03125, 0.0, 'N', 'O', '1996-03-01', '1996-03-05', '1996-03-10', 'NONE', 'TRUCK', 'lineitem 24-1'),
    (25, 7, 6, 1, 12.0, 12000.0, 0.0625, 0.03125, 'N', 'O', '1996-05-01', '1996-05-10', '1996-05-12', 'NONE', 'RAIL', 'lineitem 25-1'),
    (26, 6, 7, 1, 10.0, 15000.0, 0.0, 0.0, 'N', 'O', '1996-07-10', '1996-07-15', '1996-07-20', 'NONE', 'FOB', 'lineitem 26-1'),
    (26, 2, 3, 2, 15.0, 15000.0, 0.0625, 0.0, 'N', 'O', '1996-07-12', '1996-07-15', '1996-07-22', 'NONE', 'SHIP', 'lineitem 26-2'),
    (27, 5, 3, 1, 6.0, 8400.0, 0.03125, 0.0, 'R', 'F', '1993-12-01', '1993-12-10', '1993-12-15', 'NONE', 'SHIP', 'lineitem 27-1'),
    (27, 8, 2, 2, 4.0, 6800.0, 0.0, 0.0, 'A', 'F', '1993-12-05', '1993-12-10', '1994-01-02', 'NONE', 'MAIL', 'lineitem 27-2'),
    (28, 1, 2, 1, 4.0, 4000.0, 0.0, 0.0, 'R', 'F', '1995-09-05', '1995-09-10', '1995-09-15', 'NONE', 'TRUCK', 'lineitem 28-1'),
    (28, 3, 1, 2, 3.0, 3600.0, 0.0625, 0.0, 'A', 'F', '1995-09-20', '1995-09-25', '1995-09-30', 'NONE', 'TRUCK', 'lineitem 28-2'),
    (28, 7, 6, 3, 2.0, 2000.0, 0.0, 0.0, 'A', 'F', '1995-10-01', '1995-10-05', '1995-10-10', 'NONE', 'TRUCK', 'lineitem 28-3'),
    (29, 1, 2, 1, 100.0, 100000.0, 0.0, 0.0, 'N', 'O', '1998-09-05', '1998-09-10', '1998-09-15', 'NONE', 'SHIP', 'lineitem 29-1'),
    (29, 2, 3, 2, 150.0, 150000.0, 0.0, 0.0, 'N', 'O', '1998-09-06', '1998-09-10', '1998-09-15', 'NONE', 'SHIP', 'lineitem 29-2'),
    (29, 4, 4, 3, 80.0, 80000.0, 0.0, 0.0, 'N', 'O', '1998-09-07', '1998-09-10', '1998-09-15', 'NONE', 'SHIP', 'lineitem 29-3');

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
'A', 'F', 294.0, 398400.0, 387893.75, 393289.55078125, 15.473684210526315, 20968.42105263158, 0.029605263157894735, 19,
'N', 'F', 70.0, 108800.0, 104300.0, 108918.75, 35.0, 54400.0, 0.046875, 2,
'N', 'O', 143.0, 164800.0, 156756.25, 159541.796875, 15.88888888888889, 18311.11111111111, 0.04513888888888889, 9,
'R', 'F', 327.0, 453200.0, 437287.5, 447143.5546875, 18.166666666666668, 25177.777777777777, 0.041666666666666664, 18

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

-- TEST: tpch-q5
-- SQL:
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    customer
    join orders on true
    join lineitem on true
    join supplier on true
    join nation on true
    join region on true
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'ASIA'
  and o_orderdate >= datetime '1994-01-01'
  and o_orderdate < datetime '1995-01-01'
group by
    n_name
order by
    revenue desc;
-- EXPECTED:
'INDIA', 72000.0,
'JAPAN', 11250.0

-- TEST: tpch-q7
-- SQL:
select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            to_char(l_shipdate, '%Y') as l_year,
            l_extendedprice * (1 - l_discount) as volume
        from
            supplier
            join lineitem on true
            join orders on true
            join customer on true
            join nation n1 on true
            join nation n2 on true
        where
                s_suppkey = l_suppkey
          and o_orderkey = l_orderkey
          and c_custkey = o_custkey
          and s_nationkey = n1.n_nationkey
          and c_nationkey = n2.n_nationkey
          and (
                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
            )
          and l_shipdate between datetime '1995-01-01' and datetime '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;
-- EXPECTED:
'FRANCE', 'GERMANY', '1995', 65000.0,
'FRANCE', 'GERMANY', '1996', 7750.0,
'GERMANY', 'FRANCE', '1995', 69656.25,
'GERMANY', 'FRANCE', '1996', 11250.0

-- TEST: tpch-q8
-- SQL:
select
    o_year,
    sum(case
            when nation = 'BRAZIL' then volume
            else 0
        end) / sum(volume) as mkt_share
from
    (
        select
            to_char(o_orderdate, '%Y') as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part
            join supplier on true
            join lineitem on true
            join orders on true
            join customer on true
            join nation n1 on true
            join nation n2 on true
            join region on true
        where
                p_partkey = l_partkey
          and s_suppkey = l_suppkey
          and l_orderkey = o_orderkey
          and o_custkey = c_custkey
          and c_nationkey = n1.n_nationkey
          and n1.n_regionkey = r_regionkey
          and r_name = 'AMERICA'
          and s_nationkey = n2.n_nationkey
          and o_orderdate between datetime '1995-01-01' and datetime '1996-12-31'
          and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year;
-- EXPECTED:
'1995', 0.7910112359550562,
'1996', 0.4838709677419355

-- TEST: tpch-q10
-- SQL:
select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer
    join orders on true
    join lineitem on true
    join nation on true
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= datetime '1993-10-01'
  and o_orderdate < datetime '1994-01-01'
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
limit 20;
-- EXPECTED:
6, 'Customer#000000006', 34125.0, 603.0, 'CANADA', 'address 6', '16-555-000-0006', 'customer 6',
4, 'Customer#000000004', 8137.5, 402.0, 'ARGENTINA', 'address 4', '14-555-000-0004', 'customer 4'

-- TEST: tpch-q11
-- SQL:
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp
    join supplier on true
    join nation on true
where
    ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'GERMANY'
group by
    ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
    select
    sum(ps_supplycost * ps_availqty) * 0.0001
    from
    partsupp
    join supplier on true
    join nation on true
    where
    ps_suppkey = s_suppkey
                  and s_nationkey = n_nationkey
                  and n_name = 'GERMANY'
    )
order by
    value desc;
-- EXPECTED:
2, 3150.0,
1, 350.0,
7, 185.0,
9, 150.0,
3, 80.0,
4, 10.0

-- TEST: tpch-q12
-- SQL:
select
    l_shipmode,
    sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
    sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
from
    lineitem
        join
    orders
    on
            l_orderkey = o_orderkey
where
        l_shipmode in ('MAIL', 'SHIP')
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= datetime '1994-01-01'
  and l_receiptdate < datetime '1995-01-01'
group by
    l_shipmode
order by
    l_shipmode;
-- EXPECTED:
'MAIL', 1, 1,
'SHIP', 0, 1

-- TEST: tpch-q13
-- SQL:
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey) as c_count
        from
            customer left outer join orders on
                        c_custkey = o_custkey
                    and not (o_comment like '%special%requests%')
        group by
            c_custkey
    ) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;
-- EXPECTED:
4, 3,
3, 2,
2, 2,
0, 2,
5, 1

-- TEST: tpch-q14
-- SQL:
select
            100.00 * sum(case
                             when p_type like 'PROMO%'
                                 then l_extendedprice * (1 - l_discount)
                             else 0
            end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    lineitem
    join part on true
where
        l_partkey = p_partkey
  and l_shipdate >= datetime '1995-09-01'
  and l_shipdate < datetime '1995-10-01';
-- EXPECTED:
54.23728813559322

-- TEST: tpch-q16
-- SQL:
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp
    join part on true
where
        p_partkey = ps_partkey
  and p_brand <> 'Brand#45'
  and not (p_type like 'MEDIUM POLISHED%')
  and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
  and ps_suppkey not in (
    select
        s_suppkey
    from
        supplier
    where
            s_comment like '%Customer%Complaints%'
)
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size;
-- EXPECTED:
'Brand#12', 'PROMO BURNISHED COPPER', 3, 3,
'Brand#13', 'ECONOMY ANODIZED STEEL', 23, 2,
'Brand#23', 'ECONOMY ANODIZED STEEL', 9, 2,
'Brand#22', 'PROMO PLATED STEEL', 45, 1,
'Brand#34', 'LARGE BRUSHED BRASS', 14, 1

-- TEST: tpch-q18
-- SQL:
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer
    join orders on true
    join lineitem on true
where
        o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 300
    )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate
limit 100;
-- EXPECTED:
'Customer#000000002', 2, 29, Datetime('1998-08-10T00:00:00Z'), 330000.0, 330.0

-- TEST: tpch-q19
-- SQL:
select
    sum(l_extendedprice* (1 - l_discount)) as revenue
from
    lineitem
    join part on true
where
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#12'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 1 and l_quantity <= 1 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#23'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 10 and l_quantity <= 10 + 10
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#34'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 20 and l_quantity <= 20 + 10
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        );
-- EXPECTED:
37912.5
