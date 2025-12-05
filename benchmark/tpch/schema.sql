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