-- TEST-MATRIX: pgproto-1rsX1

-- Queries and schema are copied from benchmark/tpch.

-- TEST: tpch-explain-schema
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

-- TEST: tpch-explain-q1
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  l_returnflag::text,
  l_linestatus::text,
  sum_qty::double,
  sum_base_price::double,
  sum_disc_price::double,
  sum_charge::double,
  avg_qty::double,
  avg_price::double,
  avg_disc::double,
  count_order::int
)
  order by (l_returnflag::text, l_linestatus::text)
    scan
      projection (
        gr_expr_1::text -> l_returnflag,
        gr_expr_2::text -> l_linestatus,
        sum(sum_1::double::double)::double -> sum_qty,
        sum(sum_2::double::double)::double -> sum_base_price,
        sum(sum_3::double::double)::double -> sum_disc_price,
        sum(sum_4::double::double)::double -> sum_charge,
        sum(sum_1::double::double)::double / sum(avg_5::int::int)::int::int -> avg_qty,
        sum(sum_2::double::double)::double / sum(avg_6::int::int)::int::int -> avg_price,
        sum(avg_7::double::double)::double / sum(avg_8::int::int)::int::int -> avg_disc,
        sum(count_9::int::int)::int::int -> count_order
      )
        group by (gr_expr_1::text, gr_expr_2::text)
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              lineitem.l_returnflag::text -> gr_expr_1,
              lineitem.l_linestatus::text -> gr_expr_2,
              sum(
                (
                  lineitem.l_extendedprice::double * (1::int - lineitem.l_discount::double) * (1::int + lineitem.l_tax::double)
                )::double
              )::double -> sum_4,
              sum(lineitem.l_extendedprice::double::double)::double -> sum_2,
              sum(lineitem.l_quantity::double::double)::double -> sum_1,
              count(lineitem.l_discount::double::double)::int -> avg_8,
              count(lineitem.l_quantity::double::double)::int -> avg_5,
              count(*)::int -> count_9,
              sum(lineitem.l_discount::double::double)::double -> avg_7,
              count(lineitem.l_extendedprice::double::double)::int -> avg_6,
              sum(
                (
                  lineitem.l_extendedprice::double * (1::int - lineitem.l_discount::double)
                )::double
              )::double -> sum_3
            )
              group by (
                lineitem.l_returnflag::text,
                lineitem.l_linestatus::text
              )
                selection (
                  lineitem.l_shipdate::datetime <= to_date('1998-09-02'::text, '%Y-%m-%d'::text)::datetime
                )
                  scan lineitem
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "lineitem"."l_returnflag" as "gr_expr_1",
  "lineitem"."l_linestatus" as "gr_expr_2",
  sum (
    (
      "lineitem"."l_extendedprice" * (1::int - "lineitem"."l_discount") * (1::int + "lineitem"."l_tax")
    )::double
  ) as "sum_4",
  sum ("lineitem"."l_extendedprice"::double) as "sum_2",
  sum ("lineitem"."l_quantity"::double) as "sum_1",
  count ("lineitem"."l_discount"::double) as "avg_8",
  count ("lineitem"."l_quantity"::double) as "avg_5",
  count (*) as "count_9",
  sum ("lineitem"."l_discount"::double) as "avg_7",
  count ("lineitem"."l_extendedprice"::double) as "avg_6",
  sum (
    (
      "lineitem"."l_extendedprice" * (1::int - "lineitem"."l_discount")
    )::double
  ) as "sum_3"
FROM
  "lineitem"
WHERE
  "lineitem"."l_shipdate" <= "to_date" ('1998-09-02'::text, '%Y-%m-%d'::text)
GROUP BY
  "lineitem"."l_returnflag",
  "lineitem"."l_linestatus"
''
plan:
    [0] SCAN TABLE lineitem (~983040 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "l_returnflag",
  "l_linestatus",
  "sum_qty",
  "sum_base_price",
  "sum_disc_price",
  "sum_charge",
  "avg_qty",
  "avg_price",
  "avg_disc",
  "count_order"
FROM
  (
    SELECT
      "COL_0" as "l_returnflag",
      "COL_1" as "l_linestatus",
      sum ("COL_4"::double) as "sum_qty",
      sum ("COL_3"::double) as "sum_base_price",
      sum ("COL_10"::double) as "sum_disc_price",
      sum ("COL_2"::double) as "sum_charge",
      sum ("COL_4"::double) / sum ("COL_6"::int)::int as "avg_qty",
      sum ("COL_3"::double) / sum ("COL_9"::int)::int as "avg_price",
      sum ("COL_8"::double) / sum ("COL_5"::int)::int as "avg_disc",
      sum ("COL_7"::int)::int as "count_order"
    FROM
      "_tmp_12504932716845563831_0136"
    GROUP BY
      "COL_0",
      "COL_1"
  )
ORDER BY
  "l_returnflag",
  "l_linestatus"
''
plan:
    [0] SCAN TABLE _tmp_12504932716845563831_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: tpch-explain-q3
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 10
  projection (
    l_orderkey::int,
    revenue::double,
    o_orderdate::datetime,
    o_shippriority::int
  )
    order by (revenue::double desc, o_orderdate::datetime)
      scan
        projection (
          gr_expr_1::int -> l_orderkey,
          sum(sum_1::double::double)::double -> revenue,
          gr_expr_2::datetime -> o_orderdate,
          gr_expr_3::int -> o_shippriority
        )
          group by (
            gr_expr_1::int,
            gr_expr_2::datetime,
            gr_expr_3::int
          )
            motion [policy: full, program: ReshardIfNeeded]
              projection (
                lineitem.l_orderkey::int -> gr_expr_1,
                orders.o_orderdate::datetime -> gr_expr_2,
                orders.o_shippriority::int -> gr_expr_3,
                sum(
                  (
                    lineitem.l_extendedprice::double * (1::int - lineitem.l_discount::double)
                  )::double
                )::double -> sum_1
              )
                group by (
                  lineitem.l_orderkey::int,
                  orders.o_orderdate::datetime,
                  orders.o_shippriority::int
                )
                  selection (
                    (
                      customer.c_mktsegment::text = 'BUILDING'::text
                      and customer.c_custkey::int = orders.o_custkey::int
                      and lineitem.l_orderkey::int = orders.o_orderkey::int
                      and orders.o_orderdate::datetime < '1995-03-15 0:00:00.0 +00:00:00'::datetime
                      and lineitem.l_shipdate::datetime > '1995-03-15 0:00:00.0 +00:00:00'::datetime
                    )
                  )
                    join on (true::bool)
                      join on (true::bool)
                        scan customer
                        motion [policy: segment([ref(o_custkey)]), program: ReshardIfNeeded]
                          projection (
                            orders.o_orderkey::int -> o_orderkey,
                            orders.bucket_id::int -> bucket_id,
                            orders.o_custkey::int -> o_custkey,
                            orders.o_orderstatus::text -> o_orderstatus,
                            orders.o_totalprice::double -> o_totalprice,
                            orders.o_orderdate::datetime -> o_orderdate,
                            orders.o_orderpriority::text -> o_orderpriority,
                            orders.o_clerk::text -> o_clerk,
                            orders.o_shippriority::int -> o_shippriority,
                            orders.o_comment::text -> o_comment
                          )
                            scan orders
                      motion [policy: full, program: ReshardIfNeeded]
                        projection (
                          lineitem.l_orderkey::int -> l_orderkey,
                          lineitem.l_partkey::int -> l_partkey,
                          lineitem.l_suppkey::int -> l_suppkey,
                          lineitem.l_linenumber::int -> l_linenumber,
                          lineitem.bucket_id::int -> bucket_id,
                          lineitem.l_quantity::double -> l_quantity,
                          lineitem.l_extendedprice::double -> l_extendedprice,
                          lineitem.l_discount::double -> l_discount,
                          lineitem.l_tax::double -> l_tax,
                          lineitem.l_returnflag::text -> l_returnflag,
                          lineitem.l_linestatus::text -> l_linestatus,
                          lineitem.l_shipdate::datetime -> l_shipdate,
                          lineitem.l_commitdate::datetime -> l_commitdate,
                          lineitem.l_receiptdate::datetime -> l_receiptdate,
                          lineitem.l_shipinstruct::text -> l_shipinstruct,
                          lineitem.l_shipmode::text -> l_shipmode,
                          lineitem.l_comment::text -> l_comment
                        )
                          scan lineitem
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "orders"."o_orderkey",
  "orders"."bucket_id",
  "orders"."o_custkey",
  "orders"."o_orderstatus",
  "orders"."o_totalprice",
  "orders"."o_orderdate",
  "orders"."o_orderpriority",
  "orders"."o_clerk",
  "orders"."o_shippriority",
  "orders"."o_comment"
FROM
  "orders"
''
plan:
    [0] SCAN TABLE orders (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "lineitem"."l_orderkey",
  "lineitem"."l_partkey",
  "lineitem"."l_suppkey",
  "lineitem"."l_linenumber",
  "lineitem"."bucket_id",
  "lineitem"."l_quantity",
  "lineitem"."l_extendedprice",
  "lineitem"."l_discount",
  "lineitem"."l_tax",
  "lineitem"."l_returnflag",
  "lineitem"."l_linestatus",
  "lineitem"."l_shipdate",
  "lineitem"."l_commitdate",
  "lineitem"."l_receiptdate",
  "lineitem"."l_shipinstruct",
  "lineitem"."l_shipmode",
  "lineitem"."l_comment"
FROM
  "lineitem"
''
plan:
    [0] SCAN TABLE lineitem (~1048576 rows)
''
buckets <= [1-3000]
''
╭─────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "lineitem"."COL_0" as "gr_expr_1",
  "orders"."COL_5" as "gr_expr_2",
  "orders"."COL_8" as "gr_expr_3",
  sum (
    (
      "lineitem"."COL_6" * (1::int - "lineitem"."COL_7")
    )::double
  ) as "sum_1"
FROM
  "customer"
  INNER JOIN "_tmp_5667582682556364960_0136" as "orders" ON true::bool
  INNER JOIN "_tmp_5667582682556364960_1136" as "lineitem" ON true::bool
WHERE
  "customer"."c_mktsegment" = 'BUILDING'::text
  and "customer"."c_custkey" = "orders"."COL_2"
  and "lineitem"."COL_0" = "orders"."COL_0"
  and "orders"."COL_5" < '1995-03-15 0:00:00.0 +00:00:00'::datetime
  and "lineitem"."COL_11" > '1995-03-15 0:00:00.0 +00:00:00'::datetime
GROUP BY
  "lineitem"."COL_0",
  "orders"."COL_5",
  "orders"."COL_8"
''
plan:
    [0] SCAN TABLE _tmp_5667582682556364960_0136 AS orders (~983040 rows)
        [0] SEARCH TABLE customer USING PRIMARY KEY (c_custkey=?) (~1 row)
            [0] SCAN TABLE _tmp_5667582682556364960_1136 AS lineitem (~983040 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "l_orderkey",
  "revenue",
  "o_orderdate",
  "o_shippriority"
FROM
  (
    SELECT
      "COL_0" as "l_orderkey",
      sum ("COL_3"::double) as "revenue",
      "COL_1" as "o_orderdate",
      "COL_2" as "o_shippriority"
    FROM
      "_tmp_5796134220943901769_2136"
    GROUP BY
      "COL_0",
      "COL_1",
      "COL_2"
  )
ORDER BY
  "revenue" DESC,
  "o_orderdate"
LIMIT
  10
''
plan:
    [0] SCAN TABLE _tmp_5796134220943901769_2136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: tpch-explain-q5
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (n_name::text, revenue::double)
  order by (revenue::double desc)
    scan
      projection (
        gr_expr_1::text -> n_name,
        sum(sum_1::double::double)::double -> revenue
      )
        group by (gr_expr_1::text)
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              nation.n_name::text -> gr_expr_1,
              sum(
                (
                  lineitem.l_extendedprice::double * (1::int - lineitem.l_discount::double)
                )::double
              )::double -> sum_1
            )
              group by (nation.n_name::text)
                selection (
                  (
                    customer.c_custkey::int = orders.o_custkey::int
                    and lineitem.l_orderkey::int = orders.o_orderkey::int
                    and lineitem.l_suppkey::int = supplier.s_suppkey::int
                    and customer.c_nationkey::int = supplier.s_nationkey::int
                    and supplier.s_nationkey::int = nation.n_nationkey::int
                    and nation.n_regionkey::int = region.r_regionkey::int
                    and region.r_name::text = 'ASIA'::text
                    and orders.o_orderdate::datetime >= '1994-01-01 0:00:00.0 +00:00:00'::datetime
                    and orders.o_orderdate::datetime < '1995-01-01 0:00:00.0 +00:00:00'::datetime
                  )
                )
                  join on (true::bool)
                    join on (true::bool)
                      join on (true::bool)
                        join on (true::bool)
                          join on (true::bool)
                            scan customer
                            motion [policy: segment([ref(o_custkey)]), program: ReshardIfNeeded]
                              projection (
                                orders.o_orderkey::int -> o_orderkey,
                                orders.bucket_id::int -> bucket_id,
                                orders.o_custkey::int -> o_custkey,
                                orders.o_orderstatus::text -> o_orderstatus,
                                orders.o_totalprice::double -> o_totalprice,
                                orders.o_orderdate::datetime -> o_orderdate,
                                orders.o_orderpriority::text -> o_orderpriority,
                                orders.o_clerk::text -> o_clerk,
                                orders.o_shippriority::int -> o_shippriority,
                                orders.o_comment::text -> o_comment
                              )
                                scan orders
                          motion [policy: full, program: ReshardIfNeeded]
                            projection (
                              lineitem.l_orderkey::int -> l_orderkey,
                              lineitem.l_partkey::int -> l_partkey,
                              lineitem.l_suppkey::int -> l_suppkey,
                              lineitem.l_linenumber::int -> l_linenumber,
                              lineitem.bucket_id::int -> bucket_id,
                              lineitem.l_quantity::double -> l_quantity,
                              lineitem.l_extendedprice::double -> l_extendedprice,
                              lineitem.l_discount::double -> l_discount,
                              lineitem.l_tax::double -> l_tax,
                              lineitem.l_returnflag::text -> l_returnflag,
                              lineitem.l_linestatus::text -> l_linestatus,
                              lineitem.l_shipdate::datetime -> l_shipdate,
                              lineitem.l_commitdate::datetime -> l_commitdate,
                              lineitem.l_receiptdate::datetime -> l_receiptdate,
                              lineitem.l_shipinstruct::text -> l_shipinstruct,
                              lineitem.l_shipmode::text -> l_shipmode,
                              lineitem.l_comment::text -> l_comment
                            )
                              scan lineitem
                        motion [policy: full, program: ReshardIfNeeded]
                          projection (
                            supplier.s_suppkey::int -> s_suppkey,
                            supplier.bucket_id::int -> bucket_id,
                            supplier.s_name::text -> s_name,
                            supplier.s_address::text -> s_address,
                            supplier.s_nationkey::int -> s_nationkey,
                            supplier.s_phone::text -> s_phone,
                            supplier.s_acctbal::double -> s_acctbal,
                            supplier.s_comment::text -> s_comment
                          )
                            scan supplier
                      motion [policy: full, program: ReshardIfNeeded]
                        projection (
                          nation.n_nationkey::int -> n_nationkey,
                          nation.bucket_id::int -> bucket_id,
                          nation.n_name::text -> n_name,
                          nation.n_regionkey::int -> n_regionkey,
                          nation.n_comment::text -> n_comment
                        )
                          scan nation
                    motion [policy: full, program: ReshardIfNeeded]
                      projection (
                        region.r_regionkey::int -> r_regionkey,
                        region.bucket_id::int -> bucket_id,
                        region.r_name::text -> r_name,
                        region.r_comment::text -> r_comment
                      )
                        scan region
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "orders"."o_orderkey",
  "orders"."bucket_id",
  "orders"."o_custkey",
  "orders"."o_orderstatus",
  "orders"."o_totalprice",
  "orders"."o_orderdate",
  "orders"."o_orderpriority",
  "orders"."o_clerk",
  "orders"."o_shippriority",
  "orders"."o_comment"
FROM
  "orders"
''
plan:
    [0] SCAN TABLE orders (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "lineitem"."l_orderkey",
  "lineitem"."l_partkey",
  "lineitem"."l_suppkey",
  "lineitem"."l_linenumber",
  "lineitem"."bucket_id",
  "lineitem"."l_quantity",
  "lineitem"."l_extendedprice",
  "lineitem"."l_discount",
  "lineitem"."l_tax",
  "lineitem"."l_returnflag",
  "lineitem"."l_linestatus",
  "lineitem"."l_shipdate",
  "lineitem"."l_commitdate",
  "lineitem"."l_receiptdate",
  "lineitem"."l_shipinstruct",
  "lineitem"."l_shipmode",
  "lineitem"."l_comment"
FROM
  "lineitem"
''
plan:
    [0] SCAN TABLE lineitem (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 3. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "supplier"."s_suppkey",
  "supplier"."bucket_id",
  "supplier"."s_name",
  "supplier"."s_address",
  "supplier"."s_nationkey",
  "supplier"."s_phone",
  "supplier"."s_acctbal",
  "supplier"."s_comment"
FROM
  "supplier"
''
plan:
    [0] SCAN TABLE supplier (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 4. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "nation"."n_nationkey",
  "nation"."bucket_id",
  "nation"."n_name",
  "nation"."n_regionkey",
  "nation"."n_comment"
FROM
  "nation"
''
plan:
    [0] SCAN TABLE nation (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 5. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "region"."r_regionkey",
  "region"."bucket_id",
  "region"."r_name",
  "region"."r_comment"
FROM
  "region"
''
plan:
    [0] SCAN TABLE region (~1048576 rows)
''
buckets <= [1-3000]
''
╭─────────────────────────────────╮
│ 6. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "nation"."COL_2" as "gr_expr_1",
  sum (
    (
      "lineitem"."COL_6" * (1::int - "lineitem"."COL_7")
    )::double
  ) as "sum_1"
FROM
  "customer"
  INNER JOIN "_tmp_16147029196218747141_0136" as "orders" ON true::bool
  INNER JOIN "_tmp_16147029196218747141_1136" as "lineitem" ON true::bool
  INNER JOIN "_tmp_16147029196218747141_2136" as "supplier" ON true::bool
  INNER JOIN "_tmp_16147029196218747141_3136" as "nation" ON true::bool
  INNER JOIN "_tmp_16147029196218747141_4136" as "region" ON true::bool
WHERE
  "customer"."c_custkey" = "orders"."COL_2"
  and "lineitem"."COL_0" = "orders"."COL_0"
  and "lineitem"."COL_2" = "supplier"."COL_0"
  and "customer"."c_nationkey" = "supplier"."COL_4"
  and "supplier"."COL_4" = "nation"."COL_0"
  and "nation"."COL_3" = "region"."COL_0"
  and "region"."COL_2" = 'ASIA'::text
  and "orders"."COL_5" >= '1994-01-01 0:00:00.0 +00:00:00'::datetime
  and "orders"."COL_5" < '1995-01-01 0:00:00.0 +00:00:00'::datetime
GROUP BY
  "nation"."COL_2"
''
plan:
    [0] SCAN TABLE _tmp_16147029196218747141_0136 AS orders (~917504 rows)
        [0] SEARCH TABLE customer USING PRIMARY KEY (c_custkey=?) (~1 row)
            [0] SEARCH TABLE _tmp_16147029196218747141_1136 AS lineitem 
                 USING EPHEMERAL INDEX 
                 (COL_0=?) 
                 (~20 rows)
                [0] SEARCH TABLE _tmp_16147029196218747141_2136 AS supplier 
                     USING EPHEMERAL INDEX 
                     (COL_4=? AND COL_0=?) 
                     (~20 rows)
                    [0] SEARCH TABLE _tmp_16147029196218747141_3136 AS nation 
                         USING EPHEMERAL INDEX 
                         (COL_0=?) 
                         (~20 rows)
                        [0] SEARCH TABLE _tmp_16147029196218747141_4136 AS region 
                             USING EPHEMERAL INDEX 
                             (COL_2=? AND COL_0=?) 
                             (~20 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 7. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "n_name",
  "revenue"
FROM
  (
    SELECT
      "COL_0" as "n_name",
      sum ("COL_1"::double) as "revenue"
    FROM
      "_tmp_7369492615622028742_5136"
    GROUP BY
      "COL_0"
  )
ORDER BY
  "revenue" DESC
''
plan:
    [0] SCAN TABLE _tmp_7369492615622028742_5136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: tpch-explain-q7
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  supp_nation::text,
  cust_nation::text,
  l_year::text,
  revenue::double
)
  order by (
    supp_nation::text,
    cust_nation::text,
    l_year::text
  )
    scan
      projection (
        gr_expr_1::text -> supp_nation,
        gr_expr_2::text -> cust_nation,
        gr_expr_3::text -> l_year,
        sum(sum_1::double::double)::double -> revenue
      )
        group by (
          gr_expr_1::text,
          gr_expr_2::text,
          gr_expr_3::text
        )
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              shipping.supp_nation::text -> gr_expr_1,
              shipping.cust_nation::text -> gr_expr_2,
              shipping.l_year::text -> gr_expr_3,
              sum(shipping.volume::double::double)::double -> sum_1
            )
              group by (
                shipping.supp_nation::text,
                shipping.cust_nation::text,
                shipping.l_year::text
              )
                scan shipping
                  projection (
                    n1.n_name::text -> supp_nation,
                    n2.n_name::text -> cust_nation,
                    to_char(
                      lineitem.l_shipdate::datetime::datetime,
                      '%Y'::text
                    )::text -> l_year,
                    lineitem.l_extendedprice::double * (1::int - lineitem.l_discount::double) -> volume
                  )
                    selection (
                      (
                        supplier.s_suppkey::int = lineitem.l_suppkey::int
                        and orders.o_orderkey::int = lineitem.l_orderkey::int
                        and customer.c_custkey::int = orders.o_custkey::int
                        and supplier.s_nationkey::int = n1.n_nationkey::int
                        and customer.c_nationkey::int = n2.n_nationkey::int
                        and (
                          (
                            n1.n_name::text = 'FRANCE'::text
                            and n2.n_name::text = 'GERMANY'::text
                          ) or (
                            n1.n_name::text = 'GERMANY'::text
                            and n2.n_name::text = 'FRANCE'::text
                          )
                        )
                        and lineitem.l_shipdate::datetime >= '1995-01-01 0:00:00.0 +00:00:00'::datetime
                        and lineitem.l_shipdate::datetime <= '1996-12-31 0:00:00.0 +00:00:00'::datetime
                      )
                    )
                      join on (true::bool)
                        join on (true::bool)
                          join on (true::bool)
                            join on (true::bool)
                              join on (true::bool)
                                scan supplier
                                motion [policy: segment([ref(l_suppkey)]), program: ReshardIfNeeded]
                                  projection (
                                    lineitem.l_orderkey::int -> l_orderkey,
                                    lineitem.l_partkey::int -> l_partkey,
                                    lineitem.l_suppkey::int -> l_suppkey,
                                    lineitem.l_linenumber::int -> l_linenumber,
                                    lineitem.bucket_id::int -> bucket_id,
                                    lineitem.l_quantity::double -> l_quantity,
                                    lineitem.l_extendedprice::double -> l_extendedprice,
                                    lineitem.l_discount::double -> l_discount,
                                    lineitem.l_tax::double -> l_tax,
                                    lineitem.l_returnflag::text -> l_returnflag,
                                    lineitem.l_linestatus::text -> l_linestatus,
                                    lineitem.l_shipdate::datetime -> l_shipdate,
                                    lineitem.l_commitdate::datetime -> l_commitdate,
                                    lineitem.l_receiptdate::datetime -> l_receiptdate,
                                    lineitem.l_shipinstruct::text -> l_shipinstruct,
                                    lineitem.l_shipmode::text -> l_shipmode,
                                    lineitem.l_comment::text -> l_comment
                                  )
                                    scan lineitem
                              motion [policy: full, program: ReshardIfNeeded]
                                projection (
                                  orders.o_orderkey::int -> o_orderkey,
                                  orders.bucket_id::int -> bucket_id,
                                  orders.o_custkey::int -> o_custkey,
                                  orders.o_orderstatus::text -> o_orderstatus,
                                  orders.o_totalprice::double -> o_totalprice,
                                  orders.o_orderdate::datetime -> o_orderdate,
                                  orders.o_orderpriority::text -> o_orderpriority,
                                  orders.o_clerk::text -> o_clerk,
                                  orders.o_shippriority::int -> o_shippriority,
                                  orders.o_comment::text -> o_comment
                                )
                                  scan orders
                            motion [policy: full, program: ReshardIfNeeded]
                              projection (
                                customer.c_custkey::int -> c_custkey,
                                customer.bucket_id::int -> bucket_id,
                                customer.c_name::text -> c_name,
                                customer.c_address::text -> c_address,
                                customer.c_nationkey::int -> c_nationkey,
                                customer.c_phone::text -> c_phone,
                                customer.c_acctbal::double -> c_acctbal,
                                customer.c_mktsegment::text -> c_mktsegment,
                                customer.c_comment::text -> c_comment
                              )
                                scan customer
                          motion [policy: full, program: ReshardIfNeeded]
                            projection (
                              n1.n_nationkey::int -> n_nationkey,
                              n1.bucket_id::int -> bucket_id,
                              n1.n_name::text -> n_name,
                              n1.n_regionkey::int -> n_regionkey,
                              n1.n_comment::text -> n_comment
                            )
                              scan nation -> n1
                        motion [policy: full, program: ReshardIfNeeded]
                          projection (
                            n2.n_nationkey::int -> n_nationkey,
                            n2.bucket_id::int -> bucket_id,
                            n2.n_name::text -> n_name,
                            n2.n_regionkey::int -> n_regionkey,
                            n2.n_comment::text -> n_comment
                          )
                            scan nation -> n2
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "lineitem"."l_orderkey",
  "lineitem"."l_partkey",
  "lineitem"."l_suppkey",
  "lineitem"."l_linenumber",
  "lineitem"."bucket_id",
  "lineitem"."l_quantity",
  "lineitem"."l_extendedprice",
  "lineitem"."l_discount",
  "lineitem"."l_tax",
  "lineitem"."l_returnflag",
  "lineitem"."l_linestatus",
  "lineitem"."l_shipdate",
  "lineitem"."l_commitdate",
  "lineitem"."l_receiptdate",
  "lineitem"."l_shipinstruct",
  "lineitem"."l_shipmode",
  "lineitem"."l_comment"
FROM
  "lineitem"
''
plan:
    [0] SCAN TABLE lineitem (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "orders"."o_orderkey",
  "orders"."bucket_id",
  "orders"."o_custkey",
  "orders"."o_orderstatus",
  "orders"."o_totalprice",
  "orders"."o_orderdate",
  "orders"."o_orderpriority",
  "orders"."o_clerk",
  "orders"."o_shippriority",
  "orders"."o_comment"
FROM
  "orders"
''
plan:
    [0] SCAN TABLE orders (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 3. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "customer"."c_custkey",
  "customer"."bucket_id",
  "customer"."c_name",
  "customer"."c_address",
  "customer"."c_nationkey",
  "customer"."c_phone",
  "customer"."c_acctbal",
  "customer"."c_mktsegment",
  "customer"."c_comment"
FROM
  "customer"
''
plan:
    [0] SCAN TABLE customer (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 4. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "n1"."n_nationkey",
  "n1"."bucket_id",
  "n1"."n_name",
  "n1"."n_regionkey",
  "n1"."n_comment"
FROM
  "nation" as "n1"
''
plan:
    [0] SCAN TABLE nation AS n1 (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 5. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "n2"."n_nationkey",
  "n2"."bucket_id",
  "n2"."n_name",
  "n2"."n_regionkey",
  "n2"."n_comment"
FROM
  "nation" as "n2"
''
plan:
    [0] SCAN TABLE nation AS n2 (~1048576 rows)
''
buckets <= [1-3000]
''
╭─────────────────────────────────╮
│ 6. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "shipping"."supp_nation" as "gr_expr_1",
  "shipping"."cust_nation" as "gr_expr_2",
  "shipping"."l_year" as "gr_expr_3",
  sum ("shipping"."volume"::double) as "sum_1"
FROM
  (
    SELECT
      "n1"."COL_2" as "supp_nation",
      "n2"."COL_2" as "cust_nation",
      "to_char" ("lineitem"."COL_11"::datetime, '%Y'::text) as "l_year",
      "lineitem"."COL_6" * (1::int - "lineitem"."COL_7") as "volume"
    FROM
      "supplier"
      INNER JOIN "_tmp_8287534967795779098_0136" as "lineitem" ON true::bool
      INNER JOIN "_tmp_8287534967795779098_1136" as "orders" ON true::bool
      INNER JOIN "_tmp_8287534967795779098_2136" as "customer" ON true::bool
      INNER JOIN "_tmp_8287534967795779098_3136" as "n1" ON true::bool
      INNER JOIN "_tmp_8287534967795779098_4136" as "n2" ON true::bool
    WHERE
      "supplier"."s_suppkey" = "lineitem"."COL_2"
      and "orders"."COL_0" = "lineitem"."COL_0"
      and "customer"."COL_0" = "orders"."COL_2"
      and "supplier"."s_nationkey" = "n1"."COL_0"
      and "customer"."COL_4" = "n2"."COL_0"
      and (
        "n1"."COL_2" = 'FRANCE'::text
        and "n2"."COL_2" = 'GERMANY'::text
        or "n1"."COL_2" = 'GERMANY'::text
        and "n2"."COL_2" = 'FRANCE'::text
      )
      and "lineitem"."COL_11" >= '1995-01-01 0:00:00.0 +00:00:00'::datetime
      and "lineitem"."COL_11" <= '1996-12-31 0:00:00.0 +00:00:00'::datetime
  ) as "shipping"
GROUP BY
  "shipping"."supp_nation",
  "shipping"."cust_nation",
  "shipping"."l_year"
''
plan:
    [0] SCAN TABLE _tmp_8287534967795779098_0136 AS lineitem (~917504 rows)
        [0] SEARCH TABLE supplier USING PRIMARY KEY (s_suppkey=?) (~1 row)
            [0] SEARCH TABLE _tmp_8287534967795779098_1136 AS orders 
                 USING EPHEMERAL INDEX 
                 (COL_0=?) 
                 (~20 rows)
                [0] SEARCH TABLE _tmp_8287534967795779098_2136 AS customer 
                     USING EPHEMERAL INDEX 
                     (COL_0=?) 
                     (~20 rows)
                    [0] SEARCH TABLE _tmp_8287534967795779098_3136 AS n1 
                         USING EPHEMERAL INDEX 
                         (COL_0=?) 
                         (~20 rows)
                        [0] SEARCH TABLE _tmp_8287534967795779098_4136 AS n2 
                             USING EPHEMERAL INDEX 
                             (COL_0=?) 
                             (~20 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 7. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "supp_nation",
  "cust_nation",
  "l_year",
  "revenue"
FROM
  (
    SELECT
      "COL_0" as "supp_nation",
      "COL_1" as "cust_nation",
      "COL_2" as "l_year",
      sum ("COL_3"::double) as "revenue"
    FROM
      "_tmp_12566993161641042020_5136"
    GROUP BY
      "COL_0",
      "COL_1",
      "COL_2"
  )
ORDER BY
  "supp_nation",
  "cust_nation",
  "l_year"
''
plan:
    [0] SCAN TABLE _tmp_12566993161641042020_5136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: tpch-explain-q8
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (o_year::text, mkt_share::double)
  order by (o_year::text)
    scan
      projection (
        gr_expr_1::text -> o_year,
        sum(sum_1::double::double)::double / sum(sum_2::double::double)::double -> mkt_share
      )
        group by (gr_expr_1::text)
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              all_nations.o_year::text -> gr_expr_1,
              sum(
                case 
                  when all_nations.nation::text = 'BRAZIL'::text then all_nations.volume::double
                  else 0::int
                end::double
              )::double -> sum_1,
              sum(all_nations.volume::double::double)::double -> sum_2
            )
              group by (all_nations.o_year::text)
                scan all_nations
                  projection (
                    to_char(
                      orders.o_orderdate::datetime::datetime,
                      '%Y'::text
                    )::text -> o_year,
                    lineitem.l_extendedprice::double * (1::int - lineitem.l_discount::double) -> volume,
                    n2.n_name::text -> nation
                  )
                    selection (
                      (
                        part.p_partkey::int = lineitem.l_partkey::int
                        and supplier.s_suppkey::int = lineitem.l_suppkey::int
                        and lineitem.l_orderkey::int = orders.o_orderkey::int
                        and orders.o_custkey::int = customer.c_custkey::int
                        and customer.c_nationkey::int = n1.n_nationkey::int
                        and n1.n_regionkey::int = region.r_regionkey::int
                        and region.r_name::text = 'AMERICA'::text
                        and supplier.s_nationkey::int = n2.n_nationkey::int
                        and orders.o_orderdate::datetime >= '1995-01-01 0:00:00.0 +00:00:00'::datetime
                        and orders.o_orderdate::datetime <= '1996-12-31 0:00:00.0 +00:00:00'::datetime
                        and part.p_type::text = 'ECONOMY ANODIZED STEEL'::text
                      )
                    )
                      join on (true::bool)
                        join on (true::bool)
                          join on (true::bool)
                            join on (true::bool)
                              join on (true::bool)
                                join on (true::bool)
                                  join on (true::bool)
                                    scan part
                                    motion [policy: full, program: ReshardIfNeeded]
                                      projection (
                                        supplier.s_suppkey::int -> s_suppkey,
                                        supplier.bucket_id::int -> bucket_id,
                                        supplier.s_name::text -> s_name,
                                        supplier.s_address::text -> s_address,
                                        supplier.s_nationkey::int -> s_nationkey,
                                        supplier.s_phone::text -> s_phone,
                                        supplier.s_acctbal::double -> s_acctbal,
                                        supplier.s_comment::text -> s_comment
                                      )
                                        scan supplier
                                  motion [policy: segment([ref(l_partkey)]), program: ReshardIfNeeded]
                                    projection (
                                      lineitem.l_orderkey::int -> l_orderkey,
                                      lineitem.l_partkey::int -> l_partkey,
                                      lineitem.l_suppkey::int -> l_suppkey,
                                      lineitem.l_linenumber::int -> l_linenumber,
                                      lineitem.bucket_id::int -> bucket_id,
                                      lineitem.l_quantity::double -> l_quantity,
                                      lineitem.l_extendedprice::double -> l_extendedprice,
                                      lineitem.l_discount::double -> l_discount,
                                      lineitem.l_tax::double -> l_tax,
                                      lineitem.l_returnflag::text -> l_returnflag,
                                      lineitem.l_linestatus::text -> l_linestatus,
                                      lineitem.l_shipdate::datetime -> l_shipdate,
                                      lineitem.l_commitdate::datetime -> l_commitdate,
                                      lineitem.l_receiptdate::datetime -> l_receiptdate,
                                      lineitem.l_shipinstruct::text -> l_shipinstruct,
                                      lineitem.l_shipmode::text -> l_shipmode,
                                      lineitem.l_comment::text -> l_comment
                                    )
                                      scan lineitem
                                motion [policy: full, program: ReshardIfNeeded]
                                  projection (
                                    orders.o_orderkey::int -> o_orderkey,
                                    orders.bucket_id::int -> bucket_id,
                                    orders.o_custkey::int -> o_custkey,
                                    orders.o_orderstatus::text -> o_orderstatus,
                                    orders.o_totalprice::double -> o_totalprice,
                                    orders.o_orderdate::datetime -> o_orderdate,
                                    orders.o_orderpriority::text -> o_orderpriority,
                                    orders.o_clerk::text -> o_clerk,
                                    orders.o_shippriority::int -> o_shippriority,
                                    orders.o_comment::text -> o_comment
                                  )
                                    scan orders
                              motion [policy: full, program: ReshardIfNeeded]
                                projection (
                                  customer.c_custkey::int -> c_custkey,
                                  customer.bucket_id::int -> bucket_id,
                                  customer.c_name::text -> c_name,
                                  customer.c_address::text -> c_address,
                                  customer.c_nationkey::int -> c_nationkey,
                                  customer.c_phone::text -> c_phone,
                                  customer.c_acctbal::double -> c_acctbal,
                                  customer.c_mktsegment::text -> c_mktsegment,
                                  customer.c_comment::text -> c_comment
                                )
                                  scan customer
                            motion [policy: full, program: ReshardIfNeeded]
                              projection (
                                n1.n_nationkey::int -> n_nationkey,
                                n1.bucket_id::int -> bucket_id,
                                n1.n_name::text -> n_name,
                                n1.n_regionkey::int -> n_regionkey,
                                n1.n_comment::text -> n_comment
                              )
                                scan nation -> n1
                          motion [policy: full, program: ReshardIfNeeded]
                            projection (
                              n2.n_nationkey::int -> n_nationkey,
                              n2.bucket_id::int -> bucket_id,
                              n2.n_name::text -> n_name,
                              n2.n_regionkey::int -> n_regionkey,
                              n2.n_comment::text -> n_comment
                            )
                              scan nation -> n2
                        motion [policy: full, program: ReshardIfNeeded]
                          projection (
                            region.r_regionkey::int -> r_regionkey,
                            region.bucket_id::int -> bucket_id,
                            region.r_name::text -> r_name,
                            region.r_comment::text -> r_comment
                          )
                            scan region
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "supplier"."s_suppkey",
  "supplier"."bucket_id",
  "supplier"."s_name",
  "supplier"."s_address",
  "supplier"."s_nationkey",
  "supplier"."s_phone",
  "supplier"."s_acctbal",
  "supplier"."s_comment"
FROM
  "supplier"
''
plan:
    [0] SCAN TABLE supplier (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "lineitem"."l_orderkey",
  "lineitem"."l_partkey",
  "lineitem"."l_suppkey",
  "lineitem"."l_linenumber",
  "lineitem"."bucket_id",
  "lineitem"."l_quantity",
  "lineitem"."l_extendedprice",
  "lineitem"."l_discount",
  "lineitem"."l_tax",
  "lineitem"."l_returnflag",
  "lineitem"."l_linestatus",
  "lineitem"."l_shipdate",
  "lineitem"."l_commitdate",
  "lineitem"."l_receiptdate",
  "lineitem"."l_shipinstruct",
  "lineitem"."l_shipmode",
  "lineitem"."l_comment"
FROM
  "lineitem"
''
plan:
    [0] SCAN TABLE lineitem (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 3. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "orders"."o_orderkey",
  "orders"."bucket_id",
  "orders"."o_custkey",
  "orders"."o_orderstatus",
  "orders"."o_totalprice",
  "orders"."o_orderdate",
  "orders"."o_orderpriority",
  "orders"."o_clerk",
  "orders"."o_shippriority",
  "orders"."o_comment"
FROM
  "orders"
''
plan:
    [0] SCAN TABLE orders (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 4. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "customer"."c_custkey",
  "customer"."bucket_id",
  "customer"."c_name",
  "customer"."c_address",
  "customer"."c_nationkey",
  "customer"."c_phone",
  "customer"."c_acctbal",
  "customer"."c_mktsegment",
  "customer"."c_comment"
FROM
  "customer"
''
plan:
    [0] SCAN TABLE customer (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 5. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "n1"."n_nationkey",
  "n1"."bucket_id",
  "n1"."n_name",
  "n1"."n_regionkey",
  "n1"."n_comment"
FROM
  "nation" as "n1"
''
plan:
    [0] SCAN TABLE nation AS n1 (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 6. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "n2"."n_nationkey",
  "n2"."bucket_id",
  "n2"."n_name",
  "n2"."n_regionkey",
  "n2"."n_comment"
FROM
  "nation" as "n2"
''
plan:
    [0] SCAN TABLE nation AS n2 (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 7. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "region"."r_regionkey",
  "region"."bucket_id",
  "region"."r_name",
  "region"."r_comment"
FROM
  "region"
''
plan:
    [0] SCAN TABLE region (~1048576 rows)
''
buckets <= [1-3000]
''
╭─────────────────────────────────╮
│ 8. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "all_nations"."o_year" as "gr_expr_1",
  sum (
    CASE
      WHEN "all_nations"."nation" = 'BRAZIL'::text THEN "all_nations"."volume"
      ELSE 0::int
    END::double
  ) as "sum_1",
  sum ("all_nations"."volume"::double) as "sum_2"
FROM
  (
    SELECT
      "to_char" ("orders"."COL_5"::datetime, '%Y'::text) as "o_year",
      "lineitem"."COL_6" * (1::int - "lineitem"."COL_7") as "volume",
      "n2"."COL_2" as "nation"
    FROM
      "part"
      INNER JOIN "_tmp_16404978828759991003_0136" as "supplier" ON true::bool
      INNER JOIN "_tmp_16404978828759991003_1136" as "lineitem" ON true::bool
      INNER JOIN "_tmp_16404978828759991003_2136" as "orders" ON true::bool
      INNER JOIN "_tmp_16404978828759991003_3136" as "customer" ON true::bool
      INNER JOIN "_tmp_16404978828759991003_4136" as "n1" ON true::bool
      INNER JOIN "_tmp_16404978828759991003_5136" as "n2" ON true::bool
      INNER JOIN "_tmp_16404978828759991003_6136" as "region" ON true::bool
    WHERE
      "part"."p_partkey" = "lineitem"."COL_1"
      and "supplier"."COL_0" = "lineitem"."COL_2"
      and "lineitem"."COL_0" = "orders"."COL_0"
      and "orders"."COL_2" = "customer"."COL_0"
      and "customer"."COL_4" = "n1"."COL_0"
      and "n1"."COL_3" = "region"."COL_0"
      and "region"."COL_2" = 'AMERICA'::text
      and "supplier"."COL_4" = "n2"."COL_0"
      and "orders"."COL_5" >= '1995-01-01 0:00:00.0 +00:00:00'::datetime
      and "orders"."COL_5" <= '1996-12-31 0:00:00.0 +00:00:00'::datetime
      and "part"."p_type" = 'ECONOMY ANODIZED STEEL'::text
  ) as "all_nations"
GROUP BY
  "all_nations"."o_year"
''
plan:
    [0] SCAN TABLE _tmp_16404978828759991003_1136 AS lineitem (~1048576 rows)
        [0] SEARCH TABLE part USING PRIMARY KEY (p_partkey=?) (~1 row)
            [0] SEARCH TABLE _tmp_16404978828759991003_0136 AS supplier 
                 USING EPHEMERAL INDEX 
                 (COL_0=?) 
                 (~20 rows)
                [0] SEARCH TABLE _tmp_16404978828759991003_2136 AS orders 
                     USING EPHEMERAL INDEX 
                     (COL_0=?) 
                     (~20 rows)
                    [0] SEARCH TABLE _tmp_16404978828759991003_3136 AS customer 
                         USING EPHEMERAL INDEX 
                         (COL_0=?) 
                         (~20 rows)
                        [0] SEARCH TABLE _tmp_16404978828759991003_4136 AS n1 
                             USING EPHEMERAL INDEX 
                             (COL_0=?) 
                             (~20 rows)
                            [0] SEARCH TABLE _tmp_16404978828759991003_5136 AS n2 
                                 USING EPHEMERAL INDEX 
                                 (COL_0=?) 
                                 (~20 rows)
                                [0] SEARCH TABLE _tmp_16404978828759991003_6136 AS region 
                                     USING EPHEMERAL INDEX 
                                     (COL_2=? AND COL_0=?) 
                                     (~20 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 9. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "o_year",
  "mkt_share"
FROM
  (
    SELECT
      "COL_0" as "o_year",
      sum ("COL_1"::double) / sum ("COL_2"::double) as "mkt_share"
    FROM
      "_tmp_13492713738046281510_7136"
    GROUP BY
      "COL_0"
  )
ORDER BY
  "o_year"
''
plan:
    [0] SCAN TABLE _tmp_13492713738046281510_7136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: tpch-explain-q10
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 20
  projection (
    c_custkey::int,
    c_name::text,
    revenue::double,
    c_acctbal::double,
    n_name::text,
    c_address::text,
    c_phone::text,
    c_comment::text
  )
    order by (revenue::double desc)
      scan
        projection (
          gr_expr_1::int -> c_custkey,
          gr_expr_2::text -> c_name,
          sum(sum_1::double::double)::double -> revenue,
          gr_expr_3::double -> c_acctbal,
          gr_expr_5::text -> n_name,
          gr_expr_6::text -> c_address,
          gr_expr_4::text -> c_phone,
          gr_expr_7::text -> c_comment
        )
          group by (
            gr_expr_1::int,
            gr_expr_2::text,
            gr_expr_3::double,
            gr_expr_4::text,
            gr_expr_5::text,
            gr_expr_6::text,
            gr_expr_7::text
          )
            motion [policy: full, program: ReshardIfNeeded]
              projection (
                customer.c_custkey::int -> gr_expr_1,
                customer.c_name::text -> gr_expr_2,
                customer.c_acctbal::double -> gr_expr_3,
                customer.c_phone::text -> gr_expr_4,
                nation.n_name::text -> gr_expr_5,
                customer.c_address::text -> gr_expr_6,
                customer.c_comment::text -> gr_expr_7,
                sum(
                  (
                    lineitem.l_extendedprice::double * (1::int - lineitem.l_discount::double)
                  )::double
                )::double -> sum_1
              )
                group by (
                  customer.c_custkey::int,
                  customer.c_name::text,
                  customer.c_acctbal::double,
                  customer.c_phone::text,
                  nation.n_name::text,
                  customer.c_address::text,
                  customer.c_comment::text
                )
                  selection (
                    (
                      customer.c_custkey::int = orders.o_custkey::int
                      and lineitem.l_orderkey::int = orders.o_orderkey::int
                      and orders.o_orderdate::datetime >= '1993-10-01 0:00:00.0 +00:00:00'::datetime
                      and orders.o_orderdate::datetime < '1994-01-01 0:00:00.0 +00:00:00'::datetime
                      and lineitem.l_returnflag::text = 'R'::text
                      and customer.c_nationkey::int = nation.n_nationkey::int
                    )
                  )
                    join on (true::bool)
                      join on (true::bool)
                        join on (true::bool)
                          scan customer
                          motion [policy: segment([ref(o_custkey)]), program: ReshardIfNeeded]
                            projection (
                              orders.o_orderkey::int -> o_orderkey,
                              orders.bucket_id::int -> bucket_id,
                              orders.o_custkey::int -> o_custkey,
                              orders.o_orderstatus::text -> o_orderstatus,
                              orders.o_totalprice::double -> o_totalprice,
                              orders.o_orderdate::datetime -> o_orderdate,
                              orders.o_orderpriority::text -> o_orderpriority,
                              orders.o_clerk::text -> o_clerk,
                              orders.o_shippriority::int -> o_shippriority,
                              orders.o_comment::text -> o_comment
                            )
                              scan orders
                        motion [policy: full, program: ReshardIfNeeded]
                          projection (
                            lineitem.l_orderkey::int -> l_orderkey,
                            lineitem.l_partkey::int -> l_partkey,
                            lineitem.l_suppkey::int -> l_suppkey,
                            lineitem.l_linenumber::int -> l_linenumber,
                            lineitem.bucket_id::int -> bucket_id,
                            lineitem.l_quantity::double -> l_quantity,
                            lineitem.l_extendedprice::double -> l_extendedprice,
                            lineitem.l_discount::double -> l_discount,
                            lineitem.l_tax::double -> l_tax,
                            lineitem.l_returnflag::text -> l_returnflag,
                            lineitem.l_linestatus::text -> l_linestatus,
                            lineitem.l_shipdate::datetime -> l_shipdate,
                            lineitem.l_commitdate::datetime -> l_commitdate,
                            lineitem.l_receiptdate::datetime -> l_receiptdate,
                            lineitem.l_shipinstruct::text -> l_shipinstruct,
                            lineitem.l_shipmode::text -> l_shipmode,
                            lineitem.l_comment::text -> l_comment
                          )
                            scan lineitem
                      motion [policy: full, program: ReshardIfNeeded]
                        projection (
                          nation.n_nationkey::int -> n_nationkey,
                          nation.bucket_id::int -> bucket_id,
                          nation.n_name::text -> n_name,
                          nation.n_regionkey::int -> n_regionkey,
                          nation.n_comment::text -> n_comment
                        )
                          scan nation
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "orders"."o_orderkey",
  "orders"."bucket_id",
  "orders"."o_custkey",
  "orders"."o_orderstatus",
  "orders"."o_totalprice",
  "orders"."o_orderdate",
  "orders"."o_orderpriority",
  "orders"."o_clerk",
  "orders"."o_shippriority",
  "orders"."o_comment"
FROM
  "orders"
''
plan:
    [0] SCAN TABLE orders (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "lineitem"."l_orderkey",
  "lineitem"."l_partkey",
  "lineitem"."l_suppkey",
  "lineitem"."l_linenumber",
  "lineitem"."bucket_id",
  "lineitem"."l_quantity",
  "lineitem"."l_extendedprice",
  "lineitem"."l_discount",
  "lineitem"."l_tax",
  "lineitem"."l_returnflag",
  "lineitem"."l_linestatus",
  "lineitem"."l_shipdate",
  "lineitem"."l_commitdate",
  "lineitem"."l_receiptdate",
  "lineitem"."l_shipinstruct",
  "lineitem"."l_shipmode",
  "lineitem"."l_comment"
FROM
  "lineitem"
''
plan:
    [0] SCAN TABLE lineitem (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 3. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "nation"."n_nationkey",
  "nation"."bucket_id",
  "nation"."n_name",
  "nation"."n_regionkey",
  "nation"."n_comment"
FROM
  "nation"
''
plan:
    [0] SCAN TABLE nation (~1048576 rows)
''
buckets <= [1-3000]
''
╭─────────────────────────────────╮
│ 4. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "customer"."c_custkey" as "gr_expr_1",
  "customer"."c_name" as "gr_expr_2",
  "customer"."c_acctbal" as "gr_expr_3",
  "customer"."c_phone" as "gr_expr_4",
  "nation"."COL_2" as "gr_expr_5",
  "customer"."c_address" as "gr_expr_6",
  "customer"."c_comment" as "gr_expr_7",
  sum (
    (
      "lineitem"."COL_6" * (1::int - "lineitem"."COL_7")
    )::double
  ) as "sum_1"
FROM
  "customer"
  INNER JOIN "_tmp_6752974385567748145_0136" as "orders" ON true::bool
  INNER JOIN "_tmp_6752974385567748145_1136" as "lineitem" ON true::bool
  INNER JOIN "_tmp_6752974385567748145_2136" as "nation" ON true::bool
WHERE
  "customer"."c_custkey" = "orders"."COL_2"
  and "lineitem"."COL_0" = "orders"."COL_0"
  and "orders"."COL_5" >= '1993-10-01 0:00:00.0 +00:00:00'::datetime
  and "orders"."COL_5" < '1994-01-01 0:00:00.0 +00:00:00'::datetime
  and "lineitem"."COL_9" = 'R'::text
  and "customer"."c_nationkey" = "nation"."COL_0"
GROUP BY
  "customer"."c_custkey",
  "customer"."c_name",
  "customer"."c_acctbal",
  "customer"."c_phone",
  "nation"."COL_2",
  "customer"."c_address",
  "customer"."c_comment"
''
plan:
    [0] SCAN TABLE customer (~1048576 rows)
        [0] SCAN TABLE _tmp_6752974385567748145_1136 AS lineitem (~262144 rows)
            [0] SEARCH TABLE _tmp_6752974385567748145_0136 AS orders 
                 USING EPHEMERAL INDEX 
                 (COL_0=? AND COL_2=?) 
                 (~20 rows)
                [0] SEARCH TABLE _tmp_6752974385567748145_2136 AS nation 
                     USING EPHEMERAL INDEX 
                     (COL_0=?) 
                     (~20 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 5. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "c_custkey",
  "c_name",
  "revenue",
  "c_acctbal",
  "n_name",
  "c_address",
  "c_phone",
  "c_comment"
FROM
  (
    SELECT
      "COL_0" as "c_custkey",
      "COL_1" as "c_name",
      sum ("COL_7"::double) as "revenue",
      "COL_2" as "c_acctbal",
      "COL_4" as "n_name",
      "COL_5" as "c_address",
      "COL_3" as "c_phone",
      "COL_6" as "c_comment"
    FROM
      "_tmp_17934649577571891331_3136"
    GROUP BY
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6"
  )
ORDER BY
  "revenue" DESC
LIMIT
  20
''
plan:
    [0] SCAN TABLE _tmp_17934649577571891331_3136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: tpch-explain-q11
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (ps_partkey::int, value::double)
  order by (value::double desc)
    scan
      projection (
        gr_expr_1::int -> ps_partkey,
        sum(sum_1::double::double)::double -> value
      )
        having (sum(sum_1::double::double)::double > ROW($0))
          group by (gr_expr_1::int)
            motion [policy: full, program: ReshardIfNeeded]
              projection (
                partsupp.ps_partkey::int -> gr_expr_1,
                sum(
                  (
                    partsupp.ps_supplycost::double * partsupp.ps_availqty::int
                  )::double
                )::double -> sum_1
              )
                group by (partsupp.ps_partkey::int)
                  selection (
                    (
                      partsupp.ps_suppkey::int = supplier.s_suppkey::int
                      and supplier.s_nationkey::int = nation.n_nationkey::int
                      and nation.n_name::text = 'GERMANY'::text
                    )
                  )
                    join on (true::bool)
                      join on (true::bool)
                        scan partsupp
                        motion [policy: full, program: ReshardIfNeeded]
                          projection (
                            supplier.s_suppkey::int -> s_suppkey,
                            supplier.bucket_id::int -> bucket_id,
                            supplier.s_name::text -> s_name,
                            supplier.s_address::text -> s_address,
                            supplier.s_nationkey::int -> s_nationkey,
                            supplier.s_phone::text -> s_phone,
                            supplier.s_acctbal::double -> s_acctbal,
                            supplier.s_comment::text -> s_comment
                          )
                            scan supplier
                      motion [policy: full, program: ReshardIfNeeded]
                        projection (
                          nation.n_nationkey::int -> n_nationkey,
                          nation.bucket_id::int -> bucket_id,
                          nation.n_name::text -> n_name,
                          nation.n_regionkey::int -> n_regionkey,
                          nation.n_comment::text -> n_comment
                        )
                          scan nation
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    scan
      projection (
        sum(sum_1::double::double)::double * 0.0001::decimal -> col_1
      )
        motion [policy: full, program: ReshardIfNeeded]
          projection (
            sum(
              (
                partsupp.ps_supplycost::double * partsupp.ps_availqty::int
              )::double
            )::double -> sum_1
          )
            selection (
              (
                partsupp.ps_suppkey::int = supplier.s_suppkey::int
                and supplier.s_nationkey::int = nation.n_nationkey::int
                and nation.n_name::text = 'GERMANY'::text
              )
            )
              join on (true::bool)
                join on (true::bool)
                  scan partsupp
                  motion [policy: full, program: ReshardIfNeeded]
                    projection (
                      supplier.s_suppkey::int -> s_suppkey,
                      supplier.bucket_id::int -> bucket_id,
                      supplier.s_name::text -> s_name,
                      supplier.s_address::text -> s_address,
                      supplier.s_nationkey::int -> s_nationkey,
                      supplier.s_phone::text -> s_phone,
                      supplier.s_acctbal::double -> s_acctbal,
                      supplier.s_comment::text -> s_comment
                    )
                      scan supplier
                motion [policy: full, program: ReshardIfNeeded]
                  projection (
                    nation.n_nationkey::int -> n_nationkey,
                    nation.bucket_id::int -> bucket_id,
                    nation.n_name::text -> n_name,
                    nation.n_regionkey::int -> n_regionkey,
                    nation.n_comment::text -> n_comment
                  )
                    scan nation
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "supplier"."s_suppkey",
  "supplier"."bucket_id",
  "supplier"."s_name",
  "supplier"."s_address",
  "supplier"."s_nationkey",
  "supplier"."s_phone",
  "supplier"."s_acctbal",
  "supplier"."s_comment"
FROM
  "supplier"
''
plan:
    [0] SCAN TABLE supplier (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "nation"."n_nationkey",
  "nation"."bucket_id",
  "nation"."n_name",
  "nation"."n_regionkey",
  "nation"."n_comment"
FROM
  "nation"
''
plan:
    [0] SCAN TABLE nation (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 3. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "supplier"."s_suppkey",
  "supplier"."bucket_id",
  "supplier"."s_name",
  "supplier"."s_address",
  "supplier"."s_nationkey",
  "supplier"."s_phone",
  "supplier"."s_acctbal",
  "supplier"."s_comment"
FROM
  "supplier"
''
plan:
    [0] SCAN TABLE supplier (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 4. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "nation"."n_nationkey",
  "nation"."bucket_id",
  "nation"."n_name",
  "nation"."n_regionkey",
  "nation"."n_comment"
FROM
  "nation"
''
plan:
    [0] SCAN TABLE nation (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 5. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "partsupp"."ps_partkey" as "gr_expr_1",
  sum (
    (
      "partsupp"."ps_supplycost" * "partsupp"."ps_availqty"
    )::double
  ) as "sum_1"
FROM
  "partsupp"
  INNER JOIN "_tmp_17389738504606850982_0136" as "supplier" ON true::bool
  INNER JOIN "_tmp_17389738504606850982_1136" as "nation" ON true::bool
WHERE
  "partsupp"."ps_suppkey" = "supplier"."COL_0"
  and "supplier"."COL_4" = "nation"."COL_0"
  and "nation"."COL_2" = 'GERMANY'::text
GROUP BY
  "partsupp"."ps_partkey"
''
plan:
    [0] SCAN TABLE partsupp (~1048576 rows)
        [0] SCAN TABLE _tmp_17389738504606850982_1136 AS nation (~262144 rows)
            [0] SEARCH TABLE _tmp_17389738504606850982_0136 AS supplier 
                 USING EPHEMERAL INDEX 
                 (COL_4=? AND COL_0=?) 
                 (~20 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 6. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  sum (
    (
      "partsupp"."ps_supplycost" * "partsupp"."ps_availqty"
    )::double
  ) as "sum_1"
FROM
  "partsupp"
  INNER JOIN "_tmp_17869557022914879301_2136" as "supplier" ON true::bool
  INNER JOIN "_tmp_17869557022914879301_3136" as "nation" ON true::bool
WHERE
  "partsupp"."ps_suppkey" = "supplier"."COL_0"
  and "supplier"."COL_4" = "nation"."COL_0"
  and "nation"."COL_2" = 'GERMANY'::text
''
plan:
    [0] SCAN TABLE _tmp_17869557022914879301_3136 AS nation (~262144 rows)
        [0] SCAN TABLE partsupp (~1048576 rows)
            [0] SEARCH TABLE _tmp_17869557022914879301_2136 AS supplier 
                 USING EPHEMERAL INDEX 
                 (COL_4=? AND COL_0=?) 
                 (~20 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 7. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  sum ("COL_0"::double) * 0.0001::decimal as "col_1"
FROM
  "_tmp_10305140153159277659_4136"
''
plan:
    [0] SCAN TABLE _tmp_10305140153159277659_4136 (~1048576 rows)
''
buckets = any
''
╭───────────────────╮
│ 8. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "ps_partkey",
  "value"
FROM
  (
    SELECT
      "COL_0" as "ps_partkey",
      sum ("COL_1"::double) as "value"
    FROM
      "_tmp_16780076466944764098_5136"
    GROUP BY
      "COL_0"
    HAVING
      sum ("COL_1"::double) > (
        SELECT
          "COL_0"
        FROM
          "_tmp_16780076466944764098_6136"
      )
  )
ORDER BY
  "value" DESC
''
plan:
    [0] SCAN TABLE _tmp_16780076466944764098_5136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] EXECUTE SCALAR SUBQUERY 1
    [1] SCAN TABLE _tmp_16780076466944764098_6136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: tpch-explain-q12
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  l_shipmode::text,
  high_line_count::decimal,
  low_line_count::decimal
)
  order by (l_shipmode::text)
    scan
      projection (
        gr_expr_1::text -> l_shipmode,
        sum(sum_1::decimal::decimal)::decimal -> high_line_count,
        sum(sum_2::decimal::decimal)::decimal -> low_line_count
      )
        group by (gr_expr_1::text)
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              lineitem.l_shipmode::text -> gr_expr_1,
              sum(
                case 
                  when orders.o_orderpriority::text = '1-URGENT'::text or orders.o_orderpriority::text = '2-HIGH'::text then 1::int
                  else 0::int
                end::int
              )::decimal -> sum_1,
              sum(
                case 
                  when (
                    orders.o_orderpriority::text <> '1-URGENT'::text
                    and orders.o_orderpriority::text <> '2-HIGH'::text
                  ) then 1::int
                  else 0::int
                end::int
              )::decimal -> sum_2
            )
              group by (lineitem.l_shipmode::text)
                selection (
                  (
                    lineitem.l_shipmode::text in ROW('MAIL'::text, 'SHIP'::text)
                    and lineitem.l_commitdate::datetime < lineitem.l_receiptdate::datetime
                    and lineitem.l_shipdate::datetime < lineitem.l_commitdate::datetime
                    and lineitem.l_receiptdate::datetime >= '1994-01-01 0:00:00.0 +00:00:00'::datetime
                    and lineitem.l_receiptdate::datetime < '1995-01-01 0:00:00.0 +00:00:00'::datetime
                  )
                )
                  join on (
                    lineitem.l_orderkey::int = orders.o_orderkey::int
                  )
                    scan lineitem
                    motion [policy: full, program: ReshardIfNeeded]
                      projection (
                        orders.o_orderkey::int -> o_orderkey,
                        orders.bucket_id::int -> bucket_id,
                        orders.o_custkey::int -> o_custkey,
                        orders.o_orderstatus::text -> o_orderstatus,
                        orders.o_totalprice::double -> o_totalprice,
                        orders.o_orderdate::datetime -> o_orderdate,
                        orders.o_orderpriority::text -> o_orderpriority,
                        orders.o_clerk::text -> o_clerk,
                        orders.o_shippriority::int -> o_shippriority,
                        orders.o_comment::text -> o_comment
                      )
                        scan orders
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "orders"."o_orderkey",
  "orders"."bucket_id",
  "orders"."o_custkey",
  "orders"."o_orderstatus",
  "orders"."o_totalprice",
  "orders"."o_orderdate",
  "orders"."o_orderpriority",
  "orders"."o_clerk",
  "orders"."o_shippriority",
  "orders"."o_comment"
FROM
  "orders"
''
plan:
    [0] SCAN TABLE orders (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "lineitem"."l_shipmode" as "gr_expr_1",
  sum (
    CASE
      WHEN "orders"."COL_6" = '1-URGENT'::text
      or "orders"."COL_6" = '2-HIGH'::text THEN 1::int
      ELSE 0::int
    END::int
  ) as "sum_1",
  sum (
    CASE
      WHEN "orders"."COL_6" <> '1-URGENT'::text
      and "orders"."COL_6" <> '2-HIGH'::text THEN 1::int
      ELSE 0::int
    END::int
  ) as "sum_2"
FROM
  "lineitem"
  INNER JOIN "_tmp_16541552721638505593_0136" as "orders" ON "lineitem"."l_orderkey" = "orders"."COL_0"
WHERE
  "lineitem"."l_shipmode" in ('MAIL'::text, 'SHIP'::text)
  and "lineitem"."l_commitdate" < "lineitem"."l_receiptdate"
  and "lineitem"."l_shipdate" < "lineitem"."l_commitdate"
  and "lineitem"."l_receiptdate" >= '1994-01-01 0:00:00.0 +00:00:00'::datetime
  and "lineitem"."l_receiptdate" < '1995-01-01 0:00:00.0 +00:00:00'::datetime
GROUP BY
  "lineitem"."l_shipmode"
''
plan:
    [0] SCAN TABLE _tmp_16541552721638505593_0136 AS orders (~1048576 rows)
        [0] SEARCH TABLE lineitem USING PRIMARY KEY (l_orderkey=?) (~7 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "l_shipmode",
  "high_line_count",
  "low_line_count"
FROM
  (
    SELECT
      "COL_0" as "l_shipmode",
      sum ("COL_1"::decimal) as "high_line_count",
      sum ("COL_2"::decimal) as "low_line_count"
    FROM
      "_tmp_13446821435794994891_1136"
    GROUP BY
      "COL_0"
  )
ORDER BY
  "l_shipmode"
''
plan:
    [0] SCAN TABLE _tmp_13446821435794994891_1136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: tpch-explain-q13
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (c_count::int, custdist::int)
  order by (custdist::int desc, c_count::int desc)
    scan
      projection (
        c_orders.c_count::int -> c_count,
        count(*)::int -> custdist
      )
        group by (c_orders.c_count::int)
          scan c_orders
            projection (
              gr_expr_1::int -> c_custkey,
              sum(count_1::int::int)::int::int -> c_count
            )
              group by (gr_expr_1::int)
                motion [policy: full, program: ReshardIfNeeded]
                  projection (
                    customer.c_custkey::int -> gr_expr_1,
                    count(orders.o_orderkey::int::int)::int -> count_1
                  )
                    group by (customer.c_custkey::int)
                      left join on (
                        (
                          customer.c_custkey::int = orders.o_custkey::int
                          and not orders.o_comment::text::text LIKE '%special%requests%'::text ESCAPE '\'::text
                        )
                      )
                        scan customer
                        motion [policy: segment([ref(o_custkey)]), program: ReshardIfNeeded]
                          projection (
                            orders.o_orderkey::int -> o_orderkey,
                            orders.bucket_id::int -> bucket_id,
                            orders.o_custkey::int -> o_custkey,
                            orders.o_orderstatus::text -> o_orderstatus,
                            orders.o_totalprice::double -> o_totalprice,
                            orders.o_orderdate::datetime -> o_orderdate,
                            orders.o_orderpriority::text -> o_orderpriority,
                            orders.o_clerk::text -> o_clerk,
                            orders.o_shippriority::int -> o_shippriority,
                            orders.o_comment::text -> o_comment
                          )
                            scan orders
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "orders"."o_orderkey",
  "orders"."bucket_id",
  "orders"."o_custkey",
  "orders"."o_orderstatus",
  "orders"."o_totalprice",
  "orders"."o_orderdate",
  "orders"."o_orderpriority",
  "orders"."o_clerk",
  "orders"."o_shippriority",
  "orders"."o_comment"
FROM
  "orders"
''
plan:
    [0] SCAN TABLE orders (~1048576 rows)
''
buckets <= [1-3000]
''
╭─────────────────────────────────╮
│ 2. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "customer"."c_custkey" as "gr_expr_1",
  count ("orders"."COL_0"::int) as "count_1"
FROM
  "customer"
  LEFT JOIN "_tmp_2853321084796073973_0136" as "orders" ON "customer"."c_custkey" = "orders"."COL_2"
  and not "orders"."COL_9"::text LIKE '%special%requests%'::text ESCAPE '\'::text
GROUP BY
  "customer"."c_custkey"
''
plan:
    [0] SCAN TABLE customer (~1048576 rows)
        [0] SCAN TABLE _tmp_2853321084796073973_0136 AS orders (~262144 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "c_count",
  "custdist"
FROM
  (
    SELECT
      "c_orders"."c_count",
      count (*) as "custdist"
    FROM
      (
        SELECT
          "COL_0" as "c_custkey",
          sum ("COL_1"::int)::int as "c_count"
        FROM
          "_tmp_9702922271480073019_1136"
        GROUP BY
          "COL_0"
      ) as "c_orders"
    GROUP BY
      "c_orders"."c_count"
  )
ORDER BY
  "custdist" DESC,
  "c_count" DESC
''
plan:
    [1] SCAN TABLE _tmp_9702922271480073019_1136 (~1048576 rows)
    [1] USE TEMP B-TREE FOR GROUP BY
    [0] SCAN SUBQUERY 1 AS c_orders (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: tpch-explain-q14
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  (
    100.00::decimal * sum(sum_1::double::double)::double
  ) / sum(sum_2::double::double)::double -> promo_revenue
)
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      sum(
        (
          lineitem.l_extendedprice::double * (1::int - lineitem.l_discount::double)
        )::double
      )::double -> sum_2,
      sum(
        case 
          when part.p_type::text::text LIKE 'PROMO%'::text ESCAPE '\'::text then lineitem.l_extendedprice::double * (1::int - lineitem.l_discount::double)
          else 0::int
        end::double
      )::double -> sum_1
    )
      selection (
        (
          lineitem.l_partkey::int = part.p_partkey::int
          and lineitem.l_shipdate::datetime >= '1995-09-01 0:00:00.0 +00:00:00'::datetime
          and lineitem.l_shipdate::datetime < '1995-10-01 0:00:00.0 +00:00:00'::datetime
        )
      )
        join on (true::bool)
          scan lineitem
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              part.p_partkey::int -> p_partkey,
              part.bucket_id::int -> bucket_id,
              part.p_name::text -> p_name,
              part.p_mfgr::text -> p_mfgr,
              part.p_brand::text -> p_brand,
              part.p_type::text -> p_type,
              part.p_size::int -> p_size,
              part.p_container::text -> p_container,
              part.p_retailprice::double -> p_retailprice,
              part.p_comment::text -> p_comment
            )
              scan part
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "part"."p_partkey",
  "part"."bucket_id",
  "part"."p_name",
  "part"."p_mfgr",
  "part"."p_brand",
  "part"."p_type",
  "part"."p_size",
  "part"."p_container",
  "part"."p_retailprice",
  "part"."p_comment"
FROM
  "part"
''
plan:
    [0] SCAN TABLE part (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  sum (
    (
      "lineitem"."l_extendedprice" * (1::int - "lineitem"."l_discount")
    )::double
  ) as "sum_2",
  sum (
    CASE
      WHEN "part"."COL_5"::text LIKE 'PROMO%'::text ESCAPE '\'::text THEN "lineitem"."l_extendedprice" * (1::int - "lineitem"."l_discount")
      ELSE 0::int
    END::double
  ) as "sum_1"
FROM
  "lineitem"
  INNER JOIN "_tmp_4254979314961859233_0136" as "part" ON true::bool
WHERE
  "lineitem"."l_partkey" = "part"."COL_0"
  and "lineitem"."l_shipdate" >= '1995-09-01 0:00:00.0 +00:00:00'::datetime
  and "lineitem"."l_shipdate" < '1995-10-01 0:00:00.0 +00:00:00'::datetime
''
plan:
    [0] SCAN TABLE lineitem (~917504 rows)
        [0] SCAN TABLE _tmp_4254979314961859233_0136 AS part (~1048576 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  (100.00::decimal * sum ("COL_1"::double)) / sum ("COL_0"::double) as "promo_revenue"
FROM
  "_tmp_12731488821819863187_1136"
''
plan:
    [0] SCAN TABLE _tmp_12731488821819863187_1136 (~1048576 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: tpch-explain-q16
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  p_brand::text,
  p_type::text,
  p_size::int,
  supplier_cnt::int
)
  order by (
    supplier_cnt::int desc,
    p_brand::text,
    p_type::text,
    p_size::int
  )
    scan
      projection (
        gr_expr_1::text -> p_brand,
        gr_expr_2::text -> p_type,
        gr_expr_3::int -> p_size,
        count(distinct gr_expr_4::int)::int -> supplier_cnt
      )
        group by (
          gr_expr_1::text,
          gr_expr_2::text,
          gr_expr_3::int
        )
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              part.p_brand::text -> gr_expr_1,
              part.p_type::text -> gr_expr_2,
              part.p_size::int -> gr_expr_3,
              partsupp.ps_suppkey::int::int -> gr_expr_4
            )
              group by (
                part.p_brand::text,
                part.p_type::text,
                part.p_size::int,
                partsupp.ps_suppkey::int::int
              )
                selection (
                  (
                    part.p_partkey::int = partsupp.ps_partkey::int
                    and part.p_brand::text <> 'Brand#45'::text
                    and not part.p_type::text::text LIKE 'MEDIUM POLISHED%'::text ESCAPE '\'::text
                    and part.p_size::int in ROW(
                      49::int,
                      14::int,
                      23::int,
                      45::int,
                      19::int,
                      3::int,
                      36::int,
                      9::int
                    )
                    and not partsupp.ps_suppkey::int in ROW($0)
                  )
                )
                  join on (true::bool)
                    scan partsupp
                    motion [policy: full, program: ReshardIfNeeded]
                      projection (
                        part.p_partkey::int -> p_partkey,
                        part.bucket_id::int -> bucket_id,
                        part.p_name::text -> p_name,
                        part.p_mfgr::text -> p_mfgr,
                        part.p_brand::text -> p_brand,
                        part.p_type::text -> p_type,
                        part.p_size::int -> p_size,
                        part.p_container::text -> p_container,
                        part.p_retailprice::double -> p_retailprice,
                        part.p_comment::text -> p_comment
                      )
                        scan part
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    scan
      projection (supplier.s_suppkey::int -> s_suppkey)
        selection (
          supplier.s_comment::text::text LIKE '%Customer%Complaints%'::text ESCAPE '\'::text
        )
          scan supplier
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "part"."p_partkey",
  "part"."bucket_id",
  "part"."p_name",
  "part"."p_mfgr",
  "part"."p_brand",
  "part"."p_type",
  "part"."p_size",
  "part"."p_container",
  "part"."p_retailprice",
  "part"."p_comment"
FROM
  "part"
''
plan:
    [0] SCAN TABLE part (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "supplier"."s_suppkey"
FROM
  "supplier"
WHERE
  "supplier"."s_comment"::text LIKE '%Customer%Complaints%'::text ESCAPE '\'::text
''
plan:
    [0] SCAN TABLE supplier (~983040 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 3. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "part"."COL_4" as "gr_expr_1",
  "part"."COL_5" as "gr_expr_2",
  "part"."COL_6" as "gr_expr_3",
  "partsupp"."ps_suppkey"::int as "gr_expr_4"
FROM
  "partsupp"
  INNER JOIN "_tmp_14149410492156322724_0136" as "part" ON true::bool
WHERE
  "part"."COL_0" = "partsupp"."ps_partkey"
  and "part"."COL_4" <> 'Brand#45'::text
  and not "part"."COL_5"::text LIKE 'MEDIUM POLISHED%'::text ESCAPE '\'::text
  and "part"."COL_6" in (
    49::int,
    14::int,
    23::int,
    45::int,
    19::int,
    3::int,
    36::int,
    9::int
  )
  and not "partsupp"."ps_suppkey" in (
    SELECT
      "COL_0"
    FROM
      "_tmp_14149410492156322724_1136"
  )
GROUP BY
  "part"."COL_4",
  "part"."COL_5",
  "part"."COL_6",
  "partsupp"."ps_suppkey"::int
''
plan:
    [0] SCAN TABLE _tmp_14149410492156322724_0136 AS part (~851968 rows)
    [0] EXECUTE LIST SUBQUERY 1
        [0] SEARCH TABLE partsupp USING PRIMARY KEY (ps_partkey=?) (~9 rows)
    [0] EXECUTE LIST SUBQUERY 1
    [1] SCAN TABLE _tmp_14149410492156322724_1136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "p_brand",
  "p_type",
  "p_size",
  "supplier_cnt"
FROM
  (
    SELECT
      "COL_0" as "p_brand",
      "COL_1" as "p_type",
      "COL_2" as "p_size",
      count (DISTINCT "COL_3") as "supplier_cnt"
    FROM
      "_tmp_4785163345789539971_2136"
    GROUP BY
      "COL_0",
      "COL_1",
      "COL_2"
  )
ORDER BY
  "supplier_cnt" DESC,
  "p_brand",
  "p_type",
  "p_size"
''
plan:
    [0] SCAN TABLE _tmp_4785163345789539971_2136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: tpch-explain-q18
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 100
  projection (
    c_name::text,
    c_custkey::int,
    o_orderkey::int,
    o_orderdate::datetime,
    o_totalprice::double,
    col_1::double
  )
    order by (
      o_totalprice::double desc,
      o_orderdate::datetime
    )
      scan
        projection (
          gr_expr_1::text -> c_name,
          gr_expr_2::int -> c_custkey,
          gr_expr_3::int -> o_orderkey,
          gr_expr_4::datetime -> o_orderdate,
          gr_expr_5::double -> o_totalprice,
          sum(sum_1::double::double)::double -> col_1
        )
          group by (
            gr_expr_1::text,
            gr_expr_2::int,
            gr_expr_3::int,
            gr_expr_4::datetime,
            gr_expr_5::double
          )
            motion [policy: full, program: ReshardIfNeeded]
              projection (
                customer.c_name::text -> gr_expr_1,
                customer.c_custkey::int -> gr_expr_2,
                orders.o_orderkey::int -> gr_expr_3,
                orders.o_orderdate::datetime -> gr_expr_4,
                orders.o_totalprice::double -> gr_expr_5,
                sum(lineitem.l_quantity::double::double)::double -> sum_1
              )
                group by (
                  customer.c_name::text,
                  customer.c_custkey::int,
                  orders.o_orderkey::int,
                  orders.o_orderdate::datetime,
                  orders.o_totalprice::double
                )
                  selection (
                    (
                      orders.o_orderkey::int in ROW($0)
                      and customer.c_custkey::int = orders.o_custkey::int
                      and orders.o_orderkey::int = lineitem.l_orderkey::int
                    )
                  )
                    join on (true::bool)
                      join on (true::bool)
                        scan customer
                        motion [policy: segment([ref(o_custkey)]), program: ReshardIfNeeded]
                          projection (
                            orders.o_orderkey::int -> o_orderkey,
                            orders.bucket_id::int -> bucket_id,
                            orders.o_custkey::int -> o_custkey,
                            orders.o_orderstatus::text -> o_orderstatus,
                            orders.o_totalprice::double -> o_totalprice,
                            orders.o_orderdate::datetime -> o_orderdate,
                            orders.o_orderpriority::text -> o_orderpriority,
                            orders.o_clerk::text -> o_clerk,
                            orders.o_shippriority::int -> o_shippriority,
                            orders.o_comment::text -> o_comment
                          )
                            scan orders
                      motion [policy: full, program: ReshardIfNeeded]
                        projection (
                          lineitem.l_orderkey::int -> l_orderkey,
                          lineitem.l_partkey::int -> l_partkey,
                          lineitem.l_suppkey::int -> l_suppkey,
                          lineitem.l_linenumber::int -> l_linenumber,
                          lineitem.bucket_id::int -> bucket_id,
                          lineitem.l_quantity::double -> l_quantity,
                          lineitem.l_extendedprice::double -> l_extendedprice,
                          lineitem.l_discount::double -> l_discount,
                          lineitem.l_tax::double -> l_tax,
                          lineitem.l_returnflag::text -> l_returnflag,
                          lineitem.l_linestatus::text -> l_linestatus,
                          lineitem.l_shipdate::datetime -> l_shipdate,
                          lineitem.l_commitdate::datetime -> l_commitdate,
                          lineitem.l_receiptdate::datetime -> l_receiptdate,
                          lineitem.l_shipinstruct::text -> l_shipinstruct,
                          lineitem.l_shipmode::text -> l_shipmode,
                          lineitem.l_comment::text -> l_comment
                        )
                          scan lineitem
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    scan
      projection (gr_expr_1::int -> l_orderkey)
        having (sum(sum_1::double::double)::double > 300::int)
          group by (gr_expr_1::int)
            motion [policy: full, program: ReshardIfNeeded]
              projection (
                lineitem.l_orderkey::int -> gr_expr_1,
                sum(lineitem.l_quantity::double::double)::double -> sum_1
              )
                group by (lineitem.l_orderkey::int)
                  scan lineitem
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "orders"."o_orderkey",
  "orders"."bucket_id",
  "orders"."o_custkey",
  "orders"."o_orderstatus",
  "orders"."o_totalprice",
  "orders"."o_orderdate",
  "orders"."o_orderpriority",
  "orders"."o_clerk",
  "orders"."o_shippriority",
  "orders"."o_comment"
FROM
  "orders"
''
plan:
    [0] SCAN TABLE orders (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "lineitem"."l_orderkey",
  "lineitem"."l_partkey",
  "lineitem"."l_suppkey",
  "lineitem"."l_linenumber",
  "lineitem"."bucket_id",
  "lineitem"."l_quantity",
  "lineitem"."l_extendedprice",
  "lineitem"."l_discount",
  "lineitem"."l_tax",
  "lineitem"."l_returnflag",
  "lineitem"."l_linestatus",
  "lineitem"."l_shipdate",
  "lineitem"."l_commitdate",
  "lineitem"."l_receiptdate",
  "lineitem"."l_shipinstruct",
  "lineitem"."l_shipmode",
  "lineitem"."l_comment"
FROM
  "lineitem"
''
plan:
    [0] SCAN TABLE lineitem (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 3. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "lineitem"."l_orderkey" as "gr_expr_1",
  sum ("lineitem"."l_quantity"::double) as "sum_1"
FROM
  "lineitem"
GROUP BY
  "lineitem"."l_orderkey"
''
plan:
    [0] SCAN TABLE lineitem (~1048576 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "COL_0" as "l_orderkey"
FROM
  "_tmp_5240946567730125004_2136"
GROUP BY
  "COL_0"
HAVING
  sum ("COL_1"::double) > 300::int
''
plan:
    [0] SCAN TABLE _tmp_5240946567730125004_2136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = any
''
╭─────────────────────────────────╮
│ 5. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "customer"."c_name" as "gr_expr_1",
  "customer"."c_custkey" as "gr_expr_2",
  "orders"."COL_0" as "gr_expr_3",
  "orders"."COL_5" as "gr_expr_4",
  "orders"."COL_4" as "gr_expr_5",
  sum ("lineitem"."COL_5"::double) as "sum_1"
FROM
  "customer"
  INNER JOIN "_tmp_9932753425948525886_0136" as "orders" ON true::bool
  INNER JOIN "_tmp_9932753425948525886_1136" as "lineitem" ON true::bool
WHERE
  "orders"."COL_0" in (
    SELECT
      "COL_0"
    FROM
      "_tmp_9932753425948525886_3136"
  )
  and "customer"."c_custkey" = "orders"."COL_2"
  and "orders"."COL_0" = "lineitem"."COL_0"
GROUP BY
  "customer"."c_name",
  "customer"."c_custkey",
  "orders"."COL_0",
  "orders"."COL_5",
  "orders"."COL_4"
''
plan:
    [0] SCAN TABLE _tmp_9932753425948525886_0136 AS orders (~983040 rows)
    [0] EXECUTE LIST SUBQUERY 1
    [1] SCAN TABLE _tmp_9932753425948525886_3136 (~1048576 rows)
        [0] SCAN TABLE _tmp_9932753425948525886_1136 AS lineitem (~1048576 rows)
            [0] SEARCH TABLE customer USING PRIMARY KEY (c_custkey=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 6. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "c_name",
  "c_custkey",
  "o_orderkey",
  "o_orderdate",
  "o_totalprice",
  "col_1"
FROM
  (
    SELECT
      "COL_0" as "c_name",
      "COL_1" as "c_custkey",
      "COL_2" as "o_orderkey",
      "COL_3" as "o_orderdate",
      "COL_4" as "o_totalprice",
      sum ("COL_5"::double) as "col_1"
    FROM
      "_tmp_16750534456777283259_4136"
    GROUP BY
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4"
  )
ORDER BY
  "o_totalprice" DESC,
  "o_orderdate"
LIMIT
  100
''
plan:
    [0] SCAN TABLE _tmp_16750534456777283259_4136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: tpch-explain-q19
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (sum(sum_1::double::double)::double -> revenue)
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      sum(
        (
          lineitem.l_extendedprice::double * (1::int - lineitem.l_discount::double)
        )::double
      )::double -> sum_1
    )
      selection (
        (
          part.p_partkey::int = lineitem.l_partkey::int
          and part.p_brand::text = 'Brand#12'::text
          and part.p_container::text in ROW(
            'SM CASE'::text,
            'SM BOX'::text,
            'SM PACK'::text,
            'SM PKG'::text
          )
          and lineitem.l_quantity::double >= 1::int
          and lineitem.l_quantity::double <= 1::int + 10::int
          and part.p_size::int >= 1::int
          and part.p_size::int <= 5::int
          and lineitem.l_shipmode::text in ROW('AIR'::text, 'AIR REG'::text)
          and lineitem.l_shipinstruct::text = 'DELIVER IN PERSON'::text
        ) or (
          part.p_partkey::int = lineitem.l_partkey::int
          and part.p_brand::text = 'Brand#23'::text
          and part.p_container::text in ROW(
            'MED BAG'::text,
            'MED BOX'::text,
            'MED PKG'::text,
            'MED PACK'::text
          )
          and lineitem.l_quantity::double >= 10::int
          and lineitem.l_quantity::double <= 10::int + 10::int
          and part.p_size::int >= 1::int
          and part.p_size::int <= 10::int
          and lineitem.l_shipmode::text in ROW('AIR'::text, 'AIR REG'::text)
          and lineitem.l_shipinstruct::text = 'DELIVER IN PERSON'::text
        ) or (
          part.p_partkey::int = lineitem.l_partkey::int
          and part.p_brand::text = 'Brand#34'::text
          and part.p_container::text in ROW(
            'LG CASE'::text,
            'LG BOX'::text,
            'LG PACK'::text,
            'LG PKG'::text
          )
          and lineitem.l_quantity::double >= 20::int
          and lineitem.l_quantity::double <= 20::int + 10::int
          and part.p_size::int >= 1::int
          and part.p_size::int <= 15::int
          and lineitem.l_shipmode::text in ROW('AIR'::text, 'AIR REG'::text)
          and lineitem.l_shipinstruct::text = 'DELIVER IN PERSON'::text
        )
      )
        join on (true::bool)
          scan lineitem
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              part.p_partkey::int -> p_partkey,
              part.bucket_id::int -> bucket_id,
              part.p_name::text -> p_name,
              part.p_mfgr::text -> p_mfgr,
              part.p_brand::text -> p_brand,
              part.p_type::text -> p_type,
              part.p_size::int -> p_size,
              part.p_container::text -> p_container,
              part.p_retailprice::double -> p_retailprice,
              part.p_comment::text -> p_comment
            )
              scan part
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭──────────────────────────╮
│ 1. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "part"."p_partkey",
  "part"."bucket_id",
  "part"."p_name",
  "part"."p_mfgr",
  "part"."p_brand",
  "part"."p_type",
  "part"."p_size",
  "part"."p_container",
  "part"."p_retailprice",
  "part"."p_comment"
FROM
  "part"
''
plan:
    [0] SCAN TABLE part (~1048576 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  sum (
    (
      "lineitem"."l_extendedprice" * (1::int - "lineitem"."l_discount")
    )::double
  ) as "sum_1"
FROM
  "lineitem"
  INNER JOIN "_tmp_11521773992601555350_0136" as "part" ON true::bool
WHERE
  "part"."COL_0" = "lineitem"."l_partkey"
  and "part"."COL_4" = 'Brand#12'::text
  and "part"."COL_7" in ('SM CASE'::text, 'SM BOX'::text, 'SM PACK'::text, 'SM PKG'::text)
  and "lineitem"."l_quantity" >= 1::int
  and "lineitem"."l_quantity" <= 1::int + 10::int
  and "part"."COL_6" >= 1::int
  and "part"."COL_6" <= 5::int
  and "lineitem"."l_shipmode" in ('AIR'::text, 'AIR REG'::text)
  and "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON'::text
  or "part"."COL_0" = "lineitem"."l_partkey"
  and "part"."COL_4" = 'Brand#23'::text
  and "part"."COL_7" in ('MED BAG'::text, 'MED BOX'::text, 'MED PKG'::text, 'MED PACK'::text)
  and "lineitem"."l_quantity" >= 10::int
  and "lineitem"."l_quantity" <= 10::int + 10::int
  and "part"."COL_6" >= 1::int
  and "part"."COL_6" <= 10::int
  and "lineitem"."l_shipmode" in ('AIR'::text, 'AIR REG'::text)
  and "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON'::text
  or "part"."COL_0" = "lineitem"."l_partkey"
  and "part"."COL_4" = 'Brand#34'::text
  and "part"."COL_7" in ('LG CASE'::text, 'LG BOX'::text, 'LG PACK'::text, 'LG PKG'::text)
  and "lineitem"."l_quantity" >= 20::int
  and "lineitem"."l_quantity" <= 20::int + 10::int
  and "part"."COL_6" >= 1::int
  and "part"."COL_6" <= 15::int
  and "lineitem"."l_shipmode" in ('AIR'::text, 'AIR REG'::text)
  and "lineitem"."l_shipinstruct" = 'DELIVER IN PERSON'::text
''
plan:
    [0] SCAN TABLE lineitem (~1048576 rows)
        [0] SCAN TABLE _tmp_11521773992601555350_0136 AS part (~1048576 rows)
    [0] EXECUTE LIST SUBQUERY 1
    [0] EXECUTE LIST SUBQUERY 1
    [0] EXECUTE LIST SUBQUERY 1
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT sum ("COL_0"::double) as "revenue" FROM "_tmp_8067056639077876413_1136"
''
plan:
    [0] SCAN TABLE _tmp_8067056639077876413_1136 (~1048576 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]
