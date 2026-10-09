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
  l_returnflag::string,
  l_linestatus::string,
  sum_qty::double,
  sum_base_price::double,
  sum_disc_price::double,
  sum_charge::double,
  avg_qty::double,
  avg_price::double,
  avg_disc::double,
  count_order::int
)
  order by (l_returnflag::string, l_linestatus::string)
    scan
      projection (
        gr_expr_1::string -> l_returnflag,
        gr_expr_2::string -> l_linestatus,
        sum(sum_1::double::double)::double -> sum_qty,
        sum(sum_2::double::double)::double -> sum_base_price,
        sum(sum_3::double::double)::double -> sum_disc_price,
        sum(sum_4::double::double)::double -> sum_charge,
        sum(sum_1::double::double)::double / sum(avg_5::int::int)::int::int -> avg_qty,
        sum(sum_2::double::double)::double / sum(avg_6::int::int)::int::int -> avg_price,
        sum(avg_7::double::double)::double / sum(avg_8::int::int)::int::int -> avg_disc,
        sum(count_9::int::int)::int::int -> count_order
      )
        group by (gr_expr_1::string, gr_expr_2::string)
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              lineitem.l_returnflag::string -> gr_expr_1,
              lineitem.l_linestatus::string -> gr_expr_2,
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
                lineitem.l_returnflag::string,
                lineitem.l_linestatus::string
              )
                selection (
                  lineitem.l_shipdate::datetime <= to_date('1998-09-02'::string, '%Y-%m-%d'::string)::datetime
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
    CAST (
      (
        "lineitem"."l_extendedprice" * (CAST(1 AS int) - "lineitem"."l_discount") * (CAST(1 AS int) + "lineitem"."l_tax")
      ) as double
    )
  ) as "sum_4",
  sum (CAST ("lineitem"."l_extendedprice" as double)) as "sum_2",
  sum (CAST ("lineitem"."l_quantity" as double)) as "sum_1",
  count (CAST ("lineitem"."l_discount" as double)) as "avg_8",
  count (CAST ("lineitem"."l_quantity" as double)) as "avg_5",
  count (*) as "count_9",
  sum (CAST ("lineitem"."l_discount" as double)) as "avg_7",
  count (CAST ("lineitem"."l_extendedprice" as double)) as "avg_6",
  sum (
    CAST (
      (
        "lineitem"."l_extendedprice" * (CAST(1 AS int) - "lineitem"."l_discount")
      ) as double
    )
  ) as "sum_3"
FROM
  "lineitem"
WHERE
  "lineitem"."l_shipdate" <= "to_date" (CAST('1998-09-02' AS string), CAST('%Y-%m-%d' AS string))
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
      sum (CAST ("COL_4" as double)) as "sum_qty",
      sum (CAST ("COL_3" as double)) as "sum_base_price",
      sum (CAST ("COL_10" as double)) as "sum_disc_price",
      sum (CAST ("COL_2" as double)) as "sum_charge",
      sum (CAST ("COL_4" as double)) / CAST (sum (CAST ("COL_6" as int)) as int) as "avg_qty",
      sum (CAST ("COL_3" as double)) / CAST (sum (CAST ("COL_9" as int)) as int) as "avg_price",
      sum (CAST ("COL_8" as double)) / CAST (sum (CAST ("COL_5" as int)) as int) as "avg_disc",
      CAST (sum (CAST ("COL_7" as int)) as int) as "count_order"
    FROM
      (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4",
          "COL_5",
          "COL_6",
          "COL_7",
          "COL_8",
          "COL_9",
          "COL_10"
        FROM
          "_tmp_12504932716845563831_0136"
      )
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
                      customer.c_mktsegment::string = 'BUILDING'::string
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
                            orders.o_orderstatus::string -> o_orderstatus,
                            orders.o_totalprice::double -> o_totalprice,
                            orders.o_orderdate::datetime -> o_orderdate,
                            orders.o_orderpriority::string -> o_orderpriority,
                            orders.o_clerk::string -> o_clerk,
                            orders.o_shippriority::int -> o_shippriority,
                            orders.o_comment::string -> o_comment
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
                          lineitem.l_returnflag::string -> l_returnflag,
                          lineitem.l_linestatus::string -> l_linestatus,
                          lineitem.l_shipdate::datetime -> l_shipdate,
                          lineitem.l_commitdate::datetime -> l_commitdate,
                          lineitem.l_receiptdate::datetime -> l_receiptdate,
                          lineitem.l_shipinstruct::string -> l_shipinstruct,
                          lineitem.l_shipmode::string -> l_shipmode,
                          lineitem.l_comment::string -> l_comment
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
    CAST (
      (
        "lineitem"."COL_6" * (CAST(1 AS int) - "lineitem"."COL_7")
      ) as double
    )
  ) as "sum_1"
FROM
  "customer"
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7",
      "COL_8",
      "COL_9"
    FROM
      "_tmp_5667582682556364960_0136"
  ) as "orders" ON CAST(true AS bool)
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7",
      "COL_8",
      "COL_9",
      "COL_10",
      "COL_11",
      "COL_12",
      "COL_13",
      "COL_14",
      "COL_15",
      "COL_16"
    FROM
      "_tmp_5667582682556364960_1136"
  ) as "lineitem" ON CAST(true AS bool)
WHERE
  "customer"."c_mktsegment" = CAST('BUILDING' AS string)
  and "customer"."c_custkey" = "orders"."COL_2"
  and "lineitem"."COL_0" = "orders"."COL_0"
  and "orders"."COL_5" < CAST('1995-03-15 0:00:00.0 +00:00:00' AS datetime)
  and "lineitem"."COL_11" > CAST('1995-03-15 0:00:00.0 +00:00:00' AS datetime)
GROUP BY
  "lineitem"."COL_0",
  "orders"."COL_5",
  "orders"."COL_8"
''
plan:
    [0] SCAN TABLE _tmp_5667582682556364960_0136 (~983040 rows)
        [0] SEARCH TABLE customer USING PRIMARY KEY (c_custkey=?) (~1 row)
            [0] SCAN TABLE _tmp_5667582682556364960_1136 (~983040 rows)
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
      sum (CAST ("COL_3" as double)) as "revenue",
      "COL_1" as "o_orderdate",
      "COL_2" as "o_shippriority"
    FROM
      (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3"
        FROM
          "_tmp_5796134220943901769_2136"
      )
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
projection (n_name::string, revenue::double)
  order by (revenue::double desc)
    scan
      projection (
        gr_expr_1::string -> n_name,
        sum(sum_1::double::double)::double -> revenue
      )
        group by (gr_expr_1::string)
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              nation.n_name::string -> gr_expr_1,
              sum(
                (
                  lineitem.l_extendedprice::double * (1::int - lineitem.l_discount::double)
                )::double
              )::double -> sum_1
            )
              group by (nation.n_name::string)
                selection (
                  (
                    customer.c_custkey::int = orders.o_custkey::int
                    and lineitem.l_orderkey::int = orders.o_orderkey::int
                    and lineitem.l_suppkey::int = supplier.s_suppkey::int
                    and customer.c_nationkey::int = supplier.s_nationkey::int
                    and supplier.s_nationkey::int = nation.n_nationkey::int
                    and nation.n_regionkey::int = region.r_regionkey::int
                    and region.r_name::string = 'ASIA'::string
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
                                orders.o_orderstatus::string -> o_orderstatus,
                                orders.o_totalprice::double -> o_totalprice,
                                orders.o_orderdate::datetime -> o_orderdate,
                                orders.o_orderpriority::string -> o_orderpriority,
                                orders.o_clerk::string -> o_clerk,
                                orders.o_shippriority::int -> o_shippriority,
                                orders.o_comment::string -> o_comment
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
                              lineitem.l_returnflag::string -> l_returnflag,
                              lineitem.l_linestatus::string -> l_linestatus,
                              lineitem.l_shipdate::datetime -> l_shipdate,
                              lineitem.l_commitdate::datetime -> l_commitdate,
                              lineitem.l_receiptdate::datetime -> l_receiptdate,
                              lineitem.l_shipinstruct::string -> l_shipinstruct,
                              lineitem.l_shipmode::string -> l_shipmode,
                              lineitem.l_comment::string -> l_comment
                            )
                              scan lineitem
                        motion [policy: full, program: ReshardIfNeeded]
                          projection (
                            supplier.s_suppkey::int -> s_suppkey,
                            supplier.bucket_id::int -> bucket_id,
                            supplier.s_name::string -> s_name,
                            supplier.s_address::string -> s_address,
                            supplier.s_nationkey::int -> s_nationkey,
                            supplier.s_phone::string -> s_phone,
                            supplier.s_acctbal::double -> s_acctbal,
                            supplier.s_comment::string -> s_comment
                          )
                            scan supplier
                      motion [policy: full, program: ReshardIfNeeded]
                        projection (
                          nation.n_nationkey::int -> n_nationkey,
                          nation.bucket_id::int -> bucket_id,
                          nation.n_name::string -> n_name,
                          nation.n_regionkey::int -> n_regionkey,
                          nation.n_comment::string -> n_comment
                        )
                          scan nation
                    motion [policy: full, program: ReshardIfNeeded]
                      projection (
                        region.r_regionkey::int -> r_regionkey,
                        region.bucket_id::int -> bucket_id,
                        region.r_name::string -> r_name,
                        region.r_comment::string -> r_comment
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
    CAST (
      (
        "lineitem"."COL_6" * (CAST(1 AS int) - "lineitem"."COL_7")
      ) as double
    )
  ) as "sum_1"
FROM
  "customer"
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7",
      "COL_8",
      "COL_9"
    FROM
      "_tmp_16147029196218747141_0136"
  ) as "orders" ON CAST(true AS bool)
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7",
      "COL_8",
      "COL_9",
      "COL_10",
      "COL_11",
      "COL_12",
      "COL_13",
      "COL_14",
      "COL_15",
      "COL_16"
    FROM
      "_tmp_16147029196218747141_1136"
  ) as "lineitem" ON CAST(true AS bool)
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7"
    FROM
      "_tmp_16147029196218747141_2136"
  ) as "supplier" ON CAST(true AS bool)
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4"
    FROM
      "_tmp_16147029196218747141_3136"
  ) as "nation" ON CAST(true AS bool)
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3"
    FROM
      "_tmp_16147029196218747141_4136"
  ) as "region" ON CAST(true AS bool)
WHERE
  "customer"."c_custkey" = "orders"."COL_2"
  and "lineitem"."COL_0" = "orders"."COL_0"
  and "lineitem"."COL_2" = "supplier"."COL_0"
  and "customer"."c_nationkey" = "supplier"."COL_4"
  and "supplier"."COL_4" = "nation"."COL_0"
  and "nation"."COL_3" = "region"."COL_0"
  and "region"."COL_2" = CAST('ASIA' AS string)
  and "orders"."COL_5" >= CAST('1994-01-01 0:00:00.0 +00:00:00' AS datetime)
  and "orders"."COL_5" < CAST('1995-01-01 0:00:00.0 +00:00:00' AS datetime)
GROUP BY
  "nation"."COL_2"
''
plan:
    [0] SCAN TABLE _tmp_16147029196218747141_0136 (~917504 rows)
        [0] SEARCH TABLE customer USING PRIMARY KEY (c_custkey=?) (~1 row)
            [0] SEARCH TABLE _tmp_16147029196218747141_1136 
                 USING EPHEMERAL INDEX 
                 (COL_0=?) 
                 (~20 rows)
                [0] SEARCH TABLE _tmp_16147029196218747141_2136 
                     USING EPHEMERAL INDEX 
                     (COL_4=? AND COL_0=?) 
                     (~20 rows)
                    [0] SEARCH TABLE _tmp_16147029196218747141_3136 
                         USING EPHEMERAL INDEX 
                         (COL_0=?) 
                         (~20 rows)
                        [0] SEARCH TABLE _tmp_16147029196218747141_4136 
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
      sum (CAST ("COL_1" as double)) as "revenue"
    FROM
      (
        SELECT
          "COL_0",
          "COL_1"
        FROM
          "_tmp_7369492615622028742_5136"
      )
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
  supp_nation::string,
  cust_nation::string,
  l_year::string,
  revenue::double
)
  order by (
    supp_nation::string,
    cust_nation::string,
    l_year::string
  )
    scan
      projection (
        gr_expr_1::string -> supp_nation,
        gr_expr_2::string -> cust_nation,
        gr_expr_3::string -> l_year,
        sum(sum_1::double::double)::double -> revenue
      )
        group by (
          gr_expr_1::string,
          gr_expr_2::string,
          gr_expr_3::string
        )
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              shipping.supp_nation::string -> gr_expr_1,
              shipping.cust_nation::string -> gr_expr_2,
              shipping.l_year::string -> gr_expr_3,
              sum(shipping.volume::double::double)::double -> sum_1
            )
              group by (
                shipping.supp_nation::string,
                shipping.cust_nation::string,
                shipping.l_year::string
              )
                scan shipping
                  projection (
                    n1.n_name::string -> supp_nation,
                    n2.n_name::string -> cust_nation,
                    to_char(
                      lineitem.l_shipdate::datetime::datetime,
                      '%Y'::string
                    )::string -> l_year,
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
                            n1.n_name::string = 'FRANCE'::string
                            and n2.n_name::string = 'GERMANY'::string
                          ) or (
                            n1.n_name::string = 'GERMANY'::string
                            and n2.n_name::string = 'FRANCE'::string
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
                                    lineitem.l_returnflag::string -> l_returnflag,
                                    lineitem.l_linestatus::string -> l_linestatus,
                                    lineitem.l_shipdate::datetime -> l_shipdate,
                                    lineitem.l_commitdate::datetime -> l_commitdate,
                                    lineitem.l_receiptdate::datetime -> l_receiptdate,
                                    lineitem.l_shipinstruct::string -> l_shipinstruct,
                                    lineitem.l_shipmode::string -> l_shipmode,
                                    lineitem.l_comment::string -> l_comment
                                  )
                                    scan lineitem
                              motion [policy: full, program: ReshardIfNeeded]
                                projection (
                                  orders.o_orderkey::int -> o_orderkey,
                                  orders.bucket_id::int -> bucket_id,
                                  orders.o_custkey::int -> o_custkey,
                                  orders.o_orderstatus::string -> o_orderstatus,
                                  orders.o_totalprice::double -> o_totalprice,
                                  orders.o_orderdate::datetime -> o_orderdate,
                                  orders.o_orderpriority::string -> o_orderpriority,
                                  orders.o_clerk::string -> o_clerk,
                                  orders.o_shippriority::int -> o_shippriority,
                                  orders.o_comment::string -> o_comment
                                )
                                  scan orders
                            motion [policy: full, program: ReshardIfNeeded]
                              projection (
                                customer.c_custkey::int -> c_custkey,
                                customer.bucket_id::int -> bucket_id,
                                customer.c_name::string -> c_name,
                                customer.c_address::string -> c_address,
                                customer.c_nationkey::int -> c_nationkey,
                                customer.c_phone::string -> c_phone,
                                customer.c_acctbal::double -> c_acctbal,
                                customer.c_mktsegment::string -> c_mktsegment,
                                customer.c_comment::string -> c_comment
                              )
                                scan customer
                          motion [policy: full, program: ReshardIfNeeded]
                            projection (
                              n1.n_nationkey::int -> n_nationkey,
                              n1.bucket_id::int -> bucket_id,
                              n1.n_name::string -> n_name,
                              n1.n_regionkey::int -> n_regionkey,
                              n1.n_comment::string -> n_comment
                            )
                              scan nation -> n1
                        motion [policy: full, program: ReshardIfNeeded]
                          projection (
                            n2.n_nationkey::int -> n_nationkey,
                            n2.bucket_id::int -> bucket_id,
                            n2.n_name::string -> n_name,
                            n2.n_regionkey::int -> n_regionkey,
                            n2.n_comment::string -> n_comment
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
  sum (CAST ("shipping"."volume" as double)) as "sum_1"
FROM
  (
    SELECT
      "n1"."COL_2" as "supp_nation",
      "n2"."COL_2" as "cust_nation",
      "to_char" (
        CAST ("lineitem"."COL_11" as datetime),
        CAST('%Y' AS string)
      ) as "l_year",
      "lineitem"."COL_6" * (CAST(1 AS int) - "lineitem"."COL_7") as "volume"
    FROM
      "supplier"
      INNER JOIN (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4",
          "COL_5",
          "COL_6",
          "COL_7",
          "COL_8",
          "COL_9",
          "COL_10",
          "COL_11",
          "COL_12",
          "COL_13",
          "COL_14",
          "COL_15",
          "COL_16"
        FROM
          "_tmp_8287534967795779098_0136"
      ) as "lineitem" ON CAST(true AS bool)
      INNER JOIN (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4",
          "COL_5",
          "COL_6",
          "COL_7",
          "COL_8",
          "COL_9"
        FROM
          "_tmp_8287534967795779098_1136"
      ) as "orders" ON CAST(true AS bool)
      INNER JOIN (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4",
          "COL_5",
          "COL_6",
          "COL_7",
          "COL_8"
        FROM
          "_tmp_8287534967795779098_2136"
      ) as "customer" ON CAST(true AS bool)
      INNER JOIN (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4"
        FROM
          "_tmp_8287534967795779098_3136"
      ) as "n1" ON CAST(true AS bool)
      INNER JOIN (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4"
        FROM
          "_tmp_8287534967795779098_4136"
      ) as "n2" ON CAST(true AS bool)
    WHERE
      "supplier"."s_suppkey" = "lineitem"."COL_2"
      and "orders"."COL_0" = "lineitem"."COL_0"
      and "customer"."COL_0" = "orders"."COL_2"
      and "supplier"."s_nationkey" = "n1"."COL_0"
      and "customer"."COL_4" = "n2"."COL_0"
      and (
        "n1"."COL_2" = CAST('FRANCE' AS string)
        and "n2"."COL_2" = CAST('GERMANY' AS string)
        or "n1"."COL_2" = CAST('GERMANY' AS string)
        and "n2"."COL_2" = CAST('FRANCE' AS string)
      )
      and "lineitem"."COL_11" >= CAST('1995-01-01 0:00:00.0 +00:00:00' AS datetime)
      and "lineitem"."COL_11" <= CAST('1996-12-31 0:00:00.0 +00:00:00' AS datetime)
  ) as "shipping"
GROUP BY
  "shipping"."supp_nation",
  "shipping"."cust_nation",
  "shipping"."l_year"
''
plan:
    [0] SCAN TABLE _tmp_8287534967795779098_0136 (~917504 rows)
        [0] SEARCH TABLE supplier USING PRIMARY KEY (s_suppkey=?) (~1 row)
            [0] SEARCH TABLE _tmp_8287534967795779098_1136 
                 USING EPHEMERAL INDEX 
                 (COL_0=?) 
                 (~20 rows)
                [0] SEARCH TABLE _tmp_8287534967795779098_2136 
                     USING EPHEMERAL INDEX 
                     (COL_0=?) 
                     (~20 rows)
                    [0] SEARCH TABLE _tmp_8287534967795779098_3136 
                         USING EPHEMERAL INDEX 
                         (COL_0=?) 
                         (~20 rows)
                        [0] SEARCH TABLE _tmp_8287534967795779098_4136 
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
      sum (CAST ("COL_3" as double)) as "revenue"
    FROM
      (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3"
        FROM
          "_tmp_12566993161641042020_5136"
      )
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
projection (o_year::string, mkt_share::double)
  order by (o_year::string)
    scan
      projection (
        gr_expr_1::string -> o_year,
        sum(sum_1::double::double)::double / sum(sum_2::double::double)::double -> mkt_share
      )
        group by (gr_expr_1::string)
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              all_nations.o_year::string -> gr_expr_1,
              sum(
                case 
                  when all_nations.nation::string = 'BRAZIL'::string then all_nations.volume::double
                  else 0::int
                end::double
              )::double -> sum_1,
              sum(all_nations.volume::double::double)::double -> sum_2
            )
              group by (all_nations.o_year::string)
                scan all_nations
                  projection (
                    to_char(
                      orders.o_orderdate::datetime::datetime,
                      '%Y'::string
                    )::string -> o_year,
                    lineitem.l_extendedprice::double * (1::int - lineitem.l_discount::double) -> volume,
                    n2.n_name::string -> nation
                  )
                    selection (
                      (
                        part.p_partkey::int = lineitem.l_partkey::int
                        and supplier.s_suppkey::int = lineitem.l_suppkey::int
                        and lineitem.l_orderkey::int = orders.o_orderkey::int
                        and orders.o_custkey::int = customer.c_custkey::int
                        and customer.c_nationkey::int = n1.n_nationkey::int
                        and n1.n_regionkey::int = region.r_regionkey::int
                        and region.r_name::string = 'AMERICA'::string
                        and supplier.s_nationkey::int = n2.n_nationkey::int
                        and orders.o_orderdate::datetime >= '1995-01-01 0:00:00.0 +00:00:00'::datetime
                        and orders.o_orderdate::datetime <= '1996-12-31 0:00:00.0 +00:00:00'::datetime
                        and part.p_type::string = 'ECONOMY ANODIZED STEEL'::string
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
                                        supplier.s_name::string -> s_name,
                                        supplier.s_address::string -> s_address,
                                        supplier.s_nationkey::int -> s_nationkey,
                                        supplier.s_phone::string -> s_phone,
                                        supplier.s_acctbal::double -> s_acctbal,
                                        supplier.s_comment::string -> s_comment
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
                                      lineitem.l_returnflag::string -> l_returnflag,
                                      lineitem.l_linestatus::string -> l_linestatus,
                                      lineitem.l_shipdate::datetime -> l_shipdate,
                                      lineitem.l_commitdate::datetime -> l_commitdate,
                                      lineitem.l_receiptdate::datetime -> l_receiptdate,
                                      lineitem.l_shipinstruct::string -> l_shipinstruct,
                                      lineitem.l_shipmode::string -> l_shipmode,
                                      lineitem.l_comment::string -> l_comment
                                    )
                                      scan lineitem
                                motion [policy: full, program: ReshardIfNeeded]
                                  projection (
                                    orders.o_orderkey::int -> o_orderkey,
                                    orders.bucket_id::int -> bucket_id,
                                    orders.o_custkey::int -> o_custkey,
                                    orders.o_orderstatus::string -> o_orderstatus,
                                    orders.o_totalprice::double -> o_totalprice,
                                    orders.o_orderdate::datetime -> o_orderdate,
                                    orders.o_orderpriority::string -> o_orderpriority,
                                    orders.o_clerk::string -> o_clerk,
                                    orders.o_shippriority::int -> o_shippriority,
                                    orders.o_comment::string -> o_comment
                                  )
                                    scan orders
                              motion [policy: full, program: ReshardIfNeeded]
                                projection (
                                  customer.c_custkey::int -> c_custkey,
                                  customer.bucket_id::int -> bucket_id,
                                  customer.c_name::string -> c_name,
                                  customer.c_address::string -> c_address,
                                  customer.c_nationkey::int -> c_nationkey,
                                  customer.c_phone::string -> c_phone,
                                  customer.c_acctbal::double -> c_acctbal,
                                  customer.c_mktsegment::string -> c_mktsegment,
                                  customer.c_comment::string -> c_comment
                                )
                                  scan customer
                            motion [policy: full, program: ReshardIfNeeded]
                              projection (
                                n1.n_nationkey::int -> n_nationkey,
                                n1.bucket_id::int -> bucket_id,
                                n1.n_name::string -> n_name,
                                n1.n_regionkey::int -> n_regionkey,
                                n1.n_comment::string -> n_comment
                              )
                                scan nation -> n1
                          motion [policy: full, program: ReshardIfNeeded]
                            projection (
                              n2.n_nationkey::int -> n_nationkey,
                              n2.bucket_id::int -> bucket_id,
                              n2.n_name::string -> n_name,
                              n2.n_regionkey::int -> n_regionkey,
                              n2.n_comment::string -> n_comment
                            )
                              scan nation -> n2
                        motion [policy: full, program: ReshardIfNeeded]
                          projection (
                            region.r_regionkey::int -> r_regionkey,
                            region.bucket_id::int -> bucket_id,
                            region.r_name::string -> r_name,
                            region.r_comment::string -> r_comment
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
    CAST (
      CASE
        WHEN "all_nations"."nation" = CAST('BRAZIL' AS string) THEN "all_nations"."volume"
        ELSE CAST(0 AS int)
      END as double
    )
  ) as "sum_1",
  sum (CAST ("all_nations"."volume" as double)) as "sum_2"
FROM
  (
    SELECT
      "to_char" (
        CAST ("orders"."COL_5" as datetime),
        CAST('%Y' AS string)
      ) as "o_year",
      "lineitem"."COL_6" * (CAST(1 AS int) - "lineitem"."COL_7") as "volume",
      "n2"."COL_2" as "nation"
    FROM
      "part"
      INNER JOIN (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4",
          "COL_5",
          "COL_6",
          "COL_7"
        FROM
          "_tmp_16404978828759991003_0136"
      ) as "supplier" ON CAST(true AS bool)
      INNER JOIN (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4",
          "COL_5",
          "COL_6",
          "COL_7",
          "COL_8",
          "COL_9",
          "COL_10",
          "COL_11",
          "COL_12",
          "COL_13",
          "COL_14",
          "COL_15",
          "COL_16"
        FROM
          "_tmp_16404978828759991003_1136"
      ) as "lineitem" ON CAST(true AS bool)
      INNER JOIN (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4",
          "COL_5",
          "COL_6",
          "COL_7",
          "COL_8",
          "COL_9"
        FROM
          "_tmp_16404978828759991003_2136"
      ) as "orders" ON CAST(true AS bool)
      INNER JOIN (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4",
          "COL_5",
          "COL_6",
          "COL_7",
          "COL_8"
        FROM
          "_tmp_16404978828759991003_3136"
      ) as "customer" ON CAST(true AS bool)
      INNER JOIN (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4"
        FROM
          "_tmp_16404978828759991003_4136"
      ) as "n1" ON CAST(true AS bool)
      INNER JOIN (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4"
        FROM
          "_tmp_16404978828759991003_5136"
      ) as "n2" ON CAST(true AS bool)
      INNER JOIN (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3"
        FROM
          "_tmp_16404978828759991003_6136"
      ) as "region" ON CAST(true AS bool)
    WHERE
      "part"."p_partkey" = "lineitem"."COL_1"
      and "supplier"."COL_0" = "lineitem"."COL_2"
      and "lineitem"."COL_0" = "orders"."COL_0"
      and "orders"."COL_2" = "customer"."COL_0"
      and "customer"."COL_4" = "n1"."COL_0"
      and "n1"."COL_3" = "region"."COL_0"
      and "region"."COL_2" = CAST('AMERICA' AS string)
      and "supplier"."COL_4" = "n2"."COL_0"
      and "orders"."COL_5" >= CAST('1995-01-01 0:00:00.0 +00:00:00' AS datetime)
      and "orders"."COL_5" <= CAST('1996-12-31 0:00:00.0 +00:00:00' AS datetime)
      and "part"."p_type" = CAST('ECONOMY ANODIZED STEEL' AS string)
  ) as "all_nations"
GROUP BY
  "all_nations"."o_year"
''
plan:
    [0] SCAN TABLE _tmp_16404978828759991003_1136 (~1048576 rows)
        [0] SEARCH TABLE part USING PRIMARY KEY (p_partkey=?) (~1 row)
            [0] SEARCH TABLE _tmp_16404978828759991003_0136 
                 USING EPHEMERAL INDEX 
                 (COL_0=?) 
                 (~20 rows)
                [0] SEARCH TABLE _tmp_16404978828759991003_2136 
                     USING EPHEMERAL INDEX 
                     (COL_0=?) 
                     (~20 rows)
                    [0] SEARCH TABLE _tmp_16404978828759991003_3136 
                         USING EPHEMERAL INDEX 
                         (COL_0=?) 
                         (~20 rows)
                        [0] SEARCH TABLE _tmp_16404978828759991003_4136 
                             USING EPHEMERAL INDEX 
                             (COL_0=?) 
                             (~20 rows)
                            [0] SEARCH TABLE _tmp_16404978828759991003_5136 
                                 USING EPHEMERAL INDEX 
                                 (COL_0=?) 
                                 (~20 rows)
                                [0] SEARCH TABLE _tmp_16404978828759991003_6136 
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
      sum (CAST ("COL_1" as double)) / sum (CAST ("COL_2" as double)) as "mkt_share"
    FROM
      (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2"
        FROM
          "_tmp_13492713738046281510_7136"
      )
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
    c_name::string,
    revenue::double,
    c_acctbal::double,
    n_name::string,
    c_address::string,
    c_phone::string,
    c_comment::string
  )
    order by (revenue::double desc)
      scan
        projection (
          gr_expr_1::int -> c_custkey,
          gr_expr_2::string -> c_name,
          sum(sum_1::double::double)::double -> revenue,
          gr_expr_3::double -> c_acctbal,
          gr_expr_5::string -> n_name,
          gr_expr_6::string -> c_address,
          gr_expr_4::string -> c_phone,
          gr_expr_7::string -> c_comment
        )
          group by (
            gr_expr_1::int,
            gr_expr_2::string,
            gr_expr_3::double,
            gr_expr_4::string,
            gr_expr_5::string,
            gr_expr_6::string,
            gr_expr_7::string
          )
            motion [policy: full, program: ReshardIfNeeded]
              projection (
                customer.c_custkey::int -> gr_expr_1,
                customer.c_name::string -> gr_expr_2,
                customer.c_acctbal::double -> gr_expr_3,
                customer.c_phone::string -> gr_expr_4,
                nation.n_name::string -> gr_expr_5,
                customer.c_address::string -> gr_expr_6,
                customer.c_comment::string -> gr_expr_7,
                sum(
                  (
                    lineitem.l_extendedprice::double * (1::int - lineitem.l_discount::double)
                  )::double
                )::double -> sum_1
              )
                group by (
                  customer.c_custkey::int,
                  customer.c_name::string,
                  customer.c_acctbal::double,
                  customer.c_phone::string,
                  nation.n_name::string,
                  customer.c_address::string,
                  customer.c_comment::string
                )
                  selection (
                    (
                      customer.c_custkey::int = orders.o_custkey::int
                      and lineitem.l_orderkey::int = orders.o_orderkey::int
                      and orders.o_orderdate::datetime >= '1993-10-01 0:00:00.0 +00:00:00'::datetime
                      and orders.o_orderdate::datetime < '1994-01-01 0:00:00.0 +00:00:00'::datetime
                      and lineitem.l_returnflag::string = 'R'::string
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
                              orders.o_orderstatus::string -> o_orderstatus,
                              orders.o_totalprice::double -> o_totalprice,
                              orders.o_orderdate::datetime -> o_orderdate,
                              orders.o_orderpriority::string -> o_orderpriority,
                              orders.o_clerk::string -> o_clerk,
                              orders.o_shippriority::int -> o_shippriority,
                              orders.o_comment::string -> o_comment
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
                            lineitem.l_returnflag::string -> l_returnflag,
                            lineitem.l_linestatus::string -> l_linestatus,
                            lineitem.l_shipdate::datetime -> l_shipdate,
                            lineitem.l_commitdate::datetime -> l_commitdate,
                            lineitem.l_receiptdate::datetime -> l_receiptdate,
                            lineitem.l_shipinstruct::string -> l_shipinstruct,
                            lineitem.l_shipmode::string -> l_shipmode,
                            lineitem.l_comment::string -> l_comment
                          )
                            scan lineitem
                      motion [policy: full, program: ReshardIfNeeded]
                        projection (
                          nation.n_nationkey::int -> n_nationkey,
                          nation.bucket_id::int -> bucket_id,
                          nation.n_name::string -> n_name,
                          nation.n_regionkey::int -> n_regionkey,
                          nation.n_comment::string -> n_comment
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
    CAST (
      (
        "lineitem"."COL_6" * (CAST(1 AS int) - "lineitem"."COL_7")
      ) as double
    )
  ) as "sum_1"
FROM
  "customer"
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7",
      "COL_8",
      "COL_9"
    FROM
      "_tmp_6752974385567748145_0136"
  ) as "orders" ON CAST(true AS bool)
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7",
      "COL_8",
      "COL_9",
      "COL_10",
      "COL_11",
      "COL_12",
      "COL_13",
      "COL_14",
      "COL_15",
      "COL_16"
    FROM
      "_tmp_6752974385567748145_1136"
  ) as "lineitem" ON CAST(true AS bool)
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4"
    FROM
      "_tmp_6752974385567748145_2136"
  ) as "nation" ON CAST(true AS bool)
WHERE
  "customer"."c_custkey" = "orders"."COL_2"
  and "lineitem"."COL_0" = "orders"."COL_0"
  and "orders"."COL_5" >= CAST('1993-10-01 0:00:00.0 +00:00:00' AS datetime)
  and "orders"."COL_5" < CAST('1994-01-01 0:00:00.0 +00:00:00' AS datetime)
  and "lineitem"."COL_9" = CAST('R' AS string)
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
        [0] SCAN TABLE _tmp_6752974385567748145_1136 (~262144 rows)
            [0] SEARCH TABLE _tmp_6752974385567748145_0136 
                 USING EPHEMERAL INDEX 
                 (COL_0=? AND COL_2=?) 
                 (~20 rows)
                [0] SEARCH TABLE _tmp_6752974385567748145_2136 
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
      sum (CAST ("COL_7" as double)) as "revenue",
      "COL_2" as "c_acctbal",
      "COL_4" as "n_name",
      "COL_5" as "c_address",
      "COL_3" as "c_phone",
      "COL_6" as "c_comment"
    FROM
      (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4",
          "COL_5",
          "COL_6",
          "COL_7"
        FROM
          "_tmp_17934649577571891331_3136"
      )
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
                      and nation.n_name::string = 'GERMANY'::string
                    )
                  )
                    join on (true::bool)
                      join on (true::bool)
                        scan partsupp
                        motion [policy: full, program: ReshardIfNeeded]
                          projection (
                            supplier.s_suppkey::int -> s_suppkey,
                            supplier.bucket_id::int -> bucket_id,
                            supplier.s_name::string -> s_name,
                            supplier.s_address::string -> s_address,
                            supplier.s_nationkey::int -> s_nationkey,
                            supplier.s_phone::string -> s_phone,
                            supplier.s_acctbal::double -> s_acctbal,
                            supplier.s_comment::string -> s_comment
                          )
                            scan supplier
                      motion [policy: full, program: ReshardIfNeeded]
                        projection (
                          nation.n_nationkey::int -> n_nationkey,
                          nation.bucket_id::int -> bucket_id,
                          nation.n_name::string -> n_name,
                          nation.n_regionkey::int -> n_regionkey,
                          nation.n_comment::string -> n_comment
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
                and nation.n_name::string = 'GERMANY'::string
              )
            )
              join on (true::bool)
                join on (true::bool)
                  scan partsupp
                  motion [policy: full, program: ReshardIfNeeded]
                    projection (
                      supplier.s_suppkey::int -> s_suppkey,
                      supplier.bucket_id::int -> bucket_id,
                      supplier.s_name::string -> s_name,
                      supplier.s_address::string -> s_address,
                      supplier.s_nationkey::int -> s_nationkey,
                      supplier.s_phone::string -> s_phone,
                      supplier.s_acctbal::double -> s_acctbal,
                      supplier.s_comment::string -> s_comment
                    )
                      scan supplier
                motion [policy: full, program: ReshardIfNeeded]
                  projection (
                    nation.n_nationkey::int -> n_nationkey,
                    nation.bucket_id::int -> bucket_id,
                    nation.n_name::string -> n_name,
                    nation.n_regionkey::int -> n_regionkey,
                    nation.n_comment::string -> n_comment
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
    CAST (
      (
        "partsupp"."ps_supplycost" * "partsupp"."ps_availqty"
      ) as double
    )
  ) as "sum_1"
FROM
  "partsupp"
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7"
    FROM
      "_tmp_17389738504606850982_0136"
  ) as "supplier" ON CAST(true AS bool)
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4"
    FROM
      "_tmp_17389738504606850982_1136"
  ) as "nation" ON CAST(true AS bool)
WHERE
  "partsupp"."ps_suppkey" = "supplier"."COL_0"
  and "supplier"."COL_4" = "nation"."COL_0"
  and "nation"."COL_2" = CAST('GERMANY' AS string)
GROUP BY
  "partsupp"."ps_partkey"
''
plan:
    [0] SCAN TABLE partsupp (~1048576 rows)
        [0] SCAN TABLE _tmp_17389738504606850982_1136 (~262144 rows)
            [0] SEARCH TABLE _tmp_17389738504606850982_0136 
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
    CAST (
      (
        "partsupp"."ps_supplycost" * "partsupp"."ps_availqty"
      ) as double
    )
  ) as "sum_1"
FROM
  "partsupp"
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7"
    FROM
      "_tmp_17869557022914879301_2136"
  ) as "supplier" ON CAST(true AS bool)
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4"
    FROM
      "_tmp_17869557022914879301_3136"
  ) as "nation" ON CAST(true AS bool)
WHERE
  "partsupp"."ps_suppkey" = "supplier"."COL_0"
  and "supplier"."COL_4" = "nation"."COL_0"
  and "nation"."COL_2" = CAST('GERMANY' AS string)
''
plan:
    [0] SCAN TABLE _tmp_17869557022914879301_3136 (~262144 rows)
        [0] SCAN TABLE partsupp (~1048576 rows)
            [0] SEARCH TABLE _tmp_17869557022914879301_2136 
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
  sum (CAST ("COL_0" as double)) * CAST(0.0001 AS decimal) as "col_1"
FROM
  (
    SELECT
      "COL_0"
    FROM
      "_tmp_10305140153159277659_4136"
  )
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
      sum (CAST ("COL_1" as double)) as "value"
    FROM
      (
        SELECT
          "COL_0",
          "COL_1"
        FROM
          "_tmp_16780076466944764098_5136"
      )
    GROUP BY
      "COL_0"
    HAVING
      sum (CAST ("COL_1" as double)) > (
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
  l_shipmode::string,
  high_line_count::decimal,
  low_line_count::decimal
)
  order by (l_shipmode::string)
    scan
      projection (
        gr_expr_1::string -> l_shipmode,
        sum(sum_1::decimal::decimal)::decimal -> high_line_count,
        sum(sum_2::decimal::decimal)::decimal -> low_line_count
      )
        group by (gr_expr_1::string)
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              lineitem.l_shipmode::string -> gr_expr_1,
              sum(
                case 
                  when orders.o_orderpriority::string = '1-URGENT'::string or orders.o_orderpriority::string = '2-HIGH'::string then 1::int
                  else 0::int
                end::int
              )::decimal -> sum_1,
              sum(
                case 
                  when (
                    orders.o_orderpriority::string <> '1-URGENT'::string
                    and orders.o_orderpriority::string <> '2-HIGH'::string
                  ) then 1::int
                  else 0::int
                end::int
              )::decimal -> sum_2
            )
              group by (lineitem.l_shipmode::string)
                selection (
                  (
                    lineitem.l_shipmode::string in ROW('MAIL'::string, 'SHIP'::string)
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
                        orders.o_orderstatus::string -> o_orderstatus,
                        orders.o_totalprice::double -> o_totalprice,
                        orders.o_orderdate::datetime -> o_orderdate,
                        orders.o_orderpriority::string -> o_orderpriority,
                        orders.o_clerk::string -> o_clerk,
                        orders.o_shippriority::int -> o_shippriority,
                        orders.o_comment::string -> o_comment
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
    CAST (
      CASE
        WHEN "orders"."COL_6" = CAST('1-URGENT' AS string)
        or "orders"."COL_6" = CAST('2-HIGH' AS string) THEN CAST(1 AS int)
        ELSE CAST(0 AS int)
      END as int
    )
  ) as "sum_1",
  sum (
    CAST (
      CASE
        WHEN "orders"."COL_6" <> CAST('1-URGENT' AS string)
        and "orders"."COL_6" <> CAST('2-HIGH' AS string) THEN CAST(1 AS int)
        ELSE CAST(0 AS int)
      END as int
    )
  ) as "sum_2"
FROM
  "lineitem"
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7",
      "COL_8",
      "COL_9"
    FROM
      "_tmp_16541552721638505593_0136"
  ) as "orders" ON "lineitem"."l_orderkey" = "orders"."COL_0"
WHERE
  "lineitem"."l_shipmode" in (CAST('MAIL' AS string), CAST('SHIP' AS string))
  and "lineitem"."l_commitdate" < "lineitem"."l_receiptdate"
  and "lineitem"."l_shipdate" < "lineitem"."l_commitdate"
  and "lineitem"."l_receiptdate" >= CAST('1994-01-01 0:00:00.0 +00:00:00' AS datetime)
  and "lineitem"."l_receiptdate" < CAST('1995-01-01 0:00:00.0 +00:00:00' AS datetime)
GROUP BY
  "lineitem"."l_shipmode"
''
plan:
    [0] SCAN TABLE _tmp_16541552721638505593_0136 (~1048576 rows)
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
      sum (CAST ("COL_1" as decimal)) as "high_line_count",
      sum (CAST ("COL_2" as decimal)) as "low_line_count"
    FROM
      (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2"
        FROM
          "_tmp_13446821435794994891_1136"
      )
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
                          and not orders.o_comment::string::string LIKE '%special%requests%'::string ESCAPE '\'::string
                        )
                      )
                        scan customer
                        motion [policy: segment([ref(o_custkey)]), program: ReshardIfNeeded]
                          projection (
                            orders.o_orderkey::int -> o_orderkey,
                            orders.bucket_id::int -> bucket_id,
                            orders.o_custkey::int -> o_custkey,
                            orders.o_orderstatus::string -> o_orderstatus,
                            orders.o_totalprice::double -> o_totalprice,
                            orders.o_orderdate::datetime -> o_orderdate,
                            orders.o_orderpriority::string -> o_orderpriority,
                            orders.o_clerk::string -> o_clerk,
                            orders.o_shippriority::int -> o_shippriority,
                            orders.o_comment::string -> o_comment
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
  count (CAST ("orders"."COL_0" as int)) as "count_1"
FROM
  "customer"
  LEFT JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7",
      "COL_8",
      "COL_9"
    FROM
      "_tmp_2853321084796073973_0136"
  ) as "orders" ON "customer"."c_custkey" = "orders"."COL_2"
  and not CAST ("orders"."COL_9" as string) LIKE CAST('%special%requests%' AS string) ESCAPE CAST('\' AS string)
GROUP BY
  "customer"."c_custkey"
''
plan:
    [1] SCAN TABLE _tmp_2853321084796073973_0136 (~1048576 rows)
    [0] SCAN TABLE customer (~1048576 rows)
        [0] SCAN SUBQUERY 1 AS orders (~1 row)
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
          CAST (sum (CAST ("COL_1" as int)) as int) as "c_count"
        FROM
          (
            SELECT
              "COL_0",
              "COL_1"
            FROM
              "_tmp_9702922271480073019_1136"
          )
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
          when part.p_type::string::string LIKE 'PROMO%'::string ESCAPE '\'::string then lineitem.l_extendedprice::double * (1::int - lineitem.l_discount::double)
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
              part.p_name::string -> p_name,
              part.p_mfgr::string -> p_mfgr,
              part.p_brand::string -> p_brand,
              part.p_type::string -> p_type,
              part.p_size::int -> p_size,
              part.p_container::string -> p_container,
              part.p_retailprice::double -> p_retailprice,
              part.p_comment::string -> p_comment
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
    CAST (
      (
        "lineitem"."l_extendedprice" * (CAST(1 AS int) - "lineitem"."l_discount")
      ) as double
    )
  ) as "sum_2",
  sum (
    CAST (
      CASE
        WHEN CAST ("part"."COL_5" as string) LIKE CAST('PROMO%' AS string) ESCAPE CAST('\' AS string) THEN "lineitem"."l_extendedprice" * (CAST(1 AS int) - "lineitem"."l_discount")
        ELSE CAST(0 AS int)
      END as double
    )
  ) as "sum_1"
FROM
  "lineitem"
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7",
      "COL_8",
      "COL_9"
    FROM
      "_tmp_4254979314961859233_0136"
  ) as "part" ON CAST(true AS bool)
WHERE
  "lineitem"."l_partkey" = "part"."COL_0"
  and "lineitem"."l_shipdate" >= CAST('1995-09-01 0:00:00.0 +00:00:00' AS datetime)
  and "lineitem"."l_shipdate" < CAST('1995-10-01 0:00:00.0 +00:00:00' AS datetime)
''
plan:
    [0] SCAN TABLE lineitem (~917504 rows)
        [0] SCAN TABLE _tmp_4254979314961859233_0136 (~1048576 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  (
    CAST(100.00 AS decimal) * sum (CAST ("COL_1" as double))
  ) / sum (CAST ("COL_0" as double)) as "promo_revenue"
FROM
  (
    SELECT
      "COL_0",
      "COL_1"
    FROM
      "_tmp_12731488821819863187_1136"
  )
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
  p_brand::string,
  p_type::string,
  p_size::int,
  supplier_cnt::int
)
  order by (
    supplier_cnt::int desc,
    p_brand::string,
    p_type::string,
    p_size::int
  )
    scan
      projection (
        gr_expr_1::string -> p_brand,
        gr_expr_2::string -> p_type,
        gr_expr_3::int -> p_size,
        count(distinct gr_expr_4::int)::int -> supplier_cnt
      )
        group by (
          gr_expr_1::string,
          gr_expr_2::string,
          gr_expr_3::int
        )
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              part.p_brand::string -> gr_expr_1,
              part.p_type::string -> gr_expr_2,
              part.p_size::int -> gr_expr_3,
              partsupp.ps_suppkey::int::int -> gr_expr_4
            )
              group by (
                part.p_brand::string,
                part.p_type::string,
                part.p_size::int,
                partsupp.ps_suppkey::int::int
              )
                selection (
                  (
                    part.p_partkey::int = partsupp.ps_partkey::int
                    and part.p_brand::string <> 'Brand#45'::string
                    and not part.p_type::string::string LIKE 'MEDIUM POLISHED%'::string ESCAPE '\'::string
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
                        part.p_name::string -> p_name,
                        part.p_mfgr::string -> p_mfgr,
                        part.p_brand::string -> p_brand,
                        part.p_type::string -> p_type,
                        part.p_size::int -> p_size,
                        part.p_container::string -> p_container,
                        part.p_retailprice::double -> p_retailprice,
                        part.p_comment::string -> p_comment
                      )
                        scan part
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    scan
      projection (supplier.s_suppkey::int -> s_suppkey)
        selection (
          supplier.s_comment::string::string LIKE '%Customer%Complaints%'::string ESCAPE '\'::string
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
  CAST ("supplier"."s_comment" as string) LIKE CAST('%Customer%Complaints%' AS string) ESCAPE CAST('\' AS string)
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
  CAST ("partsupp"."ps_suppkey" as int) as "gr_expr_4"
FROM
  "partsupp"
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7",
      "COL_8",
      "COL_9"
    FROM
      "_tmp_14149410492156322724_0136"
  ) as "part" ON CAST(true AS bool)
WHERE
  "part"."COL_0" = "partsupp"."ps_partkey"
  and "part"."COL_4" <> CAST('Brand#45' AS string)
  and not CAST ("part"."COL_5" as string) LIKE CAST('MEDIUM POLISHED%' AS string) ESCAPE CAST('\' AS string)
  and "part"."COL_6" in (
    CAST(49 AS int),
    CAST(14 AS int),
    CAST(23 AS int),
    CAST(45 AS int),
    CAST(19 AS int),
    CAST(3 AS int),
    CAST(36 AS int),
    CAST(9 AS int)
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
  CAST ("partsupp"."ps_suppkey" as int)
''
plan:
    [0] SCAN TABLE _tmp_14149410492156322724_0136 (~851968 rows)
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
      (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3"
        FROM
          "_tmp_4785163345789539971_2136"
      )
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
    c_name::string,
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
          gr_expr_1::string -> c_name,
          gr_expr_2::int -> c_custkey,
          gr_expr_3::int -> o_orderkey,
          gr_expr_4::datetime -> o_orderdate,
          gr_expr_5::double -> o_totalprice,
          sum(sum_1::double::double)::double -> col_1
        )
          group by (
            gr_expr_1::string,
            gr_expr_2::int,
            gr_expr_3::int,
            gr_expr_4::datetime,
            gr_expr_5::double
          )
            motion [policy: full, program: ReshardIfNeeded]
              projection (
                customer.c_name::string -> gr_expr_1,
                customer.c_custkey::int -> gr_expr_2,
                orders.o_orderkey::int -> gr_expr_3,
                orders.o_orderdate::datetime -> gr_expr_4,
                orders.o_totalprice::double -> gr_expr_5,
                sum(lineitem.l_quantity::double::double)::double -> sum_1
              )
                group by (
                  customer.c_name::string,
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
                            orders.o_orderstatus::string -> o_orderstatus,
                            orders.o_totalprice::double -> o_totalprice,
                            orders.o_orderdate::datetime -> o_orderdate,
                            orders.o_orderpriority::string -> o_orderpriority,
                            orders.o_clerk::string -> o_clerk,
                            orders.o_shippriority::int -> o_shippriority,
                            orders.o_comment::string -> o_comment
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
                          lineitem.l_returnflag::string -> l_returnflag,
                          lineitem.l_linestatus::string -> l_linestatus,
                          lineitem.l_shipdate::datetime -> l_shipdate,
                          lineitem.l_commitdate::datetime -> l_commitdate,
                          lineitem.l_receiptdate::datetime -> l_receiptdate,
                          lineitem.l_shipinstruct::string -> l_shipinstruct,
                          lineitem.l_shipmode::string -> l_shipmode,
                          lineitem.l_comment::string -> l_comment
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
  sum (CAST ("lineitem"."l_quantity" as double)) as "sum_1"
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
  (
    SELECT
      "COL_0",
      "COL_1"
    FROM
      "_tmp_5240946567730125004_2136"
  )
GROUP BY
  "COL_0"
HAVING
  sum (CAST ("COL_1" as double)) > CAST(300 AS int)
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
  sum (CAST ("lineitem"."COL_5" as double)) as "sum_1"
FROM
  "customer"
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7",
      "COL_8",
      "COL_9"
    FROM
      "_tmp_9932753425948525886_0136"
  ) as "orders" ON CAST(true AS bool)
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7",
      "COL_8",
      "COL_9",
      "COL_10",
      "COL_11",
      "COL_12",
      "COL_13",
      "COL_14",
      "COL_15",
      "COL_16"
    FROM
      "_tmp_9932753425948525886_1136"
  ) as "lineitem" ON CAST(true AS bool)
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
    [0] SCAN TABLE _tmp_9932753425948525886_0136 (~983040 rows)
    [0] EXECUTE LIST SUBQUERY 1
    [1] SCAN TABLE _tmp_9932753425948525886_3136 (~1048576 rows)
        [0] SCAN TABLE _tmp_9932753425948525886_1136 (~1048576 rows)
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
      sum (CAST ("COL_5" as double)) as "col_1"
    FROM
      (
        SELECT
          "COL_0",
          "COL_1",
          "COL_2",
          "COL_3",
          "COL_4",
          "COL_5"
        FROM
          "_tmp_16750534456777283259_4136"
      )
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
          and part.p_brand::string = 'Brand#12'::string
          and part.p_container::string in ROW(
            'SM CASE'::string,
            'SM BOX'::string,
            'SM PACK'::string,
            'SM PKG'::string
          )
          and lineitem.l_quantity::double >= 1::int
          and lineitem.l_quantity::double <= 1::int + 10::int
          and part.p_size::int >= 1::int
          and part.p_size::int <= 5::int
          and lineitem.l_shipmode::string in ROW('AIR'::string, 'AIR REG'::string)
          and lineitem.l_shipinstruct::string = 'DELIVER IN PERSON'::string
        ) or (
          part.p_partkey::int = lineitem.l_partkey::int
          and part.p_brand::string = 'Brand#23'::string
          and part.p_container::string in ROW(
            'MED BAG'::string,
            'MED BOX'::string,
            'MED PKG'::string,
            'MED PACK'::string
          )
          and lineitem.l_quantity::double >= 10::int
          and lineitem.l_quantity::double <= 10::int + 10::int
          and part.p_size::int >= 1::int
          and part.p_size::int <= 10::int
          and lineitem.l_shipmode::string in ROW('AIR'::string, 'AIR REG'::string)
          and lineitem.l_shipinstruct::string = 'DELIVER IN PERSON'::string
        ) or (
          part.p_partkey::int = lineitem.l_partkey::int
          and part.p_brand::string = 'Brand#34'::string
          and part.p_container::string in ROW(
            'LG CASE'::string,
            'LG BOX'::string,
            'LG PACK'::string,
            'LG PKG'::string
          )
          and lineitem.l_quantity::double >= 20::int
          and lineitem.l_quantity::double <= 20::int + 10::int
          and part.p_size::int >= 1::int
          and part.p_size::int <= 15::int
          and lineitem.l_shipmode::string in ROW('AIR'::string, 'AIR REG'::string)
          and lineitem.l_shipinstruct::string = 'DELIVER IN PERSON'::string
        )
      )
        join on (true::bool)
          scan lineitem
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              part.p_partkey::int -> p_partkey,
              part.bucket_id::int -> bucket_id,
              part.p_name::string -> p_name,
              part.p_mfgr::string -> p_mfgr,
              part.p_brand::string -> p_brand,
              part.p_type::string -> p_type,
              part.p_size::int -> p_size,
              part.p_container::string -> p_container,
              part.p_retailprice::double -> p_retailprice,
              part.p_comment::string -> p_comment
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
    CAST (
      (
        "lineitem"."l_extendedprice" * (CAST(1 AS int) - "lineitem"."l_discount")
      ) as double
    )
  ) as "sum_1"
FROM
  "lineitem"
  INNER JOIN (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5",
      "COL_6",
      "COL_7",
      "COL_8",
      "COL_9"
    FROM
      "_tmp_11521773992601555350_0136"
  ) as "part" ON CAST(true AS bool)
WHERE
  "part"."COL_0" = "lineitem"."l_partkey"
  and "part"."COL_4" = CAST('Brand#12' AS string)
  and "part"."COL_7" in (
    CAST('SM CASE' AS string),
    CAST('SM BOX' AS string),
    CAST('SM PACK' AS string),
    CAST('SM PKG' AS string)
  )
  and "lineitem"."l_quantity" >= CAST(1 AS int)
  and "lineitem"."l_quantity" <= CAST(1 AS int) + CAST(10 AS int)
  and "part"."COL_6" >= CAST(1 AS int)
  and "part"."COL_6" <= CAST(5 AS int)
  and "lineitem"."l_shipmode" in (CAST('AIR' AS string), CAST('AIR REG' AS string))
  and "lineitem"."l_shipinstruct" = CAST('DELIVER IN PERSON' AS string)
  or "part"."COL_0" = "lineitem"."l_partkey"
  and "part"."COL_4" = CAST('Brand#23' AS string)
  and "part"."COL_7" in (
    CAST('MED BAG' AS string),
    CAST('MED BOX' AS string),
    CAST('MED PKG' AS string),
    CAST('MED PACK' AS string)
  )
  and "lineitem"."l_quantity" >= CAST(10 AS int)
  and "lineitem"."l_quantity" <= CAST(10 AS int) + CAST(10 AS int)
  and "part"."COL_6" >= CAST(1 AS int)
  and "part"."COL_6" <= CAST(10 AS int)
  and "lineitem"."l_shipmode" in (CAST('AIR' AS string), CAST('AIR REG' AS string))
  and "lineitem"."l_shipinstruct" = CAST('DELIVER IN PERSON' AS string)
  or "part"."COL_0" = "lineitem"."l_partkey"
  and "part"."COL_4" = CAST('Brand#34' AS string)
  and "part"."COL_7" in (
    CAST('LG CASE' AS string),
    CAST('LG BOX' AS string),
    CAST('LG PACK' AS string),
    CAST('LG PKG' AS string)
  )
  and "lineitem"."l_quantity" >= CAST(20 AS int)
  and "lineitem"."l_quantity" <= CAST(20 AS int) + CAST(10 AS int)
  and "part"."COL_6" >= CAST(1 AS int)
  and "part"."COL_6" <= CAST(15 AS int)
  and "lineitem"."l_shipmode" in (CAST('AIR' AS string), CAST('AIR REG' AS string))
  and "lineitem"."l_shipinstruct" = CAST('DELIVER IN PERSON' AS string)
''
plan:
    [0] SCAN TABLE lineitem (~1048576 rows)
        [0] SCAN TABLE _tmp_11521773992601555350_0136 (~1048576 rows)
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
SELECT
  sum (CAST ("COL_0" as double)) as "revenue"
FROM
  (
    SELECT
      "COL_0"
    FROM
      "_tmp_8067056639077876413_1136"
  )
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
