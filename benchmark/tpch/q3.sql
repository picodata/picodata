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
