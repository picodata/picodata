-- Corpus of anonymized real-world DQL queries (identifiers renamed to a, b, c, ...).
-- Consumed by corpus.rs; each `-- TEST:` comment names the following statement.
-- TEST: q1
SELECT
  CASE
    WHEN a.b > 0 THEN TRUE
    ELSE FALSE
  END AS c,
  COALESCE(d.f * -1, 0.0)::decimal AS g
FROM
  (
    SELECT
      COUNT(*) AS b
    FROM
      h i
    WHERE
      (i.j = 3023424)
  ) a
  LEFT JOIN (
    SELECT
      SUM(g) AS f
    FROM
      k i
    WHERE
      (i.j = 3023424)
      AND g > 0
  ) d ON 1 = 1 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q2
SELECT
  TO_CHAR(a, '%Y-%m-%d') AS a,
  SUM(b) AS c,
  SUM(d) AS f,
  SUM(g) AS h
FROM
  i
WHERE
  (
    j = 3023424
    AND k = 0
    AND l = 22
    AND m = 2023
    AND n = 7
  )
GROUP BY
  a option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q3
WITH
  a AS (
    SELECT
      TO_CHAR(COALESCE(b.c, b.d), '%Y-%m-%d') AS d,
      (
        b.f * COALESCE(g.h, 0) * CASE
          WHEN b.i IS NOT NULL THEN -1
          ELSE 1
        END
      ) AS j
    FROM
      k b
      LEFT JOIN l m ON b.n = m.o
      AND b.p = m.p
      LEFT JOIN k q ON q.i = b.o
      AND q.p = b.p
      LEFT JOIN r g ON b.s = g.s
      LEFT JOIN t u ON b.v = u.v
    WHERE
      (
        b.p = 3023424
        AND coalesce(m.w, b.x) = 0
        AND CAST(TO_CHAR(COALESCE(b.c, b.d), '%Y') AS INTEGER) = 2023
        AND CAST(TO_CHAR(COALESCE(b.c, b.d), '%m') AS INTEGER) = 7
        AND coalesce(m.y, u.y) = 22
        AND q.o IS NULL
        AND (
          b.i IS NULL
          OR b.i = -1
        )
      )
  )
SELECT
  d,
  COALESCE(SUM(j), 0.0) AS j
FROM
  a
GROUP BY
  d option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q4
SELECT DISTINCT
  a.b,
  a.c,
  a.d,
  f.g
FROM
  h a INDEXED BY i
  LEFT JOIN j f ON a.k = f.k
  AND a.b = f.b
WHERE
  (
    a.k = 3023424
    AND a.l = 0
    AND m = 2023
    AND n = 7
    AND a.o = 22
  )
UNION
SELECT DISTINCT
  p.b AS b,
  coalesce(q.c, p.c) AS c,
  coalesce(q.d, p.d) AS d,
  f.g AS g
FROM
  r p
  LEFT JOIN r s ON s.t = p.u
  AND s.k = p.k
  LEFT JOIN v q ON p.w = q.u
  AND p.k = q.k
  LEFT JOIN x y ON p.z = y.z
  LEFT JOIN j f ON p.k = f.k
  AND p.b = f.b
  LEFT JOIN aa ab ON p.ac = ab.ac
WHERE
  (
    p.k = 3023424
    AND coalesce(q.l, p.ad) = 0
    AND CAST(TO_CHAR(COALESCE(p.ae, p.af), '%Y') AS INTEGER) = 2023
    AND CAST(TO_CHAR(COALESCE(p.ae, p.af), '%m') AS INTEGER) = 7
    AND coalesce(q.o, ab.o) = 22
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q5
SELECT
  a AS a,
  SUM(b) AS c,
  SUM(d) AS f,
  SUM(g) AS h
FROM
  i
WHERE
  (
    j = 3023424
    AND k = 0
    AND l = 22
    AND m = '2023-07-28'
  )
GROUP BY
  a option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q6
SELECT
  a.b AS b,
  COALESCE(
    SUM(
      c.d * COALESCE(a.f, 0) * CASE
        WHEN c.g IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS h
FROM
  i c
  LEFT JOIN j k ON c.l = k.m
  AND c.n = k.n
  LEFT JOIN o a ON c.p = a.p
  LEFT JOIN q r ON c.s = r.s
  LEFT JOIN i t ON t.g = c.m
  AND t.n = c.n
WHERE
  (
    c.n = 3023424
    AND coalesce(k.u, c.v) = 0
    AND coalesce(k.w, r.w) = 22
    AND COALESCE(c.x, c.y) = '2023-07-28'
    AND t.m IS NULL
    AND (
      c.g IS NULL
      OR c.g = -1
    )
  )
GROUP BY
  a.b option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q7
SELECT DISTINCT
  a.b,
  a.c,
  a.d,
  f.g
FROM
  h a INDEXED BY i
  LEFT JOIN j f ON a.k = f.k
  AND a.b = f.b
WHERE
  (
    a.k = 3023424
    AND a.l = 0
    AND a.m = '2023-07-28'
    AND a.n = 22
  )
UNION
SELECT DISTINCT
  o.b AS b,
  coalesce(p.c, o.c) AS c,
  coalesce(p.d, o.d) AS d,
  f.g AS g
FROM
  q o
  LEFT JOIN q r ON r.s = o.t
  AND r.k = o.k
  LEFT JOIN u p ON o.v = p.t
  AND o.k = p.k
  LEFT JOIN w x ON o.y = x.y
  LEFT JOIN j f ON o.k = f.k
  AND o.b = f.b
  LEFT JOIN z aa ON o.ab = aa.ab
WHERE
  (
    o.k = 3023424
    AND coalesce(p.l, o.ac) = 0
    AND COALESCE(o.ad, o.m) = '2023-07-28'
    AND coalesce(p.n, aa.n) = 22
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q8
SELECT
  a AS b,
  c AS c,
  CASE
    WHEN 0 > d THEN d
    ELSE 0.0
  END AS f,
  CASE
    WHEN 0 > g THEN g
    ELSE 0.0
  END AS h,
  CASE
    WHEN 0 > COALESCE(i, 0.0) + COALESCE(j, 0.0) THEN COALESCE(i, 0.0) + COALESCE(j, 0.0)
    ELSE 0.0
  END AS k,
  CASE
    WHEN 0 > l THEN l
    ELSE 0.0
  END AS m,
  CASE
    WHEN 0 > n THEN n
    ELSE 0.0
  END AS o
FROM
  p q
WHERE
  (q.c = 3023424) option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q9
SELECT
  coalesce(a.b, c.d) AS b,
  coalesce(a.f, g.f) AS f,
  COALESCE(c.h, c.i) AS j,
  COALESCE(
    (
      c.k * COALESCE(l.m, 0.0) * CASE
        WHEN c.n IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS o
FROM
  p c
  LEFT JOIN p q ON q.n = c.r
  AND q.s = c.s
  LEFT JOIN t a ON c.u = a.r
  AND c.s = a.s
  LEFT JOIN v l ON c.w = l.w
  LEFT JOIN x g ON c.y = g.y
WHERE
  (
    c.s = 3023424
    AND q.r IS NULL
    AND (
      c.n IS NULL
      OR c.n = -1
    )
  ) option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q10
SELECT
  SUM(CAST(a * 100 AS INTEGER) / 100.0) AS b
FROM
  c d
WHERE
  (
    d.f = 3023424
    AND a < 0
  ) option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q11
WITH
  a AS (
    SELECT
      b.c,
      b.d,
      b.f
    FROM
      (
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 3023424
          AND h >= ('2023-04-28')
          AND h < ('2023-04-29')
          AND COALESCE(i, 0) <> 1
        UNION ALL
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 3023424
          AND h IS NULL
          AND j >= ('2023-04-28')
          AND j < ('2023-04-29')
          AND COALESCE(i, 0) <> 1
      ) b
      LEFT JOIN k l ON l.m = b.c
      AND l.f = b.f
    WHERE
      l.m IS NULL
    ORDER BY
      b.c
  ),
  n AS (
    SELECT
      o.f AS f,
      o.c AS m,
      p.q AS q,
      p.r AS r,
      o.s AS s,
      o.t AS t,
      COALESCE(o.u, o.v, 0) AS w,
      CASE
        WHEN o.u IS NOT NULL
        OR o.v IS NOT NULL THEN o.x
        ELSE '0000'
      END AS y,
      TO_CHAR(o.z, '%Y-%m-%d') AS aa,
      CASE
        WHEN o.u IS NOT NULL THEN 0
        WHEN o.v IS NOT NULL THEN 1
        ELSE 2
      END AS ab,
      ac.ad AS ad
    FROM
      a ae
      JOIN af p INDEXED by ag ON p.c = ae.d
      AND p.f = ae.f
      JOIN ah e INDEXED by ai ON e.m = ae.c
      AND e.f = p.f
      AND e.aj < 0
      JOIN ak ac INDEXED BY al ON ac.am = e.c
      AND ac.f = e.f
      AND ac.an IS NULL
      AND ac.ao IS NULL
      JOIN g o INDEXED by ap ON o.c = ac.aq
      AND o.f = ac.f
      AND COALESCE(o.i, 0) <> 1
      LEFT JOIN k ar ON ar.m = o.c
      AND ar.f = o.f
      LEFT JOIN at au ON au.t = o.t
    WHERE
      au.av = 7
      AND (
        p.aw = 0
        AND p.ax = 22
      )
  ),
  ay AS (
    SELECT
      az.f AS f,
      az.c AS m,
      az.q AS q,
      coalesce(p.r, az.r) AS r,
      coalesce(p.s, az.s) AS s,
      az.t AS t,
      COALESCE(az.u, az.v, 0) AS w,
      az.x AS y,
      TO_CHAR(az.z, '%Y-%m-%d') AS aa,
      CASE
        WHEN az.u IS NOT NULL THEN 0
        ELSE 1
      END AS ab,
      COALESCE(
        (
          az.ad * COALESCE(au.ba, 0.0) * (
            CASE
              WHEN az.bb IS NOT NULL THEN -1
              ELSE 1
            END
          )
        ),
        0.0
      ) AS ad
    FROM
      bc az
      LEFT JOIN af p INDEXED by ag ON az.d = p.c
      AND p.f = az.f
      LEFT JOIN at au ON az.t = au.t
      LEFT JOIN bc bd INDEXED by be ON bd.bb = az.c
      AND bd.f = p.f
      LEFT JOIN bf bg ON az.s = bg.s
    WHERE
      az.f = 3023424
      AND COALESCE(az.h, az.j) = ('2023-04-28')
      AND au.av = 7
      AND (
        coalesce(p.aw, az.bh) = 0
        AND coalesce(p.ax, bg.ax) = 22
      )
  ),
  bi AS (
    SELECT
      *
    FROM
      n
    UNION ALL
    SELECT
      *
    FROM
      ay
  ),
  bj AS (
    SELECT
      bi.f,
      bi.m,
      bi.q,
      bi.r,
      bi.s,
      bi.t,
      bi.w,
      bi.y,
      bi.aa,
      bi.ab,
      bi.ad,
      bk.bl,
      ROW_NUMBER() OVER (
        ORDER BY
          m DESC
      ) AS bm
    FROM
      bi
      LEFT JOIN bn bk ON bk.f = bi.f
      AND bk.q = bi.q
  )
SELECT
  *
FROM
  bj
WHERE
  bm > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 700000,
    SQL_MOTION_ROW_MAX = 17000
  );

-- TEST: q12
WITH
  a AS (
    SELECT
      b.c,
      b.d,
      b.f
    FROM
      (
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 3023424
          AND h >= ('2023-04-28')
          AND h < ('2023-04-28')
          AND COALESCE(i, 0) <> 1
        UNION ALL
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 3023424
          AND h IS NULL
          AND j >= ('2023-04-28')
          AND j < ('2023-04-28')
          AND COALESCE(i, 0) <> 1
      ) b
      LEFT JOIN k l ON l.m = b.c
      AND l.f = b.f
    WHERE
      l.m IS NULL
  ),
  n AS (
    SELECT DISTINCT
      o.f AS f,
      p.q AS q,
      p.r AS r,
      p.s AS s,
      o.t AS t,
      u.v AS v
    FROM
      a w
      JOIN x p INDEXED by y ON p.c = w.d
      AND p.f = w.f
      JOIN z e INDEXED by aa ON e.m = w.c
      AND e.f = p.f
      AND e.ab < 0
      JOIN ac ad INDEXED by ae ON ad.af = e.c
      AND ad.ag IS NULL
      AND ad.ah IS NULL
      AND ad.f = e.f
      JOIN g o INDEXED by ai ON o.c = ad.aj
      AND o.f = ad.f
      AND COALESCE(o.i, 0) <> 1
      LEFT JOIN ak al ON al.am = o.am
      LEFT JOIN an u ON u.f = p.f
      AND u.q = p.q
    WHERE
      al.ao = 7
      AND (
        p.ap = 0
        AND p.aq = 22
      )
  ),
  ar AS (
    SELECT DISTINCT
      at.f AS f,
      at.q AS q,
      coalesce(p.r, at.r) AS r,
      coalesce(p.s, at.s) AS s,
      coalesce(p.t, at.t) AS t,
      u.v AS v
    FROM
      au at
      LEFT JOIN x p INDEXED by y ON at.d = p.c
      AND at.f = p.f
      LEFT JOIN ak al ON at.am = al.am
      LEFT JOIN au av INDEXED by aw ON av.ax = at.c
      AND av.f = at.f
      LEFT JOIN ay az ON at.t = az.t
      LEFT JOIN an u ON u.f = p.f
      AND u.q = p.q
    WHERE
      at.f = 3023424
      AND COALESCE(at.h, at.j) = ('2023-04-28')
      AND al.ao = 7
      AND (
        coalesce(p.ap, at.ba) = 0
        AND coalesce(p.aq, az.aq) = 22
      )
  )
SELECT
  *
FROM
  n
UNION ALL
SELECT
  *
FROM
  ar option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q13
SELECT
  a AS a,
  b AS b,
  c AS c,
  d AS d,
  SUM(f) AS g,
  SUM(h) AS i
FROM
  j
WHERE
  (
    k = 3023424
    AND l = 0
    AND d IN (2024, 2023, 2021, 2022, 2019, 2018)
  )
GROUP BY
  a,
  b,
  c,
  d option (
    SQL_VDBE_OPCODE_MAX = 900000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q14
SELECT
  coalesce(a.b, c.b) AS b,
  CAST(TO_CHAR(COALESCE(d.f, d.g), '%Y') AS INTEGER) AS h,
  CAST(
    (
      CAST(TO_CHAR(COALESCE(d.f, d.g), '%m') AS INT) - 1
    ) / 3 + 1 AS INT
  ) AS i,
  CAST(TO_CHAR(COALESCE(d.f, d.g), '%m') AS INTEGER) AS j,
  TO_CHAR(COALESCE(d.f, d.g), '%Y-%m-%d') AS g,
  COALESCE(
    (
      d.k * COALESCE(l.m, 0.0) * CASE
        WHEN d.n IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS o
FROM
  p d
  LEFT JOIN q a ON d.r = a.s
  AND d.t = a.t
  LEFT JOIN u v ON d.w = v.w
  LEFT JOIN x l ON d.y = l.y
  LEFT JOIN z c ON d.w = c.w
  LEFT JOIN p aa ON aa.n = d.s
  AND aa.t = d.t
WHERE
  (
    d.t = 3023424
    AND coalesce(a.ab, d.ac) = 0
    AND CAST(TO_CHAR(COALESCE(d.f, d.g), '%Y') AS INTEGER) IN (2024, 2023, 2021, 2022, 2019, 2018)
    AND aa.s IS NULL
    AND (
      d.n IS NULL
      OR d.n = -1
    )
  ) option (
    SQL_VDBE_OPCODE_MAX = 100000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q15
SELECT
  a AS a,
  b AS b,
  c AS c,
  d AS d,
  SUM(f) AS g
FROM
  h
WHERE
  (
    i = 3023424
    AND f > 0
    AND a IN (
      1,
      97,
      3,
      35,
      4,
      100,
      5,
      41,
      43,
      10,
      11,
      12,
      13,
      14,
      19,
      22,
      29
    )
    AND d IN (2024, 2023, 2021, 2022, 2019, 2018)
  )
GROUP BY
  a,
  b,
  c,
  d option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q16
WITH
  a AS (
    SELECT
      a.b AS b,
      a.c AS d,
      a.f AS g,
      a.h AS i,
      TO_CHAR(a.j, '%Y-%m-%d') AS k,
      a.l AS l,
      a.m AS n
    FROM
      o a
    WHERE
      (
        a.b = 4797271
        AND a.h IN (1, 2)
      )
  ),
  p AS (
    SELECT
      a.b AS b,
      a.d AS d,
      a.g AS g,
      a.i AS i,
      a.k AS k,
      a.l AS l,
      a.n AS n,
      p.q AS q,
      p.r AS s,
      p.c AS t
    FROM
      a
      JOIN u p ON a.d = p.v
      AND a.b = p.b
    WHERE
      p.w <> 0
  ),
  x AS (
    SELECT
      p.b AS b,
      p.d AS d,
      p.g AS g,
      p.i AS i,
      p.k AS k,
      p.l AS l,
      p.n AS n,
      p.q AS q,
      p.s AS s,
      x.c AS y
    FROM
      p
      JOIN z x ON p.t = x.aa
      AND p.b = x.b
  ),
  ab AS (
    SELECT
      x.b AS b,
      x.d AS d,
      x.g AS g,
      x.i AS i,
      x.k AS k,
      x.l AS l,
      x.n AS n,
      x.q AS q,
      x.s AS s,
      ab.ac AS ac,
      COALESCE(ab.ad, 0.0) AS ad,
      TO_CHAR(ab.ae, '%Y-%m-%d') AS ae
    FROM
      x
      JOIN af ab ON x.y = ab.ag
      AND x.b = ab.b
    WHERE
      ab.ah = to_date('3000-01-01', '%Y-%m-%d')
      AND ab.ai = 0
  )
SELECT
  *
FROM
  ab option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q17
WITH
  a AS (
    SELECT
      b.c AS d,
      f.c AS g,
      1 AS h,
      i.j AS k,
      b.l AS l,
      b.m AS n,
      coalesce(f.o, b.o) AS o,
      coalesce(f.p, b.p) AS q,
      b.r AS r,
      s.t AS t,
      coalesce(u.v, b.v) AS v,
      f.w AS x,
      coalesce(f.y, b.y) AS y,
      b.z AS z,
      coalesce(f.aa, f.ab, b.ac) AS ac,
      CASE
        WHEN f.aa IS NOT NULL THEN 0
        WHEN f.ab IS NOT NULL THEN 1
        ELSE b.ad
      END AS ad,
      coalesce(f.ae, b.af) AS af,
      CASE
        WHEN f.aa IS NOT NULL
        OR f.ab IS NOT NULL THEN f.ag
        ELSE b.ah
      END AS ah,
      CASE
        WHEN b.ai IS NOT NULL THEN TRUE
        ELSE FALSE
      END AS aj
    FROM
      ak b
      JOIN al s ON s.r = b.r
      LEFT JOIN am an ON an.ao = b.c
      AND an.l = 3023424
      AND an.j = 0
      AND b.ap = 0
      AND an.l = b.l
      LEFT JOIN am aq ON an.ar = aq.c
      AND aq.l = 3023424
      AND aq.l = b.l
      LEFT JOIN am i ON aq.at = i.at
      AND i.l = 3023424
      AND i.j = 0
      AND i.l = b.l
      LEFT JOIN au f ON i.ao = f.c
      AND f.l = 3023424
      AND f.l = b.l
      LEFT JOIN av u ON u.aw = f.aw
    WHERE
      (
        b.l = 3023424
        AND b.ax = 1
        AND b.m = '2024-02-28'
        AND s.t = 11
        AND b.ap = 0
        AND b.ai IS NULL
        AND s.ay > 0
      )
  ),
  az AS (
    SELECT
      b.c AS d,
      f.c AS g,
      1 AS h,
      aq.j AS k,
      b.l AS l,
      b.m AS n,
      coalesce(f.o, b.o) AS o,
      coalesce(f.p, b.p) AS q,
      b.r AS r,
      s.t AS t,
      coalesce(u.v, b.v) AS v,
      f.w AS x,
      coalesce(f.y, b.y) AS y,
      b.z AS z,
      coalesce(f.aa, f.ab, b.ac) AS ac,
      CASE
        WHEN f.aa IS NOT NULL THEN 0
        WHEN f.ab IS NOT NULL THEN 1
        ELSE b.ad
      END AS ad,
      coalesce(f.ae, b.af) AS af,
      CASE
        WHEN f.aa IS NOT NULL
        OR f.ab IS NOT NULL THEN f.ag
        ELSE b.ah
      END AS ah,
      CASE
        WHEN b.ai IS NOT NULL THEN TRUE
        ELSE FALSE
      END AS aj
    FROM
      ak b
      JOIN al s ON s.r = b.r
      LEFT JOIN am an ON an.ao = b.c
      AND an.l = 3023424
      AND an.j = 0
      AND b.ap = 0
      AND an.l = b.l
      LEFT JOIN am aq ON an.ar = aq.at
      AND aq.l = 3023424
      AND aq.j <> 0
      AND aq.ba IS NULL
      AND aq.bb IS NULL
      AND aq.l = b.l
      LEFT JOIN au f ON aq.ao = f.c
      AND f.l = 3023424
      AND f.l = b.l
      LEFT JOIN av u ON u.aw = f.aw
    WHERE
      (
        b.l = 3023424
        AND b.ax = 1
        AND b.m = '2024-02-28'
        AND s.t = 11
        AND b.ap = 0
        AND b.ai IS NULL
        AND s.ay < 0
      )
  ),
  bc AS (
    SELECT
      b.c AS d,
      f.c AS g,
      2 AS h,
      an.j AS k,
      b.l AS l,
      b.m AS n,
      coalesce(f.o, b.o) AS o,
      coalesce(f.p, b.p) AS q,
      b.r AS r,
      s.t AS t,
      coalesce(u.v, b.v) AS v,
      f.w AS x,
      coalesce(f.y, b.y) AS y,
      b.z AS z,
      coalesce(f.aa, f.ab, b.ac) AS ac,
      CASE
        WHEN f.aa IS NOT NULL THEN 0
        WHEN f.ab IS NOT NULL THEN 1
        ELSE 2
      END AS ad,
      f.ae AS af,
      CASE
        WHEN f.aa IS NOT NULL
        OR f.ab IS NOT NULL THEN f.ag
        ELSE b.ah
      END AS ah,
      CASE
        WHEN b.ai IS NOT NULL THEN TRUE
        ELSE FALSE
      END AS aj
    FROM
      ak b
      JOIN al s ON s.r = b.r
      LEFT JOIN am an ON an.bd = b.c
      AND an.l = 3023424
      AND b.ap = 1
      AND an.l = b.l
      LEFT JOIN au f ON f.c = an.ao
      AND f.l = 3023424
      AND f.l = b.l
      LEFT JOIN av u ON u.aw = f.aw
    WHERE
      (
        b.l = 3023424
        AND b.ax = 1
        AND b.m = '2024-02-28'
        AND s.t = 11
        AND b.ap = 1
        AND b.ai IS NULL
        AND s.ay < 0
      )
  )
SELECT
  *
FROM
  a
UNION
SELECT
  *
FROM
  az
UNION
SELECT
  *
FROM
  bc option (
    SQL_VDBE_OPCODE_MAX = 900000,
    SQL_MOTION_ROW_MAX = 10000
  );

-- TEST: q18
WITH
  a AS (
    SELECT
      coalesce(b.c, d.c) AS c,
      f.g AS h,
      coalesce(f.i, d.i) AS i,
      CASE
        WHEN f.j IS NOT NULL THEN 0
        WHEN f.k IS NOT NULL THEN 1
        ELSE d.l
      END AS l,
      coalesce(f.j, f.k, d.m) AS m
    FROM
      n d
      JOIN o p ON p.q = d.q
      LEFT JOIN r s ON s.t = d.u
      AND s.v = 0
      AND d.w = 0
      AND s.x = d.x
      LEFT JOIN r y ON s.z = y.u
      AND y.x = s.x
      LEFT JOIN r aa ON y.ab = aa.ab
      AND aa.x = y.x
      AND aa.v = 0
      LEFT JOIN ac f ON aa.t = f.u
      AND f.x = aa.x
      LEFT JOIN ad b ON b.ae = f.ae
    WHERE
      (
        d.x = 3023424
        AND d.af = 1
        AND d.ag = '2024-02-28'
        AND p.ah = 11
        AND d.w = 0
        AND NOT (d.ai IS NOT NULL)
        AND p.aj > 0
      )
  ),
  ak AS (
    SELECT
      coalesce(b.c, d.c) AS c,
      f.g AS h,
      coalesce(f.i, d.i) AS i,
      CASE
        WHEN f.j IS NOT NULL THEN 0
        WHEN f.k IS NOT NULL THEN 1
        ELSE d.l
      END AS l,
      coalesce(f.j, f.k, d.m) AS m
    FROM
      n d
      JOIN o p ON p.q = d.q
      LEFT JOIN r s ON s.t = d.u
      AND s.x = d.x
      AND s.v = 0
      AND d.w = 0
      LEFT JOIN r y ON s.ab = y.ab
      AND y.x = s.x
      AND y.v <> 0
      AND y.al IS NULL
      AND y.am IS NULL
      LEFT JOIN ac f ON y.t = f.u
      AND f.x = y.x
      LEFT JOIN ad b ON b.ae = f.ae
    WHERE
      (
        d.x = 3023424
        AND d.af = 1
        AND d.ag = '2024-02-28'
        AND p.ah = 11
        AND d.w = 0
        AND NOT (d.ai IS NOT NULL)
        AND p.aj < 0
      )
  ),
  an AS (
    SELECT
      coalesce(b.c, d.c) AS c,
      f.g AS h,
      coalesce(f.i, d.i) AS i,
      CASE
        WHEN f.j IS NOT NULL THEN 0
        WHEN f.k IS NOT NULL THEN 1
        ELSE 2
      END AS l,
      coalesce(f.j, f.k, d.m) AS m
    FROM
      n d
      JOIN o p ON p.q = d.q
      LEFT JOIN r s ON s.ao = d.u
      AND s.x = d.x
      AND d.w = 1
      LEFT JOIN ac f ON f.u = s.t
      AND f.x = s.x
      LEFT JOIN ad b ON b.ae = f.ae
    WHERE
      (
        d.x = 3023424
        AND d.af = 1
        AND d.ag = '2024-02-28'
        AND p.ah = 11
        AND d.w = 1
        AND NOT (d.ai IS NOT NULL)
        AND p.aj < 0
      )
  ),
  ap AS (
    SELECT
      *
    FROM
      a
    UNION
    SELECT
      *
    FROM
      ak
    UNION
    SELECT
      *
    FROM
      an
  )
SELECT
  *
FROM
  ap option (
    SQL_VDBE_OPCODE_MAX = 600000,
    SQL_MOTION_ROW_MAX = 10000
  );

-- TEST: q19
WITH
  a AS (
    SELECT
      b,
      c AS c,
      CASE
        WHEN d IS NOT NULL THEN TRUE
        ELSE FALSE
      END AS f
    FROM
      g h
    WHERE
      (
        h.i = 3023424
        AND h.j = '2024-02-28'
        AND h.k = 1
        AND h.b <> 0
      )
  )
SELECT
  SUM(
    CASE
      WHEN b > 0 THEN b
      ELSE 0
    END
  ) AS l,
  SUM(
    CASE
      WHEN b < 0 THEN b
      ELSE 0
    END
  ) AS m,
  NULL AS n,
  h.c AS c,
  f
FROM
  a h
GROUP BY
  c,
  f option (
    SQL_VDBE_OPCODE_MAX = 500000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q20
SELECT
  a.b,
  a.c,
  a.d,
  a.f,
  a.g,
  a.h,
  a.i,
  a.j,
  a.k AS k,
  a.l,
  a.m,
  a.n,
  a.o
FROM
  p a
  LEFT JOIN q r ON a.d = r.b
  AND a.c = r.c
WHERE
  (
    a.c = 3023424
    AND a.k <= '2024-02-28'
    AND NOT COALESCE(a.s, 'false')
  )
ORDER BY
  k DESC
LIMIT
  2 option (
    SQL_VDBE_OPCODE_MAX = 100000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q21
WITH
  a AS (
    SELECT
      CAST(TO_CHAR(b, '%Y-%m-%d') AS TEXT) AS c,
      d
    FROM
      f g
    WHERE
      (
        g.h = 3023424
        AND g.b BETWEEN ('2023-01-01'::datetime) AND ('2023-05-01'::datetime)
        AND g.i = 1
        AND g.d <> 0
      )
  ),
  j AS (
    SELECT
      c,
      SUM(
        CASE
          WHEN d > 0 THEN d
          ELSE 0
        END
      ) AS k,
      SUM(
        CASE
          WHEN d < 0 THEN d
          ELSE 0
        END
      ) AS l,
      0 AS m,
      ROW_NUMBER() OVER (
        ORDER BY
          c DESC
      ) AS n
    FROM
      a
    GROUP BY
      c
    ORDER BY
      c DESC
  )
SELECT
  *
FROM
  j
WHERE
  n > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 1400000,
    SQL_MOTION_ROW_MAX = 51000
  );

-- TEST: q22
WITH
  a AS (
    SELECT
      CAST(TO_CHAR(b, '%Y-%m-%d') AS TEXT) AS c,
      d
    FROM
      f g
    WHERE
      (
        g.h = 3023424
        AND g.b BETWEEN ('2023-01-01'::datetime) AND ('2023-05-01'::datetime)
        AND g.i = 1
        AND g.d <> 0
      )
  ),
  j AS (
    SELECT
      c,
      SUM(
        CASE
          WHEN d > 0 THEN d
          ELSE 0
        END
      ) AS k,
      SUM(
        CASE
          WHEN d < 0 THEN d
          ELSE 0
        END
      ) AS l,
      0 AS m
    FROM
      a
    GROUP BY
      c
  )
SELECT
  COUNT(*) AS n
FROM
  j option (
    SQL_VDBE_OPCODE_MAX = 1400000,
    SQL_MOTION_ROW_MAX = 51000
  );

-- TEST: q23
WITH
  a AS (
    SELECT
      b.*
    FROM
      c b
      LEFT JOIN d f ON b.g = f.h
      AND b.i = f.i
    WHERE
      (
        (
          b.i = 3023424
          AND NOT COALESCE(b.j, 'false')
          AND TO_CHAR(b.k, '%Y-%m-%d')::datetime > '2020-01-02'::datetime
        )
        AND b.k < '2023-01-01'::datetime
      )
    ORDER BY
      k DESC
    LIMIT
      1
  )
SELECT
  *
FROM
  a
UNION
SELECT
  b.*
FROM
  c b
  LEFT JOIN d f ON b.g = f.h
  AND b.i = f.i
WHERE
  (
    (
      b.i = 3023424
      AND NOT COALESCE(b.j, 'false')
      AND TO_CHAR(b.k, '%Y-%m-%d')::datetime > '2020-01-02'::datetime
    )
    AND b.k BETWEEN ('2023-01-01'::datetime) AND ('2023-05-01'::datetime)
  ) option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q24
WITH
  a AS (
    SELECT
      b.c AS c,
      b.d AS d,
      CAST(TO_CHAR(COALESCE(b.f, b.g), '%Y-%m-%d') AS TEXT) AS g
    FROM
      h b
      LEFT JOIN h i ON i.j = b.k
      AND i.l = b.l
    WHERE
      i.k IS NULL
      AND (
        b.l = 4797271
        AND b.m = 1415
        AND COALESCE(b.f, b.g) BETWEEN ('2010-01-01') AND ('2025-10-20')
        AND b.c = 1333
        AND b.j IS NULL
        AND COALESCE(b.n, 0) <> 1
      )
  ),
  o AS (
    SELECT
      c,
      g,
      SUM(d) AS d,
      ROW_NUMBER() OVER (
        ORDER BY
          g DESC
      ) AS p
    FROM
      a
    GROUP BY
      c,
      g
    ORDER BY
      g DESC
  )
SELECT
  *
FROM
  o
WHERE
  p > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 53000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q25
SELECT
  *
FROM
  (
    SELECT
      a.b AS c,
      a.d AS d,
      a.f AS f,
      COALESCE(a.g, a.h, 0) AS i,
      CASE
        WHEN a.g IS NOT NULL
        OR a.h IS NOT NULL THEN a.j
        ELSE '0000'
      END AS k,
      TO_CHAR(a.l, '%Y-%m-%d') AS m,
      CASE
        WHEN a.g IS NOT NULL THEN 0
        WHEN a.h IS NOT NULL THEN 1
        ELSE 2
      END AS n,
      a.o AS o,
      0 AS p,
      ROW_NUMBER() OVER (
        ORDER BY
          a.b DESC
      ) AS q
    FROM
      r a
      LEFT JOIN r s ON s.t = a.b
      AND s.u = 8397725
      AND s.u = a.u
    WHERE
      s.b IS NULL
      AND (
        a.u = 8397725
        AND a.d = 1415
        AND COALESCE(a.v, a.w) = '2024-07-30'
        AND a.f = 1333
        AND a.t IS NULL
        AND COALESCE(a.x, 0) <> 1
      )
    ORDER BY
      c DESC
  ) AS y
WHERE
  q > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q26
SELECT
  COUNT(*) AS a,
  SUM(b) AS c
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          d.f AS g,
          d.h AS h,
          d.i AS i,
          COALESCE(d.j, d.k, 0) AS l,
          CASE
            WHEN d.j IS NOT NULL
            OR d.k IS NOT NULL THEN d.m
            ELSE '0000'
          END AS n,
          TO_CHAR(d.o, '%Y-%m-%d') AS p,
          CASE
            WHEN d.j IS NOT NULL THEN 0
            WHEN d.k IS NOT NULL THEN 1
            ELSE 2
          END AS q,
          d.b AS b,
          0 AS r,
          ROW_NUMBER() OVER (
            ORDER BY
              d.f DESC
          ) AS s
        FROM
          t d
          LEFT JOIN t u ON u.v = d.f
          AND u.w = 8397725
          AND u.w = d.w
        WHERE
          u.f IS NULL
          AND (
            d.w = 8397725
            AND d.h = 1415
            AND COALESCE(d.x, d.y) = '2024-07-30'
            AND d.i = 1333
            AND d.v IS NULL
            AND COALESCE(d.z, 0) <> 1
          )
        ORDER BY
          g DESC
      ) AS aa
  ) AS ab option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q27
SELECT
  a.b AS b,
  a.c AS c,
  TO_CHAR(COALESCE(a.d, a.f), '%Y-%m-%d') AS f
FROM
  g a
  LEFT JOIN g h ON h.i = a.j
  AND h.k = a.k
WHERE
  h.j IS NULL
  AND (
    a.k = 8397725
    AND a.l = 1415
    AND COALESCE(a.d, a.f) BETWEEN ('2010-01-01') AND ('2025-11-04')
    AND a.i IS NULL
    AND COALESCE(a.m, 0) <> 1
  ) option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q28
WITH
  a AS (
    SELECT
      b.c AS c,
      CASE
        WHEN d > 0
        AND f.g = 3 THEN d
        ELSE 0
      END AS h
    FROM
      i b
      LEFT JOIN j f ON b.k = f.k
    WHERE
      (b.l = 3023424)
  ),
  m AS (
    SELECT
      c,
      SUM(h) AS n
    FROM
      a
    GROUP BY
      c
  )
SELECT
  b.o AS o,
  m.n AS n,
  0 AS p,
  0 AS q
FROM
  r b
  LEFT JOIN m ON m.c = b.s
WHERE
  (b.l = 3023424) option (
    SQL_VDBE_OPCODE_MAX = 86000,
    SQL_MOTION_ROW_MAX = 5235
  );

-- TEST: q29
WITH
  a AS (
    SELECT
      b.c,
      b.d,
      b.f
    FROM
      (
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h >= ('2022-10-31'::datetime)
          AND h < ('2022-11-01'::datetime)
          AND COALESCE(i, 0) <> 1
        UNION ALL
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h IS NULL
          AND j >= ('2022-10-31'::datetime)
          AND j < ('2022-11-01'::datetime)
          AND COALESCE(i, 0) <> 1
      ) b
      LEFT JOIN k l ON l.m = b.c
      AND l.f = b.f
    WHERE
      l.m IS NULL
    ORDER BY
      b.c
  ),
  n AS (
    SELECT
      o.f AS f,
      o.c AS m,
      p.q AS q,
      p.r AS r,
      o.s AS s,
      o.t AS t,
      COALESCE(o.u, o.v, 0) AS w,
      CASE
        WHEN o.u IS NOT NULL
        OR o.v IS NOT NULL THEN o.x
        ELSE '0000'
      END AS y,
      TO_CHAR(o.z, '%Y-%m-%d') AS aa,
      CASE
        WHEN o.u IS NOT NULL THEN 0
        WHEN o.v IS NOT NULL THEN 1
        ELSE 2
      END AS ab,
      ac.ad AS ad
    FROM
      a ae
      JOIN af p INDEXED by ag ON p.c = ae.d
      AND p.f = ae.f
      JOIN ah e INDEXED by ai ON e.m = ae.c
      AND e.f = p.f
      AND e.aj < 0
      JOIN ak ac INDEXED BY al ON ac.am = e.c
      AND ac.f = e.f
      AND ac.an IS NULL
      AND ac.ao IS NULL
      JOIN g o INDEXED by ap ON o.c = ac.aq
      AND o.f = ac.f
      AND COALESCE(o.i, 0) <> 1
      LEFT JOIN k ar ON ar.m = o.c
      AND ar.f = o.f
      LEFT JOIN at au ON au.t = o.t
    WHERE
      au.av = 7
      AND (
        p.aw = 11
        AND p.ax = 6
      )
  ),
  ay AS (
    SELECT
      az.f AS f,
      az.c AS m,
      az.q AS q,
      coalesce(p.r, az.r) AS r,
      coalesce(p.s, az.s) AS s,
      az.t AS t,
      COALESCE(az.u, az.v, 0) AS w,
      az.x AS y,
      TO_CHAR(az.z, '%Y-%m-%d') AS aa,
      CASE
        WHEN az.u IS NOT NULL THEN 0
        ELSE 1
      END AS ab,
      COALESCE(
        (
          az.ad * COALESCE(au.ba, 0.0) * (
            CASE
              WHEN az.bb IS NOT NULL THEN -1
              ELSE 1
            END
          )
        ),
        0.0
      ) AS ad
    FROM
      bc az
      LEFT JOIN af p INDEXED by ag ON az.d = p.c
      AND p.f = az.f
      LEFT JOIN at au ON az.t = au.t
      LEFT JOIN bc bd INDEXED by be ON bd.bb = az.c
      AND bd.f = p.f
      LEFT JOIN bf bg ON az.s = bg.s
    WHERE
      az.f = 4797271
      AND COALESCE(az.h, az.j) = ('2022-10-31'::datetime)
      AND au.av = 7
      AND (
        coalesce(p.aw, az.bh) = 11
        AND coalesce(p.ax, bg.ax) = 6
      )
  ),
  bi AS (
    SELECT
      *
    FROM
      n
    UNION ALL
    SELECT
      *
    FROM
      ay
  ),
  bj AS (
    SELECT
      bi.f,
      bi.m,
      bi.q,
      bi.r,
      bi.s,
      bi.t,
      bi.w,
      bi.y,
      bi.aa,
      bi.ab,
      bi.ad,
      bk.bl,
      ROW_NUMBER() OVER (
        ORDER BY
          m DESC
      ) AS bm
    FROM
      bi
      LEFT JOIN bn bk ON bk.f = bi.f
      AND bk.q = bi.q
  )
SELECT
  *
FROM
  bj
WHERE
  bm > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q30
WITH
  a AS (
    SELECT
      b.c,
      b.d,
      b.f
    FROM
      (
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h >= ('2022-10-31'::datetime)
          AND h < ('2022-11-01'::datetime)
          AND COALESCE(i, 0) <> 1
        UNION ALL
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h IS NULL
          AND j >= ('2022-10-31'::datetime)
          AND j < ('2022-11-01'::datetime)
          AND COALESCE(i, 0) <> 1
      ) b
      LEFT JOIN k l ON l.m = b.c
      AND l.f = b.f
    WHERE
      l.m IS NULL
    ORDER BY
      b.c
  ),
  n AS (
    SELECT
      o.p AS p
    FROM
      a q
      JOIN r s INDEXED by t ON s.c = q.d
      AND s.f = q.f
      JOIN u e INDEXED by v ON e.m = q.c
      AND e.f = s.f
      AND e.w < 0
      JOIN x o INDEXED BY y ON o.z = e.c
      AND o.f = e.f
      AND o.aa IS NULL
      AND o.ab IS NULL
      JOIN g ac INDEXED by ad ON ac.c = o.ae
      AND ac.f = o.f
      AND COALESCE(ac.i, 0) <> 1
      LEFT JOIN k af ON af.m = ac.c
      AND af.f = ac.f
      LEFT JOIN ag ah ON ah.ai = ac.ai
    WHERE
      ah.aj = 7
      AND (
        s.ak = 11
        AND s.al = 6
      )
  ),
  am AS (
    SELECT
      COALESCE(
        (
          an.p * COALESCE(ah.ao, 0.0) * CASE
            WHEN an.ap IS NOT NULL THEN -1
            ELSE 1
          END
        ),
        0.0
      ) AS p
    FROM
      aq an
      LEFT JOIN r s INDEXED by t ON an.d = s.c
      AND s.f = an.f
      LEFT JOIN ag ah ON an.ai = ah.ai
      LEFT JOIN aq ar INDEXED by at ON ar.ap = an.c
      AND ar.f = s.f
      LEFT JOIN au av ON an.aw = av.aw
    WHERE
      an.f = 4797271
      AND COALESCE(an.h, an.j) = ('2022-10-31'::datetime)
      AND ah.aj = 7
      AND (
        coalesce(s.ak, an.ax) = 11
        AND coalesce(s.al, av.al) = 6
      )
  )
SELECT
  COUNT(*) AS ay,
  SUM(p) AS az
FROM
  (
    SELECT
      *
    FROM
      n
    UNION ALL
    SELECT
      *
    FROM
      am
  ) ba option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q31
WITH
  a AS (
    SELECT
      b.c,
      b.d,
      b.f
    FROM
      (
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h >= ('2024-04-02')
          AND h < ('2024-04-03')
          AND COALESCE(i, 0) <> 1
        UNION ALL
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h IS NULL
          AND j >= ('2024-04-02')
          AND j < ('2024-04-03')
          AND COALESCE(i, 0) <> 1
      ) b
      LEFT JOIN k l ON l.m = b.c
      AND l.f = b.f
    WHERE
      l.m IS NULL
  ),
  n AS (
    SELECT DISTINCT
      o.f AS f,
      p.q AS q,
      p.r AS r,
      p.s AS s,
      o.t AS t,
      u.v AS v
    FROM
      a w
      JOIN x p INDEXED by y ON p.c = w.d
      AND p.f = w.f
      JOIN z e INDEXED by aa ON e.m = w.c
      AND e.f = p.f
      AND e.ab < 0
      JOIN ac ad INDEXED by ae ON ad.af = e.c
      AND ad.ag IS NULL
      AND ad.ah IS NULL
      AND ad.f = e.f
      JOIN g o INDEXED by ai ON o.c = ad.aj
      AND o.f = ad.f
      AND COALESCE(o.i, 0) <> 1
      LEFT JOIN ak al ON al.am = o.am
      LEFT JOIN an u ON u.f = p.f
      AND u.q = p.q
    WHERE
      al.ao = 7
      AND (
        p.ap = 11
        AND p.aq = 6
      )
  ),
  ar AS (
    SELECT DISTINCT
      at.f AS f,
      at.q AS q,
      coalesce(p.r, at.r) AS r,
      coalesce(p.s, at.s) AS s,
      coalesce(p.t, at.t) AS t,
      u.v AS v
    FROM
      au at
      LEFT JOIN x p INDEXED by y ON at.d = p.c
      AND at.f = p.f
      LEFT JOIN ak al ON at.am = al.am
      LEFT JOIN au av INDEXED by aw ON av.ax = at.c
      AND av.f = at.f
      LEFT JOIN ay az ON at.t = az.t
      LEFT JOIN an u ON u.f = p.f
      AND u.q = p.q
    WHERE
      at.f = 4797271
      AND COALESCE(at.h, at.j) = ('2024-04-02')
      AND al.ao = 7
      AND (
        coalesce(p.ap, at.ba) = 11
        AND coalesce(p.aq, az.aq) = 6
      )
  )
SELECT
  *
FROM
  n
UNION ALL
SELECT
  *
FROM
  ar option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q32
SELECT
  a AS a,
  SUM(b) AS b,
  SUM(c) AS c,
  0 AS d
FROM
  f
WHERE
  (
    g = 3023424
    AND h = 11
    AND i = 6
    AND j = '2022-10-12'
  )
GROUP BY
  a
UNION ALL
SELECT
  k.a AS a,
  0 AS b,
  0 AS c,
  COALESCE(
    SUM(
      l.m * COALESCE(k.n, 0) * CASE
        WHEN l.o IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS d
FROM
  p l
  LEFT JOIN p q ON q.o = l.r
  AND q.g = l.g
  LEFT JOIN s t ON l.u = t.r
  AND l.g = t.g
  LEFT JOIN v k ON l.w = k.w
  LEFT JOIN x y ON l.z = y.z
WHERE
  (
    l.g = 3023424
    AND coalesce(t.h, l.aa) = 11
    AND coalesce(t.i, y.i) = 6
    AND COALESCE(l.ab, l.j) = '2022-10-12'
    AND q.r IS NULL
    AND (
      l.o IS NULL
      OR l.o = -1
    )
  )
GROUP BY
  k.a option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q33
SELECT DISTINCT
  a.b,
  a.c,
  a.d,
  f.g
FROM
  h a
  LEFT JOIN i f ON a.j = f.j
  AND a.b = f.b
WHERE
  (
    a.j = 3023424
    AND a.k = 11
    AND a.l = '2022-10-12'
    AND a.m = 6
  )
UNION
SELECT DISTINCT
  n.b AS b,
  coalesce(o.c, n.c) AS c,
  coalesce(o.d, n.d) AS d,
  f.g AS g
FROM
  p n
  LEFT JOIN p q ON q.r = n.s
  AND q.j = n.j
  LEFT JOIN t o ON n.u = o.s
  AND n.j = o.j
  LEFT JOIN v w ON n.x = w.x
  LEFT JOIN i f ON n.j = f.j
  AND n.b = f.b
  LEFT JOIN y z ON n.aa = z.aa
WHERE
  (
    n.j = 3023424
    AND coalesce(o.k, n.ab) = 11
    AND COALESCE(n.ac, n.l) = '2022-10-12'
    AND coalesce(o.m, z.m) = 6
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q34
WITH
  a AS (
    SELECT
      coalesce(b.c, d.c) AS c,
      TO_CHAR(COALESCE(f.g, f.h), '%Y-%m-%d') AS h,
      f.i,
      COALESCE(j.k, 0) AS k,
      f.l
    FROM
      m f
      LEFT JOIN m n ON n.l = f.o
      AND n.p = f.p
      LEFT JOIN q b ON f.r = b.o
      AND f.p = b.p
      LEFT JOIN s j ON f.t = j.t
      LEFT JOIN u d ON f.v = d.v
    WHERE
      (
        f.p = 3023424
        AND coalesce(b.w, f.x) = 11
        AND n.o IS NULL
        AND (
          f.l IS NULL
          OR f.l = -1
        )
        AND CAST(TO_CHAR(COALESCE(f.g, f.h), '%Y') AS INTEGER) IN (2024, 2023, 2022)
      )
  )
SELECT
  c AS c,
  TO_CHAR(h, '%Y-%m-%d') AS h,
  SUM(y) AS y,
  SUM(z) AS z,
  0.0 AS aa
FROM
  ab
WHERE
  (
    p = 3023424
    AND w = 11
    AND ac IN (2024, 2023, 2022)
  )
GROUP BY
  h,
  c
UNION ALL
SELECT
  c,
  h,
  0 AS y,
  0 AS z,
  COALESCE(
    SUM(
      i * k * (
        CASE
          WHEN l IS NOT NULL THEN -1
          ELSE 1
        END
      )
    ),
    0.0
  ) AS aa
FROM
  a
GROUP BY
  c,
  h option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q35
WITH
  a AS (
    SELECT
      b.c AS c,
      b.d AS d,
      b.f AS f,
      b.g AS g,
      b.h AS h,
      i AS j,
      b.k AS k,
      b.l AS l,
      b.m AS m,
      b.n AS n,
      0 AS o,
      p AS p,
      q.r AS r,
      ROW_NUMBER() OVER () AS s
    FROM
      t b
      LEFT JOIN u q ON b.v = q.v
      AND b.f = q.f
    WHERE
      (
        b.v = 3023424
        AND COALESCE(b.w, 0) <> 1
        AND b.x = '2024-02-28'
        AND b.h = 3465
        AND b.y IS NULL
        AND b.c = 11
      )
  )
SELECT
  *
FROM
  a
WHERE
  s > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q36
SELECT
  SUM(a) AS b,
  COUNT(*) AS c
FROM
  d f
WHERE
  (
    f.g = 3023424
    AND COALESCE(f.h, 0) <> 1
    AND f.i = '2024-02-28'
    AND f.j = 3465
    AND f.k IS NULL
    AND f.l = 11
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q37
SELECT DISTINCT
  a.b AS b,
  a.c AS c,
  a.d AS d,
  a.f AS g,
  a.h AS h,
  i.j AS j
FROM
  k a
  LEFT JOIN l m ON a.d = m.d
  LEFT JOIN n i ON a.o = i.o
  AND a.h = i.h
WHERE
  (
    a.o = 3023424
    AND COALESCE(a.p, 0) <> 1
    AND a.q = '2024-02-28'
    AND a.d = 3465
    AND a.r = 11
  ) option (
    SQL_VDBE_OPCODE_MAX = 700000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q38
WITH
  a AS (
    SELECT
      TO_CHAR(b.c, '%Y-%m-%d') AS d,
      b.f AS f,
      b.g AS g,
      b.h AS h,
      SUM(
        CASE
          WHEN i > 0
          AND NOT j THEN i
          ELSE 0
        END
      ) AS k,
      SUM(
        CASE
          WHEN i < 0
          AND NOT j THEN i
          ELSE 0
        END
      ) AS l,
      SUM(
        CASE
          WHEN i > 0
          AND j THEN i
          ELSE 0
        END
      ) AS m,
      SUM(
        CASE
          WHEN n = 2 THEN i
          ELSE 0
        END
      ) AS o,
      0 AS p,
      ROW_NUMBER() OVER (
        ORDER BY
          c DESC,
          (b.g IS NULL) ASC,
          b.g ASC,
          (b.h IS NULL) ASC,
          b.h ASC,
          (b.f IS NULL) ASC,
          b.f ASC
      ) AS q
    FROM
      r b
      LEFT JOIN s t ON b.g = t.g
    WHERE
      (
        b.u = 3023424
        AND COALESCE(b.v, 0) <> 1
        AND b.c BETWEEN ('2023-01-01'::datetime) AND ('2023-05-01'::datetime)
        AND b.n <> 2
      )
    GROUP BY
      b.c,
      b.h,
      b.g,
      b.f
  )
SELECT
  *
FROM
  a
WHERE
  q > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 3000000,
    SQL_MOTION_ROW_MAX = 60000
  );

-- TEST: q39
WITH
  a AS (
    SELECT
      TO_CHAR(b, '%Y-%m-%d') AS c,
      d.f AS f,
      d.g AS g,
      d.h AS h
    FROM
      i d
      LEFT JOIN j k ON d.g = k.g
    WHERE
      (
        d.l = 3023424
        AND COALESCE(d.m, 0) <> 1
        AND d.b BETWEEN ('2023-01-01'::datetime) AND ('2023-05-01'::datetime)
        AND d.n <> 2
      )
  )
SELECT
  COUNT(*) AS o
FROM
  (
    SELECT
      COUNT(*) AS o,
      c,
      h
    FROM
      a
    GROUP BY
      c,
      f,
      g,
      h
  ) AS p option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q40
SELECT DISTINCT
  a.b AS b,
  a.c AS c,
  a.d AS d,
  a.f AS g,
  a.h AS h,
  i.j AS j
FROM
  k a
  LEFT JOIN l m ON a.d = m.d
  LEFT JOIN n i ON a.o = i.o
  AND a.h = i.h
WHERE
  (
    a.o = 3023424
    AND COALESCE(a.p, 0) <> 1
    AND a.q BETWEEN ('2010-01-01') AND ('2026-01-01')
    AND a.r <> 2
  ) option (
    SQL_VDBE_OPCODE_MAX = 27000000,
    SQL_MOTION_ROW_MAX = 18000
  );

-- TEST: q41
WITH
  a AS (
    SELECT
      b.*
    FROM
      c b
      LEFT JOIN d f ON b.g = f.h
      AND b.i = f.i
    WHERE
      (
        (
          b.i = 3023424
          AND NOT COALESCE(b.j, 'false')
          AND TO_CHAR(b.k, '%Y-%m-%d')::datetime > '2020-01-02'
        )
        AND b.k < '2010-01-01'
      )
    ORDER BY
      k DESC
    LIMIT
      1
  )
SELECT
  *
FROM
  a
UNION
SELECT
  b.*
FROM
  c b
  LEFT JOIN d f ON b.g = f.h
  AND b.i = f.i
WHERE
  (
    (
      b.i = 3023424
      AND NOT COALESCE(b.j, 'false')
      AND TO_CHAR(b.k, '%Y-%m-%d')::datetime > '2020-01-02'
    )
    AND b.k BETWEEN ('2010-01-01'::datetime) AND ('2026-01-01'::datetime)
  ) option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q42
SELECT
  COALESCE(
    SUM(
      CASE
        WHEN a IS NULL THEN 0.0
        ELSE a
      END
    ),
    0.0
  ) AS b
FROM
  c d
  LEFT JOIN f g ON d.h = g.h
WHERE
  (
    d.i = 3023424
    AND COALESCE(d.j, 0) <> 1
    AND d.k BETWEEN ('2010-01-01'::datetime) AND ('2026-01-01'::datetime)
  )
  AND d.l = 2
  AND COALESCE(d.j, 0) <> 1 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q43
SELECT
  COALESCE(
    SUM(
      CASE
        WHEN a IS NULL THEN 0.0
        ELSE a
      END
    ),
    0.0
  ) AS b
FROM
  c d
  LEFT JOIN f g ON d.h = g.h
WHERE
  (
    d.i = 3023424
    AND COALESCE(d.j, 0) <> 1
    AND d.k BETWEEN ('2010-01-01'::datetime) AND ('2026-01-01'::datetime)
    AND d.l <> 2
  )
  AND d.l = 2
  AND COALESCE(d.j, 0) <> 1 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q44
SELECT DISTINCT
  a.b AS b,
  a.c AS c,
  a.d AS f,
  a.g AS g,
  h.i AS i
FROM
  j a
  LEFT JOIN k h ON a.l = h.l
  AND a.g = h.g
WHERE
  (a.l = 3023424) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q45
SELECT DISTINCT
  CAST(TO_CHAR(COALESCE(a.b, a.c), '%Y') AS INTEGER) AS d,
  a.f AS f,
  a.g AS h,
  a.i AS i,
  j.k AS k
FROM
  l a
  LEFT JOIN l m ON m.n = a.o
  AND m.p = a.p
  LEFT JOIN q j ON a.p = j.p
  AND a.i = j.i
WHERE
  (
    a.p = 3023424
    AND m.o IS NULL
    AND (
      a.n IS NULL
      OR a.n = -1
    )
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q46
SELECT
  a.b AS b,
  CASE
    WHEN MIN(c.d) IS NOT NULL THEN TO_CHAR(MIN(c.d), '%Y-%m-%d')
    ELSE NULL
  END AS f,
  SUM(g.h) AS i
FROM
  j g
  JOIN k a ON g.l = a.m
  AND g.n = a.n
  JOIN o c ON g.p = c.p
  AND g.n = c.n
  LEFT JOIN q r ON a.s = r.t
WHERE
  (
    g.n = 3023424
    AND u IN (0, 3, 42, 45, 11)
    AND NOT r.v
    AND h > 0
  )
GROUP BY
  a.b option (
    SQL_VDBE_OPCODE_MAX = 798000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q47
SELECT
  a.b AS b,
  CASE
    WHEN c.d = 3 THEN TRUE
    ELSE FALSE
  END AS f,
  a.g AS g,
  a.h AS h,
  a.i AS i,
  a.j AS j,
  CASE
    WHEN a.b = 0
    AND (
      c.d IS NULL
      OR c.d <> 3
    ) THEN CASE
      WHEN k < 0 THEN k
      ELSE 0.0
    END
    ELSE 0.0
  END AS l,
  CASE
    WHEN a.b = 3 THEN CASE
      WHEN m < 0 THEN m
      ELSE 0.0
    END
    ELSE 0.0
  END AS n,
  CASE
    WHEN a.b = 0
    AND (
      c.d IS NOT NULL
      AND c.d = 3
    ) THEN CASE
      WHEN k < 0 THEN k
      ELSE 0.0
    END
    ELSE 0.0
  END AS o,
  CASE
    WHEN a.b = 11 THEN CASE
      WHEN p < 0 THEN p
      ELSE 0.0
    END
    ELSE 0.0
  END AS q,
  CASE
    WHEN a.b IN (42, 45) THEN (
      CASE
        WHEN r < 0 THEN r
        ELSE 0.0
      END
    ) + (
      CASE
        WHEN s < 0 THEN s
        ELSE 0.0
      END
    )
    ELSE 0.0
  END AS t,
  CASE
    WHEN a.b = 2 THEN CASE
      WHEN u < 0 THEN u
      ELSE 0.0
    END
    ELSE 0.0
  END AS v,
  CASE
    WHEN c.d = 3
    AND k > 0 THEN k
    ELSE 0.0
  END AS w,
  CASE
    WHEN a.b = 1
    AND k > 0 THEN k
    ELSE 0.0
  END AS x,
  NULL AS y
FROM
  z a
  LEFT JOIN aa c ON a.h = c.h
WHERE
  (ab = 3023424) option (
    SQL_VDBE_OPCODE_MAX = 400000,
    SQL_MOTION_ROW_MAX = 5235
  );

-- TEST: q48
WITH
  a AS (
    SELECT
      b.c,
      coalesce(d.f, b.g) AS f,
      b.h,
      coalesce(d.i, b.i) AS i,
      b.j,
      b.k,
      b.l,
      b.m,
      b.n,
      b.o,
      COALESCE(
        (
          b.p * COALESCE(q.r, 0.0) * CASE
            WHEN b.n IS NOT NULL THEN -1
            ELSE 1
          END
        ),
        0.0
      ) AS s,
      CASE
        WHEN (
          t.u IS NOT NULL
          AND t.u = 3
        ) THEN TRUE
        ELSE FALSE
      END AS v
    FROM
      w b
      LEFT JOIN x d ON b.y = d.c
      AND b.o = d.o
      LEFT JOIN z t ON coalesce(d.i, b.i) = t.i
      LEFT JOIN aa q ON b.u = q.u
      LEFT JOIN w ab ON ab.n = b.c
      AND ab.o = b.o
    WHERE
      (
        b.o = 3023424
        AND ab.c IS NULL
        AND (
          b.n IS NULL
          OR b.n = -1
        )
      )
  )
SELECT
  b.f AS f,
  b.v AS v,
  b.h AS h,
  b.i AS i,
  b.j AS j,
  b.k AS k,
  CASE
    WHEN b.f = 0
    AND NOT v THEN b.s
    ELSE 0.0
  END AS ac,
  CASE
    WHEN b.f = 3 THEN b.s
    ELSE 0.0
  END AS ad,
  CASE
    WHEN b.f = 0
    AND v THEN b.s
    ELSE 0.0
  END AS ae,
  CASE
    WHEN b.f = 11 THEN b.s
    ELSE 0.0
  END AS af,
  CASE
    WHEN b.f IN (42, 45) THEN b.s
    ELSE 0.0
  END AS ag,
  CASE
    WHEN b.f = 2 THEN b.s
    ELSE 0.0
  END AS ah,
  0 AS ai,
  0 AS aj,
  TO_CHAR(COALESCE(b.l, b.m), '%Y-%m-%d') AS m
FROM
  a b option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q49
WITH
  a AS (
    SELECT
      b.c,
      b.d,
      b.f
    FROM
      (
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h >= ('2023-02-17')
          AND h < ('2023-02-18')
          AND COALESCE(i, 0) <> 1
        UNION ALL
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h IS NULL
          AND j >= ('2023-02-17')
          AND j < ('2023-02-18')
          AND COALESCE(i, 0) <> 1
      ) b
      LEFT JOIN k l ON l.m = b.c
      AND l.f = b.f
    WHERE
      l.m IS NULL
    ORDER BY
      b.c
  ),
  n AS (
    SELECT
      o.f AS f,
      o.c AS m,
      p.q AS q,
      p.r AS r,
      o.s AS s,
      o.t AS t,
      COALESCE(o.u, o.v, 0) AS w,
      CASE
        WHEN o.u IS NOT NULL
        OR o.v IS NOT NULL THEN o.x
        ELSE '0000'
      END AS y,
      TO_CHAR(o.z, '%Y-%m-%d') AS aa,
      CASE
        WHEN o.u IS NOT NULL THEN 0
        WHEN o.v IS NOT NULL THEN 1
        ELSE 2
      END AS ab,
      ac.ad AS ad
    FROM
      a ae
      JOIN af p INDEXED by ag ON p.c = ae.d
      AND p.f = ae.f
      JOIN ah e INDEXED by ai ON e.m = ae.c
      AND e.f = p.f
      AND e.aj < 0
      JOIN ak ac INDEXED BY al ON ac.am = e.c
      AND ac.f = e.f
      AND ac.an IS NULL
      AND ac.ao IS NULL
      JOIN g o INDEXED by ap ON o.c = ac.aq
      AND o.f = ac.f
      AND COALESCE(o.i, 0) <> 1
      LEFT JOIN k ar ON ar.m = o.c
      AND ar.f = o.f
      LEFT JOIN at au ON au.t = o.t
    WHERE
      au.av = 4
      AND (
        p.aw = 3
        AND p.ax = 4
      )
  ),
  ay AS (
    SELECT
      az.f AS f,
      az.c AS m,
      az.q AS q,
      coalesce(p.r, az.r) AS r,
      coalesce(p.s, az.s) AS s,
      az.t AS t,
      COALESCE(az.u, az.v, 0) AS w,
      az.x AS y,
      TO_CHAR(az.z, '%Y-%m-%d') AS aa,
      CASE
        WHEN az.u IS NOT NULL THEN 0
        ELSE 1
      END AS ab,
      COALESCE(
        (
          az.ad * COALESCE(au.ba, 0.0) * (
            CASE
              WHEN az.bb IS NOT NULL THEN -1
              ELSE 1
            END
          )
        ),
        0.0
      ) AS ad
    FROM
      bc az
      LEFT JOIN af p INDEXED by ag ON az.d = p.c
      AND p.f = az.f
      LEFT JOIN at au ON az.t = au.t
      LEFT JOIN bc bd INDEXED by be ON bd.bb = az.c
      AND bd.f = p.f
      LEFT JOIN bf bg ON az.s = bg.s
    WHERE
      az.f = 4797271
      AND COALESCE(az.h, az.j) = ('2023-02-17')
      AND au.av = 4
      AND (
        coalesce(p.aw, az.bh) = 3
        AND coalesce(p.ax, bg.ax) = 4
      )
  ),
  bi AS (
    SELECT
      *
    FROM
      n
    UNION ALL
    SELECT
      *
    FROM
      ay
  ),
  bj AS (
    SELECT
      bi.f,
      bi.m,
      bi.q,
      bi.r,
      bi.s,
      bi.t,
      bi.w,
      bi.y,
      bi.aa,
      bi.ab,
      bi.ad,
      bk.bl,
      ROW_NUMBER() OVER (
        ORDER BY
          m DESC
      ) AS bm
    FROM
      bi
      LEFT JOIN bn bk ON bk.f = bi.f
      AND bk.q = bi.q
  )
SELECT
  *
FROM
  bj
WHERE
  bm > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 2200000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q50
WITH
  a AS (
    SELECT
      b.c,
      b.d,
      b.f
    FROM
      g b
      LEFT JOIN h i ON i.j = b.c
      AND i.f = b.f
    WHERE
      b.f = 4797271
      AND COALESCE(b.k, b.l) = ('2023-02-13')
      AND COALESCE(b.m, 0) <> 1
      AND i.j IS NULL
    ORDER BY
      b.c
  ),
  n AS (
    SELECT
      o.p AS p
    FROM
      a q
      JOIN r s ON s.c = q.d
      AND s.f = q.f
      JOIN t e ON e.j = q.c
      AND e.f = s.f
      AND e.u < 0
      JOIN v o INDEXED BY w ON o.x = e.c
      AND o.f = e.f
      AND o.y IS NULL
      AND o.z IS NULL
      JOIN g aa ON aa.c = o.ab
      AND aa.f = o.f
      AND COALESCE(aa.m, 0) <> 1
      LEFT JOIN h ac ON ac.j = aa.c
      AND ac.f = aa.f
      LEFT JOIN ad ae ON ae.af = aa.af
    WHERE
      ae.ag = 4
      AND (
        s.ah = 3
        AND s.ai = 4
      )
  ),
  aj AS (
    SELECT
      COALESCE(
        (
          ak.p * COALESCE(ae.al, 0.0) * CASE
            WHEN ak.am IS NOT NULL THEN -1
            ELSE 1
          END
        ),
        0.0
      ) AS p
    FROM
      an ak
      LEFT JOIN r s ON ak.d = s.c
      AND s.f = ak.f
      LEFT JOIN ad ae ON ak.af = ae.af
      LEFT JOIN an ao ON ao.am = ak.c
      AND ao.f = s.f
      LEFT JOIN ap aq ON ak.ar = aq.ar
    WHERE
      ak.f = 4797271
      AND COALESCE(ak.k, ak.l) = ('2024-12-28')
      AND ae.ag = 4
      AND (
        coalesce(s.ah, ak.at) = 3
        AND coalesce(s.ai, aq.ai) = 4
      )
  )
SELECT
  COUNT(*) AS au,
  SUM(p) AS av
FROM
  (
    SELECT
      *
    FROM
      n
    UNION ALL
    SELECT
      *
    FROM
      aj
  ) aw option (
    SQL_VDBE_OPCODE_MAX = 80000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q51
WITH
  a AS (
    SELECT
      b.c,
      b.d,
      b.f
    FROM
      (
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h >= ('2023-02-17')
          AND h < ('2023-02-18')
          AND COALESCE(i, 0) <> 1
        UNION ALL
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h IS NULL
          AND j >= ('2023-02-17')
          AND j < ('2023-02-18')
          AND COALESCE(i, 0) <> 1
      ) b
      LEFT JOIN k l ON l.m = b.c
      AND l.f = b.f
    WHERE
      l.m IS NULL
  ),
  n AS (
    SELECT DISTINCT
      o.f AS f,
      p.q AS q,
      p.r AS r,
      p.s AS s,
      o.t AS t,
      u.v AS v
    FROM
      a w
      JOIN x p INDEXED by y ON p.c = w.d
      AND p.f = w.f
      JOIN z e INDEXED by aa ON e.m = w.c
      AND e.f = p.f
      AND e.ab < 0
      JOIN ac ad INDEXED by ae ON ad.af = e.c
      AND ad.ag IS NULL
      AND ad.ah IS NULL
      AND ad.f = e.f
      JOIN g o INDEXED by ai ON o.c = ad.aj
      AND o.f = ad.f
      AND COALESCE(o.i, 0) <> 1
      LEFT JOIN ak al ON al.am = o.am
      LEFT JOIN an u ON u.f = p.f
      AND u.q = p.q
    WHERE
      al.ao = 4
      AND (
        p.ap = 3
        AND p.aq = 4
      )
  ),
  ar AS (
    SELECT DISTINCT
      at.f AS f,
      at.q AS q,
      coalesce(p.r, at.r) AS r,
      coalesce(p.s, at.s) AS s,
      coalesce(p.t, at.t) AS t,
      u.v AS v
    FROM
      au at
      LEFT JOIN x p INDEXED by y ON at.d = p.c
      AND at.f = p.f
      LEFT JOIN ak al ON at.am = al.am
      LEFT JOIN au av INDEXED by aw ON av.ax = at.c
      AND av.f = at.f
      LEFT JOIN ay az ON at.t = az.t
      LEFT JOIN an u ON u.f = p.f
      AND u.q = p.q
    WHERE
      at.f = 4797271
      AND COALESCE(at.h, at.j) = ('2023-02-17')
      AND al.ao = 4
      AND (
        coalesce(p.ap, at.ba) = 3
        AND coalesce(p.aq, az.aq) = 4
      )
  )
SELECT
  *
FROM
  n
UNION ALL
SELECT
  *
FROM
  ar option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q52
SELECT
  a AS a,
  SUM(b) AS b,
  SUM(c) AS c,
  0 AS d
FROM
  f
WHERE
  (
    g = 4797271
    AND h = 3
    AND i = 4
    AND j = '2023-02-17'
  )
GROUP BY
  a
UNION ALL
SELECT
  k.a AS a,
  0 AS b,
  0 AS c,
  COALESCE(
    SUM(
      l.m * COALESCE(k.n, 0) * CASE
        WHEN l.o IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS d
FROM
  p l
  LEFT JOIN p q ON q.o = l.r
  AND q.g = l.g
  LEFT JOIN s t ON l.u = t.r
  AND l.g = t.g
  LEFT JOIN v k ON l.w = k.w
  LEFT JOIN x y ON l.z = y.z
WHERE
  (
    l.g = 4797271
    AND coalesce(t.h, l.aa) = 3
    AND coalesce(t.i, y.i) = 4
    AND COALESCE(l.ab, l.j) = '2023-02-17'
    AND q.r IS NULL
    AND (
      l.o IS NULL
      OR l.o = -1
    )
  )
GROUP BY
  k.a option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q53
SELECT DISTINCT
  a.b,
  a.c,
  a.d,
  f.g
FROM
  h a
  LEFT JOIN i f ON a.j = f.j
  AND a.b = f.b
WHERE
  (
    a.j = 4797271
    AND a.k = 3
    AND a.l = '2023-02-17'
    AND a.m = 4
  )
UNION
SELECT DISTINCT
  n.b AS b,
  coalesce(o.c, n.c) AS c,
  coalesce(o.d, n.d) AS d,
  f.g AS g
FROM
  p n
  LEFT JOIN p q ON q.r = n.s
  AND q.j = n.j
  LEFT JOIN t o ON n.u = o.s
  AND n.j = o.j
  LEFT JOIN v w ON n.x = w.x
  LEFT JOIN i f ON n.j = f.j
  AND n.b = f.b
  LEFT JOIN y z ON n.aa = z.aa
WHERE
  (
    n.j = 4797271
    AND coalesce(o.k, n.ab) = 3
    AND COALESCE(n.ac, n.l) = '2023-02-17'
    AND coalesce(o.m, z.m) = 4
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q54
WITH
  a AS (
    SELECT
      coalesce(b.c, d.c) AS c,
      TO_CHAR(COALESCE(f.g, f.h), '%Y-%m-%d') AS h,
      f.i,
      COALESCE(j.k, 0) AS k,
      f.l
    FROM
      m f
      LEFT JOIN m n ON n.l = f.o
      AND n.p = f.p
      LEFT JOIN q b ON f.r = b.o
      AND f.p = b.p
      LEFT JOIN s j ON f.t = j.t
      LEFT JOIN u d ON f.v = d.v
    WHERE
      (
        f.p = 4797271
        AND coalesce(b.w, f.x) = 3
        AND n.o IS NULL
        AND (
          f.l IS NULL
          OR f.l = -1
        )
        AND CAST(TO_CHAR(COALESCE(f.g, f.h), '%Y') AS INTEGER) IN (2025, 2024, 2023, 2022, 2021)
      )
  )
SELECT
  c AS c,
  TO_CHAR(h, '%Y-%m-%d') AS h,
  SUM(y) AS y,
  SUM(z) AS z,
  0.0 AS aa
FROM
  ab
WHERE
  (
    p = 4797271
    AND w = 3
    AND ac IN (2025, 2024, 2023, 2022, 2021)
  )
GROUP BY
  h,
  c
UNION ALL
SELECT
  c,
  h,
  0 AS y,
  0 AS z,
  COALESCE(
    SUM(
      i * k * (
        CASE
          WHEN l IS NOT NULL THEN -1
          ELSE 1
        END
      )
    ),
    0.0
  ) AS aa
FROM
  a
GROUP BY
  c,
  h option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q55
WITH
  a AS (
    SELECT
      b.c,
      b.d,
      b.f
    FROM
      (
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 3023424
          AND h >= ('2024-08-14')
          AND h < ('2024-08-15')
          AND COALESCE(i, 0) <> 1
        UNION ALL
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 3023424
          AND h IS NULL
          AND j >= ('2024-08-14')
          AND j < ('2024-08-15')
          AND COALESCE(i, 0) <> 1
      ) b
      LEFT JOIN k l ON l.m = b.c
      AND l.f = b.f
    WHERE
      l.m IS NULL
    ORDER BY
      b.c
  ),
  n AS (
    SELECT
      o.f AS f,
      o.c AS m,
      p.q AS q,
      p.r AS r,
      o.s AS s,
      o.t AS t,
      COALESCE(o.u, o.v, 0) AS w,
      CASE
        WHEN o.u IS NOT NULL
        OR o.v IS NOT NULL THEN o.x
        ELSE '0000'
      END AS y,
      TO_CHAR(o.z, '%Y-%m-%d') AS aa,
      CASE
        WHEN o.u IS NOT NULL THEN 0
        WHEN o.v IS NOT NULL THEN 1
        ELSE 2
      END AS ab,
      ac.ad AS ad
    FROM
      a ae
      JOIN af p INDEXED by ag ON p.c = ae.d
      AND p.f = ae.f
      JOIN ah e INDEXED by ai ON e.m = ae.c
      AND e.f = p.f
      AND e.aj < 0
      JOIN ak ac INDEXED BY al ON ac.am = e.c
      AND ac.f = e.f
      AND ac.an IS NULL
      AND ac.ao IS NULL
      JOIN g o INDEXED by ap ON o.c = ac.aq
      AND o.f = ac.f
      AND COALESCE(o.i, 0) <> 1
      LEFT JOIN k ar ON ar.m = o.c
      AND ar.f = o.f
      LEFT JOIN at au ON au.t = o.t
    WHERE
      au.av = 8
      AND (p.aw IN (42, 45))
  ),
  ax AS (
    SELECT
      ay.f AS f,
      ay.c AS m,
      ay.q AS q,
      coalesce(p.r, ay.r) AS r,
      coalesce(p.s, ay.s) AS s,
      ay.t AS t,
      COALESCE(ay.u, ay.v, 0) AS w,
      ay.x AS y,
      TO_CHAR(ay.z, '%Y-%m-%d') AS aa,
      CASE
        WHEN ay.u IS NOT NULL THEN 0
        ELSE 1
      END AS ab,
      COALESCE(
        (
          ay.ad * COALESCE(au.az, 0.0) * (
            CASE
              WHEN ay.ba IS NOT NULL THEN -1
              ELSE 1
            END
          )
        ),
        0.0
      ) AS ad
    FROM
      bb ay
      LEFT JOIN af p INDEXED by ag ON ay.d = p.c
      AND p.f = ay.f
      LEFT JOIN at au ON ay.t = au.t
      LEFT JOIN bb bc INDEXED by bd ON bc.ba = ay.c
      AND bc.f = p.f
      LEFT JOIN be bf ON ay.s = bf.s
    WHERE
      ay.f = 3023424
      AND COALESCE(ay.h, ay.j) = ('2024-08-14')
      AND au.av = 8
      AND (coalesce(p.aw, ay.bg) IN (42, 45))
  ),
  bh AS (
    SELECT
      *
    FROM
      n
    UNION ALL
    SELECT
      *
    FROM
      ax
  ),
  bi AS (
    SELECT
      bh.f,
      bh.m,
      bh.q,
      bh.r,
      bh.s,
      bh.t,
      bh.w,
      bh.y,
      bh.aa,
      bh.ab,
      bh.ad,
      bj.bk,
      ROW_NUMBER() OVER (
        ORDER BY
          m DESC
      ) AS bl
    FROM
      bh
      LEFT JOIN bm bj ON bj.f = bh.f
      AND bj.q = bh.q
  )
SELECT
  *
FROM
  bi
WHERE
  bl > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q56
WITH
  a AS (
    SELECT
      b.c,
      b.d,
      b.f
    FROM
      (
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h >= ('2024-08-14')
          AND h < ('2024-08-15')
          AND COALESCE(i, 0) <> 1
        UNION ALL
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h IS NULL
          AND j >= ('2024-08-14')
          AND j < ('2024-08-15')
          AND COALESCE(i, 0) <> 1
      ) b
      LEFT JOIN k l ON l.m = b.c
      AND l.f = b.f
    WHERE
      l.m IS NULL
    ORDER BY
      b.c
  ),
  n AS (
    SELECT
      o.p AS p
    FROM
      a q
      JOIN r s INDEXED by t ON s.c = q.d
      AND s.f = q.f
      JOIN u e INDEXED by v ON e.m = q.c
      AND e.f = s.f
      AND e.w < 0
      JOIN x o INDEXED BY y ON o.z = e.c
      AND o.f = e.f
      AND o.aa IS NULL
      AND o.ab IS NULL
      JOIN g ac INDEXED by ad ON ac.c = o.ae
      AND ac.f = o.f
      AND COALESCE(ac.i, 0) <> 1
      LEFT JOIN k af ON af.m = ac.c
      AND af.f = ac.f
      LEFT JOIN ag ah ON ah.ai = ac.ai
    WHERE
      ah.aj = 8
      AND (s.ak IN (42, 45))
  ),
  al AS (
    SELECT
      COALESCE(
        (
          am.p * COALESCE(ah.an, 0.0) * CASE
            WHEN am.ao IS NOT NULL THEN -1
            ELSE 1
          END
        ),
        0.0
      ) AS p
    FROM
      ap am
      LEFT JOIN r s INDEXED by t ON am.d = s.c
      AND s.f = am.f
      LEFT JOIN ag ah ON am.ai = ah.ai
      LEFT JOIN ap aq INDEXED by ar ON aq.ao = am.c
      AND aq.f = s.f
      LEFT JOIN at au ON am.av = au.av
    WHERE
      am.f = 4797271
      AND COALESCE(am.h, am.j) = ('2024-08-14')
      AND ah.aj = 8
      AND (coalesce(s.ak, am.aw) IN (42, 45))
  )
SELECT
  COUNT(*) AS ax,
  SUM(p) AS ay
FROM
  (
    SELECT
      *
    FROM
      n
    UNION ALL
    SELECT
      *
    FROM
      al
  ) az option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q57
WITH
  a AS (
    SELECT
      b.c,
      b.d,
      b.f
    FROM
      (
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h >= ('2024-08-14')
          AND h < ('2024-08-15')
          AND COALESCE(i, 0) <> 1
        UNION ALL
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h IS NULL
          AND j >= ('2024-08-14')
          AND j < ('2024-08-15')
          AND COALESCE(i, 0) <> 1
      ) b
      LEFT JOIN k l ON l.m = b.c
      AND l.f = b.f
    WHERE
      l.m IS NULL
  ),
  n AS (
    SELECT DISTINCT
      o.f AS f,
      p.q AS q,
      p.r AS r,
      p.s AS s,
      o.t AS t,
      u.v AS v
    FROM
      a w
      JOIN x p INDEXED by y ON p.c = w.d
      AND p.f = w.f
      JOIN z e INDEXED by aa ON e.m = w.c
      AND e.f = p.f
      AND e.ab < 0
      JOIN ac ad INDEXED by ae ON ad.af = e.c
      AND ad.ag IS NULL
      AND ad.ah IS NULL
      AND ad.f = e.f
      JOIN g o INDEXED by ai ON o.c = ad.aj
      AND o.f = ad.f
      AND COALESCE(o.i, 0) <> 1
      LEFT JOIN ak al ON al.am = o.am
      LEFT JOIN an u ON u.f = p.f
      AND u.q = p.q
    WHERE
      al.ao = 8
      AND (p.ap IN (42, 45))
  ),
  aq AS (
    SELECT DISTINCT
      ar.f AS f,
      ar.q AS q,
      coalesce(p.r, ar.r) AS r,
      coalesce(p.s, ar.s) AS s,
      coalesce(p.t, ar.t) AS t,
      u.v AS v
    FROM
      at ar
      LEFT JOIN x p INDEXED by y ON ar.d = p.c
      AND ar.f = p.f
      LEFT JOIN ak al ON ar.am = al.am
      LEFT JOIN at au INDEXED by av ON au.aw = ar.c
      AND au.f = ar.f
      LEFT JOIN ax ay ON ar.t = ay.t
      LEFT JOIN an u ON u.f = p.f
      AND u.q = p.q
    WHERE
      ar.f = 4797271
      AND COALESCE(ar.h, ar.j) = ('2024-08-14')
      AND al.ao = 8
      AND (coalesce(p.ap, ar.az) IN (42, 45))
  )
SELECT
  *
FROM
  n
UNION ALL
SELECT
  *
FROM
  aq option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q58
WITH
  a AS (
    SELECT
      TO_CHAR(COALESCE(b.c, b.d), '%Y-%m-%d') AS d,
      b.f,
      COALESCE(g.h, 0) AS h,
      b.i
    FROM
      j b
      LEFT JOIN j k ON k.i = b.l
      AND k.m = b.m
      LEFT JOIN n o ON b.p = o.l
      AND b.m = o.m
      LEFT JOIN q g ON b.r = g.r
    WHERE
      (
        b.m = 4797271
        AND coalesce(o.s, b.t) IN (42, 45)
        AND k.l IS NULL
        AND (
          b.i IS NULL
          OR b.i = -1
        )
        AND CAST(TO_CHAR(COALESCE(b.c, b.d), '%Y') AS INTEGER) = 2023
        AND CAST(TO_CHAR(COALESCE(b.c, b.d), '%m') AS INTEGER) = 9
      )
  )
SELECT
  TO_CHAR(d, '%Y-%m-%d') AS d,
  SUM(u) AS u,
  SUM(v) AS v,
  0 AS w
FROM
  x
WHERE
  (
    m = 4797271
    AND s IN (42, 45)
    AND y = 2023
    AND z = 9
  )
GROUP BY
  d
UNION ALL
SELECT
  d,
  0 AS u,
  0 AS v,
  COALESCE(
    SUM(
      f * h * (
        CASE
          WHEN i IS NOT NULL THEN -1
          ELSE 1
        END
      )
    ),
    0.0
  ) AS w
FROM
  a
GROUP BY
  d option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q59
SELECT DISTINCT
  a.b,
  a.c,
  a.d,
  f.g
FROM
  h a INDEXED BY i
  LEFT JOIN j f ON a.k = f.k
  AND a.b = f.b
WHERE
  (
    a.k = 4797271
    AND a.l IN (42, 45)
    AND m = 2023
    AND n = 9
  )
UNION
SELECT DISTINCT
  o.b AS b,
  coalesce(p.c, o.c) AS c,
  coalesce(p.d, o.d) AS d,
  f.g AS g
FROM
  q o
  LEFT JOIN q r ON r.s = o.t
  AND r.k = o.k
  LEFT JOIN u p ON o.v = p.t
  AND o.k = p.k
  LEFT JOIN w x ON o.y = x.y
  LEFT JOIN j f ON o.k = f.k
  AND o.b = f.b
  LEFT JOIN z aa ON o.ab = aa.ab
WHERE
  (
    o.k = 4797271
    AND coalesce(p.l, o.ac) IN (42, 45)
    AND CAST(TO_CHAR(COALESCE(o.ad, o.ae), '%Y') AS INTEGER) = 2023
    AND CAST(TO_CHAR(COALESCE(o.ad, o.ae), '%m') AS INTEGER) = 9
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q60
SELECT
  a AS a,
  SUM(b) AS b,
  SUM(c) AS c,
  0 AS d
FROM
  f
WHERE
  (
    g = 4797271
    AND h IN (42, 45)
    AND i = '2023-09-25'
  )
GROUP BY
  a
UNION ALL
SELECT
  j.a AS a,
  0 AS b,
  0 AS c,
  COALESCE(
    SUM(
      k.l * COALESCE(j.m, 0) * CASE
        WHEN k.n IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS d
FROM
  o k
  LEFT JOIN o p ON p.n = k.q
  AND p.g = k.g
  LEFT JOIN r s ON k.t = s.q
  AND k.g = s.g
  LEFT JOIN u j ON k.v = j.v
  LEFT JOIN w x ON k.y = x.y
WHERE
  (
    k.g = 4797271
    AND coalesce(s.h, k.z) IN (42, 45)
    AND COALESCE(k.aa, k.i) = '2023-09-25'
    AND p.q IS NULL
    AND (
      k.n IS NULL
      OR k.n = -1
    )
  )
GROUP BY
  j.a option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q61
SELECT DISTINCT
  a.b,
  a.c,
  a.d,
  f.g
FROM
  h a INDEXED BY i
  LEFT JOIN j f ON a.k = f.k
  AND a.b = f.b
WHERE
  (
    a.k = 4797271
    AND a.l IN (42, 45)
    AND a.m = '2023-09-25'
  )
UNION
SELECT DISTINCT
  n.b AS b,
  coalesce(o.c, n.c) AS c,
  coalesce(o.d, n.d) AS d,
  f.g AS g
FROM
  p n
  LEFT JOIN p q ON q.r = n.s
  AND q.k = n.k
  LEFT JOIN t o ON n.u = o.s
  AND n.k = o.k
  LEFT JOIN v w ON n.x = w.x
  LEFT JOIN j f ON n.k = f.k
  AND n.b = f.b
  LEFT JOIN y z ON n.aa = z.aa
WHERE
  (
    n.k = 4797271
    AND coalesce(o.l, n.ab) IN (42, 45)
    AND COALESCE(n.ac, n.m) = '2023-09-25'
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q62
WITH
  a AS (
    SELECT
      b,
      c,
      d,
      SUM(f) AS f,
      SUM(g) AS g,
      0 AS h
    FROM
      i
    WHERE
      (
        j = 4797271
        AND k IN (42, 45)
        AND b IN (2025, 2024, 2023)
      )
    GROUP BY
      b,
      c,
      d
  ),
  l AS (
    SELECT
      CAST(TO_CHAR(COALESCE(m.n, m.o), '%Y') AS INTEGER) AS b,
      CAST(
        (
          CAST(TO_CHAR(COALESCE(m.n, m.o), '%m') AS INT) - 1
        ) / 3 + 1 AS INT
      ) AS c,
      CAST(TO_CHAR(COALESCE(m.n, m.o), '%m') AS INTEGER) AS d,
      COALESCE(
        (
          m.p * COALESCE(q.r, 0.0) * CASE
            WHEN m.s IS NOT NULL THEN -1
            ELSE 1
          END
        ),
        0.0
      ) AS t
    FROM
      u m
      LEFT JOIN u v ON v.s = m.w
      AND v.j = m.j
      LEFT JOIN x y ON m.z = y.w
      AND m.j = y.j
      LEFT JOIN aa q ON m.ab = q.ab
      LEFT JOIN ac ad ON m.ae = ad.ae
    WHERE
      (
        m.j = 4797271
        AND coalesce(y.k, m.af) IN (42, 45)
        AND v.w IS NULL
        AND (
          m.s IS NULL
          OR m.s = -1
        )
        AND CAST(TO_CHAR(COALESCE(m.n, m.o), '%Y') AS INTEGER) IN (2025, 2024, 2023)
      )
  ),
  ag AS (
    SELECT
      b,
      c,
      d,
      0 AS f,
      0 AS g,
      SUM(t) AS h
    FROM
      l
    GROUP BY
      b,
      c,
      d
  )
SELECT
  *
FROM
  a
UNION ALL
SELECT
  *
FROM
  ag option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q63
SELECT
  a AS a,
  b AS c,
  d AS d,
  f AS f,
  CASE
    WHEN 0 > g THEN g
    ELSE 0.0
  END AS g,
  CASE
    WHEN 0 > h THEN h
    ELSE 0.0
  END AS h,
  CASE
    WHEN 0 > i THEN i
    ELSE 0.0
  END AS i,
  CASE
    WHEN 0 > COALESCE(j, 0.0) + COALESCE(k, 0.0) THEN COALESCE(j, 0.0) + COALESCE(k, 0.0)
    ELSE 0.0
  END AS l,
  CASE
    WHEN 0 > m THEN m
    ELSE 0.0
  END AS m
FROM
  n o
WHERE
  (o.a = 3023424) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q64
SELECT
  a.b AS b,
  CASE
    WHEN MIN(c.d) IS NOT NULL THEN TO_CHAR(MIN(c.d), '%Y-%m-%d')
    ELSE NULL
  END AS f,
  SUM(g.h) AS i
FROM
  j g
  JOIN k a ON g.l = a.m
  AND g.n = a.n
  JOIN o c ON g.p = c.p
  AND g.n = c.n
  LEFT JOIN q r ON a.s = r.t
WHERE
  (
    g.n = 3023424
    AND NOT r.u
    AND h > 0
  )
GROUP BY
  a.b option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q65
SELECT
  'KNO' AS a,
  b.c AS c,
  b.d AS d,
  b.f AS f,
  (
    CASE
      WHEN (
        g.h IS NOT NULL
        AND g.h = 3
      ) THEN 1
      ELSE 0
    END
  ) AS i,
  b.j AS j,
  SUM(
    (
      CASE
        WHEN (
          g.h IS NULL
          OR g.h <> 3
        )
        AND b.k > 0
        AND b.l <> 1 THEN 0.0
        ELSE COALESCE(b.k, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          g.h IS NULL
          OR g.h <> 3
        )
        AND b.m > 0 THEN 0.0
        ELSE COALESCE(b.m, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          g.h IS NULL
          OR g.h <> 3
        )
        AND b.n > 0 THEN 0.0
        ELSE COALESCE(b.n, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          g.h IS NULL
          OR g.h <> 3
        )
        AND b.o > 0 THEN 0.0
        ELSE COALESCE(b.o, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          g.h IS NULL
          OR g.h <> 3
        )
        AND b.p > 0 THEN 0.0
        ELSE COALESCE(b.p, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          g.h IS NULL
          OR g.h <> 3
        )
        AND b.q > 0 THEN 0.0
        ELSE COALESCE(b.q, 0.0)
      END
    )::decimal
  ) AS r,
  SUM(
    (
      CASE
        WHEN b.l = 2 THEN COALESCE(b.m, 0.0)
        ELSE 0.0
      END
    )::decimal
  ) AS s,
  SUM(
    (
      CASE
        WHEN b.l = 3 THEN COALESCE(b.n, 0.0)
        ELSE 0.0
      END
    )::decimal
  ) AS t,
  SUM(
    (
      CASE
        WHEN b.l IN (42, 45) THEN COALESCE(b.m, 0.0)
        ELSE 0.0
      END
    )::decimal
  ) AS u
FROM
  v b
  LEFT JOIN w g ON b.c = g.c
WHERE
  (x = 3023424)
GROUP BY
  b.c,
  b.d,
  b.f,
  (
    CASE
      WHEN (
        g.h IS NOT NULL
        AND g.h = 3
      ) THEN 1
      ELSE 0
    END
  ),
  b.j option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q66
WITH
  a AS (
    SELECT
      b.c AS c,
      b.d AS d,
      b.f AS f,
      b.g AS g,
      CASE
        WHEN (
          h.i IS NOT NULL
          AND h.i = 3
        ) THEN 1
        ELSE 0
      END AS j,
      COALESCE(
        (
          b.k * COALESCE(l.m, 0) * CASE
            WHEN b.n IS NOT NULL THEN -1
            ELSE 1
          END
        ),
        0.0
      ) AS o,
      p.q AS q
    FROM
      r b
      LEFT JOIN r s ON s.n = b.t
      AND s.u = b.u
      LEFT JOIN v h ON b.c = h.c
      LEFT JOIN w l ON b.i = l.i
      LEFT JOIN x p ON b.y = p.t
      AND b.u = p.u
    WHERE
      (
        b.u = 3023424
        AND s.t IS NULL
        AND (
          b.n IS NULL
          OR b.n = -1
        )
      )
  )
SELECT
  'BUF' AS z,
  c,
  d,
  f,
  j,
  g,
  COALESCE(SUM(o), 0.0) AS aa,
  COALESCE(
    SUM(
      CASE
        WHEN q = 2 THEN o
        ELSE 0.0
      END
    ),
    0.0
  ) AS ab,
  COALESCE(
    SUM(
      CASE
        WHEN q = 3 THEN o
        ELSE 0.0
      END
    ),
    0.0
  ) AS ac,
  COALESCE(
    SUM(
      CASE
        WHEN q IN (42, 45) THEN o
        ELSE 0.0
      END
    ),
    0.0
  ) AS ad
FROM
  a
GROUP BY
  c,
  d,
  f,
  j,
  g option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q67
SELECT
  a.b AS b,
  TO_CHAR(COALESCE(c.d, c.f), '%Y-%m-%d') AS g,
  COALESCE(
    (
      c.h * COALESCE(i.j, 0.0) * CASE
        WHEN c.k IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS l
FROM
  m c
  LEFT JOIN n a ON c.o = a.o
  LEFT JOIN p q ON c.o = q.o
  LEFT JOIN r i ON c.s = i.s
  LEFT JOIN m t ON t.k = c.u
  AND t.v = c.v
WHERE
  (
    c.v = 3023424
    AND t.u IS NULL
    AND (
      c.k IS NULL
      OR c.k = -1
    )
    AND (
      q.s IS NULL
      OR q.s <> 3
    )
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q68
SELECT
  a.b AS b,
  a.c AS c,
  a.d AS d,
  a.f AS f,
  CASE
    WHEN (
      g.h IS NOT NULL
      AND g.h = 3
    ) THEN 1
    ELSE 0
  END AS i,
  TO_CHAR(COALESCE(a.j, a.k), '%Y-%m-%d') AS l,
  COALESCE(
    (
      a.m * COALESCE(n.o, 0.0) * CASE
        WHEN a.p IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS q
FROM
  r a
  LEFT JOIN s t ON a.c = t.c
  LEFT JOIN u g ON a.c = g.c
  LEFT JOIN v n ON a.h = n.h
  LEFT JOIN r w ON w.p = a.x
  AND w.y = a.y
WHERE
  (
    a.y = 3023424
    AND w.x IS NULL
    AND (
      a.p IS NULL
      OR a.p = -1
    )
    AND (
      g.h IS NOT NULL
      AND g.h = 3
    )
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q69
SELECT
  a AS a,
  SUM(
    (
      CASE
        WHEN b < 0 THEN COALESCE(b, 0)
        ELSE 0
      END
    )::decimal
  ) AS c,
  SUM(
    (
      CASE
        WHEN b < 0
        AND (
          d.f IS NOT NULL
          AND d.f = 3
        ) THEN COALESCE(b, 0)
        ELSE 0
      END
    )::decimal
  ) AS g,
  SUM(
    (
      CASE
        WHEN b > 0
        AND (
          d.f IS NOT NULL
          AND d.f = 3
        ) THEN COALESCE(b, 0)
        ELSE 0
      END
    )::decimal
  ) AS h
FROM
  i j
  LEFT JOIN k d ON j.l = d.l
WHERE
  (
    j.m = 3023424
    AND j.b <> 0
  )
GROUP BY
  a option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q70
SELECT
  a.b AS b,
  a.c AS c,
  a.d AS d,
  a.f AS f,
  SUM(
    (
      CASE
        WHEN g < 0 THEN COALESCE(g, 0)
        ELSE 0
      END
    )::decimal
  ) AS h,
  SUM(
    (
      CASE
        WHEN g < 0
        AND (
          i.j IS NOT NULL
          AND i.j = 3
        ) THEN COALESCE(g, 0)
        ELSE 0
      END
    )::decimal
  ) AS k,
  SUM(
    (
      CASE
        WHEN g > 0
        AND (
          i.j IS NOT NULL
          AND i.j = 3
        ) THEN COALESCE(g, 0)
        ELSE 0
      END
    )::decimal
  ) AS l
FROM
  m a
  LEFT JOIN n i ON a.b = i.b
WHERE
  (a.o = 3023424)
GROUP BY
  a.b,
  a.c,
  a.d,
  a.f option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q71
SELECT
  a.b AS b,
  CASE
    WHEN MIN(c.d) IS NOT NULL THEN TO_CHAR(MIN(c.d), '%Y-%m-%d')
    ELSE NULL
  END AS f,
  SUM(g.h) AS i
FROM
  j g
  JOIN k a ON g.l = a.m
  AND g.n = a.n
  JOIN o c ON g.p = c.p
  AND g.n = c.n
  LEFT JOIN q r ON a.s = r.t
WHERE
  (
    g.n = 3023424
    AND NOT r.u
    AND h > 0
  )
GROUP BY
  a.b option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q72
SELECT
  a AS a,
  SUM(b) AS c,
  SUM(d) AS f
FROM
  g
WHERE
  (
    h = 4797271
    AND i = 4
    AND j = '2025-08-27'
  )
GROUP BY
  a option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q73
WITH
  a AS (
    SELECT
      b.c,
      b.d,
      b.f
    FROM
      (
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h >= ('2025-08-27'::datetime)
          AND h < ('2025-08-28'::datetime)
          AND COALESCE(i, 0) <> 1
        UNION ALL
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h IS NULL
          AND j >= ('2025-08-27'::datetime)
          AND j < ('2025-08-28'::datetime)
          AND COALESCE(i, 0) <> 1
      ) b
      JOIN k l ON l.m = b.c
      AND l.f = b.f
  ),
  n AS (
    SELECT
      o.f AS f,
      o.c AS m,
      p.q AS q,
      p.r AS r,
      o.s AS s,
      o.t AS t,
      COALESCE(o.u, o.v, 0) AS w,
      CASE
        WHEN o.u IS NOT NULL
        OR o.v IS NOT NULL THEN o.x
        ELSE '0000'
      END AS y,
      o.z AS aa,
      CASE
        WHEN o.u IS NOT NULL THEN 0
        WHEN o.v IS NOT NULL THEN 1
        ELSE 2
      END AS ab,
      ac.ad AS ad
    FROM
      a ae
      JOIN af p INDEXED by ag ON p.c = ae.d
      AND p.f = ae.f
      JOIN ah e INDEXED by ai ON e.m = ae.c
      AND e.f = p.f
      AND e.aj > 0
      JOIN ak ac INDEXED BY al ON ac.am = e.c
      AND ac.f = e.f
      AND ac.an IS NULL
      AND ac.ao IS NULL
      JOIN g o INDEXED by ap ON o.c = ac.aq
      AND o.f = ac.f
      AND COALESCE(o.i, 0) <> 1
      LEFT JOIN k ar INDEXED by at ON ar.m = o.c
      AND ar.f = o.f
      LEFT JOIN au av ON av.t = o.t
    WHERE
      av.aw = 6
      AND (p.ax = 4)
  ),
  ay AS (
    SELECT
      n.f,
      n.m,
      n.q,
      n.r,
      n.s,
      n.t,
      n.w,
      n.y,
      n.aa,
      n.ab,
      n.ad,
      ROW_NUMBER() OVER (
        ORDER BY
          n.m DESC
      ) AS az,
      ba.bb
    FROM
      n
      LEFT JOIN bc ba ON ba.f = n.f
      AND ba.q = n.q
  )
SELECT
  *
FROM
  ay
WHERE
  az > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q74
WITH
  a AS (
    SELECT
      b.c,
      b.d,
      b.f
    FROM
      (
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h >= ('2025-08-27')
          AND h < ('2025-08-28')
          AND COALESCE(i, 0) <> 1
        UNION ALL
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h IS NULL
          AND j >= ('2025-08-27')
          AND j < ('2025-08-28')
          AND COALESCE(i, 0) <> 1
      ) b
      JOIN k l ON l.m = b.c
      AND l.f = b.f
  ),
  n AS (
    SELECT
      o.p * q.r * CASE
        WHEN o.s IS NULL THEN 1
        ELSE -1
      END AS p
    FROM
      a t
      JOIN u v INDEXED by w ON v.c = t.d
      AND v.f = t.f
      JOIN x e INDEXED by y ON e.m = t.c
      AND e.f = v.f
      AND e.z > 0
      JOIN aa ab INDEXED BY ac ON ab.ad = e.c
      AND ab.f = e.f
      AND ab.ae IS NULL
      AND ab.af IS NULL
      JOIN g o INDEXED by ag ON o.c = ab.ah
      AND o.f = ab.f
      AND COALESCE(o.i, 0) <> 1
      LEFT JOIN k ai INDEXED by aj ON ai.m = o.c
      AND ai.f = o.f
      LEFT JOIN ak q ON q.al = o.al
    WHERE
      q.am = 6
      AND (v.an = 4)
  )
SELECT
  COUNT(*) AS ao,
  SUM(p) AS ap
FROM
  n option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q75
WITH
  a AS (
    SELECT
      b.c,
      b.d,
      b.f
    FROM
      (
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h >= ('2025-08-27')
          AND h < ('2025-08-28')
          AND COALESCE(i, 0) <> 1
        UNION ALL
        SELECT
          c,
          d,
          f
        FROM
          g
        WHERE
          f = 4797271
          AND h IS NULL
          AND j >= ('2025-08-27')
          AND j < ('2025-08-28')
          AND COALESCE(i, 0) <> 1
      ) b
      JOIN k l ON l.m = b.c
      AND l.f = b.f
  ),
  n AS (
    SELECT DISTINCT
      o.f AS f,
      p.q AS q,
      p.r AS r,
      p.s AS s,
      o.t AS t,
      u.v AS v
    FROM
      a w
      JOIN x p INDEXED by y ON p.c = w.d
      AND p.f = w.f
      JOIN z e INDEXED by aa ON e.m = w.c
      AND p.f = e.f
      AND e.ab > 0
      JOIN ac ad INDEXED by ae ON ad.af = e.c
      AND ad.f = e.f
      AND ad.ag IS NULL
      AND ad.ah IS NULL
      JOIN g o INDEXED by ai ON o.c = ad.aj
      AND o.f = ad.f
      AND COALESCE(o.i, 0) <> 1
      LEFT JOIN k ak INDEXED by al ON ak.m = o.c
      AND ak.f = o.f
      LEFT JOIN am an ON an.ao = o.ao
      LEFT JOIN ap u ON u.f = p.f
      AND u.q = p.q
    WHERE
      an.aq = 6
      AND (p.ar = 4)
  )
SELECT
  n.f,
  n.q,
  n.r,
  n.s,
  n.t,
  n.v
FROM
  n option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q76
SELECT
  TO_CHAR(a, '%Y-%m-%d') AS a,
  SUM(b) AS c,
  SUM(d) AS f
FROM
  g
WHERE
  (
    h = 4797271
    AND i = 4
    AND j = 2025
    AND k = 8
  )
GROUP BY
  h,
  a
ORDER BY
  a DESC option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q77
SELECT
  a AS a,
  b AS b,
  c AS c,
  d AS d,
  SUM(f) AS g,
  SUM(h) AS i
FROM
  j
WHERE
  (k = 4797271)
GROUP BY
  d,
  a,
  b,
  c option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q78
SELECT
  a,
  b,
  c
FROM
  d f
WHERE
  (f.g = 3023424)
ORDER BY
  a
LIMIT
  1 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q79
SELECT
  a.b AS b,
  CASE
    WHEN MIN(c.d) IS NOT NULL THEN TO_CHAR(MIN(c.d), '%Y-%m-%d')
    ELSE NULL
  END AS f,
  SUM(g.h) AS i
FROM
  j g
  JOIN k a ON g.l = a.m
  AND g.n = a.n
  JOIN o c ON g.p = c.p
  AND g.n = c.n
  LEFT JOIN q r ON a.s = r.t
WHERE
  (
    g.n = 3023424
    AND NOT r.u
    AND h > 0
  )
GROUP BY
  a.b option (
    SQL_VDBE_OPCODE_MAX = 798000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q80
SELECT
  'KNO' AS a,
  b.c AS c,
  b.d AS d,
  b.f AS f,
  (
    CASE
      WHEN (
        g.h IS NOT NULL
        AND g.h = 3
      ) THEN 1
      ELSE 0
    END
  ) AS i,
  b.j AS j,
  SUM(
    (
      CASE
        WHEN (
          g.h IS NULL
          OR g.h <> 3
        )
        AND b.k > 0
        AND b.l <> 1 THEN 0.0
        ELSE COALESCE(b.k, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          g.h IS NULL
          OR g.h <> 3
        )
        AND b.m > 0 THEN 0.0
        ELSE COALESCE(b.m, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          g.h IS NULL
          OR g.h <> 3
        )
        AND b.n > 0 THEN 0.0
        ELSE COALESCE(b.n, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          g.h IS NULL
          OR g.h <> 3
        )
        AND b.o > 0 THEN 0.0
        ELSE COALESCE(b.o, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          g.h IS NULL
          OR g.h <> 3
        )
        AND b.p > 0 THEN 0.0
        ELSE COALESCE(b.p, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          g.h IS NULL
          OR g.h <> 3
        )
        AND b.q > 0 THEN 0.0
        ELSE COALESCE(b.q, 0.0)
      END
    )::decimal
  ) AS r,
  SUM(
    (
      CASE
        WHEN b.l = 2 THEN COALESCE(b.m, 0.0)
        ELSE 0.0
      END
    )::decimal
  ) AS s,
  SUM(
    (
      CASE
        WHEN b.l = 3 THEN COALESCE(b.n, 0.0)
        ELSE 0.0
      END
    )::decimal
  ) AS t,
  SUM(
    (
      CASE
        WHEN b.l IN (42, 45) THEN COALESCE(b.m, 0.0)
        ELSE 0.0
      END
    )::decimal
  ) AS u
FROM
  v b
  LEFT JOIN w g ON b.c = g.c
WHERE
  (x = 3023424)
GROUP BY
  b.c,
  b.d,
  b.f,
  (
    CASE
      WHEN (
        g.h IS NOT NULL
        AND g.h = 3
      ) THEN 1
      ELSE 0
    END
  ),
  b.j option (
    SQL_VDBE_OPCODE_MAX = 900000,
    SQL_MOTION_ROW_MAX = 5100
  );

-- TEST: q81
WITH
  a AS (
    SELECT
      b.c AS c,
      b.d AS d,
      b.f AS f,
      b.g AS g,
      CASE
        WHEN (
          h.i IS NOT NULL
          AND h.i = 3
        ) THEN 1
        ELSE 0
      END AS j,
      COALESCE(
        (
          b.k * COALESCE(l.m, 0) * CASE
            WHEN b.n IS NOT NULL THEN -1
            ELSE 1
          END
        ),
        0.0
      ) AS o,
      p.q AS q
    FROM
      r b
      LEFT JOIN r s ON s.n = b.t
      AND s.u = b.u
      LEFT JOIN v h ON b.c = h.c
      LEFT JOIN w l ON b.i = l.i
      LEFT JOIN x p ON b.y = p.t
      AND b.u = p.u
    WHERE
      (
        b.u = 3023424
        AND s.t IS NULL
        AND (
          b.n IS NULL
          OR b.n = -1
        )
      )
  )
SELECT
  'BUF' AS z,
  c,
  c,
  d,
  f,
  j,
  g,
  COALESCE(SUM(o), 0.0) AS aa,
  COALESCE(
    SUM(
      CASE
        WHEN q = 2 THEN o
        ELSE 0.0
      END
    ),
    0.0
  ) AS ab,
  COALESCE(
    SUM(
      CASE
        WHEN q = 3 THEN o
        ELSE 0.0
      END
    ),
    0.0
  ) AS ac,
  COALESCE(
    SUM(
      CASE
        WHEN q IN (42, 45) THEN o
        ELSE 0.0
      END
    ),
    0.0
  ) AS ad
FROM
  a
GROUP BY
  c,
  d,
  f,
  j,
  g option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q82
SELECT DISTINCT
  a AS a
FROM
  b c
  JOIN d f ON c.g = f.g
  AND c.h = f.h
WHERE
  (c.h = 2466827)
  AND c.i > 0 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q83
SELECT
  count(*) AS a
FROM
  (
    SELECT DISTINCT
      b.c,
      d.f,
      d.g,
      b.h,
      b.i,
      b.j,
      b.k,
      l.m
    FROM
      n b
      LEFT JOIN o l ON b.j = l.p
      LEFT JOIN q d ON b.r = d.r
      AND b.i = d.i
    WHERE
      (
        b.r = 2466827
        AND b.s IN (126412759, 126412766, 566228222)
        AND b.c = 4
      )
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q84
WITH
  a AS (
    SELECT
      b.c AS c,
      b.d AS d,
      b.f AS f,
      b.g AS g,
      b.h AS h,
      i.j AS j,
      k.l AS l,
      k.m AS m,
      COALESCE(SUM(n + o + p + q + r + s), 0.0) AS t
    FROM
      u b
      LEFT JOIN v i ON b.g = i.w
      LEFT JOIN x k ON b.y = k.y
    WHERE
      (
        b.y = 2466827
        AND b.z IN (126412759, 126412766, 566228222)
        AND b.c = 4
      )
    GROUP BY
      c,
      l,
      m,
      d,
      b.f,
      g,
      h,
      i.j
  ),
  aa AS (
    SELECT
      a.*,
      ROW_NUMBER() OVER (
        ORDER BY
          t
      ) AS ab
    FROM
      a
    ORDER BY
      t
  )
SELECT
  *
FROM
  aa
WHERE
  ab > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q85
WITH
  a AS (
    SELECT
      a.b AS b,
      a.c AS d,
      a.f AS f,
      a.g AS g,
      a.h AS h,
      a.i AS i,
      COALESCE(a.j, 0.0) AS j
    FROM
      k a
    WHERE
      (
        a.b = 4797271
        AND a.l = 2
        AND a.g IN (1, 2)
      )
  ),
  m AS (
    SELECT
      a.b AS b,
      a.d AS d,
      m.c AS n,
      a.f AS f,
      a.g AS g,
      a.h AS h,
      a.i AS o,
      m.p AS p,
      a.j AS j
    FROM
      a
      JOIN q m ON a.d = m.r
      AND a.b = m.b
  ),
  s AS (
    SELECT
      m.b AS b,
      m.d AS d,
      m.f AS f,
      m.g AS g,
      m.h AS h,
      m.o AS o,
      m.p AS p,
      m.j AS j,
      s.c AS t
    FROM
      m
      JOIN u s ON m.n = s.v
      AND m.b = s.b
  ),
  w AS (
    SELECT
      s.b AS b,
      s.d AS d,
      s.f AS f,
      s.g AS g,
      s.h AS h,
      s.o AS o,
      s.j + CASE
        WHEN s.p IN (42, 45)
        AND w.x IS NOT NULL THEN w.x
        ELSE 0
      END AS y,
      w.z AS z
    FROM
      s
      JOIN aa w ON s.t = w.ab
      AND s.b = w.b
  ),
  ac AS (
    SELECT
      d,
      f,
      CASE
        WHEN g = 1 THEN 'installment'
        WHEN g = 2 THEN 'deferral'
        WHEN g = 4 THEN 'ink'
        WHEN g = 5 THEN 'restructurisation'
        ELSE NULL
      END AS ad,
      TO_CHAR(h, '%Y-%m-%d') AS h,
      o AS o,
      SUM(y) AS ae,
      SUM(z) AS af,
      ROW_NUMBER() OVER (
        ORDER BY
          h,
          d
      ) AS ag
    FROM
      w
    GROUP BY
      d,
      f,
      g,
      h,
      o
    ORDER BY
      h,
      d
  )
SELECT
  *
FROM
  ac
WHERE
  ag > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 50000
  );

-- TEST: q86
SELECT
  count(a.b) AS c
FROM
  d a
WHERE
  (
    a.f = 4797271
    AND a.g = 2
    AND a.h IN (1, 2)
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 50000
  );

-- TEST: q87
SELECT
  a.b AS c,
  a.d AS f,
  a.g AS h,
  a.i AS i
FROM
  j a
WHERE
  a.b = '18201061201010000510' option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q88
SELECT
  a.b AS c,
  a.d AS f,
  a.g AS h,
  a.i AS i
FROM
  j a option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q89
SELECT DISTINCT
  a.b AS b,
  c.d AS f
FROM
  g a
  JOIN h c ON a.i = c.i
  AND a.b = c.b
WHERE
  (
    a.i = 3023424
    AND (
      LOWER(a.b) LIKE '%013%'
      OR LOWER(c.d) LIKE '%
013%'
    )
  )
LIMIT
  2147483647 option (
    SQL_VDBE_OPCODE_MAX = 150000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q90
SELECT DISTINCT
  a.b AS b,
  c.d AS f
FROM
  g a
  JOIN h c ON a.b = c.b
WHERE
  (a.i = 3023424)
LIMIT
  20 option (
    SQL_VDBE_OPCODE_MAX = 127000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q91
WITH
  a AS (
    SELECT
      b.c AS c,
      b.d AS d,
      f.g AS g,
      b.h AS i,
      b.j AS j,
      b.k AS k,
      b.l AS l,
      b.m AS m,
      b.n AS n,
      b.o AS o,
      ROW_NUMBER() OVER () AS p
    FROM
      q b
      LEFT JOIN r f ON b.d = f.d
      LEFT JOIN s t ON b.d = t.u
    WHERE
      (
        b.v = 2337497
        AND f.g = 4
      )
  )
SELECT
  *
FROM
  a
WHERE
  p > 0
LIMIT
  20 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );
