-- Corpus of anonymized real-world DQL queries.
-- Global: Tables a-.. | Columns a-z,aa-.. | Indexes ix_a,..
-- Local per query: CTEs cte0,.. | Table aliases t0,t1,.. | Result columns c0,c1,..
-- Consumed by corpus.rs; each `-- TEST:` comment names the following statement.
-- TEST: q1
SELECT
  CASE
    WHEN t1.c2 > 0 THEN TRUE
    ELSE FALSE
  END AS c0,
  COALESCE(t2.c3 * -1, 0.0)::decimal AS c1
FROM
  (
    SELECT
      COUNT(*) AS c2
    FROM
      z t0
    WHERE
      (t0.cv = 3023424)
  ) t1
  LEFT JOIN (
    SELECT
      SUM(cp) AS c3
    FROM
      ac t0
    WHERE
      (t0.cv = 3023424)
      AND cp > 0
  ) t2 ON 1 = 1 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q2
SELECT
  TO_CHAR(a, '%Y-%m-%d') AS c0,
  SUM(f) AS c1,
  SUM(e) AS c2,
  SUM(g) AS c3
FROM
  a
WHERE
  (
    cv = 3023424
    AND aw = 0
    AND cq = 22
    AND bm = 2023
    AND bk = 7
  )
GROUP BY
  a option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q3
WITH
  cte0 AS (
    SELECT
      TO_CHAR(COALESCE(t0.cs, t0.a), '%Y-%m-%d') AS c0,
      (
        t0.d * COALESCE(t3.cj, 0) * CASE
          WHEN t0.h IS NOT NULL THEN -1
          ELSE 1
        END
      ) AS c1
    FROM
      n t0
      LEFT JOIN y t1 ON t0.au = t1.ag
      AND t0.cv = t1.cv
      LEFT JOIN n t2 ON t2.h = t0.ag
      AND t2.cv = t0.cv
      LEFT JOIN f t3 ON t0.cw = t3.cw
      LEFT JOIN h t4 ON t0.cu = t4.cu
    WHERE
      (
        t0.cv = 3023424
        AND coalesce(t1.aw, t0.bf) = 0
        AND CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%Y') AS integer) = 2023
        AND CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%m') AS integer) = 7
        AND coalesce(t1.cq, t4.cq) = 22
        AND t2.ag IS NULL
        AND (
          t0.h IS NULL
          OR t0.h = -1
        )
      )
  )
SELECT
  c0,
  COALESCE(SUM(c1), 0.0) AS c1
FROM
  cte0
GROUP BY
  c0 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q4
SELECT DISTINCT
  t0.ay,
  t0.bg,
  t0.ah,
  t1.cx
FROM
  a t0 INDEXED BY ix_b
  LEFT JOIN ad t1 ON t0.cv = t1.cv
  AND t0.ay = t1.ay
WHERE
  (
    t0.cv = 3023424
    AND t0.aw = 0
    AND bm = 2023
    AND bk = 7
    AND t0.cq = 22
  )
UNION
SELECT DISTINCT
  t2.ay AS c0,
  coalesce(t4.bg, t2.bg) AS c1,
  coalesce(t4.ah, t2.ah) AS c2,
  t1.cx AS c3
FROM
  n t2
  LEFT JOIN n t3 ON t3.h = t2.ag
  AND t3.cv = t2.cv
  LEFT JOIN y t4 ON t2.au = t4.ag
  AND t2.cv = t4.cv
  LEFT JOIN f t5 ON t2.cw = t5.cw
  LEFT JOIN ad t1 ON t2.cv = t1.cv
  AND t2.ay = t1.ay
  LEFT JOIN h t6 ON t2.cu = t6.cu
WHERE
  (
    t2.cv = 3023424
    AND coalesce(t4.aw, t2.bf) = 0
    AND CAST(TO_CHAR(COALESCE(t2.cs, t2.a), '%Y') AS integer) = 2023
    AND CAST(TO_CHAR(COALESCE(t2.cs, t2.a), '%m') AS integer) = 7
    AND coalesce(t4.cq, t6.cq) = 22
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q5
SELECT
  z AS c0,
  SUM(f) AS c1,
  SUM(e) AS c2,
  SUM(g) AS c3
FROM
  a
WHERE
  (
    cv = 3023424
    AND aw = 0
    AND cq = 22
    AND a = '2023-07-28'
  )
GROUP BY
  z option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q6
SELECT
  t2.z AS c0,
  COALESCE(
    SUM(
      t0.d * COALESCE(t2.cj, 0) * CASE
        WHEN t0.h IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS c1
FROM
  n t0
  LEFT JOIN y t1 ON t0.au = t1.ag
  AND t0.cv = t1.cv
  LEFT JOIN f t2 ON t0.cw = t2.cw
  LEFT JOIN h t3 ON t0.cu = t3.cu
  LEFT JOIN n t4 ON t4.h = t0.ag
  AND t4.cv = t0.cv
WHERE
  (
    t0.cv = 3023424
    AND coalesce(t1.aw, t0.bf) = 0
    AND coalesce(t1.cq, t3.cq) = 22
    AND COALESCE(t0.cs, t0.a) = '2023-07-28'
    AND t4.ag IS NULL
    AND (
      t0.h IS NULL
      OR t0.h = -1
    )
  )
GROUP BY
  t2.z option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q7
SELECT DISTINCT
  t0.ay,
  t0.bg,
  t0.ah,
  t1.cx
FROM
  a t0 INDEXED BY ix_c
  LEFT JOIN ad t1 ON t0.cv = t1.cv
  AND t0.ay = t1.ay
WHERE
  (
    t0.cv = 3023424
    AND t0.aw = 0
    AND t0.a = '2023-07-28'
    AND t0.cq = 22
  )
UNION
SELECT DISTINCT
  t2.ay AS c0,
  coalesce(t4.bg, t2.bg) AS c1,
  coalesce(t4.ah, t2.ah) AS c2,
  t1.cx AS c3
FROM
  n t2
  LEFT JOIN n t3 ON t3.h = t2.ag
  AND t3.cv = t2.cv
  LEFT JOIN y t4 ON t2.au = t4.ag
  AND t2.cv = t4.cv
  LEFT JOIN f t5 ON t2.cw = t5.cw
  LEFT JOIN ad t1 ON t2.cv = t1.cv
  AND t2.ay = t1.ay
  LEFT JOIN h t6 ON t2.cu = t6.cu
WHERE
  (
    t2.cv = 3023424
    AND coalesce(t4.aw, t2.bf) = 0
    AND COALESCE(t2.cs, t2.a) = '2023-07-28'
    AND coalesce(t4.cq, t6.cq) = 22
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q8
SELECT
  ag AS c0,
  cv AS c1,
  CASE
    WHEN 0 > bx THEN bx
    ELSE 0.0
  END AS c2,
  CASE
    WHEN 0 > bz THEN bz
    ELSE 0.0
  END AS c3,
  CASE
    WHEN 0 > COALESCE(cd, 0.0) + COALESCE(cc, 0.0) THEN COALESCE(cd, 0.0) + COALESCE(cc, 0.0)
    ELSE 0.0
  END AS c4,
  CASE
    WHEN 0 > ca THEN ca
    ELSE 0.0
  END AS c5,
  CASE
    WHEN 0 > cg THEN cg
    ELSE 0.0
  END AS c6
FROM
  s t0
WHERE
  (t0.cv = 3023424) option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q9
SELECT
  coalesce(t2.aw, t0.bf) AS c0,
  coalesce(t2.cq, t4.cq) AS c1,
  COALESCE(t0.cs, t0.a) AS c2,
  COALESCE(
    (
      t0.d * COALESCE(t3.cj, 0.0) * CASE
        WHEN t0.h IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS c3
FROM
  n t0
  LEFT JOIN n t1 ON t1.h = t0.ag
  AND t1.cv = t0.cv
  LEFT JOIN y t2 ON t0.au = t2.ag
  AND t0.cv = t2.cv
  LEFT JOIN f t3 ON t0.cw = t3.cw
  LEFT JOIN h t4 ON t0.cu = t4.cu
WHERE
  (
    t0.cv = 3023424
    AND t1.ag IS NULL
    AND (
      t0.h IS NULL
      OR t0.h = -1
    )
  ) option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q10
SELECT
  SUM(CAST(p * 100 AS integer) / 100.0) AS c0
FROM
  t t0
WHERE
  (
    t0.cv = 3023424
    AND p < 0
  ) option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q11
WITH
  cte0 AS (
    SELECT
      t0.ag,
      t0.au,
      t0.cv
    FROM
      (
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 3023424
          AND cs >= ('2023-04-28')
          AND cs < ('2023-04-29')
          AND COALESCE(bb, 0) <> 1
        UNION ALL
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 3023424
          AND cs IS NULL
          AND a >= ('2023-04-28')
          AND a < ('2023-04-29')
          AND COALESCE(bb, 0) <> 1
      ) t0
      LEFT JOIN x t1 ON t1.b = t0.ag
      AND t1.cv = t0.cv
    WHERE
      t1.b IS NULL
    ORDER BY
      t0.ag
  ),
  cte1 AS (
    SELECT
      t6.cv AS cv,
      t6.ag AS b,
      t3.ay AS ay,
      t3.bg AS bg,
      t6.cu AS cu,
      t6.cw AS cw,
      COALESCE(t6.co, t6.cm, 0) AS ac,
      CASE
        WHEN t6.co IS NOT NULL
        OR t6.cm IS NOT NULL THEN t6.cn
        ELSE '0000'
      END AS ae,
      TO_CHAR(t6.cl, '%Y-%m-%d') AS ab,
      CASE
        WHEN t6.co IS NOT NULL THEN 0
        WHEN t6.cm IS NOT NULL THEN 1
        ELSE 2
      END AS c9,
      t5.d AS d
    FROM
      cte0 t2
      JOIN y t3 INDEXED by ix_i ON t3.ag = t2.au
      AND t3.cv = t2.cv
      JOIN v t4 INDEXED by ix_f ON t4.b = t2.ag
      AND t4.cv = t3.cv
      AND t4.ai < 0
      JOIN w t5 INDEXED BY ix_g ON t5.x = t4.ag
      AND t5.cv = t4.cv
      AND t5.db IS NULL
      AND t5.da IS NULL
      JOIN l t6 INDEXED by ix_d ON t6.ag = t5.bs
      AND t6.cv = t5.cv
      AND COALESCE(t6.bb, 0) <> 1
      LEFT JOIN x t7 ON t7.b = t6.ag
      AND t7.cv = t6.cv
      LEFT JOIN f t8 ON t8.cw = t6.cw
    WHERE
      t8.z = 7
      AND (
        t3.aw = 0
        AND t3.cq = 22
      )
  ),
  cte2 AS (
    SELECT
      t9.cv AS cv,
      t9.ag AS b,
      t9.ay AS ay,
      coalesce(t3.bg, t9.bg) AS bg,
      coalesce(t3.cu, t9.cu) AS cu,
      t9.cw AS cw,
      COALESCE(t9.co, t9.cm, 0) AS ac,
      t9.cn AS ae,
      TO_CHAR(t9.cl, '%Y-%m-%d') AS ab,
      CASE
        WHEN t9.co IS NOT NULL THEN 0
        ELSE 1
      END AS c9,
      COALESCE(
        (
          t9.d * COALESCE(t8.cj, 0.0) * (
            CASE
              WHEN t9.h IS NOT NULL THEN -1
              ELSE 1
            END
          )
        ),
        0.0
      ) AS d
    FROM
      n t9
      LEFT JOIN y t3 INDEXED by ix_i ON t9.au = t3.ag
      AND t3.cv = t9.cv
      LEFT JOIN f t8 ON t9.cw = t8.cw
      LEFT JOIN n t10 INDEXED by ix_e ON t10.h = t9.ag
      AND t10.cv = t3.cv
      LEFT JOIN h t11 ON t9.cu = t11.cu
    WHERE
      t9.cv = 3023424
      AND COALESCE(t9.cs, t9.a) = ('2023-04-28')
      AND t8.z = 7
      AND (
        coalesce(t3.aw, t9.bf) = 0
        AND coalesce(t3.cq, t11.cq) = 22
      )
  ),
  cte3 AS (
    SELECT
      *
    FROM
      cte1
    UNION ALL
    SELECT
      *
    FROM
      cte2
  ),
  cte4 AS (
    SELECT
      cte3.cv,
      cte3.b,
      cte3.ay,
      cte3.bg,
      cte3.cu,
      cte3.cw,
      cte3.ac,
      cte3.ae,
      cte3.ab,
      cte3.c9,
      cte3.d,
      t12.cx,
      ROW_NUMBER() OVER (
        ORDER BY
          b DESC
      ) AS c11
    FROM
      cte3
      LEFT JOIN ad t12 ON t12.cv = cte3.cv
      AND t12.ay = cte3.ay
  )
SELECT
  *
FROM
  cte4
WHERE
  c11 > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 700000,
    SQL_MOTION_ROW_MAX = 17000
  );

-- TEST: q12
WITH
  cte0 AS (
    SELECT
      t0.ag,
      t0.au,
      t0.cv
    FROM
      (
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 3023424
          AND cs >= ('2023-04-28')
          AND cs < ('2023-04-28')
          AND COALESCE(bb, 0) <> 1
        UNION ALL
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 3023424
          AND cs IS NULL
          AND a >= ('2023-04-28')
          AND a < ('2023-04-28')
          AND COALESCE(bb, 0) <> 1
      ) t0
      LEFT JOIN x t1 ON t1.b = t0.ag
      AND t1.cv = t0.cv
    WHERE
      t1.b IS NULL
  ),
  cte1 AS (
    SELECT DISTINCT
      t6.cv AS cv,
      t3.ay AS ay,
      t3.bg AS bg,
      t3.ah AS ah,
      t6.cu AS cu,
      t8.cx AS cx
    FROM
      cte0 t2
      JOIN y t3 INDEXED by ix_i ON t3.ag = t2.au
      AND t3.cv = t2.cv
      JOIN v t4 INDEXED by ix_f ON t4.b = t2.ag
      AND t4.cv = t3.cv
      AND t4.ai < 0
      JOIN w t5 INDEXED by ix_g ON t5.x = t4.ag
      AND t5.db IS NULL
      AND t5.da IS NULL
      AND t5.cv = t4.cv
      JOIN l t6 INDEXED by ix_d ON t6.ag = t5.bs
      AND t6.cv = t5.cv
      AND COALESCE(t6.bb, 0) <> 1
      LEFT JOIN f t7 ON t7.cw = t6.cw
      LEFT JOIN ad t8 ON t8.cv = t3.cv
      AND t8.ay = t3.ay
    WHERE
      t7.z = 7
      AND (
        t3.aw = 0
        AND t3.cq = 22
      )
  ),
  cte2 AS (
    SELECT DISTINCT
      t9.cv AS cv,
      t9.ay AS ay,
      coalesce(t3.bg, t9.bg) AS bg,
      coalesce(t3.ah, t9.ah) AS ah,
      coalesce(t3.cu, t9.cu) AS cu,
      t8.cx AS cx
    FROM
      n t9
      LEFT JOIN y t3 INDEXED by ix_i ON t9.au = t3.ag
      AND t9.cv = t3.cv
      LEFT JOIN f t7 ON t9.cw = t7.cw
      LEFT JOIN n t10 INDEXED by ix_e ON t10.h = t9.ag
      AND t10.cv = t9.cv
      LEFT JOIN h t11 ON t9.cu = t11.cu
      LEFT JOIN ad t8 ON t8.cv = t3.cv
      AND t8.ay = t3.ay
    WHERE
      t9.cv = 3023424
      AND COALESCE(t9.cs, t9.a) = ('2023-04-28')
      AND t7.z = 7
      AND (
        coalesce(t3.aw, t9.bf) = 0
        AND coalesce(t3.cq, t11.cq) = 22
      )
  )
SELECT
  *
FROM
  cte1
UNION ALL
SELECT
  *
FROM
  cte2 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q13
SELECT
  cq AS c0,
  bk AS c1,
  bl AS c2,
  bm AS c3,
  SUM(f) AS c4,
  SUM(e) AS c5
FROM
  b
WHERE
  (
    cv = 3023424
    AND aw = 0
    AND bm IN (2024, 2023, 2021, 2022, 2019, 2018)
  )
GROUP BY
  cq,
  bk,
  bl,
  bm option (
    SQL_VDBE_OPCODE_MAX = 900000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q14
SELECT
  coalesce(t1.cq, t4.cq) AS c0,
  CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%Y') AS integer) AS c1,
  CAST(
    (
      CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%m') AS int) - 1
    ) / 3 + 1 AS int
  ) AS c2,
  CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%m') AS integer) AS c3,
  TO_CHAR(COALESCE(t0.cs, t0.a), '%Y-%m-%d') AS c4,
  COALESCE(
    (
      t0.d * COALESCE(t3.cj, 0.0) * CASE
        WHEN t0.h IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS c5
FROM
  n t0
  LEFT JOIN y t1 ON t0.au = t1.ag
  AND t0.cv = t1.cv
  LEFT JOIN g t2 ON t0.cu = t2.cu
  LEFT JOIN f t3 ON t0.cw = t3.cw
  LEFT JOIN h t4 ON t0.cu = t4.cu
  LEFT JOIN n t5 ON t5.h = t0.ag
  AND t5.cv = t0.cv
WHERE
  (
    t0.cv = 3023424
    AND coalesce(t1.aw, t0.bf) = 0
    AND CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%Y') AS integer) IN (2024, 2023, 2021, 2022, 2019, 2018)
    AND t5.ag IS NULL
    AND (
      t0.h IS NULL
      OR t0.h = -1
    )
  ) option (
    SQL_VDBE_OPCODE_MAX = 100000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q15
SELECT
  cq AS c0,
  bk AS c1,
  bl AS c2,
  bm AS c3,
  SUM(g) AS c4
FROM
  e
WHERE
  (
    cv = 3023424
    AND g > 0
    AND cq IN (
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
    AND bm IN (2024, 2023, 2021, 2022, 2019, 2018)
  )
GROUP BY
  cq,
  bk,
  bl,
  bm option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q16
WITH
  cte0 AS (
    SELECT
      cte0.cv AS cv,
      cte0.ag AS c1,
      cte0.r AS c2,
      cte0.s AS c3,
      TO_CHAR(cte0.q, '%Y-%m-%d') AS c4,
      cte0.bn AS bn,
      cte0.m AS c6
    FROM
      o cte0
    WHERE
      (
        cte0.cv = 4797271
        AND cte0.s IN (1, 2)
      )
  ),
  cte1 AS (
    SELECT
      cte0.cv AS cv,
      cte0.c1 AS c1,
      cte0.c2 AS c2,
      cte0.c3 AS c3,
      cte0.c4 AS c4,
      cte0.bn AS bn,
      cte0.c6 AS c6,
      cte1.aw AS aw,
      cte1.av AS cu,
      cte1.ag AS c9
    FROM
      cte0
      JOIN p cte1 ON cte0.c1 = cte1.l
      AND cte0.cv = cte1.cv
    WHERE
      cte1.af <> 0
  ),
  cte2 AS (
    SELECT
      cte1.cv AS cv,
      cte1.c1 AS c1,
      cte1.c2 AS c2,
      cte1.c3 AS c3,
      cte1.c4 AS c4,
      cte1.bn AS bn,
      cte1.c6 AS c6,
      cte1.aw AS aw,
      cte1.cu AS cu,
      cte2.ag AS c10
    FROM
      cte1
      JOIN q cte2 ON cte1.c9 = cte2.j
      AND cte1.cv = cte2.cv
  ),
  cte3 AS (
    SELECT
      cte2.cv AS cv,
      cte2.c1 AS c1,
      cte2.c2 AS c2,
      cte2.c3 AS c3,
      cte2.c4 AS c4,
      cte2.bn AS bn,
      cte2.c6 AS c6,
      cte2.aw AS aw,
      cte2.cu AS cu,
      cte3.ci AS ci,
      COALESCE(cte3.y, 0.0) AS y,
      TO_CHAR(cte3.ch, '%Y-%m-%d') AS ch
    FROM
      cte2
      JOIN r cte3 ON cte2.c10 = cte3.k
      AND cte2.cv = cte3.cv
    WHERE
      cte3.o = to_date('3000-01-01', '%Y-%m-%d')
      AND cte3.an = 0
  )
SELECT
  *
FROM
  cte3 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q17
WITH
  cte0 AS (
    SELECT
      t0.ag AS c0,
      t5.ag AS c1,
      1 AS ck,
      t4.al AS c3,
      t0.cv AS cv,
      t0.w AS a,
      coalesce(t5.ay, t0.ay) AS ay,
      coalesce(t5.bg, t0.bg) AS bh,
      t0.cw AS cw,
      t1.z AS z,
      coalesce(t6.bj, t0.bj) AS bj,
      t5.bm AS c11,
      coalesce(t5.cu, t0.cu) AS cu,
      t0.aa AS aa,
      coalesce(t5.co, t5.cm, t0.ac) AS ac,
      CASE
        WHEN t5.co IS NOT NULL THEN 0
        WHEN t5.cm IS NOT NULL THEN 1
        ELSE t0.ad
      END AS ad,
      coalesce(t5.cl, t0.ab) AS ab,
      CASE
        WHEN t5.co IS NOT NULL
        OR t5.cm IS NOT NULL THEN t5.cn
        ELSE t0.ae
      END AS ae,
      CASE
        WHEN t0.h IS NOT NULL THEN TRUE
        ELSE FALSE
      END AS c18
    FROM
      m t0
      JOIN f t1 ON t1.cw = t0.cw
      LEFT JOIN w t2 ON t2.bs = t0.ag
      AND t2.cv = 3023424
      AND t2.al = 0
      AND t0.cr = 0
      AND t2.cv = t0.cv
      LEFT JOIN w t3 ON t2.bt = t3.ag
      AND t3.cv = 3023424
      AND t3.cv = t0.cv
      LEFT JOIN w t4 ON t3.x = t4.x
      AND t4.cv = 3023424
      AND t4.al = 0
      AND t4.cv = t0.cv
      LEFT JOIN l t5 ON t4.bs = t5.ag
      AND t5.cv = 3023424
      AND t5.cv = t0.cv
      LEFT JOIN k t6 ON t6.bi = t5.bi
    WHERE
      (
        t0.cv = 3023424
        AND t0.aw = 1
        AND t0.w = '2024-02-28'
        AND t1.z = 11
        AND t0.cr = 0
        AND t0.h IS NULL
        AND t1.cj > 0
      )
  ),
  cte1 AS (
    SELECT
      t0.ag AS c0,
      t5.ag AS c1,
      1 AS ck,
      t3.al AS c3,
      t0.cv AS cv,
      t0.w AS a,
      coalesce(t5.ay, t0.ay) AS ay,
      coalesce(t5.bg, t0.bg) AS bh,
      t0.cw AS cw,
      t1.z AS z,
      coalesce(t6.bj, t0.bj) AS bj,
      t5.bm AS c11,
      coalesce(t5.cu, t0.cu) AS cu,
      t0.aa AS aa,
      coalesce(t5.co, t5.cm, t0.ac) AS ac,
      CASE
        WHEN t5.co IS NOT NULL THEN 0
        WHEN t5.cm IS NOT NULL THEN 1
        ELSE t0.ad
      END AS ad,
      coalesce(t5.cl, t0.ab) AS ab,
      CASE
        WHEN t5.co IS NOT NULL
        OR t5.cm IS NOT NULL THEN t5.cn
        ELSE t0.ae
      END AS ae,
      CASE
        WHEN t0.h IS NOT NULL THEN TRUE
        ELSE FALSE
      END AS c18
    FROM
      m t0
      JOIN f t1 ON t1.cw = t0.cw
      LEFT JOIN w t2 ON t2.bs = t0.ag
      AND t2.cv = 3023424
      AND t2.al = 0
      AND t0.cr = 0
      AND t2.cv = t0.cv
      LEFT JOIN w t3 ON t2.bt = t3.x
      AND t3.cv = 3023424
      AND t3.al <> 0
      AND t3.db IS NULL
      AND t3.da IS NULL
      AND t3.cv = t0.cv
      LEFT JOIN l t5 ON t3.bs = t5.ag
      AND t5.cv = 3023424
      AND t5.cv = t0.cv
      LEFT JOIN k t6 ON t6.bi = t5.bi
    WHERE
      (
        t0.cv = 3023424
        AND t0.aw = 1
        AND t0.w = '2024-02-28'
        AND t1.z = 11
        AND t0.cr = 0
        AND t0.h IS NULL
        AND t1.cj < 0
      )
  ),
  cte2 AS (
    SELECT
      t0.ag AS c0,
      t5.ag AS c1,
      2 AS ck,
      t2.al AS c3,
      t0.cv AS cv,
      t0.w AS a,
      coalesce(t5.ay, t0.ay) AS ay,
      coalesce(t5.bg, t0.bg) AS bh,
      t0.cw AS cw,
      t1.z AS z,
      coalesce(t6.bj, t0.bj) AS bj,
      t5.bm AS c11,
      coalesce(t5.cu, t0.cu) AS cu,
      t0.aa AS aa,
      coalesce(t5.co, t5.cm, t0.ac) AS ac,
      CASE
        WHEN t5.co IS NOT NULL THEN 0
        WHEN t5.cm IS NOT NULL THEN 1
        ELSE 2
      END AS ad,
      t5.cl AS ab,
      CASE
        WHEN t5.co IS NOT NULL
        OR t5.cm IS NOT NULL THEN t5.cn
        ELSE t0.ae
      END AS ae,
      CASE
        WHEN t0.h IS NOT NULL THEN TRUE
        ELSE FALSE
      END AS c18
    FROM
      m t0
      JOIN f t1 ON t1.cw = t0.cw
      LEFT JOIN w t2 ON t2.c = t0.ag
      AND t2.cv = 3023424
      AND t0.cr = 1
      AND t2.cv = t0.cv
      LEFT JOIN l t5 ON t5.ag = t2.bs
      AND t5.cv = 3023424
      AND t5.cv = t0.cv
      LEFT JOIN k t6 ON t6.bi = t5.bi
    WHERE
      (
        t0.cv = 3023424
        AND t0.aw = 1
        AND t0.w = '2024-02-28'
        AND t1.z = 11
        AND t0.cr = 1
        AND t0.h IS NULL
        AND t1.cj < 0
      )
  )
SELECT
  *
FROM
  cte0
UNION
SELECT
  *
FROM
  cte1
UNION
SELECT
  *
FROM
  cte2 option (
    SQL_VDBE_OPCODE_MAX = 900000,
    SQL_MOTION_ROW_MAX = 10000
  );

-- TEST: q18
WITH
  cte0 AS (
    SELECT
      coalesce(t6.bj, t0.bj) AS bj,
      t5.bm AS c1,
      coalesce(t5.cu, t0.cu) AS cu,
      CASE
        WHEN t5.co IS NOT NULL THEN 0
        WHEN t5.cm IS NOT NULL THEN 1
        ELSE t0.ad
      END AS ad,
      coalesce(t5.co, t5.cm, t0.ac) AS ac
    FROM
      m t0
      JOIN f t1 ON t1.cw = t0.cw
      LEFT JOIN w t2 ON t2.bs = t0.ag
      AND t2.al = 0
      AND t0.cr = 0
      AND t2.cv = t0.cv
      LEFT JOIN w t3 ON t2.bt = t3.ag
      AND t3.cv = t2.cv
      LEFT JOIN w t4 ON t3.x = t4.x
      AND t4.cv = t3.cv
      AND t4.al = 0
      LEFT JOIN l t5 ON t4.bs = t5.ag
      AND t5.cv = t4.cv
      LEFT JOIN k t6 ON t6.bi = t5.bi
    WHERE
      (
        t0.cv = 3023424
        AND t0.aw = 1
        AND t0.w = '2024-02-28'
        AND t1.z = 11
        AND t0.cr = 0
        AND NOT (t0.h IS NOT NULL)
        AND t1.cj > 0
      )
  ),
  cte1 AS (
    SELECT
      coalesce(t6.bj, t0.bj) AS bj,
      t5.bm AS c1,
      coalesce(t5.cu, t0.cu) AS cu,
      CASE
        WHEN t5.co IS NOT NULL THEN 0
        WHEN t5.cm IS NOT NULL THEN 1
        ELSE t0.ad
      END AS ad,
      coalesce(t5.co, t5.cm, t0.ac) AS ac
    FROM
      m t0
      JOIN f t1 ON t1.cw = t0.cw
      LEFT JOIN w t2 ON t2.bs = t0.ag
      AND t2.cv = t0.cv
      AND t2.al = 0
      AND t0.cr = 0
      LEFT JOIN w t3 ON t2.x = t3.x
      AND t3.cv = t2.cv
      AND t3.al <> 0
      AND t3.db IS NULL
      AND t3.da IS NULL
      LEFT JOIN l t5 ON t3.bs = t5.ag
      AND t5.cv = t3.cv
      LEFT JOIN k t6 ON t6.bi = t5.bi
    WHERE
      (
        t0.cv = 3023424
        AND t0.aw = 1
        AND t0.w = '2024-02-28'
        AND t1.z = 11
        AND t0.cr = 0
        AND NOT (t0.h IS NOT NULL)
        AND t1.cj < 0
      )
  ),
  cte2 AS (
    SELECT
      coalesce(t6.bj, t0.bj) AS bj,
      t5.bm AS c1,
      coalesce(t5.cu, t0.cu) AS cu,
      CASE
        WHEN t5.co IS NOT NULL THEN 0
        WHEN t5.cm IS NOT NULL THEN 1
        ELSE 2
      END AS ad,
      coalesce(t5.co, t5.cm, t0.ac) AS ac
    FROM
      m t0
      JOIN f t1 ON t1.cw = t0.cw
      LEFT JOIN w t2 ON t2.c = t0.ag
      AND t2.cv = t0.cv
      AND t0.cr = 1
      LEFT JOIN l t5 ON t5.ag = t2.bs
      AND t5.cv = t2.cv
      LEFT JOIN k t6 ON t6.bi = t5.bi
    WHERE
      (
        t0.cv = 3023424
        AND t0.aw = 1
        AND t0.w = '2024-02-28'
        AND t1.z = 11
        AND t0.cr = 1
        AND NOT (t0.h IS NOT NULL)
        AND t1.cj < 0
      )
  ),
  cte3 AS (
    SELECT
      *
    FROM
      cte0
    UNION
    SELECT
      *
    FROM
      cte1
    UNION
    SELECT
      *
    FROM
      cte2
  )
SELECT
  *
FROM
  cte3 option (
    SQL_VDBE_OPCODE_MAX = 600000,
    SQL_MOTION_ROW_MAX = 10000
  );

-- TEST: q19
WITH
  cte0 AS (
    SELECT
      aa,
      z AS z,
      CASE
        WHEN h IS NOT NULL THEN TRUE
        ELSE FALSE
      END AS c1
    FROM
      m t0
    WHERE
      (
        t0.cv = 3023424
        AND t0.w = '2024-02-28'
        AND t0.aw = 1
        AND t0.aa <> 0
      )
  )
SELECT
  SUM(
    CASE
      WHEN aa > 0 THEN aa
      ELSE 0
    END
  ) AS c2,
  SUM(
    CASE
      WHEN aa < 0 THEN aa
      ELSE 0
    END
  ) AS c3,
  NULL AS c4,
  t0.z AS c0,
  c1
FROM
  cte0 t0
GROUP BY
  z,
  c1 option (
    SQL_VDBE_OPCODE_MAX = 500000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q20
SELECT
  t0.ag,
  t0.cv,
  t0.v,
  t0.bx,
  t0.bz,
  t0.ca,
  t0.cc,
  t0.cd,
  t0.ar AS c0,
  t0.cg,
  t0.ce,
  t0.i,
  t0.cf
FROM
  u t0
  LEFT JOIN s t1 ON t0.v = t1.ag
  AND t0.cv = t1.cv
WHERE
  (
    t0.cv = 3023424
    AND t0.ar <= '2024-02-28'
    AND NOT COALESCE(t0.aq, 'false')
  )
ORDER BY
  c0 DESC
LIMIT
  2 option (
    SQL_VDBE_OPCODE_MAX = 100000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q21
WITH
  cte0 AS (
    SELECT
      CAST(TO_CHAR(w, '%Y-%m-%d') AS text) AS c0,
      aa
    FROM
      m t0
    WHERE
      (
        t0.cv = 3023424
        AND t0.w BETWEEN ('2023-01-01'::datetime) AND ('2023-05-01'::datetime)
        AND t0.aw = 1
        AND t0.aa <> 0
      )
  ),
  cte1 AS (
    SELECT
      c0,
      SUM(
        CASE
          WHEN aa > 0 THEN aa
          ELSE 0
        END
      ) AS c1,
      SUM(
        CASE
          WHEN aa < 0 THEN aa
          ELSE 0
        END
      ) AS c2,
      0 AS c3,
      ROW_NUMBER() OVER (
        ORDER BY
          c0 DESC
      ) AS c4
    FROM
      cte0
    GROUP BY
      c0
    ORDER BY
      c0 DESC
  )
SELECT
  *
FROM
  cte1
WHERE
  c4 > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 1400000,
    SQL_MOTION_ROW_MAX = 51000
  );

-- TEST: q22
WITH
  cte0 AS (
    SELECT
      CAST(TO_CHAR(w, '%Y-%m-%d') AS text) AS c0,
      aa
    FROM
      m t0
    WHERE
      (
        t0.cv = 3023424
        AND t0.w BETWEEN ('2023-01-01'::datetime) AND ('2023-05-01'::datetime)
        AND t0.aw = 1
        AND t0.aa <> 0
      )
  ),
  cte1 AS (
    SELECT
      c0,
      SUM(
        CASE
          WHEN aa > 0 THEN aa
          ELSE 0
        END
      ) AS c1,
      SUM(
        CASE
          WHEN aa < 0 THEN aa
          ELSE 0
        END
      ) AS c2,
      0 AS c3
    FROM
      cte0
    GROUP BY
      c0
  )
SELECT
  COUNT(*) AS c4
FROM
  cte1 option (
    SQL_VDBE_OPCODE_MAX = 1400000,
    SQL_MOTION_ROW_MAX = 51000
  );

-- TEST: q23
WITH
  cte0 AS (
    SELECT
      t0.*
    FROM
      u t0
      LEFT JOIN s t1 ON t0.v = t1.ag
      AND t0.cv = t1.cv
    WHERE
      (
        (
          t0.cv = 3023424
          AND NOT COALESCE(t0.aq, 'false')
          AND TO_CHAR(t0.ar, '%Y-%m-%d')::datetime > '2020-01-02'::datetime
        )
        AND t0.ar < '2023-01-01'::datetime
      )
    ORDER BY
      ar DESC
    LIMIT
      1
  )
SELECT
  *
FROM
  cte0
UNION
SELECT
  t0.*
FROM
  u t0
  LEFT JOIN s t1 ON t0.v = t1.ag
  AND t0.cv = t1.cv
WHERE
  (
    (
      t0.cv = 3023424
      AND NOT COALESCE(t0.aq, 'false')
      AND TO_CHAR(t0.ar, '%Y-%m-%d')::datetime > '2020-01-02'::datetime
    )
    AND t0.ar BETWEEN ('2023-01-01'::datetime) AND ('2023-05-01'::datetime)
  ) option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q24
WITH
  cte0 AS (
    SELECT
      t0.cu AS cu,
      t0.d AS d,
      CAST(
        TO_CHAR(COALESCE(t0.cs, t0.a), '%Y-%m-%d') AS text
      ) AS a
    FROM
      l t0
      LEFT JOIN l t1 ON t1.h = t0.ag
      AND t1.cv = t0.cv
    WHERE
      t1.ag IS NULL
      AND (
        t0.cv = 4797271
        AND t0.cw = 1415
        AND COALESCE(t0.cs, t0.a) BETWEEN ('2010-01-01') AND ('2025-10-20')
        AND t0.cu = 1333
        AND t0.h IS NULL
        AND COALESCE(t0.bb, 0) <> 1
      )
  ),
  cte1 AS (
    SELECT
      cu,
      a,
      SUM(d) AS d,
      ROW_NUMBER() OVER (
        ORDER BY
          a DESC
      ) AS c3
    FROM
      cte0
    GROUP BY
      cu,
      a
    ORDER BY
      a DESC
  )
SELECT
  *
FROM
  cte1
WHERE
  c3 > 0
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
      t0.ag AS b,
      t0.cw AS cw,
      t0.cu AS cu,
      COALESCE(t0.co, t0.cm, 0) AS ac,
      CASE
        WHEN t0.co IS NOT NULL
        OR t0.cm IS NOT NULL THEN t0.cn
        ELSE '0000'
      END AS ae,
      TO_CHAR(t0.cl, '%Y-%m-%d') AS ab,
      CASE
        WHEN t0.co IS NOT NULL THEN 0
        WHEN t0.cm IS NOT NULL THEN 1
        ELSE 2
      END AS ad,
      t0.d AS d,
      0 AS c8,
      ROW_NUMBER() OVER (
        ORDER BY
          t0.ag DESC
      ) AS c9
    FROM
      l t0
      LEFT JOIN l t1 ON t1.h = t0.ag
      AND t1.cv = 8397725
      AND t1.cv = t0.cv
    WHERE
      t1.ag IS NULL
      AND (
        t0.cv = 8397725
        AND t0.cw = 1415
        AND COALESCE(t0.cs, t0.a) = '2024-07-30'
        AND t0.cu = 1333
        AND t0.h IS NULL
        AND COALESCE(t0.bb, 0) <> 1
      )
    ORDER BY
      b DESC
  ) AS t2
WHERE
  c9 > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q26
SELECT
  COUNT(*) AS c0,
  SUM(d) AS c1
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          t0.ag AS b,
          t0.cw AS cw,
          t0.cu AS cu,
          COALESCE(t0.co, t0.cm, 0) AS ac,
          CASE
            WHEN t0.co IS NOT NULL
            OR t0.cm IS NOT NULL THEN t0.cn
            ELSE '0000'
          END AS ae,
          TO_CHAR(t0.cl, '%Y-%m-%d') AS ab,
          CASE
            WHEN t0.co IS NOT NULL THEN 0
            WHEN t0.cm IS NOT NULL THEN 1
            ELSE 2
          END AS ad,
          t0.d AS d,
          0 AS c10,
          ROW_NUMBER() OVER (
            ORDER BY
              t0.ag DESC
          ) AS c11
        FROM
          l t0
          LEFT JOIN l t1 ON t1.h = t0.ag
          AND t1.cv = 8397725
          AND t1.cv = t0.cv
        WHERE
          t1.ag IS NULL
          AND (
            t0.cv = 8397725
            AND t0.cw = 1415
            AND COALESCE(t0.cs, t0.a) = '2024-07-30'
            AND t0.cu = 1333
            AND t0.h IS NULL
            AND COALESCE(t0.bb, 0) <> 1
          )
        ORDER BY
          b DESC
      ) AS t2
  ) AS t3 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q27
SELECT
  t0.d AS c0,
  t0.cu AS c1,
  TO_CHAR(COALESCE(t0.cs, t0.a), '%Y-%m-%d') AS c2
FROM
  l t0
  LEFT JOIN l t1 ON t1.h = t0.ag
  AND t1.cv = t0.cv
WHERE
  t1.ag IS NULL
  AND (
    t0.cv = 8397725
    AND t0.cw = 1415
    AND COALESCE(t0.cs, t0.a) BETWEEN ('2010-01-01') AND ('2025-11-04')
    AND t0.h IS NULL
    AND COALESCE(t0.bb, 0) <> 1
  ) option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q28
WITH
  cte0 AS (
    SELECT
      t0.v AS v,
      CASE
        WHEN bx > 0
        AND t1.cw = 3 THEN bx
        ELSE 0
      END AS c1
    FROM
      y t0
      LEFT JOIN g t1 ON t0.cu = t1.cu
    WHERE
      (t0.cv = 3023424)
  ),
  cte1 AS (
    SELECT
      v,
      SUM(c1) AS c2
    FROM
      cte0
    GROUP BY
      v
  )
SELECT
  t0.ce AS c3,
  cte1.c2 AS c2,
  0 AS c4,
  0 AS c5
FROM
  s t0
  LEFT JOIN cte1 ON cte1.v = t0.ag
WHERE
  (t0.cv = 3023424) option (
    SQL_VDBE_OPCODE_MAX = 86000,
    SQL_MOTION_ROW_MAX = 5235
  );

-- TEST: q29
WITH
  cte0 AS (
    SELECT
      t0.ag,
      t0.au,
      t0.cv
    FROM
      (
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs >= ('2022-10-31'::datetime)
          AND cs < ('2022-11-01'::datetime)
          AND COALESCE(bb, 0) <> 1
        UNION ALL
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs IS NULL
          AND a >= ('2022-10-31'::datetime)
          AND a < ('2022-11-01'::datetime)
          AND COALESCE(bb, 0) <> 1
      ) t0
      LEFT JOIN x t1 ON t1.b = t0.ag
      AND t1.cv = t0.cv
    WHERE
      t1.b IS NULL
    ORDER BY
      t0.ag
  ),
  cte1 AS (
    SELECT
      t6.cv AS cv,
      t6.ag AS b,
      t3.ay AS ay,
      t3.bg AS bg,
      t6.cu AS cu,
      t6.cw AS cw,
      COALESCE(t6.co, t6.cm, 0) AS ac,
      CASE
        WHEN t6.co IS NOT NULL
        OR t6.cm IS NOT NULL THEN t6.cn
        ELSE '0000'
      END AS ae,
      TO_CHAR(t6.cl, '%Y-%m-%d') AS ab,
      CASE
        WHEN t6.co IS NOT NULL THEN 0
        WHEN t6.cm IS NOT NULL THEN 1
        ELSE 2
      END AS c9,
      t5.d AS d
    FROM
      cte0 t2
      JOIN y t3 INDEXED by ix_i ON t3.ag = t2.au
      AND t3.cv = t2.cv
      JOIN v t4 INDEXED by ix_f ON t4.b = t2.ag
      AND t4.cv = t3.cv
      AND t4.ai < 0
      JOIN w t5 INDEXED BY ix_g ON t5.x = t4.ag
      AND t5.cv = t4.cv
      AND t5.db IS NULL
      AND t5.da IS NULL
      JOIN l t6 INDEXED by ix_d ON t6.ag = t5.bs
      AND t6.cv = t5.cv
      AND COALESCE(t6.bb, 0) <> 1
      LEFT JOIN x t7 ON t7.b = t6.ag
      AND t7.cv = t6.cv
      LEFT JOIN f t8 ON t8.cw = t6.cw
    WHERE
      t8.z = 7
      AND (
        t3.aw = 11
        AND t3.cq = 6
      )
  ),
  cte2 AS (
    SELECT
      t9.cv AS cv,
      t9.ag AS b,
      t9.ay AS ay,
      coalesce(t3.bg, t9.bg) AS bg,
      coalesce(t3.cu, t9.cu) AS cu,
      t9.cw AS cw,
      COALESCE(t9.co, t9.cm, 0) AS ac,
      t9.cn AS ae,
      TO_CHAR(t9.cl, '%Y-%m-%d') AS ab,
      CASE
        WHEN t9.co IS NOT NULL THEN 0
        ELSE 1
      END AS c9,
      COALESCE(
        (
          t9.d * COALESCE(t8.cj, 0.0) * (
            CASE
              WHEN t9.h IS NOT NULL THEN -1
              ELSE 1
            END
          )
        ),
        0.0
      ) AS d
    FROM
      n t9
      LEFT JOIN y t3 INDEXED by ix_i ON t9.au = t3.ag
      AND t3.cv = t9.cv
      LEFT JOIN f t8 ON t9.cw = t8.cw
      LEFT JOIN n t10 INDEXED by ix_e ON t10.h = t9.ag
      AND t10.cv = t3.cv
      LEFT JOIN h t11 ON t9.cu = t11.cu
    WHERE
      t9.cv = 4797271
      AND COALESCE(t9.cs, t9.a) = ('2022-10-31'::datetime)
      AND t8.z = 7
      AND (
        coalesce(t3.aw, t9.bf) = 11
        AND coalesce(t3.cq, t11.cq) = 6
      )
  ),
  cte3 AS (
    SELECT
      *
    FROM
      cte1
    UNION ALL
    SELECT
      *
    FROM
      cte2
  ),
  cte4 AS (
    SELECT
      cte3.cv,
      cte3.b,
      cte3.ay,
      cte3.bg,
      cte3.cu,
      cte3.cw,
      cte3.ac,
      cte3.ae,
      cte3.ab,
      cte3.c9,
      cte3.d,
      t12.cx,
      ROW_NUMBER() OVER (
        ORDER BY
          b DESC
      ) AS c11
    FROM
      cte3
      LEFT JOIN ad t12 ON t12.cv = cte3.cv
      AND t12.ay = cte3.ay
  )
SELECT
  *
FROM
  cte4
WHERE
  c11 > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q30
WITH
  cte0 AS (
    SELECT
      t0.ag,
      t0.au,
      t0.cv
    FROM
      (
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs >= ('2022-10-31'::datetime)
          AND cs < ('2022-11-01'::datetime)
          AND COALESCE(bb, 0) <> 1
        UNION ALL
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs IS NULL
          AND a >= ('2022-10-31'::datetime)
          AND a < ('2022-11-01'::datetime)
          AND COALESCE(bb, 0) <> 1
      ) t0
      LEFT JOIN x t1 ON t1.b = t0.ag
      AND t1.cv = t0.cv
    WHERE
      t1.b IS NULL
    ORDER BY
      t0.ag
  ),
  cte1 AS (
    SELECT
      t5.d AS d
    FROM
      cte0 t2
      JOIN y t3 INDEXED by ix_i ON t3.ag = t2.au
      AND t3.cv = t2.cv
      JOIN v t4 INDEXED by ix_f ON t4.b = t2.ag
      AND t4.cv = t3.cv
      AND t4.ai < 0
      JOIN w t5 INDEXED BY ix_g ON t5.x = t4.ag
      AND t5.cv = t4.cv
      AND t5.db IS NULL
      AND t5.da IS NULL
      JOIN l t6 INDEXED by ix_d ON t6.ag = t5.bs
      AND t6.cv = t5.cv
      AND COALESCE(t6.bb, 0) <> 1
      LEFT JOIN x t7 ON t7.b = t6.ag
      AND t7.cv = t6.cv
      LEFT JOIN f t8 ON t8.cw = t6.cw
    WHERE
      t8.z = 7
      AND (
        t3.aw = 11
        AND t3.cq = 6
      )
  ),
  cte2 AS (
    SELECT
      COALESCE(
        (
          t9.d * COALESCE(t8.cj, 0.0) * CASE
            WHEN t9.h IS NOT NULL THEN -1
            ELSE 1
          END
        ),
        0.0
      ) AS d
    FROM
      n t9
      LEFT JOIN y t3 INDEXED by ix_i ON t9.au = t3.ag
      AND t3.cv = t9.cv
      LEFT JOIN f t8 ON t9.cw = t8.cw
      LEFT JOIN n t10 INDEXED by ix_e ON t10.h = t9.ag
      AND t10.cv = t3.cv
      LEFT JOIN h t11 ON t9.cu = t11.cu
    WHERE
      t9.cv = 4797271
      AND COALESCE(t9.cs, t9.a) = ('2022-10-31'::datetime)
      AND t8.z = 7
      AND (
        coalesce(t3.aw, t9.bf) = 11
        AND coalesce(t3.cq, t11.cq) = 6
      )
  )
SELECT
  COUNT(*) AS c1,
  SUM(d) AS c2
FROM
  (
    SELECT
      *
    FROM
      cte1
    UNION ALL
    SELECT
      *
    FROM
      cte2
  ) t12 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q31
WITH
  cte0 AS (
    SELECT
      t0.ag,
      t0.au,
      t0.cv
    FROM
      (
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs >= ('2024-04-02')
          AND cs < ('2024-04-03')
          AND COALESCE(bb, 0) <> 1
        UNION ALL
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs IS NULL
          AND a >= ('2024-04-02')
          AND a < ('2024-04-03')
          AND COALESCE(bb, 0) <> 1
      ) t0
      LEFT JOIN x t1 ON t1.b = t0.ag
      AND t1.cv = t0.cv
    WHERE
      t1.b IS NULL
  ),
  cte1 AS (
    SELECT DISTINCT
      t6.cv AS cv,
      t3.ay AS ay,
      t3.bg AS bg,
      t3.ah AS ah,
      t6.cu AS cu,
      t8.cx AS cx
    FROM
      cte0 t2
      JOIN y t3 INDEXED by ix_i ON t3.ag = t2.au
      AND t3.cv = t2.cv
      JOIN v t4 INDEXED by ix_f ON t4.b = t2.ag
      AND t4.cv = t3.cv
      AND t4.ai < 0
      JOIN w t5 INDEXED by ix_g ON t5.x = t4.ag
      AND t5.db IS NULL
      AND t5.da IS NULL
      AND t5.cv = t4.cv
      JOIN l t6 INDEXED by ix_d ON t6.ag = t5.bs
      AND t6.cv = t5.cv
      AND COALESCE(t6.bb, 0) <> 1
      LEFT JOIN f t7 ON t7.cw = t6.cw
      LEFT JOIN ad t8 ON t8.cv = t3.cv
      AND t8.ay = t3.ay
    WHERE
      t7.z = 7
      AND (
        t3.aw = 11
        AND t3.cq = 6
      )
  ),
  cte2 AS (
    SELECT DISTINCT
      t9.cv AS cv,
      t9.ay AS ay,
      coalesce(t3.bg, t9.bg) AS bg,
      coalesce(t3.ah, t9.ah) AS ah,
      coalesce(t3.cu, t9.cu) AS cu,
      t8.cx AS cx
    FROM
      n t9
      LEFT JOIN y t3 INDEXED by ix_i ON t9.au = t3.ag
      AND t9.cv = t3.cv
      LEFT JOIN f t7 ON t9.cw = t7.cw
      LEFT JOIN n t10 INDEXED by ix_e ON t10.h = t9.ag
      AND t10.cv = t9.cv
      LEFT JOIN h t11 ON t9.cu = t11.cu
      LEFT JOIN ad t8 ON t8.cv = t3.cv
      AND t8.ay = t3.ay
    WHERE
      t9.cv = 4797271
      AND COALESCE(t9.cs, t9.a) = ('2024-04-02')
      AND t7.z = 7
      AND (
        coalesce(t3.aw, t9.bf) = 11
        AND coalesce(t3.cq, t11.cq) = 6
      )
  )
SELECT
  *
FROM
  cte1
UNION ALL
SELECT
  *
FROM
  cte2 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q32
SELECT
  z AS c0,
  SUM(f) AS c1,
  SUM(e) AS c2,
  0 AS c3
FROM
  a
WHERE
  (
    cv = 3023424
    AND aw = 11
    AND cq = 6
    AND a = '2022-10-12'
  )
GROUP BY
  z
UNION ALL
SELECT
  t3.z AS c0,
  0 AS c1,
  0 AS c2,
  COALESCE(
    SUM(
      t0.d * COALESCE(t3.cj, 0) * CASE
        WHEN t0.h IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS c3
FROM
  n t0
  LEFT JOIN n t1 ON t1.h = t0.ag
  AND t1.cv = t0.cv
  LEFT JOIN y t2 ON t0.au = t2.ag
  AND t0.cv = t2.cv
  LEFT JOIN f t3 ON t0.cw = t3.cw
  LEFT JOIN h t4 ON t0.cu = t4.cu
WHERE
  (
    t0.cv = 3023424
    AND coalesce(t2.aw, t0.bf) = 11
    AND coalesce(t2.cq, t4.cq) = 6
    AND COALESCE(t0.cs, t0.a) = '2022-10-12'
    AND t1.ag IS NULL
    AND (
      t0.h IS NULL
      OR t0.h = -1
    )
  )
GROUP BY
  t3.z option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q33
SELECT DISTINCT
  t0.ay,
  t0.bg,
  t0.ah,
  t1.cx
FROM
  a t0
  LEFT JOIN ad t1 ON t0.cv = t1.cv
  AND t0.ay = t1.ay
WHERE
  (
    t0.cv = 3023424
    AND t0.aw = 11
    AND t0.a = '2022-10-12'
    AND t0.cq = 6
  )
UNION
SELECT DISTINCT
  t2.ay AS c0,
  coalesce(t4.bg, t2.bg) AS c1,
  coalesce(t4.ah, t2.ah) AS c2,
  t1.cx AS c3
FROM
  n t2
  LEFT JOIN n t3 ON t3.h = t2.ag
  AND t3.cv = t2.cv
  LEFT JOIN y t4 ON t2.au = t4.ag
  AND t2.cv = t4.cv
  LEFT JOIN f t5 ON t2.cw = t5.cw
  LEFT JOIN ad t1 ON t2.cv = t1.cv
  AND t2.ay = t1.ay
  LEFT JOIN h t6 ON t2.cu = t6.cu
WHERE
  (
    t2.cv = 3023424
    AND coalesce(t4.aw, t2.bf) = 11
    AND COALESCE(t2.cs, t2.a) = '2022-10-12'
    AND coalesce(t4.cq, t6.cq) = 6
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q34
WITH
  cte0 AS (
    SELECT
      coalesce(t2.cq, t4.cq) AS cq,
      TO_CHAR(COALESCE(t0.cs, t0.a), '%Y-%m-%d') AS a,
      t0.d,
      COALESCE(t3.cj, 0) AS cj,
      t0.h
    FROM
      n t0
      LEFT JOIN n t1 ON t1.h = t0.ag
      AND t1.cv = t0.cv
      LEFT JOIN y t2 ON t0.au = t2.ag
      AND t0.cv = t2.cv
      LEFT JOIN f t3 ON t0.cw = t3.cw
      LEFT JOIN h t4 ON t0.cu = t4.cu
    WHERE
      (
        t0.cv = 3023424
        AND coalesce(t2.aw, t0.bf) = 11
        AND t1.ag IS NULL
        AND (
          t0.h IS NULL
          OR t0.h = -1
        )
        AND CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%Y') AS integer) IN (2024, 2023, 2022)
      )
  )
SELECT
  cq AS c0,
  TO_CHAR(a, '%Y-%m-%d') AS c1,
  SUM(f) AS c3,
  SUM(e) AS c4,
  0.0 AS c5
FROM
  a
WHERE
  (
    cv = 3023424
    AND aw = 11
    AND bm IN (2024, 2023, 2022)
  )
GROUP BY
  a,
  cq
UNION ALL
SELECT
  cq,
  a,
  0 AS c3,
  0 AS c4,
  COALESCE(
    SUM(
      d * cj * (
        CASE
          WHEN h IS NOT NULL THEN -1
          ELSE 1
        END
      )
    ),
    0.0
  ) AS c5
FROM
  cte0
GROUP BY
  cq,
  a option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q35
WITH
  cte0 AS (
    SELECT
      t0.z AS z,
      t0.cw AS cw,
      t0.ay AS ay,
      t0.bg AS bg,
      t0.cu AS cu,
      aa AS d,
      t0.ac AS ac,
      t0.ad AS ad,
      t0.ae AS ae,
      t0.ab AS ab,
      0 AS c10,
      h AS h,
      t1.cx AS cx,
      ROW_NUMBER() OVER () AS c13
    FROM
      m t0
      LEFT JOIN ad t1 ON t0.cv = t1.cv
      AND t0.ay = t1.ay
    WHERE
      (
        t0.cv = 3023424
        AND COALESCE(t0.bb, 0) <> 1
        AND t0.w = '2024-02-28'
        AND t0.cu = 3465
        AND t0.bj IS NULL
        AND t0.z = 11
      )
  )
SELECT
  *
FROM
  cte0
WHERE
  c13 > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q36
SELECT
  SUM(aa) AS c0,
  COUNT(*) AS c1
FROM
  m t0
WHERE
  (
    t0.cv = 3023424
    AND COALESCE(t0.bb, 0) <> 1
    AND t0.w = '2024-02-28'
    AND t0.cu = 3465
    AND t0.bj IS NULL
    AND t0.z = 11
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q37
SELECT DISTINCT
  t0.bm AS c0,
  t0.bg AS c1,
  t0.cu AS c2,
  t0.ah AS c3,
  t0.ay AS c4,
  t2.cx AS c5
FROM
  m t0
  LEFT JOIN g t1 ON t0.cu = t1.cu
  LEFT JOIN ad t2 ON t0.cv = t2.cv
  AND t0.ay = t2.ay
WHERE
  (
    t0.cv = 3023424
    AND COALESCE(t0.bb, 0) <> 1
    AND t0.w = '2024-02-28'
    AND t0.cu = 3465
    AND t0.z = 11
  ) option (
    SQL_VDBE_OPCODE_MAX = 700000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q38
WITH
  cte0 AS (
    SELECT
      TO_CHAR(t0.w, '%Y-%m-%d') AS a,
      t0.z AS z,
      t0.cu AS cu,
      t0.bj AS bj,
      SUM(
        CASE
          WHEN aa > 0
          AND NOT ao THEN aa
          ELSE 0
        END
      ) AS c4,
      SUM(
        CASE
          WHEN aa < 0
          AND NOT ao THEN aa
          ELSE 0
        END
      ) AS f,
      SUM(
        CASE
          WHEN aa > 0
          AND ao THEN aa
          ELSE 0
        END
      ) AS g,
      SUM(
        CASE
          WHEN aw = 2 THEN aa
          ELSE 0
        END
      ) AS c7,
      0 AS c8,
      ROW_NUMBER() OVER (
        ORDER BY
          w DESC,
          (t0.cu IS NULL) ASC,
          t0.cu ASC,
          (t0.bj IS NULL) ASC,
          t0.bj ASC,
          (t0.z IS NULL) ASC,
          t0.z ASC
      ) AS c9
    FROM
      m t0
      LEFT JOIN g t1 ON t0.cu = t1.cu
    WHERE
      (
        t0.cv = 3023424
        AND COALESCE(t0.bb, 0) <> 1
        AND t0.w BETWEEN ('2023-01-01'::datetime) AND ('2023-05-01'::datetime)
        AND t0.aw <> 2
      )
    GROUP BY
      t0.w,
      t0.bj,
      t0.cu,
      t0.z
  )
SELECT
  *
FROM
  cte0
WHERE
  c9 > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 3000000,
    SQL_MOTION_ROW_MAX = 60000
  );

-- TEST: q39
WITH
  cte0 AS (
    SELECT
      TO_CHAR(w, '%Y-%m-%d') AS a,
      t0.bj AS bj,
      t0.cu AS cu,
      t0.z AS z
    FROM
      m t0
      LEFT JOIN g t1 ON t0.cu = t1.cu
    WHERE
      (
        t0.cv = 3023424
        AND COALESCE(t0.bb, 0) <> 1
        AND t0.w BETWEEN ('2023-01-01'::datetime) AND ('2023-05-01'::datetime)
        AND t0.aw <> 2
      )
  )
SELECT
  COUNT(*) AS c4
FROM
  (
    SELECT
      COUNT(*) AS c4,
      a,
      z
    FROM
      cte0
    GROUP BY
      a,
      bj,
      cu,
      z
  ) AS t2 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q40
SELECT DISTINCT
  t0.bm AS c0,
  t0.bg AS c1,
  t0.cu AS c2,
  t0.ah AS c3,
  t0.ay AS c4,
  t2.cx AS c5
FROM
  m t0
  LEFT JOIN g t1 ON t0.cu = t1.cu
  LEFT JOIN ad t2 ON t0.cv = t2.cv
  AND t0.ay = t2.ay
WHERE
  (
    t0.cv = 3023424
    AND COALESCE(t0.bb, 0) <> 1
    AND t0.w BETWEEN ('2010-01-01') AND ('2026-01-01')
    AND t0.aw <> 2
  ) option (
    SQL_VDBE_OPCODE_MAX = 27000000,
    SQL_MOTION_ROW_MAX = 18000
  );

-- TEST: q41
WITH
  cte0 AS (
    SELECT
      t0.*
    FROM
      u t0
      LEFT JOIN s t1 ON t0.v = t1.ag
      AND t0.cv = t1.cv
    WHERE
      (
        (
          t0.cv = 3023424
          AND NOT COALESCE(t0.aq, 'false')
          AND TO_CHAR(t0.ar, '%Y-%m-%d')::datetime > '2020-01-02'
        )
        AND t0.ar < '2010-01-01'
      )
    ORDER BY
      ar DESC
    LIMIT
      1
  )
SELECT
  *
FROM
  cte0
UNION
SELECT
  t0.*
FROM
  u t0
  LEFT JOIN s t1 ON t0.v = t1.ag
  AND t0.cv = t1.cv
WHERE
  (
    (
      t0.cv = 3023424
      AND NOT COALESCE(t0.aq, 'false')
      AND TO_CHAR(t0.ar, '%Y-%m-%d')::datetime > '2020-01-02'
    )
    AND t0.ar BETWEEN ('2010-01-01'::datetime) AND ('2026-01-01'::datetime)
  ) option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q42
SELECT
  COALESCE(
    SUM(
      CASE
        WHEN aa IS NULL THEN 0.0
        ELSE aa
      END
    ),
    0.0
  ) AS c0
FROM
  m t0
  LEFT JOIN g t1 ON t0.cu = t1.cu
WHERE
  (
    t0.cv = 3023424
    AND COALESCE(t0.bb, 0) <> 1
    AND t0.w BETWEEN ('2010-01-01'::datetime) AND ('2026-01-01'::datetime)
  )
  AND t0.aw = 2
  AND COALESCE(t0.bb, 0) <> 1 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q43
SELECT
  COALESCE(
    SUM(
      CASE
        WHEN aa IS NULL THEN 0.0
        ELSE aa
      END
    ),
    0.0
  ) AS c0
FROM
  m t0
  LEFT JOIN g t1 ON t0.cu = t1.cu
WHERE
  (
    t0.cv = 3023424
    AND COALESCE(t0.bb, 0) <> 1
    AND t0.w BETWEEN ('2010-01-01'::datetime) AND ('2026-01-01'::datetime)
    AND t0.aw <> 2
  )
  AND t0.aw = 2
  AND COALESCE(t0.bb, 0) <> 1 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q44
SELECT DISTINCT
  t0.bm AS c0,
  t0.bg AS c1,
  t0.ah AS c2,
  t0.ay AS c3,
  t1.cx AS c4
FROM
  c t0
  LEFT JOIN ad t1 ON t0.cv = t1.cv
  AND t0.ay = t1.ay
WHERE
  (t0.cv = 3023424) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q45
SELECT DISTINCT
  CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%Y') AS integer) AS c0,
  t0.bg AS c1,
  t0.ah AS c2,
  t0.ay AS c3,
  t2.cx AS c4
FROM
  n t0
  LEFT JOIN n t1 ON t1.h = t0.ag
  AND t1.cv = t0.cv
  LEFT JOIN ad t2 ON t0.cv = t2.cv
  AND t0.ay = t2.ay
WHERE
  (
    t0.cv = 3023424
    AND t1.ag IS NULL
    AND (
      t0.h IS NULL
      OR t0.h = -1
    )
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q46
SELECT
  t1.cq AS c0,
  CASE
    WHEN MIN(t2.u) IS NOT NULL THEN TO_CHAR(MIN(t2.u), '%Y-%m-%d')
    ELSE NULL
  END AS c1,
  SUM(t0.az) AS c2
FROM
  v t0
  JOIN y t1 ON t0.au = t1.ag
  AND t0.cv = t1.cv
  JOIN x t2 ON t0.b = t2.b
  AND t0.cv = t2.cv
  LEFT JOIN i t3 ON t1.cu = t3.br
WHERE
  (
    t0.cv = 3023424
    AND aw IN (0, 3, 42, 45, 11)
    AND NOT t3.ap
    AND az > 0
  )
GROUP BY
  t1.cq option (
    SQL_VDBE_OPCODE_MAX = 798000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q47
SELECT
  t0.aw AS c0,
  CASE
    WHEN t1.cw = 3 THEN TRUE
    ELSE FALSE
  END AS c1,
  t0.ay AS c2,
  t0.cu AS c3,
  t0.bg AS c4,
  t0.ah AS c5,
  CASE
    WHEN t0.aw = 0
    AND (
      t1.cw IS NULL
      OR t1.cw <> 3
    ) THEN CASE
      WHEN bx < 0 THEN bx
      ELSE 0.0
    END
    ELSE 0.0
  END AS c6,
  CASE
    WHEN t0.aw = 3 THEN CASE
      WHEN ca < 0 THEN ca
      ELSE 0.0
    END
    ELSE 0.0
  END AS c7,
  CASE
    WHEN t0.aw = 0
    AND (
      t1.cw IS NOT NULL
      AND t1.cw = 3
    ) THEN CASE
      WHEN bx < 0 THEN bx
      ELSE 0.0
    END
    ELSE 0.0
  END AS c8,
  CASE
    WHEN t0.aw = 11 THEN CASE
      WHEN cg < 0 THEN cg
      ELSE 0.0
    END
    ELSE 0.0
  END AS c9,
  CASE
    WHEN t0.aw IN (42, 45) THEN (
      CASE
        WHEN cd < 0 THEN cd
        ELSE 0.0
      END
    ) + (
      CASE
        WHEN cc < 0 THEN cc
        ELSE 0.0
      END
    )
    ELSE 0.0
  END AS c10,
  CASE
    WHEN t0.aw = 2 THEN CASE
      WHEN bz < 0 THEN bz
      ELSE 0.0
    END
    ELSE 0.0
  END AS c11,
  CASE
    WHEN t1.cw = 3
    AND bx > 0 THEN bx
    ELSE 0.0
  END AS c12,
  CASE
    WHEN t0.aw = 1
    AND bx > 0 THEN bx
    ELSE 0.0
  END AS c13,
  NULL AS c14
FROM
  y t0
  LEFT JOIN g t1 ON t0.cu = t1.cu
WHERE
  (cv = 3023424) option (
    SQL_VDBE_OPCODE_MAX = 400000,
    SQL_MOTION_ROW_MAX = 5235
  );

-- TEST: q48
WITH
  cte0 AS (
    SELECT
      t0.ag,
      coalesce(t1.aw, t0.bf) AS aw,
      t0.ay,
      coalesce(t1.cu, t0.cu) AS cu,
      t0.bg,
      t0.ah,
      t0.cs,
      t0.a,
      t0.h,
      t0.cv,
      COALESCE(
        (
          t0.d * COALESCE(t3.cj, 0.0) * CASE
            WHEN t0.h IS NOT NULL THEN -1
            ELSE 1
          END
        ),
        0.0
      ) AS aa,
      CASE
        WHEN (
          t2.cw IS NOT NULL
          AND t2.cw = 3
        ) THEN TRUE
        ELSE FALSE
      END AS ap
    FROM
      n t0
      LEFT JOIN y t1 ON t0.au = t1.ag
      AND t0.cv = t1.cv
      LEFT JOIN g t2 ON coalesce(t1.cu, t0.cu) = t2.cu
      LEFT JOIN f t3 ON t0.cw = t3.cw
      LEFT JOIN n t4 ON t4.h = t0.ag
      AND t4.cv = t0.cv
    WHERE
      (
        t0.cv = 3023424
        AND t4.ag IS NULL
        AND (
          t0.h IS NULL
          OR t0.h = -1
        )
      )
  )
SELECT
  t0.aw AS c0,
  t0.ap AS c3,
  t0.ay AS c4,
  t0.cu AS c1,
  t0.bg AS c5,
  t0.ah AS c6,
  CASE
    WHEN t0.aw = 0
    AND NOT ap THEN t0.aa
    ELSE 0.0
  END AS c7,
  CASE
    WHEN t0.aw = 3 THEN t0.aa
    ELSE 0.0
  END AS c8,
  CASE
    WHEN t0.aw = 0
    AND ap THEN t0.aa
    ELSE 0.0
  END AS c9,
  CASE
    WHEN t0.aw = 11 THEN t0.aa
    ELSE 0.0
  END AS c10,
  CASE
    WHEN t0.aw IN (42, 45) THEN t0.aa
    ELSE 0.0
  END AS c11,
  CASE
    WHEN t0.aw = 2 THEN t0.aa
    ELSE 0.0
  END AS c12,
  0 AS c13,
  0 AS c14,
  TO_CHAR(COALESCE(t0.cs, t0.a), '%Y-%m-%d') AS c15
FROM
  cte0 t0 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q49
WITH
  cte0 AS (
    SELECT
      t0.ag,
      t0.au,
      t0.cv
    FROM
      (
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs >= ('2023-02-17')
          AND cs < ('2023-02-18')
          AND COALESCE(bb, 0) <> 1
        UNION ALL
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs IS NULL
          AND a >= ('2023-02-17')
          AND a < ('2023-02-18')
          AND COALESCE(bb, 0) <> 1
      ) t0
      LEFT JOIN x t1 ON t1.b = t0.ag
      AND t1.cv = t0.cv
    WHERE
      t1.b IS NULL
    ORDER BY
      t0.ag
  ),
  cte1 AS (
    SELECT
      t6.cv AS cv,
      t6.ag AS b,
      t3.ay AS ay,
      t3.bg AS bg,
      t6.cu AS cu,
      t6.cw AS cw,
      COALESCE(t6.co, t6.cm, 0) AS ac,
      CASE
        WHEN t6.co IS NOT NULL
        OR t6.cm IS NOT NULL THEN t6.cn
        ELSE '0000'
      END AS ae,
      TO_CHAR(t6.cl, '%Y-%m-%d') AS ab,
      CASE
        WHEN t6.co IS NOT NULL THEN 0
        WHEN t6.cm IS NOT NULL THEN 1
        ELSE 2
      END AS c9,
      t5.d AS d
    FROM
      cte0 t2
      JOIN y t3 INDEXED by ix_i ON t3.ag = t2.au
      AND t3.cv = t2.cv
      JOIN v t4 INDEXED by ix_f ON t4.b = t2.ag
      AND t4.cv = t3.cv
      AND t4.ai < 0
      JOIN w t5 INDEXED BY ix_g ON t5.x = t4.ag
      AND t5.cv = t4.cv
      AND t5.db IS NULL
      AND t5.da IS NULL
      JOIN l t6 INDEXED by ix_d ON t6.ag = t5.bs
      AND t6.cv = t5.cv
      AND COALESCE(t6.bb, 0) <> 1
      LEFT JOIN x t7 ON t7.b = t6.ag
      AND t7.cv = t6.cv
      LEFT JOIN f t8 ON t8.cw = t6.cw
    WHERE
      t8.z = 4
      AND (
        t3.aw = 3
        AND t3.cq = 4
      )
  ),
  cte2 AS (
    SELECT
      t9.cv AS cv,
      t9.ag AS b,
      t9.ay AS ay,
      coalesce(t3.bg, t9.bg) AS bg,
      coalesce(t3.cu, t9.cu) AS cu,
      t9.cw AS cw,
      COALESCE(t9.co, t9.cm, 0) AS ac,
      t9.cn AS ae,
      TO_CHAR(t9.cl, '%Y-%m-%d') AS ab,
      CASE
        WHEN t9.co IS NOT NULL THEN 0
        ELSE 1
      END AS c9,
      COALESCE(
        (
          t9.d * COALESCE(t8.cj, 0.0) * (
            CASE
              WHEN t9.h IS NOT NULL THEN -1
              ELSE 1
            END
          )
        ),
        0.0
      ) AS d
    FROM
      n t9
      LEFT JOIN y t3 INDEXED by ix_i ON t9.au = t3.ag
      AND t3.cv = t9.cv
      LEFT JOIN f t8 ON t9.cw = t8.cw
      LEFT JOIN n t10 INDEXED by ix_e ON t10.h = t9.ag
      AND t10.cv = t3.cv
      LEFT JOIN h t11 ON t9.cu = t11.cu
    WHERE
      t9.cv = 4797271
      AND COALESCE(t9.cs, t9.a) = ('2023-02-17')
      AND t8.z = 4
      AND (
        coalesce(t3.aw, t9.bf) = 3
        AND coalesce(t3.cq, t11.cq) = 4
      )
  ),
  cte3 AS (
    SELECT
      *
    FROM
      cte1
    UNION ALL
    SELECT
      *
    FROM
      cte2
  ),
  cte4 AS (
    SELECT
      cte3.cv,
      cte3.b,
      cte3.ay,
      cte3.bg,
      cte3.cu,
      cte3.cw,
      cte3.ac,
      cte3.ae,
      cte3.ab,
      cte3.c9,
      cte3.d,
      t12.cx,
      ROW_NUMBER() OVER (
        ORDER BY
          b DESC
      ) AS c11
    FROM
      cte3
      LEFT JOIN ad t12 ON t12.cv = cte3.cv
      AND t12.ay = cte3.ay
  )
SELECT
  *
FROM
  cte4
WHERE
  c11 > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 2200000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q50
WITH
  cte0 AS (
    SELECT
      t0.ag,
      t0.au,
      t0.cv
    FROM
      l t0
      LEFT JOIN x t1 ON t1.b = t0.ag
      AND t1.cv = t0.cv
    WHERE
      t0.cv = 4797271
      AND COALESCE(t0.cs, t0.a) = ('2023-02-13')
      AND COALESCE(t0.bb, 0) <> 1
      AND t1.b IS NULL
    ORDER BY
      t0.ag
  ),
  cte1 AS (
    SELECT
      t5.d AS d
    FROM
      cte0 t2
      JOIN y t3 ON t3.ag = t2.au
      AND t3.cv = t2.cv
      JOIN v t4 ON t4.b = t2.ag
      AND t4.cv = t3.cv
      AND t4.ai < 0
      JOIN w t5 INDEXED BY ix_g ON t5.x = t4.ag
      AND t5.cv = t4.cv
      AND t5.db IS NULL
      AND t5.da IS NULL
      JOIN l t6 ON t6.ag = t5.bs
      AND t6.cv = t5.cv
      AND COALESCE(t6.bb, 0) <> 1
      LEFT JOIN x t7 ON t7.b = t6.ag
      AND t7.cv = t6.cv
      LEFT JOIN f t8 ON t8.cw = t6.cw
    WHERE
      t8.z = 4
      AND (
        t3.aw = 3
        AND t3.cq = 4
      )
  ),
  cte2 AS (
    SELECT
      COALESCE(
        (
          t9.d * COALESCE(t8.cj, 0.0) * CASE
            WHEN t9.h IS NOT NULL THEN -1
            ELSE 1
          END
        ),
        0.0
      ) AS d
    FROM
      n t9
      LEFT JOIN y t3 ON t9.au = t3.ag
      AND t3.cv = t9.cv
      LEFT JOIN f t8 ON t9.cw = t8.cw
      LEFT JOIN n t10 ON t10.h = t9.ag
      AND t10.cv = t3.cv
      LEFT JOIN h t11 ON t9.cu = t11.cu
    WHERE
      t9.cv = 4797271
      AND COALESCE(t9.cs, t9.a) = ('2024-12-28')
      AND t8.z = 4
      AND (
        coalesce(t3.aw, t9.bf) = 3
        AND coalesce(t3.cq, t11.cq) = 4
      )
  )
SELECT
  COUNT(*) AS c1,
  SUM(d) AS c2
FROM
  (
    SELECT
      *
    FROM
      cte1
    UNION ALL
    SELECT
      *
    FROM
      cte2
  ) t12 option (
    SQL_VDBE_OPCODE_MAX = 80000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q51
WITH
  cte0 AS (
    SELECT
      t0.ag,
      t0.au,
      t0.cv
    FROM
      (
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs >= ('2023-02-17')
          AND cs < ('2023-02-18')
          AND COALESCE(bb, 0) <> 1
        UNION ALL
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs IS NULL
          AND a >= ('2023-02-17')
          AND a < ('2023-02-18')
          AND COALESCE(bb, 0) <> 1
      ) t0
      LEFT JOIN x t1 ON t1.b = t0.ag
      AND t1.cv = t0.cv
    WHERE
      t1.b IS NULL
  ),
  cte1 AS (
    SELECT DISTINCT
      t6.cv AS cv,
      t3.ay AS ay,
      t3.bg AS bg,
      t3.ah AS ah,
      t6.cu AS cu,
      t8.cx AS cx
    FROM
      cte0 t2
      JOIN y t3 INDEXED by ix_i ON t3.ag = t2.au
      AND t3.cv = t2.cv
      JOIN v t4 INDEXED by ix_f ON t4.b = t2.ag
      AND t4.cv = t3.cv
      AND t4.ai < 0
      JOIN w t5 INDEXED by ix_g ON t5.x = t4.ag
      AND t5.db IS NULL
      AND t5.da IS NULL
      AND t5.cv = t4.cv
      JOIN l t6 INDEXED by ix_d ON t6.ag = t5.bs
      AND t6.cv = t5.cv
      AND COALESCE(t6.bb, 0) <> 1
      LEFT JOIN f t7 ON t7.cw = t6.cw
      LEFT JOIN ad t8 ON t8.cv = t3.cv
      AND t8.ay = t3.ay
    WHERE
      t7.z = 4
      AND (
        t3.aw = 3
        AND t3.cq = 4
      )
  ),
  cte2 AS (
    SELECT DISTINCT
      t9.cv AS cv,
      t9.ay AS ay,
      coalesce(t3.bg, t9.bg) AS bg,
      coalesce(t3.ah, t9.ah) AS ah,
      coalesce(t3.cu, t9.cu) AS cu,
      t8.cx AS cx
    FROM
      n t9
      LEFT JOIN y t3 INDEXED by ix_i ON t9.au = t3.ag
      AND t9.cv = t3.cv
      LEFT JOIN f t7 ON t9.cw = t7.cw
      LEFT JOIN n t10 INDEXED by ix_e ON t10.h = t9.ag
      AND t10.cv = t9.cv
      LEFT JOIN h t11 ON t9.cu = t11.cu
      LEFT JOIN ad t8 ON t8.cv = t3.cv
      AND t8.ay = t3.ay
    WHERE
      t9.cv = 4797271
      AND COALESCE(t9.cs, t9.a) = ('2023-02-17')
      AND t7.z = 4
      AND (
        coalesce(t3.aw, t9.bf) = 3
        AND coalesce(t3.cq, t11.cq) = 4
      )
  )
SELECT
  *
FROM
  cte1
UNION ALL
SELECT
  *
FROM
  cte2 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q52
SELECT
  z AS c0,
  SUM(f) AS c1,
  SUM(e) AS c2,
  0 AS c3
FROM
  a
WHERE
  (
    cv = 4797271
    AND aw = 3
    AND cq = 4
    AND a = '2023-02-17'
  )
GROUP BY
  z
UNION ALL
SELECT
  t3.z AS c0,
  0 AS c1,
  0 AS c2,
  COALESCE(
    SUM(
      t0.d * COALESCE(t3.cj, 0) * CASE
        WHEN t0.h IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS c3
FROM
  n t0
  LEFT JOIN n t1 ON t1.h = t0.ag
  AND t1.cv = t0.cv
  LEFT JOIN y t2 ON t0.au = t2.ag
  AND t0.cv = t2.cv
  LEFT JOIN f t3 ON t0.cw = t3.cw
  LEFT JOIN h t4 ON t0.cu = t4.cu
WHERE
  (
    t0.cv = 4797271
    AND coalesce(t2.aw, t0.bf) = 3
    AND coalesce(t2.cq, t4.cq) = 4
    AND COALESCE(t0.cs, t0.a) = '2023-02-17'
    AND t1.ag IS NULL
    AND (
      t0.h IS NULL
      OR t0.h = -1
    )
  )
GROUP BY
  t3.z option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q53
SELECT DISTINCT
  t0.ay,
  t0.bg,
  t0.ah,
  t1.cx
FROM
  a t0
  LEFT JOIN ad t1 ON t0.cv = t1.cv
  AND t0.ay = t1.ay
WHERE
  (
    t0.cv = 4797271
    AND t0.aw = 3
    AND t0.a = '2023-02-17'
    AND t0.cq = 4
  )
UNION
SELECT DISTINCT
  t2.ay AS c0,
  coalesce(t4.bg, t2.bg) AS c1,
  coalesce(t4.ah, t2.ah) AS c2,
  t1.cx AS c3
FROM
  n t2
  LEFT JOIN n t3 ON t3.h = t2.ag
  AND t3.cv = t2.cv
  LEFT JOIN y t4 ON t2.au = t4.ag
  AND t2.cv = t4.cv
  LEFT JOIN f t5 ON t2.cw = t5.cw
  LEFT JOIN ad t1 ON t2.cv = t1.cv
  AND t2.ay = t1.ay
  LEFT JOIN h t6 ON t2.cu = t6.cu
WHERE
  (
    t2.cv = 4797271
    AND coalesce(t4.aw, t2.bf) = 3
    AND COALESCE(t2.cs, t2.a) = '2023-02-17'
    AND coalesce(t4.cq, t6.cq) = 4
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q54
WITH
  cte0 AS (
    SELECT
      coalesce(t2.cq, t4.cq) AS cq,
      TO_CHAR(COALESCE(t0.cs, t0.a), '%Y-%m-%d') AS a,
      t0.d,
      COALESCE(t3.cj, 0) AS cj,
      t0.h
    FROM
      n t0
      LEFT JOIN n t1 ON t1.h = t0.ag
      AND t1.cv = t0.cv
      LEFT JOIN y t2 ON t0.au = t2.ag
      AND t0.cv = t2.cv
      LEFT JOIN f t3 ON t0.cw = t3.cw
      LEFT JOIN h t4 ON t0.cu = t4.cu
    WHERE
      (
        t0.cv = 4797271
        AND coalesce(t2.aw, t0.bf) = 3
        AND t1.ag IS NULL
        AND (
          t0.h IS NULL
          OR t0.h = -1
        )
        AND CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%Y') AS integer) IN (2025, 2024, 2023, 2022, 2021)
      )
  )
SELECT
  cq AS c0,
  TO_CHAR(a, '%Y-%m-%d') AS c1,
  SUM(f) AS c3,
  SUM(e) AS c4,
  0.0 AS c5
FROM
  a
WHERE
  (
    cv = 4797271
    AND aw = 3
    AND bm IN (2025, 2024, 2023, 2022, 2021)
  )
GROUP BY
  a,
  cq
UNION ALL
SELECT
  cq,
  a,
  0 AS c3,
  0 AS c4,
  COALESCE(
    SUM(
      d * cj * (
        CASE
          WHEN h IS NOT NULL THEN -1
          ELSE 1
        END
      )
    ),
    0.0
  ) AS c5
FROM
  cte0
GROUP BY
  cq,
  a option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q55
WITH
  cte0 AS (
    SELECT
      t0.ag,
      t0.au,
      t0.cv
    FROM
      (
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 3023424
          AND cs >= ('2024-08-14')
          AND cs < ('2024-08-15')
          AND COALESCE(bb, 0) <> 1
        UNION ALL
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 3023424
          AND cs IS NULL
          AND a >= ('2024-08-14')
          AND a < ('2024-08-15')
          AND COALESCE(bb, 0) <> 1
      ) t0
      LEFT JOIN x t1 ON t1.b = t0.ag
      AND t1.cv = t0.cv
    WHERE
      t1.b IS NULL
    ORDER BY
      t0.ag
  ),
  cte1 AS (
    SELECT
      t6.cv AS cv,
      t6.ag AS b,
      t3.ay AS ay,
      t3.bg AS bg,
      t6.cu AS cu,
      t6.cw AS cw,
      COALESCE(t6.co, t6.cm, 0) AS ac,
      CASE
        WHEN t6.co IS NOT NULL
        OR t6.cm IS NOT NULL THEN t6.cn
        ELSE '0000'
      END AS ae,
      TO_CHAR(t6.cl, '%Y-%m-%d') AS ab,
      CASE
        WHEN t6.co IS NOT NULL THEN 0
        WHEN t6.cm IS NOT NULL THEN 1
        ELSE 2
      END AS c9,
      t5.d AS d
    FROM
      cte0 t2
      JOIN y t3 INDEXED by ix_i ON t3.ag = t2.au
      AND t3.cv = t2.cv
      JOIN v t4 INDEXED by ix_f ON t4.b = t2.ag
      AND t4.cv = t3.cv
      AND t4.ai < 0
      JOIN w t5 INDEXED BY ix_g ON t5.x = t4.ag
      AND t5.cv = t4.cv
      AND t5.db IS NULL
      AND t5.da IS NULL
      JOIN l t6 INDEXED by ix_d ON t6.ag = t5.bs
      AND t6.cv = t5.cv
      AND COALESCE(t6.bb, 0) <> 1
      LEFT JOIN x t7 ON t7.b = t6.ag
      AND t7.cv = t6.cv
      LEFT JOIN f t8 ON t8.cw = t6.cw
    WHERE
      t8.z = 8
      AND (t3.aw IN (42, 45))
  ),
  cte2 AS (
    SELECT
      t9.cv AS cv,
      t9.ag AS b,
      t9.ay AS ay,
      coalesce(t3.bg, t9.bg) AS bg,
      coalesce(t3.cu, t9.cu) AS cu,
      t9.cw AS cw,
      COALESCE(t9.co, t9.cm, 0) AS ac,
      t9.cn AS ae,
      TO_CHAR(t9.cl, '%Y-%m-%d') AS ab,
      CASE
        WHEN t9.co IS NOT NULL THEN 0
        ELSE 1
      END AS c9,
      COALESCE(
        (
          t9.d * COALESCE(t8.cj, 0.0) * (
            CASE
              WHEN t9.h IS NOT NULL THEN -1
              ELSE 1
            END
          )
        ),
        0.0
      ) AS d
    FROM
      n t9
      LEFT JOIN y t3 INDEXED by ix_i ON t9.au = t3.ag
      AND t3.cv = t9.cv
      LEFT JOIN f t8 ON t9.cw = t8.cw
      LEFT JOIN n t10 INDEXED by ix_e ON t10.h = t9.ag
      AND t10.cv = t3.cv
      LEFT JOIN h t11 ON t9.cu = t11.cu
    WHERE
      t9.cv = 3023424
      AND COALESCE(t9.cs, t9.a) = ('2024-08-14')
      AND t8.z = 8
      AND (coalesce(t3.aw, t9.bf) IN (42, 45))
  ),
  cte3 AS (
    SELECT
      *
    FROM
      cte1
    UNION ALL
    SELECT
      *
    FROM
      cte2
  ),
  cte4 AS (
    SELECT
      cte3.cv,
      cte3.b,
      cte3.ay,
      cte3.bg,
      cte3.cu,
      cte3.cw,
      cte3.ac,
      cte3.ae,
      cte3.ab,
      cte3.c9,
      cte3.d,
      t12.cx,
      ROW_NUMBER() OVER (
        ORDER BY
          b DESC
      ) AS c11
    FROM
      cte3
      LEFT JOIN ad t12 ON t12.cv = cte3.cv
      AND t12.ay = cte3.ay
  )
SELECT
  *
FROM
  cte4
WHERE
  c11 > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q56
WITH
  cte0 AS (
    SELECT
      t0.ag,
      t0.au,
      t0.cv
    FROM
      (
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs >= ('2024-08-14')
          AND cs < ('2024-08-15')
          AND COALESCE(bb, 0) <> 1
        UNION ALL
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs IS NULL
          AND a >= ('2024-08-14')
          AND a < ('2024-08-15')
          AND COALESCE(bb, 0) <> 1
      ) t0
      LEFT JOIN x t1 ON t1.b = t0.ag
      AND t1.cv = t0.cv
    WHERE
      t1.b IS NULL
    ORDER BY
      t0.ag
  ),
  cte1 AS (
    SELECT
      t5.d AS d
    FROM
      cte0 t2
      JOIN y t3 INDEXED by ix_i ON t3.ag = t2.au
      AND t3.cv = t2.cv
      JOIN v t4 INDEXED by ix_f ON t4.b = t2.ag
      AND t4.cv = t3.cv
      AND t4.ai < 0
      JOIN w t5 INDEXED BY ix_g ON t5.x = t4.ag
      AND t5.cv = t4.cv
      AND t5.db IS NULL
      AND t5.da IS NULL
      JOIN l t6 INDEXED by ix_d ON t6.ag = t5.bs
      AND t6.cv = t5.cv
      AND COALESCE(t6.bb, 0) <> 1
      LEFT JOIN x t7 ON t7.b = t6.ag
      AND t7.cv = t6.cv
      LEFT JOIN f t8 ON t8.cw = t6.cw
    WHERE
      t8.z = 8
      AND (t3.aw IN (42, 45))
  ),
  cte2 AS (
    SELECT
      COALESCE(
        (
          t9.d * COALESCE(t8.cj, 0.0) * CASE
            WHEN t9.h IS NOT NULL THEN -1
            ELSE 1
          END
        ),
        0.0
      ) AS d
    FROM
      n t9
      LEFT JOIN y t3 INDEXED by ix_i ON t9.au = t3.ag
      AND t3.cv = t9.cv
      LEFT JOIN f t8 ON t9.cw = t8.cw
      LEFT JOIN n t10 INDEXED by ix_e ON t10.h = t9.ag
      AND t10.cv = t3.cv
      LEFT JOIN h t11 ON t9.cu = t11.cu
    WHERE
      t9.cv = 4797271
      AND COALESCE(t9.cs, t9.a) = ('2024-08-14')
      AND t8.z = 8
      AND (coalesce(t3.aw, t9.bf) IN (42, 45))
  )
SELECT
  COUNT(*) AS c1,
  SUM(d) AS c2
FROM
  (
    SELECT
      *
    FROM
      cte1
    UNION ALL
    SELECT
      *
    FROM
      cte2
  ) t12 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q57
WITH
  cte0 AS (
    SELECT
      t0.ag,
      t0.au,
      t0.cv
    FROM
      (
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs >= ('2024-08-14')
          AND cs < ('2024-08-15')
          AND COALESCE(bb, 0) <> 1
        UNION ALL
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs IS NULL
          AND a >= ('2024-08-14')
          AND a < ('2024-08-15')
          AND COALESCE(bb, 0) <> 1
      ) t0
      LEFT JOIN x t1 ON t1.b = t0.ag
      AND t1.cv = t0.cv
    WHERE
      t1.b IS NULL
  ),
  cte1 AS (
    SELECT DISTINCT
      t6.cv AS cv,
      t3.ay AS ay,
      t3.bg AS bg,
      t3.ah AS ah,
      t6.cu AS cu,
      t8.cx AS cx
    FROM
      cte0 t2
      JOIN y t3 INDEXED by ix_i ON t3.ag = t2.au
      AND t3.cv = t2.cv
      JOIN v t4 INDEXED by ix_f ON t4.b = t2.ag
      AND t4.cv = t3.cv
      AND t4.ai < 0
      JOIN w t5 INDEXED by ix_g ON t5.x = t4.ag
      AND t5.db IS NULL
      AND t5.da IS NULL
      AND t5.cv = t4.cv
      JOIN l t6 INDEXED by ix_d ON t6.ag = t5.bs
      AND t6.cv = t5.cv
      AND COALESCE(t6.bb, 0) <> 1
      LEFT JOIN f t7 ON t7.cw = t6.cw
      LEFT JOIN ad t8 ON t8.cv = t3.cv
      AND t8.ay = t3.ay
    WHERE
      t7.z = 8
      AND (t3.aw IN (42, 45))
  ),
  cte2 AS (
    SELECT DISTINCT
      t9.cv AS cv,
      t9.ay AS ay,
      coalesce(t3.bg, t9.bg) AS bg,
      coalesce(t3.ah, t9.ah) AS ah,
      coalesce(t3.cu, t9.cu) AS cu,
      t8.cx AS cx
    FROM
      n t9
      LEFT JOIN y t3 INDEXED by ix_i ON t9.au = t3.ag
      AND t9.cv = t3.cv
      LEFT JOIN f t7 ON t9.cw = t7.cw
      LEFT JOIN n t10 INDEXED by ix_e ON t10.h = t9.ag
      AND t10.cv = t9.cv
      LEFT JOIN h t11 ON t9.cu = t11.cu
      LEFT JOIN ad t8 ON t8.cv = t3.cv
      AND t8.ay = t3.ay
    WHERE
      t9.cv = 4797271
      AND COALESCE(t9.cs, t9.a) = ('2024-08-14')
      AND t7.z = 8
      AND (coalesce(t3.aw, t9.bf) IN (42, 45))
  )
SELECT
  *
FROM
  cte1
UNION ALL
SELECT
  *
FROM
  cte2 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q58
WITH
  cte0 AS (
    SELECT
      TO_CHAR(COALESCE(t0.cs, t0.a), '%Y-%m-%d') AS a,
      t0.d,
      COALESCE(t3.cj, 0) AS cj,
      t0.h
    FROM
      n t0
      LEFT JOIN n t1 ON t1.h = t0.ag
      AND t1.cv = t0.cv
      LEFT JOIN y t2 ON t0.au = t2.ag
      AND t0.cv = t2.cv
      LEFT JOIN f t3 ON t0.cw = t3.cw
    WHERE
      (
        t0.cv = 4797271
        AND coalesce(t2.aw, t0.bf) IN (42, 45)
        AND t1.ag IS NULL
        AND (
          t0.h IS NULL
          OR t0.h = -1
        )
        AND CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%Y') AS integer) = 2023
        AND CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%m') AS integer) = 9
      )
  )
SELECT
  TO_CHAR(a, '%Y-%m-%d') AS c0,
  SUM(f) AS c2,
  SUM(e) AS c3,
  0 AS c4
FROM
  a
WHERE
  (
    cv = 4797271
    AND aw IN (42, 45)
    AND bm = 2023
    AND bk = 9
  )
GROUP BY
  a
UNION ALL
SELECT
  a,
  0 AS c2,
  0 AS c3,
  COALESCE(
    SUM(
      d * cj * (
        CASE
          WHEN h IS NOT NULL THEN -1
          ELSE 1
        END
      )
    ),
    0.0
  ) AS c4
FROM
  cte0
GROUP BY
  a option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q59
SELECT DISTINCT
  t0.ay,
  t0.bg,
  t0.ah,
  t1.cx
FROM
  a t0 INDEXED BY ix_b
  LEFT JOIN ad t1 ON t0.cv = t1.cv
  AND t0.ay = t1.ay
WHERE
  (
    t0.cv = 4797271
    AND t0.aw IN (42, 45)
    AND bm = 2023
    AND bk = 9
  )
UNION
SELECT DISTINCT
  t2.ay AS c0,
  coalesce(t4.bg, t2.bg) AS c1,
  coalesce(t4.ah, t2.ah) AS c2,
  t1.cx AS c3
FROM
  n t2
  LEFT JOIN n t3 ON t3.h = t2.ag
  AND t3.cv = t2.cv
  LEFT JOIN y t4 ON t2.au = t4.ag
  AND t2.cv = t4.cv
  LEFT JOIN f t5 ON t2.cw = t5.cw
  LEFT JOIN ad t1 ON t2.cv = t1.cv
  AND t2.ay = t1.ay
  LEFT JOIN h t6 ON t2.cu = t6.cu
WHERE
  (
    t2.cv = 4797271
    AND coalesce(t4.aw, t2.bf) IN (42, 45)
    AND CAST(TO_CHAR(COALESCE(t2.cs, t2.a), '%Y') AS integer) = 2023
    AND CAST(TO_CHAR(COALESCE(t2.cs, t2.a), '%m') AS integer) = 9
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q60
SELECT
  z AS c0,
  SUM(f) AS c1,
  SUM(e) AS c2,
  0 AS c3
FROM
  a
WHERE
  (
    cv = 4797271
    AND aw IN (42, 45)
    AND a = '2023-09-25'
  )
GROUP BY
  z
UNION ALL
SELECT
  t3.z AS c0,
  0 AS c1,
  0 AS c2,
  COALESCE(
    SUM(
      t0.d * COALESCE(t3.cj, 0) * CASE
        WHEN t0.h IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS c3
FROM
  n t0
  LEFT JOIN n t1 ON t1.h = t0.ag
  AND t1.cv = t0.cv
  LEFT JOIN y t2 ON t0.au = t2.ag
  AND t0.cv = t2.cv
  LEFT JOIN f t3 ON t0.cw = t3.cw
  LEFT JOIN h t4 ON t0.cu = t4.cu
WHERE
  (
    t0.cv = 4797271
    AND coalesce(t2.aw, t0.bf) IN (42, 45)
    AND COALESCE(t0.cs, t0.a) = '2023-09-25'
    AND t1.ag IS NULL
    AND (
      t0.h IS NULL
      OR t0.h = -1
    )
  )
GROUP BY
  t3.z option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q61
SELECT DISTINCT
  t0.ay,
  t0.bg,
  t0.ah,
  t1.cx
FROM
  a t0 INDEXED BY ix_a
  LEFT JOIN ad t1 ON t0.cv = t1.cv
  AND t0.ay = t1.ay
WHERE
  (
    t0.cv = 4797271
    AND t0.aw IN (42, 45)
    AND t0.a = '2023-09-25'
  )
UNION
SELECT DISTINCT
  t2.ay AS c0,
  coalesce(t4.bg, t2.bg) AS c1,
  coalesce(t4.ah, t2.ah) AS c2,
  t1.cx AS c3
FROM
  n t2
  LEFT JOIN n t3 ON t3.h = t2.ag
  AND t3.cv = t2.cv
  LEFT JOIN y t4 ON t2.au = t4.ag
  AND t2.cv = t4.cv
  LEFT JOIN f t5 ON t2.cw = t5.cw
  LEFT JOIN ad t1 ON t2.cv = t1.cv
  AND t2.ay = t1.ay
  LEFT JOIN h t6 ON t2.cu = t6.cu
WHERE
  (
    t2.cv = 4797271
    AND coalesce(t4.aw, t2.bf) IN (42, 45)
    AND COALESCE(t2.cs, t2.a) = '2023-09-25'
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q62
WITH
  cte0 AS (
    SELECT
      bm,
      bl,
      bk,
      SUM(f) AS f,
      SUM(e) AS e,
      0 AS c2
    FROM
      a
    WHERE
      (
        cv = 4797271
        AND aw IN (42, 45)
        AND bm IN (2025, 2024, 2023)
      )
    GROUP BY
      bm,
      bl,
      bk
  ),
  cte1 AS (
    SELECT
      CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%Y') AS integer) AS bm,
      CAST(
        (
          CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%m') AS int) - 1
        ) / 3 + 1 AS int
      ) AS bl,
      CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%m') AS integer) AS bk,
      COALESCE(
        (
          t0.d * COALESCE(t3.cj, 0.0) * CASE
            WHEN t0.h IS NOT NULL THEN -1
            ELSE 1
          END
        ),
        0.0
      ) AS c6
    FROM
      n t0
      LEFT JOIN n t1 ON t1.h = t0.ag
      AND t1.cv = t0.cv
      LEFT JOIN y t2 ON t0.au = t2.ag
      AND t0.cv = t2.cv
      LEFT JOIN f t3 ON t0.cw = t3.cw
      LEFT JOIN h t4 ON t0.cu = t4.cu
    WHERE
      (
        t0.cv = 4797271
        AND coalesce(t2.aw, t0.bf) IN (42, 45)
        AND t1.ag IS NULL
        AND (
          t0.h IS NULL
          OR t0.h = -1
        )
        AND CAST(TO_CHAR(COALESCE(t0.cs, t0.a), '%Y') AS integer) IN (2025, 2024, 2023)
      )
  ),
  cte2 AS (
    SELECT
      bm,
      bl,
      bk,
      0 AS f,
      0 AS e,
      SUM(c6) AS c2
    FROM
      cte1
    GROUP BY
      bm,
      bl,
      bk
  )
SELECT
  *
FROM
  cte0
UNION ALL
SELECT
  *
FROM
  cte2 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q63
SELECT
  cv AS c0,
  ag AS c1,
  cf AS c2,
  ce AS c3,
  CASE
    WHEN 0 > bx THEN bx
    ELSE 0.0
  END AS c4,
  CASE
    WHEN 0 > bz THEN bz
    ELSE 0.0
  END AS c5,
  CASE
    WHEN 0 > ca THEN ca
    ELSE 0.0
  END AS c6,
  CASE
    WHEN 0 > COALESCE(cd, 0.0) + COALESCE(cc, 0.0) THEN COALESCE(cd, 0.0) + COALESCE(cc, 0.0)
    ELSE 0.0
  END AS c7,
  CASE
    WHEN 0 > cg THEN cg
    ELSE 0.0
  END AS c8
FROM
  s t0
WHERE
  (t0.cv = 3023424) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q64
SELECT
  t1.cq AS c0,
  CASE
    WHEN MIN(t2.u) IS NOT NULL THEN TO_CHAR(MIN(t2.u), '%Y-%m-%d')
    ELSE NULL
  END AS c1,
  SUM(t0.az) AS c2
FROM
  v t0
  JOIN y t1 ON t0.au = t1.ag
  AND t0.cv = t1.cv
  JOIN x t2 ON t0.b = t2.b
  AND t0.cv = t2.cv
  LEFT JOIN i t3 ON t1.cu = t3.br
WHERE
  (
    t0.cv = 3023424
    AND NOT t3.ap
    AND az > 0
  )
GROUP BY
  t1.cq option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q65
SELECT
  'KNO' AS c0,
  t0.cu AS c1,
  t0.ah AS c2,
  t0.bg AS c3,
  (
    CASE
      WHEN (
        t1.cw IS NOT NULL
        AND t1.cw = 3
      ) THEN 1
      ELSE 0
    END
  ) AS c4,
  t0.ay AS c5,
  SUM(
    (
      CASE
        WHEN (
          t1.cw IS NULL
          OR t1.cw <> 3
        )
        AND t0.bx > 0
        AND t0.aw <> 1 THEN 0.0
        ELSE COALESCE(t0.bx, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          t1.cw IS NULL
          OR t1.cw <> 3
        )
        AND t0.bz > 0 THEN 0.0
        ELSE COALESCE(t0.bz, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          t1.cw IS NULL
          OR t1.cw <> 3
        )
        AND t0.ca > 0 THEN 0.0
        ELSE COALESCE(t0.ca, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          t1.cw IS NULL
          OR t1.cw <> 3
        )
        AND t0.cd > 0 THEN 0.0
        ELSE COALESCE(t0.cd, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          t1.cw IS NULL
          OR t1.cw <> 3
        )
        AND t0.cc > 0 THEN 0.0
        ELSE COALESCE(t0.cc, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          t1.cw IS NULL
          OR t1.cw <> 3
        )
        AND t0.cg > 0 THEN 0.0
        ELSE COALESCE(t0.cg, 0.0)
      END
    )::decimal
  ) AS c6,
  SUM(
    (
      CASE
        WHEN t0.aw = 2 THEN COALESCE(t0.bz, 0.0)
        ELSE 0.0
      END
    )::decimal
  ) AS c7,
  SUM(
    (
      CASE
        WHEN t0.aw = 3 THEN COALESCE(t0.ca, 0.0)
        ELSE 0.0
      END
    )::decimal
  ) AS c8,
  SUM(
    (
      CASE
        WHEN t0.aw IN (42, 45) THEN COALESCE(t0.bz, 0.0)
        ELSE 0.0
      END
    )::decimal
  ) AS c9
FROM
  y t0
  LEFT JOIN g t1 ON t0.cu = t1.cu
WHERE
  (cv = 3023424)
GROUP BY
  t0.cu,
  t0.ah,
  t0.bg,
  (
    CASE
      WHEN (
        t1.cw IS NOT NULL
        AND t1.cw = 3
      ) THEN 1
      ELSE 0
    END
  ),
  t0.ay option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q66
WITH
  cte0 AS (
    SELECT
      t0.cu AS cu,
      t0.ah AS ah,
      t0.bg AS bg,
      t0.ay AS ay,
      CASE
        WHEN (
          t2.cw IS NOT NULL
          AND t2.cw = 3
        ) THEN 1
        ELSE 0
      END AS c4,
      COALESCE(
        (
          t0.d * COALESCE(t3.cj, 0) * CASE
            WHEN t0.h IS NOT NULL THEN -1
            ELSE 1
          END
        ),
        0.0
      ) AS aa,
      t4.aw AS aw
    FROM
      n t0
      LEFT JOIN n t1 ON t1.h = t0.ag
      AND t1.cv = t0.cv
      LEFT JOIN g t2 ON t0.cu = t2.cu
      LEFT JOIN f t3 ON t0.cw = t3.cw
      LEFT JOIN y t4 ON t0.au = t4.ag
      AND t0.cv = t4.cv
    WHERE
      (
        t0.cv = 3023424
        AND t1.ag IS NULL
        AND (
          t0.h IS NULL
          OR t0.h = -1
        )
      )
  )
SELECT
  'BUF' AS c7,
  cu,
  ah,
  bg,
  c4,
  ay,
  COALESCE(SUM(aa), 0.0) AS c8,
  COALESCE(
    SUM(
      CASE
        WHEN aw = 2 THEN aa
        ELSE 0.0
      END
    ),
    0.0
  ) AS c9,
  COALESCE(
    SUM(
      CASE
        WHEN aw = 3 THEN aa
        ELSE 0.0
      END
    ),
    0.0
  ) AS c10,
  COALESCE(
    SUM(
      CASE
        WHEN aw IN (42, 45) THEN aa
        ELSE 0.0
      END
    ),
    0.0
  ) AS c11
FROM
  cte0
GROUP BY
  cu,
  ah,
  bg,
  c4,
  ay option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q67
SELECT
  t1.cq AS c0,
  TO_CHAR(COALESCE(t0.cs, t0.a), '%Y-%m-%d') AS c1,
  COALESCE(
    (
      t0.d * COALESCE(t3.cj, 0.0) * CASE
        WHEN t0.h IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS c2
FROM
  n t0
  LEFT JOIN h t1 ON t0.cu = t1.cu
  LEFT JOIN g t2 ON t0.cu = t2.cu
  LEFT JOIN f t3 ON t0.cw = t3.cw
  LEFT JOIN n t4 ON t4.h = t0.ag
  AND t4.cv = t0.cv
WHERE
  (
    t0.cv = 3023424
    AND t4.ag IS NULL
    AND (
      t0.h IS NULL
      OR t0.h = -1
    )
    AND (
      t2.cw IS NULL
      OR t2.cw <> 3
    )
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q68
SELECT
  t0.ay AS c0,
  t0.cu AS c1,
  t0.ah AS c2,
  t0.bg AS c3,
  CASE
    WHEN (
      t2.cw IS NOT NULL
      AND t2.cw = 3
    ) THEN 1
    ELSE 0
  END AS c4,
  TO_CHAR(COALESCE(t0.cs, t0.a), '%Y-%m-%d') AS c5,
  COALESCE(
    (
      t0.d * COALESCE(t3.cj, 0.0) * CASE
        WHEN t0.h IS NOT NULL THEN -1
        ELSE 1
      END
    ),
    0.0
  ) AS c6
FROM
  n t0
  LEFT JOIN h t1 ON t0.cu = t1.cu
  LEFT JOIN g t2 ON t0.cu = t2.cu
  LEFT JOIN f t3 ON t0.cw = t3.cw
  LEFT JOIN n t4 ON t4.h = t0.ag
  AND t4.cv = t0.cv
WHERE
  (
    t0.cv = 3023424
    AND t4.ag IS NULL
    AND (
      t0.h IS NULL
      OR t0.h = -1
    )
    AND (
      t2.cw IS NOT NULL
      AND t2.cw = 3
    )
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q69
SELECT
  cq AS c0,
  SUM(
    (
      CASE
        WHEN bx < 0 THEN COALESCE(bx, 0)
        ELSE 0
      END
    )::decimal
  ) AS c1,
  SUM(
    (
      CASE
        WHEN bx < 0
        AND (
          t1.cw IS NOT NULL
          AND t1.cw = 3
        ) THEN COALESCE(bx, 0)
        ELSE 0
      END
    )::decimal
  ) AS c2,
  SUM(
    (
      CASE
        WHEN bx > 0
        AND (
          t1.cw IS NOT NULL
          AND t1.cw = 3
        ) THEN COALESCE(bx, 0)
        ELSE 0
      END
    )::decimal
  ) AS c3
FROM
  y t0
  LEFT JOIN g t1 ON t0.cu = t1.cu
WHERE
  (
    t0.cv = 3023424
    AND t0.bx <> 0
  )
GROUP BY
  cq option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q70
SELECT
  t0.cu AS c0,
  t0.ah AS c1,
  t0.bg AS c2,
  t0.ay AS c3,
  SUM(
    (
      CASE
        WHEN bx < 0 THEN COALESCE(bx, 0)
        ELSE 0
      END
    )::decimal
  ) AS c4,
  SUM(
    (
      CASE
        WHEN bx < 0
        AND (
          t1.cw IS NOT NULL
          AND t1.cw = 3
        ) THEN COALESCE(bx, 0)
        ELSE 0
      END
    )::decimal
  ) AS c5,
  SUM(
    (
      CASE
        WHEN bx > 0
        AND (
          t1.cw IS NOT NULL
          AND t1.cw = 3
        ) THEN COALESCE(bx, 0)
        ELSE 0
      END
    )::decimal
  ) AS c6
FROM
  y t0
  LEFT JOIN g t1 ON t0.cu = t1.cu
WHERE
  (t0.cv = 3023424)
GROUP BY
  t0.cu,
  t0.ah,
  t0.bg,
  t0.ay option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q71
SELECT
  t1.cq AS c0,
  CASE
    WHEN MIN(t2.u) IS NOT NULL THEN TO_CHAR(MIN(t2.u), '%Y-%m-%d')
    ELSE NULL
  END AS c1,
  SUM(t0.az) AS c2
FROM
  v t0
  JOIN y t1 ON t0.au = t1.ag
  AND t0.cv = t1.cv
  JOIN x t2 ON t0.b = t2.b
  AND t0.cv = t2.cv
  LEFT JOIN i t3 ON t1.cu = t3.br
WHERE
  (
    t0.cv = 3023424
    AND NOT t3.ap
    AND az > 0
  )
GROUP BY
  t1.cq option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q72
SELECT
  z AS c0,
  SUM(f) AS c1,
  SUM(e) AS c2
FROM
  d
WHERE
  (
    cv = 4797271
    AND cq = 4
    AND a = '2025-08-27'
  )
GROUP BY
  z option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q73
WITH
  cte0 AS (
    SELECT
      t0.ag,
      t0.au,
      t0.cv
    FROM
      (
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs >= ('2025-08-27'::datetime)
          AND cs < ('2025-08-28'::datetime)
          AND COALESCE(bb, 0) <> 1
        UNION ALL
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs IS NULL
          AND a >= ('2025-08-27'::datetime)
          AND a < ('2025-08-28'::datetime)
          AND COALESCE(bb, 0) <> 1
      ) t0
      JOIN x t1 ON t1.b = t0.ag
      AND t1.cv = t0.cv
  ),
  cte1 AS (
    SELECT
      t6.cv AS cv,
      t6.ag AS b,
      t3.ay AS ay,
      t3.bg AS bg,
      t6.cu AS cu,
      t6.cw AS cw,
      COALESCE(t6.co, t6.cm, 0) AS ac,
      CASE
        WHEN t6.co IS NOT NULL
        OR t6.cm IS NOT NULL THEN t6.cn
        ELSE '0000'
      END AS ae,
      t6.cl AS ab,
      CASE
        WHEN t6.co IS NOT NULL THEN 0
        WHEN t6.cm IS NOT NULL THEN 1
        ELSE 2
      END AS c9,
      t5.d AS d
    FROM
      cte0 t2
      JOIN y t3 INDEXED by ix_i ON t3.ag = t2.au
      AND t3.cv = t2.cv
      JOIN v t4 INDEXED by ix_f ON t4.b = t2.ag
      AND t4.cv = t3.cv
      AND t4.ai > 0
      JOIN w t5 INDEXED BY ix_g ON t5.x = t4.ag
      AND t5.cv = t4.cv
      AND t5.db IS NULL
      AND t5.da IS NULL
      JOIN l t6 INDEXED by ix_d ON t6.ag = t5.bs
      AND t6.cv = t5.cv
      AND COALESCE(t6.bb, 0) <> 1
      LEFT JOIN x t7 INDEXED by ix_h ON t7.b = t6.ag
      AND t7.cv = t6.cv
      LEFT JOIN f t8 ON t8.cw = t6.cw
    WHERE
      t8.z = 6
      AND (t3.cq = 4)
  ),
  cte2 AS (
    SELECT
      cte1.cv,
      cte1.b,
      cte1.ay,
      cte1.bg,
      cte1.cu,
      cte1.cw,
      cte1.ac,
      cte1.ae,
      cte1.ab,
      cte1.c9,
      cte1.d,
      ROW_NUMBER() OVER (
        ORDER BY
          cte1.b DESC
      ) AS c11,
      t9.cx
    FROM
      cte1
      LEFT JOIN ad t9 ON t9.cv = cte1.cv
      AND t9.ay = cte1.ay
  )
SELECT
  *
FROM
  cte2
WHERE
  c11 > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q74
WITH
  cte0 AS (
    SELECT
      t0.ag,
      t0.au,
      t0.cv
    FROM
      (
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs >= ('2025-08-27')
          AND cs < ('2025-08-28')
          AND COALESCE(bb, 0) <> 1
        UNION ALL
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs IS NULL
          AND a >= ('2025-08-27')
          AND a < ('2025-08-28')
          AND COALESCE(bb, 0) <> 1
      ) t0
      JOIN x t1 ON t1.b = t0.ag
      AND t1.cv = t0.cv
  ),
  cte1 AS (
    SELECT
      t6.d * t8.cj * CASE
        WHEN t6.h IS NULL THEN 1
        ELSE -1
      END AS d
    FROM
      cte0 t2
      JOIN y t3 INDEXED by ix_i ON t3.ag = t2.au
      AND t3.cv = t2.cv
      JOIN v t4 INDEXED by ix_f ON t4.b = t2.ag
      AND t4.cv = t3.cv
      AND t4.ai > 0
      JOIN w t5 INDEXED BY ix_g ON t5.x = t4.ag
      AND t5.cv = t4.cv
      AND t5.db IS NULL
      AND t5.da IS NULL
      JOIN l t6 INDEXED by ix_d ON t6.ag = t5.bs
      AND t6.cv = t5.cv
      AND COALESCE(t6.bb, 0) <> 1
      LEFT JOIN x t7 INDEXED by ix_h ON t7.b = t6.ag
      AND t7.cv = t6.cv
      LEFT JOIN f t8 ON t8.cw = t6.cw
    WHERE
      t8.z = 6
      AND (t3.cq = 4)
  )
SELECT
  COUNT(*) AS c1,
  SUM(d) AS c2
FROM
  cte1 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q75
WITH
  cte0 AS (
    SELECT
      t0.ag,
      t0.au,
      t0.cv
    FROM
      (
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs >= ('2025-08-27')
          AND cs < ('2025-08-28')
          AND COALESCE(bb, 0) <> 1
        UNION ALL
        SELECT
          ag,
          au,
          cv
        FROM
          l
        WHERE
          cv = 4797271
          AND cs IS NULL
          AND a >= ('2025-08-27')
          AND a < ('2025-08-28')
          AND COALESCE(bb, 0) <> 1
      ) t0
      JOIN x t1 ON t1.b = t0.ag
      AND t1.cv = t0.cv
  ),
  cte1 AS (
    SELECT DISTINCT
      t6.cv AS cv,
      t3.ay AS ay,
      t3.bg AS bg,
      t3.ah AS ah,
      t6.cu AS cu,
      t9.cx AS cx
    FROM
      cte0 t2
      JOIN y t3 INDEXED by ix_i ON t3.ag = t2.au
      AND t3.cv = t2.cv
      JOIN v t4 INDEXED by ix_f ON t4.b = t2.ag
      AND t3.cv = t4.cv
      AND t4.ai > 0
      JOIN w t5 INDEXED by ix_g ON t5.x = t4.ag
      AND t5.cv = t4.cv
      AND t5.db IS NULL
      AND t5.da IS NULL
      JOIN l t6 INDEXED by ix_d ON t6.ag = t5.bs
      AND t6.cv = t5.cv
      AND COALESCE(t6.bb, 0) <> 1
      LEFT JOIN x t7 INDEXED by ix_h ON t7.b = t6.ag
      AND t7.cv = t6.cv
      LEFT JOIN f t8 ON t8.cw = t6.cw
      LEFT JOIN ad t9 ON t9.cv = t3.cv
      AND t9.ay = t3.ay
    WHERE
      t8.z = 6
      AND (t3.cq = 4)
  )
SELECT
  cte1.cv,
  cte1.ay,
  cte1.bg,
  cte1.ah,
  cte1.cu,
  cte1.cx
FROM
  cte1 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q76
SELECT
  TO_CHAR(a, '%Y-%m-%d') AS c0,
  SUM(f) AS c1,
  SUM(e) AS c2
FROM
  d
WHERE
  (
    cv = 4797271
    AND cq = 4
    AND bm = 2025
    AND bk = 8
  )
GROUP BY
  cv,
  a
ORDER BY
  c0 DESC option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q77
SELECT
  bk AS c0,
  bl AS c1,
  bm AS c2,
  cq AS c3,
  SUM(f) AS c4,
  SUM(e) AS c5
FROM
  e
WHERE
  (cv = 4797271)
GROUP BY
  cq,
  bk,
  bl,
  bm option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q78
SELECT
  ag,
  cf,
  ce
FROM
  s t0
WHERE
  (t0.cv = 3023424)
ORDER BY
  ag
LIMIT
  1 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q79
SELECT
  t1.cq AS c0,
  CASE
    WHEN MIN(t2.u) IS NOT NULL THEN TO_CHAR(MIN(t2.u), '%Y-%m-%d')
    ELSE NULL
  END AS c1,
  SUM(t0.az) AS c2
FROM
  v t0
  JOIN y t1 ON t0.au = t1.ag
  AND t0.cv = t1.cv
  JOIN x t2 ON t0.b = t2.b
  AND t0.cv = t2.cv
  LEFT JOIN i t3 ON t1.cu = t3.br
WHERE
  (
    t0.cv = 3023424
    AND NOT t3.ap
    AND az > 0
  )
GROUP BY
  t1.cq option (
    SQL_VDBE_OPCODE_MAX = 798000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q80
SELECT
  'KNO' AS c0,
  t0.cu AS c1,
  t0.ah AS c2,
  t0.bg AS c3,
  (
    CASE
      WHEN (
        t1.cw IS NOT NULL
        AND t1.cw = 3
      ) THEN 1
      ELSE 0
    END
  ) AS c4,
  t0.ay AS c5,
  SUM(
    (
      CASE
        WHEN (
          t1.cw IS NULL
          OR t1.cw <> 3
        )
        AND t0.bx > 0
        AND t0.aw <> 1 THEN 0.0
        ELSE COALESCE(t0.bx, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          t1.cw IS NULL
          OR t1.cw <> 3
        )
        AND t0.bz > 0 THEN 0.0
        ELSE COALESCE(t0.bz, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          t1.cw IS NULL
          OR t1.cw <> 3
        )
        AND t0.ca > 0 THEN 0.0
        ELSE COALESCE(t0.ca, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          t1.cw IS NULL
          OR t1.cw <> 3
        )
        AND t0.cd > 0 THEN 0.0
        ELSE COALESCE(t0.cd, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          t1.cw IS NULL
          OR t1.cw <> 3
        )
        AND t0.cc > 0 THEN 0.0
        ELSE COALESCE(t0.cc, 0.0)
      END
    )::decimal + (
      CASE
        WHEN (
          t1.cw IS NULL
          OR t1.cw <> 3
        )
        AND t0.cg > 0 THEN 0.0
        ELSE COALESCE(t0.cg, 0.0)
      END
    )::decimal
  ) AS c6,
  SUM(
    (
      CASE
        WHEN t0.aw = 2 THEN COALESCE(t0.bz, 0.0)
        ELSE 0.0
      END
    )::decimal
  ) AS c7,
  SUM(
    (
      CASE
        WHEN t0.aw = 3 THEN COALESCE(t0.ca, 0.0)
        ELSE 0.0
      END
    )::decimal
  ) AS c8,
  SUM(
    (
      CASE
        WHEN t0.aw IN (42, 45) THEN COALESCE(t0.bz, 0.0)
        ELSE 0.0
      END
    )::decimal
  ) AS c9
FROM
  y t0
  LEFT JOIN g t1 ON t0.cu = t1.cu
WHERE
  (cv = 3023424)
GROUP BY
  t0.cu,
  t0.ah,
  t0.bg,
  (
    CASE
      WHEN (
        t1.cw IS NOT NULL
        AND t1.cw = 3
      ) THEN 1
      ELSE 0
    END
  ),
  t0.ay option (
    SQL_VDBE_OPCODE_MAX = 900000,
    SQL_MOTION_ROW_MAX = 5100
  );

-- TEST: q81
WITH
  cte0 AS (
    SELECT
      t0.cu AS cu,
      t0.ah AS ah,
      t0.bg AS bg,
      t0.ay AS ay,
      CASE
        WHEN (
          t2.cw IS NOT NULL
          AND t2.cw = 3
        ) THEN 1
        ELSE 0
      END AS c4,
      COALESCE(
        (
          t0.d * COALESCE(t3.cj, 0) * CASE
            WHEN t0.h IS NOT NULL THEN -1
            ELSE 1
          END
        ),
        0.0
      ) AS aa,
      t4.aw AS aw
    FROM
      n t0
      LEFT JOIN n t1 ON t1.h = t0.ag
      AND t1.cv = t0.cv
      LEFT JOIN g t2 ON t0.cu = t2.cu
      LEFT JOIN f t3 ON t0.cw = t3.cw
      LEFT JOIN y t4 ON t0.au = t4.ag
      AND t0.cv = t4.cv
    WHERE
      (
        t0.cv = 3023424
        AND t1.ag IS NULL
        AND (
          t0.h IS NULL
          OR t0.h = -1
        )
      )
  )
SELECT
  'BUF' AS c7,
  cu,
  cu,
  ah,
  bg,
  c4,
  ay,
  COALESCE(SUM(aa), 0.0) AS c8,
  COALESCE(
    SUM(
      CASE
        WHEN aw = 2 THEN aa
        ELSE 0.0
      END
    ),
    0.0
  ) AS c9,
  COALESCE(
    SUM(
      CASE
        WHEN aw = 3 THEN aa
        ELSE 0.0
      END
    ),
    0.0
  ) AS c10,
  COALESCE(
    SUM(
      CASE
        WHEN aw IN (42, 45) THEN aa
        ELSE 0.0
      END
    ),
    0.0
  ) AS c11
FROM
  cte0
GROUP BY
  cu,
  ah,
  bg,
  c4,
  ay option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q82
SELECT DISTINCT
  au AS c0
FROM
  v t0
  JOIN x t1 ON t0.b = t1.b
  AND t0.cv = t1.cv
WHERE
  (t0.cv = 2466827)
  AND t0.az > 0 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q83
SELECT
  count(*) AS c0
FROM
  (
    SELECT DISTINCT
      t0.cq,
      t2.aj,
      t2.am,
      t0.aw,
      t0.ay,
      t0.cu,
      t0.bg,
      t1.cz
    FROM
      y t0
      LEFT JOIN i t1 ON t0.cu = t1.br
      LEFT JOIN ab t2 ON t0.cv = t2.cv
      AND t0.ay = t2.ay
    WHERE
      (
        t0.cv = 2466827
        AND t0.ag IN (126412759, 126412766, 566228222)
        AND t0.cq = 4
      )
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q84
WITH
  cte0 AS (
    SELECT
      t0.cq AS cq,
      t0.aw AS aw,
      t0.ay AS ay,
      t0.cu AS cu,
      t0.bg AS bg,
      t1.cz AS cz,
      t2.aj AS aj,
      t2.am AS am,
      COALESCE(SUM(bx + bz + ca + cd + cc + cg), 0.0) AS bv
    FROM
      y t0
      LEFT JOIN i t1 ON t0.cu = t1.br
      LEFT JOIN ab t2 ON t0.cv = t2.cv
    WHERE
      (
        t0.cv = 2466827
        AND t0.ag IN (126412759, 126412766, 566228222)
        AND t0.cq = 4
      )
    GROUP BY
      cq,
      aj,
      am,
      aw,
      t0.ay,
      cu,
      bg,
      t1.cz
  ),
  cte1 AS (
    SELECT
      cte0.*,
      ROW_NUMBER() OVER (
        ORDER BY
          bv
      ) AS c9
    FROM
      cte0
    ORDER BY
      bv
  )
SELECT
  *
FROM
  cte1
WHERE
  c9 > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 15000
  );

-- TEST: q85
WITH
  cte0 AS (
    SELECT
      cte0.cv AS cv,
      cte0.ag AS c1,
      cte0.r AS r,
      cte0.n AS n,
      cte0.q AS q,
      cte0.ak AS ak,
      COALESCE(cte0.t, 0.0) AS t
    FROM
      o cte0
    WHERE
      (
        cte0.cv = 4797271
        AND cte0.s = 2
        AND cte0.n IN (1, 2)
      )
  ),
  cte1 AS (
    SELECT
      cte0.cv AS cv,
      cte0.c1 AS c1,
      cte1.ag AS c7,
      cte0.r AS r,
      cte0.n AS n,
      cte0.q AS q,
      cte0.ak AS ah,
      cte1.aw AS aw,
      cte0.t AS t
    FROM
      cte0
      JOIN p cte1 ON cte0.c1 = cte1.l
      AND cte0.cv = cte1.cv
  ),
  cte2 AS (
    SELECT
      cte1.cv AS cv,
      cte1.c1 AS c1,
      cte1.r AS r,
      cte1.n AS n,
      cte1.q AS q,
      cte1.ah AS ah,
      cte1.aw AS aw,
      cte1.t AS t,
      cte2.ag AS c10
    FROM
      cte1
      JOIN q cte2 ON cte1.c7 = cte2.j
      AND cte1.cv = cte2.cv
  ),
  cte3 AS (
    SELECT
      cte2.cv AS cv,
      cte2.c1 AS c1,
      cte2.r AS r,
      cte2.n AS n,
      cte2.q AS q,
      cte2.ah AS ah,
      cte2.t + CASE
        WHEN cte2.aw IN (42, 45)
        AND cte3.ci IS NOT NULL THEN cte3.ci
        ELSE 0
      END AS c11,
      cte3.y AS y
    FROM
      cte2
      JOIN r cte3 ON cte2.c10 = cte3.k
      AND cte2.cv = cte3.cv
  ),
  cte4 AS (
    SELECT
      c1,
      r,
      CASE
        WHEN n = 1 THEN 'installment'
        WHEN n = 2 THEN 'deferral'
        WHEN n = 4 THEN 'ink'
        WHEN n = 5 THEN 'restructurisation'
        ELSE NULL
      END AS c13,
      TO_CHAR(q, '%Y-%m-%d') AS q,
      ah AS ah,
      SUM(c11) AS cp,
      SUM(y) AS c15,
      ROW_NUMBER() OVER (
        ORDER BY
          q,
          c1
      ) AS c16
    FROM
      cte3
    GROUP BY
      c1,
      r,
      n,
      q,
      ah
    ORDER BY
      q,
      c1
  )
SELECT
  *
FROM
  cte4
WHERE
  c16 > 0
LIMIT
  10 option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 50000
  );

-- TEST: q86
SELECT
  count(t0.ag) AS c0
FROM
  o t0
WHERE
  (
    t0.cv = 4797271
    AND t0.s = 2
    AND t0.n IN (1, 2)
  ) option (
    SQL_VDBE_OPCODE_MAX = 1000000,
    SQL_MOTION_ROW_MAX = 50000
  );

-- TEST: q87
SELECT
  t0.ax AS c0,
  t0.bc AS c1,
  t0.cy AS c2,
  t0.ct AS c3
FROM
  i t0
WHERE
  t0.ax = '18201061201010000510' option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q88
SELECT
  t0.ax AS c0,
  t0.bc AS c1,
  t0.cy AS c2,
  t0.ct AS c3
FROM
  i t0 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q89
SELECT DISTINCT
  t0.ay AS c0,
  t1.cx AS c1
FROM
  y t0
  JOIN ad t1 ON t0.cv = t1.cv
  AND t0.ay = t1.ay
WHERE
  (
    t0.cv = 3023424
    AND (
      LOWER(t0.ay) LIKE '%013%'
      OR LOWER(t1.cx) LIKE '%
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
  t0.bg AS c0,
  t1.bc AS c1
FROM
  y t0
  JOIN j t1 ON t0.bg = t1.bg
WHERE
  (t0.cv = 3023424)
LIMIT
  20 option (
    SQL_VDBE_OPCODE_MAX = 127000,
    SQL_MOTION_ROW_MAX = 5000
  );

-- TEST: q91
WITH
  cte0 AS (
    SELECT
      t0.ba AS ba,
      t0.cu AS cu,
      t1.cq AS cq,
      t0.bo AS ay,
      t0.bp AS bp,
      t0.bh AS bh,
      t0.bq AS bq,
      t0.bu AS bu,
      t0.t AS t,
      t0.az AS az,
      ROW_NUMBER() OVER () AS c10
    FROM
      aa t0
      LEFT JOIN h t1 ON t0.cu = t1.cu
      LEFT JOIN i t2 ON t0.cu = t2.br
    WHERE
      (
        t0.cv = 2337497
        AND t1.cq = 4
      )
  )
SELECT
  *
FROM
  cte0
WHERE
  c10 > 0
LIMIT
  20 option (
    SQL_VDBE_OPCODE_MAX = 45000,
    SQL_MOTION_ROW_MAX = 5000
  )
