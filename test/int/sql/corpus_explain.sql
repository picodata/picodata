-- TEST-MATRIX: pgproto-1rsX1

-- Queries are copied from crates/sql-ast-new-corpus/src/queries.sql. The schema
-- is crates/sql-ast-new-corpus/src/corpus_mock.rs plus the secondary indexes.

-- TEST: corpus-explain-schema
-- SQL:
CREATE TABLE a (
    cv INTEGER NOT NULL,
    aw INTEGER NOT NULL,
    cq INTEGER NOT NULL,
    bk INTEGER NOT NULL,
    bl INTEGER NOT NULL,
    bm INTEGER NOT NULL,
    a DATETIME NOT NULL,
    z INTEGER NOT NULL,
    ay STRING NOT NULL,
    bg STRING NOT NULL,
    ah STRING NOT NULL,
    f DECIMAL NULL,
    e DECIMAL NULL,
    g DECIMAL NULL,
    PRIMARY KEY (cv, aw, cq, bk, bl, bm, a, z, ay, bg, ah)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_c ON a (cv, aw, cq, a);
CREATE INDEX ix_j ON a (cv, ay);
CREATE INDEX ix_k ON a (cv, aw, ay);
CREATE INDEX ix_l ON a (cv, a);
CREATE INDEX ix_a ON a (cv, aw, a);
CREATE INDEX ix_m ON a (cv, aw, bm, bk);
CREATE INDEX ix_b ON a (cv, aw, bm, bk, cq);
CREATE TABLE b (
    cv INTEGER NOT NULL,
    aw INTEGER NOT NULL,
    cq INTEGER NOT NULL,
    bk INTEGER NOT NULL,
    bl INTEGER NOT NULL,
    bm INTEGER NOT NULL,
    ay STRING NOT NULL,
    bg STRING NOT NULL,
    ah STRING NOT NULL,
    f DECIMAL NULL,
    e DECIMAL NULL,
    PRIMARY KEY (cv, aw, cq, bk, bl, bm, ay, bg, ah)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_n ON b (cv, aw, bm);
CREATE INDEX ix_o ON b (cv, aw, bm, cq, bk, bl, f, e);
CREATE TABLE c (
    cv INTEGER NOT NULL,
    bm INTEGER NOT NULL,
    ay STRING NOT NULL,
    bg STRING NOT NULL,
    ah STRING NOT NULL,
    PRIMARY KEY (cv, bm, ay, bg, ah)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_p ON c (cv, bm);
CREATE INDEX ix_q ON c (cv, ay);
CREATE TABLE d (
    cv INTEGER NOT NULL,
    cq INTEGER NOT NULL,
    bk INTEGER NOT NULL,
    bl INTEGER NOT NULL,
    bm INTEGER NOT NULL,
    a DATETIME NOT NULL,
    z INTEGER NOT NULL,
    ay STRING NOT NULL,
    f DECIMAL NULL,
    e DECIMAL NULL,
    PRIMARY KEY (cv, cq, bk, bl, bm, a, z, ay)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_r ON d (cv, ay);
CREATE INDEX ix_s ON d (cv, a);
CREATE TABLE e (
    cv INTEGER NOT NULL,
    cq INTEGER NOT NULL,
    bk INTEGER NOT NULL,
    bl INTEGER NOT NULL,
    bm INTEGER NOT NULL,
    ay STRING NOT NULL,
    f DECIMAL NULL,
    e DECIMAL NULL,
    g DECIMAL NULL,
    PRIMARY KEY (cv, cq, bk, bl, bm, ay)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_t ON e (cv, cq, bm, bl, bk);
CREATE INDEX ix_u ON e (cv, ay);
CREATE TABLE f (
    cw INTEGER NOT NULL,
    z INTEGER NULL,
    cj INTEGER NULL,
    PRIMARY KEY (cw)
) USING memtx DISTRIBUTED GLOBALLY;
CREATE TABLE g (
    cu INTEGER NOT NULL,
    cw INTEGER NULL,
    PRIMARY KEY (cu)
) USING memtx DISTRIBUTED GLOBALLY;
CREATE TABLE h (
    cu INTEGER NOT NULL,
    cq INTEGER NULL,
    PRIMARY KEY (cu)
) USING memtx DISTRIBUTED GLOBALLY;
CREATE TABLE i (
    br INTEGER NOT NULL,
    ax STRING NULL,
    cy STRING NULL,
    bc STRING NULL,
    cz BOOLEAN NULL,
    ct BOOLEAN NULL,
    ap BOOLEAN NULL,
    PRIMARY KEY (br)
) USING memtx DISTRIBUTED GLOBALLY;
CREATE TABLE j (
    bg STRING NOT NULL,
    bc STRING NULL,
    PRIMARY KEY (bg)
) USING memtx DISTRIBUTED GLOBALLY;
CREATE TABLE k (
    bi STRING NOT NULL,
    bj INTEGER NULL,
    bk INTEGER NULL,
    bl INTEGER NULL,
    PRIMARY KEY (bi)
) USING memtx DISTRIBUTED GLOBALLY;
CREATE TABLE l (
    ag INTEGER NOT NULL,
    v INTEGER NOT NULL,
    cw INTEGER NOT NULL,
    cv INTEGER NOT NULL,
    cu INTEGER NOT NULL,
    a DATETIME NOT NULL,
    u DATETIME NULL,
    d DECIMAL NOT NULL,
    au INTEGER NOT NULL,
    ah STRING NULL,
    ay STRING NULL,
    bm INTEGER NULL,
    bi STRING NULL,
    cs DATETIME NULL,
    h INTEGER NULL,
    cm INTEGER NULL,
    cn STRING NULL,
    cl DATETIME NULL,
    bg STRING NULL,
    co INTEGER NULL,
    w DATETIME NULL,
    bb INTEGER NULL,
    PRIMARY KEY (ag)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_v ON l (cv);
CREATE INDEX ix_d ON l (cv, ag);
CREATE INDEX ix_w ON l (v);
CREATE INDEX ix_x ON l (bb);
CREATE INDEX ix_y ON l (au, cv);
CREATE INDEX ix_z ON l (cv, cs);
CREATE INDEX ix_aa ON l (cv, a);
CREATE INDEX ix_ab ON l (cv, cw);
CREATE INDEX ix_ac ON l (cv, cw, cs);
CREATE INDEX ix_ad ON l (cv, cw, a);
CREATE TABLE m (
    ag INTEGER NOT NULL,
    cv INTEGER NOT NULL,
    cr INTEGER NOT NULL,
    w DATETIME NOT NULL,
    z INTEGER NULL,
    cu INTEGER NULL,
    bj INTEGER NULL,
    bm INTEGER NULL,
    aw INTEGER NULL,
    aa DECIMAL NULL,
    ao BOOLEAN NULL,
    bb INTEGER NULL,
    ay STRING NULL,
    bg STRING NULL,
    ah STRING NULL,
    cw INTEGER NULL,
    ac DECIMAL NULL,
    ae STRING NULL,
    ab DATETIME NULL,
    ad INTEGER NULL,
    h INTEGER NULL,
    PRIMARY KEY (ag, cr)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_ae ON m (cv);
CREATE INDEX ix_af ON m (cv, w);
CREATE INDEX ix_ag ON m (cv, aw);
CREATE INDEX ix_ah ON m (cv, cr);
CREATE INDEX ix_ai ON m (cv, ay);
CREATE INDEX ix_aj ON m (cv, ag);
CREATE INDEX ix_ak ON m (cv, aw, w);
CREATE TABLE n (
    ag INTEGER NOT NULL,
    v INTEGER NULL,
    cw INTEGER NOT NULL,
    cv INTEGER NOT NULL,
    cu INTEGER NOT NULL,
    a DATETIME NOT NULL,
    u DATETIME NULL,
    d DECIMAL NOT NULL,
    au INTEGER NULL,
    ah STRING NULL,
    ay STRING NULL,
    bm INTEGER NULL,
    bi STRING NULL,
    cs DATETIME NULL,
    h INTEGER NULL,
    cm INTEGER NOT NULL,
    cn STRING NULL,
    cl DATETIME NULL,
    bg STRING NULL,
    co INTEGER NULL,
    w DATETIME NULL,
    i DATETIME NULL,
    bb INTEGER NULL,
    bf INTEGER NULL,
    PRIMARY KEY (ag)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_al ON n (cv);
CREATE INDEX ix_am ON n (cv, ag);
CREATE INDEX ix_an ON n (au, cv);
CREATE INDEX ix_ao ON n (cv, ay);
CREATE INDEX ix_ap ON n (cv, cu);
CREATE INDEX ix_aq ON n (cv, cs);
CREATE INDEX ix_ar ON n (cv, a);
CREATE INDEX ix_e ON n (cv, h);
CREATE INDEX ix_as ON n (h);
CREATE TABLE o (
    ag INTEGER NOT NULL,
    cv INTEGER NOT NULL,
    r STRING NOT NULL,
    q DATETIME NOT NULL,
    n INTEGER NOT NULL,
    m INTEGER NOT NULL,
    ak STRING NULL,
    t DECIMAL NOT NULL,
    s INTEGER NOT NULL,
    bn INTEGER NOT NULL,
    bv DECIMAL NOT NULL,
    PRIMARY KEY (ag)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_at ON o (cv);
CREATE INDEX ix_au ON o (cv, s);
CREATE TABLE p (
    ag INTEGER NOT NULL,
    cv INTEGER NOT NULL,
    l INTEGER NOT NULL,
    au INTEGER NULL,
    av INTEGER NULL,
    aw INTEGER NULL,
    a DATETIME NOT NULL,
    af INTEGER NULL,
    bv DECIMAL NOT NULL,
    cs DATETIME NULL,
    PRIMARY KEY (ag)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_av ON p (cv);
CREATE INDEX ix_aw ON p (l);
CREATE INDEX ix_ax ON p (cv, l);
CREATE TABLE q (
    ag INTEGER NOT NULL,
    cv INTEGER NOT NULL,
    j INTEGER NOT NULL,
    PRIMARY KEY (ag)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_ay ON q (cv);
CREATE INDEX ix_az ON q (j);
CREATE INDEX ix_ba ON q (cv, j);
CREATE TABLE r (
    ag INTEGER NOT NULL,
    cv INTEGER NOT NULL,
    k INTEGER NOT NULL,
    ch DATETIME NOT NULL,
    ci DECIMAL NOT NULL,
    o DATETIME NOT NULL,
    y DECIMAL NULL,
    an INTEGER NOT NULL,
    PRIMARY KEY (ag)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_bb ON r (cv);
CREATE INDEX ix_bc ON r (k);
CREATE INDEX ix_bd ON r (cv, k);
CREATE TABLE s (
    ag INTEGER NOT NULL,
    cv INTEGER NOT NULL,
    bx DECIMAL NULL,
    bz DECIMAL NULL,
    ca DECIMAL NULL,
    cd DECIMAL NULL,
    cc DECIMAL NULL,
    ce DECIMAL NULL,
    cf DECIMAL NULL,
    i DATETIME NULL,
    cg DECIMAL NULL,
    ah STRING NULL,
    ay STRING NULL,
    PRIMARY KEY (ag)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_be ON s (cv);
CREATE INDEX ix_bf ON s (cv, ay);
CREATE TABLE t (
    ag INTEGER NOT NULL,
    cv INTEGER NOT NULL,
    bx DECIMAL NULL,
    p DECIMAL NULL,
    bz DECIMAL NULL,
    i DATETIME NOT NULL,
    PRIMARY KEY (ag)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_bg ON t (cv);
CREATE TABLE u (
    ag INTEGER NOT NULL,
    v INTEGER NOT NULL,
    bx DECIMAL NULL,
    bz DECIMAL NULL,
    ca DECIMAL NULL,
    cc DECIMAL NULL,
    cd DECIMAL NULL,
    ar DATETIME NOT NULL,
    cg DECIMAL NULL,
    ce DECIMAL NULL,
    i DATETIME NOT NULL,
    cf DECIMAL NULL,
    cv INTEGER NULL,
    aq BOOLEAN NULL,
    PRIMARY KEY (ag)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_bh ON u (cv);
CREATE INDEX ix_bi ON u (cv, ar);
CREATE INDEX ix_bj ON u (cv, ag);
CREATE TABLE v (
    ag INTEGER NOT NULL,
    cv INTEGER NOT NULL,
    v INTEGER NOT NULL,
    ai DECIMAL NOT NULL,
    b INTEGER NOT NULL,
    a DATETIME NOT NULL,
    az DECIMAL NOT NULL,
    au INTEGER NOT NULL,
    i DATETIME NULL,
    PRIMARY KEY (ag)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_bk ON v (cv);
CREATE INDEX ix_bl ON v (cv, ag);
CREATE INDEX ix_bm ON v (v);
CREATE INDEX ix_bn ON v (ai);
CREATE INDEX ix_bo ON v (au, cv);
CREATE INDEX ix_f ON v (b, cv);
CREATE TABLE w (
    ag INTEGER NOT NULL,
    x INTEGER NOT NULL,
    d DECIMAL NOT NULL,
    al INTEGER NOT NULL,
    bt INTEGER NULL,
    db INTEGER NULL,
    da INTEGER NULL,
    bs INTEGER NULL,
    c INTEGER NULL,
    i DATETIME NULL,
    cv INTEGER NULL,
    PRIMARY KEY (ag)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_bp ON w (cv);
CREATE INDEX ix_bq ON w (bs, cv, al);
CREATE INDEX ix_br ON w (c, cv);
CREATE INDEX ix_bs ON w (bt);
CREATE INDEX ix_bt ON w (cv, ag);
CREATE INDEX ix_g ON w (x, cv, al);
CREATE TABLE x (
    b INTEGER NOT NULL,
    cv INTEGER NULL,
    u DATETIME NULL,
    cw INTEGER NULL,
    PRIMARY KEY (b)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_bu ON x (cv);
CREATE INDEX ix_h ON x (cv, b);
CREATE TABLE y (
    ag INTEGER NOT NULL,
    cv INTEGER NOT NULL,
    ay STRING NULL,
    cu INTEGER NULL,
    bg STRING NULL,
    ah STRING NULL,
    bx DECIMAL NULL,
    bz DECIMAL NULL,
    ca DECIMAL NULL,
    cd DECIMAL NULL,
    cc DECIMAL NULL,
    cq INTEGER NULL,
    aw INTEGER NULL,
    v INTEGER NULL,
    i DATETIME NULL,
    cg DECIMAL NULL,
    PRIMARY KEY (ag)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_bv ON y (cv, ay);
CREATE INDEX ix_bw ON y (bg);
CREATE INDEX ix_bx ON y (cv, aw);
CREATE INDEX ix_by ON y (cv, cu);
CREATE INDEX ix_bz ON y (cv, cq);
CREATE INDEX ix_ca ON y (cv, ay, aw);
CREATE INDEX ix_i ON y (cv, ag);
CREATE TABLE z (
    cv INTEGER NOT NULL,
    i DATETIME NULL,
    PRIMARY KEY (cv)
) USING vinyl DISTRIBUTED BY (cv);
CREATE TABLE aa (
    ag INTEGER NOT NULL,
    cv INTEGER NULL,
    ay STRING NULL,
    r STRING NULL,
    q DATETIME NULL,
    bq DATETIME NULL,
    t DECIMAL NOT NULL,
    bo STRING NULL,
    bh STRING NULL,
    cu INTEGER NULL,
    bu DATETIME NULL,
    az DECIMAL NULL,
    i DATETIME NULL,
    ba STRING NULL,
    bp STRING NULL,
    PRIMARY KEY (ag)
) USING vinyl DISTRIBUTED BY (cv);
CREATE TABLE ab (
    cv INTEGER NOT NULL,
    aj STRING NULL,
    ay STRING NULL,
    am INTEGER NULL,
    PRIMARY KEY (cv)
) USING vinyl DISTRIBUTED BY (cv);
CREATE TABLE ac (
    cv INTEGER NOT NULL,
    bx DECIMAL NULL,
    bz DECIMAL NULL,
    ca DECIMAL NULL,
    cb DECIMAL NULL,
    cg DECIMAL NULL,
    cp DECIMAL NULL,
    i DATETIME NULL,
    PRIMARY KEY (cv)
) USING vinyl DISTRIBUTED BY (cv);
CREATE TABLE ad (
    cv INTEGER NOT NULL,
    ay STRING NOT NULL,
    cx STRING NULL,
    PRIMARY KEY (cv, ay)
) USING vinyl DISTRIBUTED BY (cv);
CREATE INDEX ix_cb ON ad (cv, ay, cx);

-- TEST: corpus-explain-q1
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  ) t2 ON 1 = 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  case 
    when t1.c2::int > 0::int then true::bool
    else false::bool
  end -> c0,
  coalesce(
    (t2.c3::decimal * -1::int)::decimal,
    0.0::decimal
  )::decimal::decimal -> c1
)
  left join on (true::bool)
    scan t1
      projection (count(*)::int -> c2)
        selection (t0.cv::int = 3023424::int)
          scan z -> t0
    motion [policy: full, program: ReshardIfNeeded]
      scan t2
        projection (sum(t0.cp::decimal::decimal)::decimal -> c3)
          selection (
            (
              t0.cv::int = 3023424::int
              and t0.cp::decimal > 0::int
            )
          )
            scan ac -> t0
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  sum ("t0"."cp"::decimal) as "c3"
FROM
  "ac" as "t0"
WHERE
  "t0"."cv" = 3023424::int
  and "t0"."cp" > 0::int
''
plan:
    [0] SEARCH TABLE ac AS t0 USING PRIMARY KEY (cv=?) (~1 row)
''
buckets = [1222]
''
╭────────────────────────────────────────╮
│ 2. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  CASE
    WHEN "t1"."c2" > 0::int THEN true::bool
    ELSE false::bool
  END as "c0",
  coalesce (("t2"."COL_0" * (-1)::int)::decimal, 0.0::decimal)::decimal as "c1"
FROM
  (
    SELECT
      count (*) as "c2"
    FROM
      "z" as "t0"
    WHERE
      "t0"."cv" = 3023424::int
  ) as "t1"
  LEFT JOIN (
    SELECT
      "COL_0"
    FROM
      "_tmp_3883115281032213722_0136"
  ) as "t2" ON true::bool
''
plan:
    [1] SEARCH TABLE z AS t0 USING PRIMARY KEY (cv=?) (~1 row)
    [2] SCAN TABLE _tmp_3883115281032213722_0136 (~1048576 rows)
    [0] SCAN SUBQUERY 1 AS t1 (~1 row)
        [0] SCAN SUBQUERY 2 AS t2 (~1 row)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q2
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  a;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  to_char(a.a::datetime::datetime, '%Y-%m-%d'::text)::text -> c0,
  sum(a.f::decimal::decimal)::decimal -> c1,
  sum(a.e::decimal::decimal)::decimal -> c2,
  sum(a.g::decimal::decimal)::decimal -> c3
)
  group by (a.a::datetime)
    selection (
      (
        a.cv::int = 3023424::int
        and a.aw::int = 0::int
        and a.cq::int = 22::int
        and a.bm::int = 2023::int
        and a.bk::int = 7::int
      )
    )
      scan a
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "to_char" ("a"."a"::datetime, '%Y-%m-%d'::text) as "c0",
  sum ("a"."f"::decimal) as "c1",
  sum ("a"."e"::decimal) as "c2",
  sum ("a"."g"::decimal) as "c3"
FROM
  "a"
WHERE
  "a"."cv" = 3023424::int
  and "a"."aw" = 0::int
  and "a"."cq" = 22::int
  and "a"."bm" = 2023::int
  and "a"."bk" = 7::int
GROUP BY
  "a"."a"
''
plan:
    [0] SEARCH TABLE a USING COVERING INDEX ix_c (cv=? AND aw=? AND cq=?) (~7 rows)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q3
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  c0;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  cte0.c0::text -> c0,
  coalesce(
    sum(cte0.c1::decimal::decimal)::decimal::decimal,
    0.0::decimal
  )::decimal -> c1
)
  group by (cte0.c0::text)
    scan cte cte0($0)
subquery $0:
  projection (
    to_char(
      coalesce(
        t0.cs::datetime::datetime,
        t0.a::datetime::datetime
      )::datetime::datetime,
      '%Y-%m-%d'::text
    )::text -> c0,
    t0.d::decimal * coalesce(t3.cj::int::decimal, 0::decimal)::decimal * case 
      when not t0.h::int is null then -1::int
      else 1::int
    end -> c1
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and coalesce(t1.aw::int::int, t0.bf::int::int)::int = 0::int
        and to_char(
          coalesce(
            t0.cs::datetime::datetime,
            t0.a::datetime::datetime
          )::datetime::datetime,
          '%Y'::text
        )::text::int = 2023::int
        and to_char(
          coalesce(
            t0.cs::datetime::datetime,
            t0.a::datetime::datetime
          )::datetime::datetime,
          '%m'::text
        )::text::int = 7::int
        and coalesce(t1.cq::int::int, t4.cq::int::int)::int = 22::int
        and t2.ag::int is null
        and (t0.h::int is null or t0.h::int = -1::int)
      )
    )
      left join on (t0.cu::int = t4.cu::int)
        left join on (t0.cw::int = t3.cw::int)
          left join on (
            ROW(t0.ag::int, t0.cv::int) = ROW(t2.h::int, t2.cv::int)
          )
            left join on (
              ROW(t0.au::int, t0.cv::int) = ROW(t1.ag::int, t1.cv::int)
            )
              scan n -> t0
              scan y -> t1
            scan n -> t2
          scan f -> t3
        scan h -> t4
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "cte0"."c0",
  coalesce (sum ("cte0"."c1"::decimal)::decimal, 0.0::decimal) as "c1"
FROM
  (
    SELECT
      "to_char" (
        coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
        '%Y-%m-%d'::text
      ) as "c0",
      "t0"."d" * coalesce ("t3"."cj"::decimal, 0::decimal) * CASE
        WHEN not "t0"."h" is null THEN (-1)::int
        ELSE 1::int
      END as "c1"
    FROM
      "n" as "t0"
      LEFT JOIN "y" as "t1" ON "t0"."au" = "t1"."ag"
      and "t0"."cv" = "t1"."cv"
      LEFT JOIN "n" as "t2" ON "t2"."h" = "t0"."ag"
      and "t2"."cv" = "t0"."cv"
      LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
      LEFT JOIN "h" as "t4" ON "t0"."cu" = "t4"."cu"
    WHERE
      "t0"."cv" = 3023424::int
      and coalesce ("t1"."aw"::int, "t0"."bf"::int) = 0::int
      and "to_char" (
        coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
        '%Y'::text
      )::int = 2023::int
      and "to_char" (
        coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
        '%m'::text
      )::int = 7::int
      and coalesce ("t1"."cq"::int, "t4"."cq"::int) = 22::int
      and "t2"."ag" is null
      and (
        "t0"."h" is null
        or "t0"."h" = (-1)::int
      )
  ) as "cte0"
GROUP BY
  "cte0"."c0"
''
plan:
    [0] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~8 rows)
        [0] SEARCH TABLE y AS t1 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
            [0] SEARCH TABLE n AS t2 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
                [0] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
                    [0] SEARCH TABLE h AS t4 USING PRIMARY KEY (cu=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q4
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
motion [policy: full, program: RemoveDuplicates]
  union
    projection (
      t0.ay::text -> ay,
      t0.bg::text -> bg,
      t0.ah::text -> ah,
      t1.cx::text -> cx
    )
      selection (
        (
          t0.cv::int = 3023424::int
          and t0.aw::int = 0::int
          and t0.bm::int = 2023::int
          and t0.bk::int = 7::int
          and t0.cq::int = 22::int
        )
      )
        left join on (
          ROW(t0.cv::int, t0.ay::text) = ROW(t1.cv::int, t1.ay::text)
        )
          scan a -> t0 (indexed by ix_b)
          scan ad -> t1
    projection (
      t2.ay::text -> c0,
      coalesce(t4.bg::text::text, t2.bg::text::text)::text -> c1,
      coalesce(t4.ah::text::text, t2.ah::text::text)::text -> c2,
      t1.cx::text -> c3
    )
      selection (
        (
          t2.cv::int = 3023424::int
          and coalesce(t4.aw::int::int, t2.bf::int::int)::int = 0::int
          and to_char(
            coalesce(
              t2.cs::datetime::datetime,
              t2.a::datetime::datetime
            )::datetime::datetime,
            '%Y'::text
          )::text::int = 2023::int
          and to_char(
            coalesce(
              t2.cs::datetime::datetime,
              t2.a::datetime::datetime
            )::datetime::datetime,
            '%m'::text
          )::text::int = 7::int
          and coalesce(t4.cq::int::int, t6.cq::int::int)::int = 22::int
        )
      )
        left join on (t2.cu::int = t6.cu::int)
          left join on (
            ROW(t2.cv::int, t2.ay::text) = ROW(t1.cv::int, t1.ay::text)
          )
            left join on (t2.cw::int = t5.cw::int)
              left join on (
                ROW(t2.au::int, t2.cv::int) = ROW(t4.ag::int, t4.cv::int)
              )
                left join on (
                  ROW(t2.ag::int, t2.cv::int) = ROW(t3.h::int, t3.cv::int)
                )
                  scan n -> t2
                  scan n -> t3
                scan y -> t4
              scan f -> t5
            scan ad -> t1
          scan h -> t6
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT DISTINCT
  "t0"."ay",
  "t0"."bg",
  "t0"."ah",
  "t1"."cx"
FROM
  "a" as "t0" INDEXED BY "ix_b"
  LEFT JOIN "ad" as "t1" ON "t0"."cv" = "t1"."cv"
  and "t0"."ay" = "t1"."ay"
WHERE
  "t0"."cv" = 3023424::int
  and "t0"."aw" = 0::int
  and "t0"."bm" = 2023::int
  and "t0"."bk" = 7::int
  and "t0"."cq" = 22::int
UNION
SELECT DISTINCT
  "t2"."ay" as "c0",
  coalesce ("t4"."bg"::text, "t2"."bg"::text) as "c1",
  coalesce ("t4"."ah"::text, "t2"."ah"::text) as "c2",
  "t1"."cx" as "c3"
FROM
  "n" as "t2"
  LEFT JOIN "n" as "t3" ON "t3"."h" = "t2"."ag"
  and "t3"."cv" = "t2"."cv"
  LEFT JOIN "y" as "t4" ON "t2"."au" = "t4"."ag"
  and "t2"."cv" = "t4"."cv"
  LEFT JOIN "f" as "t5" ON "t2"."cw" = "t5"."cw"
  LEFT JOIN "ad" as "t1" ON "t2"."cv" = "t1"."cv"
  and "t2"."ay" = "t1"."ay"
  LEFT JOIN "h" as "t6" ON "t2"."cu" = "t6"."cu"
WHERE
  "t2"."cv" = 3023424::int
  and coalesce ("t4"."aw"::int, "t2"."bf"::int) = 0::int
  and "to_char" (
    coalesce ("t2"."cs"::datetime, "t2"."a"::datetime)::datetime,
    '%Y'::text
  )::int = 2023::int
  and "to_char" (
    coalesce ("t2"."cs"::datetime, "t2"."a"::datetime)::datetime,
    '%m'::text
  )::int = 7::int
  and coalesce ("t4"."cq"::int, "t6"."cq"::int) = 22::int
''
plan:
    [1] SEARCH TABLE a AS t0 
         USING COVERING INDEX ix_b 
         (cv=? AND aw=? AND bm=? AND bk=? AND cq=?) 
         (~5 rows)
        [1] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [2] SEARCH TABLE n AS t2 USING COVERING INDEX ix_al (cv=?) (~8 rows)
        [2] SEARCH TABLE n AS t3 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [2] SEARCH TABLE y AS t4 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [2] SEARCH TABLE f AS t5 USING PRIMARY KEY (cw=?) (~1 row)
                    [2] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
                        [2] SEARCH TABLE h AS t6 USING PRIMARY KEY (cu=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q5
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  z;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  a.z::int -> c0,
  sum(a.f::decimal::decimal)::decimal -> c1,
  sum(a.e::decimal::decimal)::decimal -> c2,
  sum(a.g::decimal::decimal)::decimal -> c3
)
  group by (a.z::int)
    selection (
      (
        a.cv::int = 3023424::int
        and a.aw::int = 0::int
        and a.cq::int = 22::int
        and a.a::datetime = '2023-07-28 0:00:00.0 +00:00:00'::datetime
      )
    )
      scan a
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "a"."z" as "c0",
  sum ("a"."f"::decimal) as "c1",
  sum ("a"."e"::decimal) as "c2",
  sum ("a"."g"::decimal) as "c3"
FROM
  "a"
WHERE
  "a"."cv" = 3023424::int
  and "a"."aw" = 0::int
  and "a"."cq" = 22::int
  and "a"."a" = '2023-07-28 0:00:00.0 +00:00:00'::datetime
GROUP BY
  "a"."z"
''
plan:
    [0] SEARCH TABLE a 
         USING COVERING INDEX ix_c 
         (cv=? AND aw=? AND cq=? AND a=?) 
         (~7 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q6
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  t2.z;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t2.z::int -> c0,
  coalesce(
    sum(
      (
        t0.d::decimal * coalesce(t2.cj::int::decimal, 0::decimal)::decimal * case 
          when not t0.h::int is null then -1::int
          else 1::int
        end
      )::decimal
    )::decimal::decimal,
    0.0::decimal
  )::decimal -> c1
)
  group by (t2.z::int)
    selection (
      (
        t0.cv::int = 3023424::int
        and coalesce(t1.aw::int::int, t0.bf::int::int)::int = 0::int
        and coalesce(t1.cq::int::int, t3.cq::int::int)::int = 22::int
        and coalesce(
          t0.cs::datetime::datetime,
          t0.a::datetime::datetime
        )::datetime = '2023-07-28 0:00:00.0 +00:00:00'::datetime
        and t4.ag::int is null
        and (t0.h::int is null or t0.h::int = -1::int)
      )
    )
      left join on (
        ROW(t0.ag::int, t0.cv::int) = ROW(t4.h::int, t4.cv::int)
      )
        left join on (t0.cu::int = t3.cu::int)
          left join on (t0.cw::int = t2.cw::int)
            left join on (
              ROW(t0.au::int, t0.cv::int) = ROW(t1.ag::int, t1.cv::int)
            )
              scan n -> t0
              scan y -> t1
            scan f -> t2
          scan h -> t3
        scan n -> t4
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t2"."z" as "c0",
  coalesce (
    sum (
      (
        "t0"."d" * coalesce ("t2"."cj"::decimal, 0::decimal) * CASE
          WHEN not "t0"."h" is null THEN (-1)::int
          ELSE 1::int
        END
      )::decimal
    )::decimal,
    0.0::decimal
  ) as "c1"
FROM
  "n" as "t0"
  LEFT JOIN "y" as "t1" ON "t0"."au" = "t1"."ag"
  and "t0"."cv" = "t1"."cv"
  LEFT JOIN "f" as "t2" ON "t0"."cw" = "t2"."cw"
  LEFT JOIN "h" as "t3" ON "t0"."cu" = "t3"."cu"
  LEFT JOIN "n" as "t4" ON "t4"."h" = "t0"."ag"
  and "t4"."cv" = "t0"."cv"
WHERE
  "t0"."cv" = 3023424::int
  and coalesce ("t1"."aw"::int, "t0"."bf"::int) = 0::int
  and coalesce ("t1"."cq"::int, "t3"."cq"::int) = 22::int
  and coalesce ("t0"."cs"::datetime, "t0"."a"::datetime) = '2023-07-28 0:00:00.0 +00:00:00'::datetime
  and "t4"."ag" is null
  and (
    "t0"."h" is null
    or "t0"."h" = (-1)::int
  )
GROUP BY
  "t2"."z"
''
plan:
    [0] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~8 rows)
        [0] SEARCH TABLE y AS t1 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
            [0] SEARCH TABLE f AS t2 USING PRIMARY KEY (cw=?) (~1 row)
                [0] SEARCH TABLE h AS t3 USING PRIMARY KEY (cu=?) (~1 row)
                    [0] SEARCH TABLE n AS t4 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q7
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
motion [policy: full, program: RemoveDuplicates]
  union
    projection (
      t0.ay::text -> ay,
      t0.bg::text -> bg,
      t0.ah::text -> ah,
      t1.cx::text -> cx
    )
      selection (
        (
          t0.cv::int = 3023424::int
          and t0.aw::int = 0::int
          and t0.a::datetime = '2023-07-28 0:00:00.0 +00:00:00'::datetime
          and t0.cq::int = 22::int
        )
      )
        left join on (
          ROW(t0.cv::int, t0.ay::text) = ROW(t1.cv::int, t1.ay::text)
        )
          scan a -> t0 (indexed by ix_c)
          scan ad -> t1
    projection (
      t2.ay::text -> c0,
      coalesce(t4.bg::text::text, t2.bg::text::text)::text -> c1,
      coalesce(t4.ah::text::text, t2.ah::text::text)::text -> c2,
      t1.cx::text -> c3
    )
      selection (
        (
          t2.cv::int = 3023424::int
          and coalesce(t4.aw::int::int, t2.bf::int::int)::int = 0::int
          and coalesce(
            t2.cs::datetime::datetime,
            t2.a::datetime::datetime
          )::datetime = '2023-07-28 0:00:00.0 +00:00:00'::datetime
          and coalesce(t4.cq::int::int, t6.cq::int::int)::int = 22::int
        )
      )
        left join on (t2.cu::int = t6.cu::int)
          left join on (
            ROW(t2.cv::int, t2.ay::text) = ROW(t1.cv::int, t1.ay::text)
          )
            left join on (t2.cw::int = t5.cw::int)
              left join on (
                ROW(t2.au::int, t2.cv::int) = ROW(t4.ag::int, t4.cv::int)
              )
                left join on (
                  ROW(t2.ag::int, t2.cv::int) = ROW(t3.h::int, t3.cv::int)
                )
                  scan n -> t2
                  scan n -> t3
                scan y -> t4
              scan f -> t5
            scan ad -> t1
          scan h -> t6
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT DISTINCT
  "t0"."ay",
  "t0"."bg",
  "t0"."ah",
  "t1"."cx"
FROM
  "a" as "t0" INDEXED BY "ix_c"
  LEFT JOIN "ad" as "t1" ON "t0"."cv" = "t1"."cv"
  and "t0"."ay" = "t1"."ay"
WHERE
  "t0"."cv" = 3023424::int
  and "t0"."aw" = 0::int
  and "t0"."a" = '2023-07-28 0:00:00.0 +00:00:00'::datetime
  and "t0"."cq" = 22::int
UNION
SELECT DISTINCT
  "t2"."ay" as "c0",
  coalesce ("t4"."bg"::text, "t2"."bg"::text) as "c1",
  coalesce ("t4"."ah"::text, "t2"."ah"::text) as "c2",
  "t1"."cx" as "c3"
FROM
  "n" as "t2"
  LEFT JOIN "n" as "t3" ON "t3"."h" = "t2"."ag"
  and "t3"."cv" = "t2"."cv"
  LEFT JOIN "y" as "t4" ON "t2"."au" = "t4"."ag"
  and "t2"."cv" = "t4"."cv"
  LEFT JOIN "f" as "t5" ON "t2"."cw" = "t5"."cw"
  LEFT JOIN "ad" as "t1" ON "t2"."cv" = "t1"."cv"
  and "t2"."ay" = "t1"."ay"
  LEFT JOIN "h" as "t6" ON "t2"."cu" = "t6"."cu"
WHERE
  "t2"."cv" = 3023424::int
  and coalesce ("t4"."aw"::int, "t2"."bf"::int) = 0::int
  and coalesce ("t2"."cs"::datetime, "t2"."a"::datetime) = '2023-07-28 0:00:00.0 +00:00:00'::datetime
  and coalesce ("t4"."cq"::int, "t6"."cq"::int) = 22::int
''
plan:
    [1] SEARCH TABLE a AS t0 
         USING COVERING INDEX ix_c 
         (cv=? AND aw=? AND cq=? AND a=?) 
         (~7 rows)
        [1] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [2] SEARCH TABLE n AS t2 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [2] SEARCH TABLE n AS t3 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [2] SEARCH TABLE y AS t4 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [2] SEARCH TABLE f AS t5 USING PRIMARY KEY (cw=?) (~1 row)
                    [2] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
                        [2] SEARCH TABLE h AS t6 USING PRIMARY KEY (cu=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q8
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  (t0.cv = 3023424);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t0.ag::int -> c0,
  t0.cv::int -> c1,
  case 
    when 0::int > t0.bx::decimal then t0.bx::decimal
    else 0.0::decimal
  end -> c2,
  case 
    when 0::int > t0.bz::decimal then t0.bz::decimal
    else 0.0::decimal
  end -> c3,
  case 
    when 0::int > coalesce(t0.cd::decimal::decimal, 0.0::decimal)::decimal + coalesce(t0.cc::decimal::decimal, 0.0::decimal)::decimal then coalesce(t0.cd::decimal::decimal, 0.0::decimal)::decimal + coalesce(t0.cc::decimal::decimal, 0.0::decimal)::decimal
    else 0.0::decimal
  end -> c4,
  case 
    when 0::int > t0.ca::decimal then t0.ca::decimal
    else 0.0::decimal
  end -> c5,
  case 
    when 0::int > t0.cg::decimal then t0.cg::decimal
    else 0.0::decimal
  end -> c6
)
  selection (t0.cv::int = 3023424::int)
    scan s -> t0
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ag" as "c0",
  "t0"."cv" as "c1",
  CASE
    WHEN 0::int > "t0"."bx" THEN "t0"."bx"
    ELSE 0.0::decimal
  END as "c2",
  CASE
    WHEN 0::int > "t0"."bz" THEN "t0"."bz"
    ELSE 0.0::decimal
  END as "c3",
  CASE
    WHEN 0::int > coalesce ("t0"."cd"::decimal, 0.0::decimal) + coalesce ("t0"."cc"::decimal, 0.0::decimal) THEN coalesce ("t0"."cd"::decimal, 0.0::decimal) + coalesce ("t0"."cc"::decimal, 0.0::decimal)
    ELSE 0.0::decimal
  END as "c4",
  CASE
    WHEN 0::int > "t0"."ca" THEN "t0"."ca"
    ELSE 0.0::decimal
  END as "c5",
  CASE
    WHEN 0::int > "t0"."cg" THEN "t0"."cg"
    ELSE 0.0::decimal
  END as "c6"
FROM
  "s" as "t0"
WHERE
  "t0"."cv" = 3023424::int
''
plan:
    [0] SEARCH TABLE s AS t0 USING COVERING INDEX ix_be (cv=?) (~10 rows)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q9
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  coalesce(t2.aw::int::int, t0.bf::int::int)::int -> c0,
  coalesce(t2.cq::int::int, t4.cq::int::int)::int -> c1,
  coalesce(
    t0.cs::datetime::datetime,
    t0.a::datetime::datetime
  )::datetime -> c2,
  coalesce(
    (
      t0.d::decimal * coalesce(t3.cj::int::decimal, 0.0::decimal)::decimal * case 
        when not t0.h::int is null then -1::int
        else 1::int
      end
    )::decimal,
    0.0::decimal
  )::decimal -> c3
)
  selection (
    (
      t0.cv::int = 3023424::int
      and t1.ag::int is null
      and (t0.h::int is null or t0.h::int = -1::int)
    )
  )
    left join on (t0.cu::int = t4.cu::int)
      left join on (t0.cw::int = t3.cw::int)
        left join on (
          ROW(t0.au::int, t0.cv::int) = ROW(t2.ag::int, t2.cv::int)
        )
          left join on (
            ROW(t0.ag::int, t0.cv::int) = ROW(t1.h::int, t1.cv::int)
          )
            scan n -> t0
            scan n -> t1
          scan y -> t2
        scan f -> t3
      scan h -> t4
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  coalesce ("t2"."aw"::int, "t0"."bf"::int) as "c0",
  coalesce ("t2"."cq"::int, "t4"."cq"::int) as "c1",
  coalesce ("t0"."cs"::datetime, "t0"."a"::datetime) as "c2",
  coalesce (
    (
      "t0"."d" * coalesce ("t3"."cj"::decimal, 0.0::decimal) * CASE
        WHEN not "t0"."h" is null THEN (-1)::int
        ELSE 1::int
      END
    )::decimal,
    0.0::decimal
  ) as "c3"
FROM
  "n" as "t0"
  LEFT JOIN "n" as "t1" ON "t1"."h" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
  LEFT JOIN "y" as "t2" ON "t0"."au" = "t2"."ag"
  and "t0"."cv" = "t2"."cv"
  LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
  LEFT JOIN "h" as "t4" ON "t0"."cu" = "t4"."cu"
WHERE
  "t0"."cv" = 3023424::int
  and "t1"."ag" is null
  and (
    "t0"."h" is null
    or "t0"."h" = (-1)::int
  )
''
plan:
    [0] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [0] SEARCH TABLE n AS t1 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [0] SEARCH TABLE y AS t2 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [0] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
                    [0] SEARCH TABLE h AS t4 USING PRIMARY KEY (cu=?) (~1 row)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q10
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
SELECT
  SUM(CAST(p * 100 AS integer) / 100.0) AS c0
FROM
  t t0
WHERE
  (
    t0.cv = 3023424
    AND p < 0
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  sum(
    (
      (t0.p::decimal * 100::int)::int / 100.0::decimal
    )::decimal
  )::decimal -> c0
)
  selection (
    (
      t0.cv::int = 3023424::int
      and t0.p::decimal < 0::int
    )
  )
    scan t -> t0
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  sum (
    (("t0"."p" * 100::int)::int / 100.0::decimal)::decimal
  ) as "c0"
FROM
  "t" as "t0"
WHERE
  "t0"."cv" = 3023424::int
  and "t0"."p" < 0::int
''
plan:
    [0] SEARCH TABLE t AS t0 USING COVERING INDEX ix_bg (cv=?) (~9 rows)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q11
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  10;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 10
  projection (
    cte4.cv::int -> cv,
    cte4.b::int -> b,
    cte4.ay::text -> ay,
    cte4.bg::text -> bg,
    cte4.cu::int -> cu,
    cte4.cw::int -> cw,
    cte4.ac::int -> ac,
    cte4.ae::text -> ae,
    cte4.ab::text -> ab,
    cte4.c9::int -> c9,
    cte4.d::decimal -> d,
    cte4.cx::text -> cx,
    cte4.c11::int -> c11
  )
    selection (cte4.c11::int > 0::int)
      scan cte cte4($4)
subquery $0:
  projection (ag::int, au::int, cv::int)
    order by (ag::int)
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (
            t0.ag::int -> ag,
            t0.au::int -> au,
            t0.cv::int -> cv
          )
            selection (t1.b::int is null)
              left join on (
                ROW(t0.ag::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
              )
                scan t0
                  union all
                    projection (
                      l.ag::int -> ag,
                      l.au::int -> au,
                      l.cv::int -> cv
                    )
                      selection (
                        (
                          l.cv::int = 3023424::int
                          and l.cs::datetime >= '2023-04-28 0:00:00.0 +00:00:00'::datetime
                          and l.cs::datetime < '2023-04-29 0:00:00.0 +00:00:00'::datetime
                          and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                        )
                      )
                        scan l
                    projection (
                      l.ag::int -> ag,
                      l.au::int -> au,
                      l.cv::int -> cv
                    )
                      selection (
                        (
                          l.cv::int = 3023424::int
                          and l.cs::datetime is null
                          and l.a::datetime >= '2023-04-28 0:00:00.0 +00:00:00'::datetime
                          and l.a::datetime < '2023-04-29 0:00:00.0 +00:00:00'::datetime
                          and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                        )
                      )
                        scan l
                scan x -> t1
subquery $1:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      t6.cv::int -> cv,
      t6.ag::int -> b,
      t3.ay::text -> ay,
      t3.bg::text -> bg,
      t6.cu::int -> cu,
      t6.cw::int -> cw,
      coalesce(t6.co::int::int, t6.cm::int::int, 0::int)::int -> ac,
      case 
        when not t6.co::int is null or not t6.cm::int is null then t6.cn::text
        else '0000'::text
      end -> ae,
      to_char(t6.cl::datetime::datetime, '%Y-%m-%d'::text)::text -> ab,
      case 
        when not t6.co::int is null then 0::int
        when not t6.cm::int is null then 1::int
        else 2::int
      end -> c9,
      t5.d::decimal -> d
    )
      selection (
        (
          t8.z::int = 7::int
          and t3.aw::int = 0::int
          and t3.cq::int = 22::int
        )
      )
        left join on (t8.cw::int = t6.cw::int)
          left join on (
            ROW(t6.ag::int, t6.cv::int) = ROW(t7.b::int, t7.cv::int)
          )
            join on (
              (
                ROW(t5.bs::int, t5.cv::int) = ROW(t6.ag::int, t6.cv::int)
                and coalesce(t6.bb::int::int, 0::int)::int <> 1::int
              )
            )
              join on (
                (
                  ROW(t4.ag::int, t4.cv::int) = ROW(t5.x::int, t5.cv::int)
                  and t5.db::int is null
                  and t5.da::int is null
                )
              )
                join on (
                  (
                    ROW(t2.ag::int, t3.cv::int) = ROW(t4.b::int, t4.cv::int)
                    and t4.ai::decimal < 0::int
                  )
                )
                  join on (
                    ROW(t2.au::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
                  )
                    motion [policy: segment([ref(cv)]), program: ReshardIfNeeded]
                      scan cte t2($0)
                    scan y -> t3 (indexed by ix_i)
                  scan v -> t4 (indexed by ix_f)
                scan w -> t5 (indexed by ix_g)
              scan l -> t6 (indexed by ix_d)
            scan x -> t7
          scan f -> t8
subquery $2:
  projection (
    t9.cv::int -> cv,
    t9.ag::int -> b,
    t9.ay::text -> ay,
    coalesce(t3.bg::text::text, t9.bg::text::text)::text -> bg,
    coalesce(t3.cu::int::int, t9.cu::int::int)::int -> cu,
    t9.cw::int -> cw,
    coalesce(t9.co::int::int, t9.cm::int::int, 0::int)::int -> ac,
    t9.cn::text -> ae,
    to_char(t9.cl::datetime::datetime, '%Y-%m-%d'::text)::text -> ab,
    case 
      when not t9.co::int is null then 0::int
      else 1::int
    end -> c9,
    coalesce(
      (
        t9.d::decimal * coalesce(t8.cj::int::decimal, 0.0::decimal)::decimal * case 
          when not t9.h::int is null then -1::int
          else 1::int
        end
      )::decimal,
      0.0::decimal
    )::decimal -> d
  )
    selection (
      (
        t9.cv::int = 3023424::int
        and coalesce(
          t9.cs::datetime::datetime,
          t9.a::datetime::datetime
        )::datetime = '2023-04-28 0:00:00.0 +00:00:00'::datetime
        and t8.z::int = 7::int
        and coalesce(t3.aw::int::int, t9.bf::int::int)::int = 0::int
        and coalesce(t3.cq::int::int, t11.cq::int::int)::int = 22::int
      )
    )
      left join on (t9.cu::int = t11.cu::int)
        left join on (
          ROW(t9.ag::int, t3.cv::int) = ROW(t10.h::int, t10.cv::int)
        )
          left join on (t9.cw::int = t8.cw::int)
            left join on (
              ROW(t9.au::int, t9.cv::int) = ROW(t3.ag::int, t3.cv::int)
            )
              scan n -> t9
              scan y -> t3 (indexed by ix_i)
            scan f -> t8
          scan n -> t10 (indexed by ix_e)
        scan h -> t11
subquery $3:
  motion [policy: full, program: ReshardIfNeeded]
    union all
      motion [policy: local, program: SerializeAsEmptyTable(true)]
        projection (
          cte1.cv::int -> cv,
          cte1.b::int -> b,
          cte1.ay::text -> ay,
          cte1.bg::text -> bg,
          cte1.cu::int -> cu,
          cte1.cw::int -> cw,
          cte1.ac::int -> ac,
          cte1.ae::text -> ae,
          cte1.ab::text -> ab,
          cte1.c9::int -> c9,
          cte1.d::decimal -> d
        )
          scan cte cte1($1)
      projection (
        cte2.cv::int -> cv,
        cte2.b::int -> b,
        cte2.ay::text -> ay,
        cte2.bg::text -> bg,
        cte2.cu::int -> cu,
        cte2.cw::int -> cw,
        cte2.ac::int -> ac,
        cte2.ae::text -> ae,
        cte2.ab::text -> ab,
        cte2.c9::int -> c9,
        cte2.d::decimal -> d
      )
        scan cte cte2($2)
subquery $4:
  projection (
    unnamed_join.cv::int -> cv,
    unnamed_join.b::int -> b,
    unnamed_join.ay::text -> ay,
    unnamed_join.bg::text -> bg,
    unnamed_join.cu::int -> cu,
    unnamed_join.cw::int -> cw,
    unnamed_join.ac::int -> ac,
    unnamed_join.ae::text -> ae,
    unnamed_join.ab::text -> ab,
    unnamed_join.c9::int -> c9,
    unnamed_join.d::decimal -> d,
    unnamed_join.cx::text -> cx,
    row_number() over (order by (unnamed_join.b::int desc) ) -> c11
  )
    motion [policy: full, program: AddMissingRowsForLeftJoin]
      projection (
        cte3.cv::int -> cv,
        cte3.b::int -> b,
        cte3.ay::text -> ay,
        cte3.bg::text -> bg,
        cte3.cu::int -> cu,
        cte3.cw::int -> cw,
        cte3.ac::int -> ac,
        cte3.ae::text -> ae,
        cte3.ab::text -> ab,
        cte3.c9::int -> c9,
        cte3.d::decimal -> d,
        t12.cv::int -> cv,
        t12.ay::text -> ay,
        t12.bucket_id::int -> bucket_id,
        t12.cx::text -> cx
      )
        join on (
          ROW(cte3.cv::int, cte3.ay::text) = ROW(t12.cv::int, t12.ay::text)
        )
          scan cte cte3($3)
          scan ad -> t12
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ag",
  "t0"."au",
  "t0"."cv"
FROM
  (
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 3023424::int
      and "l"."cs" >= '2023-04-28 0:00:00.0 +00:00:00'::datetime
      and "l"."cs" < '2023-04-29 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
    UNION ALL
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 3023424::int
      and "l"."cs" is null
      and "l"."a" >= '2023-04-28 0:00:00.0 +00:00:00'::datetime
      and "l"."a" < '2023-04-29 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
  ) as "t0"
  LEFT JOIN "x" as "t1" ON "t1"."b" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
WHERE
  "t1"."b" is null
''
plan:
    [2] SEARCH TABLE l USING COVERING INDEX ix_z (cv=? AND cs>? AND cs<?) (~1 row)
    [3] SEARCH TABLE l USING COVERING INDEX ix_aa (cv=? AND a>? AND a<?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t0 (~1 row)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
''
buckets = [1222]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "COL_0" as "ag",
  "COL_1" as "au",
  "COL_2" as "cv"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_877372333083973691_1136"
  )
ORDER BY
  "COL_0"
''
plan:
    [0] SCAN TABLE _tmp_877372333083973691_1136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
╭─────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "t6"."cv",
  "t6"."ag" as "b",
  "t3"."ay",
  "t3"."bg",
  "t6"."cu",
  "t6"."cw",
  coalesce ("t6"."co"::int, "t6"."cm"::int, 0::int) as "ac",
  CASE
    WHEN not "t6"."co" is null
    or not "t6"."cm" is null THEN "t6"."cn"
    ELSE '0000'::text
  END as "ae",
  "to_char" ("t6"."cl"::datetime, '%Y-%m-%d'::text) as "ab",
  CASE
    WHEN not "t6"."co" is null THEN 0::int
    WHEN not "t6"."cm" is null THEN 1::int
    ELSE 2::int
  END as "c9",
  "t5"."d"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_18309399111922391115_2136"
  ) as "t2"
  INNER JOIN "y" as "t3" INDEXED BY "ix_i" ON "t3"."ag" = "t2"."COL_1"
  and "t3"."cv" = "t2"."COL_2"
  INNER JOIN "v" as "t4" INDEXED BY "ix_f" ON "t4"."b" = "t2"."COL_0"
  and "t4"."cv" = "t3"."cv"
  and "t4"."ai" < 0::int
  INNER JOIN "w" as "t5" INDEXED BY "ix_g" ON "t5"."x" = "t4"."ag"
  and "t5"."cv" = "t4"."cv"
  and "t5"."db" is null
  and "t5"."da" is null
  INNER JOIN "l" as "t6" INDEXED BY "ix_d" ON "t6"."ag" = "t5"."bs"
  and "t6"."cv" = "t5"."cv"
  and coalesce ("t6"."bb"::int, 0::int) <> 1::int
  LEFT JOIN "x" as "t7" ON "t7"."b" = "t6"."ag"
  and "t7"."cv" = "t6"."cv"
  LEFT JOIN "f" as "t8" ON "t8"."cw" = "t6"."cw"
WHERE
  "t8"."z" = 7::int
  and "t3"."aw" = 0::int
  and "t3"."cq" = 22::int
''
plan:
    [0] SCAN TABLE _tmp_18309399111922391115_2136 (~1048576 rows)
        [0] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~7 rows)
            [0] SEARCH TABLE v AS t4 USING COVERING INDEX ix_f (b=? AND cv=?) (~8 rows)
                [0] SEARCH TABLE w AS t5 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                    [0] SEARCH TABLE l AS t6 USING COVERING INDEX ix_d (cv=? AND ag=?) (~8 rows)
                        [0] SEARCH TABLE x AS t7 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
                            [0] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
''
buckets <= [1-3000]
''
╭────────────────────────────────────────╮
│ 4. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  *
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
      "_tmp_5092332724665672962_3136"
  ) as "cte1"
UNION ALL
SELECT
  *
FROM
  (
    SELECT
      "t9"."cv",
      "t9"."ag" as "b",
      "t9"."ay",
      coalesce ("t3"."bg"::text, "t9"."bg"::text) as "bg",
      coalesce ("t3"."cu"::int, "t9"."cu"::int) as "cu",
      "t9"."cw",
      coalesce ("t9"."co"::int, "t9"."cm"::int, 0::int) as "ac",
      "t9"."cn" as "ae",
      "to_char" ("t9"."cl"::datetime, '%Y-%m-%d'::text) as "ab",
      CASE
        WHEN not "t9"."co" is null THEN 0::int
        ELSE 1::int
      END as "c9",
      coalesce (
        (
          "t9"."d" * coalesce ("t8"."cj"::decimal, 0.0::decimal) * CASE
            WHEN not "t9"."h" is null THEN (-1)::int
            ELSE 1::int
          END
        )::decimal,
        0.0::decimal
      ) as "d"
    FROM
      "n" as "t9"
      LEFT JOIN "y" as "t3" INDEXED BY "ix_i" ON "t9"."au" = "t3"."ag"
      and "t3"."cv" = "t9"."cv"
      LEFT JOIN "f" as "t8" ON "t9"."cw" = "t8"."cw"
      LEFT JOIN "n" as "t10" INDEXED BY "ix_e" ON "t10"."h" = "t9"."ag"
      and "t10"."cv" = "t3"."cv"
      LEFT JOIN "h" as "t11" ON "t9"."cu" = "t11"."cu"
    WHERE
      "t9"."cv" = 3023424::int
      and coalesce ("t9"."cs"::datetime, "t9"."a"::datetime) = '2023-04-28 0:00:00.0 +00:00:00'::datetime
      and "t8"."z" = 7::int
      and coalesce ("t3"."aw"::int, "t9"."bf"::int) = 0::int
      and coalesce ("t3"."cq"::int, "t11"."cq"::int) = 22::int
  ) as "cte2"
''
plan:
    [1] SCAN TABLE _tmp_5092332724665672962_3136 (~1048576 rows)
    [2] SEARCH TABLE n AS t9 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [2] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [2] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
                [2] SEARCH TABLE n AS t10 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
                    [2] SEARCH TABLE h AS t11 USING PRIMARY KEY (cu=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = [1222]
''
╭──────────────────────────╮
│ 5. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "cte3"."COL_0" as "cv",
  "cte3"."COL_1" as "b",
  "cte3"."COL_2" as "ay",
  "cte3"."COL_3" as "bg",
  "cte3"."COL_4" as "cu",
  "cte3"."COL_5" as "cw",
  "cte3"."COL_6" as "ac",
  "cte3"."COL_7" as "ae",
  "cte3"."COL_8" as "ab",
  "cte3"."COL_9" as "c9",
  "cte3"."COL_10" as "d",
  "t12"."cv",
  "t12"."ay",
  "t12"."bucket_id",
  "t12"."cx"
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
      "_tmp_6654303618351556102_5136"
  ) as "cte3"
  INNER JOIN "ad" as "t12" ON "t12"."cv" = "cte3"."COL_0"
  and "t12"."ay" = "cte3"."COL_2"
''
plan:
    [0] SCAN TABLE _tmp_6654303618351556102_5136 (~1048576 rows)
        [0] SEARCH TABLE ad AS t12 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 6. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "unnamed_join"."COL_0" as "cv",
      "unnamed_join"."COL_1" as "b",
      "unnamed_join"."COL_2" as "ay",
      "unnamed_join"."COL_3" as "bg",
      "unnamed_join"."COL_4" as "cu",
      "unnamed_join"."COL_5" as "cw",
      "unnamed_join"."COL_6" as "ac",
      "unnamed_join"."COL_7" as "ae",
      "unnamed_join"."COL_8" as "ab",
      "unnamed_join"."COL_9" as "c9",
      "unnamed_join"."COL_10" as "d",
      "unnamed_join"."COL_14" as "cx",
      row_number () OVER (
        ORDER BY
          "unnamed_join"."COL_1" DESC) as "c11"
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
          "COL_10",
          "COL_11",
          "COL_12",
          "COL_13",
          "COL_14"
        FROM
          "_tmp_6219172643915444872_6136"
      ) as "unnamed_join"
  ) as "cte4"
WHERE
  "cte4"."c11" > 0::int
LIMIT
  10
''
plan:
    [2] SCAN TABLE _tmp_6219172643915444872_6136 (~1048576 rows)
    [2] USE TEMP B-TREE FOR ORDER BY
    [1] SCAN SUBQUERY 2 (~1 row)
    [0] SCAN SUBQUERY 1 AS cte4 (~1 row)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q12
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  cte2;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union all
  motion [policy: segment([ref(cv)]), program: ReshardIfNeeded]
    projection (
      cte1.cv::int -> cv,
      cte1.ay::text -> ay,
      cte1.bg::text -> bg,
      cte1.ah::text -> ah,
      cte1.cu::int -> cu,
      cte1.cx::text -> cx
    )
      scan cte cte1($1)
  projection (
    cte2.cv::int -> cv,
    cte2.ay::text -> ay,
    cte2.bg::text -> bg,
    cte2.ah::text -> ah,
    cte2.cu::int -> cu,
    cte2.cx::text -> cx
  )
    scan cte cte2($2)
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      t0.ag::int -> ag,
      t0.au::int -> au,
      t0.cv::int -> cv
    )
      selection (t1.b::int is null)
        left join on (
          ROW(t0.ag::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
        )
          scan t0
            union all
              projection (
                l.ag::int -> ag,
                l.au::int -> au,
                l.cv::int -> cv
              )
                selection (
                  (
                    l.cv::int = 3023424::int
                    and l.cs::datetime >= '2023-04-28 0:00:00.0 +00:00:00'::datetime
                    and l.cs::datetime < '2023-04-28 0:00:00.0 +00:00:00'::datetime
                    and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                  )
                )
                  scan l
              projection (
                l.ag::int -> ag,
                l.au::int -> au,
                l.cv::int -> cv
              )
                selection (
                  (
                    l.cv::int = 3023424::int
                    and l.cs::datetime is null
                    and l.a::datetime >= '2023-04-28 0:00:00.0 +00:00:00'::datetime
                    and l.a::datetime < '2023-04-28 0:00:00.0 +00:00:00'::datetime
                    and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                  )
                )
                  scan l
          scan x -> t1
subquery $1:
  projection (
    gr_expr_1::int -> cv,
    gr_expr_2::text -> ay,
    gr_expr_3::text -> bg,
    gr_expr_4::text -> ah,
    gr_expr_5::int -> cu,
    gr_expr_6::text -> cx
  )
    group by (
      gr_expr_1::int,
      gr_expr_2::text,
      gr_expr_3::text,
      gr_expr_4::text,
      gr_expr_5::int,
      gr_expr_6::text
    )
      motion [policy: full, program: ReshardIfNeeded]
        projection (
          t6.cv::int -> gr_expr_1,
          t3.ay::text -> gr_expr_2,
          t3.bg::text -> gr_expr_3,
          t3.ah::text -> gr_expr_4,
          t6.cu::int -> gr_expr_5,
          t8.cx::text -> gr_expr_6
        )
          group by (
            t6.cv::int,
            t3.ay::text,
            t3.bg::text,
            t3.ah::text,
            t6.cu::int,
            t8.cx::text
          )
            selection (
              (
                t7.z::int = 7::int
                and t3.aw::int = 0::int
                and t3.cq::int = 22::int
              )
            )
              left join on (
                ROW(t3.cv::int, t3.ay::text) = ROW(t8.cv::int, t8.ay::text)
              )
                left join on (t7.cw::int = t6.cw::int)
                  join on (
                    (
                      ROW(t5.bs::int, t5.cv::int) = ROW(t6.ag::int, t6.cv::int)
                      and coalesce(t6.bb::int::int, 0::int)::int <> 1::int
                    )
                  )
                    join on (
                      (
                        ROW(t4.cv::int, t4.ag::int) = ROW(t5.cv::int, t5.x::int)
                        and t5.db::int is null
                        and t5.da::int is null
                      )
                    )
                      join on (
                        (
                          ROW(t2.ag::int, t3.cv::int) = ROW(t4.b::int, t4.cv::int)
                          and t4.ai::decimal < 0::int
                        )
                      )
                        join on (
                          ROW(t2.au::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
                        )
                          scan cte t2($0)
                          scan y -> t3 (indexed by ix_i)
                        scan v -> t4 (indexed by ix_f)
                      scan w -> t5 (indexed by ix_g)
                    scan l -> t6 (indexed by ix_d)
                  scan f -> t7
                scan ad -> t8
subquery $2:
  projection (
    t9.cv::int -> cv,
    t9.ay::text -> ay,
    coalesce(t3.bg::text::text, t9.bg::text::text)::text -> bg,
    coalesce(t3.ah::text::text, t9.ah::text::text)::text -> ah,
    coalesce(t3.cu::int::int, t9.cu::int::int)::int -> cu,
    t8.cx::text -> cx
  )
    selection (
      (
        t9.cv::int = 3023424::int
        and coalesce(
          t9.cs::datetime::datetime,
          t9.a::datetime::datetime
        )::datetime = '2023-04-28 0:00:00.0 +00:00:00'::datetime
        and t7.z::int = 7::int
        and coalesce(t3.aw::int::int, t9.bf::int::int)::int = 0::int
        and coalesce(t3.cq::int::int, t11.cq::int::int)::int = 22::int
      )
    )
      left join on (
        ROW(t3.cv::int, t3.ay::text) = ROW(t8.cv::int, t8.ay::text)
      )
        left join on (t9.cu::int = t11.cu::int)
          left join on (
            ROW(t9.ag::int, t9.cv::int) = ROW(t10.h::int, t10.cv::int)
          )
            left join on (t9.cw::int = t7.cw::int)
              left join on (
                ROW(t9.au::int, t9.cv::int) = ROW(t3.ag::int, t3.cv::int)
              )
                scan n -> t9
                scan y -> t3 (indexed by ix_i)
              scan f -> t7
            scan n -> t10 (indexed by ix_e)
          scan h -> t11
        scan ad -> t8
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ag",
  "t0"."au",
  "t0"."cv"
FROM
  (
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 3023424::int
      and "l"."cs" >= '2023-04-28 0:00:00.0 +00:00:00'::datetime
      and "l"."cs" < '2023-04-28 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
    UNION ALL
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 3023424::int
      and "l"."cs" is null
      and "l"."a" >= '2023-04-28 0:00:00.0 +00:00:00'::datetime
      and "l"."a" < '2023-04-28 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
  ) as "t0"
  LEFT JOIN "x" as "t1" ON "t1"."b" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
WHERE
  "t1"."b" is null
''
plan:
    [2] SEARCH TABLE l USING COVERING INDEX ix_z (cv=? AND cs>? AND cs<?) (~1 row)
    [3] SEARCH TABLE l USING COVERING INDEX ix_aa (cv=? AND a>? AND a<?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t0 (~1 row)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
''
buckets = [1222]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "t6"."cv" as "gr_expr_1",
  "t3"."ay" as "gr_expr_2",
  "t3"."bg" as "gr_expr_3",
  "t3"."ah" as "gr_expr_4",
  "t6"."cu" as "gr_expr_5",
  "t8"."cx" as "gr_expr_6"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_5930600967966593789_0136"
  ) as "t2"
  INNER JOIN "y" as "t3" INDEXED BY "ix_i" ON "t3"."ag" = "t2"."COL_1"
  and "t3"."cv" = "t2"."COL_2"
  INNER JOIN "v" as "t4" INDEXED BY "ix_f" ON "t4"."b" = "t2"."COL_0"
  and "t4"."cv" = "t3"."cv"
  and "t4"."ai" < 0::int
  INNER JOIN "w" as "t5" INDEXED BY "ix_g" ON "t5"."x" = "t4"."ag"
  and "t5"."db" is null
  and "t5"."da" is null
  and "t5"."cv" = "t4"."cv"
  INNER JOIN "l" as "t6" INDEXED BY "ix_d" ON "t6"."ag" = "t5"."bs"
  and "t6"."cv" = "t5"."cv"
  and coalesce ("t6"."bb"::int, 0::int) <> 1::int
  LEFT JOIN "f" as "t7" ON "t7"."cw" = "t6"."cw"
  LEFT JOIN "ad" as "t8" ON "t8"."cv" = "t3"."cv"
  and "t8"."ay" = "t3"."ay"
WHERE
  "t7"."z" = 7::int
  and "t3"."aw" = 0::int
  and "t3"."cq" = 22::int
GROUP BY
  "t6"."cv",
  "t3"."ay",
  "t3"."bg",
  "t3"."ah",
  "t6"."cu",
  "t8"."cx"
''
plan:
    [0] SCAN TABLE _tmp_5930600967966593789_0136 (~1048576 rows)
        [0] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~7 rows)
            [0] SEARCH TABLE v AS t4 USING COVERING INDEX ix_f (b=? AND cv=?) (~8 rows)
                [0] SEARCH TABLE w AS t5 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                    [0] SEARCH TABLE l AS t6 USING COVERING INDEX ix_d (cv=? AND ag=?) (~8 rows)
                        [0] SEARCH TABLE f AS t7 USING PRIMARY KEY (cw=?) (~1 row)
                            [0] SEARCH TABLE ad AS t8 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT DISTINCT
      "COL_0" as "cv",
      "COL_1" as "ay",
      "COL_2" as "bg",
      "COL_3" as "ah",
      "COL_4" as "cu",
      "COL_5" as "cx"
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
          "_tmp_2630807218819645061_1136"
      )
    GROUP BY
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5"
  ) as "cte1"
''
plan:
    [1] SCAN TABLE _tmp_2630807218819645061_1136 (~1048576 rows)
    [1] USE TEMP B-TREE FOR GROUP BY
    [1] USE TEMP B-TREE FOR DISTINCT
    [0] SCAN SUBQUERY 1 AS cte1 (~1 row)
''
buckets = any
''
╭─────────────────────────────────────────╮
│ 4. Query (DYN-FILTERED STORAGE, <= 1/1) │
╰─────────────────────────────────────────╯
''
SELECT
  "COL_0",
  "COL_1",
  "COL_2",
  "COL_3",
  "COL_4",
  "COL_5"
FROM
  "_tmp_6144964650535932087_2136"
UNION ALL
SELECT
  *
FROM
  (
    SELECT DISTINCT
      "t9"."cv",
      "t9"."ay",
      coalesce ("t3"."bg"::text, "t9"."bg"::text) as "bg",
      coalesce ("t3"."ah"::text, "t9"."ah"::text) as "ah",
      coalesce ("t3"."cu"::int, "t9"."cu"::int) as "cu",
      "t8"."cx"
    FROM
      "n" as "t9"
      LEFT JOIN "y" as "t3" INDEXED BY "ix_i" ON "t9"."au" = "t3"."ag"
      and "t9"."cv" = "t3"."cv"
      LEFT JOIN "f" as "t7" ON "t9"."cw" = "t7"."cw"
      LEFT JOIN "n" as "t10" INDEXED BY "ix_e" ON "t10"."h" = "t9"."ag"
      and "t10"."cv" = "t9"."cv"
      LEFT JOIN "h" as "t11" ON "t9"."cu" = "t11"."cu"
      LEFT JOIN "ad" as "t8" ON "t8"."cv" = "t3"."cv"
      and "t8"."ay" = "t3"."ay"
    WHERE
      "t9"."cv" = 3023424::int
      and coalesce ("t9"."cs"::datetime, "t9"."a"::datetime) = '2023-04-28 0:00:00.0 +00:00:00'::datetime
      and "t7"."z" = 7::int
      and coalesce ("t3"."aw"::int, "t9"."bf"::int) = 0::int
      and coalesce ("t3"."cq"::int, "t11"."cq"::int) = 22::int
  ) as "cte2"
''
plan:
    [1] SCAN TABLE _tmp_6144964650535932087_2136 (~1048576 rows)
    [3] SEARCH TABLE n AS t9 USING COVERING INDEX ix_ao (cv=?) (~9 rows)
        [3] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [3] SEARCH TABLE f AS t7 USING PRIMARY KEY (cw=?) (~1 row)
                [3] SEARCH TABLE n AS t10 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
                    [3] SEARCH TABLE h AS t11 USING PRIMARY KEY (cu=?) (~1 row)
                        [3] SEARCH TABLE ad AS t8 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [3] USE TEMP B-TREE FOR DISTINCT
    [2] SCAN SUBQUERY 3 AS cte2 (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets <= [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q13
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  bm;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  b.cq::int -> c0,
  b.bk::int -> c1,
  b.bl::int -> c2,
  b.bm::int -> c3,
  sum(b.f::decimal::decimal)::decimal -> c4,
  sum(b.e::decimal::decimal)::decimal -> c5
)
  group by (b.cq::int, b.bk::int, b.bl::int, b.bm::int)
    selection (
      (
        b.cv::int = 3023424::int
        and b.aw::int = 0::int
        and b.bm::int in ROW(
          2024::int,
          2023::int,
          2021::int,
          2022::int,
          2019::int,
          2018::int
        )
      )
    )
      scan b
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "b"."cq" as "c0",
  "b"."bk" as "c1",
  "b"."bl" as "c2",
  "b"."bm" as "c3",
  sum ("b"."f"::decimal) as "c4",
  sum ("b"."e"::decimal) as "c5"
FROM
  "b"
WHERE
  "b"."cv" = 3023424::int
  and "b"."aw" = 0::int
  and "b"."bm" in (
    2024::int,
    2023::int,
    2021::int,
    2022::int,
    2019::int,
    2018::int
  )
GROUP BY
  "b"."cq",
  "b"."bk",
  "b"."bl",
  "b"."bm"
''
plan:
    [0] SEARCH TABLE b USING COVERING INDEX ix_o (cv=? AND aw=? AND bm=?) (~7 rows)
    [0] EXECUTE LIST SUBQUERY 1
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q14
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  coalesce(t1.cq::int::int, t4.cq::int::int)::int -> c0,
  to_char(
    coalesce(
      t0.cs::datetime::datetime,
      t0.a::datetime::datetime
    )::datetime::datetime,
    '%Y'::text
  )::text::int -> c1,
  (
    (
      to_char(
        coalesce(
          t0.cs::datetime::datetime,
          t0.a::datetime::datetime
        )::datetime::datetime,
        '%m'::text
      )::text::int - 1::int
    ) / 3::int + 1::int
  )::int -> c2,
  to_char(
    coalesce(
      t0.cs::datetime::datetime,
      t0.a::datetime::datetime
    )::datetime::datetime,
    '%m'::text
  )::text::int -> c3,
  to_char(
    coalesce(
      t0.cs::datetime::datetime,
      t0.a::datetime::datetime
    )::datetime::datetime,
    '%Y-%m-%d'::text
  )::text -> c4,
  coalesce(
    (
      t0.d::decimal * coalesce(t3.cj::int::decimal, 0.0::decimal)::decimal * case 
        when not t0.h::int is null then -1::int
        else 1::int
      end
    )::decimal,
    0.0::decimal
  )::decimal -> c5
)
  selection (
    (
      t0.cv::int = 3023424::int
      and coalesce(t1.aw::int::int, t0.bf::int::int)::int = 0::int
      and to_char(
        coalesce(
          t0.cs::datetime::datetime,
          t0.a::datetime::datetime
        )::datetime::datetime,
        '%Y'::text
      )::text::int in ROW(
        2024::int,
        2023::int,
        2021::int,
        2022::int,
        2019::int,
        2018::int
      )
      and t5.ag::int is null
      and (t0.h::int is null or t0.h::int = -1::int)
    )
  )
    left join on (
      ROW(t0.ag::int, t0.cv::int) = ROW(t5.h::int, t5.cv::int)
    )
      left join on (t0.cu::int = t4.cu::int)
        left join on (t0.cw::int = t3.cw::int)
          left join on (t0.cu::int = t2.cu::int)
            left join on (
              ROW(t0.au::int, t0.cv::int) = ROW(t1.ag::int, t1.cv::int)
            )
              scan n -> t0
              scan y -> t1
            scan g -> t2
          scan f -> t3
        scan h -> t4
      scan n -> t5
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  coalesce ("t1"."cq"::int, "t4"."cq"::int) as "c0",
  "to_char" (
    coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
    '%Y'::text
  )::int as "c1",
  (
    (
      "to_char" (
        coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
        '%m'::text
      )::int - 1::int
    ) / 3::int + 1::int
  )::int as "c2",
  "to_char" (
    coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
    '%m'::text
  )::int as "c3",
  "to_char" (
    coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
    '%Y-%m-%d'::text
  ) as "c4",
  coalesce (
    (
      "t0"."d" * coalesce ("t3"."cj"::decimal, 0.0::decimal) * CASE
        WHEN not "t0"."h" is null THEN (-1)::int
        ELSE 1::int
      END
    )::decimal,
    0.0::decimal
  ) as "c5"
FROM
  "n" as "t0"
  LEFT JOIN "y" as "t1" ON "t0"."au" = "t1"."ag"
  and "t0"."cv" = "t1"."cv"
  LEFT JOIN "g" as "t2" ON "t0"."cu" = "t2"."cu"
  LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
  LEFT JOIN "h" as "t4" ON "t0"."cu" = "t4"."cu"
  LEFT JOIN "n" as "t5" ON "t5"."h" = "t0"."ag"
  and "t5"."cv" = "t0"."cv"
WHERE
  "t0"."cv" = 3023424::int
  and coalesce ("t1"."aw"::int, "t0"."bf"::int) = 0::int
  and "to_char" (
    coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
    '%Y'::text
  )::int in (
    2024::int,
    2023::int,
    2021::int,
    2022::int,
    2019::int,
    2018::int
  )
  and "t5"."ag" is null
  and (
    "t0"."h" is null
    or "t0"."h" = (-1)::int
  )
''
plan:
    [0] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~8 rows)
    [0] EXECUTE LIST SUBQUERY 1
        [0] SEARCH TABLE y AS t1 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
            [0] SEARCH TABLE g AS t2 USING PRIMARY KEY (cu=?) (~1 row)
                [0] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
                    [0] SEARCH TABLE h AS t4 USING PRIMARY KEY (cu=?) (~1 row)
                        [0] SEARCH TABLE n AS t5 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q15
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  bm;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  e.cq::int -> c0,
  e.bk::int -> c1,
  e.bl::int -> c2,
  e.bm::int -> c3,
  sum(e.g::decimal::decimal)::decimal -> c4
)
  group by (e.cq::int, e.bk::int, e.bl::int, e.bm::int)
    selection (
      (
        e.cv::int = 3023424::int
        and e.g::decimal > 0::int
        and e.cq::int in ROW(
          1::int,
          97::int,
          3::int,
          35::int,
          4::int,
          100::int,
          5::int,
          41::int,
          43::int,
          10::int,
          11::int,
          12::int,
          13::int,
          14::int,
          19::int,
          22::int,
          29::int
        )
        and e.bm::int in ROW(
          2024::int,
          2023::int,
          2021::int,
          2022::int,
          2019::int,
          2018::int
        )
      )
    )
      scan e
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "e"."cq" as "c0",
  "e"."bk" as "c1",
  "e"."bl" as "c2",
  "e"."bm" as "c3",
  sum ("e"."g"::decimal) as "c4"
FROM
  "e"
WHERE
  "e"."cv" = 3023424::int
  and "e"."g" > 0::int
  and "e"."cq" in (
    1::int,
    97::int,
    3::int,
    35::int,
    4::int,
    100::int,
    5::int,
    41::int,
    43::int,
    10::int,
    11::int,
    12::int,
    13::int,
    14::int,
    19::int,
    22::int,
    29::int
  )
  and "e"."bm" in (
    2024::int,
    2023::int,
    2021::int,
    2022::int,
    2019::int,
    2018::int
  )
GROUP BY
  "e"."cq",
  "e"."bk",
  "e"."bl",
  "e"."bm"
''
plan:
    [0] SEARCH TABLE e USING COVERING INDEX ix_t (cv=? AND cq=? AND bm=?) (~7 rows)
    [0] EXECUTE LIST SUBQUERY 1
    [0] EXECUTE LIST SUBQUERY 1
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q16
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  cte3;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  cte3.cv::int -> cv,
  cte3.c1::int -> c1,
  cte3.c2::text -> c2,
  cte3.c3::int -> c3,
  cte3.c4::text -> c4,
  cte3.bn::int -> bn,
  cte3.c6::int -> c6,
  cte3.aw::int -> aw,
  cte3.cu::int -> cu,
  cte3.ci::decimal -> ci,
  cte3.y::decimal -> y,
  cte3.ch::text -> ch
)
  scan cte cte3($3)
subquery $0:
  projection (
    cte0.cv::int -> cv,
    cte0.ag::int -> c1,
    cte0.r::text -> c2,
    cte0.s::int -> c3,
    to_char(cte0.q::datetime::datetime, '%Y-%m-%d'::text)::text -> c4,
    cte0.bn::int -> bn,
    cte0.m::int -> c6
  )
    selection (
      (
        cte0.cv::int = 4797271::int
        and cte0.s::int in ROW(1::int, 2::int)
      )
    )
      scan o -> cte0
subquery $1:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      cte0.cv::int -> cv,
      cte0.c1::int -> c1,
      cte0.c2::text -> c2,
      cte0.c3::int -> c3,
      cte0.c4::text -> c4,
      cte0.bn::int -> bn,
      cte0.c6::int -> c6,
      cte1.aw::int -> aw,
      cte1.av::int -> cu,
      cte1.ag::int -> c9
    )
      selection (cte1.af::int <> 0::int)
        join on (
          ROW(cte0.c1::int, cte0.cv::int) = ROW(cte1.l::int, cte1.cv::int)
        )
          scan cte cte0($0)
          scan p -> cte1
subquery $2:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      cte1.cv::int -> cv,
      cte1.c1::int -> c1,
      cte1.c2::text -> c2,
      cte1.c3::int -> c3,
      cte1.c4::text -> c4,
      cte1.bn::int -> bn,
      cte1.c6::int -> c6,
      cte1.aw::int -> aw,
      cte1.cu::int -> cu,
      cte2.ag::int -> c10
    )
      join on (
        ROW(cte1.c9::int, cte1.cv::int) = ROW(cte2.j::int, cte2.cv::int)
      )
        scan cte cte1($1)
        scan q -> cte2
subquery $3:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      cte2.cv::int -> cv,
      cte2.c1::int -> c1,
      cte2.c2::text -> c2,
      cte2.c3::int -> c3,
      cte2.c4::text -> c4,
      cte2.bn::int -> bn,
      cte2.c6::int -> c6,
      cte2.aw::int -> aw,
      cte2.cu::int -> cu,
      cte3.ci::decimal -> ci,
      coalesce(cte3.y::decimal::decimal, 0.0::decimal)::decimal -> y,
      to_char(cte3.ch::datetime::datetime, '%Y-%m-%d'::text)::text -> ch
    )
      selection (
        (
          cte3.o::datetime = to_date('3000-01-01'::text, '%Y-%m-%d'::text)::datetime
          and cte3.an::int = 0::int
        )
      )
        join on (
          ROW(cte2.c10::int, cte2.cv::int) = ROW(cte3.k::int, cte3.cv::int)
        )
          scan cte cte2($2)
          scan r -> cte3
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
  "cte0"."cv",
  "cte0"."c1",
  "cte0"."c2",
  "cte0"."c3",
  "cte0"."c4",
  "cte0"."bn",
  "cte0"."c6",
  "cte1"."aw",
  "cte1"."av" as "cu",
  "cte1"."ag" as "c9"
FROM
  (
    SELECT
      "cte0"."cv",
      "cte0"."ag" as "c1",
      "cte0"."r" as "c2",
      "cte0"."s" as "c3",
      "to_char" ("cte0"."q"::datetime, '%Y-%m-%d'::text) as "c4",
      "cte0"."bn",
      "cte0"."m" as "c6"
    FROM
      "o" as "cte0"
    WHERE
      "cte0"."cv" = 4797271::int
      and "cte0"."s" in (1::int, 2::int)
  ) as "cte0"
  INNER JOIN "p" as "cte1" ON "cte0"."c1" = "cte1"."l"
  and "cte0"."cv" = "cte1"."cv"
WHERE
  "cte1"."af" <> 0::int
''
plan:
    [0] SEARCH TABLE p AS cte1 USING COVERING INDEX ix_av (cv=?) (~9 rows)
        [0] SEARCH TABLE o AS cte0 USING PRIMARY KEY (ag=?) (~1 row)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "cte1"."COL_0" as "cv",
  "cte1"."COL_1" as "c1",
  "cte1"."COL_2" as "c2",
  "cte1"."COL_3" as "c3",
  "cte1"."COL_4" as "c4",
  "cte1"."COL_5" as "bn",
  "cte1"."COL_6" as "c6",
  "cte1"."COL_7" as "aw",
  "cte1"."COL_8" as "cu",
  "cte2"."ag" as "c10"
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
      "COL_9"
    FROM
      "_tmp_4220975242363887950_0136"
  ) as "cte1"
  INNER JOIN "q" as "cte2" ON "cte1"."COL_9" = "cte2"."j"
  and "cte1"."COL_0" = "cte2"."cv"
''
plan:
    [0] SCAN TABLE _tmp_4220975242363887950_0136 (~1048576 rows)
        [0] SEARCH TABLE q AS cte2 USING COVERING INDEX ix_ba (cv=? AND j=?) (~8 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 3. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "cte2"."COL_0" as "cv",
  "cte2"."COL_1" as "c1",
  "cte2"."COL_2" as "c2",
  "cte2"."COL_3" as "c3",
  "cte2"."COL_4" as "c4",
  "cte2"."COL_5" as "bn",
  "cte2"."COL_6" as "c6",
  "cte2"."COL_7" as "aw",
  "cte2"."COL_8" as "cu",
  "cte3"."ci",
  coalesce ("cte3"."y"::decimal, 0.0::decimal) as "y",
  "to_char" ("cte3"."ch"::datetime, '%Y-%m-%d'::text) as "ch"
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
      "COL_9"
    FROM
      "_tmp_6458289239517524612_1136"
  ) as "cte2"
  INNER JOIN "r" as "cte3" ON "cte2"."COL_9" = "cte3"."k"
  and "cte2"."COL_0" = "cte3"."cv"
WHERE
  "cte3"."o" = "to_date" ('3000-01-01'::text, '%Y-%m-%d'::text)
  and "cte3"."an" = 0::int
''
plan:
    [0] SCAN TABLE _tmp_6458289239517524612_1136 (~1048576 rows)
        [0] SEARCH TABLE r AS cte3 USING COVERING INDEX ix_bd (cv=? AND k=?) (~7 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
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
      "COL_10",
      "COL_11"
    FROM
      "_tmp_2520004333235554259_2136"
  ) as "cte3"
''
plan:
    [0] SCAN TABLE _tmp_2520004333235554259_2136 (~1048576 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q17
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  cte2;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
motion [policy: full, program: RemoveDuplicates]
  union
    motion [policy: local, program: [RemoveDuplicates, SerializeAsEmptyTable(true)]]
      union
        projection (
          cte0.c0::int -> c0,
          cte0.c1::int -> c1,
          cte0.ck::int -> ck,
          cte0.c3::int -> c3,
          cte0.cv::int -> cv,
          cte0.a::datetime -> a,
          cte0.ay::text -> ay,
          cte0.bh::text -> bh,
          cte0.cw::int -> cw,
          cte0.z::int -> z,
          cte0.bj::int -> bj,
          cte0.c11::int -> c11,
          cte0.cu::int -> cu,
          cte0.aa::decimal -> aa,
          cte0.ac::decimal -> ac,
          cte0.ad::int -> ad,
          cte0.ab::datetime -> ab,
          cte0.ae::text -> ae,
          cte0.c18::bool -> c18
        )
          scan cte cte0($0)
        projection (
          cte1.c0::int -> c0,
          cte1.c1::int -> c1,
          cte1.ck::int -> ck,
          cte1.c3::int -> c3,
          cte1.cv::int -> cv,
          cte1.a::datetime -> a,
          cte1.ay::text -> ay,
          cte1.bh::text -> bh,
          cte1.cw::int -> cw,
          cte1.z::int -> z,
          cte1.bj::int -> bj,
          cte1.c11::int -> c11,
          cte1.cu::int -> cu,
          cte1.aa::decimal -> aa,
          cte1.ac::decimal -> ac,
          cte1.ad::int -> ad,
          cte1.ab::datetime -> ab,
          cte1.ae::text -> ae,
          cte1.c18::bool -> c18
        )
          scan cte cte1($1)
    projection (
      cte2.c0::int -> c0,
      cte2.c1::int -> c1,
      cte2.ck::int -> ck,
      cte2.c3::int -> c3,
      cte2.cv::int -> cv,
      cte2.a::datetime -> a,
      cte2.ay::text -> ay,
      cte2.bh::text -> bh,
      cte2.cw::int -> cw,
      cte2.z::int -> z,
      cte2.bj::int -> bj,
      cte2.c11::int -> c11,
      cte2.cu::int -> cu,
      cte2.aa::decimal -> aa,
      cte2.ac::decimal -> ac,
      cte2.ad::int -> ad,
      cte2.ab::datetime -> ab,
      cte2.ae::text -> ae,
      cte2.c18::bool -> c18
    )
      scan cte cte2($2)
subquery $0:
  projection (
    t0.ag::int -> c0,
    t5.ag::int -> c1,
    1::int -> ck,
    t4.al::int -> c3,
    t0.cv::int -> cv,
    t0.w::datetime -> a,
    coalesce(t5.ay::text::text, t0.ay::text::text)::text -> ay,
    coalesce(t5.bg::text::text, t0.bg::text::text)::text -> bh,
    t0.cw::int -> cw,
    t1.z::int -> z,
    coalesce(t6.bj::int::int, t0.bj::int::int)::int -> bj,
    t5.bm::int -> c11,
    coalesce(t5.cu::int::int, t0.cu::int::int)::int -> cu,
    t0.aa::decimal -> aa,
    coalesce(
      t5.co::int::decimal,
      t5.cm::int::decimal,
      t0.ac::decimal::decimal
    )::decimal -> ac,
    case 
      when not t5.co::int is null then 0::int
      when not t5.cm::int is null then 1::int
      else t0.ad::int
    end -> ad,
    coalesce(
      t5.cl::datetime::datetime,
      t0.ab::datetime::datetime
    )::datetime -> ab,
    case 
      when not t5.co::int is null or not t5.cm::int is null then t5.cn::text
      else t0.ae::text
    end -> ae,
    case 
      when not t0.h::int is null then true::bool
      else false::bool
    end -> c18
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and t0.aw::int = 1::int
        and t0.w::datetime = '2024-02-28 0:00:00.0 +00:00:00'::datetime
        and t1.z::int = 11::int
        and t0.cr::int = 0::int
        and t0.h::int is null
        and t1.cj::int > 0::int
      )
    )
      left join on (t6.bi::text = t5.bi::text)
        left join on (
          (
            ROW(t0.cv::int, t4.bs::int) = ROW(t5.cv::int, t5.ag::int)
            and t5.cv::int = 3023424::int
          )
        )
          left join on (
            (
              ROW(t0.cv::int, t3.x::int) = ROW(t4.cv::int, t4.x::int)
              and ROW(t4.al::int, t4.cv::int) = ROW(0::int, 3023424::int)
            )
          )
            left join on (
              (
                ROW(t0.cv::int, t2.bt::int) = ROW(t3.cv::int, t3.ag::int)
                and t3.cv::int = 3023424::int
              )
            )
              left join on (
                (
                  ROW(t0.cv::int, t0.ag::int) = ROW(t2.cv::int, t2.bs::int)
                  and ROW(t0.cr::int, t2.al::int, t2.cv::int) = ROW(0::int, 0::int, 3023424::int)
                )
              )
                join on (t1.cw::int = t0.cw::int)
                  scan m -> t0
                  scan f -> t1
                scan w -> t2
              scan w -> t3
            scan w -> t4
          scan l -> t5
        scan k -> t6
subquery $1:
  projection (
    t0.ag::int -> c0,
    t5.ag::int -> c1,
    1::int -> ck,
    t3.al::int -> c3,
    t0.cv::int -> cv,
    t0.w::datetime -> a,
    coalesce(t5.ay::text::text, t0.ay::text::text)::text -> ay,
    coalesce(t5.bg::text::text, t0.bg::text::text)::text -> bh,
    t0.cw::int -> cw,
    t1.z::int -> z,
    coalesce(t6.bj::int::int, t0.bj::int::int)::int -> bj,
    t5.bm::int -> c11,
    coalesce(t5.cu::int::int, t0.cu::int::int)::int -> cu,
    t0.aa::decimal -> aa,
    coalesce(
      t5.co::int::decimal,
      t5.cm::int::decimal,
      t0.ac::decimal::decimal
    )::decimal -> ac,
    case 
      when not t5.co::int is null then 0::int
      when not t5.cm::int is null then 1::int
      else t0.ad::int
    end -> ad,
    coalesce(
      t5.cl::datetime::datetime,
      t0.ab::datetime::datetime
    )::datetime -> ab,
    case 
      when not t5.co::int is null or not t5.cm::int is null then t5.cn::text
      else t0.ae::text
    end -> ae,
    case 
      when not t0.h::int is null then true::bool
      else false::bool
    end -> c18
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and t0.aw::int = 1::int
        and t0.w::datetime = '2024-02-28 0:00:00.0 +00:00:00'::datetime
        and t1.z::int = 11::int
        and t0.cr::int = 0::int
        and t0.h::int is null
        and t1.cj::int < 0::int
      )
    )
      left join on (t6.bi::text = t5.bi::text)
        left join on (
          (
            ROW(t0.cv::int, t3.bs::int) = ROW(t5.cv::int, t5.ag::int)
            and t5.cv::int = 3023424::int
          )
        )
          left join on (
            (
              ROW(t0.cv::int, t2.bt::int) = ROW(t3.cv::int, t3.x::int)
              and t3.cv::int = 3023424::int
              and t3.al::int <> 0::int
              and t3.db::int is null
              and t3.da::int is null
            )
          )
            left join on (
              (
                ROW(t0.cv::int, t0.ag::int) = ROW(t2.cv::int, t2.bs::int)
                and ROW(t0.cr::int, t2.al::int, t2.cv::int) = ROW(0::int, 0::int, 3023424::int)
              )
            )
              join on (t1.cw::int = t0.cw::int)
                scan m -> t0
                scan f -> t1
              scan w -> t2
            scan w -> t3
          scan l -> t5
        scan k -> t6
subquery $2:
  projection (
    t0.ag::int -> c0,
    t5.ag::int -> c1,
    2::int -> ck,
    t2.al::int -> c3,
    t0.cv::int -> cv,
    t0.w::datetime -> a,
    coalesce(t5.ay::text::text, t0.ay::text::text)::text -> ay,
    coalesce(t5.bg::text::text, t0.bg::text::text)::text -> bh,
    t0.cw::int -> cw,
    t1.z::int -> z,
    coalesce(t6.bj::int::int, t0.bj::int::int)::int -> bj,
    t5.bm::int -> c11,
    coalesce(t5.cu::int::int, t0.cu::int::int)::int -> cu,
    t0.aa::decimal -> aa,
    coalesce(
      t5.co::int::decimal,
      t5.cm::int::decimal,
      t0.ac::decimal::decimal
    )::decimal -> ac,
    case 
      when not t5.co::int is null then 0::int
      when not t5.cm::int is null then 1::int
      else 2::int
    end -> ad,
    t5.cl::datetime -> ab,
    case 
      when not t5.co::int is null or not t5.cm::int is null then t5.cn::text
      else t0.ae::text
    end -> ae,
    case 
      when not t0.h::int is null then true::bool
      else false::bool
    end -> c18
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and t0.aw::int = 1::int
        and t0.w::datetime = '2024-02-28 0:00:00.0 +00:00:00'::datetime
        and t1.z::int = 11::int
        and t0.cr::int = 1::int
        and t0.h::int is null
        and t1.cj::int < 0::int
      )
    )
      left join on (t6.bi::text = t5.bi::text)
        left join on (
          (
            ROW(t0.cv::int, t2.bs::int) = ROW(t5.cv::int, t5.ag::int)
            and t5.cv::int = 3023424::int
          )
        )
          left join on (
            (
              ROW(t0.cv::int, t0.ag::int) = ROW(t2.cv::int, t2.c::int)
              and ROW(t0.cr::int, t2.cv::int) = ROW(1::int, 3023424::int)
            )
          )
            join on (t1.cw::int = t0.cw::int)
              scan m -> t0
              scan f -> t1
            scan w -> t2
          scan l -> t5
        scan k -> t6
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "t0"."ag" as "c0",
      "t5"."ag" as "c1",
      1::int as "ck",
      "t4"."al" as "c3",
      "t0"."cv",
      "t0"."w" as "a",
      coalesce ("t5"."ay"::text, "t0"."ay"::text) as "ay",
      coalesce ("t5"."bg"::text, "t0"."bg"::text) as "bh",
      "t0"."cw",
      "t1"."z",
      coalesce ("t6"."bj"::int, "t0"."bj"::int) as "bj",
      "t5"."bm" as "c11",
      coalesce ("t5"."cu"::int, "t0"."cu"::int) as "cu",
      "t0"."aa",
      coalesce (
        "t5"."co"::decimal,
        "t5"."cm"::decimal,
        "t0"."ac"::decimal
      ) as "ac",
      CASE
        WHEN not "t5"."co" is null THEN 0::int
        WHEN not "t5"."cm" is null THEN 1::int
        ELSE "t0"."ad"
      END as "ad",
      coalesce ("t5"."cl"::datetime, "t0"."ab"::datetime) as "ab",
      CASE
        WHEN not "t5"."co" is null
        or not "t5"."cm" is null THEN "t5"."cn"
        ELSE "t0"."ae"
      END as "ae",
      CASE
        WHEN not "t0"."h" is null THEN true::bool
        ELSE false::bool
      END as "c18"
    FROM
      "m" as "t0"
      INNER JOIN "f" as "t1" ON "t1"."cw" = "t0"."cw"
      LEFT JOIN "w" as "t2" ON "t2"."bs" = "t0"."ag"
      and "t2"."cv" = 3023424::int
      and "t2"."al" = 0::int
      and "t0"."cr" = 0::int
      and "t2"."cv" = "t0"."cv"
      LEFT JOIN "w" as "t3" ON "t2"."bt" = "t3"."ag"
      and "t3"."cv" = 3023424::int
      and "t3"."cv" = "t0"."cv"
      LEFT JOIN "w" as "t4" ON "t3"."x" = "t4"."x"
      and "t4"."cv" = 3023424::int
      and "t4"."al" = 0::int
      and "t4"."cv" = "t0"."cv"
      LEFT JOIN "l" as "t5" ON "t4"."bs" = "t5"."ag"
      and "t5"."cv" = 3023424::int
      and "t5"."cv" = "t0"."cv"
      LEFT JOIN "k" as "t6" ON "t6"."bi" = "t5"."bi"
    WHERE
      "t0"."cv" = 3023424::int
      and "t0"."aw" = 1::int
      and "t0"."w" = '2024-02-28 0:00:00.0 +00:00:00'::datetime
      and "t1"."z" = 11::int
      and "t0"."cr" = 0::int
      and "t0"."h" is null
      and "t1"."cj" > 0::int
  ) as "cte0"
UNION
SELECT
  *
FROM
  (
    SELECT
      "t0"."ag" as "c0",
      "t5"."ag" as "c1",
      1::int as "ck",
      "t3"."al" as "c3",
      "t0"."cv",
      "t0"."w" as "a",
      coalesce ("t5"."ay"::text, "t0"."ay"::text) as "ay",
      coalesce ("t5"."bg"::text, "t0"."bg"::text) as "bh",
      "t0"."cw",
      "t1"."z",
      coalesce ("t6"."bj"::int, "t0"."bj"::int) as "bj",
      "t5"."bm" as "c11",
      coalesce ("t5"."cu"::int, "t0"."cu"::int) as "cu",
      "t0"."aa",
      coalesce (
        "t5"."co"::decimal,
        "t5"."cm"::decimal,
        "t0"."ac"::decimal
      ) as "ac",
      CASE
        WHEN not "t5"."co" is null THEN 0::int
        WHEN not "t5"."cm" is null THEN 1::int
        ELSE "t0"."ad"
      END as "ad",
      coalesce ("t5"."cl"::datetime, "t0"."ab"::datetime) as "ab",
      CASE
        WHEN not "t5"."co" is null
        or not "t5"."cm" is null THEN "t5"."cn"
        ELSE "t0"."ae"
      END as "ae",
      CASE
        WHEN not "t0"."h" is null THEN true::bool
        ELSE false::bool
      END as "c18"
    FROM
      "m" as "t0"
      INNER JOIN "f" as "t1" ON "t1"."cw" = "t0"."cw"
      LEFT JOIN "w" as "t2" ON "t2"."bs" = "t0"."ag"
      and "t2"."cv" = 3023424::int
      and "t2"."al" = 0::int
      and "t0"."cr" = 0::int
      and "t2"."cv" = "t0"."cv"
      LEFT JOIN "w" as "t3" ON "t2"."bt" = "t3"."x"
      and "t3"."cv" = 3023424::int
      and "t3"."al" <> 0::int
      and "t3"."db" is null
      and "t3"."da" is null
      and "t3"."cv" = "t0"."cv"
      LEFT JOIN "l" as "t5" ON "t3"."bs" = "t5"."ag"
      and "t5"."cv" = 3023424::int
      and "t5"."cv" = "t0"."cv"
      LEFT JOIN "k" as "t6" ON "t6"."bi" = "t5"."bi"
    WHERE
      "t0"."cv" = 3023424::int
      and "t0"."aw" = 1::int
      and "t0"."w" = '2024-02-28 0:00:00.0 +00:00:00'::datetime
      and "t1"."z" = 11::int
      and "t0"."cr" = 0::int
      and "t0"."h" is null
      and "t1"."cj" < 0::int
  ) as "cte1"
UNION
SELECT
  *
FROM
  (
    SELECT
      "t0"."ag" as "c0",
      "t5"."ag" as "c1",
      2::int as "ck",
      "t2"."al" as "c3",
      "t0"."cv",
      "t0"."w" as "a",
      coalesce ("t5"."ay"::text, "t0"."ay"::text) as "ay",
      coalesce ("t5"."bg"::text, "t0"."bg"::text) as "bh",
      "t0"."cw",
      "t1"."z",
      coalesce ("t6"."bj"::int, "t0"."bj"::int) as "bj",
      "t5"."bm" as "c11",
      coalesce ("t5"."cu"::int, "t0"."cu"::int) as "cu",
      "t0"."aa",
      coalesce (
        "t5"."co"::decimal,
        "t5"."cm"::decimal,
        "t0"."ac"::decimal
      ) as "ac",
      CASE
        WHEN not "t5"."co" is null THEN 0::int
        WHEN not "t5"."cm" is null THEN 1::int
        ELSE 2::int
      END as "ad",
      "t5"."cl" as "ab",
      CASE
        WHEN not "t5"."co" is null
        or not "t5"."cm" is null THEN "t5"."cn"
        ELSE "t0"."ae"
      END as "ae",
      CASE
        WHEN not "t0"."h" is null THEN true::bool
        ELSE false::bool
      END as "c18"
    FROM
      "m" as "t0"
      INNER JOIN "f" as "t1" ON "t1"."cw" = "t0"."cw"
      LEFT JOIN "w" as "t2" ON "t2"."c" = "t0"."ag"
      and "t2"."cv" = 3023424::int
      and "t0"."cr" = 1::int
      and "t2"."cv" = "t0"."cv"
      LEFT JOIN "l" as "t5" ON "t5"."ag" = "t2"."bs"
      and "t5"."cv" = 3023424::int
      and "t5"."cv" = "t0"."cv"
      LEFT JOIN "k" as "t6" ON "t6"."bi" = "t5"."bi"
    WHERE
      "t0"."cv" = 3023424::int
      and "t0"."aw" = 1::int
      and "t0"."w" = '2024-02-28 0:00:00.0 +00:00:00'::datetime
      and "t1"."z" = 11::int
      and "t0"."cr" = 1::int
      and "t0"."h" is null
      and "t1"."cj" < 0::int
  ) as "cte2"
''
plan:
    [2] SEARCH TABLE m AS t0 
         USING COVERING INDEX ix_ak 
         (cv=? AND aw=? AND w=?) 
         (~7 rows)
        [2] SEARCH TABLE f AS t1 USING PRIMARY KEY (cw=?) (~1 row)
            [2] SEARCH TABLE w AS t2 
                 USING COVERING INDEX ix_bq 
                 (bs=? AND cv=? AND al=?) 
                 (~7 rows)
                [2] SEARCH TABLE w AS t3 USING COVERING INDEX ix_bt (cv=? AND ag=?) (~1 row)
                    [2] SEARCH TABLE w AS t4 USING COVERING INDEX ix_g (x=? AND cv=? AND al=?) (~7 rows)
                        [2] SEARCH TABLE l AS t5 USING COVERING INDEX ix_d (cv=? AND ag=?) (~1 row)
                            [2] SEARCH TABLE k AS t6 USING PRIMARY KEY (bi=?) (~1 row)
    [3] SEARCH TABLE m AS t0 
         USING COVERING INDEX ix_ak 
         (cv=? AND aw=? AND w=?) 
         (~7 rows)
        [3] SEARCH TABLE f AS t1 USING PRIMARY KEY (cw=?) (~1 row)
            [3] SEARCH TABLE w AS t2 
                 USING COVERING INDEX ix_bq 
                 (bs=? AND cv=? AND al=?) 
                 (~7 rows)
                [3] SEARCH TABLE w AS t3 USING COVERING INDEX ix_g (x=? AND cv=?) (~6 rows)
                    [3] SEARCH TABLE l AS t5 USING COVERING INDEX ix_d (cv=? AND ag=?) (~1 row)
                        [3] SEARCH TABLE k AS t6 USING PRIMARY KEY (bi=?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 USING TEMP B-TREE (UNION)
    [4] SEARCH TABLE m AS t0 
         USING COVERING INDEX ix_ak 
         (cv=? AND aw=? AND w=?) 
         (~7 rows)
        [4] SEARCH TABLE f AS t1 USING PRIMARY KEY (cw=?) (~1 row)
            [4] SEARCH TABLE w AS t2 USING COVERING INDEX ix_br (c=? AND cv=?) (~7 rows)
                [4] SEARCH TABLE l AS t5 USING COVERING INDEX ix_d (cv=? AND ag=?) (~1 row)
                    [4] SEARCH TABLE k AS t6 USING PRIMARY KEY (bi=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 4 USING TEMP B-TREE (UNION)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q18
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  cte3;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  cte3.bj::int -> bj,
  cte3.c1::int -> c1,
  cte3.cu::int -> cu,
  cte3.ad::int -> ad,
  cte3.ac::decimal -> ac
)
  scan cte cte3($3)
subquery $0:
  projection (
    coalesce(t6.bj::int::int, t0.bj::int::int)::int -> bj,
    t5.bm::int -> c1,
    coalesce(t5.cu::int::int, t0.cu::int::int)::int -> cu,
    case 
      when not t5.co::int is null then 0::int
      when not t5.cm::int is null then 1::int
      else t0.ad::int
    end -> ad,
    coalesce(
      t5.co::int::decimal,
      t5.cm::int::decimal,
      t0.ac::decimal::decimal
    )::decimal -> ac
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and t0.aw::int = 1::int
        and t0.w::datetime = '2024-02-28 0:00:00.0 +00:00:00'::datetime
        and t1.z::int = 11::int
        and t0.cr::int = 0::int
        and not (not t0.h::int is null)
        and t1.cj::int > 0::int
      )
    )
      left join on (t6.bi::text = t5.bi::text)
        left join on (
          ROW(t4.bs::int, t4.cv::int) = ROW(t5.ag::int, t5.cv::int)
        )
          left join on (
            (
              ROW(t3.x::int, t3.cv::int) = ROW(t4.x::int, t4.cv::int)
              and t4.al::int = 0::int
            )
          )
            left join on (
              ROW(t2.bt::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
            )
              left join on (
                (
                  ROW(t0.cv::int, t0.ag::int) = ROW(t2.cv::int, t2.bs::int)
                  and ROW(t0.cr::int, t2.al::int) = ROW(0::int, 0::int)
                )
              )
                join on (t1.cw::int = t0.cw::int)
                  scan m -> t0
                  scan f -> t1
                scan w -> t2
              scan w -> t3
            scan w -> t4
          scan l -> t5
        scan k -> t6
subquery $1:
  projection (
    coalesce(t6.bj::int::int, t0.bj::int::int)::int -> bj,
    t5.bm::int -> c1,
    coalesce(t5.cu::int::int, t0.cu::int::int)::int -> cu,
    case 
      when not t5.co::int is null then 0::int
      when not t5.cm::int is null then 1::int
      else t0.ad::int
    end -> ad,
    coalesce(
      t5.co::int::decimal,
      t5.cm::int::decimal,
      t0.ac::decimal::decimal
    )::decimal -> ac
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and t0.aw::int = 1::int
        and t0.w::datetime = '2024-02-28 0:00:00.0 +00:00:00'::datetime
        and t1.z::int = 11::int
        and t0.cr::int = 0::int
        and not (not t0.h::int is null)
        and t1.cj::int < 0::int
      )
    )
      left join on (t6.bi::text = t5.bi::text)
        left join on (
          ROW(t3.bs::int, t3.cv::int) = ROW(t5.ag::int, t5.cv::int)
        )
          left join on (
            (
              ROW(t2.x::int, t2.cv::int) = ROW(t3.x::int, t3.cv::int)
              and t3.al::int <> 0::int
              and t3.db::int is null
              and t3.da::int is null
            )
          )
            left join on (
              (
                ROW(t0.ag::int, t0.cv::int) = ROW(t2.bs::int, t2.cv::int)
                and ROW(t0.cr::int, t2.al::int) = ROW(0::int, 0::int)
              )
            )
              join on (t1.cw::int = t0.cw::int)
                scan m -> t0
                scan f -> t1
              scan w -> t2
            scan w -> t3
          scan l -> t5
        scan k -> t6
subquery $2:
  projection (
    coalesce(t6.bj::int::int, t0.bj::int::int)::int -> bj,
    t5.bm::int -> c1,
    coalesce(t5.cu::int::int, t0.cu::int::int)::int -> cu,
    case 
      when not t5.co::int is null then 0::int
      when not t5.cm::int is null then 1::int
      else 2::int
    end -> ad,
    coalesce(
      t5.co::int::decimal,
      t5.cm::int::decimal,
      t0.ac::decimal::decimal
    )::decimal -> ac
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and t0.aw::int = 1::int
        and t0.w::datetime = '2024-02-28 0:00:00.0 +00:00:00'::datetime
        and t1.z::int = 11::int
        and t0.cr::int = 1::int
        and not (not t0.h::int is null)
        and t1.cj::int < 0::int
      )
    )
      left join on (t6.bi::text = t5.bi::text)
        left join on (
          ROW(t2.bs::int, t2.cv::int) = ROW(t5.ag::int, t5.cv::int)
        )
          left join on (
            (
              ROW(t0.ag::int, t0.cv::int) = ROW(t2.c::int, t2.cv::int)
              and t0.cr::int = 1::int
            )
          )
            join on (t1.cw::int = t0.cw::int)
              scan m -> t0
              scan f -> t1
            scan w -> t2
          scan l -> t5
        scan k -> t6
subquery $3:
  motion [policy: full, program: RemoveDuplicates]
    union
      motion [policy: local, program: [RemoveDuplicates, SerializeAsEmptyTable(true)]]
        union
          projection (
            cte0.bj::int -> bj,
            cte0.c1::int -> c1,
            cte0.cu::int -> cu,
            cte0.ad::int -> ad,
            cte0.ac::decimal -> ac
          )
            scan cte cte0($0)
          projection (
            cte1.bj::int -> bj,
            cte1.c1::int -> c1,
            cte1.cu::int -> cu,
            cte1.ad::int -> ad,
            cte1.ac::decimal -> ac
          )
            scan cte cte1($1)
      projection (
        cte2.bj::int -> bj,
        cte2.c1::int -> c1,
        cte2.cu::int -> cu,
        cte2.ad::int -> ad,
        cte2.ac::decimal -> ac
      )
        scan cte cte2($2)
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      coalesce ("t6"."bj"::int, "t0"."bj"::int) as "bj",
      "t5"."bm" as "c1",
      coalesce ("t5"."cu"::int, "t0"."cu"::int) as "cu",
      CASE
        WHEN not "t5"."co" is null THEN 0::int
        WHEN not "t5"."cm" is null THEN 1::int
        ELSE "t0"."ad"
      END as "ad",
      coalesce (
        "t5"."co"::decimal,
        "t5"."cm"::decimal,
        "t0"."ac"::decimal
      ) as "ac"
    FROM
      "m" as "t0"
      INNER JOIN "f" as "t1" ON "t1"."cw" = "t0"."cw"
      LEFT JOIN "w" as "t2" ON "t2"."bs" = "t0"."ag"
      and "t2"."al" = 0::int
      and "t0"."cr" = 0::int
      and "t2"."cv" = "t0"."cv"
      LEFT JOIN "w" as "t3" ON "t2"."bt" = "t3"."ag"
      and "t3"."cv" = "t2"."cv"
      LEFT JOIN "w" as "t4" ON "t3"."x" = "t4"."x"
      and "t4"."cv" = "t3"."cv"
      and "t4"."al" = 0::int
      LEFT JOIN "l" as "t5" ON "t4"."bs" = "t5"."ag"
      and "t5"."cv" = "t4"."cv"
      LEFT JOIN "k" as "t6" ON "t6"."bi" = "t5"."bi"
    WHERE
      "t0"."cv" = 3023424::int
      and "t0"."aw" = 1::int
      and "t0"."w" = '2024-02-28 0:00:00.0 +00:00:00'::datetime
      and "t1"."z" = 11::int
      and "t0"."cr" = 0::int
      and not (not "t0"."h" is null)
      and "t1"."cj" > 0::int
  ) as "cte0"
UNION
SELECT
  *
FROM
  (
    SELECT
      coalesce ("t6"."bj"::int, "t0"."bj"::int) as "bj",
      "t5"."bm" as "c1",
      coalesce ("t5"."cu"::int, "t0"."cu"::int) as "cu",
      CASE
        WHEN not "t5"."co" is null THEN 0::int
        WHEN not "t5"."cm" is null THEN 1::int
        ELSE "t0"."ad"
      END as "ad",
      coalesce (
        "t5"."co"::decimal,
        "t5"."cm"::decimal,
        "t0"."ac"::decimal
      ) as "ac"
    FROM
      "m" as "t0"
      INNER JOIN "f" as "t1" ON "t1"."cw" = "t0"."cw"
      LEFT JOIN "w" as "t2" ON "t2"."bs" = "t0"."ag"
      and "t2"."cv" = "t0"."cv"
      and "t2"."al" = 0::int
      and "t0"."cr" = 0::int
      LEFT JOIN "w" as "t3" ON "t2"."x" = "t3"."x"
      and "t3"."cv" = "t2"."cv"
      and "t3"."al" <> 0::int
      and "t3"."db" is null
      and "t3"."da" is null
      LEFT JOIN "l" as "t5" ON "t3"."bs" = "t5"."ag"
      and "t5"."cv" = "t3"."cv"
      LEFT JOIN "k" as "t6" ON "t6"."bi" = "t5"."bi"
    WHERE
      "t0"."cv" = 3023424::int
      and "t0"."aw" = 1::int
      and "t0"."w" = '2024-02-28 0:00:00.0 +00:00:00'::datetime
      and "t1"."z" = 11::int
      and "t0"."cr" = 0::int
      and not (not "t0"."h" is null)
      and "t1"."cj" < 0::int
  ) as "cte1"
UNION
SELECT
  *
FROM
  (
    SELECT
      coalesce ("t6"."bj"::int, "t0"."bj"::int) as "bj",
      "t5"."bm" as "c1",
      coalesce ("t5"."cu"::int, "t0"."cu"::int) as "cu",
      CASE
        WHEN not "t5"."co" is null THEN 0::int
        WHEN not "t5"."cm" is null THEN 1::int
        ELSE 2::int
      END as "ad",
      coalesce (
        "t5"."co"::decimal,
        "t5"."cm"::decimal,
        "t0"."ac"::decimal
      ) as "ac"
    FROM
      "m" as "t0"
      INNER JOIN "f" as "t1" ON "t1"."cw" = "t0"."cw"
      LEFT JOIN "w" as "t2" ON "t2"."c" = "t0"."ag"
      and "t2"."cv" = "t0"."cv"
      and "t0"."cr" = 1::int
      LEFT JOIN "l" as "t5" ON "t5"."ag" = "t2"."bs"
      and "t5"."cv" = "t2"."cv"
      LEFT JOIN "k" as "t6" ON "t6"."bi" = "t5"."bi"
    WHERE
      "t0"."cv" = 3023424::int
      and "t0"."aw" = 1::int
      and "t0"."w" = '2024-02-28 0:00:00.0 +00:00:00'::datetime
      and "t1"."z" = 11::int
      and "t0"."cr" = 1::int
      and not (not "t0"."h" is null)
      and "t1"."cj" < 0::int
  ) as "cte2"
''
plan:
    [2] SEARCH TABLE m AS t0 
         USING COVERING INDEX ix_ak 
         (cv=? AND aw=? AND w=?) 
         (~7 rows)
        [2] SEARCH TABLE f AS t1 USING PRIMARY KEY (cw=?) (~1 row)
            [2] SEARCH TABLE w AS t2 
                 USING COVERING INDEX ix_bq 
                 (bs=? AND cv=? AND al=?) 
                 (~7 rows)
                [2] SEARCH TABLE w AS t3 USING COVERING INDEX ix_bt (cv=? AND ag=?) (~1 row)
                    [2] SEARCH TABLE w AS t4 USING COVERING INDEX ix_g (x=? AND cv=? AND al=?) (~8 rows)
                        [2] SEARCH TABLE l AS t5 USING COVERING INDEX ix_d (cv=? AND ag=?) (~1 row)
                            [2] SEARCH TABLE k AS t6 USING PRIMARY KEY (bi=?) (~1 row)
    [3] SEARCH TABLE m AS t0 
         USING COVERING INDEX ix_ak 
         (cv=? AND aw=? AND w=?) 
         (~7 rows)
        [3] SEARCH TABLE f AS t1 USING PRIMARY KEY (cw=?) (~1 row)
            [3] SEARCH TABLE w AS t2 
                 USING COVERING INDEX ix_bq 
                 (bs=? AND cv=? AND al=?) 
                 (~7 rows)
                [3] SEARCH TABLE w AS t3 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                    [3] SEARCH TABLE l AS t5 USING COVERING INDEX ix_d (cv=? AND ag=?) (~1 row)
                        [3] SEARCH TABLE k AS t6 USING PRIMARY KEY (bi=?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 USING TEMP B-TREE (UNION)
    [4] SEARCH TABLE m AS t0 
         USING COVERING INDEX ix_ak 
         (cv=? AND aw=? AND w=?) 
         (~7 rows)
        [4] SEARCH TABLE f AS t1 USING PRIMARY KEY (cw=?) (~1 row)
            [4] SEARCH TABLE w AS t2 USING COVERING INDEX ix_br (c=? AND cv=?) (~8 rows)
                [4] SEARCH TABLE l AS t5 USING COVERING INDEX ix_d (cv=? AND ag=?) (~1 row)
                    [4] SEARCH TABLE k AS t6 USING PRIMARY KEY (bi=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 4 USING TEMP B-TREE (UNION)
''
buckets = [1222]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4"
    FROM
      "_tmp_6220259963157982208_1136"
  ) as "cte3"
''
plan:
    [0] SCAN TABLE _tmp_6220259963157982208_1136 (~1048576 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q19
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  c1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  sum(
    case 
      when t0.aa::decimal > 0::int then t0.aa::decimal
      else 0::int
    end::decimal
  )::decimal -> c2,
  sum(
    case 
      when t0.aa::decimal < 0::int then t0.aa::decimal
      else 0::int
    end::decimal
  )::decimal -> c3,
  NULL::unknown -> c4,
  t0.z::int -> c0,
  t0.c1::bool -> c1
)
  group by (t0.z::int, t0.c1::bool)
    scan cte t0($0)
subquery $0:
  projection (
    t0.aa::decimal -> aa,
    t0.z::int -> z,
    case 
      when not t0.h::int is null then true::bool
      else false::bool
    end -> c1
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and t0.w::datetime = '2024-02-28 0:00:00.0 +00:00:00'::datetime
        and t0.aw::int = 1::int
        and t0.aa::decimal <> 0::int
      )
    )
      scan m -> t0
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  sum (
    CASE
      WHEN "t0"."aa" > 0::int THEN "t0"."aa"
      ELSE 0::int
    END::decimal
  ) as "c2",
  sum (
    CASE
      WHEN "t0"."aa" < 0::int THEN "t0"."aa"
      ELSE 0::int
    END::decimal
  ) as "c3",
  NULL as "c4",
  "t0"."z" as "c0",
  "t0"."c1"
FROM
  (
    SELECT
      "t0"."aa",
      "t0"."z",
      CASE
        WHEN not "t0"."h" is null THEN true::bool
        ELSE false::bool
      END as "c1"
    FROM
      "m" as "t0"
    WHERE
      "t0"."cv" = 3023424::int
      and "t0"."w" = '2024-02-28 0:00:00.0 +00:00:00'::datetime
      and "t0"."aw" = 1::int
      and "t0"."aa" <> 0::int
  ) as "t0"
GROUP BY
  "t0"."z",
  "t0"."c1"
''
plan:
    [0] SEARCH TABLE m AS t0 
         USING COVERING INDEX ix_ak 
         (cv=? AND aw=? AND w=?) 
         (~7 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q20
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  2;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 2
  projection (
    ag::int,
    cv::int,
    v::int,
    bx::decimal,
    bz::decimal,
    ca::decimal,
    cc::decimal,
    cd::decimal,
    c0::datetime,
    cg::decimal,
    ce::decimal,
    i::datetime,
    cf::decimal
  )
    order by (c0::datetime desc)
      scan
        projection (
          t0.ag::int -> ag,
          t0.cv::int -> cv,
          t0.v::int -> v,
          t0.bx::decimal -> bx,
          t0.bz::decimal -> bz,
          t0.ca::decimal -> ca,
          t0.cc::decimal -> cc,
          t0.cd::decimal -> cd,
          t0.ar::datetime -> c0,
          t0.cg::decimal -> cg,
          t0.ce::decimal -> ce,
          t0.i::datetime -> i,
          t0.cf::decimal -> cf
        )
          selection (
            (
              t0.cv::int = 3023424::int
              and t0.ar::datetime <= '2024-02-28 0:00:00.0 +00:00:00'::datetime
              and not coalesce(t0.aq::bool::bool, false::bool)::bool
            )
          )
            left join on (
              ROW(t0.v::int, t0.cv::int) = ROW(t1.ag::int, t1.cv::int)
            )
              scan u -> t0
              scan s -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "ag",
  "cv",
  "v",
  "bx",
  "bz",
  "ca",
  "cc",
  "cd",
  "c0",
  "cg",
  "ce",
  "i",
  "cf"
FROM
  (
    SELECT
      "t0"."ag",
      "t0"."cv",
      "t0"."v",
      "t0"."bx",
      "t0"."bz",
      "t0"."ca",
      "t0"."cc",
      "t0"."cd",
      "t0"."ar" as "c0",
      "t0"."cg",
      "t0"."ce",
      "t0"."i",
      "t0"."cf"
    FROM
      "u" as "t0"
      LEFT JOIN "s" as "t1" ON "t0"."v" = "t1"."ag"
      and "t0"."cv" = "t1"."cv"
    WHERE
      "t0"."cv" = 3023424::int
      and "t0"."ar" <= '2024-02-28 0:00:00.0 +00:00:00'::datetime
      and not coalesce ("t0"."aq"::bool, false::bool)
  )
ORDER BY
  "c0" DESC
LIMIT
  2
''
plan:
    [0] SEARCH TABLE u AS t0 USING COVERING INDEX ix_bi (cv=? AND ar<?) (~2 rows)
        [0] SEARCH TABLE s AS t1 USING PRIMARY KEY (ag=?) (~1 row)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q21
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  10;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 10
  projection (
    cte1.c0::text -> c0,
    cte1.c1::decimal -> c1,
    cte1.c2::decimal -> c2,
    cte1.c3::int -> c3,
    cte1.c4::int -> c4
  )
    selection (cte1.c4::int > 0::int)
      scan cte cte1($1)
subquery $0:
  projection (
    to_char(t0.w::datetime::datetime, '%Y-%m-%d'::text)::text::text -> c0,
    t0.aa::decimal -> aa
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and t0.w::datetime >= '2023-01-01 0:00:00.0 +00:00:00'::datetime
        and t0.w::datetime <= '2023-05-01 0:00:00.0 +00:00:00'::datetime
        and t0.aw::int = 1::int
        and t0.aa::decimal <> 0::int
      )
    )
      scan m -> t0
subquery $1:
  projection (
    c0::text,
    c1::decimal,
    c2::decimal,
    c3::int,
    c4::int
  )
    order by (c0::text desc)
      scan
        projection (
          cte0.c0::text -> c0,
          sum(
            case 
              when cte0.aa::decimal > 0::int then cte0.aa::decimal
              else 0::int
            end::decimal
          )::decimal -> c1,
          sum(
            case 
              when cte0.aa::decimal < 0::int then cte0.aa::decimal
              else 0::int
            end::decimal
          )::decimal -> c2,
          0::int -> c3,
          row_number() over (order by (cte0.c0::text desc) ) -> c4
        )
          group by (cte0.c0::text)
            motion [policy: full, program: ReshardIfNeeded]
              projection (cte0.c0::text -> c0, cte0.aa::decimal -> aa)
                scan cte cte0($0)
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "cte0"."c0",
  "cte0"."aa"
FROM
  (
    SELECT
      "to_char" ("t0"."w"::datetime, '%Y-%m-%d'::text)::text as "c0",
      "t0"."aa"
    FROM
      "m" as "t0"
    WHERE
      "t0"."cv" = 3023424::int
      and "t0"."w" >= '2023-01-01 0:00:00.0 +00:00:00'::datetime
      and "t0"."w" <= '2023-05-01 0:00:00.0 +00:00:00'::datetime
      and "t0"."aw" = 1::int
      and "t0"."aa" <> 0::int
  ) as "cte0"
''
plan:
    [0] SEARCH TABLE m AS t0 
         USING COVERING INDEX ix_ak 
         (cv=? AND aw=? AND w>? AND w<?) 
         (~1 row)
''
buckets = [1222]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "c0",
      "c1",
      "c2",
      "c3",
      "c4"
    FROM
      (
        SELECT
          "cte0"."COL_0" as "c0",
          sum (
            CASE
              WHEN "cte0"."COL_1" > 0::int THEN "cte0"."COL_1"
              ELSE 0::int
            END::decimal
          ) as "c1",
          sum (
            CASE
              WHEN "cte0"."COL_1" < 0::int THEN "cte0"."COL_1"
              ELSE 0::int
            END::decimal
          ) as "c2",
          0::int as "c3",
          row_number () OVER (
            ORDER BY
              "cte0"."COL_0" DESC) as "c4"
        FROM
          (
            SELECT
              "COL_0",
              "COL_1"
            FROM
              "_tmp_3708030572845756690_1136"
          ) as "cte0"
        GROUP BY
          "cte0"."COL_0"
      )
    ORDER BY
      "c0" DESC
  ) as "cte1"
WHERE
  "cte1"."c4" > 0::int
LIMIT
  10
''
plan:
    [2] SCAN TABLE _tmp_3708030572845756690_1136 (~1048576 rows)
    [2] USE TEMP B-TREE FOR GROUP BY
    [2] USE TEMP B-TREE FOR ORDER BY
    [1] SCAN SUBQUERY 2 (~1 row)
    [0] SCAN SUBQUERY 1 (~1 row)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q22
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  cte1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (count(*)::int -> c4)
  scan cte cte1($1)
subquery $0:
  projection (
    to_char(t0.w::datetime::datetime, '%Y-%m-%d'::text)::text::text -> c0,
    t0.aa::decimal -> aa
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and t0.w::datetime >= '2023-01-01 0:00:00.0 +00:00:00'::datetime
        and t0.w::datetime <= '2023-05-01 0:00:00.0 +00:00:00'::datetime
        and t0.aw::int = 1::int
        and t0.aa::decimal <> 0::int
      )
    )
      scan m -> t0
subquery $1:
  projection (
    cte0.c0::text -> c0,
    sum(
      case 
        when cte0.aa::decimal > 0::int then cte0.aa::decimal
        else 0::int
      end::decimal
    )::decimal -> c1,
    sum(
      case 
        when cte0.aa::decimal < 0::int then cte0.aa::decimal
        else 0::int
      end::decimal
    )::decimal -> c2,
    0::int -> c3
  )
    group by (cte0.c0::text)
      scan cte cte0($0)
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  count (*) as "c4"
FROM
  (
    SELECT
      "cte0"."c0",
      sum (
        CASE
          WHEN "cte0"."aa" > 0::int THEN "cte0"."aa"
          ELSE 0::int
        END::decimal
      ) as "c1",
      sum (
        CASE
          WHEN "cte0"."aa" < 0::int THEN "cte0"."aa"
          ELSE 0::int
        END::decimal
      ) as "c2",
      0::int as "c3"
    FROM
      (
        SELECT
          "to_char" ("t0"."w"::datetime, '%Y-%m-%d'::text)::text as "c0",
          "t0"."aa"
        FROM
          "m" as "t0"
        WHERE
          "t0"."cv" = 3023424::int
          and "t0"."w" >= '2023-01-01 0:00:00.0 +00:00:00'::datetime
          and "t0"."w" <= '2023-05-01 0:00:00.0 +00:00:00'::datetime
          and "t0"."aw" = 1::int
          and "t0"."aa" <> 0::int
      ) as "cte0"
    GROUP BY
      "cte0"."c0"
  ) as "cte1"
''
plan:
    [1] SEARCH TABLE m AS t0 
         USING COVERING INDEX ix_ak 
         (cv=? AND aw=? AND w>? AND w<?) 
         (~1 row)
    [1] USE TEMP B-TREE FOR GROUP BY
    [0] SCAN SUBQUERY 1 AS cte1 (~1 row)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q23
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
motion [policy: full, program: RemoveDuplicates]
  union
    projection (
      cte0.ag::int -> ag,
      cte0.v::int -> v,
      cte0.bx::decimal -> bx,
      cte0.bz::decimal -> bz,
      cte0.ca::decimal -> ca,
      cte0.cc::decimal -> cc,
      cte0.cd::decimal -> cd,
      cte0.ar::datetime -> ar,
      cte0.cg::decimal -> cg,
      cte0.ce::decimal -> ce,
      cte0.i::datetime -> i,
      cte0.cf::decimal -> cf,
      cte0.cv::int -> cv,
      cte0.aq::bool -> aq
    )
      scan cte cte0($0)
    projection (
      t0.ag::int -> ag,
      t0.v::int -> v,
      t0.bx::decimal -> bx,
      t0.bz::decimal -> bz,
      t0.ca::decimal -> ca,
      t0.cc::decimal -> cc,
      t0.cd::decimal -> cd,
      t0.ar::datetime -> ar,
      t0.cg::decimal -> cg,
      t0.ce::decimal -> ce,
      t0.i::datetime -> i,
      t0.cf::decimal -> cf,
      t0.cv::int -> cv,
      t0.aq::bool -> aq
    )
      selection (
        (
          t0.cv::int = 3023424::int
          and not coalesce(t0.aq::bool::bool, false::bool)::bool
          and to_char(t0.ar::datetime::datetime, '%Y-%m-%d'::text)::text::datetime > '2020-01-02 0:00:00.0 +00:00:00'::datetime
          and t0.ar::datetime >= '2023-01-01 0:00:00.0 +00:00:00'::datetime
          and t0.ar::datetime <= '2023-05-01 0:00:00.0 +00:00:00'::datetime
        )
      )
        left join on (
          ROW(t0.v::int, t0.cv::int) = ROW(t1.ag::int, t1.cv::int)
        )
          scan u -> t0
          scan s -> t1
subquery $0:
  limit 1
    projection (
      ag::int,
      v::int,
      bx::decimal,
      bz::decimal,
      ca::decimal,
      cc::decimal,
      cd::decimal,
      ar::datetime,
      cg::decimal,
      ce::decimal,
      i::datetime,
      cf::decimal,
      cv::int,
      aq::bool
    )
      order by (ar::datetime desc)
        scan
          projection (
            t0.ag::int -> ag,
            t0.v::int -> v,
            t0.bx::decimal -> bx,
            t0.bz::decimal -> bz,
            t0.ca::decimal -> ca,
            t0.cc::decimal -> cc,
            t0.cd::decimal -> cd,
            t0.ar::datetime -> ar,
            t0.cg::decimal -> cg,
            t0.ce::decimal -> ce,
            t0.i::datetime -> i,
            t0.cf::decimal -> cf,
            t0.cv::int -> cv,
            t0.aq::bool -> aq
          )
            selection (
              (
                t0.cv::int = 3023424::int
                and not coalesce(t0.aq::bool::bool, false::bool)::bool
                and to_char(t0.ar::datetime::datetime, '%Y-%m-%d'::text)::text::datetime > '2020-01-02 0:00:00.0 +00:00:00'::datetime
                and t0.ar::datetime < '2023-01-01 0:00:00.0 +00:00:00'::datetime
              )
            )
              left join on (
                ROW(t0.v::int, t0.cv::int) = ROW(t1.ag::int, t1.cv::int)
              )
                scan u -> t0
                scan s -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "ag",
      "v",
      "bx",
      "bz",
      "ca",
      "cc",
      "cd",
      "ar",
      "cg",
      "ce",
      "i",
      "cf",
      "cv",
      "aq"
    FROM
      (
        SELECT
          "t0"."ag",
          "t0"."v",
          "t0"."bx",
          "t0"."bz",
          "t0"."ca",
          "t0"."cc",
          "t0"."cd",
          "t0"."ar",
          "t0"."cg",
          "t0"."ce",
          "t0"."i",
          "t0"."cf",
          "t0"."cv",
          "t0"."aq"
        FROM
          "u" as "t0"
          LEFT JOIN "s" as "t1" ON "t0"."v" = "t1"."ag"
          and "t0"."cv" = "t1"."cv"
        WHERE
          "t0"."cv" = 3023424::int
          and not coalesce ("t0"."aq"::bool, false::bool)
          and "to_char" ("t0"."ar"::datetime, '%Y-%m-%d'::text)::datetime > '2020-01-02 0:00:00.0 +00:00:00'::datetime
          and "t0"."ar" < '2023-01-01 0:00:00.0 +00:00:00'::datetime
      )
    ORDER BY
      "ar" DESC
    LIMIT
      1
  ) as "cte0"
UNION
SELECT
  "t0"."ag",
  "t0"."v",
  "t0"."bx",
  "t0"."bz",
  "t0"."ca",
  "t0"."cc",
  "t0"."cd",
  "t0"."ar",
  "t0"."cg",
  "t0"."ce",
  "t0"."i",
  "t0"."cf",
  "t0"."cv",
  "t0"."aq"
FROM
  "u" as "t0"
  LEFT JOIN "s" as "t1" ON "t0"."v" = "t1"."ag"
  and "t0"."cv" = "t1"."cv"
WHERE
  "t0"."cv" = 3023424::int
  and not coalesce ("t0"."aq"::bool, false::bool)
  and "to_char" ("t0"."ar"::datetime, '%Y-%m-%d'::text)::datetime > '2020-01-02 0:00:00.0 +00:00:00'::datetime
  and "t0"."ar" >= '2023-01-01 0:00:00.0 +00:00:00'::datetime
  and "t0"."ar" <= '2023-05-01 0:00:00.0 +00:00:00'::datetime
''
plan:
    [2] SEARCH TABLE u AS t0 USING COVERING INDEX ix_bi (cv=? AND ar<?) (~2 rows)
        [2] SEARCH TABLE s AS t1 USING PRIMARY KEY (ag=?) (~1 row)
    [1] SCAN SUBQUERY 2 AS cte0 (~1 row)
    [3] SEARCH TABLE u AS t0 
         USING COVERING INDEX ix_bi 
         (cv=? AND ar>? AND ar<?) 
         (~1 row)
        [3] SEARCH TABLE s AS t1 USING PRIMARY KEY (ag=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 3 USING TEMP B-TREE (UNION)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q24
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  10;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 10
  projection (
    cte1.cu::int -> cu,
    cte1.a::text -> a,
    cte1.d::decimal -> d,
    cte1.c3::int -> c3
  )
    selection (cte1.c3::int > 0::int)
      scan cte cte1($1)
subquery $0:
  projection (
    t0.cu::int -> cu,
    t0.d::decimal -> d,
    to_char(
      coalesce(
        t0.cs::datetime::datetime,
        t0.a::datetime::datetime
      )::datetime::datetime,
      '%Y-%m-%d'::text
    )::text::text -> a
  )
    selection (
      (
        t1.ag::int is null
        and t0.cv::int = 4797271::int
        and t0.cw::int = 1415::int
        and coalesce(
          t0.cs::datetime::datetime,
          t0.a::datetime::datetime
        )::datetime >= '2010-01-01 0:00:00.0 +00:00:00'::datetime
        and coalesce(
          t0.cs::datetime::datetime,
          t0.a::datetime::datetime
        )::datetime <= '2025-10-20 0:00:00.0 +00:00:00'::datetime
        and t0.cu::int = 1333::int
        and t0.h::int is null
        and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
      )
    )
      left join on (
        ROW(t0.ag::int, t0.cv::int) = ROW(t1.h::int, t1.cv::int)
      )
        scan l -> t0
        scan l -> t1
subquery $1:
  projection (cu::int, a::text, d::decimal, c3::int)
    order by (a::text desc)
      scan
        projection (
          cte0.cu::int -> cu,
          cte0.a::text -> a,
          sum(cte0.d::decimal::decimal)::decimal -> d,
          row_number() over (order by (cte0.a::text desc) ) -> c3
        )
          group by (cte0.cu::int, cte0.a::text)
            motion [policy: full, program: ReshardIfNeeded]
              projection (
                cte0.cu::int -> cu,
                cte0.d::decimal -> d,
                cte0.a::text -> a
              )
                scan cte cte0($0)
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "cte0"."cu",
  "cte0"."d",
  "cte0"."a"
FROM
  (
    SELECT
      "t0"."cu",
      "t0"."d",
      "to_char" (
        coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
        '%Y-%m-%d'::text
      )::text as "a"
    FROM
      "l" as "t0"
      LEFT JOIN "l" as "t1" ON "t1"."h" = "t0"."ag"
      and "t1"."cv" = "t0"."cv"
    WHERE
      "t1"."ag" is null
      and "t0"."cv" = 4797271::int
      and "t0"."cw" = 1415::int
      and coalesce ("t0"."cs"::datetime, "t0"."a"::datetime) >= '2010-01-01 0:00:00.0 +00:00:00'::datetime
      and coalesce ("t0"."cs"::datetime, "t0"."a"::datetime) <= '2025-10-20 0:00:00.0 +00:00:00'::datetime
      and "t0"."cu" = 1333::int
      and "t0"."h" is null
      and coalesce ("t0"."bb"::int, 0::int) <> 1::int
  ) as "cte0"
''
plan:
    [0] SEARCH TABLE l AS t0 USING COVERING INDEX ix_ab (cv=? AND cw=?) (~6 rows)
        [0] SEARCH TABLE l AS t1 USING COVERING INDEX ix_v (cv=?) (~8 rows)
''
buckets = [874]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "cu",
      "a",
      "d",
      "c3"
    FROM
      (
        SELECT
          "cte0"."COL_0" as "cu",
          "cte0"."COL_2" as "a",
          sum ("cte0"."COL_1"::decimal) as "d",
          row_number () OVER (
            ORDER BY
              "cte0"."COL_2" DESC) as "c3"
        FROM
          (
            SELECT
              "COL_0",
              "COL_1",
              "COL_2"
            FROM
              "_tmp_12018535486545688128_1136"
          ) as "cte0"
        GROUP BY
          "cte0"."COL_0",
          "cte0"."COL_2"
      )
    ORDER BY
      "a" DESC
  ) as "cte1"
WHERE
  "cte1"."c3" > 0::int
LIMIT
  10
''
plan:
    [2] SCAN TABLE _tmp_12018535486545688128_1136 (~1048576 rows)
    [2] USE TEMP B-TREE FOR GROUP BY
    [2] USE TEMP B-TREE FOR ORDER BY
    [1] SCAN SUBQUERY 2 (~1 row)
    [0] SCAN SUBQUERY 1 (~1 row)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [874]

-- TEST: corpus-explain-q25
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  10;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 10
  projection (
    t2.b::int -> b,
    t2.cw::int -> cw,
    t2.cu::int -> cu,
    t2.ac::int -> ac,
    t2.ae::text -> ae,
    t2.ab::text -> ab,
    t2.ad::int -> ad,
    t2.d::decimal -> d,
    t2.c8::int -> c8,
    t2.c9::int -> c9
  )
    selection (t2.c9::int > 0::int)
      scan t2
        projection (
          b::int,
          cw::int,
          cu::int,
          ac::int,
          ae::text,
          ab::text,
          ad::int,
          d::decimal,
          c8::int,
          c9::int
        )
          order by (b::int desc)
            scan
              projection (
                ag::int -> b,
                cw::int,
                cu::int,
                coalesce(co::int::int, cm::int::int, 0::int)::int -> ac,
                case 
                  when not co::int is null or not cm::int is null then cn::text
                  else '0000'::text
                end -> ae,
                to_char(cl::datetime::datetime, '%Y-%m-%d'::text)::text -> ab,
                case 
                  when not co::int is null then 0::int
                  when not cm::int is null then 1::int
                  else 2::int
                end -> ad,
                d::decimal,
                0::int -> c8,
                row_number() over (order by (ag::int desc) ) -> c9
              )
                motion [policy: full, program: ReshardIfNeeded]
                  projection (
                    t0.ag::int -> ag,
                    t0.cw::int -> cw,
                    t0.cu::int -> cu,
                    t0.d::decimal -> d,
                    t0.cm::int -> cm,
                    t0.cn::text -> cn,
                    t0.cl::datetime -> cl,
                    t0.co::int -> co
                  )
                    selection (
                      (
                        t1.ag::int is null
                        and t0.cv::int = 8397725::int
                        and t0.cw::int = 1415::int
                        and coalesce(
                          t0.cs::datetime::datetime,
                          t0.a::datetime::datetime
                        )::datetime = '2024-07-30 0:00:00.0 +00:00:00'::datetime
                        and t0.cu::int = 1333::int
                        and t0.h::int is null
                        and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
                      )
                    )
                      left join on (
                        (
                          ROW(t0.cv::int, t0.ag::int) = ROW(t1.cv::int, t1.h::int)
                          and t1.cv::int = 8397725::int
                        )
                      )
                        scan l -> t0
                        scan l -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ag",
  "t0"."cw",
  "t0"."cu",
  "t0"."d",
  "t0"."cm",
  "t0"."cn",
  "t0"."cl",
  "t0"."co"
FROM
  "l" as "t0"
  LEFT JOIN "l" as "t1" ON "t1"."h" = "t0"."ag"
  and "t1"."cv" = 8397725::int
  and "t1"."cv" = "t0"."cv"
WHERE
  "t1"."ag" is null
  and "t0"."cv" = 8397725::int
  and "t0"."cw" = 1415::int
  and coalesce ("t0"."cs"::datetime, "t0"."a"::datetime) = '2024-07-30 0:00:00.0 +00:00:00'::datetime
  and "t0"."cu" = 1333::int
  and "t0"."h" is null
  and coalesce ("t0"."bb"::int, 0::int) <> 1::int
''
plan:
    [0] SEARCH TABLE l AS t0 USING COVERING INDEX ix_ab (cv=? AND cw=?) (~6 rows)
        [0] SEARCH TABLE l AS t1 USING COVERING INDEX ix_v (cv=?) (~8 rows)
''
buckets = [2739]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "b",
      "cw",
      "cu",
      "ac",
      "ae",
      "ab",
      "ad",
      "d",
      "c8",
      "c9"
    FROM
      (
        SELECT
          "COL_0" as "b",
          "COL_1" as "cw",
          "COL_2" as "cu",
          coalesce ("COL_7"::int, "COL_4"::int, 0::int) as "ac",
          CASE
            WHEN not "COL_7" is null
            or not "COL_4" is null THEN "COL_5"
            ELSE '0000'::text
          END as "ae",
          "to_char" ("COL_6"::datetime, '%Y-%m-%d'::text) as "ab",
          CASE
            WHEN not "COL_7" is null THEN 0::int
            WHEN not "COL_4" is null THEN 1::int
            ELSE 2::int
          END as "ad",
          "COL_3" as "d",
          0::int as "c8",
          row_number () OVER (
            ORDER BY
              "COL_0" DESC) as "c9"
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
              "_tmp_10320311687366327512_1136"
          )
      )
    ORDER BY
      "b" DESC
  ) as "t2"
WHERE
  "t2"."c9" > 0::int
LIMIT
  10
''
plan:
    [2] SCAN TABLE _tmp_10320311687366327512_1136 (~1048576 rows)
    [2] USE TEMP B-TREE FOR ORDER BY
    [1] SCAN SUBQUERY 2 (~1 row)
    [0] SCAN SUBQUERY 1 (~1 row)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [2739]

-- TEST: corpus-explain-q26
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  ) AS t3;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  count(*)::int -> c0,
  sum(t3.d::decimal::decimal)::decimal -> c1
)
  scan t3
    projection (
      t2.b::int -> b,
      t2.cw::int -> cw,
      t2.cu::int -> cu,
      t2.ac::int -> ac,
      t2.ae::text -> ae,
      t2.ab::text -> ab,
      t2.ad::int -> ad,
      t2.d::decimal -> d,
      t2.c10::int -> c10,
      t2.c11::int -> c11
    )
      scan t2
        projection (
          b::int,
          cw::int,
          cu::int,
          ac::int,
          ae::text,
          ab::text,
          ad::int,
          d::decimal,
          c10::int,
          c11::int
        )
          order by (b::int desc)
            scan
              projection (
                ag::int -> b,
                cw::int,
                cu::int,
                coalesce(co::int::int, cm::int::int, 0::int)::int -> ac,
                case 
                  when not co::int is null or not cm::int is null then cn::text
                  else '0000'::text
                end -> ae,
                to_char(cl::datetime::datetime, '%Y-%m-%d'::text)::text -> ab,
                case 
                  when not co::int is null then 0::int
                  when not cm::int is null then 1::int
                  else 2::int
                end -> ad,
                d::decimal,
                0::int -> c10,
                row_number() over (order by (ag::int desc) ) -> c11
              )
                motion [policy: full, program: ReshardIfNeeded]
                  projection (
                    t0.ag::int -> ag,
                    t0.cw::int -> cw,
                    t0.cu::int -> cu,
                    t0.d::decimal -> d,
                    t0.cm::int -> cm,
                    t0.cn::text -> cn,
                    t0.cl::datetime -> cl,
                    t0.co::int -> co
                  )
                    selection (
                      (
                        t1.ag::int is null
                        and t0.cv::int = 8397725::int
                        and t0.cw::int = 1415::int
                        and coalesce(
                          t0.cs::datetime::datetime,
                          t0.a::datetime::datetime
                        )::datetime = '2024-07-30 0:00:00.0 +00:00:00'::datetime
                        and t0.cu::int = 1333::int
                        and t0.h::int is null
                        and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
                      )
                    )
                      left join on (
                        (
                          ROW(t0.cv::int, t0.ag::int) = ROW(t1.cv::int, t1.h::int)
                          and t1.cv::int = 8397725::int
                        )
                      )
                        scan l -> t0
                        scan l -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ag",
  "t0"."cw",
  "t0"."cu",
  "t0"."d",
  "t0"."cm",
  "t0"."cn",
  "t0"."cl",
  "t0"."co"
FROM
  "l" as "t0"
  LEFT JOIN "l" as "t1" ON "t1"."h" = "t0"."ag"
  and "t1"."cv" = 8397725::int
  and "t1"."cv" = "t0"."cv"
WHERE
  "t1"."ag" is null
  and "t0"."cv" = 8397725::int
  and "t0"."cw" = 1415::int
  and coalesce ("t0"."cs"::datetime, "t0"."a"::datetime) = '2024-07-30 0:00:00.0 +00:00:00'::datetime
  and "t0"."cu" = 1333::int
  and "t0"."h" is null
  and coalesce ("t0"."bb"::int, 0::int) <> 1::int
''
plan:
    [0] SEARCH TABLE l AS t0 USING COVERING INDEX ix_ab (cv=? AND cw=?) (~6 rows)
        [0] SEARCH TABLE l AS t1 USING COVERING INDEX ix_v (cv=?) (~8 rows)
''
buckets = [2739]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  count (*) as "c0",
  sum ("t3"."d"::decimal) as "c1"
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          "b",
          "cw",
          "cu",
          "ac",
          "ae",
          "ab",
          "ad",
          "d",
          "c10",
          "c11"
        FROM
          (
            SELECT
              "COL_0" as "b",
              "COL_1" as "cw",
              "COL_2" as "cu",
              coalesce ("COL_7"::int, "COL_4"::int, 0::int) as "ac",
              CASE
                WHEN not "COL_7" is null
                or not "COL_4" is null THEN "COL_5"
                ELSE '0000'::text
              END as "ae",
              "to_char" ("COL_6"::datetime, '%Y-%m-%d'::text) as "ab",
              CASE
                WHEN not "COL_7" is null THEN 0::int
                WHEN not "COL_4" is null THEN 1::int
                ELSE 2::int
              END as "ad",
              "COL_3" as "d",
              0::int as "c10",
              row_number () OVER (
                ORDER BY
                  "COL_0" DESC) as "c11"
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
                  "_tmp_11483270331600713565_1136"
              )
          )
        ORDER BY
          "b" DESC
      ) as "t2"
  ) as "t3"
''
plan:
    [3] SCAN TABLE _tmp_11483270331600713565_1136 (~1048576 rows)
    [3] USE TEMP B-TREE FOR ORDER BY
    [2] SCAN SUBQUERY 3 (~1 row)
    [1] SCAN SUBQUERY 2 (~1 row)
    [1] USE TEMP B-TREE FOR ORDER BY
    [0] SCAN SUBQUERY 1 AS t2 (~1 row)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [2739]

-- TEST: corpus-explain-q27
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t0.d::decimal -> c0,
  t0.cu::int -> c1,
  to_char(
    coalesce(
      t0.cs::datetime::datetime,
      t0.a::datetime::datetime
    )::datetime::datetime,
    '%Y-%m-%d'::text
  )::text -> c2
)
  selection (
    (
      t1.ag::int is null
      and t0.cv::int = 8397725::int
      and t0.cw::int = 1415::int
      and coalesce(
        t0.cs::datetime::datetime,
        t0.a::datetime::datetime
      )::datetime >= '2010-01-01 0:00:00.0 +00:00:00'::datetime
      and coalesce(
        t0.cs::datetime::datetime,
        t0.a::datetime::datetime
      )::datetime <= '2025-11-04 0:00:00.0 +00:00:00'::datetime
      and t0.h::int is null
      and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
    )
  )
    left join on (
      ROW(t0.ag::int, t0.cv::int) = ROW(t1.h::int, t1.cv::int)
    )
      scan l -> t0
      scan l -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."d" as "c0",
  "t0"."cu" as "c1",
  "to_char" (
    coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
    '%Y-%m-%d'::text
  ) as "c2"
FROM
  "l" as "t0"
  LEFT JOIN "l" as "t1" ON "t1"."h" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
WHERE
  "t1"."ag" is null
  and "t0"."cv" = 8397725::int
  and "t0"."cw" = 1415::int
  and coalesce ("t0"."cs"::datetime, "t0"."a"::datetime) >= '2010-01-01 0:00:00.0 +00:00:00'::datetime
  and coalesce ("t0"."cs"::datetime, "t0"."a"::datetime) <= '2025-11-04 0:00:00.0 +00:00:00'::datetime
  and "t0"."h" is null
  and coalesce ("t0"."bb"::int, 0::int) <> 1::int
''
plan:
    [0] SEARCH TABLE l AS t0 USING COVERING INDEX ix_ab (cv=? AND cw=?) (~6 rows)
        [0] SEARCH TABLE l AS t1 USING COVERING INDEX ix_v (cv=?) (~8 rows)
''
buckets = [2739]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [2739]

-- TEST: corpus-explain-q28
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  (t0.cv = 3023424);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t0.ce::decimal -> c3,
  cte1.c2::decimal -> c2,
  0::int -> c4,
  0::int -> c5
)
  selection (t0.cv::int = 3023424::int)
    left join on (cte1.v::int = t0.ag::int)
      scan s -> t0
      motion [policy: full, program: ReshardIfNeeded]
        scan cte cte1($1)
subquery $0:
  projection (
    t0.v::int -> v,
    case 
      when (
        t0.bx::decimal > 0::int
        and t1.cw::int = 3::int
      ) then t0.bx::decimal
      else 0::int
    end -> c1
  )
    selection (t0.cv::int = 3023424::int)
      left join on (t0.cu::int = t1.cu::int)
        scan y -> t0
        scan g -> t1
subquery $1:
  projection (
    cte0.v::int -> v,
    sum(cte0.c1::decimal::decimal)::decimal -> c2
  )
    group by (cte0.v::int)
      scan cte cte0($0)
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "cte0"."v",
  sum ("cte0"."c1"::decimal) as "c2"
FROM
  (
    SELECT
      "t0"."v",
      CASE
        WHEN "t0"."bx" > 0::int
        and "t1"."cw" = 3::int THEN "t0"."bx"
        ELSE 0::int
      END as "c1"
    FROM
      "y" as "t0"
      LEFT JOIN "g" as "t1" ON "t0"."cu" = "t1"."cu"
    WHERE
      "t0"."cv" = 3023424::int
  ) as "cte0"
GROUP BY
  "cte0"."v"
''
plan:
    [0] SEARCH TABLE y AS t0 USING COVERING INDEX ix_bv (cv=?) (~10 rows)
        [0] SEARCH TABLE g AS t1 USING PRIMARY KEY (cu=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1222]
''
╭────────────────────────────────────────╮
│ 2. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ce" as "c3",
  "cte1"."COL_1" as "c2",
  0::int as "c4",
  0::int as "c5"
FROM
  "s" as "t0"
  LEFT JOIN (
    SELECT
      "COL_0",
      "COL_1"
    FROM
      "_tmp_7837470984463317261_0136"
  ) as "cte1" ON "cte1"."COL_0" = "t0"."ag"
WHERE
  "t0"."cv" = 3023424::int
''
plan:
    [1] SCAN TABLE _tmp_7837470984463317261_0136 (~1048576 rows)
    [0] SEARCH TABLE s AS t0 USING COVERING INDEX ix_be (cv=?) (~10 rows)
        [0] SCAN SUBQUERY 1 AS cte1 (~1 row)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q29
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  10;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 10
  projection (
    cte4.cv::int -> cv,
    cte4.b::int -> b,
    cte4.ay::text -> ay,
    cte4.bg::text -> bg,
    cte4.cu::int -> cu,
    cte4.cw::int -> cw,
    cte4.ac::int -> ac,
    cte4.ae::text -> ae,
    cte4.ab::text -> ab,
    cte4.c9::int -> c9,
    cte4.d::decimal -> d,
    cte4.cx::text -> cx,
    cte4.c11::int -> c11
  )
    selection (cte4.c11::int > 0::int)
      scan cte cte4($4)
subquery $0:
  projection (ag::int, au::int, cv::int)
    order by (ag::int)
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (
            t0.ag::int -> ag,
            t0.au::int -> au,
            t0.cv::int -> cv
          )
            selection (t1.b::int is null)
              left join on (
                ROW(t0.ag::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
              )
                scan t0
                  union all
                    projection (
                      l.ag::int -> ag,
                      l.au::int -> au,
                      l.cv::int -> cv
                    )
                      selection (
                        (
                          l.cv::int = 4797271::int
                          and l.cs::datetime >= '2022-10-31 0:00:00.0 +00:00:00'::datetime
                          and l.cs::datetime < '2022-11-01 0:00:00.0 +00:00:00'::datetime
                          and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                        )
                      )
                        scan l
                    projection (
                      l.ag::int -> ag,
                      l.au::int -> au,
                      l.cv::int -> cv
                    )
                      selection (
                        (
                          l.cv::int = 4797271::int
                          and l.cs::datetime is null
                          and l.a::datetime >= '2022-10-31 0:00:00.0 +00:00:00'::datetime
                          and l.a::datetime < '2022-11-01 0:00:00.0 +00:00:00'::datetime
                          and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                        )
                      )
                        scan l
                scan x -> t1
subquery $1:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      t6.cv::int -> cv,
      t6.ag::int -> b,
      t3.ay::text -> ay,
      t3.bg::text -> bg,
      t6.cu::int -> cu,
      t6.cw::int -> cw,
      coalesce(t6.co::int::int, t6.cm::int::int, 0::int)::int -> ac,
      case 
        when not t6.co::int is null or not t6.cm::int is null then t6.cn::text
        else '0000'::text
      end -> ae,
      to_char(t6.cl::datetime::datetime, '%Y-%m-%d'::text)::text -> ab,
      case 
        when not t6.co::int is null then 0::int
        when not t6.cm::int is null then 1::int
        else 2::int
      end -> c9,
      t5.d::decimal -> d
    )
      selection (
        (
          t8.z::int = 7::int
          and t3.aw::int = 11::int
          and t3.cq::int = 6::int
        )
      )
        left join on (t8.cw::int = t6.cw::int)
          left join on (
            ROW(t6.ag::int, t6.cv::int) = ROW(t7.b::int, t7.cv::int)
          )
            join on (
              (
                ROW(t5.bs::int, t5.cv::int) = ROW(t6.ag::int, t6.cv::int)
                and coalesce(t6.bb::int::int, 0::int)::int <> 1::int
              )
            )
              join on (
                (
                  ROW(t4.ag::int, t4.cv::int) = ROW(t5.x::int, t5.cv::int)
                  and t5.db::int is null
                  and t5.da::int is null
                )
              )
                join on (
                  (
                    ROW(t2.ag::int, t3.cv::int) = ROW(t4.b::int, t4.cv::int)
                    and t4.ai::decimal < 0::int
                  )
                )
                  join on (
                    ROW(t2.au::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
                  )
                    motion [policy: segment([ref(cv)]), program: ReshardIfNeeded]
                      scan cte t2($0)
                    scan y -> t3 (indexed by ix_i)
                  scan v -> t4 (indexed by ix_f)
                scan w -> t5 (indexed by ix_g)
              scan l -> t6 (indexed by ix_d)
            scan x -> t7
          scan f -> t8
subquery $2:
  projection (
    t9.cv::int -> cv,
    t9.ag::int -> b,
    t9.ay::text -> ay,
    coalesce(t3.bg::text::text, t9.bg::text::text)::text -> bg,
    coalesce(t3.cu::int::int, t9.cu::int::int)::int -> cu,
    t9.cw::int -> cw,
    coalesce(t9.co::int::int, t9.cm::int::int, 0::int)::int -> ac,
    t9.cn::text -> ae,
    to_char(t9.cl::datetime::datetime, '%Y-%m-%d'::text)::text -> ab,
    case 
      when not t9.co::int is null then 0::int
      else 1::int
    end -> c9,
    coalesce(
      (
        t9.d::decimal * coalesce(t8.cj::int::decimal, 0.0::decimal)::decimal * case 
          when not t9.h::int is null then -1::int
          else 1::int
        end
      )::decimal,
      0.0::decimal
    )::decimal -> d
  )
    selection (
      (
        t9.cv::int = 4797271::int
        and coalesce(
          t9.cs::datetime::datetime,
          t9.a::datetime::datetime
        )::datetime = '2022-10-31 0:00:00.0 +00:00:00'::datetime
        and t8.z::int = 7::int
        and coalesce(t3.aw::int::int, t9.bf::int::int)::int = 11::int
        and coalesce(t3.cq::int::int, t11.cq::int::int)::int = 6::int
      )
    )
      left join on (t9.cu::int = t11.cu::int)
        left join on (
          ROW(t9.ag::int, t3.cv::int) = ROW(t10.h::int, t10.cv::int)
        )
          left join on (t9.cw::int = t8.cw::int)
            left join on (
              ROW(t9.au::int, t9.cv::int) = ROW(t3.ag::int, t3.cv::int)
            )
              scan n -> t9
              scan y -> t3 (indexed by ix_i)
            scan f -> t8
          scan n -> t10 (indexed by ix_e)
        scan h -> t11
subquery $3:
  motion [policy: full, program: ReshardIfNeeded]
    union all
      motion [policy: local, program: SerializeAsEmptyTable(true)]
        projection (
          cte1.cv::int -> cv,
          cte1.b::int -> b,
          cte1.ay::text -> ay,
          cte1.bg::text -> bg,
          cte1.cu::int -> cu,
          cte1.cw::int -> cw,
          cte1.ac::int -> ac,
          cte1.ae::text -> ae,
          cte1.ab::text -> ab,
          cte1.c9::int -> c9,
          cte1.d::decimal -> d
        )
          scan cte cte1($1)
      projection (
        cte2.cv::int -> cv,
        cte2.b::int -> b,
        cte2.ay::text -> ay,
        cte2.bg::text -> bg,
        cte2.cu::int -> cu,
        cte2.cw::int -> cw,
        cte2.ac::int -> ac,
        cte2.ae::text -> ae,
        cte2.ab::text -> ab,
        cte2.c9::int -> c9,
        cte2.d::decimal -> d
      )
        scan cte cte2($2)
subquery $4:
  projection (
    unnamed_join.cv::int -> cv,
    unnamed_join.b::int -> b,
    unnamed_join.ay::text -> ay,
    unnamed_join.bg::text -> bg,
    unnamed_join.cu::int -> cu,
    unnamed_join.cw::int -> cw,
    unnamed_join.ac::int -> ac,
    unnamed_join.ae::text -> ae,
    unnamed_join.ab::text -> ab,
    unnamed_join.c9::int -> c9,
    unnamed_join.d::decimal -> d,
    unnamed_join.cx::text -> cx,
    row_number() over (order by (unnamed_join.b::int desc) ) -> c11
  )
    motion [policy: full, program: AddMissingRowsForLeftJoin]
      projection (
        cte3.cv::int -> cv,
        cte3.b::int -> b,
        cte3.ay::text -> ay,
        cte3.bg::text -> bg,
        cte3.cu::int -> cu,
        cte3.cw::int -> cw,
        cte3.ac::int -> ac,
        cte3.ae::text -> ae,
        cte3.ab::text -> ab,
        cte3.c9::int -> c9,
        cte3.d::decimal -> d,
        t12.cv::int -> cv,
        t12.ay::text -> ay,
        t12.bucket_id::int -> bucket_id,
        t12.cx::text -> cx
      )
        join on (
          ROW(cte3.cv::int, cte3.ay::text) = ROW(t12.cv::int, t12.ay::text)
        )
          scan cte cte3($3)
          scan ad -> t12
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ag",
  "t0"."au",
  "t0"."cv"
FROM
  (
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" >= '2022-10-31 0:00:00.0 +00:00:00'::datetime
      and "l"."cs" < '2022-11-01 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
    UNION ALL
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" is null
      and "l"."a" >= '2022-10-31 0:00:00.0 +00:00:00'::datetime
      and "l"."a" < '2022-11-01 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
  ) as "t0"
  LEFT JOIN "x" as "t1" ON "t1"."b" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
WHERE
  "t1"."b" is null
''
plan:
    [2] SEARCH TABLE l USING COVERING INDEX ix_z (cv=? AND cs>? AND cs<?) (~1 row)
    [3] SEARCH TABLE l USING COVERING INDEX ix_aa (cv=? AND a>? AND a<?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t0 (~1 row)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
''
buckets = [874]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "COL_0" as "ag",
  "COL_1" as "au",
  "COL_2" as "cv"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_877372333083973691_1136"
  )
ORDER BY
  "COL_0"
''
plan:
    [0] SCAN TABLE _tmp_877372333083973691_1136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
╭─────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "t6"."cv",
  "t6"."ag" as "b",
  "t3"."ay",
  "t3"."bg",
  "t6"."cu",
  "t6"."cw",
  coalesce ("t6"."co"::int, "t6"."cm"::int, 0::int) as "ac",
  CASE
    WHEN not "t6"."co" is null
    or not "t6"."cm" is null THEN "t6"."cn"
    ELSE '0000'::text
  END as "ae",
  "to_char" ("t6"."cl"::datetime, '%Y-%m-%d'::text) as "ab",
  CASE
    WHEN not "t6"."co" is null THEN 0::int
    WHEN not "t6"."cm" is null THEN 1::int
    ELSE 2::int
  END as "c9",
  "t5"."d"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_18309399111922391115_2136"
  ) as "t2"
  INNER JOIN "y" as "t3" INDEXED BY "ix_i" ON "t3"."ag" = "t2"."COL_1"
  and "t3"."cv" = "t2"."COL_2"
  INNER JOIN "v" as "t4" INDEXED BY "ix_f" ON "t4"."b" = "t2"."COL_0"
  and "t4"."cv" = "t3"."cv"
  and "t4"."ai" < 0::int
  INNER JOIN "w" as "t5" INDEXED BY "ix_g" ON "t5"."x" = "t4"."ag"
  and "t5"."cv" = "t4"."cv"
  and "t5"."db" is null
  and "t5"."da" is null
  INNER JOIN "l" as "t6" INDEXED BY "ix_d" ON "t6"."ag" = "t5"."bs"
  and "t6"."cv" = "t5"."cv"
  and coalesce ("t6"."bb"::int, 0::int) <> 1::int
  LEFT JOIN "x" as "t7" ON "t7"."b" = "t6"."ag"
  and "t7"."cv" = "t6"."cv"
  LEFT JOIN "f" as "t8" ON "t8"."cw" = "t6"."cw"
WHERE
  "t8"."z" = 7::int
  and "t3"."aw" = 11::int
  and "t3"."cq" = 6::int
''
plan:
    [0] SCAN TABLE _tmp_18309399111922391115_2136 (~1048576 rows)
        [0] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~7 rows)
            [0] SEARCH TABLE v AS t4 USING COVERING INDEX ix_f (b=? AND cv=?) (~8 rows)
                [0] SEARCH TABLE w AS t5 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                    [0] SEARCH TABLE l AS t6 USING COVERING INDEX ix_d (cv=? AND ag=?) (~8 rows)
                        [0] SEARCH TABLE x AS t7 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
                            [0] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
''
buckets <= [1-3000]
''
╭────────────────────────────────────────╮
│ 4. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  *
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
      "_tmp_5092332724665672962_3136"
  ) as "cte1"
UNION ALL
SELECT
  *
FROM
  (
    SELECT
      "t9"."cv",
      "t9"."ag" as "b",
      "t9"."ay",
      coalesce ("t3"."bg"::text, "t9"."bg"::text) as "bg",
      coalesce ("t3"."cu"::int, "t9"."cu"::int) as "cu",
      "t9"."cw",
      coalesce ("t9"."co"::int, "t9"."cm"::int, 0::int) as "ac",
      "t9"."cn" as "ae",
      "to_char" ("t9"."cl"::datetime, '%Y-%m-%d'::text) as "ab",
      CASE
        WHEN not "t9"."co" is null THEN 0::int
        ELSE 1::int
      END as "c9",
      coalesce (
        (
          "t9"."d" * coalesce ("t8"."cj"::decimal, 0.0::decimal) * CASE
            WHEN not "t9"."h" is null THEN (-1)::int
            ELSE 1::int
          END
        )::decimal,
        0.0::decimal
      ) as "d"
    FROM
      "n" as "t9"
      LEFT JOIN "y" as "t3" INDEXED BY "ix_i" ON "t9"."au" = "t3"."ag"
      and "t3"."cv" = "t9"."cv"
      LEFT JOIN "f" as "t8" ON "t9"."cw" = "t8"."cw"
      LEFT JOIN "n" as "t10" INDEXED BY "ix_e" ON "t10"."h" = "t9"."ag"
      and "t10"."cv" = "t3"."cv"
      LEFT JOIN "h" as "t11" ON "t9"."cu" = "t11"."cu"
    WHERE
      "t9"."cv" = 4797271::int
      and coalesce ("t9"."cs"::datetime, "t9"."a"::datetime) = '2022-10-31 0:00:00.0 +00:00:00'::datetime
      and "t8"."z" = 7::int
      and coalesce ("t3"."aw"::int, "t9"."bf"::int) = 11::int
      and coalesce ("t3"."cq"::int, "t11"."cq"::int) = 6::int
  ) as "cte2"
''
plan:
    [1] SCAN TABLE _tmp_5092332724665672962_3136 (~1048576 rows)
    [2] SEARCH TABLE n AS t9 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [2] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [2] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
                [2] SEARCH TABLE n AS t10 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
                    [2] SEARCH TABLE h AS t11 USING PRIMARY KEY (cu=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = [874]
''
╭──────────────────────────╮
│ 5. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "cte3"."COL_0" as "cv",
  "cte3"."COL_1" as "b",
  "cte3"."COL_2" as "ay",
  "cte3"."COL_3" as "bg",
  "cte3"."COL_4" as "cu",
  "cte3"."COL_5" as "cw",
  "cte3"."COL_6" as "ac",
  "cte3"."COL_7" as "ae",
  "cte3"."COL_8" as "ab",
  "cte3"."COL_9" as "c9",
  "cte3"."COL_10" as "d",
  "t12"."cv",
  "t12"."ay",
  "t12"."bucket_id",
  "t12"."cx"
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
      "_tmp_6654303618351556102_5136"
  ) as "cte3"
  INNER JOIN "ad" as "t12" ON "t12"."cv" = "cte3"."COL_0"
  and "t12"."ay" = "cte3"."COL_2"
''
plan:
    [0] SCAN TABLE _tmp_6654303618351556102_5136 (~1048576 rows)
        [0] SEARCH TABLE ad AS t12 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 6. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "unnamed_join"."COL_0" as "cv",
      "unnamed_join"."COL_1" as "b",
      "unnamed_join"."COL_2" as "ay",
      "unnamed_join"."COL_3" as "bg",
      "unnamed_join"."COL_4" as "cu",
      "unnamed_join"."COL_5" as "cw",
      "unnamed_join"."COL_6" as "ac",
      "unnamed_join"."COL_7" as "ae",
      "unnamed_join"."COL_8" as "ab",
      "unnamed_join"."COL_9" as "c9",
      "unnamed_join"."COL_10" as "d",
      "unnamed_join"."COL_14" as "cx",
      row_number () OVER (
        ORDER BY
          "unnamed_join"."COL_1" DESC) as "c11"
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
          "COL_10",
          "COL_11",
          "COL_12",
          "COL_13",
          "COL_14"
        FROM
          "_tmp_6219172643915444872_6136"
      ) as "unnamed_join"
  ) as "cte4"
WHERE
  "cte4"."c11" > 0::int
LIMIT
  10
''
plan:
    [2] SCAN TABLE _tmp_6219172643915444872_6136 (~1048576 rows)
    [2] USE TEMP B-TREE FOR ORDER BY
    [1] SCAN SUBQUERY 2 (~1 row)
    [0] SCAN SUBQUERY 1 AS cte4 (~1 row)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q30
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  ) t12;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  sum(count_1::int::int)::int::int -> c1,
  sum(sum_2::decimal::decimal)::decimal -> c2
)
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      count(*)::int -> count_1,
      sum(t12.d::decimal::decimal)::decimal -> sum_2
    )
      scan t12
        union all
          motion [policy: local, program: SerializeAsEmptyTable(true)]
            projection (cte1.d::decimal -> d)
              scan cte cte1($1)
          projection (cte2.d::decimal -> d)
            scan cte cte2($2)
subquery $0:
  projection (ag::int, au::int, cv::int)
    order by (ag::int)
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (
            t0.ag::int -> ag,
            t0.au::int -> au,
            t0.cv::int -> cv
          )
            selection (t1.b::int is null)
              left join on (
                ROW(t0.ag::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
              )
                scan t0
                  union all
                    projection (
                      l.ag::int -> ag,
                      l.au::int -> au,
                      l.cv::int -> cv
                    )
                      selection (
                        (
                          l.cv::int = 4797271::int
                          and l.cs::datetime >= '2022-10-31 0:00:00.0 +00:00:00'::datetime
                          and l.cs::datetime < '2022-11-01 0:00:00.0 +00:00:00'::datetime
                          and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                        )
                      )
                        scan l
                    projection (
                      l.ag::int -> ag,
                      l.au::int -> au,
                      l.cv::int -> cv
                    )
                      selection (
                        (
                          l.cv::int = 4797271::int
                          and l.cs::datetime is null
                          and l.a::datetime >= '2022-10-31 0:00:00.0 +00:00:00'::datetime
                          and l.a::datetime < '2022-11-01 0:00:00.0 +00:00:00'::datetime
                          and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                        )
                      )
                        scan l
                scan x -> t1
subquery $1:
  motion [policy: full, program: ReshardIfNeeded]
    projection (t5.d::decimal -> d)
      selection (
        (
          t8.z::int = 7::int
          and t3.aw::int = 11::int
          and t3.cq::int = 6::int
        )
      )
        left join on (t8.cw::int = t6.cw::int)
          left join on (
            ROW(t6.ag::int, t6.cv::int) = ROW(t7.b::int, t7.cv::int)
          )
            join on (
              (
                ROW(t5.bs::int, t5.cv::int) = ROW(t6.ag::int, t6.cv::int)
                and coalesce(t6.bb::int::int, 0::int)::int <> 1::int
              )
            )
              join on (
                (
                  ROW(t4.ag::int, t4.cv::int) = ROW(t5.x::int, t5.cv::int)
                  and t5.db::int is null
                  and t5.da::int is null
                )
              )
                join on (
                  (
                    ROW(t2.ag::int, t3.cv::int) = ROW(t4.b::int, t4.cv::int)
                    and t4.ai::decimal < 0::int
                  )
                )
                  join on (
                    ROW(t2.au::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
                  )
                    motion [policy: segment([ref(cv)]), program: ReshardIfNeeded]
                      scan cte t2($0)
                    scan y -> t3 (indexed by ix_i)
                  scan v -> t4 (indexed by ix_f)
                scan w -> t5 (indexed by ix_g)
              scan l -> t6 (indexed by ix_d)
            scan x -> t7
          scan f -> t8
subquery $2:
  projection (
    coalesce(
      (
        t9.d::decimal * coalesce(t8.cj::int::decimal, 0.0::decimal)::decimal * case 
          when not t9.h::int is null then -1::int
          else 1::int
        end
      )::decimal,
      0.0::decimal
    )::decimal -> d
  )
    selection (
      (
        t9.cv::int = 4797271::int
        and coalesce(
          t9.cs::datetime::datetime,
          t9.a::datetime::datetime
        )::datetime = '2022-10-31 0:00:00.0 +00:00:00'::datetime
        and t8.z::int = 7::int
        and coalesce(t3.aw::int::int, t9.bf::int::int)::int = 11::int
        and coalesce(t3.cq::int::int, t11.cq::int::int)::int = 6::int
      )
    )
      left join on (t9.cu::int = t11.cu::int)
        left join on (
          ROW(t9.ag::int, t3.cv::int) = ROW(t10.h::int, t10.cv::int)
        )
          left join on (t9.cw::int = t8.cw::int)
            left join on (
              ROW(t9.au::int, t9.cv::int) = ROW(t3.ag::int, t3.cv::int)
            )
              scan n -> t9
              scan y -> t3 (indexed by ix_i)
            scan f -> t8
          scan n -> t10 (indexed by ix_e)
        scan h -> t11
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ag",
  "t0"."au",
  "t0"."cv"
FROM
  (
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" >= '2022-10-31 0:00:00.0 +00:00:00'::datetime
      and "l"."cs" < '2022-11-01 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
    UNION ALL
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" is null
      and "l"."a" >= '2022-10-31 0:00:00.0 +00:00:00'::datetime
      and "l"."a" < '2022-11-01 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
  ) as "t0"
  LEFT JOIN "x" as "t1" ON "t1"."b" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
WHERE
  "t1"."b" is null
''
plan:
    [2] SEARCH TABLE l USING COVERING INDEX ix_z (cv=? AND cs>? AND cs<?) (~1 row)
    [3] SEARCH TABLE l USING COVERING INDEX ix_aa (cv=? AND a>? AND a<?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t0 (~1 row)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
''
buckets = [874]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "COL_0" as "ag",
  "COL_1" as "au",
  "COL_2" as "cv"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_8641021163966270088_0136"
  )
ORDER BY
  "COL_0"
''
plan:
    [0] SCAN TABLE _tmp_8641021163966270088_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
╭─────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "t5"."d"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_2465481866393360794_1136"
  ) as "t2"
  INNER JOIN "y" as "t3" INDEXED BY "ix_i" ON "t3"."ag" = "t2"."COL_1"
  and "t3"."cv" = "t2"."COL_2"
  INNER JOIN "v" as "t4" INDEXED BY "ix_f" ON "t4"."b" = "t2"."COL_0"
  and "t4"."cv" = "t3"."cv"
  and "t4"."ai" < 0::int
  INNER JOIN "w" as "t5" INDEXED BY "ix_g" ON "t5"."x" = "t4"."ag"
  and "t5"."cv" = "t4"."cv"
  and "t5"."db" is null
  and "t5"."da" is null
  INNER JOIN "l" as "t6" INDEXED BY "ix_d" ON "t6"."ag" = "t5"."bs"
  and "t6"."cv" = "t5"."cv"
  and coalesce ("t6"."bb"::int, 0::int) <> 1::int
  LEFT JOIN "x" as "t7" ON "t7"."b" = "t6"."ag"
  and "t7"."cv" = "t6"."cv"
  LEFT JOIN "f" as "t8" ON "t8"."cw" = "t6"."cw"
WHERE
  "t8"."z" = 7::int
  and "t3"."aw" = 11::int
  and "t3"."cq" = 6::int
''
plan:
    [0] SCAN TABLE _tmp_2465481866393360794_1136 (~1048576 rows)
        [0] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~7 rows)
            [0] SEARCH TABLE v AS t4 USING COVERING INDEX ix_f (b=? AND cv=?) (~8 rows)
                [0] SEARCH TABLE w AS t5 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                    [0] SEARCH TABLE l AS t6 USING COVERING INDEX ix_d (cv=? AND ag=?) (~8 rows)
                        [0] SEARCH TABLE x AS t7 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
                            [0] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
''
buckets <= [1-3000]
''
╭────────────────────────────────────────╮
│ 4. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  count (*) as "count_1",
  sum ("t12"."COL_0"::decimal) as "sum_2"
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          "COL_0"
        FROM
          "_tmp_5746341004775003441_2136"
      ) as "cte1"
    UNION ALL
    SELECT
      *
    FROM
      (
        SELECT
          coalesce (
            (
              "t9"."d" * coalesce ("t8"."cj"::decimal, 0.0::decimal) * CASE
                WHEN not "t9"."h" is null THEN (-1)::int
                ELSE 1::int
              END
            )::decimal,
            0.0::decimal
          ) as "d"
        FROM
          "n" as "t9"
          LEFT JOIN "y" as "t3" INDEXED BY "ix_i" ON "t9"."au" = "t3"."ag"
          and "t3"."cv" = "t9"."cv"
          LEFT JOIN "f" as "t8" ON "t9"."cw" = "t8"."cw"
          LEFT JOIN "n" as "t10" INDEXED BY "ix_e" ON "t10"."h" = "t9"."ag"
          and "t10"."cv" = "t3"."cv"
          LEFT JOIN "h" as "t11" ON "t9"."cu" = "t11"."cu"
        WHERE
          "t9"."cv" = 4797271::int
          and coalesce ("t9"."cs"::datetime, "t9"."a"::datetime) = '2022-10-31 0:00:00.0 +00:00:00'::datetime
          and "t8"."z" = 7::int
          and coalesce ("t3"."aw"::int, "t9"."bf"::int) = 11::int
          and coalesce ("t3"."cq"::int, "t11"."cq"::int) = 6::int
      ) as "cte2"
  ) as "t12"
''
plan:
    [2] SCAN TABLE _tmp_5746341004775003441_2136 (~1048576 rows)
    [3] SEARCH TABLE n AS t9 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [3] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [3] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
                [3] SEARCH TABLE n AS t10 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
                    [3] SEARCH TABLE h AS t11 USING PRIMARY KEY (cu=?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t12 (~1 row)
''
buckets = [874]
''
╭───────────────────╮
│ 5. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  sum ("COL_0"::int)::int as "c1",
  sum ("COL_1"::decimal) as "c2"
FROM
  (
    SELECT
      "COL_0",
      "COL_1"
    FROM
      "_tmp_17257311967805472214_4136"
  )
''
plan:
    [0] SCAN TABLE _tmp_17257311967805472214_4136 (~1048576 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q31
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  cte2;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union all
  motion [policy: segment([ref(cv)]), program: ReshardIfNeeded]
    projection (
      cte1.cv::int -> cv,
      cte1.ay::text -> ay,
      cte1.bg::text -> bg,
      cte1.ah::text -> ah,
      cte1.cu::int -> cu,
      cte1.cx::text -> cx
    )
      scan cte cte1($1)
  projection (
    cte2.cv::int -> cv,
    cte2.ay::text -> ay,
    cte2.bg::text -> bg,
    cte2.ah::text -> ah,
    cte2.cu::int -> cu,
    cte2.cx::text -> cx
  )
    scan cte cte2($2)
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      t0.ag::int -> ag,
      t0.au::int -> au,
      t0.cv::int -> cv
    )
      selection (t1.b::int is null)
        left join on (
          ROW(t0.ag::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
        )
          scan t0
            union all
              projection (
                l.ag::int -> ag,
                l.au::int -> au,
                l.cv::int -> cv
              )
                selection (
                  (
                    l.cv::int = 4797271::int
                    and l.cs::datetime >= '2024-04-02 0:00:00.0 +00:00:00'::datetime
                    and l.cs::datetime < '2024-04-03 0:00:00.0 +00:00:00'::datetime
                    and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                  )
                )
                  scan l
              projection (
                l.ag::int -> ag,
                l.au::int -> au,
                l.cv::int -> cv
              )
                selection (
                  (
                    l.cv::int = 4797271::int
                    and l.cs::datetime is null
                    and l.a::datetime >= '2024-04-02 0:00:00.0 +00:00:00'::datetime
                    and l.a::datetime < '2024-04-03 0:00:00.0 +00:00:00'::datetime
                    and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                  )
                )
                  scan l
          scan x -> t1
subquery $1:
  projection (
    gr_expr_1::int -> cv,
    gr_expr_2::text -> ay,
    gr_expr_3::text -> bg,
    gr_expr_4::text -> ah,
    gr_expr_5::int -> cu,
    gr_expr_6::text -> cx
  )
    group by (
      gr_expr_1::int,
      gr_expr_2::text,
      gr_expr_3::text,
      gr_expr_4::text,
      gr_expr_5::int,
      gr_expr_6::text
    )
      motion [policy: full, program: ReshardIfNeeded]
        projection (
          t6.cv::int -> gr_expr_1,
          t3.ay::text -> gr_expr_2,
          t3.bg::text -> gr_expr_3,
          t3.ah::text -> gr_expr_4,
          t6.cu::int -> gr_expr_5,
          t8.cx::text -> gr_expr_6
        )
          group by (
            t6.cv::int,
            t3.ay::text,
            t3.bg::text,
            t3.ah::text,
            t6.cu::int,
            t8.cx::text
          )
            selection (
              (
                t7.z::int = 7::int
                and t3.aw::int = 11::int
                and t3.cq::int = 6::int
              )
            )
              left join on (
                ROW(t3.cv::int, t3.ay::text) = ROW(t8.cv::int, t8.ay::text)
              )
                left join on (t7.cw::int = t6.cw::int)
                  join on (
                    (
                      ROW(t5.bs::int, t5.cv::int) = ROW(t6.ag::int, t6.cv::int)
                      and coalesce(t6.bb::int::int, 0::int)::int <> 1::int
                    )
                  )
                    join on (
                      (
                        ROW(t4.cv::int, t4.ag::int) = ROW(t5.cv::int, t5.x::int)
                        and t5.db::int is null
                        and t5.da::int is null
                      )
                    )
                      join on (
                        (
                          ROW(t2.ag::int, t3.cv::int) = ROW(t4.b::int, t4.cv::int)
                          and t4.ai::decimal < 0::int
                        )
                      )
                        join on (
                          ROW(t2.au::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
                        )
                          scan cte t2($0)
                          scan y -> t3 (indexed by ix_i)
                        scan v -> t4 (indexed by ix_f)
                      scan w -> t5 (indexed by ix_g)
                    scan l -> t6 (indexed by ix_d)
                  scan f -> t7
                scan ad -> t8
subquery $2:
  projection (
    t9.cv::int -> cv,
    t9.ay::text -> ay,
    coalesce(t3.bg::text::text, t9.bg::text::text)::text -> bg,
    coalesce(t3.ah::text::text, t9.ah::text::text)::text -> ah,
    coalesce(t3.cu::int::int, t9.cu::int::int)::int -> cu,
    t8.cx::text -> cx
  )
    selection (
      (
        t9.cv::int = 4797271::int
        and coalesce(
          t9.cs::datetime::datetime,
          t9.a::datetime::datetime
        )::datetime = '2024-04-02 0:00:00.0 +00:00:00'::datetime
        and t7.z::int = 7::int
        and coalesce(t3.aw::int::int, t9.bf::int::int)::int = 11::int
        and coalesce(t3.cq::int::int, t11.cq::int::int)::int = 6::int
      )
    )
      left join on (
        ROW(t3.cv::int, t3.ay::text) = ROW(t8.cv::int, t8.ay::text)
      )
        left join on (t9.cu::int = t11.cu::int)
          left join on (
            ROW(t9.ag::int, t9.cv::int) = ROW(t10.h::int, t10.cv::int)
          )
            left join on (t9.cw::int = t7.cw::int)
              left join on (
                ROW(t9.au::int, t9.cv::int) = ROW(t3.ag::int, t3.cv::int)
              )
                scan n -> t9
                scan y -> t3 (indexed by ix_i)
              scan f -> t7
            scan n -> t10 (indexed by ix_e)
          scan h -> t11
        scan ad -> t8
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ag",
  "t0"."au",
  "t0"."cv"
FROM
  (
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" >= '2024-04-02 0:00:00.0 +00:00:00'::datetime
      and "l"."cs" < '2024-04-03 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
    UNION ALL
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" is null
      and "l"."a" >= '2024-04-02 0:00:00.0 +00:00:00'::datetime
      and "l"."a" < '2024-04-03 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
  ) as "t0"
  LEFT JOIN "x" as "t1" ON "t1"."b" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
WHERE
  "t1"."b" is null
''
plan:
    [2] SEARCH TABLE l USING COVERING INDEX ix_z (cv=? AND cs>? AND cs<?) (~1 row)
    [3] SEARCH TABLE l USING COVERING INDEX ix_aa (cv=? AND a>? AND a<?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t0 (~1 row)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
''
buckets = [874]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "t6"."cv" as "gr_expr_1",
  "t3"."ay" as "gr_expr_2",
  "t3"."bg" as "gr_expr_3",
  "t3"."ah" as "gr_expr_4",
  "t6"."cu" as "gr_expr_5",
  "t8"."cx" as "gr_expr_6"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_5930600967966593789_0136"
  ) as "t2"
  INNER JOIN "y" as "t3" INDEXED BY "ix_i" ON "t3"."ag" = "t2"."COL_1"
  and "t3"."cv" = "t2"."COL_2"
  INNER JOIN "v" as "t4" INDEXED BY "ix_f" ON "t4"."b" = "t2"."COL_0"
  and "t4"."cv" = "t3"."cv"
  and "t4"."ai" < 0::int
  INNER JOIN "w" as "t5" INDEXED BY "ix_g" ON "t5"."x" = "t4"."ag"
  and "t5"."db" is null
  and "t5"."da" is null
  and "t5"."cv" = "t4"."cv"
  INNER JOIN "l" as "t6" INDEXED BY "ix_d" ON "t6"."ag" = "t5"."bs"
  and "t6"."cv" = "t5"."cv"
  and coalesce ("t6"."bb"::int, 0::int) <> 1::int
  LEFT JOIN "f" as "t7" ON "t7"."cw" = "t6"."cw"
  LEFT JOIN "ad" as "t8" ON "t8"."cv" = "t3"."cv"
  and "t8"."ay" = "t3"."ay"
WHERE
  "t7"."z" = 7::int
  and "t3"."aw" = 11::int
  and "t3"."cq" = 6::int
GROUP BY
  "t6"."cv",
  "t3"."ay",
  "t3"."bg",
  "t3"."ah",
  "t6"."cu",
  "t8"."cx"
''
plan:
    [0] SCAN TABLE _tmp_5930600967966593789_0136 (~1048576 rows)
        [0] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~7 rows)
            [0] SEARCH TABLE v AS t4 USING COVERING INDEX ix_f (b=? AND cv=?) (~8 rows)
                [0] SEARCH TABLE w AS t5 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                    [0] SEARCH TABLE l AS t6 USING COVERING INDEX ix_d (cv=? AND ag=?) (~8 rows)
                        [0] SEARCH TABLE f AS t7 USING PRIMARY KEY (cw=?) (~1 row)
                            [0] SEARCH TABLE ad AS t8 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT DISTINCT
      "COL_0" as "cv",
      "COL_1" as "ay",
      "COL_2" as "bg",
      "COL_3" as "ah",
      "COL_4" as "cu",
      "COL_5" as "cx"
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
          "_tmp_2630807218819645061_1136"
      )
    GROUP BY
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5"
  ) as "cte1"
''
plan:
    [1] SCAN TABLE _tmp_2630807218819645061_1136 (~1048576 rows)
    [1] USE TEMP B-TREE FOR GROUP BY
    [1] USE TEMP B-TREE FOR DISTINCT
    [0] SCAN SUBQUERY 1 AS cte1 (~1 row)
''
buckets = any
''
╭─────────────────────────────────────────╮
│ 4. Query (DYN-FILTERED STORAGE, <= 1/1) │
╰─────────────────────────────────────────╯
''
SELECT
  "COL_0",
  "COL_1",
  "COL_2",
  "COL_3",
  "COL_4",
  "COL_5"
FROM
  "_tmp_6144964650535932087_2136"
UNION ALL
SELECT
  *
FROM
  (
    SELECT DISTINCT
      "t9"."cv",
      "t9"."ay",
      coalesce ("t3"."bg"::text, "t9"."bg"::text) as "bg",
      coalesce ("t3"."ah"::text, "t9"."ah"::text) as "ah",
      coalesce ("t3"."cu"::int, "t9"."cu"::int) as "cu",
      "t8"."cx"
    FROM
      "n" as "t9"
      LEFT JOIN "y" as "t3" INDEXED BY "ix_i" ON "t9"."au" = "t3"."ag"
      and "t9"."cv" = "t3"."cv"
      LEFT JOIN "f" as "t7" ON "t9"."cw" = "t7"."cw"
      LEFT JOIN "n" as "t10" INDEXED BY "ix_e" ON "t10"."h" = "t9"."ag"
      and "t10"."cv" = "t9"."cv"
      LEFT JOIN "h" as "t11" ON "t9"."cu" = "t11"."cu"
      LEFT JOIN "ad" as "t8" ON "t8"."cv" = "t3"."cv"
      and "t8"."ay" = "t3"."ay"
    WHERE
      "t9"."cv" = 4797271::int
      and coalesce ("t9"."cs"::datetime, "t9"."a"::datetime) = '2024-04-02 0:00:00.0 +00:00:00'::datetime
      and "t7"."z" = 7::int
      and coalesce ("t3"."aw"::int, "t9"."bf"::int) = 11::int
      and coalesce ("t3"."cq"::int, "t11"."cq"::int) = 6::int
  ) as "cte2"
''
plan:
    [1] SCAN TABLE _tmp_6144964650535932087_2136 (~1048576 rows)
    [3] SEARCH TABLE n AS t9 USING COVERING INDEX ix_ao (cv=?) (~9 rows)
        [3] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [3] SEARCH TABLE f AS t7 USING PRIMARY KEY (cw=?) (~1 row)
                [3] SEARCH TABLE n AS t10 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
                    [3] SEARCH TABLE h AS t11 USING PRIMARY KEY (cu=?) (~1 row)
                        [3] SEARCH TABLE ad AS t8 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [3] USE TEMP B-TREE FOR DISTINCT
    [2] SCAN SUBQUERY 3 AS cte2 (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets <= [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q32
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  t3.z;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union all
  projection (
    a.z::int -> c0,
    sum(a.f::decimal::decimal)::decimal -> c1,
    sum(a.e::decimal::decimal)::decimal -> c2,
    0::int -> c3
  )
    group by (a.z::int)
      selection (
        (
          a.cv::int = 3023424::int
          and a.aw::int = 11::int
          and a.cq::int = 6::int
          and a.a::datetime = '2022-10-12 0:00:00.0 +00:00:00'::datetime
        )
      )
        scan a
  projection (
    t3.z::int -> c0,
    0::int -> c1,
    0::int -> c2,
    coalesce(
      sum(
        (
          t0.d::decimal * coalesce(t3.cj::int::decimal, 0::decimal)::decimal * case 
            when not t0.h::int is null then -1::int
            else 1::int
          end
        )::decimal
      )::decimal::decimal,
      0.0::decimal
    )::decimal -> c3
  )
    group by (t3.z::int)
      selection (
        (
          t0.cv::int = 3023424::int
          and coalesce(t2.aw::int::int, t0.bf::int::int)::int = 11::int
          and coalesce(t2.cq::int::int, t4.cq::int::int)::int = 6::int
          and coalesce(
            t0.cs::datetime::datetime,
            t0.a::datetime::datetime
          )::datetime = '2022-10-12 0:00:00.0 +00:00:00'::datetime
          and t1.ag::int is null
          and (t0.h::int is null or t0.h::int = -1::int)
        )
      )
        left join on (t0.cu::int = t4.cu::int)
          left join on (t0.cw::int = t3.cw::int)
            left join on (
              ROW(t0.au::int, t0.cv::int) = ROW(t2.ag::int, t2.cv::int)
            )
              left join on (
                ROW(t0.ag::int, t0.cv::int) = ROW(t1.h::int, t1.cv::int)
              )
                scan n -> t0
                scan n -> t1
              scan y -> t2
            scan f -> t3
          scan h -> t4
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "a"."z" as "c0",
  sum ("a"."f"::decimal) as "c1",
  sum ("a"."e"::decimal) as "c2",
  0::int as "c3"
FROM
  "a"
WHERE
  "a"."cv" = 3023424::int
  and "a"."aw" = 11::int
  and "a"."cq" = 6::int
  and "a"."a" = '2022-10-12 0:00:00.0 +00:00:00'::datetime
GROUP BY
  "a"."z"
UNION ALL
SELECT
  "t3"."z" as "c0",
  0::int as "c1",
  0::int as "c2",
  coalesce (
    sum (
      (
        "t0"."d" * coalesce ("t3"."cj"::decimal, 0::decimal) * CASE
          WHEN not "t0"."h" is null THEN (-1)::int
          ELSE 1::int
        END
      )::decimal
    )::decimal,
    0.0::decimal
  ) as "c3"
FROM
  "n" as "t0"
  LEFT JOIN "n" as "t1" ON "t1"."h" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
  LEFT JOIN "y" as "t2" ON "t0"."au" = "t2"."ag"
  and "t0"."cv" = "t2"."cv"
  LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
  LEFT JOIN "h" as "t4" ON "t0"."cu" = "t4"."cu"
WHERE
  "t0"."cv" = 3023424::int
  and coalesce ("t2"."aw"::int, "t0"."bf"::int) = 11::int
  and coalesce ("t2"."cq"::int, "t4"."cq"::int) = 6::int
  and coalesce ("t0"."cs"::datetime, "t0"."a"::datetime) = '2022-10-12 0:00:00.0 +00:00:00'::datetime
  and "t1"."ag" is null
  and (
    "t0"."h" is null
    or "t0"."h" = (-1)::int
  )
GROUP BY
  "t3"."z"
''
plan:
    [1] SEARCH TABLE a 
         USING COVERING INDEX ix_c 
         (cv=? AND aw=? AND cq=? AND a=?) 
         (~7 rows)
    [1] USE TEMP B-TREE FOR GROUP BY
    [2] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~8 rows)
        [2] SEARCH TABLE n AS t1 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [2] SEARCH TABLE y AS t2 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [2] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
                    [2] SEARCH TABLE h AS t4 USING PRIMARY KEY (cu=?) (~1 row)
    [2] USE TEMP B-TREE FOR GROUP BY
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q33
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
motion [policy: full, program: RemoveDuplicates]
  union
    projection (
      t0.ay::text -> ay,
      t0.bg::text -> bg,
      t0.ah::text -> ah,
      t1.cx::text -> cx
    )
      selection (
        (
          t0.cv::int = 3023424::int
          and t0.aw::int = 11::int
          and t0.a::datetime = '2022-10-12 0:00:00.0 +00:00:00'::datetime
          and t0.cq::int = 6::int
        )
      )
        left join on (
          ROW(t0.cv::int, t0.ay::text) = ROW(t1.cv::int, t1.ay::text)
        )
          scan a -> t0
          scan ad -> t1
    projection (
      t2.ay::text -> c0,
      coalesce(t4.bg::text::text, t2.bg::text::text)::text -> c1,
      coalesce(t4.ah::text::text, t2.ah::text::text)::text -> c2,
      t1.cx::text -> c3
    )
      selection (
        (
          t2.cv::int = 3023424::int
          and coalesce(t4.aw::int::int, t2.bf::int::int)::int = 11::int
          and coalesce(
            t2.cs::datetime::datetime,
            t2.a::datetime::datetime
          )::datetime = '2022-10-12 0:00:00.0 +00:00:00'::datetime
          and coalesce(t4.cq::int::int, t6.cq::int::int)::int = 6::int
        )
      )
        left join on (t2.cu::int = t6.cu::int)
          left join on (
            ROW(t2.cv::int, t2.ay::text) = ROW(t1.cv::int, t1.ay::text)
          )
            left join on (t2.cw::int = t5.cw::int)
              left join on (
                ROW(t2.au::int, t2.cv::int) = ROW(t4.ag::int, t4.cv::int)
              )
                left join on (
                  ROW(t2.ag::int, t2.cv::int) = ROW(t3.h::int, t3.cv::int)
                )
                  scan n -> t2
                  scan n -> t3
                scan y -> t4
              scan f -> t5
            scan ad -> t1
          scan h -> t6
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT DISTINCT
  "t0"."ay",
  "t0"."bg",
  "t0"."ah",
  "t1"."cx"
FROM
  "a" as "t0"
  LEFT JOIN "ad" as "t1" ON "t0"."cv" = "t1"."cv"
  and "t0"."ay" = "t1"."ay"
WHERE
  "t0"."cv" = 3023424::int
  and "t0"."aw" = 11::int
  and "t0"."a" = '2022-10-12 0:00:00.0 +00:00:00'::datetime
  and "t0"."cq" = 6::int
UNION
SELECT DISTINCT
  "t2"."ay" as "c0",
  coalesce ("t4"."bg"::text, "t2"."bg"::text) as "c1",
  coalesce ("t4"."ah"::text, "t2"."ah"::text) as "c2",
  "t1"."cx" as "c3"
FROM
  "n" as "t2"
  LEFT JOIN "n" as "t3" ON "t3"."h" = "t2"."ag"
  and "t3"."cv" = "t2"."cv"
  LEFT JOIN "y" as "t4" ON "t2"."au" = "t4"."ag"
  and "t2"."cv" = "t4"."cv"
  LEFT JOIN "f" as "t5" ON "t2"."cw" = "t5"."cw"
  LEFT JOIN "ad" as "t1" ON "t2"."cv" = "t1"."cv"
  and "t2"."ay" = "t1"."ay"
  LEFT JOIN "h" as "t6" ON "t2"."cu" = "t6"."cu"
WHERE
  "t2"."cv" = 3023424::int
  and coalesce ("t4"."aw"::int, "t2"."bf"::int) = 11::int
  and coalesce ("t2"."cs"::datetime, "t2"."a"::datetime) = '2022-10-12 0:00:00.0 +00:00:00'::datetime
  and coalesce ("t4"."cq"::int, "t6"."cq"::int) = 6::int
''
plan:
    [1] SEARCH TABLE a AS t0 
         USING COVERING INDEX ix_c 
         (cv=? AND aw=? AND cq=? AND a=?) 
         (~7 rows)
        [1] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [2] SEARCH TABLE n AS t2 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [2] SEARCH TABLE n AS t3 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [2] SEARCH TABLE y AS t4 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [2] SEARCH TABLE f AS t5 USING PRIMARY KEY (cw=?) (~1 row)
                    [2] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
                        [2] SEARCH TABLE h AS t6 USING PRIMARY KEY (cu=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q34
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  a;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union all
  projection (
    a.cq::int -> c0,
    to_char(a.a::datetime::datetime, '%Y-%m-%d'::text)::text -> c1,
    sum(a.f::decimal::decimal)::decimal -> c3,
    sum(a.e::decimal::decimal)::decimal -> c4,
    0.0::decimal -> c5
  )
    group by (a.a::datetime, a.cq::int)
      selection (
        (
          a.cv::int = 3023424::int
          and a.aw::int = 11::int
          and a.bm::int in ROW(2024::int, 2023::int, 2022::int)
        )
      )
        scan a
  projection (
    cte0.cq::int -> cq,
    cte0.a::text -> a,
    0::int -> c3,
    0::int -> c4,
    coalesce(
      sum(
        (
          cte0.d::decimal * cte0.cj::int * case 
            when not cte0.h::int is null then -1::int
            else 1::int
          end
        )::decimal
      )::decimal::decimal,
      0.0::decimal
    )::decimal -> c5
  )
    group by (cte0.cq::int, cte0.a::text)
      scan cte cte0($0)
subquery $0:
  projection (
    coalesce(t2.cq::int::int, t4.cq::int::int)::int -> cq,
    to_char(
      coalesce(
        t0.cs::datetime::datetime,
        t0.a::datetime::datetime
      )::datetime::datetime,
      '%Y-%m-%d'::text
    )::text -> a,
    t0.d::decimal -> d,
    coalesce(t3.cj::int::int, 0::int)::int -> cj,
    t0.h::int -> h
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and coalesce(t2.aw::int::int, t0.bf::int::int)::int = 11::int
        and t1.ag::int is null
        and (t0.h::int is null or t0.h::int = -1::int)
        and to_char(
          coalesce(
            t0.cs::datetime::datetime,
            t0.a::datetime::datetime
          )::datetime::datetime,
          '%Y'::text
        )::text::int in ROW(2024::int, 2023::int, 2022::int)
      )
    )
      left join on (t0.cu::int = t4.cu::int)
        left join on (t0.cw::int = t3.cw::int)
          left join on (
            ROW(t0.au::int, t0.cv::int) = ROW(t2.ag::int, t2.cv::int)
          )
            left join on (
              ROW(t0.ag::int, t0.cv::int) = ROW(t1.h::int, t1.cv::int)
            )
              scan n -> t0
              scan n -> t1
            scan y -> t2
          scan f -> t3
        scan h -> t4
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "a"."cq" as "c0",
  "to_char" ("a"."a"::datetime, '%Y-%m-%d'::text) as "c1",
  sum ("a"."f"::decimal) as "c3",
  sum ("a"."e"::decimal) as "c4",
  0.0::decimal as "c5"
FROM
  "a"
WHERE
  "a"."cv" = 3023424::int
  and "a"."aw" = 11::int
  and "a"."bm" in (2024::int, 2023::int, 2022::int)
GROUP BY
  "a"."a",
  "a"."cq"
UNION ALL
SELECT
  "cte0"."cq",
  "cte0"."a",
  0::int as "c3",
  0::int as "c4",
  coalesce (
    sum (
      (
        "cte0"."d" * "cte0"."cj" * CASE
          WHEN not "cte0"."h" is null THEN (-1)::int
          ELSE 1::int
        END
      )::decimal
    )::decimal,
    0.0::decimal
  ) as "c5"
FROM
  (
    SELECT
      coalesce ("t2"."cq"::int, "t4"."cq"::int) as "cq",
      "to_char" (
        coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
        '%Y-%m-%d'::text
      ) as "a",
      "t0"."d",
      coalesce ("t3"."cj"::int, 0::int) as "cj",
      "t0"."h"
    FROM
      "n" as "t0"
      LEFT JOIN "n" as "t1" ON "t1"."h" = "t0"."ag"
      and "t1"."cv" = "t0"."cv"
      LEFT JOIN "y" as "t2" ON "t0"."au" = "t2"."ag"
      and "t0"."cv" = "t2"."cv"
      LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
      LEFT JOIN "h" as "t4" ON "t0"."cu" = "t4"."cu"
    WHERE
      "t0"."cv" = 3023424::int
      and coalesce ("t2"."aw"::int, "t0"."bf"::int) = 11::int
      and "t1"."ag" is null
      and (
        "t0"."h" is null
        or "t0"."h" = (-1)::int
      )
      and "to_char" (
        coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
        '%Y'::text
      )::int in (2024::int, 2023::int, 2022::int)
  ) as "cte0"
GROUP BY
  "cte0"."cq",
  "cte0"."a"
''
plan:
    [1] SEARCH TABLE a USING COVERING INDEX ix_c (cv=? AND aw=?) (~8 rows)
    [1] EXECUTE LIST SUBQUERY 2
    [2] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~8 rows)
    [2] EXECUTE LIST SUBQUERY 3
        [2] SEARCH TABLE n AS t1 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [2] SEARCH TABLE y AS t2 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [2] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
                    [2] SEARCH TABLE h AS t4 USING PRIMARY KEY (cu=?) (~1 row)
    [2] USE TEMP B-TREE FOR GROUP BY
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q35
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  10;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 10
  projection (
    cte0.z::int -> z,
    cte0.cw::int -> cw,
    cte0.ay::text -> ay,
    cte0.bg::text -> bg,
    cte0.cu::int -> cu,
    cte0.d::decimal -> d,
    cte0.ac::decimal -> ac,
    cte0.ad::int -> ad,
    cte0.ae::text -> ae,
    cte0.ab::datetime -> ab,
    cte0.c10::int -> c10,
    cte0.h::int -> h,
    cte0.cx::text -> cx,
    cte0.c13::int -> c13
  )
    selection (cte0.c13::int > 0::int)
      scan cte cte0($0)
subquery $0:
  projection (
    z::int,
    cw::int,
    ay::text,
    bg::text,
    cu::int,
    aa::decimal -> d,
    ac::decimal,
    ad::int,
    ae::text,
    ab::datetime,
    0::int -> c10,
    h::int,
    cx::text,
    row_number() over () -> c13
  )
    motion [policy: full, program: ReshardIfNeeded]
      projection (
        t0.z::int -> z,
        t0.cu::int -> cu,
        t0.aa::decimal -> aa,
        t0.ay::text -> ay,
        t0.bg::text -> bg,
        t0.cw::int -> cw,
        t0.ac::decimal -> ac,
        t0.ae::text -> ae,
        t0.ab::datetime -> ab,
        t0.ad::int -> ad,
        t0.h::int -> h,
        t1.cx::text -> cx
      )
        selection (
          (
            t0.cv::int = 3023424::int
            and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
            and t0.w::datetime = '2024-02-28 0:00:00.0 +00:00:00'::datetime
            and t0.cu::int = 3465::int
            and t0.bj::int is null
            and t0.z::int = 11::int
          )
        )
          left join on (
            ROW(t0.cv::int, t0.ay::text) = ROW(t1.cv::int, t1.ay::text)
          )
            scan m -> t0
            scan ad -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."z",
  "t0"."cu",
  "t0"."aa",
  "t0"."ay",
  "t0"."bg",
  "t0"."cw",
  "t0"."ac",
  "t0"."ae",
  "t0"."ab",
  "t0"."ad",
  "t0"."h",
  "t1"."cx"
FROM
  "m" as "t0"
  LEFT JOIN "ad" as "t1" ON "t0"."cv" = "t1"."cv"
  and "t0"."ay" = "t1"."ay"
WHERE
  "t0"."cv" = 3023424::int
  and coalesce ("t0"."bb"::int, 0::int) <> 1::int
  and "t0"."w" = '2024-02-28 0:00:00.0 +00:00:00'::datetime
  and "t0"."cu" = 3465::int
  and "t0"."bj" is null
  and "t0"."z" = 11::int
''
plan:
    [0] SEARCH TABLE m AS t0 USING COVERING INDEX ix_af (cv=? AND w=?) (~6 rows)
        [0] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
''
buckets = [1222]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "COL_0" as "z",
      "COL_5" as "cw",
      "COL_3" as "ay",
      "COL_4" as "bg",
      "COL_1" as "cu",
      "COL_2" as "d",
      "COL_6" as "ac",
      "COL_9" as "ad",
      "COL_7" as "ae",
      "COL_8" as "ab",
      0::int as "c10",
      "COL_10" as "h",
      "COL_11" as "cx",
      row_number () OVER () as "c13"
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
          "COL_10",
          "COL_11"
        FROM
          "_tmp_2939879774221965089_1136"
      )
  ) as "cte0"
WHERE
  "cte0"."c13" > 0::int
LIMIT
  10
''
plan:
    [2] SCAN TABLE _tmp_2939879774221965089_1136 (~1048576 rows)
    [1] SCAN SUBQUERY 2 (~1 row)
    [0] SCAN SUBQUERY 1 AS cte0 (~1 row)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q36
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  sum(t0.aa::decimal::decimal)::decimal -> c0,
  count(*)::int -> c1
)
  selection (
    (
      t0.cv::int = 3023424::int
      and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
      and t0.w::datetime = '2024-02-28 0:00:00.0 +00:00:00'::datetime
      and t0.cu::int = 3465::int
      and t0.bj::int is null
      and t0.z::int = 11::int
    )
  )
    scan m -> t0
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  sum ("t0"."aa"::decimal) as "c0",
  count (*) as "c1"
FROM
  "m" as "t0"
WHERE
  "t0"."cv" = 3023424::int
  and coalesce ("t0"."bb"::int, 0::int) <> 1::int
  and "t0"."w" = '2024-02-28 0:00:00.0 +00:00:00'::datetime
  and "t0"."cu" = 3465::int
  and "t0"."bj" is null
  and "t0"."z" = 11::int
''
plan:
    [0] SEARCH TABLE m AS t0 USING COVERING INDEX ix_af (cv=? AND w=?) (~6 rows)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q37
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t0.bm::int -> c0,
  t0.bg::text -> c1,
  t0.cu::int -> c2,
  t0.ah::text -> c3,
  t0.ay::text -> c4,
  t2.cx::text -> c5
)
  selection (
    (
      t0.cv::int = 3023424::int
      and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
      and t0.w::datetime = '2024-02-28 0:00:00.0 +00:00:00'::datetime
      and t0.cu::int = 3465::int
      and t0.z::int = 11::int
    )
  )
    left join on (
      ROW(t0.cv::int, t0.ay::text) = ROW(t2.cv::int, t2.ay::text)
    )
      left join on (t0.cu::int = t1.cu::int)
        scan m -> t0
        scan g -> t1
      scan ad -> t2
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT DISTINCT
  "t0"."bm" as "c0",
  "t0"."bg" as "c1",
  "t0"."cu" as "c2",
  "t0"."ah" as "c3",
  "t0"."ay" as "c4",
  "t2"."cx" as "c5"
FROM
  "m" as "t0"
  LEFT JOIN "g" as "t1" ON "t0"."cu" = "t1"."cu"
  LEFT JOIN "ad" as "t2" ON "t0"."cv" = "t2"."cv"
  and "t0"."ay" = "t2"."ay"
WHERE
  "t0"."cv" = 3023424::int
  and coalesce ("t0"."bb"::int, 0::int) <> 1::int
  and "t0"."w" = '2024-02-28 0:00:00.0 +00:00:00'::datetime
  and "t0"."cu" = 3465::int
  and "t0"."z" = 11::int
''
plan:
    [0] SEARCH TABLE m AS t0 USING COVERING INDEX ix_af (cv=? AND w=?) (~7 rows)
        [0] SEARCH TABLE g AS t1 USING PRIMARY KEY (cu=?) (~1 row)
            [0] SEARCH TABLE ad AS t2 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [0] USE TEMP B-TREE FOR DISTINCT
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q38
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  10;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 10
  projection (
    cte0.a::text -> a,
    cte0.z::int -> z,
    cte0.cu::int -> cu,
    cte0.bj::int -> bj,
    cte0.c4::decimal -> c4,
    cte0.f::decimal -> f,
    cte0.g::decimal -> g,
    cte0.c7::decimal -> c7,
    cte0.c8::int -> c8,
    cte0.c9::int -> c9
  )
    selection (cte0.c9::int > 0::int)
      scan cte cte0($0)
subquery $0:
  projection (
    to_char(w::datetime::datetime, '%Y-%m-%d'::text)::text -> a,
    z::int,
    cu::int,
    bj::int,
    sum(
      case 
        when (aa::decimal > 0::int and not ao::bool) then aa::decimal
        else 0::int
      end::decimal
    )::decimal -> c4,
    sum(
      case 
        when (aa::decimal < 0::int and not ao::bool) then aa::decimal
        else 0::int
      end::decimal
    )::decimal -> f,
    sum(
      case 
        when (aa::decimal > 0::int and ao::bool) then aa::decimal
        else 0::int
      end::decimal
    )::decimal -> g,
    sum(
      case 
        when aw::int = 2::int then aa::decimal
        else 0::int
      end::decimal
    )::decimal -> c7,
    0::int -> c8,
    row_number() over (order by (
      w::datetime asc,
      cu::int is null asc,
      cu::int asc,
      bj::int is null asc,
      bj::int asc,
      z::int is null asc,
      z::int desc
    ) ) -> c9
  )
    group by (w::datetime, bj::int, cu::int, z::int)
      motion [policy: full, program: ReshardIfNeeded]
        projection (
          t0.ag::int -> ag,
          t0.cv::int -> cv,
          t0.cr::int -> cr,
          t0.bucket_id::int -> bucket_id,
          t0.w::datetime -> w,
          t0.z::int -> z,
          t0.cu::int -> cu,
          t0.bj::int -> bj,
          t0.bm::int -> bm,
          t0.aw::int -> aw,
          t0.aa::decimal -> aa,
          t0.ao::bool -> ao,
          t0.bb::int -> bb,
          t0.ay::text -> ay,
          t0.bg::text -> bg,
          t0.ah::text -> ah,
          t0.cw::int -> cw,
          t0.ac::decimal -> ac,
          t0.ae::text -> ae,
          t0.ab::datetime -> ab,
          t0.ad::int -> ad,
          t0.h::int -> h,
          t1.cu::int -> cu,
          t1.cw::int -> cw
        )
          selection (
            (
              t0.cv::int = 3023424::int
              and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
              and t0.w::datetime >= '2023-01-01 0:00:00.0 +00:00:00'::datetime
              and t0.w::datetime <= '2023-05-01 0:00:00.0 +00:00:00'::datetime
              and t0.aw::int <> 2::int
            )
          )
            left join on (t0.cu::int = t1.cu::int)
              scan m -> t0
              scan g -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ag",
  "t0"."cv",
  "t0"."cr",
  "t0"."bucket_id",
  "t0"."w",
  "t0"."z",
  "t0"."cu",
  "t0"."bj",
  "t0"."bm",
  "t0"."aw",
  "t0"."aa",
  "t0"."ao",
  "t0"."bb",
  "t0"."ay",
  "t0"."bg",
  "t0"."ah",
  "t0"."cw",
  "t0"."ac",
  "t0"."ae",
  "t0"."ab",
  "t0"."ad",
  "t0"."h",
  "t1"."cu",
  "t1"."cw"
FROM
  "m" as "t0"
  LEFT JOIN "g" as "t1" ON "t0"."cu" = "t1"."cu"
WHERE
  "t0"."cv" = 3023424::int
  and coalesce ("t0"."bb"::int, 0::int) <> 1::int
  and "t0"."w" >= '2023-01-01 0:00:00.0 +00:00:00'::datetime
  and "t0"."w" <= '2023-05-01 0:00:00.0 +00:00:00'::datetime
  and "t0"."aw" <> 2::int
''
plan:
    [0] SEARCH TABLE m AS t0 USING COVERING INDEX ix_af (cv=? AND w>? AND w<?) (~1 row)
        [0] SEARCH TABLE g AS t1 USING PRIMARY KEY (cu=?) (~1 row)
''
buckets = [1222]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "to_char" ("COL_4"::datetime, '%Y-%m-%d'::text) as "a",
      "COL_5" as "z",
      "COL_6" as "cu",
      "COL_7" as "bj",
      sum (
        CASE
          WHEN "COL_10" > 0::int
          and not "COL_11" THEN "COL_10"
          ELSE 0::int
        END::decimal
      ) as "c4",
      sum (
        CASE
          WHEN "COL_10" < 0::int
          and not "COL_11" THEN "COL_10"
          ELSE 0::int
        END::decimal
      ) as "f",
      sum (
        CASE
          WHEN "COL_10" > 0::int
          and "COL_11" THEN "COL_10"
          ELSE 0::int
        END::decimal
      ) as "g",
      sum (
        CASE
          WHEN "COL_9" = 2::int THEN "COL_10"
          ELSE 0::int
        END::decimal
      ) as "c7",
      0::int as "c8",
      row_number () OVER (
        ORDER BY
          "COL_4" DESC,
          "COL_6" is null ASC,
          "COL_6" ASC,
          "COL_7" is null ASC,
          "COL_7" ASC,
          "COL_5" is null ASC,
          "COL_5" ASC
      ) as "c9"
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
          "COL_10",
          "COL_11",
          "COL_12",
          "COL_13",
          "COL_14",
          "COL_15",
          "COL_16",
          "COL_17",
          "COL_18",
          "COL_19",
          "COL_20",
          "COL_21",
          "COL_22",
          "COL_23"
        FROM
          "_tmp_4032040051388636457_1136"
      )
    GROUP BY
      "COL_4",
      "COL_7",
      "COL_6",
      "COL_5"
  ) as "cte0"
WHERE
  "cte0"."c9" > 0::int
LIMIT
  10
''
plan:
    [2] SCAN TABLE _tmp_4032040051388636457_1136 (~1048576 rows)
    [2] USE TEMP B-TREE FOR GROUP BY
    [2] USE TEMP B-TREE FOR ORDER BY
    [1] SCAN SUBQUERY 2 (~1 row)
    [0] SCAN SUBQUERY 1 AS cte0 (~1 row)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q39
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  ) AS t2;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (count(*)::int -> c4)
  scan t2
    projection (
      count(*)::int -> c4,
      cte0.a::text -> a,
      cte0.z::int -> z
    )
      group by (
        cte0.a::text,
        cte0.bj::int,
        cte0.cu::int,
        cte0.z::int
      )
        scan cte cte0($0)
subquery $0:
  projection (
    to_char(t0.w::datetime::datetime, '%Y-%m-%d'::text)::text -> a,
    t0.bj::int -> bj,
    t0.cu::int -> cu,
    t0.z::int -> z
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
        and t0.w::datetime >= '2023-01-01 0:00:00.0 +00:00:00'::datetime
        and t0.w::datetime <= '2023-05-01 0:00:00.0 +00:00:00'::datetime
        and t0.aw::int <> 2::int
      )
    )
      left join on (t0.cu::int = t1.cu::int)
        scan m -> t0
        scan g -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  count (*) as "c4"
FROM
  (
    SELECT
      count (*) as "c4",
      "cte0"."a",
      "cte0"."z"
    FROM
      (
        SELECT
          "to_char" ("t0"."w"::datetime, '%Y-%m-%d'::text) as "a",
          "t0"."bj",
          "t0"."cu",
          "t0"."z"
        FROM
          "m" as "t0"
          LEFT JOIN "g" as "t1" ON "t0"."cu" = "t1"."cu"
        WHERE
          "t0"."cv" = 3023424::int
          and coalesce ("t0"."bb"::int, 0::int) <> 1::int
          and "t0"."w" >= '2023-01-01 0:00:00.0 +00:00:00'::datetime
          and "t0"."w" <= '2023-05-01 0:00:00.0 +00:00:00'::datetime
          and "t0"."aw" <> 2::int
      ) as "cte0"
    GROUP BY
      "cte0"."a",
      "cte0"."bj",
      "cte0"."cu",
      "cte0"."z"
  ) as "t2"
''
plan:
    [1] SEARCH TABLE m AS t0 USING COVERING INDEX ix_af (cv=? AND w>? AND w<?) (~1 row)
        [1] SEARCH TABLE g AS t1 USING PRIMARY KEY (cu=?) (~1 row)
    [1] USE TEMP B-TREE FOR GROUP BY
    [0] SCAN SUBQUERY 1 AS t2 (~1 row)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q40
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t0.bm::int -> c0,
  t0.bg::text -> c1,
  t0.cu::int -> c2,
  t0.ah::text -> c3,
  t0.ay::text -> c4,
  t2.cx::text -> c5
)
  selection (
    (
      t0.cv::int = 3023424::int
      and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
      and t0.w::datetime >= '2010-01-01 0:00:00.0 +00:00:00'::datetime
      and t0.w::datetime <= '2026-01-01 0:00:00.0 +00:00:00'::datetime
      and t0.aw::int <> 2::int
    )
  )
    left join on (
      ROW(t0.cv::int, t0.ay::text) = ROW(t2.cv::int, t2.ay::text)
    )
      left join on (t0.cu::int = t1.cu::int)
        scan m -> t0
        scan g -> t1
      scan ad -> t2
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT DISTINCT
  "t0"."bm" as "c0",
  "t0"."bg" as "c1",
  "t0"."cu" as "c2",
  "t0"."ah" as "c3",
  "t0"."ay" as "c4",
  "t2"."cx" as "c5"
FROM
  "m" as "t0"
  LEFT JOIN "g" as "t1" ON "t0"."cu" = "t1"."cu"
  LEFT JOIN "ad" as "t2" ON "t0"."cv" = "t2"."cv"
  and "t0"."ay" = "t2"."ay"
WHERE
  "t0"."cv" = 3023424::int
  and coalesce ("t0"."bb"::int, 0::int) <> 1::int
  and "t0"."w" >= '2010-01-01 0:00:00.0 +00:00:00'::datetime
  and "t0"."w" <= '2026-01-01 0:00:00.0 +00:00:00'::datetime
  and "t0"."aw" <> 2::int
''
plan:
    [0] SEARCH TABLE m AS t0 USING COVERING INDEX ix_af (cv=? AND w>? AND w<?) (~1 row)
        [0] SEARCH TABLE g AS t1 USING PRIMARY KEY (cu=?) (~1 row)
            [0] SEARCH TABLE ad AS t2 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [0] USE TEMP B-TREE FOR DISTINCT
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q41
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
motion [policy: full, program: RemoveDuplicates]
  union
    projection (
      cte0.ag::int -> ag,
      cte0.v::int -> v,
      cte0.bx::decimal -> bx,
      cte0.bz::decimal -> bz,
      cte0.ca::decimal -> ca,
      cte0.cc::decimal -> cc,
      cte0.cd::decimal -> cd,
      cte0.ar::datetime -> ar,
      cte0.cg::decimal -> cg,
      cte0.ce::decimal -> ce,
      cte0.i::datetime -> i,
      cte0.cf::decimal -> cf,
      cte0.cv::int -> cv,
      cte0.aq::bool -> aq
    )
      scan cte cte0($0)
    projection (
      t0.ag::int -> ag,
      t0.v::int -> v,
      t0.bx::decimal -> bx,
      t0.bz::decimal -> bz,
      t0.ca::decimal -> ca,
      t0.cc::decimal -> cc,
      t0.cd::decimal -> cd,
      t0.ar::datetime -> ar,
      t0.cg::decimal -> cg,
      t0.ce::decimal -> ce,
      t0.i::datetime -> i,
      t0.cf::decimal -> cf,
      t0.cv::int -> cv,
      t0.aq::bool -> aq
    )
      selection (
        (
          t0.cv::int = 3023424::int
          and not coalesce(t0.aq::bool::bool, false::bool)::bool
          and to_char(t0.ar::datetime::datetime, '%Y-%m-%d'::text)::text::datetime > '2020-01-02 0:00:00.0 +00:00:00'::datetime
          and t0.ar::datetime >= '2010-01-01 0:00:00.0 +00:00:00'::datetime
          and t0.ar::datetime <= '2026-01-01 0:00:00.0 +00:00:00'::datetime
        )
      )
        left join on (
          ROW(t0.v::int, t0.cv::int) = ROW(t1.ag::int, t1.cv::int)
        )
          scan u -> t0
          scan s -> t1
subquery $0:
  limit 1
    projection (
      ag::int,
      v::int,
      bx::decimal,
      bz::decimal,
      ca::decimal,
      cc::decimal,
      cd::decimal,
      ar::datetime,
      cg::decimal,
      ce::decimal,
      i::datetime,
      cf::decimal,
      cv::int,
      aq::bool
    )
      order by (ar::datetime desc)
        scan
          projection (
            t0.ag::int -> ag,
            t0.v::int -> v,
            t0.bx::decimal -> bx,
            t0.bz::decimal -> bz,
            t0.ca::decimal -> ca,
            t0.cc::decimal -> cc,
            t0.cd::decimal -> cd,
            t0.ar::datetime -> ar,
            t0.cg::decimal -> cg,
            t0.ce::decimal -> ce,
            t0.i::datetime -> i,
            t0.cf::decimal -> cf,
            t0.cv::int -> cv,
            t0.aq::bool -> aq
          )
            selection (
              (
                t0.cv::int = 3023424::int
                and not coalesce(t0.aq::bool::bool, false::bool)::bool
                and to_char(t0.ar::datetime::datetime, '%Y-%m-%d'::text)::text::datetime > '2020-01-02 0:00:00.0 +00:00:00'::datetime
                and t0.ar::datetime < '2010-01-01 0:00:00.0 +00:00:00'::datetime
              )
            )
              left join on (
                ROW(t0.v::int, t0.cv::int) = ROW(t1.ag::int, t1.cv::int)
              )
                scan u -> t0
                scan s -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "ag",
      "v",
      "bx",
      "bz",
      "ca",
      "cc",
      "cd",
      "ar",
      "cg",
      "ce",
      "i",
      "cf",
      "cv",
      "aq"
    FROM
      (
        SELECT
          "t0"."ag",
          "t0"."v",
          "t0"."bx",
          "t0"."bz",
          "t0"."ca",
          "t0"."cc",
          "t0"."cd",
          "t0"."ar",
          "t0"."cg",
          "t0"."ce",
          "t0"."i",
          "t0"."cf",
          "t0"."cv",
          "t0"."aq"
        FROM
          "u" as "t0"
          LEFT JOIN "s" as "t1" ON "t0"."v" = "t1"."ag"
          and "t0"."cv" = "t1"."cv"
        WHERE
          "t0"."cv" = 3023424::int
          and not coalesce ("t0"."aq"::bool, false::bool)
          and "to_char" ("t0"."ar"::datetime, '%Y-%m-%d'::text)::datetime > '2020-01-02 0:00:00.0 +00:00:00'::datetime
          and "t0"."ar" < '2010-01-01 0:00:00.0 +00:00:00'::datetime
      )
    ORDER BY
      "ar" DESC
    LIMIT
      1
  ) as "cte0"
UNION
SELECT
  "t0"."ag",
  "t0"."v",
  "t0"."bx",
  "t0"."bz",
  "t0"."ca",
  "t0"."cc",
  "t0"."cd",
  "t0"."ar",
  "t0"."cg",
  "t0"."ce",
  "t0"."i",
  "t0"."cf",
  "t0"."cv",
  "t0"."aq"
FROM
  "u" as "t0"
  LEFT JOIN "s" as "t1" ON "t0"."v" = "t1"."ag"
  and "t0"."cv" = "t1"."cv"
WHERE
  "t0"."cv" = 3023424::int
  and not coalesce ("t0"."aq"::bool, false::bool)
  and "to_char" ("t0"."ar"::datetime, '%Y-%m-%d'::text)::datetime > '2020-01-02 0:00:00.0 +00:00:00'::datetime
  and "t0"."ar" >= '2010-01-01 0:00:00.0 +00:00:00'::datetime
  and "t0"."ar" <= '2026-01-01 0:00:00.0 +00:00:00'::datetime
''
plan:
    [2] SEARCH TABLE u AS t0 USING COVERING INDEX ix_bi (cv=? AND ar<?) (~2 rows)
        [2] SEARCH TABLE s AS t1 USING PRIMARY KEY (ag=?) (~1 row)
    [1] SCAN SUBQUERY 2 AS cte0 (~1 row)
    [3] SEARCH TABLE u AS t0 
         USING COVERING INDEX ix_bi 
         (cv=? AND ar>? AND ar<?) 
         (~1 row)
        [3] SEARCH TABLE s AS t1 USING PRIMARY KEY (ag=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 3 USING TEMP B-TREE (UNION)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q42
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  AND COALESCE(t0.bb, 0) <> 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  coalesce(
    sum(
      case 
        when t0.aa::decimal is null then 0.0::decimal
        else t0.aa::decimal
      end::decimal
    )::decimal::decimal,
    0.0::decimal
  )::decimal -> c0
)
  selection (
    (
      t0.cv::int = 3023424::int
      and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
      and t0.w::datetime >= '2010-01-01 0:00:00.0 +00:00:00'::datetime
      and t0.w::datetime <= '2026-01-01 0:00:00.0 +00:00:00'::datetime
      and t0.aw::int = 2::int
      and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
    )
  )
    left join on (t0.cu::int = t1.cu::int)
      scan m -> t0
      scan g -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  coalesce (
    sum (
      CASE
        WHEN "t0"."aa" is null THEN 0.0::decimal
        ELSE "t0"."aa"
      END::decimal
    )::decimal,
    0.0::decimal
  ) as "c0"
FROM
  "m" as "t0"
  LEFT JOIN "g" as "t1" ON "t0"."cu" = "t1"."cu"
WHERE
  "t0"."cv" = 3023424::int
  and coalesce ("t0"."bb"::int, 0::int) <> 1::int
  and "t0"."w" >= '2010-01-01 0:00:00.0 +00:00:00'::datetime
  and "t0"."w" <= '2026-01-01 0:00:00.0 +00:00:00'::datetime
  and "t0"."aw" = 2::int
  and coalesce ("t0"."bb"::int, 0::int) <> 1::int
''
plan:
    [0] SEARCH TABLE m AS t0 
         USING COVERING INDEX ix_ak 
         (cv=? AND aw=? AND w>? AND w<?) 
         (~1 row)
        [0] SEARCH TABLE g AS t1 USING PRIMARY KEY (cu=?) (~1 row)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q43
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  AND COALESCE(t0.bb, 0) <> 1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  coalesce(
    sum(
      case 
        when t0.aa::decimal is null then 0.0::decimal
        else t0.aa::decimal
      end::decimal
    )::decimal::decimal,
    0.0::decimal
  )::decimal -> c0
)
  selection (
    (
      t0.cv::int = 3023424::int
      and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
      and t0.w::datetime >= '2010-01-01 0:00:00.0 +00:00:00'::datetime
      and t0.w::datetime <= '2026-01-01 0:00:00.0 +00:00:00'::datetime
      and t0.aw::int <> 2::int
      and t0.aw::int = 2::int
      and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
    )
  )
    left join on (t0.cu::int = t1.cu::int)
      scan m -> t0
      scan g -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  coalesce (
    sum (
      CASE
        WHEN "t0"."aa" is null THEN 0.0::decimal
        ELSE "t0"."aa"
      END::decimal
    )::decimal,
    0.0::decimal
  ) as "c0"
FROM
  "m" as "t0"
  LEFT JOIN "g" as "t1" ON "t0"."cu" = "t1"."cu"
WHERE
  "t0"."cv" = 3023424::int
  and coalesce ("t0"."bb"::int, 0::int) <> 1::int
  and "t0"."w" >= '2010-01-01 0:00:00.0 +00:00:00'::datetime
  and "t0"."w" <= '2026-01-01 0:00:00.0 +00:00:00'::datetime
  and "t0"."aw" <> 2::int
  and "t0"."aw" = 2::int
  and coalesce ("t0"."bb"::int, 0::int) <> 1::int
''
plan:
    [0] SEARCH TABLE m AS t0 
         USING COVERING INDEX ix_ak 
         (cv=? AND aw=? AND w>? AND w<?) 
         (~1 row)
        [0] SEARCH TABLE g AS t1 USING PRIMARY KEY (cu=?) (~1 row)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q44
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  (t0.cv = 3023424);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t0.bm::int -> c0,
  t0.bg::text -> c1,
  t0.ah::text -> c2,
  t0.ay::text -> c3,
  t1.cx::text -> c4
)
  selection (t0.cv::int = 3023424::int)
    left join on (
      ROW(t0.cv::int, t0.ay::text) = ROW(t1.cv::int, t1.ay::text)
    )
      scan c -> t0
      scan ad -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT DISTINCT
  "t0"."bm" as "c0",
  "t0"."bg" as "c1",
  "t0"."ah" as "c2",
  "t0"."ay" as "c3",
  "t1"."cx" as "c4"
FROM
  "c" as "t0"
  LEFT JOIN "ad" as "t1" ON "t0"."cv" = "t1"."cv"
  and "t0"."ay" = "t1"."ay"
WHERE
  "t0"."cv" = 3023424::int
''
plan:
    [0] SEARCH TABLE c AS t0 USING PRIMARY KEY (cv=?) (~10 rows)
        [0] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q45
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  to_char(
    coalesce(
      t0.cs::datetime::datetime,
      t0.a::datetime::datetime
    )::datetime::datetime,
    '%Y'::text
  )::text::int -> c0,
  t0.bg::text -> c1,
  t0.ah::text -> c2,
  t0.ay::text -> c3,
  t2.cx::text -> c4
)
  selection (
    (
      t0.cv::int = 3023424::int
      and t1.ag::int is null
      and (t0.h::int is null or t0.h::int = -1::int)
    )
  )
    left join on (
      ROW(t0.cv::int, t0.ay::text) = ROW(t2.cv::int, t2.ay::text)
    )
      left join on (
        ROW(t0.ag::int, t0.cv::int) = ROW(t1.h::int, t1.cv::int)
      )
        scan n -> t0
        scan n -> t1
      scan ad -> t2
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT DISTINCT
  "to_char" (
    coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
    '%Y'::text
  )::int as "c0",
  "t0"."bg" as "c1",
  "t0"."ah" as "c2",
  "t0"."ay" as "c3",
  "t2"."cx" as "c4"
FROM
  "n" as "t0"
  LEFT JOIN "n" as "t1" ON "t1"."h" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
  LEFT JOIN "ad" as "t2" ON "t0"."cv" = "t2"."cv"
  and "t0"."ay" = "t2"."ay"
WHERE
  "t0"."cv" = 3023424::int
  and "t1"."ag" is null
  and (
    "t0"."h" is null
    or "t0"."h" = (-1)::int
  )
''
plan:
    [0] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [0] SEARCH TABLE n AS t1 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [0] SEARCH TABLE ad AS t2 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [0] USE TEMP B-TREE FOR DISTINCT
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q46
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  t1.cq;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t1.cq::int -> c0,
  case 
    when not min(t2.u::datetime::datetime)::datetime is null then to_char(
      min(t2.u::datetime::datetime)::datetime::datetime,
      '%Y-%m-%d'::text
    )::text
    else NULL::unknown
  end -> c1,
  sum(t0.az::decimal::decimal)::decimal -> c2
)
  group by (t1.cq::int)
    selection (
      (
        t0.cv::int = 3023424::int
        and t1.aw::int in ROW(0::int, 3::int, 42::int, 45::int, 11::int)
        and not t3.ap::bool
        and t0.az::decimal > 0::int
      )
    )
      left join on (t1.cu::int = t3.br::int)
        join on (
          ROW(t0.b::int, t0.cv::int) = ROW(t2.b::int, t2.cv::int)
        )
          join on (
            ROW(t0.au::int, t0.cv::int) = ROW(t1.ag::int, t1.cv::int)
          )
            scan v -> t0
            scan y -> t1
          scan x -> t2
        scan i -> t3
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t1"."cq" as "c0",
  CASE
    WHEN not min ("t2"."u"::datetime) is null THEN "to_char" (min ("t2"."u"::datetime)::datetime, '%Y-%m-%d'::text)
    ELSE NULL
  END as "c1",
  sum ("t0"."az"::decimal) as "c2"
FROM
  "v" as "t0"
  INNER JOIN "y" as "t1" ON "t0"."au" = "t1"."ag"
  and "t0"."cv" = "t1"."cv"
  INNER JOIN "x" as "t2" ON "t0"."b" = "t2"."b"
  and "t0"."cv" = "t2"."cv"
  LEFT JOIN "i" as "t3" ON "t1"."cu" = "t3"."br"
WHERE
  "t0"."cv" = 3023424::int
  and "t1"."aw" in (0::int, 3::int, 42::int, 45::int, 11::int)
  and not "t3"."ap"
  and "t0"."az" > 0::int
GROUP BY
  "t1"."cq"
''
plan:
    [0] SEARCH TABLE v AS t0 USING COVERING INDEX ix_bk (cv=?) (~9 rows)
        [0] SEARCH TABLE y AS t1 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
    [0] EXECUTE LIST SUBQUERY 1
            [0] SEARCH TABLE x AS t2 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
                [0] SEARCH TABLE i AS t3 USING PRIMARY KEY (br=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q47
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  (cv = 3023424);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t0.aw::int -> c0,
  case 
    when t1.cw::int = 3::int then true::bool
    else false::bool
  end -> c1,
  t0.ay::text -> c2,
  t0.cu::int -> c3,
  t0.bg::text -> c4,
  t0.ah::text -> c5,
  case 
    when (
      t0.aw::int = 0::int
      and (t1.cw::int is null or t1.cw::int <> 3::int)
    ) then case 
      when t0.bx::decimal < 0::int then t0.bx::decimal
      else 0.0::decimal
    end
    else 0.0::decimal
  end -> c6,
  case 
    when t0.aw::int = 3::int then case 
      when t0.ca::decimal < 0::int then t0.ca::decimal
      else 0.0::decimal
    end
    else 0.0::decimal
  end -> c7,
  case 
    when (
      t0.aw::int = 0::int
      and not t1.cw::int is null
      and t1.cw::int = 3::int
    ) then case 
      when t0.bx::decimal < 0::int then t0.bx::decimal
      else 0.0::decimal
    end
    else 0.0::decimal
  end -> c8,
  case 
    when t0.aw::int = 11::int then case 
      when t0.cg::decimal < 0::int then t0.cg::decimal
      else 0.0::decimal
    end
    else 0.0::decimal
  end -> c9,
  case 
    when t0.aw::int in ROW(42::int, 45::int) then case 
      when t0.cd::decimal < 0::int then t0.cd::decimal
      else 0.0::decimal
    end + case 
      when t0.cc::decimal < 0::int then t0.cc::decimal
      else 0.0::decimal
    end
    else 0.0::decimal
  end -> c10,
  case 
    when t0.aw::int = 2::int then case 
      when t0.bz::decimal < 0::int then t0.bz::decimal
      else 0.0::decimal
    end
    else 0.0::decimal
  end -> c11,
  case 
    when (
      t1.cw::int = 3::int
      and t0.bx::decimal > 0::int
    ) then t0.bx::decimal
    else 0.0::decimal
  end -> c12,
  case 
    when (
      t0.aw::int = 1::int
      and t0.bx::decimal > 0::int
    ) then t0.bx::decimal
    else 0.0::decimal
  end -> c13,
  NULL::unknown -> c14
)
  selection (t0.cv::int = 3023424::int)
    left join on (t0.cu::int = t1.cu::int)
      scan y -> t0
      scan g -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."aw" as "c0",
  CASE
    WHEN "t1"."cw" = 3::int THEN true::bool
    ELSE false::bool
  END as "c1",
  "t0"."ay" as "c2",
  "t0"."cu" as "c3",
  "t0"."bg" as "c4",
  "t0"."ah" as "c5",
  CASE
    WHEN "t0"."aw" = 0::int
    and (
      "t1"."cw" is null
      or "t1"."cw" <> 3::int
    ) THEN CASE
      WHEN "t0"."bx" < 0::int THEN "t0"."bx"
      ELSE 0.0::decimal
    END
    ELSE 0.0::decimal
  END as "c6",
  CASE
    WHEN "t0"."aw" = 3::int THEN CASE
      WHEN "t0"."ca" < 0::int THEN "t0"."ca"
      ELSE 0.0::decimal
    END
    ELSE 0.0::decimal
  END as "c7",
  CASE
    WHEN "t0"."aw" = 0::int
    and not "t1"."cw" is null
    and "t1"."cw" = 3::int THEN CASE
      WHEN "t0"."bx" < 0::int THEN "t0"."bx"
      ELSE 0.0::decimal
    END
    ELSE 0.0::decimal
  END as "c8",
  CASE
    WHEN "t0"."aw" = 11::int THEN CASE
      WHEN "t0"."cg" < 0::int THEN "t0"."cg"
      ELSE 0.0::decimal
    END
    ELSE 0.0::decimal
  END as "c9",
  CASE
    WHEN "t0"."aw" in (42::int, 45::int) THEN CASE
      WHEN "t0"."cd" < 0::int THEN "t0"."cd"
      ELSE 0.0::decimal
    END + CASE
      WHEN "t0"."cc" < 0::int THEN "t0"."cc"
      ELSE 0.0::decimal
    END
    ELSE 0.0::decimal
  END as "c10",
  CASE
    WHEN "t0"."aw" = 2::int THEN CASE
      WHEN "t0"."bz" < 0::int THEN "t0"."bz"
      ELSE 0.0::decimal
    END
    ELSE 0.0::decimal
  END as "c11",
  CASE
    WHEN "t1"."cw" = 3::int
    and "t0"."bx" > 0::int THEN "t0"."bx"
    ELSE 0.0::decimal
  END as "c12",
  CASE
    WHEN "t0"."aw" = 1::int
    and "t0"."bx" > 0::int THEN "t0"."bx"
    ELSE 0.0::decimal
  END as "c13",
  NULL as "c14"
FROM
  "y" as "t0"
  LEFT JOIN "g" as "t1" ON "t0"."cu" = "t1"."cu"
WHERE
  "t0"."cv" = 3023424::int
''
plan:
    [0] SEARCH TABLE y AS t0 USING COVERING INDEX ix_bv (cv=?) (~10 rows)
        [0] SEARCH TABLE g AS t1 USING PRIMARY KEY (cu=?) (~1 row)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q48
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  cte0 t0;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t0.aw::int -> c0,
  t0.ap::bool -> c3,
  t0.ay::text -> c4,
  t0.cu::int -> c1,
  t0.bg::text -> c5,
  t0.ah::text -> c6,
  case 
    when (t0.aw::int = 0::int and not t0.ap::bool) then t0.aa::decimal
    else 0.0::decimal
  end -> c7,
  case 
    when t0.aw::int = 3::int then t0.aa::decimal
    else 0.0::decimal
  end -> c8,
  case 
    when (t0.aw::int = 0::int and t0.ap::bool) then t0.aa::decimal
    else 0.0::decimal
  end -> c9,
  case 
    when t0.aw::int = 11::int then t0.aa::decimal
    else 0.0::decimal
  end -> c10,
  case 
    when t0.aw::int in ROW(42::int, 45::int) then t0.aa::decimal
    else 0.0::decimal
  end -> c11,
  case 
    when t0.aw::int = 2::int then t0.aa::decimal
    else 0.0::decimal
  end -> c12,
  0::int -> c13,
  0::int -> c14,
  to_char(
    coalesce(
      t0.cs::datetime::datetime,
      t0.a::datetime::datetime
    )::datetime::datetime,
    '%Y-%m-%d'::text
  )::text -> c15
)
  scan cte t0($0)
subquery $0:
  projection (
    t0.ag::int -> ag,
    coalesce(t1.aw::int::int, t0.bf::int::int)::int -> aw,
    t0.ay::text -> ay,
    coalesce(t1.cu::int::int, t0.cu::int::int)::int -> cu,
    t0.bg::text -> bg,
    t0.ah::text -> ah,
    t0.cs::datetime -> cs,
    t0.a::datetime -> a,
    t0.h::int -> h,
    t0.cv::int -> cv,
    coalesce(
      (
        t0.d::decimal * coalesce(t3.cj::int::decimal, 0.0::decimal)::decimal * case 
          when not t0.h::int is null then -1::int
          else 1::int
        end
      )::decimal,
      0.0::decimal
    )::decimal -> aa,
    case 
      when (not t2.cw::int is null and t2.cw::int = 3::int) then true::bool
      else false::bool
    end -> ap
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and t4.ag::int is null
        and (t0.h::int is null or t0.h::int = -1::int)
      )
    )
      left join on (
        ROW(t0.ag::int, t0.cv::int) = ROW(t4.h::int, t4.cv::int)
      )
        left join on (t0.cw::int = t3.cw::int)
          left join on (
            coalesce(t1.cu::int::int, t0.cu::int::int)::int = t2.cu::int
          )
            left join on (
              ROW(t0.au::int, t0.cv::int) = ROW(t1.ag::int, t1.cv::int)
            )
              scan n -> t0
              scan y -> t1
            scan g -> t2
          scan f -> t3
        scan n -> t4
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."aw" as "c0",
  "t0"."ap" as "c3",
  "t0"."ay" as "c4",
  "t0"."cu" as "c1",
  "t0"."bg" as "c5",
  "t0"."ah" as "c6",
  CASE
    WHEN "t0"."aw" = 0::int
    and not "t0"."ap" THEN "t0"."aa"
    ELSE 0.0::decimal
  END as "c7",
  CASE
    WHEN "t0"."aw" = 3::int THEN "t0"."aa"
    ELSE 0.0::decimal
  END as "c8",
  CASE
    WHEN "t0"."aw" = 0::int
    and "t0"."ap" THEN "t0"."aa"
    ELSE 0.0::decimal
  END as "c9",
  CASE
    WHEN "t0"."aw" = 11::int THEN "t0"."aa"
    ELSE 0.0::decimal
  END as "c10",
  CASE
    WHEN "t0"."aw" in (42::int, 45::int) THEN "t0"."aa"
    ELSE 0.0::decimal
  END as "c11",
  CASE
    WHEN "t0"."aw" = 2::int THEN "t0"."aa"
    ELSE 0.0::decimal
  END as "c12",
  0::int as "c13",
  0::int as "c14",
  "to_char" (
    coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
    '%Y-%m-%d'::text
  ) as "c15"
FROM
  (
    SELECT
      "t0"."ag",
      coalesce ("t1"."aw"::int, "t0"."bf"::int) as "aw",
      "t0"."ay",
      coalesce ("t1"."cu"::int, "t0"."cu"::int) as "cu",
      "t0"."bg",
      "t0"."ah",
      "t0"."cs",
      "t0"."a",
      "t0"."h",
      "t0"."cv",
      coalesce (
        (
          "t0"."d" * coalesce ("t3"."cj"::decimal, 0.0::decimal) * CASE
            WHEN not "t0"."h" is null THEN (-1)::int
            ELSE 1::int
          END
        )::decimal,
        0.0::decimal
      ) as "aa",
      CASE
        WHEN not "t2"."cw" is null
        and "t2"."cw" = 3::int THEN true::bool
        ELSE false::bool
      END as "ap"
    FROM
      "n" as "t0"
      LEFT JOIN "y" as "t1" ON "t0"."au" = "t1"."ag"
      and "t0"."cv" = "t1"."cv"
      LEFT JOIN "g" as "t2" ON coalesce ("t1"."cu"::int, "t0"."cu"::int) = "t2"."cu"
      LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
      LEFT JOIN "n" as "t4" ON "t4"."h" = "t0"."ag"
      and "t4"."cv" = "t0"."cv"
    WHERE
      "t0"."cv" = 3023424::int
      and "t4"."ag" is null
      and (
        "t0"."h" is null
        or "t0"."h" = (-1)::int
      )
  ) as "t0"
''
plan:
    [0] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [0] SEARCH TABLE y AS t1 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
            [0] SEARCH TABLE g AS t2 USING PRIMARY KEY (cu=?) (~1 row)
                [0] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
                    [0] SEARCH TABLE n AS t4 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q49
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  10;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 10
  projection (
    cte4.cv::int -> cv,
    cte4.b::int -> b,
    cte4.ay::text -> ay,
    cte4.bg::text -> bg,
    cte4.cu::int -> cu,
    cte4.cw::int -> cw,
    cte4.ac::int -> ac,
    cte4.ae::text -> ae,
    cte4.ab::text -> ab,
    cte4.c9::int -> c9,
    cte4.d::decimal -> d,
    cte4.cx::text -> cx,
    cte4.c11::int -> c11
  )
    selection (cte4.c11::int > 0::int)
      scan cte cte4($4)
subquery $0:
  projection (ag::int, au::int, cv::int)
    order by (ag::int)
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (
            t0.ag::int -> ag,
            t0.au::int -> au,
            t0.cv::int -> cv
          )
            selection (t1.b::int is null)
              left join on (
                ROW(t0.ag::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
              )
                scan t0
                  union all
                    projection (
                      l.ag::int -> ag,
                      l.au::int -> au,
                      l.cv::int -> cv
                    )
                      selection (
                        (
                          l.cv::int = 4797271::int
                          and l.cs::datetime >= '2023-02-17 0:00:00.0 +00:00:00'::datetime
                          and l.cs::datetime < '2023-02-18 0:00:00.0 +00:00:00'::datetime
                          and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                        )
                      )
                        scan l
                    projection (
                      l.ag::int -> ag,
                      l.au::int -> au,
                      l.cv::int -> cv
                    )
                      selection (
                        (
                          l.cv::int = 4797271::int
                          and l.cs::datetime is null
                          and l.a::datetime >= '2023-02-17 0:00:00.0 +00:00:00'::datetime
                          and l.a::datetime < '2023-02-18 0:00:00.0 +00:00:00'::datetime
                          and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                        )
                      )
                        scan l
                scan x -> t1
subquery $1:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      t6.cv::int -> cv,
      t6.ag::int -> b,
      t3.ay::text -> ay,
      t3.bg::text -> bg,
      t6.cu::int -> cu,
      t6.cw::int -> cw,
      coalesce(t6.co::int::int, t6.cm::int::int, 0::int)::int -> ac,
      case 
        when not t6.co::int is null or not t6.cm::int is null then t6.cn::text
        else '0000'::text
      end -> ae,
      to_char(t6.cl::datetime::datetime, '%Y-%m-%d'::text)::text -> ab,
      case 
        when not t6.co::int is null then 0::int
        when not t6.cm::int is null then 1::int
        else 2::int
      end -> c9,
      t5.d::decimal -> d
    )
      selection (
        (
          t8.z::int = 4::int
          and t3.aw::int = 3::int
          and t3.cq::int = 4::int
        )
      )
        left join on (t8.cw::int = t6.cw::int)
          left join on (
            ROW(t6.ag::int, t6.cv::int) = ROW(t7.b::int, t7.cv::int)
          )
            join on (
              (
                ROW(t5.bs::int, t5.cv::int) = ROW(t6.ag::int, t6.cv::int)
                and coalesce(t6.bb::int::int, 0::int)::int <> 1::int
              )
            )
              join on (
                (
                  ROW(t4.ag::int, t4.cv::int) = ROW(t5.x::int, t5.cv::int)
                  and t5.db::int is null
                  and t5.da::int is null
                )
              )
                join on (
                  (
                    ROW(t2.ag::int, t3.cv::int) = ROW(t4.b::int, t4.cv::int)
                    and t4.ai::decimal < 0::int
                  )
                )
                  join on (
                    ROW(t2.au::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
                  )
                    motion [policy: segment([ref(cv)]), program: ReshardIfNeeded]
                      scan cte t2($0)
                    scan y -> t3 (indexed by ix_i)
                  scan v -> t4 (indexed by ix_f)
                scan w -> t5 (indexed by ix_g)
              scan l -> t6 (indexed by ix_d)
            scan x -> t7
          scan f -> t8
subquery $2:
  projection (
    t9.cv::int -> cv,
    t9.ag::int -> b,
    t9.ay::text -> ay,
    coalesce(t3.bg::text::text, t9.bg::text::text)::text -> bg,
    coalesce(t3.cu::int::int, t9.cu::int::int)::int -> cu,
    t9.cw::int -> cw,
    coalesce(t9.co::int::int, t9.cm::int::int, 0::int)::int -> ac,
    t9.cn::text -> ae,
    to_char(t9.cl::datetime::datetime, '%Y-%m-%d'::text)::text -> ab,
    case 
      when not t9.co::int is null then 0::int
      else 1::int
    end -> c9,
    coalesce(
      (
        t9.d::decimal * coalesce(t8.cj::int::decimal, 0.0::decimal)::decimal * case 
          when not t9.h::int is null then -1::int
          else 1::int
        end
      )::decimal,
      0.0::decimal
    )::decimal -> d
  )
    selection (
      (
        t9.cv::int = 4797271::int
        and coalesce(
          t9.cs::datetime::datetime,
          t9.a::datetime::datetime
        )::datetime = '2023-02-17 0:00:00.0 +00:00:00'::datetime
        and t8.z::int = 4::int
        and coalesce(t3.aw::int::int, t9.bf::int::int)::int = 3::int
        and coalesce(t3.cq::int::int, t11.cq::int::int)::int = 4::int
      )
    )
      left join on (t9.cu::int = t11.cu::int)
        left join on (
          ROW(t9.ag::int, t3.cv::int) = ROW(t10.h::int, t10.cv::int)
        )
          left join on (t9.cw::int = t8.cw::int)
            left join on (
              ROW(t9.au::int, t9.cv::int) = ROW(t3.ag::int, t3.cv::int)
            )
              scan n -> t9
              scan y -> t3 (indexed by ix_i)
            scan f -> t8
          scan n -> t10 (indexed by ix_e)
        scan h -> t11
subquery $3:
  motion [policy: full, program: ReshardIfNeeded]
    union all
      motion [policy: local, program: SerializeAsEmptyTable(true)]
        projection (
          cte1.cv::int -> cv,
          cte1.b::int -> b,
          cte1.ay::text -> ay,
          cte1.bg::text -> bg,
          cte1.cu::int -> cu,
          cte1.cw::int -> cw,
          cte1.ac::int -> ac,
          cte1.ae::text -> ae,
          cte1.ab::text -> ab,
          cte1.c9::int -> c9,
          cte1.d::decimal -> d
        )
          scan cte cte1($1)
      projection (
        cte2.cv::int -> cv,
        cte2.b::int -> b,
        cte2.ay::text -> ay,
        cte2.bg::text -> bg,
        cte2.cu::int -> cu,
        cte2.cw::int -> cw,
        cte2.ac::int -> ac,
        cte2.ae::text -> ae,
        cte2.ab::text -> ab,
        cte2.c9::int -> c9,
        cte2.d::decimal -> d
      )
        scan cte cte2($2)
subquery $4:
  projection (
    unnamed_join.cv::int -> cv,
    unnamed_join.b::int -> b,
    unnamed_join.ay::text -> ay,
    unnamed_join.bg::text -> bg,
    unnamed_join.cu::int -> cu,
    unnamed_join.cw::int -> cw,
    unnamed_join.ac::int -> ac,
    unnamed_join.ae::text -> ae,
    unnamed_join.ab::text -> ab,
    unnamed_join.c9::int -> c9,
    unnamed_join.d::decimal -> d,
    unnamed_join.cx::text -> cx,
    row_number() over (order by (unnamed_join.b::int desc) ) -> c11
  )
    motion [policy: full, program: AddMissingRowsForLeftJoin]
      projection (
        cte3.cv::int -> cv,
        cte3.b::int -> b,
        cte3.ay::text -> ay,
        cte3.bg::text -> bg,
        cte3.cu::int -> cu,
        cte3.cw::int -> cw,
        cte3.ac::int -> ac,
        cte3.ae::text -> ae,
        cte3.ab::text -> ab,
        cte3.c9::int -> c9,
        cte3.d::decimal -> d,
        t12.cv::int -> cv,
        t12.ay::text -> ay,
        t12.bucket_id::int -> bucket_id,
        t12.cx::text -> cx
      )
        join on (
          ROW(cte3.cv::int, cte3.ay::text) = ROW(t12.cv::int, t12.ay::text)
        )
          scan cte cte3($3)
          scan ad -> t12
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ag",
  "t0"."au",
  "t0"."cv"
FROM
  (
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" >= '2023-02-17 0:00:00.0 +00:00:00'::datetime
      and "l"."cs" < '2023-02-18 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
    UNION ALL
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" is null
      and "l"."a" >= '2023-02-17 0:00:00.0 +00:00:00'::datetime
      and "l"."a" < '2023-02-18 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
  ) as "t0"
  LEFT JOIN "x" as "t1" ON "t1"."b" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
WHERE
  "t1"."b" is null
''
plan:
    [2] SEARCH TABLE l USING COVERING INDEX ix_z (cv=? AND cs>? AND cs<?) (~1 row)
    [3] SEARCH TABLE l USING COVERING INDEX ix_aa (cv=? AND a>? AND a<?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t0 (~1 row)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
''
buckets = [874]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "COL_0" as "ag",
  "COL_1" as "au",
  "COL_2" as "cv"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_877372333083973691_1136"
  )
ORDER BY
  "COL_0"
''
plan:
    [0] SCAN TABLE _tmp_877372333083973691_1136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
╭─────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "t6"."cv",
  "t6"."ag" as "b",
  "t3"."ay",
  "t3"."bg",
  "t6"."cu",
  "t6"."cw",
  coalesce ("t6"."co"::int, "t6"."cm"::int, 0::int) as "ac",
  CASE
    WHEN not "t6"."co" is null
    or not "t6"."cm" is null THEN "t6"."cn"
    ELSE '0000'::text
  END as "ae",
  "to_char" ("t6"."cl"::datetime, '%Y-%m-%d'::text) as "ab",
  CASE
    WHEN not "t6"."co" is null THEN 0::int
    WHEN not "t6"."cm" is null THEN 1::int
    ELSE 2::int
  END as "c9",
  "t5"."d"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_18309399111922391115_2136"
  ) as "t2"
  INNER JOIN "y" as "t3" INDEXED BY "ix_i" ON "t3"."ag" = "t2"."COL_1"
  and "t3"."cv" = "t2"."COL_2"
  INNER JOIN "v" as "t4" INDEXED BY "ix_f" ON "t4"."b" = "t2"."COL_0"
  and "t4"."cv" = "t3"."cv"
  and "t4"."ai" < 0::int
  INNER JOIN "w" as "t5" INDEXED BY "ix_g" ON "t5"."x" = "t4"."ag"
  and "t5"."cv" = "t4"."cv"
  and "t5"."db" is null
  and "t5"."da" is null
  INNER JOIN "l" as "t6" INDEXED BY "ix_d" ON "t6"."ag" = "t5"."bs"
  and "t6"."cv" = "t5"."cv"
  and coalesce ("t6"."bb"::int, 0::int) <> 1::int
  LEFT JOIN "x" as "t7" ON "t7"."b" = "t6"."ag"
  and "t7"."cv" = "t6"."cv"
  LEFT JOIN "f" as "t8" ON "t8"."cw" = "t6"."cw"
WHERE
  "t8"."z" = 4::int
  and "t3"."aw" = 3::int
  and "t3"."cq" = 4::int
''
plan:
    [0] SCAN TABLE _tmp_18309399111922391115_2136 (~1048576 rows)
        [0] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~7 rows)
            [0] SEARCH TABLE v AS t4 USING COVERING INDEX ix_f (b=? AND cv=?) (~8 rows)
                [0] SEARCH TABLE w AS t5 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                    [0] SEARCH TABLE l AS t6 USING COVERING INDEX ix_d (cv=? AND ag=?) (~8 rows)
                        [0] SEARCH TABLE x AS t7 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
                            [0] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
''
buckets <= [1-3000]
''
╭────────────────────────────────────────╮
│ 4. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  *
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
      "_tmp_5092332724665672962_3136"
  ) as "cte1"
UNION ALL
SELECT
  *
FROM
  (
    SELECT
      "t9"."cv",
      "t9"."ag" as "b",
      "t9"."ay",
      coalesce ("t3"."bg"::text, "t9"."bg"::text) as "bg",
      coalesce ("t3"."cu"::int, "t9"."cu"::int) as "cu",
      "t9"."cw",
      coalesce ("t9"."co"::int, "t9"."cm"::int, 0::int) as "ac",
      "t9"."cn" as "ae",
      "to_char" ("t9"."cl"::datetime, '%Y-%m-%d'::text) as "ab",
      CASE
        WHEN not "t9"."co" is null THEN 0::int
        ELSE 1::int
      END as "c9",
      coalesce (
        (
          "t9"."d" * coalesce ("t8"."cj"::decimal, 0.0::decimal) * CASE
            WHEN not "t9"."h" is null THEN (-1)::int
            ELSE 1::int
          END
        )::decimal,
        0.0::decimal
      ) as "d"
    FROM
      "n" as "t9"
      LEFT JOIN "y" as "t3" INDEXED BY "ix_i" ON "t9"."au" = "t3"."ag"
      and "t3"."cv" = "t9"."cv"
      LEFT JOIN "f" as "t8" ON "t9"."cw" = "t8"."cw"
      LEFT JOIN "n" as "t10" INDEXED BY "ix_e" ON "t10"."h" = "t9"."ag"
      and "t10"."cv" = "t3"."cv"
      LEFT JOIN "h" as "t11" ON "t9"."cu" = "t11"."cu"
    WHERE
      "t9"."cv" = 4797271::int
      and coalesce ("t9"."cs"::datetime, "t9"."a"::datetime) = '2023-02-17 0:00:00.0 +00:00:00'::datetime
      and "t8"."z" = 4::int
      and coalesce ("t3"."aw"::int, "t9"."bf"::int) = 3::int
      and coalesce ("t3"."cq"::int, "t11"."cq"::int) = 4::int
  ) as "cte2"
''
plan:
    [1] SCAN TABLE _tmp_5092332724665672962_3136 (~1048576 rows)
    [2] SEARCH TABLE n AS t9 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [2] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [2] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
                [2] SEARCH TABLE n AS t10 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
                    [2] SEARCH TABLE h AS t11 USING PRIMARY KEY (cu=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = [874]
''
╭──────────────────────────╮
│ 5. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "cte3"."COL_0" as "cv",
  "cte3"."COL_1" as "b",
  "cte3"."COL_2" as "ay",
  "cte3"."COL_3" as "bg",
  "cte3"."COL_4" as "cu",
  "cte3"."COL_5" as "cw",
  "cte3"."COL_6" as "ac",
  "cte3"."COL_7" as "ae",
  "cte3"."COL_8" as "ab",
  "cte3"."COL_9" as "c9",
  "cte3"."COL_10" as "d",
  "t12"."cv",
  "t12"."ay",
  "t12"."bucket_id",
  "t12"."cx"
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
      "_tmp_6654303618351556102_5136"
  ) as "cte3"
  INNER JOIN "ad" as "t12" ON "t12"."cv" = "cte3"."COL_0"
  and "t12"."ay" = "cte3"."COL_2"
''
plan:
    [0] SCAN TABLE _tmp_6654303618351556102_5136 (~1048576 rows)
        [0] SEARCH TABLE ad AS t12 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 6. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "unnamed_join"."COL_0" as "cv",
      "unnamed_join"."COL_1" as "b",
      "unnamed_join"."COL_2" as "ay",
      "unnamed_join"."COL_3" as "bg",
      "unnamed_join"."COL_4" as "cu",
      "unnamed_join"."COL_5" as "cw",
      "unnamed_join"."COL_6" as "ac",
      "unnamed_join"."COL_7" as "ae",
      "unnamed_join"."COL_8" as "ab",
      "unnamed_join"."COL_9" as "c9",
      "unnamed_join"."COL_10" as "d",
      "unnamed_join"."COL_14" as "cx",
      row_number () OVER (
        ORDER BY
          "unnamed_join"."COL_1" DESC) as "c11"
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
          "COL_10",
          "COL_11",
          "COL_12",
          "COL_13",
          "COL_14"
        FROM
          "_tmp_6219172643915444872_6136"
      ) as "unnamed_join"
  ) as "cte4"
WHERE
  "cte4"."c11" > 0::int
LIMIT
  10
''
plan:
    [2] SCAN TABLE _tmp_6219172643915444872_6136 (~1048576 rows)
    [2] USE TEMP B-TREE FOR ORDER BY
    [1] SCAN SUBQUERY 2 (~1 row)
    [0] SCAN SUBQUERY 1 AS cte4 (~1 row)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q50
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  ) t12;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  sum(count_1::int::int)::int::int -> c1,
  sum(sum_2::decimal::decimal)::decimal -> c2
)
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      count(*)::int -> count_1,
      sum(t12.d::decimal::decimal)::decimal -> sum_2
    )
      scan t12
        union all
          motion [policy: local, program: SerializeAsEmptyTable(true)]
            projection (cte1.d::decimal -> d)
              scan cte cte1($1)
          projection (cte2.d::decimal -> d)
            scan cte cte2($2)
subquery $0:
  projection (ag::int, au::int, cv::int)
    order by (ag::int)
      scan
        projection (
          t0.ag::int -> ag,
          t0.au::int -> au,
          t0.cv::int -> cv
        )
          selection (
            (
              t0.cv::int = 4797271::int
              and coalesce(
                t0.cs::datetime::datetime,
                t0.a::datetime::datetime
              )::datetime = '2023-02-13 0:00:00.0 +00:00:00'::datetime
              and coalesce(t0.bb::int::int, 0::int)::int <> 1::int
              and t1.b::int is null
            )
          )
            left join on (
              ROW(t0.ag::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
            )
              scan l -> t0
              scan x -> t1
subquery $1:
  motion [policy: full, program: ReshardIfNeeded]
    projection (t5.d::decimal -> d)
      selection (
        (
          t8.z::int = 4::int
          and t3.aw::int = 3::int
          and t3.cq::int = 4::int
        )
      )
        left join on (t8.cw::int = t6.cw::int)
          left join on (
            ROW(t6.ag::int, t6.cv::int) = ROW(t7.b::int, t7.cv::int)
          )
            join on (
              (
                ROW(t5.bs::int, t5.cv::int) = ROW(t6.ag::int, t6.cv::int)
                and coalesce(t6.bb::int::int, 0::int)::int <> 1::int
              )
            )
              join on (
                (
                  ROW(t4.ag::int, t4.cv::int) = ROW(t5.x::int, t5.cv::int)
                  and t5.db::int is null
                  and t5.da::int is null
                )
              )
                join on (
                  (
                    ROW(t2.ag::int, t3.cv::int) = ROW(t4.b::int, t4.cv::int)
                    and t4.ai::decimal < 0::int
                  )
                )
                  join on (
                    ROW(t2.au::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
                  )
                    scan cte t2($0)
                    scan y -> t3
                  scan v -> t4
                scan w -> t5 (indexed by ix_g)
              scan l -> t6
            scan x -> t7
          scan f -> t8
subquery $2:
  projection (
    coalesce(
      (
        t9.d::decimal * coalesce(t8.cj::int::decimal, 0.0::decimal)::decimal * case 
          when not t9.h::int is null then -1::int
          else 1::int
        end
      )::decimal,
      0.0::decimal
    )::decimal -> d
  )
    selection (
      (
        t9.cv::int = 4797271::int
        and coalesce(
          t9.cs::datetime::datetime,
          t9.a::datetime::datetime
        )::datetime = '2024-12-28 0:00:00.0 +00:00:00'::datetime
        and t8.z::int = 4::int
        and coalesce(t3.aw::int::int, t9.bf::int::int)::int = 3::int
        and coalesce(t3.cq::int::int, t11.cq::int::int)::int = 4::int
      )
    )
      left join on (t9.cu::int = t11.cu::int)
        left join on (
          ROW(t9.ag::int, t3.cv::int) = ROW(t10.h::int, t10.cv::int)
        )
          left join on (t9.cw::int = t8.cw::int)
            left join on (
              ROW(t9.au::int, t9.cv::int) = ROW(t3.ag::int, t3.cv::int)
            )
              scan n -> t9
              scan y -> t3
            scan f -> t8
          scan n -> t10
        scan h -> t11
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
  "t5"."d"
FROM
  (
    SELECT
      "ag",
      "au",
      "cv"
    FROM
      (
        SELECT
          "t0"."ag",
          "t0"."au",
          "t0"."cv"
        FROM
          "l" as "t0"
          LEFT JOIN "x" as "t1" ON "t1"."b" = "t0"."ag"
          and "t1"."cv" = "t0"."cv"
        WHERE
          "t0"."cv" = 4797271::int
          and coalesce ("t0"."cs"::datetime, "t0"."a"::datetime) = '2023-02-13 0:00:00.0 +00:00:00'::datetime
          and coalesce ("t0"."bb"::int, 0::int) <> 1::int
          and "t1"."b" is null
      )
    ORDER BY
      "ag"
  ) as "t2"
  INNER JOIN "y" as "t3" ON "t3"."ag" = "t2"."au"
  and "t3"."cv" = "t2"."cv"
  INNER JOIN "v" as "t4" ON "t4"."b" = "t2"."ag"
  and "t4"."cv" = "t3"."cv"
  and "t4"."ai" < 0::int
  INNER JOIN "w" as "t5" INDEXED BY "ix_g" ON "t5"."x" = "t4"."ag"
  and "t5"."cv" = "t4"."cv"
  and "t5"."db" is null
  and "t5"."da" is null
  INNER JOIN "l" as "t6" ON "t6"."ag" = "t5"."bs"
  and "t6"."cv" = "t5"."cv"
  and coalesce ("t6"."bb"::int, 0::int) <> 1::int
  LEFT JOIN "x" as "t7" ON "t7"."b" = "t6"."ag"
  and "t7"."cv" = "t6"."cv"
  LEFT JOIN "f" as "t8" ON "t8"."cw" = "t6"."cw"
WHERE
  "t8"."z" = 4::int
  and "t3"."aw" = 3::int
  and "t3"."cq" = 4::int
''
plan:
    [0] SEARCH TABLE l AS t0 USING COVERING INDEX ix_d (cv=?) (~8 rows)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
            [0] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [0] SEARCH TABLE v AS t4 USING COVERING INDEX ix_f (b=? AND cv=?) (~8 rows)
                    [0] SEARCH TABLE w AS t5 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                        [0] SEARCH TABLE l AS t6 USING COVERING INDEX ix_d (cv=? AND ag=?) (~1 row)
                            [0] SEARCH TABLE x AS t7 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
                                [0] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
''
buckets <= [1-3000]
''
╭────────────────────────────────────────╮
│ 2. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  count (*) as "count_1",
  sum ("t12"."COL_0"::decimal) as "sum_2"
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          "COL_0"
        FROM
          "_tmp_312298085277628206_0136"
      ) as "cte1"
    UNION ALL
    SELECT
      *
    FROM
      (
        SELECT
          coalesce (
            (
              "t9"."d" * coalesce ("t8"."cj"::decimal, 0.0::decimal) * CASE
                WHEN not "t9"."h" is null THEN (-1)::int
                ELSE 1::int
              END
            )::decimal,
            0.0::decimal
          ) as "d"
        FROM
          "n" as "t9"
          LEFT JOIN "y" as "t3" ON "t9"."au" = "t3"."ag"
          and "t3"."cv" = "t9"."cv"
          LEFT JOIN "f" as "t8" ON "t9"."cw" = "t8"."cw"
          LEFT JOIN "n" as "t10" ON "t10"."h" = "t9"."ag"
          and "t10"."cv" = "t3"."cv"
          LEFT JOIN "h" as "t11" ON "t9"."cu" = "t11"."cu"
        WHERE
          "t9"."cv" = 4797271::int
          and coalesce ("t9"."cs"::datetime, "t9"."a"::datetime) = '2024-12-28 0:00:00.0 +00:00:00'::datetime
          and "t8"."z" = 4::int
          and coalesce ("t3"."aw"::int, "t9"."bf"::int) = 3::int
          and coalesce ("t3"."cq"::int, "t11"."cq"::int) = 4::int
      ) as "cte2"
  ) as "t12"
''
plan:
    [2] SCAN TABLE _tmp_312298085277628206_0136 (~1048576 rows)
    [3] SEARCH TABLE n AS t9 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [3] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
            [3] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
                [3] SEARCH TABLE n AS t10 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
                    [3] SEARCH TABLE h AS t11 USING PRIMARY KEY (cu=?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t12 (~1 row)
''
buckets = [874]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  sum ("COL_0"::int)::int as "c1",
  sum ("COL_1"::decimal) as "c2"
FROM
  (
    SELECT
      "COL_0",
      "COL_1"
    FROM
      "_tmp_13121145912930815082_2136"
  )
''
plan:
    [0] SCAN TABLE _tmp_13121145912930815082_2136 (~1048576 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q51
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  cte2;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union all
  motion [policy: segment([ref(cv)]), program: ReshardIfNeeded]
    projection (
      cte1.cv::int -> cv,
      cte1.ay::text -> ay,
      cte1.bg::text -> bg,
      cte1.ah::text -> ah,
      cte1.cu::int -> cu,
      cte1.cx::text -> cx
    )
      scan cte cte1($1)
  projection (
    cte2.cv::int -> cv,
    cte2.ay::text -> ay,
    cte2.bg::text -> bg,
    cte2.ah::text -> ah,
    cte2.cu::int -> cu,
    cte2.cx::text -> cx
  )
    scan cte cte2($2)
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      t0.ag::int -> ag,
      t0.au::int -> au,
      t0.cv::int -> cv
    )
      selection (t1.b::int is null)
        left join on (
          ROW(t0.ag::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
        )
          scan t0
            union all
              projection (
                l.ag::int -> ag,
                l.au::int -> au,
                l.cv::int -> cv
              )
                selection (
                  (
                    l.cv::int = 4797271::int
                    and l.cs::datetime >= '2023-02-17 0:00:00.0 +00:00:00'::datetime
                    and l.cs::datetime < '2023-02-18 0:00:00.0 +00:00:00'::datetime
                    and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                  )
                )
                  scan l
              projection (
                l.ag::int -> ag,
                l.au::int -> au,
                l.cv::int -> cv
              )
                selection (
                  (
                    l.cv::int = 4797271::int
                    and l.cs::datetime is null
                    and l.a::datetime >= '2023-02-17 0:00:00.0 +00:00:00'::datetime
                    and l.a::datetime < '2023-02-18 0:00:00.0 +00:00:00'::datetime
                    and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                  )
                )
                  scan l
          scan x -> t1
subquery $1:
  projection (
    gr_expr_1::int -> cv,
    gr_expr_2::text -> ay,
    gr_expr_3::text -> bg,
    gr_expr_4::text -> ah,
    gr_expr_5::int -> cu,
    gr_expr_6::text -> cx
  )
    group by (
      gr_expr_1::int,
      gr_expr_2::text,
      gr_expr_3::text,
      gr_expr_4::text,
      gr_expr_5::int,
      gr_expr_6::text
    )
      motion [policy: full, program: ReshardIfNeeded]
        projection (
          t6.cv::int -> gr_expr_1,
          t3.ay::text -> gr_expr_2,
          t3.bg::text -> gr_expr_3,
          t3.ah::text -> gr_expr_4,
          t6.cu::int -> gr_expr_5,
          t8.cx::text -> gr_expr_6
        )
          group by (
            t6.cv::int,
            t3.ay::text,
            t3.bg::text,
            t3.ah::text,
            t6.cu::int,
            t8.cx::text
          )
            selection (
              (
                t7.z::int = 4::int
                and t3.aw::int = 3::int
                and t3.cq::int = 4::int
              )
            )
              left join on (
                ROW(t3.cv::int, t3.ay::text) = ROW(t8.cv::int, t8.ay::text)
              )
                left join on (t7.cw::int = t6.cw::int)
                  join on (
                    (
                      ROW(t5.bs::int, t5.cv::int) = ROW(t6.ag::int, t6.cv::int)
                      and coalesce(t6.bb::int::int, 0::int)::int <> 1::int
                    )
                  )
                    join on (
                      (
                        ROW(t4.cv::int, t4.ag::int) = ROW(t5.cv::int, t5.x::int)
                        and t5.db::int is null
                        and t5.da::int is null
                      )
                    )
                      join on (
                        (
                          ROW(t2.ag::int, t3.cv::int) = ROW(t4.b::int, t4.cv::int)
                          and t4.ai::decimal < 0::int
                        )
                      )
                        join on (
                          ROW(t2.au::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
                        )
                          scan cte t2($0)
                          scan y -> t3 (indexed by ix_i)
                        scan v -> t4 (indexed by ix_f)
                      scan w -> t5 (indexed by ix_g)
                    scan l -> t6 (indexed by ix_d)
                  scan f -> t7
                scan ad -> t8
subquery $2:
  projection (
    t9.cv::int -> cv,
    t9.ay::text -> ay,
    coalesce(t3.bg::text::text, t9.bg::text::text)::text -> bg,
    coalesce(t3.ah::text::text, t9.ah::text::text)::text -> ah,
    coalesce(t3.cu::int::int, t9.cu::int::int)::int -> cu,
    t8.cx::text -> cx
  )
    selection (
      (
        t9.cv::int = 4797271::int
        and coalesce(
          t9.cs::datetime::datetime,
          t9.a::datetime::datetime
        )::datetime = '2023-02-17 0:00:00.0 +00:00:00'::datetime
        and t7.z::int = 4::int
        and coalesce(t3.aw::int::int, t9.bf::int::int)::int = 3::int
        and coalesce(t3.cq::int::int, t11.cq::int::int)::int = 4::int
      )
    )
      left join on (
        ROW(t3.cv::int, t3.ay::text) = ROW(t8.cv::int, t8.ay::text)
      )
        left join on (t9.cu::int = t11.cu::int)
          left join on (
            ROW(t9.ag::int, t9.cv::int) = ROW(t10.h::int, t10.cv::int)
          )
            left join on (t9.cw::int = t7.cw::int)
              left join on (
                ROW(t9.au::int, t9.cv::int) = ROW(t3.ag::int, t3.cv::int)
              )
                scan n -> t9
                scan y -> t3 (indexed by ix_i)
              scan f -> t7
            scan n -> t10 (indexed by ix_e)
          scan h -> t11
        scan ad -> t8
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ag",
  "t0"."au",
  "t0"."cv"
FROM
  (
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" >= '2023-02-17 0:00:00.0 +00:00:00'::datetime
      and "l"."cs" < '2023-02-18 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
    UNION ALL
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" is null
      and "l"."a" >= '2023-02-17 0:00:00.0 +00:00:00'::datetime
      and "l"."a" < '2023-02-18 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
  ) as "t0"
  LEFT JOIN "x" as "t1" ON "t1"."b" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
WHERE
  "t1"."b" is null
''
plan:
    [2] SEARCH TABLE l USING COVERING INDEX ix_z (cv=? AND cs>? AND cs<?) (~1 row)
    [3] SEARCH TABLE l USING COVERING INDEX ix_aa (cv=? AND a>? AND a<?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t0 (~1 row)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
''
buckets = [874]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "t6"."cv" as "gr_expr_1",
  "t3"."ay" as "gr_expr_2",
  "t3"."bg" as "gr_expr_3",
  "t3"."ah" as "gr_expr_4",
  "t6"."cu" as "gr_expr_5",
  "t8"."cx" as "gr_expr_6"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_5930600967966593789_0136"
  ) as "t2"
  INNER JOIN "y" as "t3" INDEXED BY "ix_i" ON "t3"."ag" = "t2"."COL_1"
  and "t3"."cv" = "t2"."COL_2"
  INNER JOIN "v" as "t4" INDEXED BY "ix_f" ON "t4"."b" = "t2"."COL_0"
  and "t4"."cv" = "t3"."cv"
  and "t4"."ai" < 0::int
  INNER JOIN "w" as "t5" INDEXED BY "ix_g" ON "t5"."x" = "t4"."ag"
  and "t5"."db" is null
  and "t5"."da" is null
  and "t5"."cv" = "t4"."cv"
  INNER JOIN "l" as "t6" INDEXED BY "ix_d" ON "t6"."ag" = "t5"."bs"
  and "t6"."cv" = "t5"."cv"
  and coalesce ("t6"."bb"::int, 0::int) <> 1::int
  LEFT JOIN "f" as "t7" ON "t7"."cw" = "t6"."cw"
  LEFT JOIN "ad" as "t8" ON "t8"."cv" = "t3"."cv"
  and "t8"."ay" = "t3"."ay"
WHERE
  "t7"."z" = 4::int
  and "t3"."aw" = 3::int
  and "t3"."cq" = 4::int
GROUP BY
  "t6"."cv",
  "t3"."ay",
  "t3"."bg",
  "t3"."ah",
  "t6"."cu",
  "t8"."cx"
''
plan:
    [0] SCAN TABLE _tmp_5930600967966593789_0136 (~1048576 rows)
        [0] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~7 rows)
            [0] SEARCH TABLE v AS t4 USING COVERING INDEX ix_f (b=? AND cv=?) (~8 rows)
                [0] SEARCH TABLE w AS t5 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                    [0] SEARCH TABLE l AS t6 USING COVERING INDEX ix_d (cv=? AND ag=?) (~8 rows)
                        [0] SEARCH TABLE f AS t7 USING PRIMARY KEY (cw=?) (~1 row)
                            [0] SEARCH TABLE ad AS t8 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT DISTINCT
      "COL_0" as "cv",
      "COL_1" as "ay",
      "COL_2" as "bg",
      "COL_3" as "ah",
      "COL_4" as "cu",
      "COL_5" as "cx"
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
          "_tmp_2630807218819645061_1136"
      )
    GROUP BY
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5"
  ) as "cte1"
''
plan:
    [1] SCAN TABLE _tmp_2630807218819645061_1136 (~1048576 rows)
    [1] USE TEMP B-TREE FOR GROUP BY
    [1] USE TEMP B-TREE FOR DISTINCT
    [0] SCAN SUBQUERY 1 AS cte1 (~1 row)
''
buckets = any
''
╭─────────────────────────────────────────╮
│ 4. Query (DYN-FILTERED STORAGE, <= 1/1) │
╰─────────────────────────────────────────╯
''
SELECT
  "COL_0",
  "COL_1",
  "COL_2",
  "COL_3",
  "COL_4",
  "COL_5"
FROM
  "_tmp_6144964650535932087_2136"
UNION ALL
SELECT
  *
FROM
  (
    SELECT DISTINCT
      "t9"."cv",
      "t9"."ay",
      coalesce ("t3"."bg"::text, "t9"."bg"::text) as "bg",
      coalesce ("t3"."ah"::text, "t9"."ah"::text) as "ah",
      coalesce ("t3"."cu"::int, "t9"."cu"::int) as "cu",
      "t8"."cx"
    FROM
      "n" as "t9"
      LEFT JOIN "y" as "t3" INDEXED BY "ix_i" ON "t9"."au" = "t3"."ag"
      and "t9"."cv" = "t3"."cv"
      LEFT JOIN "f" as "t7" ON "t9"."cw" = "t7"."cw"
      LEFT JOIN "n" as "t10" INDEXED BY "ix_e" ON "t10"."h" = "t9"."ag"
      and "t10"."cv" = "t9"."cv"
      LEFT JOIN "h" as "t11" ON "t9"."cu" = "t11"."cu"
      LEFT JOIN "ad" as "t8" ON "t8"."cv" = "t3"."cv"
      and "t8"."ay" = "t3"."ay"
    WHERE
      "t9"."cv" = 4797271::int
      and coalesce ("t9"."cs"::datetime, "t9"."a"::datetime) = '2023-02-17 0:00:00.0 +00:00:00'::datetime
      and "t7"."z" = 4::int
      and coalesce ("t3"."aw"::int, "t9"."bf"::int) = 3::int
      and coalesce ("t3"."cq"::int, "t11"."cq"::int) = 4::int
  ) as "cte2"
''
plan:
    [1] SCAN TABLE _tmp_6144964650535932087_2136 (~1048576 rows)
    [3] SEARCH TABLE n AS t9 USING COVERING INDEX ix_ao (cv=?) (~9 rows)
        [3] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [3] SEARCH TABLE f AS t7 USING PRIMARY KEY (cw=?) (~1 row)
                [3] SEARCH TABLE n AS t10 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
                    [3] SEARCH TABLE h AS t11 USING PRIMARY KEY (cu=?) (~1 row)
                        [3] SEARCH TABLE ad AS t8 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [3] USE TEMP B-TREE FOR DISTINCT
    [2] SCAN SUBQUERY 3 AS cte2 (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets <= [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q52
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  t3.z;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union all
  projection (
    a.z::int -> c0,
    sum(a.f::decimal::decimal)::decimal -> c1,
    sum(a.e::decimal::decimal)::decimal -> c2,
    0::int -> c3
  )
    group by (a.z::int)
      selection (
        (
          a.cv::int = 4797271::int
          and a.aw::int = 3::int
          and a.cq::int = 4::int
          and a.a::datetime = '2023-02-17 0:00:00.0 +00:00:00'::datetime
        )
      )
        scan a
  projection (
    t3.z::int -> c0,
    0::int -> c1,
    0::int -> c2,
    coalesce(
      sum(
        (
          t0.d::decimal * coalesce(t3.cj::int::decimal, 0::decimal)::decimal * case 
            when not t0.h::int is null then -1::int
            else 1::int
          end
        )::decimal
      )::decimal::decimal,
      0.0::decimal
    )::decimal -> c3
  )
    group by (t3.z::int)
      selection (
        (
          t0.cv::int = 4797271::int
          and coalesce(t2.aw::int::int, t0.bf::int::int)::int = 3::int
          and coalesce(t2.cq::int::int, t4.cq::int::int)::int = 4::int
          and coalesce(
            t0.cs::datetime::datetime,
            t0.a::datetime::datetime
          )::datetime = '2023-02-17 0:00:00.0 +00:00:00'::datetime
          and t1.ag::int is null
          and (t0.h::int is null or t0.h::int = -1::int)
        )
      )
        left join on (t0.cu::int = t4.cu::int)
          left join on (t0.cw::int = t3.cw::int)
            left join on (
              ROW(t0.au::int, t0.cv::int) = ROW(t2.ag::int, t2.cv::int)
            )
              left join on (
                ROW(t0.ag::int, t0.cv::int) = ROW(t1.h::int, t1.cv::int)
              )
                scan n -> t0
                scan n -> t1
              scan y -> t2
            scan f -> t3
          scan h -> t4
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "a"."z" as "c0",
  sum ("a"."f"::decimal) as "c1",
  sum ("a"."e"::decimal) as "c2",
  0::int as "c3"
FROM
  "a"
WHERE
  "a"."cv" = 4797271::int
  and "a"."aw" = 3::int
  and "a"."cq" = 4::int
  and "a"."a" = '2023-02-17 0:00:00.0 +00:00:00'::datetime
GROUP BY
  "a"."z"
UNION ALL
SELECT
  "t3"."z" as "c0",
  0::int as "c1",
  0::int as "c2",
  coalesce (
    sum (
      (
        "t0"."d" * coalesce ("t3"."cj"::decimal, 0::decimal) * CASE
          WHEN not "t0"."h" is null THEN (-1)::int
          ELSE 1::int
        END
      )::decimal
    )::decimal,
    0.0::decimal
  ) as "c3"
FROM
  "n" as "t0"
  LEFT JOIN "n" as "t1" ON "t1"."h" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
  LEFT JOIN "y" as "t2" ON "t0"."au" = "t2"."ag"
  and "t0"."cv" = "t2"."cv"
  LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
  LEFT JOIN "h" as "t4" ON "t0"."cu" = "t4"."cu"
WHERE
  "t0"."cv" = 4797271::int
  and coalesce ("t2"."aw"::int, "t0"."bf"::int) = 3::int
  and coalesce ("t2"."cq"::int, "t4"."cq"::int) = 4::int
  and coalesce ("t0"."cs"::datetime, "t0"."a"::datetime) = '2023-02-17 0:00:00.0 +00:00:00'::datetime
  and "t1"."ag" is null
  and (
    "t0"."h" is null
    or "t0"."h" = (-1)::int
  )
GROUP BY
  "t3"."z"
''
plan:
    [1] SEARCH TABLE a 
         USING COVERING INDEX ix_c 
         (cv=? AND aw=? AND cq=? AND a=?) 
         (~7 rows)
    [1] USE TEMP B-TREE FOR GROUP BY
    [2] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~8 rows)
        [2] SEARCH TABLE n AS t1 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [2] SEARCH TABLE y AS t2 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [2] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
                    [2] SEARCH TABLE h AS t4 USING PRIMARY KEY (cu=?) (~1 row)
    [2] USE TEMP B-TREE FOR GROUP BY
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [874]

-- TEST: corpus-explain-q53
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
motion [policy: full, program: RemoveDuplicates]
  union
    projection (
      t0.ay::text -> ay,
      t0.bg::text -> bg,
      t0.ah::text -> ah,
      t1.cx::text -> cx
    )
      selection (
        (
          t0.cv::int = 4797271::int
          and t0.aw::int = 3::int
          and t0.a::datetime = '2023-02-17 0:00:00.0 +00:00:00'::datetime
          and t0.cq::int = 4::int
        )
      )
        left join on (
          ROW(t0.cv::int, t0.ay::text) = ROW(t1.cv::int, t1.ay::text)
        )
          scan a -> t0
          scan ad -> t1
    projection (
      t2.ay::text -> c0,
      coalesce(t4.bg::text::text, t2.bg::text::text)::text -> c1,
      coalesce(t4.ah::text::text, t2.ah::text::text)::text -> c2,
      t1.cx::text -> c3
    )
      selection (
        (
          t2.cv::int = 4797271::int
          and coalesce(t4.aw::int::int, t2.bf::int::int)::int = 3::int
          and coalesce(
            t2.cs::datetime::datetime,
            t2.a::datetime::datetime
          )::datetime = '2023-02-17 0:00:00.0 +00:00:00'::datetime
          and coalesce(t4.cq::int::int, t6.cq::int::int)::int = 4::int
        )
      )
        left join on (t2.cu::int = t6.cu::int)
          left join on (
            ROW(t2.cv::int, t2.ay::text) = ROW(t1.cv::int, t1.ay::text)
          )
            left join on (t2.cw::int = t5.cw::int)
              left join on (
                ROW(t2.au::int, t2.cv::int) = ROW(t4.ag::int, t4.cv::int)
              )
                left join on (
                  ROW(t2.ag::int, t2.cv::int) = ROW(t3.h::int, t3.cv::int)
                )
                  scan n -> t2
                  scan n -> t3
                scan y -> t4
              scan f -> t5
            scan ad -> t1
          scan h -> t6
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT DISTINCT
  "t0"."ay",
  "t0"."bg",
  "t0"."ah",
  "t1"."cx"
FROM
  "a" as "t0"
  LEFT JOIN "ad" as "t1" ON "t0"."cv" = "t1"."cv"
  and "t0"."ay" = "t1"."ay"
WHERE
  "t0"."cv" = 4797271::int
  and "t0"."aw" = 3::int
  and "t0"."a" = '2023-02-17 0:00:00.0 +00:00:00'::datetime
  and "t0"."cq" = 4::int
UNION
SELECT DISTINCT
  "t2"."ay" as "c0",
  coalesce ("t4"."bg"::text, "t2"."bg"::text) as "c1",
  coalesce ("t4"."ah"::text, "t2"."ah"::text) as "c2",
  "t1"."cx" as "c3"
FROM
  "n" as "t2"
  LEFT JOIN "n" as "t3" ON "t3"."h" = "t2"."ag"
  and "t3"."cv" = "t2"."cv"
  LEFT JOIN "y" as "t4" ON "t2"."au" = "t4"."ag"
  and "t2"."cv" = "t4"."cv"
  LEFT JOIN "f" as "t5" ON "t2"."cw" = "t5"."cw"
  LEFT JOIN "ad" as "t1" ON "t2"."cv" = "t1"."cv"
  and "t2"."ay" = "t1"."ay"
  LEFT JOIN "h" as "t6" ON "t2"."cu" = "t6"."cu"
WHERE
  "t2"."cv" = 4797271::int
  and coalesce ("t4"."aw"::int, "t2"."bf"::int) = 3::int
  and coalesce ("t2"."cs"::datetime, "t2"."a"::datetime) = '2023-02-17 0:00:00.0 +00:00:00'::datetime
  and coalesce ("t4"."cq"::int, "t6"."cq"::int) = 4::int
''
plan:
    [1] SEARCH TABLE a AS t0 
         USING COVERING INDEX ix_c 
         (cv=? AND aw=? AND cq=? AND a=?) 
         (~7 rows)
        [1] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [2] SEARCH TABLE n AS t2 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [2] SEARCH TABLE n AS t3 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [2] SEARCH TABLE y AS t4 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [2] SEARCH TABLE f AS t5 USING PRIMARY KEY (cw=?) (~1 row)
                    [2] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
                        [2] SEARCH TABLE h AS t6 USING PRIMARY KEY (cu=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)
''
buckets = [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [874]

-- TEST: corpus-explain-q54
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  a;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union all
  projection (
    a.cq::int -> c0,
    to_char(a.a::datetime::datetime, '%Y-%m-%d'::text)::text -> c1,
    sum(a.f::decimal::decimal)::decimal -> c3,
    sum(a.e::decimal::decimal)::decimal -> c4,
    0.0::decimal -> c5
  )
    group by (a.a::datetime, a.cq::int)
      selection (
        (
          a.cv::int = 4797271::int
          and a.aw::int = 3::int
          and a.bm::int in ROW(
            2025::int,
            2024::int,
            2023::int,
            2022::int,
            2021::int
          )
        )
      )
        scan a
  projection (
    cte0.cq::int -> cq,
    cte0.a::text -> a,
    0::int -> c3,
    0::int -> c4,
    coalesce(
      sum(
        (
          cte0.d::decimal * cte0.cj::int * case 
            when not cte0.h::int is null then -1::int
            else 1::int
          end
        )::decimal
      )::decimal::decimal,
      0.0::decimal
    )::decimal -> c5
  )
    group by (cte0.cq::int, cte0.a::text)
      scan cte cte0($0)
subquery $0:
  projection (
    coalesce(t2.cq::int::int, t4.cq::int::int)::int -> cq,
    to_char(
      coalesce(
        t0.cs::datetime::datetime,
        t0.a::datetime::datetime
      )::datetime::datetime,
      '%Y-%m-%d'::text
    )::text -> a,
    t0.d::decimal -> d,
    coalesce(t3.cj::int::int, 0::int)::int -> cj,
    t0.h::int -> h
  )
    selection (
      (
        t0.cv::int = 4797271::int
        and coalesce(t2.aw::int::int, t0.bf::int::int)::int = 3::int
        and t1.ag::int is null
        and (t0.h::int is null or t0.h::int = -1::int)
        and to_char(
          coalesce(
            t0.cs::datetime::datetime,
            t0.a::datetime::datetime
          )::datetime::datetime,
          '%Y'::text
        )::text::int in ROW(
          2025::int,
          2024::int,
          2023::int,
          2022::int,
          2021::int
        )
      )
    )
      left join on (t0.cu::int = t4.cu::int)
        left join on (t0.cw::int = t3.cw::int)
          left join on (
            ROW(t0.au::int, t0.cv::int) = ROW(t2.ag::int, t2.cv::int)
          )
            left join on (
              ROW(t0.ag::int, t0.cv::int) = ROW(t1.h::int, t1.cv::int)
            )
              scan n -> t0
              scan n -> t1
            scan y -> t2
          scan f -> t3
        scan h -> t4
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "a"."cq" as "c0",
  "to_char" ("a"."a"::datetime, '%Y-%m-%d'::text) as "c1",
  sum ("a"."f"::decimal) as "c3",
  sum ("a"."e"::decimal) as "c4",
  0.0::decimal as "c5"
FROM
  "a"
WHERE
  "a"."cv" = 4797271::int
  and "a"."aw" = 3::int
  and "a"."bm" in (2025::int, 2024::int, 2023::int, 2022::int, 2021::int)
GROUP BY
  "a"."a",
  "a"."cq"
UNION ALL
SELECT
  "cte0"."cq",
  "cte0"."a",
  0::int as "c3",
  0::int as "c4",
  coalesce (
    sum (
      (
        "cte0"."d" * "cte0"."cj" * CASE
          WHEN not "cte0"."h" is null THEN (-1)::int
          ELSE 1::int
        END
      )::decimal
    )::decimal,
    0.0::decimal
  ) as "c5"
FROM
  (
    SELECT
      coalesce ("t2"."cq"::int, "t4"."cq"::int) as "cq",
      "to_char" (
        coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
        '%Y-%m-%d'::text
      ) as "a",
      "t0"."d",
      coalesce ("t3"."cj"::int, 0::int) as "cj",
      "t0"."h"
    FROM
      "n" as "t0"
      LEFT JOIN "n" as "t1" ON "t1"."h" = "t0"."ag"
      and "t1"."cv" = "t0"."cv"
      LEFT JOIN "y" as "t2" ON "t0"."au" = "t2"."ag"
      and "t0"."cv" = "t2"."cv"
      LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
      LEFT JOIN "h" as "t4" ON "t0"."cu" = "t4"."cu"
    WHERE
      "t0"."cv" = 4797271::int
      and coalesce ("t2"."aw"::int, "t0"."bf"::int) = 3::int
      and "t1"."ag" is null
      and (
        "t0"."h" is null
        or "t0"."h" = (-1)::int
      )
      and "to_char" (
        coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
        '%Y'::text
      )::int in (2025::int, 2024::int, 2023::int, 2022::int, 2021::int)
  ) as "cte0"
GROUP BY
  "cte0"."cq",
  "cte0"."a"
''
plan:
    [1] SEARCH TABLE a USING COVERING INDEX ix_c (cv=? AND aw=?) (~8 rows)
    [1] EXECUTE LIST SUBQUERY 2
    [2] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~8 rows)
    [2] EXECUTE LIST SUBQUERY 3
        [2] SEARCH TABLE n AS t1 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [2] SEARCH TABLE y AS t2 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [2] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
                    [2] SEARCH TABLE h AS t4 USING PRIMARY KEY (cu=?) (~1 row)
    [2] USE TEMP B-TREE FOR GROUP BY
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [874]

-- TEST: corpus-explain-q55
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  10;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 10
  projection (
    cte4.cv::int -> cv,
    cte4.b::int -> b,
    cte4.ay::text -> ay,
    cte4.bg::text -> bg,
    cte4.cu::int -> cu,
    cte4.cw::int -> cw,
    cte4.ac::int -> ac,
    cte4.ae::text -> ae,
    cte4.ab::text -> ab,
    cte4.c9::int -> c9,
    cte4.d::decimal -> d,
    cte4.cx::text -> cx,
    cte4.c11::int -> c11
  )
    selection (cte4.c11::int > 0::int)
      scan cte cte4($4)
subquery $0:
  projection (ag::int, au::int, cv::int)
    order by (ag::int)
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (
            t0.ag::int -> ag,
            t0.au::int -> au,
            t0.cv::int -> cv
          )
            selection (t1.b::int is null)
              left join on (
                ROW(t0.ag::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
              )
                scan t0
                  union all
                    projection (
                      l.ag::int -> ag,
                      l.au::int -> au,
                      l.cv::int -> cv
                    )
                      selection (
                        (
                          l.cv::int = 3023424::int
                          and l.cs::datetime >= '2024-08-14 0:00:00.0 +00:00:00'::datetime
                          and l.cs::datetime < '2024-08-15 0:00:00.0 +00:00:00'::datetime
                          and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                        )
                      )
                        scan l
                    projection (
                      l.ag::int -> ag,
                      l.au::int -> au,
                      l.cv::int -> cv
                    )
                      selection (
                        (
                          l.cv::int = 3023424::int
                          and l.cs::datetime is null
                          and l.a::datetime >= '2024-08-14 0:00:00.0 +00:00:00'::datetime
                          and l.a::datetime < '2024-08-15 0:00:00.0 +00:00:00'::datetime
                          and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                        )
                      )
                        scan l
                scan x -> t1
subquery $1:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      t6.cv::int -> cv,
      t6.ag::int -> b,
      t3.ay::text -> ay,
      t3.bg::text -> bg,
      t6.cu::int -> cu,
      t6.cw::int -> cw,
      coalesce(t6.co::int::int, t6.cm::int::int, 0::int)::int -> ac,
      case 
        when not t6.co::int is null or not t6.cm::int is null then t6.cn::text
        else '0000'::text
      end -> ae,
      to_char(t6.cl::datetime::datetime, '%Y-%m-%d'::text)::text -> ab,
      case 
        when not t6.co::int is null then 0::int
        when not t6.cm::int is null then 1::int
        else 2::int
      end -> c9,
      t5.d::decimal -> d
    )
      selection (
        (
          t8.z::int = 8::int
          and t3.aw::int in ROW(42::int, 45::int)
        )
      )
        left join on (t8.cw::int = t6.cw::int)
          left join on (
            ROW(t6.ag::int, t6.cv::int) = ROW(t7.b::int, t7.cv::int)
          )
            join on (
              (
                ROW(t5.bs::int, t5.cv::int) = ROW(t6.ag::int, t6.cv::int)
                and coalesce(t6.bb::int::int, 0::int)::int <> 1::int
              )
            )
              join on (
                (
                  ROW(t4.ag::int, t4.cv::int) = ROW(t5.x::int, t5.cv::int)
                  and t5.db::int is null
                  and t5.da::int is null
                )
              )
                join on (
                  (
                    ROW(t2.ag::int, t3.cv::int) = ROW(t4.b::int, t4.cv::int)
                    and t4.ai::decimal < 0::int
                  )
                )
                  join on (
                    ROW(t2.au::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
                  )
                    motion [policy: segment([ref(cv)]), program: ReshardIfNeeded]
                      scan cte t2($0)
                    scan y -> t3 (indexed by ix_i)
                  scan v -> t4 (indexed by ix_f)
                scan w -> t5 (indexed by ix_g)
              scan l -> t6 (indexed by ix_d)
            scan x -> t7
          scan f -> t8
subquery $2:
  projection (
    t9.cv::int -> cv,
    t9.ag::int -> b,
    t9.ay::text -> ay,
    coalesce(t3.bg::text::text, t9.bg::text::text)::text -> bg,
    coalesce(t3.cu::int::int, t9.cu::int::int)::int -> cu,
    t9.cw::int -> cw,
    coalesce(t9.co::int::int, t9.cm::int::int, 0::int)::int -> ac,
    t9.cn::text -> ae,
    to_char(t9.cl::datetime::datetime, '%Y-%m-%d'::text)::text -> ab,
    case 
      when not t9.co::int is null then 0::int
      else 1::int
    end -> c9,
    coalesce(
      (
        t9.d::decimal * coalesce(t8.cj::int::decimal, 0.0::decimal)::decimal * case 
          when not t9.h::int is null then -1::int
          else 1::int
        end
      )::decimal,
      0.0::decimal
    )::decimal -> d
  )
    selection (
      (
        t9.cv::int = 3023424::int
        and coalesce(
          t9.cs::datetime::datetime,
          t9.a::datetime::datetime
        )::datetime = '2024-08-14 0:00:00.0 +00:00:00'::datetime
        and t8.z::int = 8::int
        and coalesce(t3.aw::int::int, t9.bf::int::int)::int in ROW(42::int, 45::int)
      )
    )
      left join on (t9.cu::int = t11.cu::int)
        left join on (
          ROW(t9.ag::int, t3.cv::int) = ROW(t10.h::int, t10.cv::int)
        )
          left join on (t9.cw::int = t8.cw::int)
            left join on (
              ROW(t9.au::int, t9.cv::int) = ROW(t3.ag::int, t3.cv::int)
            )
              scan n -> t9
              scan y -> t3 (indexed by ix_i)
            scan f -> t8
          scan n -> t10 (indexed by ix_e)
        scan h -> t11
subquery $3:
  motion [policy: full, program: ReshardIfNeeded]
    union all
      motion [policy: local, program: SerializeAsEmptyTable(true)]
        projection (
          cte1.cv::int -> cv,
          cte1.b::int -> b,
          cte1.ay::text -> ay,
          cte1.bg::text -> bg,
          cte1.cu::int -> cu,
          cte1.cw::int -> cw,
          cte1.ac::int -> ac,
          cte1.ae::text -> ae,
          cte1.ab::text -> ab,
          cte1.c9::int -> c9,
          cte1.d::decimal -> d
        )
          scan cte cte1($1)
      projection (
        cte2.cv::int -> cv,
        cte2.b::int -> b,
        cte2.ay::text -> ay,
        cte2.bg::text -> bg,
        cte2.cu::int -> cu,
        cte2.cw::int -> cw,
        cte2.ac::int -> ac,
        cte2.ae::text -> ae,
        cte2.ab::text -> ab,
        cte2.c9::int -> c9,
        cte2.d::decimal -> d
      )
        scan cte cte2($2)
subquery $4:
  projection (
    unnamed_join.cv::int -> cv,
    unnamed_join.b::int -> b,
    unnamed_join.ay::text -> ay,
    unnamed_join.bg::text -> bg,
    unnamed_join.cu::int -> cu,
    unnamed_join.cw::int -> cw,
    unnamed_join.ac::int -> ac,
    unnamed_join.ae::text -> ae,
    unnamed_join.ab::text -> ab,
    unnamed_join.c9::int -> c9,
    unnamed_join.d::decimal -> d,
    unnamed_join.cx::text -> cx,
    row_number() over (order by (unnamed_join.b::int desc) ) -> c11
  )
    motion [policy: full, program: AddMissingRowsForLeftJoin]
      projection (
        cte3.cv::int -> cv,
        cte3.b::int -> b,
        cte3.ay::text -> ay,
        cte3.bg::text -> bg,
        cte3.cu::int -> cu,
        cte3.cw::int -> cw,
        cte3.ac::int -> ac,
        cte3.ae::text -> ae,
        cte3.ab::text -> ab,
        cte3.c9::int -> c9,
        cte3.d::decimal -> d,
        t12.cv::int -> cv,
        t12.ay::text -> ay,
        t12.bucket_id::int -> bucket_id,
        t12.cx::text -> cx
      )
        join on (
          ROW(cte3.cv::int, cte3.ay::text) = ROW(t12.cv::int, t12.ay::text)
        )
          scan cte cte3($3)
          scan ad -> t12
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ag",
  "t0"."au",
  "t0"."cv"
FROM
  (
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 3023424::int
      and "l"."cs" >= '2024-08-14 0:00:00.0 +00:00:00'::datetime
      and "l"."cs" < '2024-08-15 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
    UNION ALL
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 3023424::int
      and "l"."cs" is null
      and "l"."a" >= '2024-08-14 0:00:00.0 +00:00:00'::datetime
      and "l"."a" < '2024-08-15 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
  ) as "t0"
  LEFT JOIN "x" as "t1" ON "t1"."b" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
WHERE
  "t1"."b" is null
''
plan:
    [2] SEARCH TABLE l USING COVERING INDEX ix_z (cv=? AND cs>? AND cs<?) (~1 row)
    [3] SEARCH TABLE l USING COVERING INDEX ix_aa (cv=? AND a>? AND a<?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t0 (~1 row)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
''
buckets = [1222]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "COL_0" as "ag",
  "COL_1" as "au",
  "COL_2" as "cv"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_877372333083973691_1136"
  )
ORDER BY
  "COL_0"
''
plan:
    [0] SCAN TABLE _tmp_877372333083973691_1136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
╭─────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "t6"."cv",
  "t6"."ag" as "b",
  "t3"."ay",
  "t3"."bg",
  "t6"."cu",
  "t6"."cw",
  coalesce ("t6"."co"::int, "t6"."cm"::int, 0::int) as "ac",
  CASE
    WHEN not "t6"."co" is null
    or not "t6"."cm" is null THEN "t6"."cn"
    ELSE '0000'::text
  END as "ae",
  "to_char" ("t6"."cl"::datetime, '%Y-%m-%d'::text) as "ab",
  CASE
    WHEN not "t6"."co" is null THEN 0::int
    WHEN not "t6"."cm" is null THEN 1::int
    ELSE 2::int
  END as "c9",
  "t5"."d"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_9249821361873699528_2136"
  ) as "t2"
  INNER JOIN "y" as "t3" INDEXED BY "ix_i" ON "t3"."ag" = "t2"."COL_1"
  and "t3"."cv" = "t2"."COL_2"
  INNER JOIN "v" as "t4" INDEXED BY "ix_f" ON "t4"."b" = "t2"."COL_0"
  and "t4"."cv" = "t3"."cv"
  and "t4"."ai" < 0::int
  INNER JOIN "w" as "t5" INDEXED BY "ix_g" ON "t5"."x" = "t4"."ag"
  and "t5"."cv" = "t4"."cv"
  and "t5"."db" is null
  and "t5"."da" is null
  INNER JOIN "l" as "t6" INDEXED BY "ix_d" ON "t6"."ag" = "t5"."bs"
  and "t6"."cv" = "t5"."cv"
  and coalesce ("t6"."bb"::int, 0::int) <> 1::int
  LEFT JOIN "x" as "t7" ON "t7"."b" = "t6"."ag"
  and "t7"."cv" = "t6"."cv"
  LEFT JOIN "f" as "t8" ON "t8"."cw" = "t6"."cw"
WHERE
  "t8"."z" = 8::int
  and "t3"."aw" in (42::int, 45::int)
''
plan:
    [0] SCAN TABLE _tmp_9249821361873699528_2136 (~1048576 rows)
        [0] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [0] SEARCH TABLE v AS t4 USING COVERING INDEX ix_f (b=? AND cv=?) (~8 rows)
                [0] SEARCH TABLE w AS t5 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                    [0] SEARCH TABLE l AS t6 USING COVERING INDEX ix_d (cv=? AND ag=?) (~8 rows)
                        [0] SEARCH TABLE x AS t7 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
                            [0] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
''
buckets <= [1-3000]
''
╭────────────────────────────────────────╮
│ 4. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  *
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
      "_tmp_12210160973438973025_3136"
  ) as "cte1"
UNION ALL
SELECT
  *
FROM
  (
    SELECT
      "t9"."cv",
      "t9"."ag" as "b",
      "t9"."ay",
      coalesce ("t3"."bg"::text, "t9"."bg"::text) as "bg",
      coalesce ("t3"."cu"::int, "t9"."cu"::int) as "cu",
      "t9"."cw",
      coalesce ("t9"."co"::int, "t9"."cm"::int, 0::int) as "ac",
      "t9"."cn" as "ae",
      "to_char" ("t9"."cl"::datetime, '%Y-%m-%d'::text) as "ab",
      CASE
        WHEN not "t9"."co" is null THEN 0::int
        ELSE 1::int
      END as "c9",
      coalesce (
        (
          "t9"."d" * coalesce ("t8"."cj"::decimal, 0.0::decimal) * CASE
            WHEN not "t9"."h" is null THEN (-1)::int
            ELSE 1::int
          END
        )::decimal,
        0.0::decimal
      ) as "d"
    FROM
      "n" as "t9"
      LEFT JOIN "y" as "t3" INDEXED BY "ix_i" ON "t9"."au" = "t3"."ag"
      and "t3"."cv" = "t9"."cv"
      LEFT JOIN "f" as "t8" ON "t9"."cw" = "t8"."cw"
      LEFT JOIN "n" as "t10" INDEXED BY "ix_e" ON "t10"."h" = "t9"."ag"
      and "t10"."cv" = "t3"."cv"
      LEFT JOIN "h" as "t11" ON "t9"."cu" = "t11"."cu"
    WHERE
      "t9"."cv" = 3023424::int
      and coalesce ("t9"."cs"::datetime, "t9"."a"::datetime) = '2024-08-14 0:00:00.0 +00:00:00'::datetime
      and "t8"."z" = 8::int
      and coalesce ("t3"."aw"::int, "t9"."bf"::int) in (42::int, 45::int)
  ) as "cte2"
''
plan:
    [1] SCAN TABLE _tmp_12210160973438973025_3136 (~1048576 rows)
    [2] SEARCH TABLE n AS t9 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [2] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [2] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
                [2] SEARCH TABLE n AS t10 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
                    [2] SEARCH TABLE h AS t11 USING PRIMARY KEY (cu=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = [1222]
''
╭──────────────────────────╮
│ 5. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "cte3"."COL_0" as "cv",
  "cte3"."COL_1" as "b",
  "cte3"."COL_2" as "ay",
  "cte3"."COL_3" as "bg",
  "cte3"."COL_4" as "cu",
  "cte3"."COL_5" as "cw",
  "cte3"."COL_6" as "ac",
  "cte3"."COL_7" as "ae",
  "cte3"."COL_8" as "ab",
  "cte3"."COL_9" as "c9",
  "cte3"."COL_10" as "d",
  "t12"."cv",
  "t12"."ay",
  "t12"."bucket_id",
  "t12"."cx"
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
      "_tmp_6654303618351556102_5136"
  ) as "cte3"
  INNER JOIN "ad" as "t12" ON "t12"."cv" = "cte3"."COL_0"
  and "t12"."ay" = "cte3"."COL_2"
''
plan:
    [0] SCAN TABLE _tmp_6654303618351556102_5136 (~1048576 rows)
        [0] SEARCH TABLE ad AS t12 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 6. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "unnamed_join"."COL_0" as "cv",
      "unnamed_join"."COL_1" as "b",
      "unnamed_join"."COL_2" as "ay",
      "unnamed_join"."COL_3" as "bg",
      "unnamed_join"."COL_4" as "cu",
      "unnamed_join"."COL_5" as "cw",
      "unnamed_join"."COL_6" as "ac",
      "unnamed_join"."COL_7" as "ae",
      "unnamed_join"."COL_8" as "ab",
      "unnamed_join"."COL_9" as "c9",
      "unnamed_join"."COL_10" as "d",
      "unnamed_join"."COL_14" as "cx",
      row_number () OVER (
        ORDER BY
          "unnamed_join"."COL_1" DESC) as "c11"
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
          "COL_10",
          "COL_11",
          "COL_12",
          "COL_13",
          "COL_14"
        FROM
          "_tmp_6219172643915444872_6136"
      ) as "unnamed_join"
  ) as "cte4"
WHERE
  "cte4"."c11" > 0::int
LIMIT
  10
''
plan:
    [2] SCAN TABLE _tmp_6219172643915444872_6136 (~1048576 rows)
    [2] USE TEMP B-TREE FOR ORDER BY
    [1] SCAN SUBQUERY 2 (~1 row)
    [0] SCAN SUBQUERY 1 AS cte4 (~1 row)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q56
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  ) t12;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  sum(count_1::int::int)::int::int -> c1,
  sum(sum_2::decimal::decimal)::decimal -> c2
)
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      count(*)::int -> count_1,
      sum(t12.d::decimal::decimal)::decimal -> sum_2
    )
      scan t12
        union all
          motion [policy: local, program: SerializeAsEmptyTable(true)]
            projection (cte1.d::decimal -> d)
              scan cte cte1($1)
          projection (cte2.d::decimal -> d)
            scan cte cte2($2)
subquery $0:
  projection (ag::int, au::int, cv::int)
    order by (ag::int)
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (
            t0.ag::int -> ag,
            t0.au::int -> au,
            t0.cv::int -> cv
          )
            selection (t1.b::int is null)
              left join on (
                ROW(t0.ag::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
              )
                scan t0
                  union all
                    projection (
                      l.ag::int -> ag,
                      l.au::int -> au,
                      l.cv::int -> cv
                    )
                      selection (
                        (
                          l.cv::int = 4797271::int
                          and l.cs::datetime >= '2024-08-14 0:00:00.0 +00:00:00'::datetime
                          and l.cs::datetime < '2024-08-15 0:00:00.0 +00:00:00'::datetime
                          and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                        )
                      )
                        scan l
                    projection (
                      l.ag::int -> ag,
                      l.au::int -> au,
                      l.cv::int -> cv
                    )
                      selection (
                        (
                          l.cv::int = 4797271::int
                          and l.cs::datetime is null
                          and l.a::datetime >= '2024-08-14 0:00:00.0 +00:00:00'::datetime
                          and l.a::datetime < '2024-08-15 0:00:00.0 +00:00:00'::datetime
                          and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                        )
                      )
                        scan l
                scan x -> t1
subquery $1:
  motion [policy: full, program: ReshardIfNeeded]
    projection (t5.d::decimal -> d)
      selection (
        (
          t8.z::int = 8::int
          and t3.aw::int in ROW(42::int, 45::int)
        )
      )
        left join on (t8.cw::int = t6.cw::int)
          left join on (
            ROW(t6.ag::int, t6.cv::int) = ROW(t7.b::int, t7.cv::int)
          )
            join on (
              (
                ROW(t5.bs::int, t5.cv::int) = ROW(t6.ag::int, t6.cv::int)
                and coalesce(t6.bb::int::int, 0::int)::int <> 1::int
              )
            )
              join on (
                (
                  ROW(t4.ag::int, t4.cv::int) = ROW(t5.x::int, t5.cv::int)
                  and t5.db::int is null
                  and t5.da::int is null
                )
              )
                join on (
                  (
                    ROW(t2.ag::int, t3.cv::int) = ROW(t4.b::int, t4.cv::int)
                    and t4.ai::decimal < 0::int
                  )
                )
                  join on (
                    ROW(t2.au::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
                  )
                    motion [policy: segment([ref(cv)]), program: ReshardIfNeeded]
                      scan cte t2($0)
                    scan y -> t3 (indexed by ix_i)
                  scan v -> t4 (indexed by ix_f)
                scan w -> t5 (indexed by ix_g)
              scan l -> t6 (indexed by ix_d)
            scan x -> t7
          scan f -> t8
subquery $2:
  projection (
    coalesce(
      (
        t9.d::decimal * coalesce(t8.cj::int::decimal, 0.0::decimal)::decimal * case 
          when not t9.h::int is null then -1::int
          else 1::int
        end
      )::decimal,
      0.0::decimal
    )::decimal -> d
  )
    selection (
      (
        t9.cv::int = 4797271::int
        and coalesce(
          t9.cs::datetime::datetime,
          t9.a::datetime::datetime
        )::datetime = '2024-08-14 0:00:00.0 +00:00:00'::datetime
        and t8.z::int = 8::int
        and coalesce(t3.aw::int::int, t9.bf::int::int)::int in ROW(42::int, 45::int)
      )
    )
      left join on (t9.cu::int = t11.cu::int)
        left join on (
          ROW(t9.ag::int, t3.cv::int) = ROW(t10.h::int, t10.cv::int)
        )
          left join on (t9.cw::int = t8.cw::int)
            left join on (
              ROW(t9.au::int, t9.cv::int) = ROW(t3.ag::int, t3.cv::int)
            )
              scan n -> t9
              scan y -> t3 (indexed by ix_i)
            scan f -> t8
          scan n -> t10 (indexed by ix_e)
        scan h -> t11
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ag",
  "t0"."au",
  "t0"."cv"
FROM
  (
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" >= '2024-08-14 0:00:00.0 +00:00:00'::datetime
      and "l"."cs" < '2024-08-15 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
    UNION ALL
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" is null
      and "l"."a" >= '2024-08-14 0:00:00.0 +00:00:00'::datetime
      and "l"."a" < '2024-08-15 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
  ) as "t0"
  LEFT JOIN "x" as "t1" ON "t1"."b" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
WHERE
  "t1"."b" is null
''
plan:
    [2] SEARCH TABLE l USING COVERING INDEX ix_z (cv=? AND cs>? AND cs<?) (~1 row)
    [3] SEARCH TABLE l USING COVERING INDEX ix_aa (cv=? AND a>? AND a<?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t0 (~1 row)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
''
buckets = [874]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "COL_0" as "ag",
  "COL_1" as "au",
  "COL_2" as "cv"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_8641021163966270088_0136"
  )
ORDER BY
  "COL_0"
''
plan:
    [0] SCAN TABLE _tmp_8641021163966270088_0136 (~1048576 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
╭─────────────────────────────────╮
│ 3. Query (DYN-FILTERED STORAGE) │
╰─────────────────────────────────╯
''
SELECT
  "t5"."d"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_361463343318768109_1136"
  ) as "t2"
  INNER JOIN "y" as "t3" INDEXED BY "ix_i" ON "t3"."ag" = "t2"."COL_1"
  and "t3"."cv" = "t2"."COL_2"
  INNER JOIN "v" as "t4" INDEXED BY "ix_f" ON "t4"."b" = "t2"."COL_0"
  and "t4"."cv" = "t3"."cv"
  and "t4"."ai" < 0::int
  INNER JOIN "w" as "t5" INDEXED BY "ix_g" ON "t5"."x" = "t4"."ag"
  and "t5"."cv" = "t4"."cv"
  and "t5"."db" is null
  and "t5"."da" is null
  INNER JOIN "l" as "t6" INDEXED BY "ix_d" ON "t6"."ag" = "t5"."bs"
  and "t6"."cv" = "t5"."cv"
  and coalesce ("t6"."bb"::int, 0::int) <> 1::int
  LEFT JOIN "x" as "t7" ON "t7"."b" = "t6"."ag"
  and "t7"."cv" = "t6"."cv"
  LEFT JOIN "f" as "t8" ON "t8"."cw" = "t6"."cw"
WHERE
  "t8"."z" = 8::int
  and "t3"."aw" in (42::int, 45::int)
''
plan:
    [0] SCAN TABLE _tmp_361463343318768109_1136 (~1048576 rows)
        [0] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [0] SEARCH TABLE v AS t4 USING COVERING INDEX ix_f (b=? AND cv=?) (~8 rows)
                [0] SEARCH TABLE w AS t5 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                    [0] SEARCH TABLE l AS t6 USING COVERING INDEX ix_d (cv=? AND ag=?) (~8 rows)
                        [0] SEARCH TABLE x AS t7 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
                            [0] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
''
buckets <= [1-3000]
''
╭────────────────────────────────────────╮
│ 4. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  count (*) as "count_1",
  sum ("t12"."COL_0"::decimal) as "sum_2"
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          "COL_0"
        FROM
          "_tmp_5243388994332312857_2136"
      ) as "cte1"
    UNION ALL
    SELECT
      *
    FROM
      (
        SELECT
          coalesce (
            (
              "t9"."d" * coalesce ("t8"."cj"::decimal, 0.0::decimal) * CASE
                WHEN not "t9"."h" is null THEN (-1)::int
                ELSE 1::int
              END
            )::decimal,
            0.0::decimal
          ) as "d"
        FROM
          "n" as "t9"
          LEFT JOIN "y" as "t3" INDEXED BY "ix_i" ON "t9"."au" = "t3"."ag"
          and "t3"."cv" = "t9"."cv"
          LEFT JOIN "f" as "t8" ON "t9"."cw" = "t8"."cw"
          LEFT JOIN "n" as "t10" INDEXED BY "ix_e" ON "t10"."h" = "t9"."ag"
          and "t10"."cv" = "t3"."cv"
          LEFT JOIN "h" as "t11" ON "t9"."cu" = "t11"."cu"
        WHERE
          "t9"."cv" = 4797271::int
          and coalesce ("t9"."cs"::datetime, "t9"."a"::datetime) = '2024-08-14 0:00:00.0 +00:00:00'::datetime
          and "t8"."z" = 8::int
          and coalesce ("t3"."aw"::int, "t9"."bf"::int) in (42::int, 45::int)
      ) as "cte2"
  ) as "t12"
''
plan:
    [2] SCAN TABLE _tmp_5243388994332312857_2136 (~1048576 rows)
    [3] SEARCH TABLE n AS t9 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [3] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [3] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
                [3] SEARCH TABLE n AS t10 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
                    [3] SEARCH TABLE h AS t11 USING PRIMARY KEY (cu=?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t12 (~1 row)
''
buckets = [874]
''
╭───────────────────╮
│ 5. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  sum ("COL_0"::int)::int as "c1",
  sum ("COL_1"::decimal) as "c2"
FROM
  (
    SELECT
      "COL_0",
      "COL_1"
    FROM
      "_tmp_17257311967805472214_4136"
  )
''
plan:
    [0] SCAN TABLE _tmp_17257311967805472214_4136 (~1048576 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q57
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  cte2;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union all
  motion [policy: segment([ref(cv)]), program: ReshardIfNeeded]
    projection (
      cte1.cv::int -> cv,
      cte1.ay::text -> ay,
      cte1.bg::text -> bg,
      cte1.ah::text -> ah,
      cte1.cu::int -> cu,
      cte1.cx::text -> cx
    )
      scan cte cte1($1)
  projection (
    cte2.cv::int -> cv,
    cte2.ay::text -> ay,
    cte2.bg::text -> bg,
    cte2.ah::text -> ah,
    cte2.cu::int -> cu,
    cte2.cx::text -> cx
  )
    scan cte cte2($2)
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      t0.ag::int -> ag,
      t0.au::int -> au,
      t0.cv::int -> cv
    )
      selection (t1.b::int is null)
        left join on (
          ROW(t0.ag::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
        )
          scan t0
            union all
              projection (
                l.ag::int -> ag,
                l.au::int -> au,
                l.cv::int -> cv
              )
                selection (
                  (
                    l.cv::int = 4797271::int
                    and l.cs::datetime >= '2024-08-14 0:00:00.0 +00:00:00'::datetime
                    and l.cs::datetime < '2024-08-15 0:00:00.0 +00:00:00'::datetime
                    and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                  )
                )
                  scan l
              projection (
                l.ag::int -> ag,
                l.au::int -> au,
                l.cv::int -> cv
              )
                selection (
                  (
                    l.cv::int = 4797271::int
                    and l.cs::datetime is null
                    and l.a::datetime >= '2024-08-14 0:00:00.0 +00:00:00'::datetime
                    and l.a::datetime < '2024-08-15 0:00:00.0 +00:00:00'::datetime
                    and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                  )
                )
                  scan l
          scan x -> t1
subquery $1:
  projection (
    gr_expr_1::int -> cv,
    gr_expr_2::text -> ay,
    gr_expr_3::text -> bg,
    gr_expr_4::text -> ah,
    gr_expr_5::int -> cu,
    gr_expr_6::text -> cx
  )
    group by (
      gr_expr_1::int,
      gr_expr_2::text,
      gr_expr_3::text,
      gr_expr_4::text,
      gr_expr_5::int,
      gr_expr_6::text
    )
      motion [policy: full, program: ReshardIfNeeded]
        projection (
          t6.cv::int -> gr_expr_1,
          t3.ay::text -> gr_expr_2,
          t3.bg::text -> gr_expr_3,
          t3.ah::text -> gr_expr_4,
          t6.cu::int -> gr_expr_5,
          t8.cx::text -> gr_expr_6
        )
          group by (
            t6.cv::int,
            t3.ay::text,
            t3.bg::text,
            t3.ah::text,
            t6.cu::int,
            t8.cx::text
          )
            selection (
              (
                t7.z::int = 8::int
                and t3.aw::int in ROW(42::int, 45::int)
              )
            )
              left join on (
                ROW(t3.cv::int, t3.ay::text) = ROW(t8.cv::int, t8.ay::text)
              )
                left join on (t7.cw::int = t6.cw::int)
                  join on (
                    (
                      ROW(t5.bs::int, t5.cv::int) = ROW(t6.ag::int, t6.cv::int)
                      and coalesce(t6.bb::int::int, 0::int)::int <> 1::int
                    )
                  )
                    join on (
                      (
                        ROW(t4.cv::int, t4.ag::int) = ROW(t5.cv::int, t5.x::int)
                        and t5.db::int is null
                        and t5.da::int is null
                      )
                    )
                      join on (
                        (
                          ROW(t2.ag::int, t3.cv::int) = ROW(t4.b::int, t4.cv::int)
                          and t4.ai::decimal < 0::int
                        )
                      )
                        join on (
                          ROW(t2.au::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
                        )
                          scan cte t2($0)
                          scan y -> t3 (indexed by ix_i)
                        scan v -> t4 (indexed by ix_f)
                      scan w -> t5 (indexed by ix_g)
                    scan l -> t6 (indexed by ix_d)
                  scan f -> t7
                scan ad -> t8
subquery $2:
  projection (
    t9.cv::int -> cv,
    t9.ay::text -> ay,
    coalesce(t3.bg::text::text, t9.bg::text::text)::text -> bg,
    coalesce(t3.ah::text::text, t9.ah::text::text)::text -> ah,
    coalesce(t3.cu::int::int, t9.cu::int::int)::int -> cu,
    t8.cx::text -> cx
  )
    selection (
      (
        t9.cv::int = 4797271::int
        and coalesce(
          t9.cs::datetime::datetime,
          t9.a::datetime::datetime
        )::datetime = '2024-08-14 0:00:00.0 +00:00:00'::datetime
        and t7.z::int = 8::int
        and coalesce(t3.aw::int::int, t9.bf::int::int)::int in ROW(42::int, 45::int)
      )
    )
      left join on (
        ROW(t3.cv::int, t3.ay::text) = ROW(t8.cv::int, t8.ay::text)
      )
        left join on (t9.cu::int = t11.cu::int)
          left join on (
            ROW(t9.ag::int, t9.cv::int) = ROW(t10.h::int, t10.cv::int)
          )
            left join on (t9.cw::int = t7.cw::int)
              left join on (
                ROW(t9.au::int, t9.cv::int) = ROW(t3.ag::int, t3.cv::int)
              )
                scan n -> t9
                scan y -> t3 (indexed by ix_i)
              scan f -> t7
            scan n -> t10 (indexed by ix_e)
          scan h -> t11
        scan ad -> t8
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ag",
  "t0"."au",
  "t0"."cv"
FROM
  (
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" >= '2024-08-14 0:00:00.0 +00:00:00'::datetime
      and "l"."cs" < '2024-08-15 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
    UNION ALL
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" is null
      and "l"."a" >= '2024-08-14 0:00:00.0 +00:00:00'::datetime
      and "l"."a" < '2024-08-15 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
  ) as "t0"
  LEFT JOIN "x" as "t1" ON "t1"."b" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
WHERE
  "t1"."b" is null
''
plan:
    [2] SEARCH TABLE l USING COVERING INDEX ix_z (cv=? AND cs>? AND cs<?) (~1 row)
    [3] SEARCH TABLE l USING COVERING INDEX ix_aa (cv=? AND a>? AND a<?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t0 (~1 row)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
''
buckets = [874]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "t6"."cv" as "gr_expr_1",
  "t3"."ay" as "gr_expr_2",
  "t3"."bg" as "gr_expr_3",
  "t3"."ah" as "gr_expr_4",
  "t6"."cu" as "gr_expr_5",
  "t8"."cx" as "gr_expr_6"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_9304316427558608414_0136"
  ) as "t2"
  INNER JOIN "y" as "t3" INDEXED BY "ix_i" ON "t3"."ag" = "t2"."COL_1"
  and "t3"."cv" = "t2"."COL_2"
  INNER JOIN "v" as "t4" INDEXED BY "ix_f" ON "t4"."b" = "t2"."COL_0"
  and "t4"."cv" = "t3"."cv"
  and "t4"."ai" < 0::int
  INNER JOIN "w" as "t5" INDEXED BY "ix_g" ON "t5"."x" = "t4"."ag"
  and "t5"."db" is null
  and "t5"."da" is null
  and "t5"."cv" = "t4"."cv"
  INNER JOIN "l" as "t6" INDEXED BY "ix_d" ON "t6"."ag" = "t5"."bs"
  and "t6"."cv" = "t5"."cv"
  and coalesce ("t6"."bb"::int, 0::int) <> 1::int
  LEFT JOIN "f" as "t7" ON "t7"."cw" = "t6"."cw"
  LEFT JOIN "ad" as "t8" ON "t8"."cv" = "t3"."cv"
  and "t8"."ay" = "t3"."ay"
WHERE
  "t7"."z" = 8::int
  and "t3"."aw" in (42::int, 45::int)
GROUP BY
  "t6"."cv",
  "t3"."ay",
  "t3"."bg",
  "t3"."ah",
  "t6"."cu",
  "t8"."cx"
''
plan:
    [0] SCAN TABLE _tmp_9304316427558608414_0136 (~1048576 rows)
        [0] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [0] SEARCH TABLE v AS t4 USING COVERING INDEX ix_f (b=? AND cv=?) (~8 rows)
                [0] SEARCH TABLE w AS t5 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                    [0] SEARCH TABLE l AS t6 USING COVERING INDEX ix_d (cv=? AND ag=?) (~8 rows)
                        [0] SEARCH TABLE f AS t7 USING PRIMARY KEY (cw=?) (~1 row)
                            [0] SEARCH TABLE ad AS t8 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT DISTINCT
      "COL_0" as "cv",
      "COL_1" as "ay",
      "COL_2" as "bg",
      "COL_3" as "ah",
      "COL_4" as "cu",
      "COL_5" as "cx"
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
          "_tmp_2630807218819645061_1136"
      )
    GROUP BY
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5"
  ) as "cte1"
''
plan:
    [1] SCAN TABLE _tmp_2630807218819645061_1136 (~1048576 rows)
    [1] USE TEMP B-TREE FOR GROUP BY
    [1] USE TEMP B-TREE FOR DISTINCT
    [0] SCAN SUBQUERY 1 AS cte1 (~1 row)
''
buckets = any
''
╭─────────────────────────────────────────╮
│ 4. Query (DYN-FILTERED STORAGE, <= 1/1) │
╰─────────────────────────────────────────╯
''
SELECT
  "COL_0",
  "COL_1",
  "COL_2",
  "COL_3",
  "COL_4",
  "COL_5"
FROM
  "_tmp_10757321093052108077_2136"
UNION ALL
SELECT
  *
FROM
  (
    SELECT DISTINCT
      "t9"."cv",
      "t9"."ay",
      coalesce ("t3"."bg"::text, "t9"."bg"::text) as "bg",
      coalesce ("t3"."ah"::text, "t9"."ah"::text) as "ah",
      coalesce ("t3"."cu"::int, "t9"."cu"::int) as "cu",
      "t8"."cx"
    FROM
      "n" as "t9"
      LEFT JOIN "y" as "t3" INDEXED BY "ix_i" ON "t9"."au" = "t3"."ag"
      and "t9"."cv" = "t3"."cv"
      LEFT JOIN "f" as "t7" ON "t9"."cw" = "t7"."cw"
      LEFT JOIN "n" as "t10" INDEXED BY "ix_e" ON "t10"."h" = "t9"."ag"
      and "t10"."cv" = "t9"."cv"
      LEFT JOIN "h" as "t11" ON "t9"."cu" = "t11"."cu"
      LEFT JOIN "ad" as "t8" ON "t8"."cv" = "t3"."cv"
      and "t8"."ay" = "t3"."ay"
    WHERE
      "t9"."cv" = 4797271::int
      and coalesce ("t9"."cs"::datetime, "t9"."a"::datetime) = '2024-08-14 0:00:00.0 +00:00:00'::datetime
      and "t7"."z" = 8::int
      and coalesce ("t3"."aw"::int, "t9"."bf"::int) in (42::int, 45::int)
  ) as "cte2"
''
plan:
    [1] SCAN TABLE _tmp_10757321093052108077_2136 (~1048576 rows)
    [3] SEARCH TABLE n AS t9 USING COVERING INDEX ix_ao (cv=?) (~9 rows)
        [3] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [3] SEARCH TABLE f AS t7 USING PRIMARY KEY (cw=?) (~1 row)
                [3] SEARCH TABLE n AS t10 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
                    [3] SEARCH TABLE h AS t11 USING PRIMARY KEY (cu=?) (~1 row)
                        [3] SEARCH TABLE ad AS t8 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [3] USE TEMP B-TREE FOR DISTINCT
    [2] SCAN SUBQUERY 3 AS cte2 (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets <= [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q58
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  a;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union all
  projection (
    to_char(a.a::datetime::datetime, '%Y-%m-%d'::text)::text -> c0,
    sum(a.f::decimal::decimal)::decimal -> c2,
    sum(a.e::decimal::decimal)::decimal -> c3,
    0::int -> c4
  )
    group by (a.a::datetime)
      selection (
        (
          a.cv::int = 4797271::int
          and a.aw::int in ROW(42::int, 45::int)
          and a.bm::int = 2023::int
          and a.bk::int = 9::int
        )
      )
        scan a
  projection (
    cte0.a::text -> a,
    0::int -> c2,
    0::int -> c3,
    coalesce(
      sum(
        (
          cte0.d::decimal * cte0.cj::int * case 
            when not cte0.h::int is null then -1::int
            else 1::int
          end
        )::decimal
      )::decimal::decimal,
      0.0::decimal
    )::decimal -> c4
  )
    group by (cte0.a::text)
      scan cte cte0($0)
subquery $0:
  projection (
    to_char(
      coalesce(
        t0.cs::datetime::datetime,
        t0.a::datetime::datetime
      )::datetime::datetime,
      '%Y-%m-%d'::text
    )::text -> a,
    t0.d::decimal -> d,
    coalesce(t3.cj::int::int, 0::int)::int -> cj,
    t0.h::int -> h
  )
    selection (
      (
        t0.cv::int = 4797271::int
        and coalesce(t2.aw::int::int, t0.bf::int::int)::int in ROW(42::int, 45::int)
        and t1.ag::int is null
        and (t0.h::int is null or t0.h::int = -1::int)
        and to_char(
          coalesce(
            t0.cs::datetime::datetime,
            t0.a::datetime::datetime
          )::datetime::datetime,
          '%Y'::text
        )::text::int = 2023::int
        and to_char(
          coalesce(
            t0.cs::datetime::datetime,
            t0.a::datetime::datetime
          )::datetime::datetime,
          '%m'::text
        )::text::int = 9::int
      )
    )
      left join on (t0.cw::int = t3.cw::int)
        left join on (
          ROW(t0.au::int, t0.cv::int) = ROW(t2.ag::int, t2.cv::int)
        )
          left join on (
            ROW(t0.ag::int, t0.cv::int) = ROW(t1.h::int, t1.cv::int)
          )
            scan n -> t0
            scan n -> t1
          scan y -> t2
        scan f -> t3
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "to_char" ("a"."a"::datetime, '%Y-%m-%d'::text) as "c0",
  sum ("a"."f"::decimal) as "c2",
  sum ("a"."e"::decimal) as "c3",
  0::int as "c4"
FROM
  "a"
WHERE
  "a"."cv" = 4797271::int
  and "a"."aw" in (42::int, 45::int)
  and "a"."bm" = 2023::int
  and "a"."bk" = 9::int
GROUP BY
  "a"."a"
UNION ALL
SELECT
  "cte0"."a",
  0::int as "c2",
  0::int as "c3",
  coalesce (
    sum (
      (
        "cte0"."d" * "cte0"."cj" * CASE
          WHEN not "cte0"."h" is null THEN (-1)::int
          ELSE 1::int
        END
      )::decimal
    )::decimal,
    0.0::decimal
  ) as "c4"
FROM
  (
    SELECT
      "to_char" (
        coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
        '%Y-%m-%d'::text
      ) as "a",
      "t0"."d",
      coalesce ("t3"."cj"::int, 0::int) as "cj",
      "t0"."h"
    FROM
      "n" as "t0"
      LEFT JOIN "n" as "t1" ON "t1"."h" = "t0"."ag"
      and "t1"."cv" = "t0"."cv"
      LEFT JOIN "y" as "t2" ON "t0"."au" = "t2"."ag"
      and "t0"."cv" = "t2"."cv"
      LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
    WHERE
      "t0"."cv" = 4797271::int
      and coalesce ("t2"."aw"::int, "t0"."bf"::int) in (42::int, 45::int)
      and "t1"."ag" is null
      and (
        "t0"."h" is null
        or "t0"."h" = (-1)::int
      )
      and "to_char" (
        coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
        '%Y'::text
      )::int = 2023::int
      and "to_char" (
        coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
        '%m'::text
      )::int = 9::int
  ) as "cte0"
GROUP BY
  "cte0"."a"
''
plan:
    [1] SEARCH TABLE a USING COVERING INDEX ix_l (cv=?) (~8 rows)
    [2] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~8 rows)
        [2] SEARCH TABLE n AS t1 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [2] SEARCH TABLE y AS t2 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [2] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
    [2] USE TEMP B-TREE FOR GROUP BY
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [874]

-- TEST: corpus-explain-q59
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
motion [policy: full, program: RemoveDuplicates]
  union
    projection (
      t0.ay::text -> ay,
      t0.bg::text -> bg,
      t0.ah::text -> ah,
      t1.cx::text -> cx
    )
      selection (
        (
          t0.cv::int = 4797271::int
          and t0.aw::int in ROW(42::int, 45::int)
          and t0.bm::int = 2023::int
          and t0.bk::int = 9::int
        )
      )
        left join on (
          ROW(t0.cv::int, t0.ay::text) = ROW(t1.cv::int, t1.ay::text)
        )
          scan a -> t0 (indexed by ix_b)
          scan ad -> t1
    projection (
      t2.ay::text -> c0,
      coalesce(t4.bg::text::text, t2.bg::text::text)::text -> c1,
      coalesce(t4.ah::text::text, t2.ah::text::text)::text -> c2,
      t1.cx::text -> c3
    )
      selection (
        (
          t2.cv::int = 4797271::int
          and coalesce(t4.aw::int::int, t2.bf::int::int)::int in ROW(42::int, 45::int)
          and to_char(
            coalesce(
              t2.cs::datetime::datetime,
              t2.a::datetime::datetime
            )::datetime::datetime,
            '%Y'::text
          )::text::int = 2023::int
          and to_char(
            coalesce(
              t2.cs::datetime::datetime,
              t2.a::datetime::datetime
            )::datetime::datetime,
            '%m'::text
          )::text::int = 9::int
        )
      )
        left join on (t2.cu::int = t6.cu::int)
          left join on (
            ROW(t2.cv::int, t2.ay::text) = ROW(t1.cv::int, t1.ay::text)
          )
            left join on (t2.cw::int = t5.cw::int)
              left join on (
                ROW(t2.au::int, t2.cv::int) = ROW(t4.ag::int, t4.cv::int)
              )
                left join on (
                  ROW(t2.ag::int, t2.cv::int) = ROW(t3.h::int, t3.cv::int)
                )
                  scan n -> t2
                  scan n -> t3
                scan y -> t4
              scan f -> t5
            scan ad -> t1
          scan h -> t6
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT DISTINCT
  "t0"."ay",
  "t0"."bg",
  "t0"."ah",
  "t1"."cx"
FROM
  "a" as "t0" INDEXED BY "ix_b"
  LEFT JOIN "ad" as "t1" ON "t0"."cv" = "t1"."cv"
  and "t0"."ay" = "t1"."ay"
WHERE
  "t0"."cv" = 4797271::int
  and "t0"."aw" in (42::int, 45::int)
  and "t0"."bm" = 2023::int
  and "t0"."bk" = 9::int
UNION
SELECT DISTINCT
  "t2"."ay" as "c0",
  coalesce ("t4"."bg"::text, "t2"."bg"::text) as "c1",
  coalesce ("t4"."ah"::text, "t2"."ah"::text) as "c2",
  "t1"."cx" as "c3"
FROM
  "n" as "t2"
  LEFT JOIN "n" as "t3" ON "t3"."h" = "t2"."ag"
  and "t3"."cv" = "t2"."cv"
  LEFT JOIN "y" as "t4" ON "t2"."au" = "t4"."ag"
  and "t2"."cv" = "t4"."cv"
  LEFT JOIN "f" as "t5" ON "t2"."cw" = "t5"."cw"
  LEFT JOIN "ad" as "t1" ON "t2"."cv" = "t1"."cv"
  and "t2"."ay" = "t1"."ay"
  LEFT JOIN "h" as "t6" ON "t2"."cu" = "t6"."cu"
WHERE
  "t2"."cv" = 4797271::int
  and coalesce ("t4"."aw"::int, "t2"."bf"::int) in (42::int, 45::int)
  and "to_char" (
    coalesce ("t2"."cs"::datetime, "t2"."a"::datetime)::datetime,
    '%Y'::text
  )::int = 2023::int
  and "to_char" (
    coalesce ("t2"."cs"::datetime, "t2"."a"::datetime)::datetime,
    '%m'::text
  )::int = 9::int
''
plan:
    [1] SEARCH TABLE a AS t0 
         USING COVERING INDEX ix_b 
         (cv=? AND aw=? AND bm=? AND bk=?) 
         (~6 rows)
    [1] EXECUTE LIST SUBQUERY 2
        [1] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [2] SEARCH TABLE n AS t2 USING COVERING INDEX ix_al (cv=?) (~8 rows)
        [2] SEARCH TABLE n AS t3 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [2] SEARCH TABLE y AS t4 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [2] SEARCH TABLE f AS t5 USING PRIMARY KEY (cw=?) (~1 row)
                    [2] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
                        [2] SEARCH TABLE h AS t6 USING PRIMARY KEY (cu=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)
''
buckets = [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [874]

-- TEST: corpus-explain-q60
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  t3.z;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union all
  projection (
    a.z::int -> c0,
    sum(a.f::decimal::decimal)::decimal -> c1,
    sum(a.e::decimal::decimal)::decimal -> c2,
    0::int -> c3
  )
    group by (a.z::int)
      selection (
        (
          a.cv::int = 4797271::int
          and a.aw::int in ROW(42::int, 45::int)
          and a.a::datetime = '2023-09-25 0:00:00.0 +00:00:00'::datetime
        )
      )
        scan a
  projection (
    t3.z::int -> c0,
    0::int -> c1,
    0::int -> c2,
    coalesce(
      sum(
        (
          t0.d::decimal * coalesce(t3.cj::int::decimal, 0::decimal)::decimal * case 
            when not t0.h::int is null then -1::int
            else 1::int
          end
        )::decimal
      )::decimal::decimal,
      0.0::decimal
    )::decimal -> c3
  )
    group by (t3.z::int)
      selection (
        (
          t0.cv::int = 4797271::int
          and coalesce(t2.aw::int::int, t0.bf::int::int)::int in ROW(42::int, 45::int)
          and coalesce(
            t0.cs::datetime::datetime,
            t0.a::datetime::datetime
          )::datetime = '2023-09-25 0:00:00.0 +00:00:00'::datetime
          and t1.ag::int is null
          and (t0.h::int is null or t0.h::int = -1::int)
        )
      )
        left join on (t0.cu::int = t4.cu::int)
          left join on (t0.cw::int = t3.cw::int)
            left join on (
              ROW(t0.au::int, t0.cv::int) = ROW(t2.ag::int, t2.cv::int)
            )
              left join on (
                ROW(t0.ag::int, t0.cv::int) = ROW(t1.h::int, t1.cv::int)
              )
                scan n -> t0
                scan n -> t1
              scan y -> t2
            scan f -> t3
          scan h -> t4
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "a"."z" as "c0",
  sum ("a"."f"::decimal) as "c1",
  sum ("a"."e"::decimal) as "c2",
  0::int as "c3"
FROM
  "a"
WHERE
  "a"."cv" = 4797271::int
  and "a"."aw" in (42::int, 45::int)
  and "a"."a" = '2023-09-25 0:00:00.0 +00:00:00'::datetime
GROUP BY
  "a"."z"
UNION ALL
SELECT
  "t3"."z" as "c0",
  0::int as "c1",
  0::int as "c2",
  coalesce (
    sum (
      (
        "t0"."d" * coalesce ("t3"."cj"::decimal, 0::decimal) * CASE
          WHEN not "t0"."h" is null THEN (-1)::int
          ELSE 1::int
        END
      )::decimal
    )::decimal,
    0.0::decimal
  ) as "c3"
FROM
  "n" as "t0"
  LEFT JOIN "n" as "t1" ON "t1"."h" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
  LEFT JOIN "y" as "t2" ON "t0"."au" = "t2"."ag"
  and "t0"."cv" = "t2"."cv"
  LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
  LEFT JOIN "h" as "t4" ON "t0"."cu" = "t4"."cu"
WHERE
  "t0"."cv" = 4797271::int
  and coalesce ("t2"."aw"::int, "t0"."bf"::int) in (42::int, 45::int)
  and coalesce ("t0"."cs"::datetime, "t0"."a"::datetime) = '2023-09-25 0:00:00.0 +00:00:00'::datetime
  and "t1"."ag" is null
  and (
    "t0"."h" is null
    or "t0"."h" = (-1)::int
  )
GROUP BY
  "t3"."z"
''
plan:
    [1] SEARCH TABLE a USING COVERING INDEX ix_a (cv=? AND aw=? AND a=?) (~7 rows)
    [1] EXECUTE LIST SUBQUERY 2
    [1] USE TEMP B-TREE FOR GROUP BY
    [2] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~8 rows)
        [2] SEARCH TABLE n AS t1 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [2] SEARCH TABLE y AS t2 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [2] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
                    [2] SEARCH TABLE h AS t4 USING PRIMARY KEY (cu=?) (~1 row)
    [2] USE TEMP B-TREE FOR GROUP BY
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [874]

-- TEST: corpus-explain-q61
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
motion [policy: full, program: RemoveDuplicates]
  union
    projection (
      t0.ay::text -> ay,
      t0.bg::text -> bg,
      t0.ah::text -> ah,
      t1.cx::text -> cx
    )
      selection (
        (
          t0.cv::int = 4797271::int
          and t0.aw::int in ROW(42::int, 45::int)
          and t0.a::datetime = '2023-09-25 0:00:00.0 +00:00:00'::datetime
        )
      )
        left join on (
          ROW(t0.cv::int, t0.ay::text) = ROW(t1.cv::int, t1.ay::text)
        )
          scan a -> t0 (indexed by ix_a)
          scan ad -> t1
    projection (
      t2.ay::text -> c0,
      coalesce(t4.bg::text::text, t2.bg::text::text)::text -> c1,
      coalesce(t4.ah::text::text, t2.ah::text::text)::text -> c2,
      t1.cx::text -> c3
    )
      selection (
        (
          t2.cv::int = 4797271::int
          and coalesce(t4.aw::int::int, t2.bf::int::int)::int in ROW(42::int, 45::int)
          and coalesce(
            t2.cs::datetime::datetime,
            t2.a::datetime::datetime
          )::datetime = '2023-09-25 0:00:00.0 +00:00:00'::datetime
        )
      )
        left join on (t2.cu::int = t6.cu::int)
          left join on (
            ROW(t2.cv::int, t2.ay::text) = ROW(t1.cv::int, t1.ay::text)
          )
            left join on (t2.cw::int = t5.cw::int)
              left join on (
                ROW(t2.au::int, t2.cv::int) = ROW(t4.ag::int, t4.cv::int)
              )
                left join on (
                  ROW(t2.ag::int, t2.cv::int) = ROW(t3.h::int, t3.cv::int)
                )
                  scan n -> t2
                  scan n -> t3
                scan y -> t4
              scan f -> t5
            scan ad -> t1
          scan h -> t6
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT DISTINCT
  "t0"."ay",
  "t0"."bg",
  "t0"."ah",
  "t1"."cx"
FROM
  "a" as "t0" INDEXED BY "ix_a"
  LEFT JOIN "ad" as "t1" ON "t0"."cv" = "t1"."cv"
  and "t0"."ay" = "t1"."ay"
WHERE
  "t0"."cv" = 4797271::int
  and "t0"."aw" in (42::int, 45::int)
  and "t0"."a" = '2023-09-25 0:00:00.0 +00:00:00'::datetime
UNION
SELECT DISTINCT
  "t2"."ay" as "c0",
  coalesce ("t4"."bg"::text, "t2"."bg"::text) as "c1",
  coalesce ("t4"."ah"::text, "t2"."ah"::text) as "c2",
  "t1"."cx" as "c3"
FROM
  "n" as "t2"
  LEFT JOIN "n" as "t3" ON "t3"."h" = "t2"."ag"
  and "t3"."cv" = "t2"."cv"
  LEFT JOIN "y" as "t4" ON "t2"."au" = "t4"."ag"
  and "t2"."cv" = "t4"."cv"
  LEFT JOIN "f" as "t5" ON "t2"."cw" = "t5"."cw"
  LEFT JOIN "ad" as "t1" ON "t2"."cv" = "t1"."cv"
  and "t2"."ay" = "t1"."ay"
  LEFT JOIN "h" as "t6" ON "t2"."cu" = "t6"."cu"
WHERE
  "t2"."cv" = 4797271::int
  and coalesce ("t4"."aw"::int, "t2"."bf"::int) in (42::int, 45::int)
  and coalesce ("t2"."cs"::datetime, "t2"."a"::datetime) = '2023-09-25 0:00:00.0 +00:00:00'::datetime
''
plan:
    [1] SEARCH TABLE a AS t0 USING COVERING INDEX ix_a (cv=? AND aw=? AND a=?) (~7 rows)
    [1] EXECUTE LIST SUBQUERY 2
        [1] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [2] SEARCH TABLE n AS t2 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [2] SEARCH TABLE n AS t3 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [2] SEARCH TABLE y AS t4 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [2] SEARCH TABLE f AS t5 USING PRIMARY KEY (cw=?) (~1 row)
                    [2] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
                        [2] SEARCH TABLE h AS t6 USING PRIMARY KEY (cu=?) (~1 row)
    [0] COMPOUND SUBQUERIES 1 AND 2 USING TEMP B-TREE (UNION)
''
buckets = [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [874]

-- TEST: corpus-explain-q62
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  cte2;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
union all
  projection (
    cte0.bm::int -> bm,
    cte0.bl::int -> bl,
    cte0.bk::int -> bk,
    cte0.f::decimal -> f,
    cte0.e::decimal -> e,
    cte0.c2::int -> c2
  )
    scan cte cte0($0)
  projection (
    cte2.bm::int -> bm,
    cte2.bl::int -> bl,
    cte2.bk::int -> bk,
    cte2.f::int -> f,
    cte2.e::int -> e,
    cte2.c2::decimal -> c2
  )
    scan cte cte2($2)
subquery $0:
  projection (
    a.bm::int -> bm,
    a.bl::int -> bl,
    a.bk::int -> bk,
    sum(a.f::decimal::decimal)::decimal -> f,
    sum(a.e::decimal::decimal)::decimal -> e,
    0::int -> c2
  )
    group by (a.bm::int, a.bl::int, a.bk::int)
      selection (
        (
          a.cv::int = 4797271::int
          and a.aw::int in ROW(42::int, 45::int)
          and a.bm::int in ROW(2025::int, 2024::int, 2023::int)
        )
      )
        scan a
subquery $1:
  projection (
    to_char(
      coalesce(
        t0.cs::datetime::datetime,
        t0.a::datetime::datetime
      )::datetime::datetime,
      '%Y'::text
    )::text::int -> bm,
    (
      (
        to_char(
          coalesce(
            t0.cs::datetime::datetime,
            t0.a::datetime::datetime
          )::datetime::datetime,
          '%m'::text
        )::text::int - 1::int
      ) / 3::int + 1::int
    )::int -> bl,
    to_char(
      coalesce(
        t0.cs::datetime::datetime,
        t0.a::datetime::datetime
      )::datetime::datetime,
      '%m'::text
    )::text::int -> bk,
    coalesce(
      (
        t0.d::decimal * coalesce(t3.cj::int::decimal, 0.0::decimal)::decimal * case 
          when not t0.h::int is null then -1::int
          else 1::int
        end
      )::decimal,
      0.0::decimal
    )::decimal -> c6
  )
    selection (
      (
        t0.cv::int = 4797271::int
        and coalesce(t2.aw::int::int, t0.bf::int::int)::int in ROW(42::int, 45::int)
        and t1.ag::int is null
        and (t0.h::int is null or t0.h::int = -1::int)
        and to_char(
          coalesce(
            t0.cs::datetime::datetime,
            t0.a::datetime::datetime
          )::datetime::datetime,
          '%Y'::text
        )::text::int in ROW(2025::int, 2024::int, 2023::int)
      )
    )
      left join on (t0.cu::int = t4.cu::int)
        left join on (t0.cw::int = t3.cw::int)
          left join on (
            ROW(t0.au::int, t0.cv::int) = ROW(t2.ag::int, t2.cv::int)
          )
            left join on (
              ROW(t0.ag::int, t0.cv::int) = ROW(t1.h::int, t1.cv::int)
            )
              scan n -> t0
              scan n -> t1
            scan y -> t2
          scan f -> t3
        scan h -> t4
subquery $2:
  projection (
    cte1.bm::int -> bm,
    cte1.bl::int -> bl,
    cte1.bk::int -> bk,
    0::int -> f,
    0::int -> e,
    sum(cte1.c6::decimal::decimal)::decimal -> c2
  )
    group by (cte1.bm::int, cte1.bl::int, cte1.bk::int)
      scan cte cte1($1)
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "a"."bm",
      "a"."bl",
      "a"."bk",
      sum ("a"."f"::decimal) as "f",
      sum ("a"."e"::decimal) as "e",
      0::int as "c2"
    FROM
      "a"
    WHERE
      "a"."cv" = 4797271::int
      and "a"."aw" in (42::int, 45::int)
      and "a"."bm" in (2025::int, 2024::int, 2023::int)
    GROUP BY
      "a"."bm",
      "a"."bl",
      "a"."bk"
  ) as "cte0"
UNION ALL
SELECT
  *
FROM
  (
    SELECT
      "cte1"."bm",
      "cte1"."bl",
      "cte1"."bk",
      0::int as "f",
      0::int as "e",
      sum ("cte1"."c6"::decimal) as "c2"
    FROM
      (
        SELECT
          "to_char" (
            coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
            '%Y'::text
          )::int as "bm",
          (
            (
              "to_char" (
                coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
                '%m'::text
              )::int - 1::int
            ) / 3::int + 1::int
          )::int as "bl",
          "to_char" (
            coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
            '%m'::text
          )::int as "bk",
          coalesce (
            (
              "t0"."d" * coalesce ("t3"."cj"::decimal, 0.0::decimal) * CASE
                WHEN not "t0"."h" is null THEN (-1)::int
                ELSE 1::int
              END
            )::decimal,
            0.0::decimal
          ) as "c6"
        FROM
          "n" as "t0"
          LEFT JOIN "n" as "t1" ON "t1"."h" = "t0"."ag"
          and "t1"."cv" = "t0"."cv"
          LEFT JOIN "y" as "t2" ON "t0"."au" = "t2"."ag"
          and "t0"."cv" = "t2"."cv"
          LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
          LEFT JOIN "h" as "t4" ON "t0"."cu" = "t4"."cu"
        WHERE
          "t0"."cv" = 4797271::int
          and coalesce ("t2"."aw"::int, "t0"."bf"::int) in (42::int, 45::int)
          and "t1"."ag" is null
          and (
            "t0"."h" is null
            or "t0"."h" = (-1)::int
          )
          and "to_char" (
            coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
            '%Y'::text
          )::int in (2025::int, 2024::int, 2023::int)
      ) as "cte1"
    GROUP BY
      "cte1"."bm",
      "cte1"."bl",
      "cte1"."bk"
  ) as "cte2"
''
plan:
    [1] SEARCH TABLE a USING COVERING INDEX ix_m (cv=? AND aw=? AND bm=?) (~7 rows)
    [1] EXECUTE LIST SUBQUERY 2
    [1] EXECUTE LIST SUBQUERY 2
    [1] USE TEMP B-TREE FOR GROUP BY
    [2] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~8 rows)
    [2] EXECUTE LIST SUBQUERY 3
        [2] SEARCH TABLE n AS t1 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [2] SEARCH TABLE y AS t2 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
                [2] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
                    [2] SEARCH TABLE h AS t4 USING PRIMARY KEY (cu=?) (~1 row)
    [2] USE TEMP B-TREE FOR GROUP BY
    [0] COMPOUND SUBQUERIES 1 AND 2 (UNION ALL)
''
buckets = [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [874]

-- TEST: corpus-explain-q63
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  (t0.cv = 3023424);
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t0.cv::int -> c0,
  t0.ag::int -> c1,
  t0.cf::decimal -> c2,
  t0.ce::decimal -> c3,
  case 
    when 0::int > t0.bx::decimal then t0.bx::decimal
    else 0.0::decimal
  end -> c4,
  case 
    when 0::int > t0.bz::decimal then t0.bz::decimal
    else 0.0::decimal
  end -> c5,
  case 
    when 0::int > t0.ca::decimal then t0.ca::decimal
    else 0.0::decimal
  end -> c6,
  case 
    when 0::int > coalesce(t0.cd::decimal::decimal, 0.0::decimal)::decimal + coalesce(t0.cc::decimal::decimal, 0.0::decimal)::decimal then coalesce(t0.cd::decimal::decimal, 0.0::decimal)::decimal + coalesce(t0.cc::decimal::decimal, 0.0::decimal)::decimal
    else 0.0::decimal
  end -> c7,
  case 
    when 0::int > t0.cg::decimal then t0.cg::decimal
    else 0.0::decimal
  end -> c8
)
  selection (t0.cv::int = 3023424::int)
    scan s -> t0
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."cv" as "c0",
  "t0"."ag" as "c1",
  "t0"."cf" as "c2",
  "t0"."ce" as "c3",
  CASE
    WHEN 0::int > "t0"."bx" THEN "t0"."bx"
    ELSE 0.0::decimal
  END as "c4",
  CASE
    WHEN 0::int > "t0"."bz" THEN "t0"."bz"
    ELSE 0.0::decimal
  END as "c5",
  CASE
    WHEN 0::int > "t0"."ca" THEN "t0"."ca"
    ELSE 0.0::decimal
  END as "c6",
  CASE
    WHEN 0::int > coalesce ("t0"."cd"::decimal, 0.0::decimal) + coalesce ("t0"."cc"::decimal, 0.0::decimal) THEN coalesce ("t0"."cd"::decimal, 0.0::decimal) + coalesce ("t0"."cc"::decimal, 0.0::decimal)
    ELSE 0.0::decimal
  END as "c7",
  CASE
    WHEN 0::int > "t0"."cg" THEN "t0"."cg"
    ELSE 0.0::decimal
  END as "c8"
FROM
  "s" as "t0"
WHERE
  "t0"."cv" = 3023424::int
''
plan:
    [0] SEARCH TABLE s AS t0 USING COVERING INDEX ix_be (cv=?) (~10 rows)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q64
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  t1.cq;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t1.cq::int -> c0,
  case 
    when not min(t2.u::datetime::datetime)::datetime is null then to_char(
      min(t2.u::datetime::datetime)::datetime::datetime,
      '%Y-%m-%d'::text
    )::text
    else NULL::unknown
  end -> c1,
  sum(t0.az::decimal::decimal)::decimal -> c2
)
  group by (t1.cq::int)
    selection (
      (
        t0.cv::int = 3023424::int
        and not t3.ap::bool
        and t0.az::decimal > 0::int
      )
    )
      left join on (t1.cu::int = t3.br::int)
        join on (
          ROW(t0.b::int, t0.cv::int) = ROW(t2.b::int, t2.cv::int)
        )
          join on (
            ROW(t0.au::int, t0.cv::int) = ROW(t1.ag::int, t1.cv::int)
          )
            scan v -> t0
            scan y -> t1
          scan x -> t2
        scan i -> t3
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t1"."cq" as "c0",
  CASE
    WHEN not min ("t2"."u"::datetime) is null THEN "to_char" (min ("t2"."u"::datetime)::datetime, '%Y-%m-%d'::text)
    ELSE NULL
  END as "c1",
  sum ("t0"."az"::decimal) as "c2"
FROM
  "v" as "t0"
  INNER JOIN "y" as "t1" ON "t0"."au" = "t1"."ag"
  and "t0"."cv" = "t1"."cv"
  INNER JOIN "x" as "t2" ON "t0"."b" = "t2"."b"
  and "t0"."cv" = "t2"."cv"
  LEFT JOIN "i" as "t3" ON "t1"."cu" = "t3"."br"
WHERE
  "t0"."cv" = 3023424::int
  and not "t3"."ap"
  and "t0"."az" > 0::int
GROUP BY
  "t1"."cq"
''
plan:
    [0] SEARCH TABLE v AS t0 USING COVERING INDEX ix_bk (cv=?) (~9 rows)
        [0] SEARCH TABLE y AS t1 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
            [0] SEARCH TABLE x AS t2 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
                [0] SEARCH TABLE i AS t3 USING PRIMARY KEY (br=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q65
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  t0.ay;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  'KNO'::text -> c0,
  t0.cu::int -> c1,
  t0.ah::text -> c2,
  t0.bg::text -> c3,
  case 
    when (not t1.cw::int is null and t1.cw::int = 3::int) then 1::int
    else 0::int
  end -> c4,
  t0.ay::text -> c5,
  sum(
    (
      case 
        when (
          (t1.cw::int is null or t1.cw::int <> 3::int)
          and t0.bx::decimal > 0::int
          and t0.aw::int <> 1::int
        ) then 0.0::decimal
        else coalesce(t0.bx::decimal::decimal, 0.0::decimal)::decimal
      end::decimal + case 
        when (
          (t1.cw::int is null or t1.cw::int <> 3::int)
          and t0.bz::decimal > 0::int
        ) then 0.0::decimal
        else coalesce(t0.bz::decimal::decimal, 0.0::decimal)::decimal
      end::decimal + case 
        when (
          (t1.cw::int is null or t1.cw::int <> 3::int)
          and t0.ca::decimal > 0::int
        ) then 0.0::decimal
        else coalesce(t0.ca::decimal::decimal, 0.0::decimal)::decimal
      end::decimal + case 
        when (
          (t1.cw::int is null or t1.cw::int <> 3::int)
          and t0.cd::decimal > 0::int
        ) then 0.0::decimal
        else coalesce(t0.cd::decimal::decimal, 0.0::decimal)::decimal
      end::decimal + case 
        when (
          (t1.cw::int is null or t1.cw::int <> 3::int)
          and t0.cc::decimal > 0::int
        ) then 0.0::decimal
        else coalesce(t0.cc::decimal::decimal, 0.0::decimal)::decimal
      end::decimal + case 
        when (
          (t1.cw::int is null or t1.cw::int <> 3::int)
          and t0.cg::decimal > 0::int
        ) then 0.0::decimal
        else coalesce(t0.cg::decimal::decimal, 0.0::decimal)::decimal
      end::decimal
    )::decimal
  )::decimal -> c6,
  sum(
    case 
      when t0.aw::int = 2::int then coalesce(t0.bz::decimal::decimal, 0.0::decimal)::decimal
      else 0.0::decimal
    end::decimal
  )::decimal -> c7,
  sum(
    case 
      when t0.aw::int = 3::int then coalesce(t0.ca::decimal::decimal, 0.0::decimal)::decimal
      else 0.0::decimal
    end::decimal
  )::decimal -> c8,
  sum(
    case 
      when t0.aw::int in ROW(42::int, 45::int) then coalesce(t0.bz::decimal::decimal, 0.0::decimal)::decimal
      else 0.0::decimal
    end::decimal
  )::decimal -> c9
)
  group by (
    t0.cu::int,
    t0.ah::text,
    t0.bg::text,
    case 
      when (not t1.cw::int is null and t1.cw::int = 3::int) then 1::int
      else 0::int
    end,
    t0.ay::text
  )
    selection (t0.cv::int = 3023424::int)
      left join on (t0.cu::int = t1.cu::int)
        scan y -> t0
        scan g -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  'KNO'::text as "c0",
  "t0"."cu" as "c1",
  "t0"."ah" as "c2",
  "t0"."bg" as "c3",
  CASE
    WHEN not "t1"."cw" is null
    and "t1"."cw" = 3::int THEN 1::int
    ELSE 0::int
  END as "c4",
  "t0"."ay" as "c5",
  sum (
    (
      CASE
        WHEN (
          "t1"."cw" is null
          or "t1"."cw" <> 3::int
        )
        and "t0"."bx" > 0::int
        and "t0"."aw" <> 1::int THEN 0.0::decimal
        ELSE coalesce ("t0"."bx"::decimal, 0.0::decimal)
      END::decimal + CASE
        WHEN (
          "t1"."cw" is null
          or "t1"."cw" <> 3::int
        )
        and "t0"."bz" > 0::int THEN 0.0::decimal
        ELSE coalesce ("t0"."bz"::decimal, 0.0::decimal)
      END::decimal + CASE
        WHEN (
          "t1"."cw" is null
          or "t1"."cw" <> 3::int
        )
        and "t0"."ca" > 0::int THEN 0.0::decimal
        ELSE coalesce ("t0"."ca"::decimal, 0.0::decimal)
      END::decimal + CASE
        WHEN (
          "t1"."cw" is null
          or "t1"."cw" <> 3::int
        )
        and "t0"."cd" > 0::int THEN 0.0::decimal
        ELSE coalesce ("t0"."cd"::decimal, 0.0::decimal)
      END::decimal + CASE
        WHEN (
          "t1"."cw" is null
          or "t1"."cw" <> 3::int
        )
        and "t0"."cc" > 0::int THEN 0.0::decimal
        ELSE coalesce ("t0"."cc"::decimal, 0.0::decimal)
      END::decimal + CASE
        WHEN (
          "t1"."cw" is null
          or "t1"."cw" <> 3::int
        )
        and "t0"."cg" > 0::int THEN 0.0::decimal
        ELSE coalesce ("t0"."cg"::decimal, 0.0::decimal)
      END::decimal
    )::decimal
  ) as "c6",
  sum (
    CASE
      WHEN "t0"."aw" = 2::int THEN coalesce ("t0"."bz"::decimal, 0.0::decimal)
      ELSE 0.0::decimal
    END::decimal
  ) as "c7",
  sum (
    CASE
      WHEN "t0"."aw" = 3::int THEN coalesce ("t0"."ca"::decimal, 0.0::decimal)
      ELSE 0.0::decimal
    END::decimal
  ) as "c8",
  sum (
    CASE
      WHEN "t0"."aw" in (42::int, 45::int) THEN coalesce ("t0"."bz"::decimal, 0.0::decimal)
      ELSE 0.0::decimal
    END::decimal
  ) as "c9"
FROM
  "y" as "t0"
  LEFT JOIN "g" as "t1" ON "t0"."cu" = "t1"."cu"
WHERE
  "t0"."cv" = 3023424::int
GROUP BY
  "t0"."cu",
  "t0"."ah",
  "t0"."bg",
  CASE
    WHEN not "t1"."cw" is null
    and "t1"."cw" = 3::int THEN 1::int
    ELSE 0::int
  END,
  "t0"."ay"
''
plan:
    [0] SEARCH TABLE y AS t0 USING COVERING INDEX ix_by (cv=?) (~10 rows)
        [0] SEARCH TABLE g AS t1 USING PRIMARY KEY (cu=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q66
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  ay;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  'BUF'::text -> c7,
  cte0.cu::int -> cu,
  cte0.ah::text -> ah,
  cte0.bg::text -> bg,
  cte0.c4::int -> c4,
  cte0.ay::text -> ay,
  coalesce(
    sum(cte0.aa::decimal::decimal)::decimal::decimal,
    0.0::decimal
  )::decimal -> c8,
  coalesce(
    sum(
      case 
        when cte0.aw::int = 2::int then cte0.aa::decimal
        else 0.0::decimal
      end::decimal
    )::decimal::decimal,
    0.0::decimal
  )::decimal -> c9,
  coalesce(
    sum(
      case 
        when cte0.aw::int = 3::int then cte0.aa::decimal
        else 0.0::decimal
      end::decimal
    )::decimal::decimal,
    0.0::decimal
  )::decimal -> c10,
  coalesce(
    sum(
      case 
        when cte0.aw::int in ROW(42::int, 45::int) then cte0.aa::decimal
        else 0.0::decimal
      end::decimal
    )::decimal::decimal,
    0.0::decimal
  )::decimal -> c11
)
  group by (
    cte0.cu::int,
    cte0.ah::text,
    cte0.bg::text,
    cte0.c4::int,
    cte0.ay::text
  )
    scan cte cte0($0)
subquery $0:
  projection (
    t0.cu::int -> cu,
    t0.ah::text -> ah,
    t0.bg::text -> bg,
    t0.ay::text -> ay,
    case 
      when (not t2.cw::int is null and t2.cw::int = 3::int) then 1::int
      else 0::int
    end -> c4,
    coalesce(
      (
        t0.d::decimal * coalesce(t3.cj::int::decimal, 0::decimal)::decimal * case 
          when not t0.h::int is null then -1::int
          else 1::int
        end
      )::decimal,
      0.0::decimal
    )::decimal -> aa,
    t4.aw::int -> aw
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and t1.ag::int is null
        and (t0.h::int is null or t0.h::int = -1::int)
      )
    )
      left join on (
        ROW(t0.au::int, t0.cv::int) = ROW(t4.ag::int, t4.cv::int)
      )
        left join on (t0.cw::int = t3.cw::int)
          left join on (t0.cu::int = t2.cu::int)
            left join on (
              ROW(t0.ag::int, t0.cv::int) = ROW(t1.h::int, t1.cv::int)
            )
              scan n -> t0
              scan n -> t1
            scan g -> t2
          scan f -> t3
        scan y -> t4
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  'BUF'::text as "c7",
  "cte0"."cu",
  "cte0"."ah",
  "cte0"."bg",
  "cte0"."c4",
  "cte0"."ay",
  coalesce (sum ("cte0"."aa"::decimal)::decimal, 0.0::decimal) as "c8",
  coalesce (
    sum (
      CASE
        WHEN "cte0"."aw" = 2::int THEN "cte0"."aa"
        ELSE 0.0::decimal
      END::decimal
    )::decimal,
    0.0::decimal
  ) as "c9",
  coalesce (
    sum (
      CASE
        WHEN "cte0"."aw" = 3::int THEN "cte0"."aa"
        ELSE 0.0::decimal
      END::decimal
    )::decimal,
    0.0::decimal
  ) as "c10",
  coalesce (
    sum (
      CASE
        WHEN "cte0"."aw" in (42::int, 45::int) THEN "cte0"."aa"
        ELSE 0.0::decimal
      END::decimal
    )::decimal,
    0.0::decimal
  ) as "c11"
FROM
  (
    SELECT
      "t0"."cu",
      "t0"."ah",
      "t0"."bg",
      "t0"."ay",
      CASE
        WHEN not "t2"."cw" is null
        and "t2"."cw" = 3::int THEN 1::int
        ELSE 0::int
      END as "c4",
      coalesce (
        (
          "t0"."d" * coalesce ("t3"."cj"::decimal, 0::decimal) * CASE
            WHEN not "t0"."h" is null THEN (-1)::int
            ELSE 1::int
          END
        )::decimal,
        0.0::decimal
      ) as "aa",
      "t4"."aw"
    FROM
      "n" as "t0"
      LEFT JOIN "n" as "t1" ON "t1"."h" = "t0"."ag"
      and "t1"."cv" = "t0"."cv"
      LEFT JOIN "g" as "t2" ON "t0"."cu" = "t2"."cu"
      LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
      LEFT JOIN "y" as "t4" ON "t0"."au" = "t4"."ag"
      and "t0"."cv" = "t4"."cv"
    WHERE
      "t0"."cv" = 3023424::int
      and "t1"."ag" is null
      and (
        "t0"."h" is null
        or "t0"."h" = (-1)::int
      )
  ) as "cte0"
GROUP BY
  "cte0"."cu",
  "cte0"."ah",
  "cte0"."bg",
  "cte0"."c4",
  "cte0"."ay"
''
plan:
    [0] SEARCH TABLE n AS t0 USING COVERING INDEX ix_ap (cv=?) (~9 rows)
        [0] SEARCH TABLE n AS t1 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [0] SEARCH TABLE g AS t2 USING PRIMARY KEY (cu=?) (~1 row)
                [0] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
                    [0] SEARCH TABLE y AS t4 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q67
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t1.cq::int -> c0,
  to_char(
    coalesce(
      t0.cs::datetime::datetime,
      t0.a::datetime::datetime
    )::datetime::datetime,
    '%Y-%m-%d'::text
  )::text -> c1,
  coalesce(
    (
      t0.d::decimal * coalesce(t3.cj::int::decimal, 0.0::decimal)::decimal * case 
        when not t0.h::int is null then -1::int
        else 1::int
      end
    )::decimal,
    0.0::decimal
  )::decimal -> c2
)
  selection (
    (
      t0.cv::int = 3023424::int
      and t4.ag::int is null
      and (t0.h::int is null or t0.h::int = -1::int)
      and (t2.cw::int is null or t2.cw::int <> 3::int)
    )
  )
    left join on (
      ROW(t0.ag::int, t0.cv::int) = ROW(t4.h::int, t4.cv::int)
    )
      left join on (t0.cw::int = t3.cw::int)
        left join on (t0.cu::int = t2.cu::int)
          left join on (t0.cu::int = t1.cu::int)
            scan n -> t0
            scan h -> t1
          scan g -> t2
        scan f -> t3
      scan n -> t4
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t1"."cq" as "c0",
  "to_char" (
    coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
    '%Y-%m-%d'::text
  ) as "c1",
  coalesce (
    (
      "t0"."d" * coalesce ("t3"."cj"::decimal, 0.0::decimal) * CASE
        WHEN not "t0"."h" is null THEN (-1)::int
        ELSE 1::int
      END
    )::decimal,
    0.0::decimal
  ) as "c2"
FROM
  "n" as "t0"
  LEFT JOIN "h" as "t1" ON "t0"."cu" = "t1"."cu"
  LEFT JOIN "g" as "t2" ON "t0"."cu" = "t2"."cu"
  LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
  LEFT JOIN "n" as "t4" ON "t4"."h" = "t0"."ag"
  and "t4"."cv" = "t0"."cv"
WHERE
  "t0"."cv" = 3023424::int
  and "t4"."ag" is null
  and (
    "t0"."h" is null
    or "t0"."h" = (-1)::int
  )
  and (
    "t2"."cw" is null
    or "t2"."cw" <> 3::int
  )
''
plan:
    [0] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [0] SEARCH TABLE h AS t1 USING PRIMARY KEY (cu=?) (~1 row)
            [0] SEARCH TABLE g AS t2 USING PRIMARY KEY (cu=?) (~1 row)
                [0] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
                    [0] SEARCH TABLE n AS t4 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q68
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t0.ay::text -> c0,
  t0.cu::int -> c1,
  t0.ah::text -> c2,
  t0.bg::text -> c3,
  case 
    when (not t2.cw::int is null and t2.cw::int = 3::int) then 1::int
    else 0::int
  end -> c4,
  to_char(
    coalesce(
      t0.cs::datetime::datetime,
      t0.a::datetime::datetime
    )::datetime::datetime,
    '%Y-%m-%d'::text
  )::text -> c5,
  coalesce(
    (
      t0.d::decimal * coalesce(t3.cj::int::decimal, 0.0::decimal)::decimal * case 
        when not t0.h::int is null then -1::int
        else 1::int
      end
    )::decimal,
    0.0::decimal
  )::decimal -> c6
)
  selection (
    (
      t0.cv::int = 3023424::int
      and t4.ag::int is null
      and (t0.h::int is null or t0.h::int = -1::int)
      and not t2.cw::int is null
      and t2.cw::int = 3::int
    )
  )
    left join on (
      ROW(t0.ag::int, t0.cv::int) = ROW(t4.h::int, t4.cv::int)
    )
      left join on (t0.cw::int = t3.cw::int)
        left join on (t0.cu::int = t2.cu::int)
          left join on (t0.cu::int = t1.cu::int)
            scan n -> t0
            scan h -> t1
          scan g -> t2
        scan f -> t3
      scan n -> t4
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."ay" as "c0",
  "t0"."cu" as "c1",
  "t0"."ah" as "c2",
  "t0"."bg" as "c3",
  CASE
    WHEN not "t2"."cw" is null
    and "t2"."cw" = 3::int THEN 1::int
    ELSE 0::int
  END as "c4",
  "to_char" (
    coalesce ("t0"."cs"::datetime, "t0"."a"::datetime)::datetime,
    '%Y-%m-%d'::text
  ) as "c5",
  coalesce (
    (
      "t0"."d" * coalesce ("t3"."cj"::decimal, 0.0::decimal) * CASE
        WHEN not "t0"."h" is null THEN (-1)::int
        ELSE 1::int
      END
    )::decimal,
    0.0::decimal
  ) as "c6"
FROM
  "n" as "t0"
  LEFT JOIN "h" as "t1" ON "t0"."cu" = "t1"."cu"
  LEFT JOIN "g" as "t2" ON "t0"."cu" = "t2"."cu"
  LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
  LEFT JOIN "n" as "t4" ON "t4"."h" = "t0"."ag"
  and "t4"."cv" = "t0"."cv"
WHERE
  "t0"."cv" = 3023424::int
  and "t4"."ag" is null
  and (
    "t0"."h" is null
    or "t0"."h" = (-1)::int
  )
  and not "t2"."cw" is null
  and "t2"."cw" = 3::int
''
plan:
    [0] SEARCH TABLE n AS t0 USING COVERING INDEX ix_al (cv=?) (~9 rows)
        [0] SEARCH TABLE h AS t1 USING PRIMARY KEY (cu=?) (~1 row)
            [0] SEARCH TABLE g AS t2 USING PRIMARY KEY (cu=?) (~1 row)
                [0] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
                    [0] SEARCH TABLE n AS t4 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q69
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  cq;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t0.cq::int -> c0,
  sum(
    case 
      when t0.bx::decimal < 0::int then coalesce(t0.bx::decimal::decimal, 0::decimal)::decimal
      else 0::int
    end::decimal
  )::decimal -> c1,
  sum(
    case 
      when (
        t0.bx::decimal < 0::int
        and not t1.cw::int is null
        and t1.cw::int = 3::int
      ) then coalesce(t0.bx::decimal::decimal, 0::decimal)::decimal
      else 0::int
    end::decimal
  )::decimal -> c2,
  sum(
    case 
      when (
        t0.bx::decimal > 0::int
        and not t1.cw::int is null
        and t1.cw::int = 3::int
      ) then coalesce(t0.bx::decimal::decimal, 0::decimal)::decimal
      else 0::int
    end::decimal
  )::decimal -> c3
)
  group by (t0.cq::int)
    selection (
      (
        t0.cv::int = 3023424::int
        and t0.bx::decimal <> 0::int
      )
    )
      left join on (t0.cu::int = t1.cu::int)
        scan y -> t0
        scan g -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."cq" as "c0",
  sum (
    CASE
      WHEN "t0"."bx" < 0::int THEN coalesce ("t0"."bx"::decimal, 0::decimal)
      ELSE 0::int
    END::decimal
  ) as "c1",
  sum (
    CASE
      WHEN "t0"."bx" < 0::int
      and not "t1"."cw" is null
      and "t1"."cw" = 3::int THEN coalesce ("t0"."bx"::decimal, 0::decimal)
      ELSE 0::int
    END::decimal
  ) as "c2",
  sum (
    CASE
      WHEN "t0"."bx" > 0::int
      and not "t1"."cw" is null
      and "t1"."cw" = 3::int THEN coalesce ("t0"."bx"::decimal, 0::decimal)
      ELSE 0::int
    END::decimal
  ) as "c3"
FROM
  "y" as "t0"
  LEFT JOIN "g" as "t1" ON "t0"."cu" = "t1"."cu"
WHERE
  "t0"."cv" = 3023424::int
  and "t0"."bx" <> 0::int
GROUP BY
  "t0"."cq"
''
plan:
    [0] SEARCH TABLE y AS t0 USING COVERING INDEX ix_bz (cv=?) (~9 rows)
        [0] SEARCH TABLE g AS t1 USING PRIMARY KEY (cu=?) (~1 row)
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q70
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  t0.ay;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t0.cu::int -> c0,
  t0.ah::text -> c1,
  t0.bg::text -> c2,
  t0.ay::text -> c3,
  sum(
    case 
      when t0.bx::decimal < 0::int then coalesce(t0.bx::decimal::decimal, 0::decimal)::decimal
      else 0::int
    end::decimal
  )::decimal -> c4,
  sum(
    case 
      when (
        t0.bx::decimal < 0::int
        and not t1.cw::int is null
        and t1.cw::int = 3::int
      ) then coalesce(t0.bx::decimal::decimal, 0::decimal)::decimal
      else 0::int
    end::decimal
  )::decimal -> c5,
  sum(
    case 
      when (
        t0.bx::decimal > 0::int
        and not t1.cw::int is null
        and t1.cw::int = 3::int
      ) then coalesce(t0.bx::decimal::decimal, 0::decimal)::decimal
      else 0::int
    end::decimal
  )::decimal -> c6
)
  group by (
    t0.cu::int,
    t0.ah::text,
    t0.bg::text,
    t0.ay::text
  )
    selection (t0.cv::int = 3023424::int)
      left join on (t0.cu::int = t1.cu::int)
        scan y -> t0
        scan g -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."cu" as "c0",
  "t0"."ah" as "c1",
  "t0"."bg" as "c2",
  "t0"."ay" as "c3",
  sum (
    CASE
      WHEN "t0"."bx" < 0::int THEN coalesce ("t0"."bx"::decimal, 0::decimal)
      ELSE 0::int
    END::decimal
  ) as "c4",
  sum (
    CASE
      WHEN "t0"."bx" < 0::int
      and not "t1"."cw" is null
      and "t1"."cw" = 3::int THEN coalesce ("t0"."bx"::decimal, 0::decimal)
      ELSE 0::int
    END::decimal
  ) as "c5",
  sum (
    CASE
      WHEN "t0"."bx" > 0::int
      and not "t1"."cw" is null
      and "t1"."cw" = 3::int THEN coalesce ("t0"."bx"::decimal, 0::decimal)
      ELSE 0::int
    END::decimal
  ) as "c6"
FROM
  "y" as "t0"
  LEFT JOIN "g" as "t1" ON "t0"."cu" = "t1"."cu"
WHERE
  "t0"."cv" = 3023424::int
GROUP BY
  "t0"."cu",
  "t0"."ah",
  "t0"."bg",
  "t0"."ay"
''
plan:
    [0] SEARCH TABLE y AS t0 USING COVERING INDEX ix_by (cv=?) (~10 rows)
        [0] SEARCH TABLE g AS t1 USING PRIMARY KEY (cu=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q71
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  t1.cq;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t1.cq::int -> c0,
  case 
    when not min(t2.u::datetime::datetime)::datetime is null then to_char(
      min(t2.u::datetime::datetime)::datetime::datetime,
      '%Y-%m-%d'::text
    )::text
    else NULL::unknown
  end -> c1,
  sum(t0.az::decimal::decimal)::decimal -> c2
)
  group by (t1.cq::int)
    selection (
      (
        t0.cv::int = 3023424::int
        and not t3.ap::bool
        and t0.az::decimal > 0::int
      )
    )
      left join on (t1.cu::int = t3.br::int)
        join on (
          ROW(t0.b::int, t0.cv::int) = ROW(t2.b::int, t2.cv::int)
        )
          join on (
            ROW(t0.au::int, t0.cv::int) = ROW(t1.ag::int, t1.cv::int)
          )
            scan v -> t0
            scan y -> t1
          scan x -> t2
        scan i -> t3
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t1"."cq" as "c0",
  CASE
    WHEN not min ("t2"."u"::datetime) is null THEN "to_char" (min ("t2"."u"::datetime)::datetime, '%Y-%m-%d'::text)
    ELSE NULL
  END as "c1",
  sum ("t0"."az"::decimal) as "c2"
FROM
  "v" as "t0"
  INNER JOIN "y" as "t1" ON "t0"."au" = "t1"."ag"
  and "t0"."cv" = "t1"."cv"
  INNER JOIN "x" as "t2" ON "t0"."b" = "t2"."b"
  and "t0"."cv" = "t2"."cv"
  LEFT JOIN "i" as "t3" ON "t1"."cu" = "t3"."br"
WHERE
  "t0"."cv" = 3023424::int
  and not "t3"."ap"
  and "t0"."az" > 0::int
GROUP BY
  "t1"."cq"
''
plan:
    [0] SEARCH TABLE v AS t0 USING COVERING INDEX ix_bk (cv=?) (~9 rows)
        [0] SEARCH TABLE y AS t1 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
            [0] SEARCH TABLE x AS t2 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
                [0] SEARCH TABLE i AS t3 USING PRIMARY KEY (br=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q72
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  z;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  d.z::int -> c0,
  sum(d.f::decimal::decimal)::decimal -> c1,
  sum(d.e::decimal::decimal)::decimal -> c2
)
  group by (d.z::int)
    selection (
      (
        d.cv::int = 4797271::int
        and d.cq::int = 4::int
        and d.a::datetime = '2025-08-27 0:00:00.0 +00:00:00'::datetime
      )
    )
      scan d
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "d"."z" as "c0",
  sum ("d"."f"::decimal) as "c1",
  sum ("d"."e"::decimal) as "c2"
FROM
  "d"
WHERE
  "d"."cv" = 4797271::int
  and "d"."cq" = 4::int
  and "d"."a" = '2025-08-27 0:00:00.0 +00:00:00'::datetime
GROUP BY
  "d"."z"
''
plan:
    [0] SEARCH TABLE d USING COVERING INDEX ix_s (cv=? AND a=?) (~8 rows)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [874]

-- TEST: corpus-explain-q73
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  10;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 10
  projection (
    cte2.cv::int -> cv,
    cte2.b::int -> b,
    cte2.ay::text -> ay,
    cte2.bg::text -> bg,
    cte2.cu::int -> cu,
    cte2.cw::int -> cw,
    cte2.ac::int -> ac,
    cte2.ae::text -> ae,
    cte2.ab::datetime -> ab,
    cte2.c9::int -> c9,
    cte2.d::decimal -> d,
    cte2.c11::int -> c11,
    cte2.cx::text -> cx
  )
    selection (cte2.c11::int > 0::int)
      scan cte cte2($2)
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      t0.ag::int -> ag,
      t0.au::int -> au,
      t0.cv::int -> cv
    )
      join on (
        ROW(t0.ag::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
      )
        scan t0
          union all
            projection (
              l.ag::int -> ag,
              l.au::int -> au,
              l.cv::int -> cv
            )
              selection (
                (
                  l.cv::int = 4797271::int
                  and l.cs::datetime >= '2025-08-27 0:00:00.0 +00:00:00'::datetime
                  and l.cs::datetime < '2025-08-28 0:00:00.0 +00:00:00'::datetime
                  and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                )
              )
                scan l
            projection (
              l.ag::int -> ag,
              l.au::int -> au,
              l.cv::int -> cv
            )
              selection (
                (
                  l.cv::int = 4797271::int
                  and l.cs::datetime is null
                  and l.a::datetime >= '2025-08-27 0:00:00.0 +00:00:00'::datetime
                  and l.a::datetime < '2025-08-28 0:00:00.0 +00:00:00'::datetime
                  and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                )
              )
                scan l
        scan x -> t1
subquery $1:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      t6.cv::int -> cv,
      t6.ag::int -> b,
      t3.ay::text -> ay,
      t3.bg::text -> bg,
      t6.cu::int -> cu,
      t6.cw::int -> cw,
      coalesce(t6.co::int::int, t6.cm::int::int, 0::int)::int -> ac,
      case 
        when not t6.co::int is null or not t6.cm::int is null then t6.cn::text
        else '0000'::text
      end -> ae,
      t6.cl::datetime -> ab,
      case 
        when not t6.co::int is null then 0::int
        when not t6.cm::int is null then 1::int
        else 2::int
      end -> c9,
      t5.d::decimal -> d
    )
      selection ((t8.z::int = 6::int and t3.cq::int = 4::int))
        left join on (t8.cw::int = t6.cw::int)
          left join on (
            ROW(t6.ag::int, t6.cv::int) = ROW(t7.b::int, t7.cv::int)
          )
            join on (
              (
                ROW(t5.bs::int, t5.cv::int) = ROW(t6.ag::int, t6.cv::int)
                and coalesce(t6.bb::int::int, 0::int)::int <> 1::int
              )
            )
              join on (
                (
                  ROW(t4.ag::int, t4.cv::int) = ROW(t5.x::int, t5.cv::int)
                  and t5.db::int is null
                  and t5.da::int is null
                )
              )
                join on (
                  (
                    ROW(t2.ag::int, t3.cv::int) = ROW(t4.b::int, t4.cv::int)
                    and t4.ai::decimal > 0::int
                  )
                )
                  join on (
                    ROW(t2.au::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
                  )
                    scan cte t2($0)
                    scan y -> t3 (indexed by ix_i)
                  scan v -> t4 (indexed by ix_f)
                scan w -> t5 (indexed by ix_g)
              scan l -> t6 (indexed by ix_d)
            scan x -> t7 (indexed by ix_h)
          scan f -> t8
subquery $2:
  projection (
    unnamed_join.cv::int -> cv,
    unnamed_join.b::int -> b,
    unnamed_join.ay::text -> ay,
    unnamed_join.bg::text -> bg,
    unnamed_join.cu::int -> cu,
    unnamed_join.cw::int -> cw,
    unnamed_join.ac::int -> ac,
    unnamed_join.ae::text -> ae,
    unnamed_join.ab::datetime -> ab,
    unnamed_join.c9::int -> c9,
    unnamed_join.d::decimal -> d,
    row_number() over (order by (unnamed_join.b::int desc) ) -> c11,
    unnamed_join.cx::text -> cx
  )
    motion [policy: full, program: AddMissingRowsForLeftJoin]
      projection (
        cte1.cv::int -> cv,
        cte1.b::int -> b,
        cte1.ay::text -> ay,
        cte1.bg::text -> bg,
        cte1.cu::int -> cu,
        cte1.cw::int -> cw,
        cte1.ac::int -> ac,
        cte1.ae::text -> ae,
        cte1.ab::datetime -> ab,
        cte1.c9::int -> c9,
        cte1.d::decimal -> d,
        t9.cv::int -> cv,
        t9.ay::text -> ay,
        t9.bucket_id::int -> bucket_id,
        t9.cx::text -> cx
      )
        join on (
          ROW(cte1.cv::int, cte1.ay::text) = ROW(t9.cv::int, t9.ay::text)
        )
          scan cte cte1($1)
          scan ad -> t9
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
  "t0"."ag",
  "t0"."au",
  "t0"."cv"
FROM
  (
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" >= '2025-08-27 0:00:00.0 +00:00:00'::datetime
      and "l"."cs" < '2025-08-28 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
    UNION ALL
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" is null
      and "l"."a" >= '2025-08-27 0:00:00.0 +00:00:00'::datetime
      and "l"."a" < '2025-08-28 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
  ) as "t0"
  INNER JOIN "x" as "t1" ON "t1"."b" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
''
plan:
    [2] SEARCH TABLE l USING COVERING INDEX ix_z (cv=? AND cs>? AND cs<?) (~1 row)
    [3] SEARCH TABLE l USING COVERING INDEX ix_aa (cv=? AND a>? AND a<?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t0 (~1 row)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "t6"."cv",
  "t6"."ag" as "b",
  "t3"."ay",
  "t3"."bg",
  "t6"."cu",
  "t6"."cw",
  coalesce ("t6"."co"::int, "t6"."cm"::int, 0::int) as "ac",
  CASE
    WHEN not "t6"."co" is null
    or not "t6"."cm" is null THEN "t6"."cn"
    ELSE '0000'::text
  END as "ae",
  "t6"."cl" as "ab",
  CASE
    WHEN not "t6"."co" is null THEN 0::int
    WHEN not "t6"."cm" is null THEN 1::int
    ELSE 2::int
  END as "c9",
  "t5"."d"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_8725203292501515120_1136"
  ) as "t2"
  INNER JOIN "y" as "t3" INDEXED BY "ix_i" ON "t3"."ag" = "t2"."COL_1"
  and "t3"."cv" = "t2"."COL_2"
  INNER JOIN "v" as "t4" INDEXED BY "ix_f" ON "t4"."b" = "t2"."COL_0"
  and "t4"."cv" = "t3"."cv"
  and "t4"."ai" > 0::int
  INNER JOIN "w" as "t5" INDEXED BY "ix_g" ON "t5"."x" = "t4"."ag"
  and "t5"."cv" = "t4"."cv"
  and "t5"."db" is null
  and "t5"."da" is null
  INNER JOIN "l" as "t6" INDEXED BY "ix_d" ON "t6"."ag" = "t5"."bs"
  and "t6"."cv" = "t5"."cv"
  and coalesce ("t6"."bb"::int, 0::int) <> 1::int
  LEFT JOIN "x" as "t7" INDEXED BY "ix_h" ON "t7"."b" = "t6"."ag"
  and "t7"."cv" = "t6"."cv"
  LEFT JOIN "f" as "t8" ON "t8"."cw" = "t6"."cw"
WHERE
  "t8"."z" = 6::int
  and "t3"."cq" = 4::int
''
plan:
    [0] SCAN TABLE _tmp_8725203292501515120_1136 (~1048576 rows)
        [0] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [0] SEARCH TABLE v AS t4 USING COVERING INDEX ix_f (b=? AND cv=?) (~8 rows)
                [0] SEARCH TABLE w AS t5 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                    [0] SEARCH TABLE l AS t6 USING COVERING INDEX ix_d (cv=? AND ag=?) (~8 rows)
                        [0] SEARCH TABLE x AS t7 USING COVERING INDEX ix_h (cv=? AND b=?) (~8 rows)
                            [0] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 3. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "cte1"."COL_0" as "cv",
  "cte1"."COL_1" as "b",
  "cte1"."COL_2" as "ay",
  "cte1"."COL_3" as "bg",
  "cte1"."COL_4" as "cu",
  "cte1"."COL_5" as "cw",
  "cte1"."COL_6" as "ac",
  "cte1"."COL_7" as "ae",
  "cte1"."COL_8" as "ab",
  "cte1"."COL_9" as "c9",
  "cte1"."COL_10" as "d",
  "t9"."cv",
  "t9"."ay",
  "t9"."bucket_id",
  "t9"."cx"
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
      "_tmp_16174517406888418969_2136"
  ) as "cte1"
  INNER JOIN "ad" as "t9" ON "t9"."cv" = "cte1"."COL_0"
  and "t9"."ay" = "cte1"."COL_2"
''
plan:
    [0] SCAN TABLE _tmp_16174517406888418969_2136 (~1048576 rows)
        [0] SEARCH TABLE ad AS t9 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "unnamed_join"."COL_0" as "cv",
      "unnamed_join"."COL_1" as "b",
      "unnamed_join"."COL_2" as "ay",
      "unnamed_join"."COL_3" as "bg",
      "unnamed_join"."COL_4" as "cu",
      "unnamed_join"."COL_5" as "cw",
      "unnamed_join"."COL_6" as "ac",
      "unnamed_join"."COL_7" as "ae",
      "unnamed_join"."COL_8" as "ab",
      "unnamed_join"."COL_9" as "c9",
      "unnamed_join"."COL_10" as "d",
      row_number () OVER (
        ORDER BY
          "unnamed_join"."COL_1" DESC) as "c11",
      "unnamed_join"."COL_14" as "cx"
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
          "COL_10",
          "COL_11",
          "COL_12",
          "COL_13",
          "COL_14"
        FROM
          "_tmp_2380985124844724623_3136"
      ) as "unnamed_join"
  ) as "cte2"
WHERE
  "cte2"."c11" > 0::int
LIMIT
  10
''
plan:
    [2] SCAN TABLE _tmp_2380985124844724623_3136 (~1048576 rows)
    [2] USE TEMP B-TREE FOR ORDER BY
    [1] SCAN SUBQUERY 2 (~1 row)
    [0] SCAN SUBQUERY 1 AS cte2 (~1 row)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q74
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  cte1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  count(*)::int -> c1,
  sum(cte1.d::decimal::decimal)::decimal -> c2
)
  scan cte cte1($1)
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      t0.ag::int -> ag,
      t0.au::int -> au,
      t0.cv::int -> cv
    )
      join on (
        ROW(t0.ag::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
      )
        scan t0
          union all
            projection (
              l.ag::int -> ag,
              l.au::int -> au,
              l.cv::int -> cv
            )
              selection (
                (
                  l.cv::int = 4797271::int
                  and l.cs::datetime >= '2025-08-27 0:00:00.0 +00:00:00'::datetime
                  and l.cs::datetime < '2025-08-28 0:00:00.0 +00:00:00'::datetime
                  and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                )
              )
                scan l
            projection (
              l.ag::int -> ag,
              l.au::int -> au,
              l.cv::int -> cv
            )
              selection (
                (
                  l.cv::int = 4797271::int
                  and l.cs::datetime is null
                  and l.a::datetime >= '2025-08-27 0:00:00.0 +00:00:00'::datetime
                  and l.a::datetime < '2025-08-28 0:00:00.0 +00:00:00'::datetime
                  and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                )
              )
                scan l
        scan x -> t1
subquery $1:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      t6.d::decimal * t8.cj::int * case 
        when t6.h::int is null then 1::int
        else -1::int
      end -> d
    )
      selection ((t8.z::int = 6::int and t3.cq::int = 4::int))
        left join on (t8.cw::int = t6.cw::int)
          left join on (
            ROW(t6.ag::int, t6.cv::int) = ROW(t7.b::int, t7.cv::int)
          )
            join on (
              (
                ROW(t5.bs::int, t5.cv::int) = ROW(t6.ag::int, t6.cv::int)
                and coalesce(t6.bb::int::int, 0::int)::int <> 1::int
              )
            )
              join on (
                (
                  ROW(t4.ag::int, t4.cv::int) = ROW(t5.x::int, t5.cv::int)
                  and t5.db::int is null
                  and t5.da::int is null
                )
              )
                join on (
                  (
                    ROW(t2.ag::int, t3.cv::int) = ROW(t4.b::int, t4.cv::int)
                    and t4.ai::decimal > 0::int
                  )
                )
                  join on (
                    ROW(t2.au::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
                  )
                    scan cte t2($0)
                    scan y -> t3 (indexed by ix_i)
                  scan v -> t4 (indexed by ix_f)
                scan w -> t5 (indexed by ix_g)
              scan l -> t6 (indexed by ix_d)
            scan x -> t7 (indexed by ix_h)
          scan f -> t8
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
  "t0"."ag",
  "t0"."au",
  "t0"."cv"
FROM
  (
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" >= '2025-08-27 0:00:00.0 +00:00:00'::datetime
      and "l"."cs" < '2025-08-28 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
    UNION ALL
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" is null
      and "l"."a" >= '2025-08-27 0:00:00.0 +00:00:00'::datetime
      and "l"."a" < '2025-08-28 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
  ) as "t0"
  INNER JOIN "x" as "t1" ON "t1"."b" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
''
plan:
    [2] SEARCH TABLE l USING COVERING INDEX ix_z (cv=? AND cs>? AND cs<?) (~1 row)
    [3] SEARCH TABLE l USING COVERING INDEX ix_aa (cv=? AND a>? AND a<?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t0 (~1 row)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "t6"."d" * "t8"."cj" * CASE
    WHEN "t6"."h" is null THEN 1::int
    ELSE (-1)::int
  END as "d"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_5906582692487102708_0136"
  ) as "t2"
  INNER JOIN "y" as "t3" INDEXED BY "ix_i" ON "t3"."ag" = "t2"."COL_1"
  and "t3"."cv" = "t2"."COL_2"
  INNER JOIN "v" as "t4" INDEXED BY "ix_f" ON "t4"."b" = "t2"."COL_0"
  and "t4"."cv" = "t3"."cv"
  and "t4"."ai" > 0::int
  INNER JOIN "w" as "t5" INDEXED BY "ix_g" ON "t5"."x" = "t4"."ag"
  and "t5"."cv" = "t4"."cv"
  and "t5"."db" is null
  and "t5"."da" is null
  INNER JOIN "l" as "t6" INDEXED BY "ix_d" ON "t6"."ag" = "t5"."bs"
  and "t6"."cv" = "t5"."cv"
  and coalesce ("t6"."bb"::int, 0::int) <> 1::int
  LEFT JOIN "x" as "t7" INDEXED BY "ix_h" ON "t7"."b" = "t6"."ag"
  and "t7"."cv" = "t6"."cv"
  LEFT JOIN "f" as "t8" ON "t8"."cw" = "t6"."cw"
WHERE
  "t8"."z" = 6::int
  and "t3"."cq" = 4::int
''
plan:
    [0] SCAN TABLE _tmp_5906582692487102708_0136 (~1048576 rows)
        [0] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [0] SEARCH TABLE v AS t4 USING COVERING INDEX ix_f (b=? AND cv=?) (~8 rows)
                [0] SEARCH TABLE w AS t5 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                    [0] SEARCH TABLE l AS t6 USING COVERING INDEX ix_d (cv=? AND ag=?) (~8 rows)
                        [0] SEARCH TABLE x AS t7 USING COVERING INDEX ix_h (cv=? AND b=?) (~8 rows)
                            [0] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  count (*) as "c1",
  sum ("cte1"."COL_0"::decimal) as "c2"
FROM
  (
    SELECT
      "COL_0"
    FROM
      "_tmp_2709976974189790209_1136"
  ) as "cte1"
''
plan:
    [0] SCAN TABLE _tmp_2709976974189790209_1136 (~1048576 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q75
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  cte1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  cte1.cv::int -> cv,
  cte1.ay::text -> ay,
  cte1.bg::text -> bg,
  cte1.ah::text -> ah,
  cte1.cu::int -> cu,
  cte1.cx::text -> cx
)
  scan cte cte1($1)
subquery $0:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      t0.ag::int -> ag,
      t0.au::int -> au,
      t0.cv::int -> cv
    )
      join on (
        ROW(t0.ag::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
      )
        scan t0
          union all
            projection (
              l.ag::int -> ag,
              l.au::int -> au,
              l.cv::int -> cv
            )
              selection (
                (
                  l.cv::int = 4797271::int
                  and l.cs::datetime >= '2025-08-27 0:00:00.0 +00:00:00'::datetime
                  and l.cs::datetime < '2025-08-28 0:00:00.0 +00:00:00'::datetime
                  and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                )
              )
                scan l
            projection (
              l.ag::int -> ag,
              l.au::int -> au,
              l.cv::int -> cv
            )
              selection (
                (
                  l.cv::int = 4797271::int
                  and l.cs::datetime is null
                  and l.a::datetime >= '2025-08-27 0:00:00.0 +00:00:00'::datetime
                  and l.a::datetime < '2025-08-28 0:00:00.0 +00:00:00'::datetime
                  and coalesce(l.bb::int::int, 0::int)::int <> 1::int
                )
              )
                scan l
        scan x -> t1
subquery $1:
  projection (
    gr_expr_1::int -> cv,
    gr_expr_2::text -> ay,
    gr_expr_3::text -> bg,
    gr_expr_4::text -> ah,
    gr_expr_5::int -> cu,
    gr_expr_6::text -> cx
  )
    group by (
      gr_expr_1::int,
      gr_expr_2::text,
      gr_expr_3::text,
      gr_expr_4::text,
      gr_expr_5::int,
      gr_expr_6::text
    )
      motion [policy: full, program: ReshardIfNeeded]
        projection (
          t6.cv::int -> gr_expr_1,
          t3.ay::text -> gr_expr_2,
          t3.bg::text -> gr_expr_3,
          t3.ah::text -> gr_expr_4,
          t6.cu::int -> gr_expr_5,
          t9.cx::text -> gr_expr_6
        )
          group by (
            t6.cv::int,
            t3.ay::text,
            t3.bg::text,
            t3.ah::text,
            t6.cu::int,
            t9.cx::text
          )
            selection ((t8.z::int = 6::int and t3.cq::int = 4::int))
              left join on (
                ROW(t3.cv::int, t3.ay::text) = ROW(t9.cv::int, t9.ay::text)
              )
                left join on (t8.cw::int = t6.cw::int)
                  left join on (
                    ROW(t6.ag::int, t6.cv::int) = ROW(t7.b::int, t7.cv::int)
                  )
                    join on (
                      (
                        ROW(t5.bs::int, t5.cv::int) = ROW(t6.ag::int, t6.cv::int)
                        and coalesce(t6.bb::int::int, 0::int)::int <> 1::int
                      )
                    )
                      join on (
                        (
                          ROW(t4.ag::int, t4.cv::int) = ROW(t5.x::int, t5.cv::int)
                          and t5.db::int is null
                          and t5.da::int is null
                        )
                      )
                        join on (
                          (
                            ROW(t2.ag::int, t3.cv::int) = ROW(t4.b::int, t4.cv::int)
                            and t4.ai::decimal > 0::int
                          )
                        )
                          join on (
                            ROW(t2.au::int, t2.cv::int) = ROW(t3.ag::int, t3.cv::int)
                          )
                            scan cte t2($0)
                            scan y -> t3 (indexed by ix_i)
                          scan v -> t4 (indexed by ix_f)
                        scan w -> t5 (indexed by ix_g)
                      scan l -> t6 (indexed by ix_d)
                    scan x -> t7 (indexed by ix_h)
                  scan f -> t8
                scan ad -> t9
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
  "t0"."ag",
  "t0"."au",
  "t0"."cv"
FROM
  (
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" >= '2025-08-27 0:00:00.0 +00:00:00'::datetime
      and "l"."cs" < '2025-08-28 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
    UNION ALL
    SELECT
      "l"."ag",
      "l"."au",
      "l"."cv"
    FROM
      "l"
    WHERE
      "l"."cv" = 4797271::int
      and "l"."cs" is null
      and "l"."a" >= '2025-08-27 0:00:00.0 +00:00:00'::datetime
      and "l"."a" < '2025-08-28 0:00:00.0 +00:00:00'::datetime
      and coalesce ("l"."bb"::int, 0::int) <> 1::int
  ) as "t0"
  INNER JOIN "x" as "t1" ON "t1"."b" = "t0"."ag"
  and "t1"."cv" = "t0"."cv"
''
plan:
    [2] SEARCH TABLE l USING COVERING INDEX ix_z (cv=? AND cs>? AND cs<?) (~1 row)
    [3] SEARCH TABLE l USING COVERING INDEX ix_aa (cv=? AND a>? AND a<?) (~1 row)
    [1] COMPOUND SUBQUERIES 2 AND 3 (UNION ALL)
    [0] SCAN SUBQUERY 1 AS t0 (~1 row)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "t6"."cv" as "gr_expr_1",
  "t3"."ay" as "gr_expr_2",
  "t3"."bg" as "gr_expr_3",
  "t3"."ah" as "gr_expr_4",
  "t6"."cu" as "gr_expr_5",
  "t9"."cx" as "gr_expr_6"
FROM
  (
    SELECT
      "COL_0",
      "COL_1",
      "COL_2"
    FROM
      "_tmp_10431566060019010066_0136"
  ) as "t2"
  INNER JOIN "y" as "t3" INDEXED BY "ix_i" ON "t3"."ag" = "t2"."COL_1"
  and "t3"."cv" = "t2"."COL_2"
  INNER JOIN "v" as "t4" INDEXED BY "ix_f" ON "t4"."b" = "t2"."COL_0"
  and "t3"."cv" = "t4"."cv"
  and "t4"."ai" > 0::int
  INNER JOIN "w" as "t5" INDEXED BY "ix_g" ON "t5"."x" = "t4"."ag"
  and "t5"."cv" = "t4"."cv"
  and "t5"."db" is null
  and "t5"."da" is null
  INNER JOIN "l" as "t6" INDEXED BY "ix_d" ON "t6"."ag" = "t5"."bs"
  and "t6"."cv" = "t5"."cv"
  and coalesce ("t6"."bb"::int, 0::int) <> 1::int
  LEFT JOIN "x" as "t7" INDEXED BY "ix_h" ON "t7"."b" = "t6"."ag"
  and "t7"."cv" = "t6"."cv"
  LEFT JOIN "f" as "t8" ON "t8"."cw" = "t6"."cw"
  LEFT JOIN "ad" as "t9" ON "t9"."cv" = "t3"."cv"
  and "t9"."ay" = "t3"."ay"
WHERE
  "t8"."z" = 6::int
  and "t3"."cq" = 4::int
GROUP BY
  "t6"."cv",
  "t3"."ay",
  "t3"."bg",
  "t3"."ah",
  "t6"."cu",
  "t9"."cx"
''
plan:
    [0] SCAN TABLE _tmp_10431566060019010066_0136 (~1048576 rows)
        [0] SEARCH TABLE y AS t3 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
            [0] SEARCH TABLE v AS t4 USING COVERING INDEX ix_f (b=? AND cv=?) (~8 rows)
                [0] SEARCH TABLE w AS t5 USING COVERING INDEX ix_g (x=? AND cv=?) (~7 rows)
                    [0] SEARCH TABLE l AS t6 USING COVERING INDEX ix_d (cv=? AND ag=?) (~8 rows)
                        [0] SEARCH TABLE x AS t7 USING COVERING INDEX ix_h (cv=? AND b=?) (~8 rows)
                            [0] SEARCH TABLE f AS t8 USING PRIMARY KEY (cw=?) (~1 row)
                                [0] SEARCH TABLE ad AS t9 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 3. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "cte1"."cv",
  "cte1"."ay",
  "cte1"."bg",
  "cte1"."ah",
  "cte1"."cu",
  "cte1"."cx"
FROM
  (
    SELECT DISTINCT
      "COL_0" as "cv",
      "COL_1" as "ay",
      "COL_2" as "bg",
      "COL_3" as "ah",
      "COL_4" as "cu",
      "COL_5" as "cx"
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
          "_tmp_6826455715585436808_1136"
      )
    GROUP BY
      "COL_0",
      "COL_1",
      "COL_2",
      "COL_3",
      "COL_4",
      "COL_5"
  ) as "cte1"
''
plan:
    [1] SCAN TABLE _tmp_6826455715585436808_1136 (~1048576 rows)
    [1] USE TEMP B-TREE FOR GROUP BY
    [1] USE TEMP B-TREE FOR DISTINCT
    [0] SCAN SUBQUERY 1 AS cte1 (~1 row)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q76
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  c0 DESC;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (c0::text, c1::decimal, c2::decimal)
  order by (c0::text desc)
    scan
      projection (
        to_char(d.a::datetime::datetime, '%Y-%m-%d'::text)::text -> c0,
        sum(d.f::decimal::decimal)::decimal -> c1,
        sum(d.e::decimal::decimal)::decimal -> c2
      )
        group by (d.cv::int, d.a::datetime)
          selection (
            (
              d.cv::int = 4797271::int
              and d.cq::int = 4::int
              and d.bm::int = 2025::int
              and d.bk::int = 8::int
            )
          )
            scan d
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "c0",
  "c1",
  "c2"
FROM
  (
    SELECT
      "to_char" ("d"."a"::datetime, '%Y-%m-%d'::text) as "c0",
      sum ("d"."f"::decimal) as "c1",
      sum ("d"."e"::decimal) as "c2"
    FROM
      "d"
    WHERE
      "d"."cv" = 4797271::int
      and "d"."cq" = 4::int
      and "d"."bm" = 2025::int
      and "d"."bk" = 8::int
    GROUP BY
      "d"."cv",
      "d"."a"
  )
ORDER BY
  "c0" DESC
''
plan:
    [0] SEARCH TABLE d USING COVERING INDEX ix_s (cv=?) (~8 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [874]

-- TEST: corpus-explain-q77
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  bm;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  e.bk::int -> c0,
  e.bl::int -> c1,
  e.bm::int -> c2,
  e.cq::int -> c3,
  sum(e.f::decimal::decimal)::decimal -> c4,
  sum(e.e::decimal::decimal)::decimal -> c5
)
  group by (e.cq::int, e.bk::int, e.bl::int, e.bm::int)
    selection (e.cv::int = 4797271::int)
      scan e
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "e"."bk" as "c0",
  "e"."bl" as "c1",
  "e"."bm" as "c2",
  "e"."cq" as "c3",
  sum ("e"."f"::decimal) as "c4",
  sum ("e"."e"::decimal) as "c5"
FROM
  "e"
WHERE
  "e"."cv" = 4797271::int
GROUP BY
  "e"."cq",
  "e"."bk",
  "e"."bl",
  "e"."bm"
''
plan:
    [0] SEARCH TABLE e USING PRIMARY KEY (cv=?) (~10 rows)
''
buckets = [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [874]

-- TEST: corpus-explain-q78
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  1;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 1
  projection (ag::int, cf::decimal, ce::decimal)
    order by (ag::int)
      scan
        projection (
          t0.ag::int -> ag,
          t0.cf::decimal -> cf,
          t0.ce::decimal -> ce
        )
          selection (t0.cv::int = 3023424::int)
            scan s -> t0
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "ag",
  "cf",
  "ce"
FROM
  (
    SELECT
      "t0"."ag",
      "t0"."cf",
      "t0"."ce"
    FROM
      "s" as "t0"
    WHERE
      "t0"."cv" = 3023424::int
  )
ORDER BY
  "ag"
LIMIT
  1
''
plan:
    [0] SEARCH TABLE s AS t0 USING COVERING INDEX ix_be (cv=?) (~10 rows)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q79
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  t1.cq;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t1.cq::int -> c0,
  case 
    when not min(t2.u::datetime::datetime)::datetime is null then to_char(
      min(t2.u::datetime::datetime)::datetime::datetime,
      '%Y-%m-%d'::text
    )::text
    else NULL::unknown
  end -> c1,
  sum(t0.az::decimal::decimal)::decimal -> c2
)
  group by (t1.cq::int)
    selection (
      (
        t0.cv::int = 3023424::int
        and not t3.ap::bool
        and t0.az::decimal > 0::int
      )
    )
      left join on (t1.cu::int = t3.br::int)
        join on (
          ROW(t0.b::int, t0.cv::int) = ROW(t2.b::int, t2.cv::int)
        )
          join on (
            ROW(t0.au::int, t0.cv::int) = ROW(t1.ag::int, t1.cv::int)
          )
            scan v -> t0
            scan y -> t1
          scan x -> t2
        scan i -> t3
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t1"."cq" as "c0",
  CASE
    WHEN not min ("t2"."u"::datetime) is null THEN "to_char" (min ("t2"."u"::datetime)::datetime, '%Y-%m-%d'::text)
    ELSE NULL
  END as "c1",
  sum ("t0"."az"::decimal) as "c2"
FROM
  "v" as "t0"
  INNER JOIN "y" as "t1" ON "t0"."au" = "t1"."ag"
  and "t0"."cv" = "t1"."cv"
  INNER JOIN "x" as "t2" ON "t0"."b" = "t2"."b"
  and "t0"."cv" = "t2"."cv"
  LEFT JOIN "i" as "t3" ON "t1"."cu" = "t3"."br"
WHERE
  "t0"."cv" = 3023424::int
  and not "t3"."ap"
  and "t0"."az" > 0::int
GROUP BY
  "t1"."cq"
''
plan:
    [0] SEARCH TABLE v AS t0 USING COVERING INDEX ix_bk (cv=?) (~9 rows)
        [0] SEARCH TABLE y AS t1 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
            [0] SEARCH TABLE x AS t2 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
                [0] SEARCH TABLE i AS t3 USING PRIMARY KEY (br=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q80
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  t0.ay;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  'KNO'::text -> c0,
  t0.cu::int -> c1,
  t0.ah::text -> c2,
  t0.bg::text -> c3,
  case 
    when (not t1.cw::int is null and t1.cw::int = 3::int) then 1::int
    else 0::int
  end -> c4,
  t0.ay::text -> c5,
  sum(
    (
      case 
        when (
          (t1.cw::int is null or t1.cw::int <> 3::int)
          and t0.bx::decimal > 0::int
          and t0.aw::int <> 1::int
        ) then 0.0::decimal
        else coalesce(t0.bx::decimal::decimal, 0.0::decimal)::decimal
      end::decimal + case 
        when (
          (t1.cw::int is null or t1.cw::int <> 3::int)
          and t0.bz::decimal > 0::int
        ) then 0.0::decimal
        else coalesce(t0.bz::decimal::decimal, 0.0::decimal)::decimal
      end::decimal + case 
        when (
          (t1.cw::int is null or t1.cw::int <> 3::int)
          and t0.ca::decimal > 0::int
        ) then 0.0::decimal
        else coalesce(t0.ca::decimal::decimal, 0.0::decimal)::decimal
      end::decimal + case 
        when (
          (t1.cw::int is null or t1.cw::int <> 3::int)
          and t0.cd::decimal > 0::int
        ) then 0.0::decimal
        else coalesce(t0.cd::decimal::decimal, 0.0::decimal)::decimal
      end::decimal + case 
        when (
          (t1.cw::int is null or t1.cw::int <> 3::int)
          and t0.cc::decimal > 0::int
        ) then 0.0::decimal
        else coalesce(t0.cc::decimal::decimal, 0.0::decimal)::decimal
      end::decimal + case 
        when (
          (t1.cw::int is null or t1.cw::int <> 3::int)
          and t0.cg::decimal > 0::int
        ) then 0.0::decimal
        else coalesce(t0.cg::decimal::decimal, 0.0::decimal)::decimal
      end::decimal
    )::decimal
  )::decimal -> c6,
  sum(
    case 
      when t0.aw::int = 2::int then coalesce(t0.bz::decimal::decimal, 0.0::decimal)::decimal
      else 0.0::decimal
    end::decimal
  )::decimal -> c7,
  sum(
    case 
      when t0.aw::int = 3::int then coalesce(t0.ca::decimal::decimal, 0.0::decimal)::decimal
      else 0.0::decimal
    end::decimal
  )::decimal -> c8,
  sum(
    case 
      when t0.aw::int in ROW(42::int, 45::int) then coalesce(t0.bz::decimal::decimal, 0.0::decimal)::decimal
      else 0.0::decimal
    end::decimal
  )::decimal -> c9
)
  group by (
    t0.cu::int,
    t0.ah::text,
    t0.bg::text,
    case 
      when (not t1.cw::int is null and t1.cw::int = 3::int) then 1::int
      else 0::int
    end,
    t0.ay::text
  )
    selection (t0.cv::int = 3023424::int)
      left join on (t0.cu::int = t1.cu::int)
        scan y -> t0
        scan g -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  'KNO'::text as "c0",
  "t0"."cu" as "c1",
  "t0"."ah" as "c2",
  "t0"."bg" as "c3",
  CASE
    WHEN not "t1"."cw" is null
    and "t1"."cw" = 3::int THEN 1::int
    ELSE 0::int
  END as "c4",
  "t0"."ay" as "c5",
  sum (
    (
      CASE
        WHEN (
          "t1"."cw" is null
          or "t1"."cw" <> 3::int
        )
        and "t0"."bx" > 0::int
        and "t0"."aw" <> 1::int THEN 0.0::decimal
        ELSE coalesce ("t0"."bx"::decimal, 0.0::decimal)
      END::decimal + CASE
        WHEN (
          "t1"."cw" is null
          or "t1"."cw" <> 3::int
        )
        and "t0"."bz" > 0::int THEN 0.0::decimal
        ELSE coalesce ("t0"."bz"::decimal, 0.0::decimal)
      END::decimal + CASE
        WHEN (
          "t1"."cw" is null
          or "t1"."cw" <> 3::int
        )
        and "t0"."ca" > 0::int THEN 0.0::decimal
        ELSE coalesce ("t0"."ca"::decimal, 0.0::decimal)
      END::decimal + CASE
        WHEN (
          "t1"."cw" is null
          or "t1"."cw" <> 3::int
        )
        and "t0"."cd" > 0::int THEN 0.0::decimal
        ELSE coalesce ("t0"."cd"::decimal, 0.0::decimal)
      END::decimal + CASE
        WHEN (
          "t1"."cw" is null
          or "t1"."cw" <> 3::int
        )
        and "t0"."cc" > 0::int THEN 0.0::decimal
        ELSE coalesce ("t0"."cc"::decimal, 0.0::decimal)
      END::decimal + CASE
        WHEN (
          "t1"."cw" is null
          or "t1"."cw" <> 3::int
        )
        and "t0"."cg" > 0::int THEN 0.0::decimal
        ELSE coalesce ("t0"."cg"::decimal, 0.0::decimal)
      END::decimal
    )::decimal
  ) as "c6",
  sum (
    CASE
      WHEN "t0"."aw" = 2::int THEN coalesce ("t0"."bz"::decimal, 0.0::decimal)
      ELSE 0.0::decimal
    END::decimal
  ) as "c7",
  sum (
    CASE
      WHEN "t0"."aw" = 3::int THEN coalesce ("t0"."ca"::decimal, 0.0::decimal)
      ELSE 0.0::decimal
    END::decimal
  ) as "c8",
  sum (
    CASE
      WHEN "t0"."aw" in (42::int, 45::int) THEN coalesce ("t0"."bz"::decimal, 0.0::decimal)
      ELSE 0.0::decimal
    END::decimal
  ) as "c9"
FROM
  "y" as "t0"
  LEFT JOIN "g" as "t1" ON "t0"."cu" = "t1"."cu"
WHERE
  "t0"."cv" = 3023424::int
GROUP BY
  "t0"."cu",
  "t0"."ah",
  "t0"."bg",
  CASE
    WHEN not "t1"."cw" is null
    and "t1"."cw" = 3::int THEN 1::int
    ELSE 0::int
  END,
  "t0"."ay"
''
plan:
    [0] SEARCH TABLE y AS t0 USING COVERING INDEX ix_by (cv=?) (~10 rows)
        [0] SEARCH TABLE g AS t1 USING PRIMARY KEY (cu=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q81
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  ay;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  'BUF'::text -> c7,
  cte0.cu::int -> cu,
  cte0.cu::int -> cu,
  cte0.ah::text -> ah,
  cte0.bg::text -> bg,
  cte0.c4::int -> c4,
  cte0.ay::text -> ay,
  coalesce(
    sum(cte0.aa::decimal::decimal)::decimal::decimal,
    0.0::decimal
  )::decimal -> c8,
  coalesce(
    sum(
      case 
        when cte0.aw::int = 2::int then cte0.aa::decimal
        else 0.0::decimal
      end::decimal
    )::decimal::decimal,
    0.0::decimal
  )::decimal -> c9,
  coalesce(
    sum(
      case 
        when cte0.aw::int = 3::int then cte0.aa::decimal
        else 0.0::decimal
      end::decimal
    )::decimal::decimal,
    0.0::decimal
  )::decimal -> c10,
  coalesce(
    sum(
      case 
        when cte0.aw::int in ROW(42::int, 45::int) then cte0.aa::decimal
        else 0.0::decimal
      end::decimal
    )::decimal::decimal,
    0.0::decimal
  )::decimal -> c11
)
  group by (
    cte0.cu::int,
    cte0.ah::text,
    cte0.bg::text,
    cte0.c4::int,
    cte0.ay::text
  )
    scan cte cte0($0)
subquery $0:
  projection (
    t0.cu::int -> cu,
    t0.ah::text -> ah,
    t0.bg::text -> bg,
    t0.ay::text -> ay,
    case 
      when (not t2.cw::int is null and t2.cw::int = 3::int) then 1::int
      else 0::int
    end -> c4,
    coalesce(
      (
        t0.d::decimal * coalesce(t3.cj::int::decimal, 0::decimal)::decimal * case 
          when not t0.h::int is null then -1::int
          else 1::int
        end
      )::decimal,
      0.0::decimal
    )::decimal -> aa,
    t4.aw::int -> aw
  )
    selection (
      (
        t0.cv::int = 3023424::int
        and t1.ag::int is null
        and (t0.h::int is null or t0.h::int = -1::int)
      )
    )
      left join on (
        ROW(t0.au::int, t0.cv::int) = ROW(t4.ag::int, t4.cv::int)
      )
        left join on (t0.cw::int = t3.cw::int)
          left join on (t0.cu::int = t2.cu::int)
            left join on (
              ROW(t0.ag::int, t0.cv::int) = ROW(t1.h::int, t1.cv::int)
            )
              scan n -> t0
              scan n -> t1
            scan g -> t2
          scan f -> t3
        scan y -> t4
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  'BUF'::text as "c7",
  "cte0"."cu",
  "cte0"."cu",
  "cte0"."ah",
  "cte0"."bg",
  "cte0"."c4",
  "cte0"."ay",
  coalesce (sum ("cte0"."aa"::decimal)::decimal, 0.0::decimal) as "c8",
  coalesce (
    sum (
      CASE
        WHEN "cte0"."aw" = 2::int THEN "cte0"."aa"
        ELSE 0.0::decimal
      END::decimal
    )::decimal,
    0.0::decimal
  ) as "c9",
  coalesce (
    sum (
      CASE
        WHEN "cte0"."aw" = 3::int THEN "cte0"."aa"
        ELSE 0.0::decimal
      END::decimal
    )::decimal,
    0.0::decimal
  ) as "c10",
  coalesce (
    sum (
      CASE
        WHEN "cte0"."aw" in (42::int, 45::int) THEN "cte0"."aa"
        ELSE 0.0::decimal
      END::decimal
    )::decimal,
    0.0::decimal
  ) as "c11"
FROM
  (
    SELECT
      "t0"."cu",
      "t0"."ah",
      "t0"."bg",
      "t0"."ay",
      CASE
        WHEN not "t2"."cw" is null
        and "t2"."cw" = 3::int THEN 1::int
        ELSE 0::int
      END as "c4",
      coalesce (
        (
          "t0"."d" * coalesce ("t3"."cj"::decimal, 0::decimal) * CASE
            WHEN not "t0"."h" is null THEN (-1)::int
            ELSE 1::int
          END
        )::decimal,
        0.0::decimal
      ) as "aa",
      "t4"."aw"
    FROM
      "n" as "t0"
      LEFT JOIN "n" as "t1" ON "t1"."h" = "t0"."ag"
      and "t1"."cv" = "t0"."cv"
      LEFT JOIN "g" as "t2" ON "t0"."cu" = "t2"."cu"
      LEFT JOIN "f" as "t3" ON "t0"."cw" = "t3"."cw"
      LEFT JOIN "y" as "t4" ON "t0"."au" = "t4"."ag"
      and "t0"."cv" = "t4"."cv"
    WHERE
      "t0"."cv" = 3023424::int
      and "t1"."ag" is null
      and (
        "t0"."h" is null
        or "t0"."h" = (-1)::int
      )
  ) as "cte0"
GROUP BY
  "cte0"."cu",
  "cte0"."ah",
  "cte0"."bg",
  "cte0"."c4",
  "cte0"."ay"
''
plan:
    [0] SEARCH TABLE n AS t0 USING COVERING INDEX ix_ap (cv=?) (~9 rows)
        [0] SEARCH TABLE n AS t1 USING COVERING INDEX ix_e (cv=? AND h=?) (~8 rows)
            [0] SEARCH TABLE g AS t2 USING PRIMARY KEY (cu=?) (~1 row)
                [0] SEARCH TABLE f AS t3 USING PRIMARY KEY (cw=?) (~1 row)
                    [0] SEARCH TABLE y AS t4 USING COVERING INDEX ix_i (cv=? AND ag=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q82
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
SELECT DISTINCT
  au AS c0
FROM
  v t0
  JOIN x t1 ON t0.b = t1.b
  AND t0.cv = t1.cv
WHERE
  (t0.cv = 2466827)
  AND t0.az > 0;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (t0.au::int -> c0)
  selection (
    (
      t0.cv::int = 2466827::int
      and t0.az::decimal > 0::int
    )
  )
    join on (
      ROW(t0.b::int, t0.cv::int) = ROW(t1.b::int, t1.cv::int)
    )
      scan v -> t0
      scan x -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT DISTINCT
  "t0"."au" as "c0"
FROM
  "v" as "t0"
  INNER JOIN "x" as "t1" ON "t0"."b" = "t1"."b"
  and "t0"."cv" = "t1"."cv"
WHERE
  "t0"."cv" = 2466827::int
  and "t0"."az" > 0::int
''
plan:
    [0] SEARCH TABLE v AS t0 USING COVERING INDEX ix_bk (cv=?) (~9 rows)
        [0] SEARCH TABLE x AS t1 USING COVERING INDEX ix_h (cv=? AND b=?) (~1 row)
    [0] USE TEMP B-TREE FOR DISTINCT
''
buckets = [613]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [613]

-- TEST: corpus-explain-q83
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (count(*)::int -> c0)
  scan unnamed_subquery
    projection (
      t0.cq::int -> cq,
      t2.aj::text -> aj,
      t2.am::int -> am,
      t0.aw::int -> aw,
      t0.ay::text -> ay,
      t0.cu::int -> cu,
      t0.bg::text -> bg,
      t1.cz::bool -> cz
    )
      selection (
        (
          t0.cv::int = 2466827::int
          and t0.ag::int in ROW(126412759::int, 126412766::int, 566228222::int)
          and t0.cq::int = 4::int
        )
      )
        left join on (
          ROW(t0.cv::int, t0.ay::text) = ROW(t2.cv::int, t2.ay::text)
        )
          left join on (t0.cu::int = t1.br::int)
            scan y -> t0
            scan i -> t1
          scan ab -> t2
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  count (*) as "c0"
FROM
  (
    SELECT DISTINCT
      "t0"."cq",
      "t2"."aj",
      "t2"."am",
      "t0"."aw",
      "t0"."ay",
      "t0"."cu",
      "t0"."bg",
      "t1"."cz"
    FROM
      "y" as "t0"
      LEFT JOIN "i" as "t1" ON "t0"."cu" = "t1"."br"
      LEFT JOIN "ab" as "t2" ON "t0"."cv" = "t2"."cv"
      and "t0"."ay" = "t2"."ay"
    WHERE
      "t0"."cv" = 2466827::int
      and "t0"."ag" in (126412759::int, 126412766::int, 566228222::int)
      and "t0"."cq" = 4::int
  ) as "unnamed_subquery"
''
plan:
    [1] SEARCH TABLE y AS t0 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
    [1] EXECUTE LIST SUBQUERY 2
        [1] SEARCH TABLE i AS t1 USING PRIMARY KEY (br=?) (~1 row)
            [1] SEARCH TABLE ab AS t2 USING PRIMARY KEY (cv=?) (~1 row)
    [1] USE TEMP B-TREE FOR DISTINCT
    [0] SCAN SUBQUERY 1 AS unnamed_subquery (~1 row)
''
buckets = [613]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [613]

-- TEST: corpus-explain-q84
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  10;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 10
  projection (
    cte1.cq::int -> cq,
    cte1.aw::int -> aw,
    cte1.ay::text -> ay,
    cte1.cu::int -> cu,
    cte1.bg::text -> bg,
    cte1.cz::bool -> cz,
    cte1.aj::text -> aj,
    cte1.am::int -> am,
    cte1.bv::decimal -> bv,
    cte1.c9::int -> c9
  )
    selection (cte1.c9::int > 0::int)
      scan cte cte1($1)
subquery $0:
  projection (
    t0.cq::int -> cq,
    t0.aw::int -> aw,
    t0.ay::text -> ay,
    t0.cu::int -> cu,
    t0.bg::text -> bg,
    t1.cz::bool -> cz,
    t2.aj::text -> aj,
    t2.am::int -> am,
    coalesce(
      sum(
        (
          t0.bx::decimal + t0.bz::decimal + t0.ca::decimal + t0.cd::decimal + t0.cc::decimal + t0.cg::decimal
        )::decimal
      )::decimal::decimal,
      0.0::decimal
    )::decimal -> bv
  )
    group by (
      t0.cq::int,
      t2.aj::text,
      t2.am::int,
      t0.aw::int,
      t0.ay::text,
      t0.cu::int,
      t0.bg::text,
      t1.cz::bool
    )
      selection (
        (
          t0.cv::int = 2466827::int
          and t0.ag::int in ROW(126412759::int, 126412766::int, 566228222::int)
          and t0.cq::int = 4::int
        )
      )
        left join on (t0.cv::int = t2.cv::int)
          left join on (t0.cu::int = t1.br::int)
            scan y -> t0
            scan i -> t1
          scan ab -> t2
subquery $1:
  projection (
    cq::int,
    aw::int,
    ay::text,
    cu::int,
    bg::text,
    cz::bool,
    aj::text,
    am::int,
    bv::decimal,
    c9::int
  )
    order by (bv::decimal)
      scan
        projection (
          cte0.cq::int -> cq,
          cte0.aw::int -> aw,
          cte0.ay::text -> ay,
          cte0.cu::int -> cu,
          cte0.bg::text -> bg,
          cte0.cz::bool -> cz,
          cte0.aj::text -> aj,
          cte0.am::int -> am,
          cte0.bv::decimal -> bv,
          row_number() over (order by (cte0.bv::decimal asc) ) -> c9
        )
          motion [policy: full, program: ReshardIfNeeded]
            projection (
              cte0.cq::int -> cq,
              cte0.aw::int -> aw,
              cte0.ay::text -> ay,
              cte0.cu::int -> cu,
              cte0.bg::text -> bg,
              cte0.cz::bool -> cz,
              cte0.aj::text -> aj,
              cte0.am::int -> am,
              cte0.bv::decimal -> bv
            )
              scan cte cte0($0)
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "cte0"."cq",
  "cte0"."aw",
  "cte0"."ay",
  "cte0"."cu",
  "cte0"."bg",
  "cte0"."cz",
  "cte0"."aj",
  "cte0"."am",
  "cte0"."bv"
FROM
  (
    SELECT
      "t0"."cq",
      "t0"."aw",
      "t0"."ay",
      "t0"."cu",
      "t0"."bg",
      "t1"."cz",
      "t2"."aj",
      "t2"."am",
      coalesce (
        sum (
          (
            "t0"."bx" + "t0"."bz" + "t0"."ca" + "t0"."cd" + "t0"."cc" + "t0"."cg"
          )::decimal
        )::decimal,
        0.0::decimal
      ) as "bv"
    FROM
      "y" as "t0"
      LEFT JOIN "i" as "t1" ON "t0"."cu" = "t1"."br"
      LEFT JOIN "ab" as "t2" ON "t0"."cv" = "t2"."cv"
    WHERE
      "t0"."cv" = 2466827::int
      and "t0"."ag" in (126412759::int, 126412766::int, 566228222::int)
      and "t0"."cq" = 4::int
    GROUP BY
      "t0"."cq",
      "t2"."aj",
      "t2"."am",
      "t0"."aw",
      "t0"."ay",
      "t0"."cu",
      "t0"."bg",
      "t1"."cz"
  ) as "cte0"
''
plan:
    [0] SEARCH TABLE y AS t0 USING COVERING INDEX ix_i (cv=? AND ag=?) (~8 rows)
    [0] EXECUTE LIST SUBQUERY 1
        [0] SEARCH TABLE i AS t1 USING PRIMARY KEY (br=?) (~1 row)
            [0] SEARCH TABLE ab AS t2 USING PRIMARY KEY (cv=?) (~1 row)
    [0] USE TEMP B-TREE FOR GROUP BY
''
buckets = [613]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "COL_0" as "cq",
      "COL_1" as "aw",
      "COL_2" as "ay",
      "COL_3" as "cu",
      "COL_4" as "bg",
      "COL_5" as "cz",
      "COL_6" as "aj",
      "COL_7" as "am",
      "COL_8" as "bv",
      "c9"
    FROM
      (
        SELECT
          "cte0".*,
          row_number () OVER (
            ORDER BY
              "cte0"."COL_8" ASC) as "c9"
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
              "COL_8"
            FROM
              "_tmp_8834505955264533567_1136"
          ) as "cte0"
      )
    ORDER BY
      "COL_8"
  ) as "cte1"
WHERE
  "cte1"."c9" > 0::int
LIMIT
  10
''
plan:
    [2] SCAN TABLE _tmp_8834505955264533567_1136 (~1048576 rows)
    [2] USE TEMP B-TREE FOR ORDER BY
    [1] SCAN SUBQUERY 2 (~1 row)
    [0] SCAN SUBQUERY 1 (~1 row)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [613]

-- TEST: corpus-explain-q85
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  10;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 10
  projection (
    cte4.c1::int -> c1,
    cte4.r::text -> r,
    cte4.c13::text -> c13,
    cte4.q::text -> q,
    cte4.ah::text -> ah,
    cte4.cp::decimal -> cp,
    cte4.c15::decimal -> c15,
    cte4.c16::int -> c16
  )
    selection (cte4.c16::int > 0::int)
      scan cte cte4($4)
subquery $0:
  projection (
    cte0.cv::int -> cv,
    cte0.ag::int -> c1,
    cte0.r::text -> r,
    cte0.n::int -> n,
    cte0.q::datetime -> q,
    cte0.ak::text -> ak,
    coalesce(cte0.t::decimal::decimal, 0.0::decimal)::decimal -> t
  )
    selection (
      (
        cte0.cv::int = 4797271::int
        and cte0.s::int = 2::int
        and cte0.n::int in ROW(1::int, 2::int)
      )
    )
      scan o -> cte0
subquery $1:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      cte0.cv::int -> cv,
      cte0.c1::int -> c1,
      cte1.ag::int -> c7,
      cte0.r::text -> r,
      cte0.n::int -> n,
      cte0.q::datetime -> q,
      cte0.ak::text -> ah,
      cte1.aw::int -> aw,
      cte0.t::decimal -> t
    )
      join on (
        ROW(cte0.c1::int, cte0.cv::int) = ROW(cte1.l::int, cte1.cv::int)
      )
        scan cte cte0($0)
        scan p -> cte1
subquery $2:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      cte1.cv::int -> cv,
      cte1.c1::int -> c1,
      cte1.r::text -> r,
      cte1.n::int -> n,
      cte1.q::datetime -> q,
      cte1.ah::text -> ah,
      cte1.aw::int -> aw,
      cte1.t::decimal -> t,
      cte2.ag::int -> c10
    )
      join on (
        ROW(cte1.c7::int, cte1.cv::int) = ROW(cte2.j::int, cte2.cv::int)
      )
        scan cte cte1($1)
        scan q -> cte2
subquery $3:
  motion [policy: full, program: ReshardIfNeeded]
    projection (
      cte2.cv::int -> cv,
      cte2.c1::int -> c1,
      cte2.r::text -> r,
      cte2.n::int -> n,
      cte2.q::datetime -> q,
      cte2.ah::text -> ah,
      cte2.t::decimal + case 
        when (
          cte2.aw::int in ROW(42::int, 45::int)
          and not cte3.ci::decimal is null
        ) then cte3.ci::decimal
        else 0::int
      end -> c11,
      cte3.y::decimal -> y
    )
      join on (
        ROW(cte2.c10::int, cte2.cv::int) = ROW(cte3.k::int, cte3.cv::int)
      )
        scan cte cte2($2)
        scan r -> cte3
subquery $4:
  projection (
    c1::int,
    r::text,
    c13::text,
    q::text,
    ah::text,
    cp::decimal,
    c15::decimal,
    c16::int
  )
    order by (q::text, c1::int)
      scan
        projection (
          cte3.c1::int -> c1,
          cte3.r::text -> r,
          case 
            when cte3.n::int = 1::int then 'installment'::text
            when cte3.n::int = 2::int then 'deferral'::text
            when cte3.n::int = 4::int then 'ink'::text
            when cte3.n::int = 5::int then 'restructurisation'::text
            else NULL::unknown
          end -> c13,
          to_char(cte3.q::datetime::datetime, '%Y-%m-%d'::text)::text -> q,
          cte3.ah::text -> ah,
          sum(cte3.c11::decimal::decimal)::decimal -> cp,
          sum(cte3.y::decimal::decimal)::decimal -> c15,
          row_number() over (order by (cte3.q::datetime asc, cte3.c1::int asc) ) -> c16
        )
          group by (
            cte3.c1::int,
            cte3.r::text,
            cte3.n::int,
            cte3.q::datetime,
            cte3.ah::text
          )
            scan cte cte3($3)
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
  "cte0"."cv",
  "cte0"."c1",
  "cte1"."ag" as "c7",
  "cte0"."r",
  "cte0"."n",
  "cte0"."q",
  "cte0"."ak" as "ah",
  "cte1"."aw",
  "cte0"."t"
FROM
  (
    SELECT
      "cte0"."cv",
      "cte0"."ag" as "c1",
      "cte0"."r",
      "cte0"."n",
      "cte0"."q",
      "cte0"."ak",
      coalesce ("cte0"."t"::decimal, 0.0::decimal) as "t"
    FROM
      "o" as "cte0"
    WHERE
      "cte0"."cv" = 4797271::int
      and "cte0"."s" = 2::int
      and "cte0"."n" in (1::int, 2::int)
  ) as "cte0"
  INNER JOIN "p" as "cte1" ON "cte0"."c1" = "cte1"."l"
  and "cte0"."cv" = "cte1"."cv"
''
plan:
    [0] SEARCH TABLE p AS cte1 USING COVERING INDEX ix_av (cv=?) (~10 rows)
        [0] SEARCH TABLE o AS cte0 USING PRIMARY KEY (ag=?) (~1 row)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 2. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "cte1"."COL_0" as "cv",
  "cte1"."COL_1" as "c1",
  "cte1"."COL_3" as "r",
  "cte1"."COL_4" as "n",
  "cte1"."COL_5" as "q",
  "cte1"."COL_6" as "ah",
  "cte1"."COL_7" as "aw",
  "cte1"."COL_8" as "t",
  "cte2"."ag" as "c10"
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
      "COL_8"
    FROM
      "_tmp_13962755054996946564_1136"
  ) as "cte1"
  INNER JOIN "q" as "cte2" ON "cte1"."COL_2" = "cte2"."j"
  and "cte1"."COL_0" = "cte2"."cv"
''
plan:
    [0] SCAN TABLE _tmp_13962755054996946564_1136 (~1048576 rows)
        [0] SEARCH TABLE q AS cte2 USING COVERING INDEX ix_ba (cv=? AND j=?) (~8 rows)
''
buckets <= [1-3000]
''
╭──────────────────────────╮
│ 3. Query (WHOLE STORAGE) │
╰──────────────────────────╯
''
SELECT
  "cte2"."COL_0" as "cv",
  "cte2"."COL_1" as "c1",
  "cte2"."COL_2" as "r",
  "cte2"."COL_3" as "n",
  "cte2"."COL_4" as "q",
  "cte2"."COL_5" as "ah",
  "cte2"."COL_7" + CASE
    WHEN "cte2"."COL_6" in (42::int, 45::int)
    and not "cte3"."ci" is null THEN "cte3"."ci"
    ELSE 0::int
  END as "c11",
  "cte3"."y"
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
      "COL_8"
    FROM
      "_tmp_3868301188177026271_2136"
  ) as "cte2"
  INNER JOIN "r" as "cte3" ON "cte2"."COL_8" = "cte3"."k"
  and "cte2"."COL_0" = "cte3"."cv"
''
plan:
    [0] SCAN TABLE _tmp_3868301188177026271_2136 (~1048576 rows)
        [0] SEARCH TABLE r AS cte3 USING COVERING INDEX ix_bd (cv=? AND k=?) (~8 rows)
''
buckets <= [1-3000]
''
╭───────────────────╮
│ 4. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "c1",
      "r",
      "c13",
      "q",
      "ah",
      "cp",
      "c15",
      "c16"
    FROM
      (
        SELECT
          "cte3"."COL_1" as "c1",
          "cte3"."COL_2" as "r",
          CASE
            WHEN "cte3"."COL_3" = 1::int THEN 'installment'::text
            WHEN "cte3"."COL_3" = 2::int THEN 'deferral'::text
            WHEN "cte3"."COL_3" = 4::int THEN 'ink'::text
            WHEN "cte3"."COL_3" = 5::int THEN 'restructurisation'::text
            ELSE NULL
          END as "c13",
          "to_char" ("cte3"."COL_4"::datetime, '%Y-%m-%d'::text) as "q",
          "cte3"."COL_5" as "ah",
          sum ("cte3"."COL_6"::decimal) as "cp",
          sum ("cte3"."COL_7"::decimal) as "c15",
          row_number () OVER (
            ORDER BY
              "cte3"."COL_4" ASC, "cte3"."COL_1" ASC) as "c16"
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
              "_tmp_15189758233677688509_3136"
          ) as "cte3"
        GROUP BY
          "cte3"."COL_1",
          "cte3"."COL_2",
          "cte3"."COL_3",
          "cte3"."COL_4",
          "cte3"."COL_5"
      )
    ORDER BY
      "q",
      "c1"
  ) as "cte4"
WHERE
  "cte4"."c16" > 0::int
LIMIT
  10
''
plan:
    [2] SCAN TABLE _tmp_15189758233677688509_3136 (~1048576 rows)
    [2] USE TEMP B-TREE FOR GROUP BY
    [2] USE TEMP B-TREE FOR ORDER BY
    [1] SCAN SUBQUERY 2 (~1 row)
    [0] SCAN SUBQUERY 1 (~1 row)
    [0] USE TEMP B-TREE FOR ORDER BY
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets <= [1-3000]

-- TEST: corpus-explain-q86
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
SELECT
  count(t0.ag) AS c0
FROM
  o t0
WHERE
  (
    t0.cv = 4797271
    AND t0.s = 2
    AND t0.n IN (1, 2)
  );
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (count(t0.ag::int::int)::int -> c0)
  selection (
    (
      t0.cv::int = 4797271::int
      and t0.s::int = 2::int
      and t0.n::int in ROW(1::int, 2::int)
    )
  )
    scan o -> t0
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  count ("t0"."ag"::int) as "c0"
FROM
  "o" as "t0"
WHERE
  "t0"."cv" = 4797271::int
  and "t0"."s" = 2::int
  and "t0"."n" in (1::int, 2::int)
''
plan:
    [0] SEARCH TABLE o AS t0 USING COVERING INDEX ix_au (cv=? AND s=?) (~8 rows)
''
buckets = [874]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [874]

-- TEST: corpus-explain-q87
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
SELECT
  t0.ax AS c0,
  t0.bc AS c1,
  t0.cy AS c2,
  t0.ct AS c3
FROM
  i t0
WHERE
  t0.ax = '18201061201010000510';
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t0.ax::text -> c0,
  t0.bc::text -> c1,
  t0.cy::text -> c2,
  t0.ct::bool -> c3
)
  selection (t0.ax::text = '18201061201010000510'::text)
    scan i -> t0
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "t0"."ax" as "c0",
  "t0"."bc" as "c1",
  "t0"."cy" as "c2",
  "t0"."ct" as "c3"
FROM
  "i" as "t0"
WHERE
  "t0"."ax" = '18201061201010000510'::text
''
plan:
    [0] SCAN TABLE i AS t0 (~262144 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = any

-- TEST: corpus-explain-q88
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
SELECT
  t0.ax AS c0,
  t0.bc AS c1,
  t0.cy AS c2,
  t0.ct AS c3
FROM
  i t0;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
projection (
  t0.ax::text -> c0,
  t0.bc::text -> c1,
  t0.cy::text -> c2,
  t0.ct::bool -> c3
)
  scan i -> t0
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭───────────────────╮
│ 1. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  "t0"."ax" as "c0",
  "t0"."bc" as "c1",
  "t0"."cy" as "c2",
  "t0"."ct" as "c3"
FROM
  "i" as "t0"
''
plan:
    [0] SCAN TABLE i AS t0 (~1048576 rows)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = any

-- TEST: corpus-explain-q89
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  2147483647;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 2147483647
  projection (t0.ay::text -> c0, t1.cx::text -> c1)
    selection (
      (
        t0.cv::int = 3023424::int
        and (
          lower(t0.ay::text::text)::text::text LIKE '%013%'::text ESCAPE '\'::text or lower(t1.cx::text::text)::text::text LIKE '%
          013%'::text ESCAPE '\'::text
        )
      )
    )
      join on (
        ROW(t0.cv::int, t0.ay::text) = ROW(t1.cv::int, t1.ay::text)
      )
        scan y -> t0
        scan ad -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT DISTINCT
  "t0"."ay" as "c0",
  "t1"."cx" as "c1"
FROM
  "y" as "t0"
  INNER JOIN "ad" as "t1" ON "t0"."cv" = "t1"."cv"
  and "t0"."ay" = "t1"."ay"
WHERE
  "t0"."cv" = 3023424::int
  and (
    lower ("t0"."ay"::text)::text LIKE '%013%'::text ESCAPE '\'::text
    or lower ("t1"."cx"::text)::text LIKE '%
013%'::text ESCAPE '\'::text
  )
LIMIT
  2147483647
''
plan:
    [0] SEARCH TABLE y AS t0 USING COVERING INDEX ix_bv (cv=?) (~10 rows)
        [0] SEARCH TABLE ad AS t1 USING PRIMARY KEY (cv=? AND ay=?) (~1 row)
    [0] USE TEMP B-TREE FOR DISTINCT
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q90
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
SELECT DISTINCT
  t0.bg AS c0,
  t1.bc AS c1
FROM
  y t0
  JOIN j t1 ON t0.bg = t1.bg
WHERE
  (t0.cv = 3023424)
LIMIT
  20;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 20
  projection (t0.bg::text -> c0, t1.bc::text -> c1)
    selection (t0.cv::int = 3023424::int)
      join on (t0.bg::text = t1.bg::text)
        scan y -> t0
        scan j -> t1
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT DISTINCT
  "t0"."bg" as "c0",
  "t1"."bc" as "c1"
FROM
  "y" as "t0"
  INNER JOIN "j" as "t1" ON "t0"."bg" = "t1"."bg"
WHERE
  "t0"."cv" = 3023424::int
LIMIT
  20
''
plan:
    [0] SEARCH TABLE y AS t0 USING COVERING INDEX ix_bv (cv=?) (~10 rows)
        [0] SEARCH TABLE j AS t1 USING PRIMARY KEY (bg=?) (~1 row)
    [0] USE TEMP B-TREE FOR DISTINCT
''
buckets = [1222]
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [1222]

-- TEST: corpus-explain-q91
-- SQL:
EXPLAIN (LOGICAL, RAW, BUCKETS, FMT)
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
  20;
-- EXPECTED:
──────────────────────────────────────────────────────────────────────
 # Logical plan                                                       
──────────────────────────────────────────────────────────────────────
''
limit 20
  projection (
    cte0.ba::text -> ba,
    cte0.cu::int -> cu,
    cte0.cq::int -> cq,
    cte0.ay::text -> ay,
    cte0.bp::text -> bp,
    cte0.bh::text -> bh,
    cte0.bq::datetime -> bq,
    cte0.bu::datetime -> bu,
    cte0.t::decimal -> t,
    cte0.az::decimal -> az,
    cte0.c10::int -> c10
  )
    selection (cte0.c10::int > 0::int)
      scan cte cte0($0)
subquery $0:
  projection (
    ba::text,
    cu::int,
    cq::int,
    bo::text -> ay,
    bp::text,
    bh::text,
    bq::datetime,
    bu::datetime,
    t::decimal,
    az::decimal,
    row_number() over () -> c10
  )
    motion [policy: full, program: ReshardIfNeeded]
      projection (
        t0.bq::datetime -> bq,
        t0.t::decimal -> t,
        t0.bo::text -> bo,
        t0.bh::text -> bh,
        t0.cu::int -> cu,
        t0.bu::datetime -> bu,
        t0.az::decimal -> az,
        t0.ba::text -> ba,
        t0.bp::text -> bp,
        t1.cq::int -> cq
      )
        selection (
          (
            t0.cv::int = 2337497::int
            and t1.cq::int = 4::int
          )
        )
          left join on (t0.cu::int = t2.br::int)
            left join on (t0.cu::int = t1.cu::int)
              scan aa -> t0
              scan h -> t1
            scan i -> t2
''
──────────────────────────────────────────────────────────────────────
 # Raw plan                                                           
──────────────────────────────────────────────────────────────────────
''
╭────────────────────────────────────────╮
│ 1. Query (CONST-FILTERED STORAGE, 1/1) │
╰────────────────────────────────────────╯
''
SELECT
  "t0"."bq",
  "t0"."t",
  "t0"."bo",
  "t0"."bh",
  "t0"."cu",
  "t0"."bu",
  "t0"."az",
  "t0"."ba",
  "t0"."bp",
  "t1"."cq"
FROM
  "aa" as "t0"
  LEFT JOIN "h" as "t1" ON "t0"."cu" = "t1"."cu"
  LEFT JOIN "i" as "t2" ON "t0"."cu" = "t2"."br"
WHERE
  "t0"."cv" = 2337497::int
  and "t1"."cq" = 4::int
''
plan:
    [0] SCAN TABLE aa AS t0 (~262144 rows)
        [0] SEARCH TABLE h AS t1 USING PRIMARY KEY (cu=?) (~1 row)
            [0] SEARCH TABLE i AS t2 USING PRIMARY KEY (br=?) (~1 row)
''
buckets = [926]
''
╭───────────────────╮
│ 2. Query (ROUTER) │
╰───────────────────╯
''
SELECT
  *
FROM
  (
    SELECT
      "COL_7" as "ba",
      "COL_4" as "cu",
      "COL_9" as "cq",
      "COL_2" as "ay",
      "COL_8" as "bp",
      "COL_3" as "bh",
      "COL_0" as "bq",
      "COL_5" as "bu",
      "COL_1" as "t",
      "COL_6" as "az",
      row_number () OVER () as "c10"
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
          "COL_9"
        FROM
          "_tmp_9391536498192629513_1136"
      )
  ) as "cte0"
WHERE
  "cte0"."c10" > 0::int
LIMIT
  20
''
plan:
    [2] SCAN TABLE _tmp_9391536498192629513_1136 (~1048576 rows)
    [1] SCAN SUBQUERY 2 (~1 row)
    [0] SCAN SUBQUERY 1 AS cte0 (~1 row)
''
buckets = any
''
──────────────────────────────────────────────────────────────────────
 # Buckets                                                            
──────────────────────────────────────────────────────────────────────
''
buckets = [926]
