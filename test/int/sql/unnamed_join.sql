-- TEST: unnamed_join
-- SQL:
DROP TABLE IF EXISTS cd_corporate_account;
DROP TABLE IF EXISTS a_cd_corporate_client;

CREATE TABLE cd_corporate_account
(
    account_sk          varchar(45),
    client_id           varchar(13),
    primary key (account_sk)
) using memtx
DISTRIBUTED BY (account_sk);
CREATE TABLE a_cd_corporate_client
(
    client_id           varchar(13),
    src_system          text,
    primary key (client_id, src_system)
) using memtx
DISTRIBUTED BY (client_id);

INSERT INTO cd_corporate_account (account_sk, client_id)
VALUES ('account_id_1', 'client_id_1'),
       ('account_id_2', 'client_id_2');

INSERT INTO a_cd_corporate_client (client_id, src_system)
VALUES ('client_id_1', 'src_1');

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

create table t1(a int primary key, b int, c int);
create table t2 (a int primary key, b int, c int);

-- TEST: issue-1344
-- SQL:
WITH d AS
         (SELECT 0,
                 0,
                 e.c f,
                 1 g,
                 h.b i
          FROM t2 h
                   JOIN t1 j ON h.a = j.b
                   JOIN t2 e ON e.b > j.c
          UNION SELECT k.c,
                       sum(l.c),
                       min(h.b),
                       k.b,
                       count(h.c)
          FROM t1 h
                   JOIN t2 j ON j.c = h.c
                   JOIN t1 k ON k.b = h.c
                   JOIN t2 l ON h.a > l.a
          GROUP BY k.b,
                   k.c),
     m AS
         (SELECT 0,
                 l.b,
                 3
          FROM t1 h
                   JOIN t2 j ON h.c = j.a
                   JOIN d ON 3 = j.a
                   JOIN t2 l ON l.a < d.g),
     n AS
         (SELECT 1
          FROM d
                   LEFT JOIN t1 j ON j.b = 2
                   JOIN t2 k ON j.a = k.b
                   JOIN m ON k.c > f)
SELECT 1
FROM n;
-- EXPECTED:


-- TEST: issue-2489
-- SQL:
with r9 as (select 'clientMdmId' as mdm_id_first, client_id
            from cd_corporate_account),
     r9_mdm as (select *
                from r9
                         left join a_cd_corporate_client m on r9.client_id = m.client_id
                         left join r9 m_f on r9.mdm_id_first = m_f.mdm_id_first)
select *
from r9_mdm;
-- EXPECTED:
'clientMdmId', 'client_id_1', 'client_id_1', 'src_1', 'clientMdmId', 'client_id_1',
'clientMdmId', 'client_id_1', 'client_id_1', 'src_1', 'clientMdmId', 'client_id_2',
'clientMdmId', 'client_id_2', NULL, NULL, 'clientMdmId', 'client_id_1',
'clientMdmId', 'client_id_2', NULL, NULL, 'clientMdmId', 'client_id_2',

-- TEST: issue-2489-explain
-- SQL:
EXPLAIN
with r9 as (select 'clientMdmId' as mdm_id_first, client_id
            from cd_corporate_account),
     r9_mdm as (select *
                from r9
                         left join a_cd_corporate_client m on r9.client_id = m.client_id
                         left join r9 m_f on r9.mdm_id_first = m_f.mdm_id_first)
select *
from r9_mdm;
-- EXPECTED:
projection ("r9_mdm"."mdm_id_first"::string -> "mdm_id_first", "r9_mdm"."client_id"::string -> "client_id", "r9_mdm"."client_id"::string -> "client_id", "r9_mdm"."src_system"::string -> "src_system", "r9_mdm"."mdm_id_first"::string -> "mdm_id_first", "r9_mdm"."client_id"::string -> "client_id")
    scan cte r9_mdm($1)
subquery $0:
motion [policy: full]
                                    projection ('clientMdmId'::string -> "mdm_id_first", "cd_corporate_account"."client_id"::string -> "client_id")
                                        scan "cd_corporate_account"
subquery $1:
projection ("unnamed_join"."mdm_id_first"::string -> "mdm_id_first", "unnamed_join"."client_id"::string -> "client_id", "unnamed_join"."client_id"::string -> "client_id", "unnamed_join"."src_system"::string -> "src_system", "m_f"."mdm_id_first"::string -> "mdm_id_first", "m_f"."client_id"::string -> "client_id")
            left join on "unnamed_join"."mdm_id_first"::string = "m_f"."mdm_id_first"::string
                motion [policy: full]
                    projection ("r9"."mdm_id_first"::string -> "mdm_id_first", "r9"."client_id"::string -> "client_id", "m"."client_id"::string -> "client_id", "m"."src_system"::string -> "src_system", "m"."bucket_id"::int -> "bucket_id")
                        join on "r9"."client_id"::string = "m"."client_id"::string
                            scan cte r9($0)
                            scan "a_cd_corporate_client" -> "m"
                scan cte m_f($0)
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: unnamed_join_cte_name_conflict
-- SQL:
EXPLAIN
with unnamed_join as (select 'clientMdmId' as mdm_id_first, client_id
            from cd_corporate_account),
     r9_mdm as (select *
                from unnamed_join
                         left join a_cd_corporate_client m on unnamed_join.client_id = m.client_id
                         left join unnamed_join m_f on unnamed_join.mdm_id_first = m_f.mdm_id_first)
select *
from r9_mdm;
-- EXPECTED:
projection ("r9_mdm"."mdm_id_first"::string -> "mdm_id_first", "r9_mdm"."client_id"::string -> "client_id", "r9_mdm"."client_id"::string -> "client_id", "r9_mdm"."src_system"::string -> "src_system", "r9_mdm"."mdm_id_first"::string -> "mdm_id_first", "r9_mdm"."client_id"::string -> "client_id")
    scan cte r9_mdm($1)
subquery $0:
motion [policy: full]
                                    projection ('clientMdmId'::string -> "mdm_id_first", "cd_corporate_account"."client_id"::string -> "client_id")
                                        scan "cd_corporate_account"
subquery $1:
projection ("unnamed_join_1"."mdm_id_first"::string -> "mdm_id_first", "unnamed_join_1"."client_id"::string -> "client_id", "unnamed_join_1"."client_id"::string -> "client_id", "unnamed_join_1"."src_system"::string -> "src_system", "m_f"."mdm_id_first"::string -> "mdm_id_first", "m_f"."client_id"::string -> "client_id")
            left join on "unnamed_join_1"."mdm_id_first"::string = "m_f"."mdm_id_first"::string
                motion [policy: full]
                    projection ("unnamed_join"."mdm_id_first"::string -> "mdm_id_first", "unnamed_join"."client_id"::string -> "client_id", "m"."client_id"::string -> "client_id", "m"."src_system"::string -> "src_system", "m"."bucket_id"::int -> "bucket_id")
                        join on "unnamed_join"."client_id"::string = "m"."client_id"::string
                            scan cte unnamed_join($0)
                            scan "a_cd_corporate_client" -> "m"
                scan cte m_f($0)
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: unnamed_join_alias_name_conflict
-- SQL:
EXPLAIN
with r9 as (select 'clientMdmId' as mdm_id_first, client_id
            from cd_corporate_account),
     r9_mdm as (select *
                from r9 as unnamed_join
                         left join a_cd_corporate_client unnamed_join_2 on unnamed_join.client_id = unnamed_join_2.client_id
                         left join r9 unnamed_join_1 on unnamed_join.mdm_id_first = unnamed_join_1.mdm_id_first)
select *
from r9_mdm;
-- EXPECTED:
projection ("r9_mdm"."mdm_id_first"::string -> "mdm_id_first", "r9_mdm"."client_id"::string -> "client_id", "r9_mdm"."client_id"::string -> "client_id", "r9_mdm"."src_system"::string -> "src_system", "r9_mdm"."mdm_id_first"::string -> "mdm_id_first", "r9_mdm"."client_id"::string -> "client_id")
    scan cte r9_mdm($1)
subquery $0:
motion [policy: full]
                                    projection ('clientMdmId'::string -> "mdm_id_first", "cd_corporate_account"."client_id"::string -> "client_id")
                                        scan "cd_corporate_account"
subquery $1:
projection ("unnamed_join_3"."mdm_id_first"::string -> "mdm_id_first", "unnamed_join_3"."client_id"::string -> "client_id", "unnamed_join_3"."client_id"::string -> "client_id", "unnamed_join_3"."src_system"::string -> "src_system", "unnamed_join_1"."mdm_id_first"::string -> "mdm_id_first", "unnamed_join_1"."client_id"::string -> "client_id")
            left join on "unnamed_join_3"."mdm_id_first"::string = "unnamed_join_1"."mdm_id_first"::string
                motion [policy: full]
                    projection ("unnamed_join"."mdm_id_first"::string -> "mdm_id_first", "unnamed_join"."client_id"::string -> "client_id", "unnamed_join_2"."client_id"::string -> "client_id", "unnamed_join_2"."src_system"::string -> "src_system", "unnamed_join_2"."bucket_id"::int -> "bucket_id")
                        join on "unnamed_join"."client_id"::string = "unnamed_join_2"."client_id"::string
                            scan cte unnamed_join($0)
                            scan "a_cd_corporate_client" -> "unnamed_join_2"
                scan cte unnamed_join_1($0)
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]

-- TEST: unnamed_join_table_name_conflict_prepare
-- SQL:
DROP TABLE IF EXISTS unnamed_join;
CREATE TABLE unnamed_join
(
    client_id           varchar(13),
    src_system          text,
    primary key (client_id, src_system)
) using memtx
DISTRIBUTED BY (client_id);

-- TEST: unnamed_join_table_name_conflict
-- SQL:
EXPLAIN
with r9 as (select 'clientMdmId' as mdm_id_first, client_id
            from cd_corporate_account),
     r9_mdm as (select *
                from r9
                         left join unnamed_join m on r9.client_id = m.client_id
                         left join r9 m_f on r9.mdm_id_first = m_f.mdm_id_first)
select *
from r9_mdm;
-- EXPECTED:
projection ("r9_mdm"."mdm_id_first"::string -> "mdm_id_first", "r9_mdm"."client_id"::string -> "client_id", "r9_mdm"."client_id"::string -> "client_id", "r9_mdm"."src_system"::string -> "src_system", "r9_mdm"."mdm_id_first"::string -> "mdm_id_first", "r9_mdm"."client_id"::string -> "client_id")
    scan cte r9_mdm($1)
subquery $0:
motion [policy: full]
                                    projection ('clientMdmId'::string -> "mdm_id_first", "cd_corporate_account"."client_id"::string -> "client_id")
                                        scan "cd_corporate_account"
subquery $1:
projection ("unnamed_join_1"."mdm_id_first"::string -> "mdm_id_first", "unnamed_join_1"."client_id"::string -> "client_id", "unnamed_join_1"."client_id"::string -> "client_id", "unnamed_join_1"."src_system"::string -> "src_system", "m_f"."mdm_id_first"::string -> "mdm_id_first", "m_f"."client_id"::string -> "client_id")
            left join on "unnamed_join_1"."mdm_id_first"::string = "m_f"."mdm_id_first"::string
                motion [policy: full]
                    projection ("r9"."mdm_id_first"::string -> "mdm_id_first", "r9"."client_id"::string -> "client_id", "m"."client_id"::string -> "client_id", "m"."src_system"::string -> "src_system", "m"."bucket_id"::int -> "bucket_id")
                        join on "r9"."client_id"::string = "m"."client_id"::string
                            scan cte r9($0)
                            scan "unnamed_join" -> "m"
                scan cte m_f($0)
execution options:
    sql_vdbe_opcode_max = 45000
    sql_motion_row_max = 5000
buckets = [1-3000]