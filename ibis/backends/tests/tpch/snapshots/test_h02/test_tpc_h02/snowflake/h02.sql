WITH t4 AS (
  SELECT
    t9."P_PARTKEY" AS "p_partkey",
    t9."P_NAME" AS "p_name",
    t9."P_MFGR" AS "p_mfgr",
    t9."P_BRAND" AS "p_brand",
    t9."P_TYPE" AS "p_type",
    t9."P_SIZE" AS "p_size",
    t9."P_CONTAINER" AS "p_container",
    t9."P_RETAILPRICE" AS "p_retailprice",
    t9."P_COMMENT" AS "p_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."PART" AS t9
), t3 AS (
  SELECT
    t9."PS_PARTKEY" AS "ps_partkey",
    t9."PS_SUPPKEY" AS "ps_suppkey",
    t9."PS_AVAILQTY" AS "ps_availqty",
    t9."PS_SUPPLYCOST" AS "ps_supplycost",
    t9."PS_COMMENT" AS "ps_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."PARTSUPP" AS t9
), t2 AS (
  SELECT
    t9."S_SUPPKEY" AS "s_suppkey",
    t9."S_NAME" AS "s_name",
    t9."S_ADDRESS" AS "s_address",
    t9."S_NATIONKEY" AS "s_nationkey",
    t9."S_PHONE" AS "s_phone",
    t9."S_ACCTBAL" AS "s_acctbal",
    t9."S_COMMENT" AS "s_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."SUPPLIER" AS t9
), t1 AS (
  SELECT
    t9."N_NATIONKEY" AS "n_nationkey",
    t9."N_NAME" AS "n_name",
    t9."N_REGIONKEY" AS "n_regionkey",
    t9."N_COMMENT" AS "n_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."NATION" AS t9
), t0 AS (
  SELECT
    t9."R_REGIONKEY" AS "r_regionkey",
    t9."R_NAME" AS "r_name",
    t9."R_COMMENT" AS "r_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."REGION" AS t9
), t6 AS (
  SELECT
    t4."p_partkey" AS "p_partkey",
    t4."p_name" AS "p_name",
    t4."p_mfgr" AS "p_mfgr",
    t4."p_brand" AS "p_brand",
    t4."p_type" AS "p_type",
    t4."p_size" AS "p_size",
    t4."p_container" AS "p_container",
    t4."p_retailprice" AS "p_retailprice",
    t4."p_comment" AS "p_comment",
    t3."ps_partkey" AS "ps_partkey",
    t3."ps_suppkey" AS "ps_suppkey",
    t3."ps_availqty" AS "ps_availqty",
    t3."ps_supplycost" AS "ps_supplycost",
    t3."ps_comment" AS "ps_comment",
    t2."s_suppkey" AS "s_suppkey",
    t2."s_name" AS "s_name",
    t2."s_address" AS "s_address",
    t2."s_nationkey" AS "s_nationkey",
    t2."s_phone" AS "s_phone",
    t2."s_acctbal" AS "s_acctbal",
    t2."s_comment" AS "s_comment",
    t1."n_nationkey" AS "n_nationkey",
    t1."n_name" AS "n_name",
    t1."n_regionkey" AS "n_regionkey",
    t1."n_comment" AS "n_comment",
    t0."r_regionkey" AS "r_regionkey",
    t0."r_name" AS "r_name",
    t0."r_comment" AS "r_comment"
  FROM t4
  JOIN t3
    ON t4."p_partkey" = t3."ps_partkey"
  JOIN t2
    ON t2."s_suppkey" = t3."ps_suppkey"
  JOIN t1
    ON t2."s_nationkey" = t1."n_nationkey"
  JOIN t0
    ON t1."n_regionkey" = t0."r_regionkey"
), t5 AS (
  SELECT
    t3."ps_partkey" AS "ps_partkey",
    t3."ps_suppkey" AS "ps_suppkey",
    t3."ps_availqty" AS "ps_availqty",
    t3."ps_supplycost" AS "ps_supplycost",
    t3."ps_comment" AS "ps_comment",
    t2."s_suppkey" AS "s_suppkey",
    t2."s_name" AS "s_name",
    t2."s_address" AS "s_address",
    t2."s_nationkey" AS "s_nationkey",
    t2."s_phone" AS "s_phone",
    t2."s_acctbal" AS "s_acctbal",
    t2."s_comment" AS "s_comment",
    t1."n_nationkey" AS "n_nationkey",
    t1."n_name" AS "n_name",
    t1."n_regionkey" AS "n_regionkey",
    t1."n_comment" AS "n_comment",
    t0."r_regionkey" AS "r_regionkey",
    t0."r_name" AS "r_name",
    t0."r_comment" AS "r_comment"
  FROM t3
  JOIN t2
    ON t2."s_suppkey" = t3."ps_suppkey"
  JOIN t1
    ON t2."s_nationkey" = t1."n_nationkey"
  JOIN t0
    ON t1."n_regionkey" = t0."r_regionkey"
), t7 AS (
  SELECT
    t6."p_partkey" AS "p_partkey",
    t6."p_name" AS "p_name",
    t6."p_mfgr" AS "p_mfgr",
    t6."p_brand" AS "p_brand",
    t6."p_type" AS "p_type",
    t6."p_size" AS "p_size",
    t6."p_container" AS "p_container",
    t6."p_retailprice" AS "p_retailprice",
    t6."p_comment" AS "p_comment",
    t6."ps_partkey" AS "ps_partkey",
    t6."ps_suppkey" AS "ps_suppkey",
    t6."ps_availqty" AS "ps_availqty",
    t6."ps_supplycost" AS "ps_supplycost",
    t6."ps_comment" AS "ps_comment",
    t6."s_suppkey" AS "s_suppkey",
    t6."s_name" AS "s_name",
    t6."s_address" AS "s_address",
    t6."s_nationkey" AS "s_nationkey",
    t6."s_phone" AS "s_phone",
    t6."s_acctbal" AS "s_acctbal",
    t6."s_comment" AS "s_comment",
    t6."n_nationkey" AS "n_nationkey",
    t6."n_name" AS "n_name",
    t6."n_regionkey" AS "n_regionkey",
    t6."n_comment" AS "n_comment",
    t6."r_regionkey" AS "r_regionkey",
    t6."r_name" AS "r_name",
    t6."r_comment" AS "r_comment"
  FROM t6
  WHERE
    t6."p_size" = 15
    AND t6."p_type" LIKE '%BRASS'
    AND t6."r_name" = 'EUROPE'
    AND t6."ps_supplycost" = (
      SELECT
        MIN(t5."ps_supplycost") AS "Min(ps_supplycost)"
      FROM t5
      WHERE
        t5."r_name" = 'EUROPE' AND t6."p_partkey" = t5."ps_partkey"
    )
)
SELECT
  t8."s_acctbal",
  t8."s_name",
  t8."n_name",
  t8."p_partkey",
  t8."p_mfgr",
  t8."s_address",
  t8."s_phone",
  t8."s_comment"
FROM (
  SELECT
    t7."s_acctbal" AS "s_acctbal",
    t7."s_name" AS "s_name",
    t7."n_name" AS "n_name",
    t7."p_partkey" AS "p_partkey",
    t7."p_mfgr" AS "p_mfgr",
    t7."s_address" AS "s_address",
    t7."s_phone" AS "s_phone",
    t7."s_comment" AS "s_comment"
  FROM t7
) AS t8
ORDER BY
  t8."s_acctbal" DESC,
  t8."n_name" ASC,
  t8."s_name" ASC,
  t8."p_partkey" ASC
LIMIT 100