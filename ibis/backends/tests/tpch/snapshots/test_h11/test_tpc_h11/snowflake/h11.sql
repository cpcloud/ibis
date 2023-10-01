WITH t2 AS (
  SELECT
    t6."PS_PARTKEY" AS "ps_partkey",
    t6."PS_SUPPKEY" AS "ps_suppkey",
    t6."PS_AVAILQTY" AS "ps_availqty",
    t6."PS_SUPPLYCOST" AS "ps_supplycost",
    t6."PS_COMMENT" AS "ps_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."PARTSUPP" AS t6
), t0 AS (
  SELECT
    t6."S_SUPPKEY" AS "s_suppkey",
    t6."S_NAME" AS "s_name",
    t6."S_ADDRESS" AS "s_address",
    t6."S_NATIONKEY" AS "s_nationkey",
    t6."S_PHONE" AS "s_phone",
    t6."S_ACCTBAL" AS "s_acctbal",
    t6."S_COMMENT" AS "s_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."SUPPLIER" AS t6
), t1 AS (
  SELECT
    t6."N_NATIONKEY" AS "n_nationkey",
    t6."N_NAME" AS "n_name",
    t6."N_REGIONKEY" AS "n_regionkey",
    t6."N_COMMENT" AS "n_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."NATION" AS t6
), t3 AS (
  SELECT
    t2."ps_partkey" AS "ps_partkey",
    t2."ps_suppkey" AS "ps_suppkey",
    t2."ps_availqty" AS "ps_availqty",
    t2."ps_supplycost" AS "ps_supplycost",
    t2."ps_comment" AS "ps_comment",
    t0."s_suppkey" AS "s_suppkey",
    t0."s_name" AS "s_name",
    t0."s_address" AS "s_address",
    t0."s_nationkey" AS "s_nationkey",
    t0."s_phone" AS "s_phone",
    t0."s_acctbal" AS "s_acctbal",
    t0."s_comment" AS "s_comment",
    t1."n_nationkey" AS "n_nationkey",
    t1."n_name" AS "n_name",
    t1."n_regionkey" AS "n_regionkey",
    t1."n_comment" AS "n_comment"
  FROM t2
  JOIN t0
    ON t2."ps_suppkey" = t0."s_suppkey"
  JOIN t1
    ON t1."n_nationkey" = t0."s_nationkey"
), t4 AS (
  SELECT
    t3."ps_partkey" AS "ps_partkey",
    SUM(t3."ps_supplycost" * t3."ps_availqty") AS "value"
  FROM t3
  WHERE
    t3."n_name" = 'GERMANY'
  GROUP BY
    1
)
SELECT
  t5."ps_partkey",
  t5."value"
FROM (
  SELECT
    t4."ps_partkey" AS "ps_partkey",
    t4."value" AS "value"
  FROM t4
  WHERE
    t4."value" > (
      SELECT
        SUM(t3."ps_supplycost" * t3."ps_availqty") AS "total"
      FROM t3
      WHERE
        t3."n_name" = 'GERMANY'
    ) * 0.0001
) AS t5
ORDER BY
  t5."value" DESC