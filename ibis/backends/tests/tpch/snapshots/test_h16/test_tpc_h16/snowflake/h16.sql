WITH t1 AS (
  SELECT
    t5."PS_PARTKEY" AS "ps_partkey",
    t5."PS_SUPPKEY" AS "ps_suppkey",
    t5."PS_AVAILQTY" AS "ps_availqty",
    t5."PS_SUPPLYCOST" AS "ps_supplycost",
    t5."PS_COMMENT" AS "ps_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."PARTSUPP" AS t5
), t2 AS (
  SELECT
    t5."P_PARTKEY" AS "p_partkey",
    t5."P_NAME" AS "p_name",
    t5."P_MFGR" AS "p_mfgr",
    t5."P_BRAND" AS "p_brand",
    t5."P_TYPE" AS "p_type",
    t5."P_SIZE" AS "p_size",
    t5."P_CONTAINER" AS "p_container",
    t5."P_RETAILPRICE" AS "p_retailprice",
    t5."P_COMMENT" AS "p_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."PART" AS t5
), t3 AS (
  SELECT
    t1."ps_partkey" AS "ps_partkey",
    t1."ps_suppkey" AS "ps_suppkey",
    t1."ps_availqty" AS "ps_availqty",
    t1."ps_supplycost" AS "ps_supplycost",
    t1."ps_comment" AS "ps_comment",
    t2."p_partkey" AS "p_partkey",
    t2."p_name" AS "p_name",
    t2."p_mfgr" AS "p_mfgr",
    t2."p_brand" AS "p_brand",
    t2."p_type" AS "p_type",
    t2."p_size" AS "p_size",
    t2."p_container" AS "p_container",
    t2."p_retailprice" AS "p_retailprice",
    t2."p_comment" AS "p_comment"
  FROM t1
  JOIN t2
    ON t2."p_partkey" = t1."ps_partkey"
), t0 AS (
  SELECT
    t5."S_SUPPKEY" AS "s_suppkey",
    t5."S_NAME" AS "s_name",
    t5."S_ADDRESS" AS "s_address",
    t5."S_NATIONKEY" AS "s_nationkey",
    t5."S_PHONE" AS "s_phone",
    t5."S_ACCTBAL" AS "s_acctbal",
    t5."S_COMMENT" AS "s_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."SUPPLIER" AS t5
)
SELECT
  t4."p_brand",
  t4."p_type",
  t4."p_size",
  t4."supplier_cnt"
FROM (
  SELECT
    t3."p_brand" AS "p_brand",
    t3."p_type" AS "p_type",
    t3."p_size" AS "p_size",
    COUNT(DISTINCT t3."ps_suppkey") AS "supplier_cnt"
  FROM t3
  WHERE
    t3."p_brand" <> 'Brand#45'
    AND NOT t3."p_type" LIKE 'MEDIUM POLISHED%'
    AND t3."p_size" IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND (
      NOT t3."ps_suppkey" IN (
        SELECT
          t5."s_suppkey"
        FROM (
          SELECT
            t0."s_suppkey" AS "s_suppkey",
            t0."s_name" AS "s_name",
            t0."s_address" AS "s_address",
            t0."s_nationkey" AS "s_nationkey",
            t0."s_phone" AS "s_phone",
            t0."s_acctbal" AS "s_acctbal",
            t0."s_comment" AS "s_comment"
          FROM t0
          WHERE
            t0."s_comment" LIKE '%Customer%Complaints%'
        ) AS t5
      )
    )
  GROUP BY
    1,
    2,
    3
) AS t4
ORDER BY
  t4."supplier_cnt" DESC,
  t4."p_brand" ASC,
  t4."p_type" ASC,
  t4."p_size" ASC