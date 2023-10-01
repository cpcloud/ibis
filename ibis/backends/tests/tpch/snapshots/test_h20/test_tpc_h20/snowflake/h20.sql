WITH t4 AS (
  SELECT
    t8."S_SUPPKEY" AS "s_suppkey",
    t8."S_NAME" AS "s_name",
    t8."S_ADDRESS" AS "s_address",
    t8."S_NATIONKEY" AS "s_nationkey",
    t8."S_PHONE" AS "s_phone",
    t8."S_ACCTBAL" AS "s_acctbal",
    t8."S_COMMENT" AS "s_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."SUPPLIER" AS t8
), t3 AS (
  SELECT
    t8."N_NATIONKEY" AS "n_nationkey",
    t8."N_NAME" AS "n_name",
    t8."N_REGIONKEY" AS "n_regionkey",
    t8."N_COMMENT" AS "n_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."NATION" AS t8
), t5 AS (
  SELECT
    t4."s_suppkey" AS "s_suppkey",
    t4."s_name" AS "s_name",
    t4."s_address" AS "s_address",
    t4."s_nationkey" AS "s_nationkey",
    t4."s_phone" AS "s_phone",
    t4."s_acctbal" AS "s_acctbal",
    t4."s_comment" AS "s_comment",
    t3."n_nationkey" AS "n_nationkey",
    t3."n_name" AS "n_name",
    t3."n_regionkey" AS "n_regionkey",
    t3."n_comment" AS "n_comment"
  FROM t4
  JOIN t3
    ON t4."s_nationkey" = t3."n_nationkey"
), t1 AS (
  SELECT
    t8."PS_PARTKEY" AS "ps_partkey",
    t8."PS_SUPPKEY" AS "ps_suppkey",
    t8."PS_AVAILQTY" AS "ps_availqty",
    t8."PS_SUPPLYCOST" AS "ps_supplycost",
    t8."PS_COMMENT" AS "ps_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."PARTSUPP" AS t8
), t2 AS (
  SELECT
    t8."P_PARTKEY" AS "p_partkey",
    t8."P_NAME" AS "p_name",
    t8."P_MFGR" AS "p_mfgr",
    t8."P_BRAND" AS "p_brand",
    t8."P_TYPE" AS "p_type",
    t8."P_SIZE" AS "p_size",
    t8."P_CONTAINER" AS "p_container",
    t8."P_RETAILPRICE" AS "p_retailprice",
    t8."P_COMMENT" AS "p_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."PART" AS t8
), t0 AS (
  SELECT
    t8."L_ORDERKEY" AS "l_orderkey",
    t8."L_PARTKEY" AS "l_partkey",
    t8."L_SUPPKEY" AS "l_suppkey",
    t8."L_LINENUMBER" AS "l_linenumber",
    t8."L_QUANTITY" AS "l_quantity",
    t8."L_EXTENDEDPRICE" AS "l_extendedprice",
    t8."L_DISCOUNT" AS "l_discount",
    t8."L_TAX" AS "l_tax",
    t8."L_RETURNFLAG" AS "l_returnflag",
    t8."L_LINESTATUS" AS "l_linestatus",
    t8."L_SHIPDATE" AS "l_shipdate",
    t8."L_COMMITDATE" AS "l_commitdate",
    t8."L_RECEIPTDATE" AS "l_receiptdate",
    t8."L_SHIPINSTRUCT" AS "l_shipinstruct",
    t8."L_SHIPMODE" AS "l_shipmode",
    t8."L_COMMENT" AS "l_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM" AS t8
), t6 AS (
  SELECT
    t5."s_suppkey" AS "s_suppkey",
    t5."s_name" AS "s_name",
    t5."s_address" AS "s_address",
    t5."s_nationkey" AS "s_nationkey",
    t5."s_phone" AS "s_phone",
    t5."s_acctbal" AS "s_acctbal",
    t5."s_comment" AS "s_comment",
    t5."n_nationkey" AS "n_nationkey",
    t5."n_name" AS "n_name",
    t5."n_regionkey" AS "n_regionkey",
    t5."n_comment" AS "n_comment"
  FROM t5
  WHERE
    t5."n_name" = 'CANADA'
    AND t5."s_suppkey" IN (
      SELECT
        t8."ps_suppkey"
      FROM (
        SELECT
          t1."ps_partkey" AS "ps_partkey",
          t1."ps_suppkey" AS "ps_suppkey",
          t1."ps_availqty" AS "ps_availqty",
          t1."ps_supplycost" AS "ps_supplycost",
          t1."ps_comment" AS "ps_comment"
        FROM t1
        WHERE
          t1."ps_partkey" IN (
            SELECT
              t9."p_partkey"
            FROM (
              SELECT
                t2."p_partkey" AS "p_partkey",
                t2."p_name" AS "p_name",
                t2."p_mfgr" AS "p_mfgr",
                t2."p_brand" AS "p_brand",
                t2."p_type" AS "p_type",
                t2."p_size" AS "p_size",
                t2."p_container" AS "p_container",
                t2."p_retailprice" AS "p_retailprice",
                t2."p_comment" AS "p_comment"
              FROM t2
              WHERE
                t2."p_name" LIKE 'forest%'
            ) AS t9
          )
          AND t1."ps_availqty" > (
            SELECT
              SUM(t0."l_quantity") AS "Sum(l_quantity)"
            FROM t0
            WHERE
              t0."l_partkey" = t1."ps_partkey"
              AND t0."l_suppkey" = t1."ps_suppkey"
              AND t0."l_shipdate" >= DATE_FROM_PARTS(1994, 1, 1)
              AND t0."l_shipdate" < DATE_FROM_PARTS(1995, 1, 1)
          ) * 0.5
      ) AS t8
    )
)
SELECT
  t7."s_name",
  t7."s_address"
FROM (
  SELECT
    t6."s_name" AS "s_name",
    t6."s_address" AS "s_address"
  FROM t6
) AS t7
ORDER BY
  t7."s_name" ASC