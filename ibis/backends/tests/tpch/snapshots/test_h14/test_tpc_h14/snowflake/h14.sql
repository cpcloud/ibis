WITH t1 AS (
  SELECT
    t2."L_ORDERKEY" AS "l_orderkey",
    t2."L_PARTKEY" AS "l_partkey",
    t2."L_SUPPKEY" AS "l_suppkey",
    t2."L_LINENUMBER" AS "l_linenumber",
    t2."L_QUANTITY" AS "l_quantity",
    t2."L_EXTENDEDPRICE" AS "l_extendedprice",
    t2."L_DISCOUNT" AS "l_discount",
    t2."L_TAX" AS "l_tax",
    t2."L_RETURNFLAG" AS "l_returnflag",
    t2."L_LINESTATUS" AS "l_linestatus",
    t2."L_SHIPDATE" AS "l_shipdate",
    t2."L_COMMITDATE" AS "l_commitdate",
    t2."L_RECEIPTDATE" AS "l_receiptdate",
    t2."L_SHIPINSTRUCT" AS "l_shipinstruct",
    t2."L_SHIPMODE" AS "l_shipmode",
    t2."L_COMMENT" AS "l_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM" AS t2
), t0 AS (
  SELECT
    t2."P_PARTKEY" AS "p_partkey",
    t2."P_NAME" AS "p_name",
    t2."P_MFGR" AS "p_mfgr",
    t2."P_BRAND" AS "p_brand",
    t2."P_TYPE" AS "p_type",
    t2."P_SIZE" AS "p_size",
    t2."P_CONTAINER" AS "p_container",
    t2."P_RETAILPRICE" AS "p_retailprice",
    t2."P_COMMENT" AS "p_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."PART" AS t2
)
SELECT
  (
    SUM(
      IFF(t0."p_type" LIKE 'PROMO%', t1."l_extendedprice" * (
        1 - t1."l_discount"
      ), 0)
    ) * 100
  ) / SUM(t1."l_extendedprice" * (
    1 - t1."l_discount"
  )) AS "promo_revenue"
FROM t1
JOIN t0
  ON t1."l_partkey" = t0."p_partkey"
WHERE
  t1."l_shipdate" >= DATE_FROM_PARTS(1995, 9, 1)
  AND t1."l_shipdate" < DATE_FROM_PARTS(1995, 10, 1)