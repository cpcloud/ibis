WITH t2 AS (
  SELECT
    t6."S_SUPPKEY" AS "s_suppkey",
    t6."S_NAME" AS "s_name",
    t6."S_ADDRESS" AS "s_address",
    t6."S_NATIONKEY" AS "s_nationkey",
    t6."S_PHONE" AS "s_phone",
    t6."S_ACCTBAL" AS "s_acctbal",
    t6."S_COMMENT" AS "s_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."SUPPLIER" AS t6
), t0 AS (
  SELECT
    t6."L_ORDERKEY" AS "l_orderkey",
    t6."L_PARTKEY" AS "l_partkey",
    t6."L_SUPPKEY" AS "l_suppkey",
    t6."L_LINENUMBER" AS "l_linenumber",
    t6."L_QUANTITY" AS "l_quantity",
    t6."L_EXTENDEDPRICE" AS "l_extendedprice",
    t6."L_DISCOUNT" AS "l_discount",
    t6."L_TAX" AS "l_tax",
    t6."L_RETURNFLAG" AS "l_returnflag",
    t6."L_LINESTATUS" AS "l_linestatus",
    t6."L_SHIPDATE" AS "l_shipdate",
    t6."L_COMMITDATE" AS "l_commitdate",
    t6."L_RECEIPTDATE" AS "l_receiptdate",
    t6."L_SHIPINSTRUCT" AS "l_shipinstruct",
    t6."L_SHIPMODE" AS "l_shipmode",
    t6."L_COMMENT" AS "l_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM" AS t6
), t1 AS (
  SELECT
    t0."l_suppkey" AS "l_suppkey",
    SUM(t0."l_extendedprice" * (
      1 - t0."l_discount"
    )) AS "total_revenue"
  FROM t0
  WHERE
    t0."l_shipdate" >= DATE_FROM_PARTS(1996, 1, 1)
    AND t0."l_shipdate" < DATE_FROM_PARTS(1996, 4, 1)
  GROUP BY
    1
), t3 AS (
  SELECT
    t2."s_suppkey" AS "s_suppkey",
    t2."s_name" AS "s_name",
    t2."s_address" AS "s_address",
    t2."s_nationkey" AS "s_nationkey",
    t2."s_phone" AS "s_phone",
    t2."s_acctbal" AS "s_acctbal",
    t2."s_comment" AS "s_comment",
    t1."l_suppkey" AS "l_suppkey",
    t1."total_revenue" AS "total_revenue"
  FROM t2
  JOIN t1
    ON t2."s_suppkey" = t1."l_suppkey"
), t4 AS (
  SELECT
    t3."s_suppkey" AS "s_suppkey",
    t3."s_name" AS "s_name",
    t3."s_address" AS "s_address",
    t3."s_nationkey" AS "s_nationkey",
    t3."s_phone" AS "s_phone",
    t3."s_acctbal" AS "s_acctbal",
    t3."s_comment" AS "s_comment",
    t3."l_suppkey" AS "l_suppkey",
    t3."total_revenue" AS "total_revenue"
  FROM t3
  WHERE
    t3."total_revenue" = (
      SELECT
        MAX(t1."total_revenue") AS "Max(total_revenue)"
      FROM t1
    )
)
SELECT
  t5."s_suppkey",
  t5."s_name",
  t5."s_address",
  t5."s_phone",
  t5."total_revenue"
FROM (
  SELECT
    t4."s_suppkey" AS "s_suppkey",
    t4."s_name" AS "s_name",
    t4."s_address" AS "s_address",
    t4."s_nationkey" AS "s_nationkey",
    t4."s_phone" AS "s_phone",
    t4."s_acctbal" AS "s_acctbal",
    t4."s_comment" AS "s_comment",
    t4."l_suppkey" AS "l_suppkey",
    t4."total_revenue" AS "total_revenue"
  FROM t4
  ORDER BY
    t4."s_suppkey" ASC
) AS t5