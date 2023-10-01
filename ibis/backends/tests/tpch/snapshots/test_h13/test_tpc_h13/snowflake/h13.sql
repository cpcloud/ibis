WITH t1 AS (
  SELECT
    t5."C_CUSTKEY" AS "c_custkey",
    t5."C_NAME" AS "c_name",
    t5."C_ADDRESS" AS "c_address",
    t5."C_NATIONKEY" AS "c_nationkey",
    t5."C_PHONE" AS "c_phone",
    t5."C_ACCTBAL" AS "c_acctbal",
    t5."C_MKTSEGMENT" AS "c_mktsegment",
    t5."C_COMMENT" AS "c_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER" AS t5
), t0 AS (
  SELECT
    t5."O_ORDERKEY" AS "o_orderkey",
    t5."O_CUSTKEY" AS "o_custkey",
    t5."O_ORDERSTATUS" AS "o_orderstatus",
    t5."O_TOTALPRICE" AS "o_totalprice",
    t5."O_ORDERDATE" AS "o_orderdate",
    t5."O_ORDERPRIORITY" AS "o_orderpriority",
    t5."O_CLERK" AS "o_clerk",
    t5."O_SHIPPRIORITY" AS "o_shippriority",
    t5."O_COMMENT" AS "o_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."ORDERS" AS t5
), t2 AS (
  SELECT
    t1."c_custkey" AS "c_custkey",
    t1."c_name" AS "c_name",
    t1."c_address" AS "c_address",
    t1."c_nationkey" AS "c_nationkey",
    t1."c_phone" AS "c_phone",
    t1."c_acctbal" AS "c_acctbal",
    t1."c_mktsegment" AS "c_mktsegment",
    t1."c_comment" AS "c_comment",
    t0."o_orderkey" AS "o_orderkey",
    t0."o_custkey" AS "o_custkey",
    t0."o_orderstatus" AS "o_orderstatus",
    t0."o_totalprice" AS "o_totalprice",
    t0."o_orderdate" AS "o_orderdate",
    t0."o_orderpriority" AS "o_orderpriority",
    t0."o_clerk" AS "o_clerk",
    t0."o_shippriority" AS "o_shippriority",
    t0."o_comment" AS "o_comment"
  FROM t1
  LEFT OUTER JOIN t0
    ON t1."c_custkey" = t0."o_custkey" AND NOT t0."o_comment" LIKE '%special%requests%'
), t3 AS (
  SELECT
    t2."c_custkey" AS "c_custkey",
    COUNT(t2."o_orderkey") AS "c_count"
  FROM t2
  GROUP BY
    1
)
SELECT
  t4."c_count",
  t4."custdist"
FROM (
  SELECT
    t3."c_count" AS "c_count",
    COUNT(*) AS "custdist"
  FROM t3
  GROUP BY
    1
) AS t4
ORDER BY
  t4."custdist" DESC,
  t4."c_count" DESC