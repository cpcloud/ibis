WITH t2 AS (
  SELECT
    t6."C_CUSTKEY" AS "c_custkey",
    t6."C_NAME" AS "c_name",
    t6."C_ADDRESS" AS "c_address",
    t6."C_NATIONKEY" AS "c_nationkey",
    t6."C_PHONE" AS "c_phone",
    t6."C_ACCTBAL" AS "c_acctbal",
    t6."C_MKTSEGMENT" AS "c_mktsegment",
    t6."C_COMMENT" AS "c_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER" AS t6
), t0 AS (
  SELECT
    t6."O_ORDERKEY" AS "o_orderkey",
    t6."O_CUSTKEY" AS "o_custkey",
    t6."O_ORDERSTATUS" AS "o_orderstatus",
    t6."O_TOTALPRICE" AS "o_totalprice",
    t6."O_ORDERDATE" AS "o_orderdate",
    t6."O_ORDERPRIORITY" AS "o_orderpriority",
    t6."O_CLERK" AS "o_clerk",
    t6."O_SHIPPRIORITY" AS "o_shippriority",
    t6."O_COMMENT" AS "o_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."ORDERS" AS t6
), t1 AS (
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
), t3 AS (
  SELECT
    t2."c_custkey" AS "c_custkey",
    t2."c_name" AS "c_name",
    t2."c_address" AS "c_address",
    t2."c_nationkey" AS "c_nationkey",
    t2."c_phone" AS "c_phone",
    t2."c_acctbal" AS "c_acctbal",
    t2."c_mktsegment" AS "c_mktsegment",
    t2."c_comment" AS "c_comment",
    t0."o_orderkey" AS "o_orderkey",
    t0."o_custkey" AS "o_custkey",
    t0."o_orderstatus" AS "o_orderstatus",
    t0."o_totalprice" AS "o_totalprice",
    t0."o_orderdate" AS "o_orderdate",
    t0."o_orderpriority" AS "o_orderpriority",
    t0."o_clerk" AS "o_clerk",
    t0."o_shippriority" AS "o_shippriority",
    t0."o_comment" AS "o_comment",
    t1."l_orderkey" AS "l_orderkey",
    t1."l_partkey" AS "l_partkey",
    t1."l_suppkey" AS "l_suppkey",
    t1."l_linenumber" AS "l_linenumber",
    t1."l_quantity" AS "l_quantity",
    t1."l_extendedprice" AS "l_extendedprice",
    t1."l_discount" AS "l_discount",
    t1."l_tax" AS "l_tax",
    t1."l_returnflag" AS "l_returnflag",
    t1."l_linestatus" AS "l_linestatus",
    t1."l_shipdate" AS "l_shipdate",
    t1."l_commitdate" AS "l_commitdate",
    t1."l_receiptdate" AS "l_receiptdate",
    t1."l_shipinstruct" AS "l_shipinstruct",
    t1."l_shipmode" AS "l_shipmode",
    t1."l_comment" AS "l_comment"
  FROM t2
  JOIN t0
    ON t2."c_custkey" = t0."o_custkey"
  JOIN t1
    ON t1."l_orderkey" = t0."o_orderkey"
), t4 AS (
  SELECT
    t3."l_orderkey" AS "l_orderkey",
    t3."o_orderdate" AS "o_orderdate",
    t3."o_shippriority" AS "o_shippriority",
    SUM(t3."l_extendedprice" * (
      1 - t3."l_discount"
    )) AS "revenue"
  FROM t3
  WHERE
    t3."c_mktsegment" = 'BUILDING'
    AND t3."o_orderdate" < DATE_FROM_PARTS(1995, 3, 15)
    AND t3."l_shipdate" > DATE_FROM_PARTS(1995, 3, 15)
  GROUP BY
    1,
    2,
    3
)
SELECT
  t5."l_orderkey",
  t5."revenue",
  t5."o_orderdate",
  t5."o_shippriority"
FROM (
  SELECT
    t4."l_orderkey" AS "l_orderkey",
    t4."revenue" AS "revenue",
    t4."o_orderdate" AS "o_orderdate",
    t4."o_shippriority" AS "o_shippriority"
  FROM t4
) AS t5
ORDER BY
  t5."revenue" DESC,
  t5."o_orderdate" ASC
LIMIT 10