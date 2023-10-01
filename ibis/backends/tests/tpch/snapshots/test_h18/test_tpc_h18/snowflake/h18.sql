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
), t1 AS (
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
), t4 AS (
  SELECT
    t2."c_custkey" AS "c_custkey",
    t2."c_name" AS "c_name",
    t2."c_address" AS "c_address",
    t2."c_nationkey" AS "c_nationkey",
    t2."c_phone" AS "c_phone",
    t2."c_acctbal" AS "c_acctbal",
    t2."c_mktsegment" AS "c_mktsegment",
    t2."c_comment" AS "c_comment",
    t1."o_orderkey" AS "o_orderkey",
    t1."o_custkey" AS "o_custkey",
    t1."o_orderstatus" AS "o_orderstatus",
    t1."o_totalprice" AS "o_totalprice",
    t1."o_orderdate" AS "o_orderdate",
    t1."o_orderpriority" AS "o_orderpriority",
    t1."o_clerk" AS "o_clerk",
    t1."o_shippriority" AS "o_shippriority",
    t1."o_comment" AS "o_comment",
    t0."l_orderkey" AS "l_orderkey",
    t0."l_partkey" AS "l_partkey",
    t0."l_suppkey" AS "l_suppkey",
    t0."l_linenumber" AS "l_linenumber",
    t0."l_quantity" AS "l_quantity",
    t0."l_extendedprice" AS "l_extendedprice",
    t0."l_discount" AS "l_discount",
    t0."l_tax" AS "l_tax",
    t0."l_returnflag" AS "l_returnflag",
    t0."l_linestatus" AS "l_linestatus",
    t0."l_shipdate" AS "l_shipdate",
    t0."l_commitdate" AS "l_commitdate",
    t0."l_receiptdate" AS "l_receiptdate",
    t0."l_shipinstruct" AS "l_shipinstruct",
    t0."l_shipmode" AS "l_shipmode",
    t0."l_comment" AS "l_comment"
  FROM t2
  JOIN t1
    ON t2."c_custkey" = t1."o_custkey"
  JOIN t0
    ON t1."o_orderkey" = t0."l_orderkey"
), t3 AS (
  SELECT
    t0."l_orderkey" AS "l_orderkey",
    SUM(t0."l_quantity") AS "qty_sum"
  FROM t0
  GROUP BY
    1
)
SELECT
  t5."c_name",
  t5."c_custkey",
  t5."o_orderkey",
  t5."o_orderdate",
  t5."o_totalprice",
  t5."sum_qty"
FROM (
  SELECT
    t4."c_name" AS "c_name",
    t4."c_custkey" AS "c_custkey",
    t4."o_orderkey" AS "o_orderkey",
    t4."o_orderdate" AS "o_orderdate",
    t4."o_totalprice" AS "o_totalprice",
    SUM(t4."l_quantity") AS "sum_qty"
  FROM t4
  WHERE
    t4."o_orderkey" IN (
      SELECT
        t6."l_orderkey"
      FROM (
        SELECT
          t3."l_orderkey" AS "l_orderkey",
          t3."qty_sum" AS "qty_sum"
        FROM t3
        WHERE
          t3."qty_sum" > 300
      ) AS t6
    )
  GROUP BY
    1,
    2,
    3,
    4,
    5
) AS t5
ORDER BY
  t5."o_totalprice" DESC,
  t5."o_orderdate" ASC
LIMIT 100