WITH t1 AS (
  SELECT
    t4."O_ORDERKEY" AS "o_orderkey",
    t4."O_CUSTKEY" AS "o_custkey",
    t4."O_ORDERSTATUS" AS "o_orderstatus",
    t4."O_TOTALPRICE" AS "o_totalprice",
    t4."O_ORDERDATE" AS "o_orderdate",
    t4."O_ORDERPRIORITY" AS "o_orderpriority",
    t4."O_CLERK" AS "o_clerk",
    t4."O_SHIPPRIORITY" AS "o_shippriority",
    t4."O_COMMENT" AS "o_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."ORDERS" AS t4
), t0 AS (
  SELECT
    t4."L_ORDERKEY" AS "l_orderkey",
    t4."L_PARTKEY" AS "l_partkey",
    t4."L_SUPPKEY" AS "l_suppkey",
    t4."L_LINENUMBER" AS "l_linenumber",
    t4."L_QUANTITY" AS "l_quantity",
    t4."L_EXTENDEDPRICE" AS "l_extendedprice",
    t4."L_DISCOUNT" AS "l_discount",
    t4."L_TAX" AS "l_tax",
    t4."L_RETURNFLAG" AS "l_returnflag",
    t4."L_LINESTATUS" AS "l_linestatus",
    t4."L_SHIPDATE" AS "l_shipdate",
    t4."L_COMMITDATE" AS "l_commitdate",
    t4."L_RECEIPTDATE" AS "l_receiptdate",
    t4."L_SHIPINSTRUCT" AS "l_shipinstruct",
    t4."L_SHIPMODE" AS "l_shipmode",
    t4."L_COMMENT" AS "l_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM" AS t4
), t2 AS (
  SELECT
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
  FROM t1
  JOIN t0
    ON t1."o_orderkey" = t0."l_orderkey"
)
SELECT
  t3."l_shipmode",
  t3."high_line_count",
  t3."low_line_count"
FROM (
  SELECT
    t2."l_shipmode" AS "l_shipmode",
    SUM(CASE t2."o_orderpriority" WHEN '1-URGENT' THEN 1 WHEN '2-HIGH' THEN 1 ELSE 0 END) AS "high_line_count",
    SUM(CASE t2."o_orderpriority" WHEN '1-URGENT' THEN 0 WHEN '2-HIGH' THEN 0 ELSE 1 END) AS "low_line_count"
  FROM t2
  WHERE
    t2."l_shipmode" IN ('MAIL', 'SHIP')
    AND t2."l_commitdate" < t2."l_receiptdate"
    AND t2."l_shipdate" < t2."l_commitdate"
    AND t2."l_receiptdate" >= DATE_FROM_PARTS(1994, 1, 1)
    AND t2."l_receiptdate" < DATE_FROM_PARTS(1995, 1, 1)
  GROUP BY
    1
) AS t3
ORDER BY
  t3."l_shipmode" ASC