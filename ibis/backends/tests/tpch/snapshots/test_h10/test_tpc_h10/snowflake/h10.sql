WITH t1 AS (
  SELECT
    t7."C_CUSTKEY" AS "c_custkey",
    t7."C_NAME" AS "c_name",
    t7."C_ADDRESS" AS "c_address",
    t7."C_NATIONKEY" AS "c_nationkey",
    t7."C_PHONE" AS "c_phone",
    t7."C_ACCTBAL" AS "c_acctbal",
    t7."C_MKTSEGMENT" AS "c_mktsegment",
    t7."C_COMMENT" AS "c_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER" AS t7
), t2 AS (
  SELECT
    t7."O_ORDERKEY" AS "o_orderkey",
    t7."O_CUSTKEY" AS "o_custkey",
    t7."O_ORDERSTATUS" AS "o_orderstatus",
    t7."O_TOTALPRICE" AS "o_totalprice",
    t7."O_ORDERDATE" AS "o_orderdate",
    t7."O_ORDERPRIORITY" AS "o_orderpriority",
    t7."O_CLERK" AS "o_clerk",
    t7."O_SHIPPRIORITY" AS "o_shippriority",
    t7."O_COMMENT" AS "o_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."ORDERS" AS t7
), t3 AS (
  SELECT
    t7."L_ORDERKEY" AS "l_orderkey",
    t7."L_PARTKEY" AS "l_partkey",
    t7."L_SUPPKEY" AS "l_suppkey",
    t7."L_LINENUMBER" AS "l_linenumber",
    t7."L_QUANTITY" AS "l_quantity",
    t7."L_EXTENDEDPRICE" AS "l_extendedprice",
    t7."L_DISCOUNT" AS "l_discount",
    t7."L_TAX" AS "l_tax",
    t7."L_RETURNFLAG" AS "l_returnflag",
    t7."L_LINESTATUS" AS "l_linestatus",
    t7."L_SHIPDATE" AS "l_shipdate",
    t7."L_COMMITDATE" AS "l_commitdate",
    t7."L_RECEIPTDATE" AS "l_receiptdate",
    t7."L_SHIPINSTRUCT" AS "l_shipinstruct",
    t7."L_SHIPMODE" AS "l_shipmode",
    t7."L_COMMENT" AS "l_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM" AS t7
), t0 AS (
  SELECT
    t7."N_NATIONKEY" AS "n_nationkey",
    t7."N_NAME" AS "n_name",
    t7."N_REGIONKEY" AS "n_regionkey",
    t7."N_COMMENT" AS "n_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."NATION" AS t7
), t4 AS (
  SELECT
    t1."c_custkey" AS "c_custkey",
    t1."c_name" AS "c_name",
    t1."c_address" AS "c_address",
    t1."c_nationkey" AS "c_nationkey",
    t1."c_phone" AS "c_phone",
    t1."c_acctbal" AS "c_acctbal",
    t1."c_mktsegment" AS "c_mktsegment",
    t1."c_comment" AS "c_comment",
    t2."o_orderkey" AS "o_orderkey",
    t2."o_custkey" AS "o_custkey",
    t2."o_orderstatus" AS "o_orderstatus",
    t2."o_totalprice" AS "o_totalprice",
    t2."o_orderdate" AS "o_orderdate",
    t2."o_orderpriority" AS "o_orderpriority",
    t2."o_clerk" AS "o_clerk",
    t2."o_shippriority" AS "o_shippriority",
    t2."o_comment" AS "o_comment",
    t3."l_orderkey" AS "l_orderkey",
    t3."l_partkey" AS "l_partkey",
    t3."l_suppkey" AS "l_suppkey",
    t3."l_linenumber" AS "l_linenumber",
    t3."l_quantity" AS "l_quantity",
    t3."l_extendedprice" AS "l_extendedprice",
    t3."l_discount" AS "l_discount",
    t3."l_tax" AS "l_tax",
    t3."l_returnflag" AS "l_returnflag",
    t3."l_linestatus" AS "l_linestatus",
    t3."l_shipdate" AS "l_shipdate",
    t3."l_commitdate" AS "l_commitdate",
    t3."l_receiptdate" AS "l_receiptdate",
    t3."l_shipinstruct" AS "l_shipinstruct",
    t3."l_shipmode" AS "l_shipmode",
    t3."l_comment" AS "l_comment",
    t0."n_nationkey" AS "n_nationkey",
    t0."n_name" AS "n_name",
    t0."n_regionkey" AS "n_regionkey",
    t0."n_comment" AS "n_comment"
  FROM t1
  JOIN t2
    ON t1."c_custkey" = t2."o_custkey"
  JOIN t3
    ON t3."l_orderkey" = t2."o_orderkey"
  JOIN t0
    ON t1."c_nationkey" = t0."n_nationkey"
), t5 AS (
  SELECT
    t4."c_custkey" AS "c_custkey",
    t4."c_name" AS "c_name",
    t4."c_acctbal" AS "c_acctbal",
    t4."n_name" AS "n_name",
    t4."c_address" AS "c_address",
    t4."c_phone" AS "c_phone",
    t4."c_comment" AS "c_comment",
    SUM(t4."l_extendedprice" * (
      1 - t4."l_discount"
    )) AS "revenue"
  FROM t4
  WHERE
    t4."o_orderdate" >= DATE_FROM_PARTS(1993, 10, 1)
    AND t4."o_orderdate" < DATE_FROM_PARTS(1994, 1, 1)
    AND t4."l_returnflag" = 'R'
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7
)
SELECT
  t6."c_custkey",
  t6."c_name",
  t6."revenue",
  t6."c_acctbal",
  t6."n_name",
  t6."c_address",
  t6."c_phone",
  t6."c_comment"
FROM (
  SELECT
    t5."c_custkey" AS "c_custkey",
    t5."c_name" AS "c_name",
    t5."revenue" AS "revenue",
    t5."c_acctbal" AS "c_acctbal",
    t5."n_name" AS "n_name",
    t5."c_address" AS "c_address",
    t5."c_phone" AS "c_phone",
    t5."c_comment" AS "c_comment"
  FROM t5
) AS t6
ORDER BY
  t6."revenue" DESC
LIMIT 20