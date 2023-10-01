WITH t2 AS (
  SELECT
    t8."C_CUSTKEY" AS "c_custkey",
    t8."C_NAME" AS "c_name",
    t8."C_ADDRESS" AS "c_address",
    t8."C_NATIONKEY" AS "c_nationkey",
    t8."C_PHONE" AS "c_phone",
    t8."C_ACCTBAL" AS "c_acctbal",
    t8."C_MKTSEGMENT" AS "c_mktsegment",
    t8."C_COMMENT" AS "c_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER" AS t8
), t5 AS (
  SELECT
    t8."O_ORDERKEY" AS "o_orderkey",
    t8."O_CUSTKEY" AS "o_custkey",
    t8."O_ORDERSTATUS" AS "o_orderstatus",
    t8."O_TOTALPRICE" AS "o_totalprice",
    t8."O_ORDERDATE" AS "o_orderdate",
    t8."O_ORDERPRIORITY" AS "o_orderpriority",
    t8."O_CLERK" AS "o_clerk",
    t8."O_SHIPPRIORITY" AS "o_shippriority",
    t8."O_COMMENT" AS "o_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."ORDERS" AS t8
), t4 AS (
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
), t1 AS (
  SELECT
    t8."S_SUPPKEY" AS "s_suppkey",
    t8."S_NAME" AS "s_name",
    t8."S_ADDRESS" AS "s_address",
    t8."S_NATIONKEY" AS "s_nationkey",
    t8."S_PHONE" AS "s_phone",
    t8."S_ACCTBAL" AS "s_acctbal",
    t8."S_COMMENT" AS "s_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."SUPPLIER" AS t8
), t0 AS (
  SELECT
    t8."N_NATIONKEY" AS "n_nationkey",
    t8."N_NAME" AS "n_name",
    t8."N_REGIONKEY" AS "n_regionkey",
    t8."N_COMMENT" AS "n_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."NATION" AS t8
), t3 AS (
  SELECT
    t8."R_REGIONKEY" AS "r_regionkey",
    t8."R_NAME" AS "r_name",
    t8."R_COMMENT" AS "r_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."REGION" AS t8
), t6 AS (
  SELECT
    t2."c_custkey" AS "c_custkey",
    t2."c_name" AS "c_name",
    t2."c_address" AS "c_address",
    t2."c_nationkey" AS "c_nationkey",
    t2."c_phone" AS "c_phone",
    t2."c_acctbal" AS "c_acctbal",
    t2."c_mktsegment" AS "c_mktsegment",
    t2."c_comment" AS "c_comment",
    t5."o_orderkey" AS "o_orderkey",
    t5."o_custkey" AS "o_custkey",
    t5."o_orderstatus" AS "o_orderstatus",
    t5."o_totalprice" AS "o_totalprice",
    t5."o_orderdate" AS "o_orderdate",
    t5."o_orderpriority" AS "o_orderpriority",
    t5."o_clerk" AS "o_clerk",
    t5."o_shippriority" AS "o_shippriority",
    t5."o_comment" AS "o_comment",
    t4."l_orderkey" AS "l_orderkey",
    t4."l_partkey" AS "l_partkey",
    t4."l_suppkey" AS "l_suppkey",
    t4."l_linenumber" AS "l_linenumber",
    t4."l_quantity" AS "l_quantity",
    t4."l_extendedprice" AS "l_extendedprice",
    t4."l_discount" AS "l_discount",
    t4."l_tax" AS "l_tax",
    t4."l_returnflag" AS "l_returnflag",
    t4."l_linestatus" AS "l_linestatus",
    t4."l_shipdate" AS "l_shipdate",
    t4."l_commitdate" AS "l_commitdate",
    t4."l_receiptdate" AS "l_receiptdate",
    t4."l_shipinstruct" AS "l_shipinstruct",
    t4."l_shipmode" AS "l_shipmode",
    t4."l_comment" AS "l_comment",
    t1."s_suppkey" AS "s_suppkey",
    t1."s_name" AS "s_name",
    t1."s_address" AS "s_address",
    t1."s_nationkey" AS "s_nationkey",
    t1."s_phone" AS "s_phone",
    t1."s_acctbal" AS "s_acctbal",
    t1."s_comment" AS "s_comment",
    t0."n_nationkey" AS "n_nationkey",
    t0."n_name" AS "n_name",
    t0."n_regionkey" AS "n_regionkey",
    t0."n_comment" AS "n_comment",
    t3."r_regionkey" AS "r_regionkey",
    t3."r_name" AS "r_name",
    t3."r_comment" AS "r_comment"
  FROM t2
  JOIN t5
    ON t2."c_custkey" = t5."o_custkey"
  JOIN t4
    ON t4."l_orderkey" = t5."o_orderkey"
  JOIN t1
    ON t4."l_suppkey" = t1."s_suppkey"
  JOIN t0
    ON t2."c_nationkey" = t1."s_nationkey" AND t1."s_nationkey" = t0."n_nationkey"
  JOIN t3
    ON t0."n_regionkey" = t3."r_regionkey"
)
SELECT
  t7."n_name",
  t7."revenue"
FROM (
  SELECT
    t6."n_name" AS "n_name",
    SUM(t6."l_extendedprice" * (
      1 - t6."l_discount"
    )) AS "revenue"
  FROM t6
  WHERE
    t6."r_name" = 'ASIA'
    AND t6."o_orderdate" >= DATE_FROM_PARTS(1994, 1, 1)
    AND t6."o_orderdate" < DATE_FROM_PARTS(1995, 1, 1)
  GROUP BY
    1
) AS t7
ORDER BY
  t7."revenue" DESC