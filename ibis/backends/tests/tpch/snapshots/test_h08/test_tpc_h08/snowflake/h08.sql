WITH t6 AS (
  SELECT
    t11."P_PARTKEY" AS "p_partkey",
    t11."P_NAME" AS "p_name",
    t11."P_MFGR" AS "p_mfgr",
    t11."P_BRAND" AS "p_brand",
    t11."P_TYPE" AS "p_type",
    t11."P_SIZE" AS "p_size",
    t11."P_CONTAINER" AS "p_container",
    t11."P_RETAILPRICE" AS "p_retailprice",
    t11."P_COMMENT" AS "p_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."PART" AS t11
), t0 AS (
  SELECT
    t11."L_ORDERKEY" AS "l_orderkey",
    t11."L_PARTKEY" AS "l_partkey",
    t11."L_SUPPKEY" AS "l_suppkey",
    t11."L_LINENUMBER" AS "l_linenumber",
    t11."L_QUANTITY" AS "l_quantity",
    t11."L_EXTENDEDPRICE" AS "l_extendedprice",
    t11."L_DISCOUNT" AS "l_discount",
    t11."L_TAX" AS "l_tax",
    t11."L_RETURNFLAG" AS "l_returnflag",
    t11."L_LINESTATUS" AS "l_linestatus",
    t11."L_SHIPDATE" AS "l_shipdate",
    t11."L_COMMITDATE" AS "l_commitdate",
    t11."L_RECEIPTDATE" AS "l_receiptdate",
    t11."L_SHIPINSTRUCT" AS "l_shipinstruct",
    t11."L_SHIPMODE" AS "l_shipmode",
    t11."L_COMMENT" AS "l_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM" AS t11
), t3 AS (
  SELECT
    t11."S_SUPPKEY" AS "s_suppkey",
    t11."S_NAME" AS "s_name",
    t11."S_ADDRESS" AS "s_address",
    t11."S_NATIONKEY" AS "s_nationkey",
    t11."S_PHONE" AS "s_phone",
    t11."S_ACCTBAL" AS "s_acctbal",
    t11."S_COMMENT" AS "s_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."SUPPLIER" AS t11
), t2 AS (
  SELECT
    t11."O_ORDERKEY" AS "o_orderkey",
    t11."O_CUSTKEY" AS "o_custkey",
    t11."O_ORDERSTATUS" AS "o_orderstatus",
    t11."O_TOTALPRICE" AS "o_totalprice",
    t11."O_ORDERDATE" AS "o_orderdate",
    t11."O_ORDERPRIORITY" AS "o_orderpriority",
    t11."O_CLERK" AS "o_clerk",
    t11."O_SHIPPRIORITY" AS "o_shippriority",
    t11."O_COMMENT" AS "o_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."ORDERS" AS t11
), t5 AS (
  SELECT
    t11."C_CUSTKEY" AS "c_custkey",
    t11."C_NAME" AS "c_name",
    t11."C_ADDRESS" AS "c_address",
    t11."C_NATIONKEY" AS "c_nationkey",
    t11."C_PHONE" AS "c_phone",
    t11."C_ACCTBAL" AS "c_acctbal",
    t11."C_MKTSEGMENT" AS "c_mktsegment",
    t11."C_COMMENT" AS "c_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER" AS t11
), t1 AS (
  SELECT
    t11."N_NATIONKEY" AS "n_nationkey",
    t11."N_NAME" AS "n_name",
    t11."N_REGIONKEY" AS "n_regionkey",
    t11."N_COMMENT" AS "n_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."NATION" AS t11
), t4 AS (
  SELECT
    t11."R_REGIONKEY" AS "r_regionkey",
    t11."R_NAME" AS "r_name",
    t11."R_COMMENT" AS "r_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."REGION" AS t11
), t7 AS (
  SELECT
    CAST(DATE_PART(year, t2."o_orderdate") AS SMALLINT) AS "o_year",
    t0."l_extendedprice" * (
      1 - t0."l_discount"
    ) AS "volume",
    t11."n_name" AS "nation",
    t4."r_name" AS "r_name",
    t2."o_orderdate" AS "o_orderdate",
    t6."p_type" AS "p_type"
  FROM t6
  JOIN t0
    ON t6."p_partkey" = t0."l_partkey"
  JOIN t3
    ON t3."s_suppkey" = t0."l_suppkey"
  JOIN t2
    ON t0."l_orderkey" = t2."o_orderkey"
  JOIN t5
    ON t2."o_custkey" = t5."c_custkey"
  JOIN t1
    ON t5."c_nationkey" = t1."n_nationkey"
  JOIN t4
    ON t1."n_regionkey" = t4."r_regionkey"
  JOIN t1 AS t11
    ON t3."s_nationkey" = t11."n_nationkey"
), t8 AS (
  SELECT
    t7."o_year" AS "o_year",
    t7."volume" AS "volume",
    t7."nation" AS "nation",
    t7."r_name" AS "r_name",
    t7."o_orderdate" AS "o_orderdate",
    t7."p_type" AS "p_type"
  FROM t7
  WHERE
    t7."r_name" = 'AMERICA'
    AND t7."o_orderdate" BETWEEN DATE_FROM_PARTS(1995, 1, 1) AND DATE_FROM_PARTS(1996, 12, 31)
    AND t7."p_type" = 'ECONOMY ANODIZED STEEL'
), t9 AS (
  SELECT
    t8."o_year" AS "o_year",
    t8."volume" AS "volume",
    t8."nation" AS "nation",
    t8."r_name" AS "r_name",
    t8."o_orderdate" AS "o_orderdate",
    t8."p_type" AS "p_type",
    CASE WHEN (
      t8."nation" = 'BRAZIL'
    ) THEN t8."volume" ELSE 0 END AS "nation_volume"
  FROM t8
)
SELECT
  t10."o_year",
  t10."mkt_share"
FROM (
  SELECT
    t9."o_year" AS "o_year",
    SUM(t9."nation_volume") / SUM(t9."volume") AS "mkt_share"
  FROM t9
  GROUP BY
    1
) AS t10
ORDER BY
  t10."o_year" ASC