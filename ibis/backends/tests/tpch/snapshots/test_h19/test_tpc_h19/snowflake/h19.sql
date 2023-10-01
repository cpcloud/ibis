WITH t0 AS (
  SELECT
    t3."L_ORDERKEY" AS "l_orderkey",
    t3."L_PARTKEY" AS "l_partkey",
    t3."L_SUPPKEY" AS "l_suppkey",
    t3."L_LINENUMBER" AS "l_linenumber",
    t3."L_QUANTITY" AS "l_quantity",
    t3."L_EXTENDEDPRICE" AS "l_extendedprice",
    t3."L_DISCOUNT" AS "l_discount",
    t3."L_TAX" AS "l_tax",
    t3."L_RETURNFLAG" AS "l_returnflag",
    t3."L_LINESTATUS" AS "l_linestatus",
    t3."L_SHIPDATE" AS "l_shipdate",
    t3."L_COMMITDATE" AS "l_commitdate",
    t3."L_RECEIPTDATE" AS "l_receiptdate",
    t3."L_SHIPINSTRUCT" AS "l_shipinstruct",
    t3."L_SHIPMODE" AS "l_shipmode",
    t3."L_COMMENT" AS "l_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM" AS t3
), t1 AS (
  SELECT
    t3."P_PARTKEY" AS "p_partkey",
    t3."P_NAME" AS "p_name",
    t3."P_MFGR" AS "p_mfgr",
    t3."P_BRAND" AS "p_brand",
    t3."P_TYPE" AS "p_type",
    t3."P_SIZE" AS "p_size",
    t3."P_CONTAINER" AS "p_container",
    t3."P_RETAILPRICE" AS "p_retailprice",
    t3."P_COMMENT" AS "p_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."PART" AS t3
), t2 AS (
  SELECT
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
    t0."l_comment" AS "l_comment",
    t1."p_partkey" AS "p_partkey",
    t1."p_name" AS "p_name",
    t1."p_mfgr" AS "p_mfgr",
    t1."p_brand" AS "p_brand",
    t1."p_type" AS "p_type",
    t1."p_size" AS "p_size",
    t1."p_container" AS "p_container",
    t1."p_retailprice" AS "p_retailprice",
    t1."p_comment" AS "p_comment"
  FROM t0
  JOIN t1
    ON t1."p_partkey" = t0."l_partkey"
)
SELECT
  SUM(t2."l_extendedprice" * (
    1 - t2."l_discount"
  )) AS "revenue"
FROM t2
WHERE
  t2."p_brand" = 'Brand#12'
  AND t2."p_container" IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
  AND t2."l_quantity" >= 1
  AND t2."l_quantity" <= 11
  AND t2."p_size" BETWEEN 1 AND 5
  AND t2."l_shipmode" IN ('AIR', 'AIR REG')
  AND t2."l_shipinstruct" = 'DELIVER IN PERSON'
  OR t2."p_brand" = 'Brand#23'
  AND t2."p_container" IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
  AND t2."l_quantity" >= 10
  AND t2."l_quantity" <= 20
  AND t2."p_size" BETWEEN 1 AND 10
  AND t2."l_shipmode" IN ('AIR', 'AIR REG')
  AND t2."l_shipinstruct" = 'DELIVER IN PERSON'
  OR t2."p_brand" = 'Brand#34'
  AND t2."p_container" IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
  AND t2."l_quantity" >= 20
  AND t2."l_quantity" <= 30
  AND t2."p_size" BETWEEN 1 AND 15
  AND t2."l_shipmode" IN ('AIR', 'AIR REG')
  AND t2."l_shipinstruct" = 'DELIVER IN PERSON'