WITH t0 AS (
  SELECT
    t4."C_CUSTKEY" AS "c_custkey",
    t4."C_NAME" AS "c_name",
    t4."C_ADDRESS" AS "c_address",
    t4."C_NATIONKEY" AS "c_nationkey",
    t4."C_PHONE" AS "c_phone",
    t4."C_ACCTBAL" AS "c_acctbal",
    t4."C_MKTSEGMENT" AS "c_mktsegment",
    t4."C_COMMENT" AS "c_comment"
  FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."CUSTOMER" AS t4
), t1 AS (
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
), t2 AS (
  SELECT
    CASE
      WHEN (
        0 + 1 >= 1
      )
      THEN SUBSTR(t0."c_phone", 0 + 1, 2)
      ELSE SUBSTR(t0."c_phone", 0 + 1 + LENGTH(t0."c_phone"), 2)
    END AS "cntrycode",
    t0."c_acctbal" AS "c_acctbal"
  FROM t0
  WHERE
    CASE
      WHEN (
        0 + 1 >= 1
      )
      THEN SUBSTR(t0."c_phone", 0 + 1, 2)
      ELSE SUBSTR(t0."c_phone", 0 + 1 + LENGTH(t0."c_phone"), 2)
    END IN ('13', '31', '23', '29', '30', '18', '17')
    AND t0."c_acctbal" > (
      SELECT
        AVG(t0."c_acctbal") AS "avg_bal"
      FROM t0
      WHERE
        t0."c_acctbal" > 0.0
        AND CASE
          WHEN (
            0 + 1 >= 1
          )
          THEN SUBSTR(t0."c_phone", 0 + 1, 2)
          ELSE SUBSTR(t0."c_phone", 0 + 1 + LENGTH(t0."c_phone"), 2)
        END IN ('13', '31', '23', '29', '30', '18', '17')
    )
    AND NOT (
      EXISTS(
        SELECT
          1 AS anon_1
        FROM t1
        WHERE
          t1."o_custkey" = t0."c_custkey"
      )
    )
)
SELECT
  t3."cntrycode",
  t3."numcust",
  t3."totacctbal"
FROM (
  SELECT
    t2."cntrycode" AS "cntrycode",
    COUNT(*) AS "numcust",
    SUM(t2."c_acctbal") AS "totacctbal"
  FROM t2
  GROUP BY
    1
) AS t3
ORDER BY
  t3."cntrycode" ASC