WITH t0 AS (
  SELECT
    t2.o_orderkey AS o_orderkey,
    t2.o_custkey AS o_custkey,
    t2.o_orderstatus AS o_orderstatus,
    t2.o_totalprice AS o_totalprice,
    t2.o_orderdate AS o_orderdate,
    t2.o_orderpriority AS o_orderpriority,
    t2.o_clerk AS o_clerk,
    t2.o_shippriority AS o_shippriority,
    t2.o_comment AS o_comment,
    t3.l_orderkey AS l_orderkey,
    t3.l_partkey AS l_partkey,
    t3.l_suppkey AS l_suppkey,
    t3.l_linenumber AS l_linenumber,
    t3.l_quantity AS l_quantity,
    t3.l_extendedprice AS l_extendedprice,
    t3.l_discount AS l_discount,
    t3.l_tax AS l_tax,
    t3.l_returnflag AS l_returnflag,
    t3.l_linestatus AS l_linestatus,
    t3.l_shipdate AS l_shipdate,
    t3.l_commitdate AS l_commitdate,
    t3.l_receiptdate AS l_receiptdate,
    t3.l_shipinstruct AS l_shipinstruct,
    t3.l_shipmode AS l_shipmode,
    t3.l_comment AS l_comment
  FROM main.orders AS t2
  JOIN main.lineitem AS t3
    ON t2.o_orderkey = t3.l_orderkey
)
SELECT
  t1.l_shipmode,
  t1.high_line_count,
  t1.low_line_count
FROM (
  SELECT
    t0.l_shipmode AS l_shipmode,
    SUM(
      CASE t0.o_orderpriority
        WHEN '1-URGENT'
        THEN CAST(1 AS TINYINT)
        WHEN '2-HIGH'
        THEN CAST(1 AS TINYINT)
        ELSE CAST(0 AS TINYINT)
      END
    ) AS high_line_count,
    SUM(
      CASE t0.o_orderpriority
        WHEN '1-URGENT'
        THEN CAST(0 AS TINYINT)
        WHEN '2-HIGH'
        THEN CAST(0 AS TINYINT)
        ELSE CAST(1 AS TINYINT)
      END
    ) AS low_line_count
  FROM t0
  WHERE
    t0.l_shipmode IN ('MAIL', 'SHIP')
    AND t0.l_commitdate < t0.l_receiptdate
    AND t0.l_shipdate < t0.l_commitdate
    AND t0.l_receiptdate >= CAST('1994-01-01' AS DATE)
    AND t0.l_receiptdate < CAST('1995-01-01' AS DATE)
  GROUP BY
    1
) AS t1
ORDER BY
  t1.l_shipmode ASC