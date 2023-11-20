SELECT
  *
FROM (
  SELECT
    t4.l_shipmode AS l_shipmode,
    SUM(
      CASE t4.o_orderpriority
        WHEN '1-URGENT'
        THEN CAST(1 AS TINYINT)
        WHEN '2-HIGH'
        THEN CAST(1 AS TINYINT)
        ELSE CAST(0 AS TINYINT)
      END
    ) AS high_line_count,
    SUM(
      CASE t4.o_orderpriority
        WHEN '1-URGENT'
        THEN CAST(0 AS TINYINT)
        WHEN '2-HIGH'
        THEN CAST(0 AS TINYINT)
        ELSE CAST(1 AS TINYINT)
      END
    ) AS low_line_count
  FROM (
    SELECT
      *
    FROM (
      SELECT
        t0.o_orderkey AS o_orderkey,
        t0.o_custkey AS o_custkey,
        t0.o_orderstatus AS o_orderstatus,
        t0.o_totalprice AS o_totalprice,
        t0.o_orderdate AS o_orderdate,
        t0.o_orderpriority AS o_orderpriority,
        t0.o_clerk AS o_clerk,
        t0.o_shippriority AS o_shippriority,
        t0.o_comment AS o_comment,
        t1.l_orderkey AS l_orderkey,
        t1.l_partkey AS l_partkey,
        t1.l_suppkey AS l_suppkey,
        t1.l_linenumber AS l_linenumber,
        t1.l_quantity AS l_quantity,
        t1.l_extendedprice AS l_extendedprice,
        t1.l_discount AS l_discount,
        t1.l_tax AS l_tax,
        t1.l_returnflag AS l_returnflag,
        t1.l_linestatus AS l_linestatus,
        t1.l_shipdate AS l_shipdate,
        t1.l_commitdate AS l_commitdate,
        t1.l_receiptdate AS l_receiptdate,
        t1.l_shipinstruct AS l_shipinstruct,
        t1.l_shipmode AS l_shipmode,
        t1.l_comment AS l_comment
      FROM "orders" AS t0
      INNER JOIN "lineitem" AS t1
        ON (
          t0.o_orderkey = t1.l_orderkey
        )
    ) AS t3
    WHERE
      t3.l_shipmode IN ('MAIL', 'SHIP')
      AND (
        t3.l_commitdate < t3.l_receiptdate
      )
      AND (
        t3.l_shipdate < t3.l_commitdate
      )
      AND (
        t3.l_receiptdate >= MAKE_DATE(1994, 1, 1)
      )
      AND (
        t3.l_receiptdate < MAKE_DATE(1995, 1, 1)
      )
  ) AS t4
  GROUP BY
    1
) AS t5
ORDER BY
  t5.l_shipmode ASC