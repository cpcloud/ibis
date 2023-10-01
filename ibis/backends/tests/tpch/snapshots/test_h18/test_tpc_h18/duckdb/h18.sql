WITH t1 AS (
  SELECT
    t3.c_custkey AS c_custkey,
    t3.c_name AS c_name,
    t3.c_address AS c_address,
    t3.c_nationkey AS c_nationkey,
    t3.c_phone AS c_phone,
    t3.c_acctbal AS c_acctbal,
    t3.c_mktsegment AS c_mktsegment,
    t3.c_comment AS c_comment,
    t4.o_orderkey AS o_orderkey,
    t4.o_custkey AS o_custkey,
    t4.o_orderstatus AS o_orderstatus,
    t4.o_totalprice AS o_totalprice,
    t4.o_orderdate AS o_orderdate,
    t4.o_orderpriority AS o_orderpriority,
    t4.o_clerk AS o_clerk,
    t4.o_shippriority AS o_shippriority,
    t4.o_comment AS o_comment,
    t5.l_orderkey AS l_orderkey,
    t5.l_partkey AS l_partkey,
    t5.l_suppkey AS l_suppkey,
    t5.l_linenumber AS l_linenumber,
    t5.l_quantity AS l_quantity,
    t5.l_extendedprice AS l_extendedprice,
    t5.l_discount AS l_discount,
    t5.l_tax AS l_tax,
    t5.l_returnflag AS l_returnflag,
    t5.l_linestatus AS l_linestatus,
    t5.l_shipdate AS l_shipdate,
    t5.l_commitdate AS l_commitdate,
    t5.l_receiptdate AS l_receiptdate,
    t5.l_shipinstruct AS l_shipinstruct,
    t5.l_shipmode AS l_shipmode,
    t5.l_comment AS l_comment
  FROM main.customer AS t3
  JOIN main.orders AS t4
    ON t3.c_custkey = t4.o_custkey
  JOIN main.lineitem AS t5
    ON t4.o_orderkey = t5.l_orderkey
), t0 AS (
  SELECT
    t3.l_orderkey AS l_orderkey,
    SUM(t3.l_quantity) AS qty_sum
  FROM main.lineitem AS t3
  GROUP BY
    1
)
SELECT
  t2.c_name,
  t2.c_custkey,
  t2.o_orderkey,
  t2.o_orderdate,
  t2.o_totalprice,
  t2.sum_qty
FROM (
  SELECT
    t1.c_name AS c_name,
    t1.c_custkey AS c_custkey,
    t1.o_orderkey AS o_orderkey,
    t1.o_orderdate AS o_orderdate,
    t1.o_totalprice AS o_totalprice,
    SUM(t1.l_quantity) AS sum_qty
  FROM t1
  WHERE
    t1.o_orderkey IN (
      SELECT
        t6.l_orderkey
      FROM (
        SELECT
          t0.l_orderkey AS l_orderkey,
          t0.qty_sum AS qty_sum
        FROM t0
        WHERE
          t0.qty_sum > CAST(300 AS SMALLINT)
      ) AS t6
    )
  GROUP BY
    1,
    2,
    3,
    4,
    5
) AS t2
ORDER BY
  t2.o_totalprice DESC,
  t2.o_orderdate ASC
LIMIT 100