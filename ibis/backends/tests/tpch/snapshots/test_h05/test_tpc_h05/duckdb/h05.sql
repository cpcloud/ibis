WITH t0 AS (
  SELECT
    t2.c_custkey AS c_custkey,
    t2.c_name AS c_name,
    t2.c_address AS c_address,
    t2.c_nationkey AS c_nationkey,
    t2.c_phone AS c_phone,
    t2.c_acctbal AS c_acctbal,
    t2.c_mktsegment AS c_mktsegment,
    t2.c_comment AS c_comment,
    t3.o_orderkey AS o_orderkey,
    t3.o_custkey AS o_custkey,
    t3.o_orderstatus AS o_orderstatus,
    t3.o_totalprice AS o_totalprice,
    t3.o_orderdate AS o_orderdate,
    t3.o_orderpriority AS o_orderpriority,
    t3.o_clerk AS o_clerk,
    t3.o_shippriority AS o_shippriority,
    t3.o_comment AS o_comment,
    t4.l_orderkey AS l_orderkey,
    t4.l_partkey AS l_partkey,
    t4.l_suppkey AS l_suppkey,
    t4.l_linenumber AS l_linenumber,
    t4.l_quantity AS l_quantity,
    t4.l_extendedprice AS l_extendedprice,
    t4.l_discount AS l_discount,
    t4.l_tax AS l_tax,
    t4.l_returnflag AS l_returnflag,
    t4.l_linestatus AS l_linestatus,
    t4.l_shipdate AS l_shipdate,
    t4.l_commitdate AS l_commitdate,
    t4.l_receiptdate AS l_receiptdate,
    t4.l_shipinstruct AS l_shipinstruct,
    t4.l_shipmode AS l_shipmode,
    t4.l_comment AS l_comment,
    t5.s_suppkey AS s_suppkey,
    t5.s_name AS s_name,
    t5.s_address AS s_address,
    t5.s_nationkey AS s_nationkey,
    t5.s_phone AS s_phone,
    t5.s_acctbal AS s_acctbal,
    t5.s_comment AS s_comment,
    t6.n_nationkey AS n_nationkey,
    t6.n_name AS n_name,
    t6.n_regionkey AS n_regionkey,
    t6.n_comment AS n_comment,
    t7.r_regionkey AS r_regionkey,
    t7.r_name AS r_name,
    t7.r_comment AS r_comment
  FROM main.customer AS t2
  JOIN main.orders AS t3
    ON t2.c_custkey = t3.o_custkey
  JOIN main.lineitem AS t4
    ON t4.l_orderkey = t3.o_orderkey
  JOIN main.supplier AS t5
    ON t4.l_suppkey = t5.s_suppkey
  JOIN main.nation AS t6
    ON t2.c_nationkey = t5.s_nationkey AND t5.s_nationkey = t6.n_nationkey
  JOIN main.region AS t7
    ON t6.n_regionkey = t7.r_regionkey
)
SELECT
  t1.n_name,
  t1.revenue
FROM (
  SELECT
    t0.n_name AS n_name,
    SUM(t0.l_extendedprice * (
      CAST(1 AS TINYINT) - t0.l_discount
    )) AS revenue
  FROM t0
  WHERE
    t0.r_name = 'ASIA'
    AND t0.o_orderdate >= CAST('1994-01-01' AS DATE)
    AND t0.o_orderdate < CAST('1995-01-01' AS DATE)
  GROUP BY
    1
) AS t1
ORDER BY
  t1.revenue DESC