SELECT
  *
FROM (
  SELECT
    t12.n_name AS n_name,
    SUM((
      t12.l_extendedprice * (
        CAST(1 AS TINYINT) - t12.l_discount
      )
    )) AS revenue
  FROM (
    SELECT
      *
    FROM (
      SELECT
        t0.c_custkey AS c_custkey,
        t0.c_name AS c_name,
        t0.c_address AS c_address,
        t0.c_nationkey AS c_nationkey,
        t0.c_phone AS c_phone,
        t0.c_acctbal AS c_acctbal,
        t0.c_mktsegment AS c_mktsegment,
        t0.c_comment AS c_comment,
        t1.o_orderkey AS o_orderkey,
        t1.o_custkey AS o_custkey,
        t1.o_orderstatus AS o_orderstatus,
        t1.o_totalprice AS o_totalprice,
        t1.o_orderdate AS o_orderdate,
        t1.o_orderpriority AS o_orderpriority,
        t1.o_clerk AS o_clerk,
        t1.o_shippriority AS o_shippriority,
        t1.o_comment AS o_comment,
        t2.l_orderkey AS l_orderkey,
        t2.l_partkey AS l_partkey,
        t2.l_suppkey AS l_suppkey,
        t2.l_linenumber AS l_linenumber,
        t2.l_quantity AS l_quantity,
        t2.l_extendedprice AS l_extendedprice,
        t2.l_discount AS l_discount,
        t2.l_tax AS l_tax,
        t2.l_returnflag AS l_returnflag,
        t2.l_linestatus AS l_linestatus,
        t2.l_shipdate AS l_shipdate,
        t2.l_commitdate AS l_commitdate,
        t2.l_receiptdate AS l_receiptdate,
        t2.l_shipinstruct AS l_shipinstruct,
        t2.l_shipmode AS l_shipmode,
        t2.l_comment AS l_comment,
        t3.s_suppkey AS s_suppkey,
        t3.s_name AS s_name,
        t3.s_address AS s_address,
        t3.s_nationkey AS s_nationkey,
        t3.s_phone AS s_phone,
        t3.s_acctbal AS s_acctbal,
        t3.s_comment AS s_comment,
        t4.n_nationkey AS n_nationkey,
        t4.n_name AS n_name,
        t4.n_regionkey AS n_regionkey,
        t4.n_comment AS n_comment,
        t5.r_regionkey AS r_regionkey,
        t5.r_name AS r_name,
        t5.r_comment AS r_comment
      FROM "customer" AS t0
      INNER JOIN "orders" AS t1
        ON (
          t0.c_custkey = t1.o_custkey
        )
      INNER JOIN "lineitem" AS t2
        ON (
          t2.l_orderkey = t1.o_orderkey
        )
      INNER JOIN "supplier" AS t3
        ON (
          t2.l_suppkey = t3.s_suppkey
        )
      INNER JOIN "nation" AS t4
        ON (
          t0.c_nationkey = t3.s_nationkey
        ) AND (
          t3.s_nationkey = t4.n_nationkey
        )
      INNER JOIN "region" AS t5
        ON (
          t4.n_regionkey = t5.r_regionkey
        )
    ) AS t11
    WHERE
      (
        t11.r_name = 'ASIA'
      )
      AND (
        t11.o_orderdate >= MAKE_DATE(1994, 1, 1)
      )
      AND (
        t11.o_orderdate < MAKE_DATE(1995, 1, 1)
      )
  ) AS t12
  GROUP BY
    1
) AS t13
ORDER BY
  t13.revenue DESC