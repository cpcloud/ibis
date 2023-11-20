SELECT
  *
FROM (
  SELECT
    *
  FROM (
    SELECT
      t9.c_custkey AS c_custkey,
      t9.c_name AS c_name,
      t9.revenue AS revenue,
      t9.c_acctbal AS c_acctbal,
      t9.n_name AS n_name,
      t9.c_address AS c_address,
      t9.c_phone AS c_phone,
      t9.c_comment AS c_comment
    FROM (
      SELECT
        t8.c_custkey AS c_custkey,
        t8.c_name AS c_name,
        t8.c_acctbal AS c_acctbal,
        t8.n_name AS n_name,
        t8.c_address AS c_address,
        t8.c_phone AS c_phone,
        t8.c_comment AS c_comment,
        SUM((
          t8.l_extendedprice * (
            CAST(1 AS TINYINT) - t8.l_discount
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
            t3.n_nationkey AS n_nationkey,
            t3.n_name AS n_name,
            t3.n_regionkey AS n_regionkey,
            t3.n_comment AS n_comment
          FROM "customer" AS t0
          INNER JOIN "orders" AS t1
            ON (
              t0.c_custkey = t1.o_custkey
            )
          INNER JOIN "lineitem" AS t2
            ON (
              t2.l_orderkey = t1.o_orderkey
            )
          INNER JOIN "nation" AS t3
            ON (
              t0.c_nationkey = t3.n_nationkey
            )
        ) AS t7
        WHERE
          (
            (
              t7.o_orderdate >= MAKE_DATE(1993, 10, 1)
            )
            AND (
              t7.o_orderdate < MAKE_DATE(1994, 1, 1)
            )
          )
          AND (
            t7.l_returnflag = 'R'
          )
      ) AS t8
      GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7
    ) AS t9
  ) AS t10
  ORDER BY
    t10.revenue DESC
) AS t11
LIMIT 20