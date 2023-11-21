SELECT
  *
FROM (
  SELECT
    t12.supp_nation AS supp_nation,
    t12.cust_nation AS cust_nation,
    t12.l_year AS l_year,
    SUM(t12.volume) AS revenue
  FROM (
    SELECT
      *
    FROM (
      SELECT
        t4.n_name AS supp_nation,
        t5.n_name AS cust_nation,
        t1.l_shipdate AS l_shipdate,
        t1.l_extendedprice AS l_extendedprice,
        t1.l_discount AS l_discount,
        EXTRACT('year' FROM t1.l_shipdate) AS l_year,
        (
          t1.l_extendedprice * (
            CAST(1 AS TINYINT) - t1.l_discount
          )
        ) AS volume
      FROM "supplier" AS t0
      INNER JOIN "lineitem" AS t1
        ON (
          t0.s_suppkey = t1.l_suppkey
        )
      INNER JOIN "orders" AS t2
        ON (
          t2.o_orderkey = t1.l_orderkey
        )
      INNER JOIN "customer" AS t3
        ON (
          t3.c_custkey = t2.o_custkey
        )
      INNER JOIN "nation" AS t4
        ON (
          t0.s_nationkey = t4.n_nationkey
        )
      INNER JOIN "nation" AS t5
        ON (
          t3.c_nationkey = t5.n_nationkey
        )
    ) AS t11
    WHERE
      (
        (
          (
            t11.cust_nation = 'FRANCE'
          ) AND (
            t11.supp_nation = 'GERMANY'
          )
        )
        OR (
          (
            t11.cust_nation = 'GERMANY'
          ) AND (
            t11.supp_nation = 'FRANCE'
          )
        )
      )
      AND t11.l_shipdate BETWEEN MAKE_DATE(1995, 1, 1) AND MAKE_DATE(1996, 12, 31)
  ) AS t12
  GROUP BY
    1,
    2,
    3
) AS t13
ORDER BY
  t13.supp_nation ASC,
  t13.cust_nation ASC,
  t13.l_year ASC