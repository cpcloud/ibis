SELECT
  *
FROM (
  SELECT
    t17.o_year AS o_year,
    (
      SUM(t17.nation_volume) / SUM(t17.volume)
    ) AS mkt_share
  FROM (
    SELECT
      t16.o_year AS o_year,
      t16.volume AS volume,
      t16.nation AS nation,
      t16.r_name AS r_name,
      t16.o_orderdate AS o_orderdate,
      t16.p_type AS p_type,
      CASE WHEN (
        t16.nation = 'BRAZIL'
      ) THEN t16.volume ELSE CAST(0 AS TINYINT) END AS nation_volume
    FROM (
      SELECT
        *
      FROM (
        SELECT
          EXTRACT('year' FROM t3.o_orderdate) AS o_year,
          (
            t1.l_extendedprice * (
              CAST(1 AS TINYINT) - t1.l_discount
            )
          ) AS volume,
          t7.n_name AS nation,
          t6.r_name AS r_name,
          t3.o_orderdate AS o_orderdate,
          t0.p_type AS p_type
        FROM "part" AS t0
        INNER JOIN "lineitem" AS t1
          ON (
            t0.p_partkey = t1.l_partkey
          )
        INNER JOIN "supplier" AS t2
          ON (
            t2.s_suppkey = t1.l_suppkey
          )
        INNER JOIN "orders" AS t3
          ON (
            t1.l_orderkey = t3.o_orderkey
          )
        INNER JOIN "customer" AS t4
          ON (
            t3.o_custkey = t4.c_custkey
          )
        INNER JOIN "nation" AS t5
          ON (
            t4.c_nationkey = t5.n_nationkey
          )
        INNER JOIN "region" AS t6
          ON (
            t5.n_regionkey = t6.r_regionkey
          )
        INNER JOIN "nation" AS t7
          ON (
            t2.s_nationkey = t7.n_nationkey
          )
      ) AS t15
      WHERE
        (
          t15.r_name = 'AMERICA'
        )
        AND t15.o_orderdate BETWEEN MAKE_DATE(1995, 1, 1) AND MAKE_DATE(1996, 12, 31)
        AND (
          t15.p_type = 'ECONOMY ANODIZED STEEL'
        )
    ) AS t16
  ) AS t17
  GROUP BY
    1
) AS t18
ORDER BY
  t18.o_year ASC