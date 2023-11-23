SELECT
  *
FROM (
  SELECT
    t13.s_name AS s_name,
    t13.s_address AS s_address
  FROM (
    SELECT
      *
    FROM (
      SELECT
        t0.s_suppkey AS s_suppkey,
        t0.s_name AS s_name,
        t0.s_address AS s_address,
        t0.s_nationkey AS s_nationkey,
        t0.s_phone AS s_phone,
        t0.s_acctbal AS s_acctbal,
        t0.s_comment AS s_comment,
        t1.n_nationkey AS n_nationkey,
        t1.n_name AS n_name,
        t1.n_regionkey AS n_regionkey,
        t1.n_comment AS n_comment
      FROM "supplier" AS t0
      INNER JOIN "nation" AS t1
        ON (
          t0.s_nationkey = t1.n_nationkey
        )
    ) AS t8
    WHERE
      (
        t8.n_name = 'CANADA'
      )
      AND t8.s_suppkey IN ((
        SELECT
          t11.ps_suppkey AS ps_suppkey
        FROM (
          SELECT
            *
          FROM "partsupp" AS t2
          WHERE
            t2.ps_partkey IN ((
              SELECT
                t6.p_partkey AS p_partkey
              FROM (
                SELECT
                  *
                FROM "part" AS t3
                WHERE
                  t3.p_name LIKE 'forest%'
              ) AS t6
            ))
            AND (
              t2.ps_availqty > (
                (
                  SELECT
                    SUM(t7.l_quantity) AS "Sum(l_quantity)"
                  FROM (
                    SELECT
                      *
                    FROM "lineitem" AS t4
                    WHERE
                      (
                        t4.l_partkey = t2.ps_partkey
                      )
                      AND (
                        t4.l_suppkey = t2.ps_suppkey
                      )
                      AND (
                        t4.l_shipdate >= MAKE_DATE(1994, 1, 1)
                      )
                      AND (
                        t4.l_shipdate < MAKE_DATE(1995, 1, 1)
                      )
                  ) AS t7
                ) * CAST(0.5 AS DOUBLE)
              )
            )
        ) AS t11
      ))
  ) AS t13
) AS t14
ORDER BY
  t14.s_name ASC