SELECT
  t8.s_suppkey AS s_suppkey,
  t8.s_name AS s_name,
  t8.s_address AS s_address,
  t8.s_phone AS s_phone,
  t8.total_revenue AS total_revenue
FROM (
  SELECT
    *
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
        t3.l_suppkey AS l_suppkey,
        t3.total_revenue AS total_revenue
      FROM "supplier" AS t0
      INNER JOIN (
        SELECT
          t2.l_suppkey AS l_suppkey,
          SUM((
            t2.l_extendedprice * (
              CAST(1 AS TINYINT) - t2.l_discount
            )
          )) AS total_revenue
        FROM (
          SELECT
            *
          FROM "lineitem" AS t1
          WHERE
            (
              t1.l_shipdate >= MAKE_DATE(1996, 1, 1)
            )
            AND (
              t1.l_shipdate < MAKE_DATE(1996, 4, 1)
            )
        ) AS t2
        GROUP BY
          1
      ) AS t3
        ON (
          t0.s_suppkey = t3.l_suppkey
        )
    ) AS t5
    WHERE
      (
        t5.total_revenue = (
          SELECT
            MAX(t5.total_revenue) AS "Max(total_revenue)"
          FROM (
            SELECT
              t0.s_suppkey AS s_suppkey,
              t0.s_name AS s_name,
              t0.s_address AS s_address,
              t0.s_nationkey AS s_nationkey,
              t0.s_phone AS s_phone,
              t0.s_acctbal AS s_acctbal,
              t0.s_comment AS s_comment,
              t3.l_suppkey AS l_suppkey,
              t3.total_revenue AS total_revenue
            FROM "supplier" AS t0
            INNER JOIN (
              SELECT
                t2.l_suppkey AS l_suppkey,
                SUM((
                  t2.l_extendedprice * (
                    CAST(1 AS TINYINT) - t2.l_discount
                  )
                )) AS total_revenue
              FROM (
                SELECT
                  *
                FROM "lineitem" AS t1
                WHERE
                  (
                    t1.l_shipdate >= MAKE_DATE(1996, 1, 1)
                  )
                  AND (
                    t1.l_shipdate < MAKE_DATE(1996, 4, 1)
                  )
              ) AS t2
              GROUP BY
                1
            ) AS t3
              ON (
                t0.s_suppkey = t3.l_suppkey
              )
          ) AS t5
        )
      )
  ) AS t7
  ORDER BY
    t7.s_suppkey ASC
) AS t8