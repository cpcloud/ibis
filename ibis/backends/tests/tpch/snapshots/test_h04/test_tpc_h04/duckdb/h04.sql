SELECT
  *
FROM (
  SELECT
    t4.o_orderpriority AS o_orderpriority,
    COUNT(*) AS order_count
  FROM (
    SELECT
      *
    FROM "orders" AS t0
    WHERE
      EXISTS(
        (
          SELECT
            CAST(1 AS TINYINT) AS "1"
          FROM (
            SELECT
              *
            FROM "lineitem" AS t1
            WHERE
              (
                (
                  t1.l_orderkey = t0.o_orderkey
                ) AND (
                  t1.l_commitdate < t1.l_receiptdate
                )
              )
          ) AS t2
        )
      )
      AND (
        t0.o_orderdate >= MAKE_DATE(1993, 7, 1)
      )
      AND (
        t0.o_orderdate < MAKE_DATE(1993, 10, 1)
      )
  ) AS t4
  GROUP BY
    1
) AS t5
ORDER BY
  t5.o_orderpriority ASC