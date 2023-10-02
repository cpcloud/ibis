WITH t0 AS (
  SELECT
    t3.ps_partkey AS ps_partkey,
    t3.ps_suppkey AS ps_suppkey,
    t3.ps_availqty AS ps_availqty,
    t3.ps_supplycost AS ps_supplycost,
    t4.s_suppkey AS s_suppkey,
    t4.s_nationkey AS s_nationkey,
    t5.n_nationkey AS n_nationkey,
    t5.n_name AS n_name
  FROM partsupp AS t3
  JOIN supplier AS t4
    ON t3.ps_suppkey = t4.s_suppkey
  JOIN nation AS t5
    ON t5.n_nationkey = t4.s_nationkey
), t1 AS (
  SELECT
    t0.ps_partkey AS ps_partkey,
    SUM(t0.ps_supplycost * t0.ps_availqty) AS value
  FROM t0
  WHERE
    t0.n_name = 'GERMANY'
  GROUP BY
    1
)
SELECT
  t2.ps_partkey,
  t2.value
FROM (
  SELECT
    t1.ps_partkey AS ps_partkey,
    t1.value AS value
  FROM t1
  WHERE
    t1.value > (
      SELECT
        anon_1.total
      FROM (
        SELECT
          SUM(t0.ps_supplycost * t0.ps_availqty) AS total
        FROM t0
        WHERE
          t0.n_name = 'GERMANY'
      ) AS anon_1
    ) * 0.0001
) AS t2
ORDER BY
  t2.value DESC