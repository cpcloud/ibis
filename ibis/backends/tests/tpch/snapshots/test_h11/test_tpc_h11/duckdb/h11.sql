WITH t0 AS (
  SELECT
    t3.ps_partkey AS ps_partkey,
    t3.ps_suppkey AS ps_suppkey,
    t3.ps_availqty AS ps_availqty,
    t3.ps_supplycost AS ps_supplycost,
    t3.ps_comment AS ps_comment,
    t4.s_suppkey AS s_suppkey,
    t4.s_name AS s_name,
    t4.s_address AS s_address,
    t4.s_nationkey AS s_nationkey,
    t4.s_phone AS s_phone,
    t4.s_acctbal AS s_acctbal,
    t4.s_comment AS s_comment,
    t5.n_nationkey AS n_nationkey,
    t5.n_name AS n_name,
    t5.n_regionkey AS n_regionkey,
    t5.n_comment AS n_comment
  FROM main.partsupp AS t3
  JOIN main.supplier AS t4
    ON t3.ps_suppkey = t4.s_suppkey
  JOIN main.nation AS t5
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
        SUM(t0.ps_supplycost * t0.ps_availqty) AS total
      FROM t0
      WHERE
        t0.n_name = 'GERMANY'
    ) * CAST(0.0001 AS REAL(53))
) AS t2
ORDER BY
  t2.value DESC