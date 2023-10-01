WITH t0 AS (
  SELECT
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
    t4.n_comment AS n_comment
  FROM main.supplier AS t3
  JOIN main.nation AS t4
    ON t3.s_nationkey = t4.n_nationkey
), t1 AS (
  SELECT
    t0.s_suppkey AS s_suppkey,
    t0.s_name AS s_name,
    t0.s_address AS s_address,
    t0.s_nationkey AS s_nationkey,
    t0.s_phone AS s_phone,
    t0.s_acctbal AS s_acctbal,
    t0.s_comment AS s_comment,
    t0.n_nationkey AS n_nationkey,
    t0.n_name AS n_name,
    t0.n_regionkey AS n_regionkey,
    t0.n_comment AS n_comment
  FROM t0
  WHERE
    t0.n_name = 'CANADA'
    AND t0.s_suppkey IN (
      SELECT
        t5.ps_suppkey
      FROM (
        SELECT
          t6.ps_partkey AS ps_partkey,
          t6.ps_suppkey AS ps_suppkey,
          t6.ps_availqty AS ps_availqty,
          t6.ps_supplycost AS ps_supplycost,
          t6.ps_comment AS ps_comment
        FROM main.partsupp AS t6
        WHERE
          t6.ps_partkey IN (
            SELECT
              t7.p_partkey
            FROM (
              SELECT
                t8.p_partkey AS p_partkey,
                t8.p_name AS p_name,
                t8.p_mfgr AS p_mfgr,
                t8.p_brand AS p_brand,
                t8.p_type AS p_type,
                t8.p_size AS p_size,
                t8.p_container AS p_container,
                t8.p_retailprice AS p_retailprice,
                t8.p_comment AS p_comment
              FROM main.part AS t8
              WHERE
                t8.p_name LIKE 'forest%'
            ) AS t7
          )
          AND t6.ps_availqty > (
            SELECT
              SUM(t8.l_quantity) AS "Sum(l_quantity)"
            FROM main.lineitem AS t8
            WHERE
              t8.l_partkey = t6.ps_partkey
              AND t8.l_suppkey = t6.ps_suppkey
              AND t8.l_shipdate >= CAST('1994-01-01' AS DATE)
              AND t8.l_shipdate < CAST('1995-01-01' AS DATE)
          ) * CAST(0.5 AS REAL(53))
      ) AS t5
    )
)
SELECT
  t2.s_name,
  t2.s_address
FROM (
  SELECT
    t1.s_name AS s_name,
    t1.s_address AS s_address
  FROM t1
) AS t2
ORDER BY
  t2.s_name ASC