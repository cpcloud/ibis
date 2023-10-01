WITH t1 AS (
  SELECT
    t4.p_partkey AS p_partkey,
    t4.p_name AS p_name,
    t4.p_mfgr AS p_mfgr,
    t4.p_brand AS p_brand,
    t4.p_type AS p_type,
    t4.p_size AS p_size,
    t4.p_container AS p_container,
    t4.p_retailprice AS p_retailprice,
    t4.p_comment AS p_comment,
    t5.ps_partkey AS ps_partkey,
    t5.ps_suppkey AS ps_suppkey,
    t5.ps_availqty AS ps_availqty,
    t5.ps_supplycost AS ps_supplycost,
    t5.ps_comment AS ps_comment,
    t6.s_suppkey AS s_suppkey,
    t6.s_name AS s_name,
    t6.s_address AS s_address,
    t6.s_nationkey AS s_nationkey,
    t6.s_phone AS s_phone,
    t6.s_acctbal AS s_acctbal,
    t6.s_comment AS s_comment,
    t7.n_nationkey AS n_nationkey,
    t7.n_name AS n_name,
    t7.n_regionkey AS n_regionkey,
    t7.n_comment AS n_comment,
    t8.r_regionkey AS r_regionkey,
    t8.r_name AS r_name,
    t8.r_comment AS r_comment
  FROM main.part AS t4
  JOIN main.partsupp AS t5
    ON t4.p_partkey = t5.ps_partkey
  JOIN main.supplier AS t6
    ON t6.s_suppkey = t5.ps_suppkey
  JOIN main.nation AS t7
    ON t6.s_nationkey = t7.n_nationkey
  JOIN main.region AS t8
    ON t7.n_regionkey = t8.r_regionkey
), t0 AS (
  SELECT
    t4.ps_partkey AS ps_partkey,
    t4.ps_suppkey AS ps_suppkey,
    t4.ps_availqty AS ps_availqty,
    t4.ps_supplycost AS ps_supplycost,
    t4.ps_comment AS ps_comment,
    t5.s_suppkey AS s_suppkey,
    t5.s_name AS s_name,
    t5.s_address AS s_address,
    t5.s_nationkey AS s_nationkey,
    t5.s_phone AS s_phone,
    t5.s_acctbal AS s_acctbal,
    t5.s_comment AS s_comment,
    t6.n_nationkey AS n_nationkey,
    t6.n_name AS n_name,
    t6.n_regionkey AS n_regionkey,
    t6.n_comment AS n_comment,
    t7.r_regionkey AS r_regionkey,
    t7.r_name AS r_name,
    t7.r_comment AS r_comment
  FROM main.partsupp AS t4
  JOIN main.supplier AS t5
    ON t5.s_suppkey = t4.ps_suppkey
  JOIN main.nation AS t6
    ON t5.s_nationkey = t6.n_nationkey
  JOIN main.region AS t7
    ON t6.n_regionkey = t7.r_regionkey
), t2 AS (
  SELECT
    t1.p_partkey AS p_partkey,
    t1.p_name AS p_name,
    t1.p_mfgr AS p_mfgr,
    t1.p_brand AS p_brand,
    t1.p_type AS p_type,
    t1.p_size AS p_size,
    t1.p_container AS p_container,
    t1.p_retailprice AS p_retailprice,
    t1.p_comment AS p_comment,
    t1.ps_partkey AS ps_partkey,
    t1.ps_suppkey AS ps_suppkey,
    t1.ps_availqty AS ps_availqty,
    t1.ps_supplycost AS ps_supplycost,
    t1.ps_comment AS ps_comment,
    t1.s_suppkey AS s_suppkey,
    t1.s_name AS s_name,
    t1.s_address AS s_address,
    t1.s_nationkey AS s_nationkey,
    t1.s_phone AS s_phone,
    t1.s_acctbal AS s_acctbal,
    t1.s_comment AS s_comment,
    t1.n_nationkey AS n_nationkey,
    t1.n_name AS n_name,
    t1.n_regionkey AS n_regionkey,
    t1.n_comment AS n_comment,
    t1.r_regionkey AS r_regionkey,
    t1.r_name AS r_name,
    t1.r_comment AS r_comment
  FROM t1
  WHERE
    t1.p_size = CAST(15 AS TINYINT)
    AND t1.p_type LIKE '%BRASS'
    AND t1.r_name = 'EUROPE'
    AND t1.ps_supplycost = (
      SELECT
        MIN(t0.ps_supplycost) AS "Min(ps_supplycost)"
      FROM t0
      WHERE
        t0.r_name = 'EUROPE' AND t1.p_partkey = t0.ps_partkey
    )
)
SELECT
  t3.s_acctbal,
  t3.s_name,
  t3.n_name,
  t3.p_partkey,
  t3.p_mfgr,
  t3.s_address,
  t3.s_phone,
  t3.s_comment
FROM (
  SELECT
    t2.s_acctbal AS s_acctbal,
    t2.s_name AS s_name,
    t2.n_name AS n_name,
    t2.p_partkey AS p_partkey,
    t2.p_mfgr AS p_mfgr,
    t2.s_address AS s_address,
    t2.s_phone AS s_phone,
    t2.s_comment AS s_comment
  FROM t2
) AS t3
ORDER BY
  t3.s_acctbal DESC,
  t3.n_name ASC,
  t3.s_name ASC,
  t3.p_partkey ASC
LIMIT 100