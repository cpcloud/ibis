WITH t0 AS (
  SELECT
    t1.l_partkey AS l_partkey,
    t1.l_quantity AS l_quantity,
    t1.l_extendedprice AS l_extendedprice,
    t2.p_partkey AS p_partkey,
    t2.p_brand AS p_brand,
    t2.p_container AS p_container
  FROM lineitem AS t1
  JOIN part AS t2
    ON t2.p_partkey = t1.l_partkey
)
SELECT
  SUM(t0.l_extendedprice) / 7 AS avg_yearly
FROM t0
WHERE
  t0.p_brand = 'Brand#23'
  AND t0.p_container = 'MED BOX'
  AND t0.l_quantity < (
    SELECT
      AVG(t1.l_quantity) AS "Mean(l_quantity)"
    FROM lineitem AS t1
    WHERE
      t1.l_partkey = t0.p_partkey
  ) * 0.2