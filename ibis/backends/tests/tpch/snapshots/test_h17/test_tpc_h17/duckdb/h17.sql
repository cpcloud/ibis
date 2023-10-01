WITH t0 AS (
  SELECT
    t1.l_orderkey AS l_orderkey,
    t1.l_partkey AS l_partkey,
    t1.l_suppkey AS l_suppkey,
    t1.l_linenumber AS l_linenumber,
    t1.l_quantity AS l_quantity,
    t1.l_extendedprice AS l_extendedprice,
    t1.l_discount AS l_discount,
    t1.l_tax AS l_tax,
    t1.l_returnflag AS l_returnflag,
    t1.l_linestatus AS l_linestatus,
    t1.l_shipdate AS l_shipdate,
    t1.l_commitdate AS l_commitdate,
    t1.l_receiptdate AS l_receiptdate,
    t1.l_shipinstruct AS l_shipinstruct,
    t1.l_shipmode AS l_shipmode,
    t1.l_comment AS l_comment,
    t2.p_partkey AS p_partkey,
    t2.p_name AS p_name,
    t2.p_mfgr AS p_mfgr,
    t2.p_brand AS p_brand,
    t2.p_type AS p_type,
    t2.p_size AS p_size,
    t2.p_container AS p_container,
    t2.p_retailprice AS p_retailprice,
    t2.p_comment AS p_comment
  FROM main.lineitem AS t1
  JOIN main.part AS t2
    ON t2.p_partkey = t1.l_partkey
)
SELECT
  SUM(t0.l_extendedprice) / CAST(7.0 AS REAL(53)) AS avg_yearly
FROM t0
WHERE
  t0.p_brand = 'Brand#23'
  AND t0.p_container = 'MED BOX'
  AND t0.l_quantity < (
    SELECT
      AVG(t1.l_quantity) AS "Mean(l_quantity)"
    FROM main.lineitem AS t1
    WHERE
      t1.l_partkey = t0.p_partkey
  ) * CAST(0.2 AS REAL(53))