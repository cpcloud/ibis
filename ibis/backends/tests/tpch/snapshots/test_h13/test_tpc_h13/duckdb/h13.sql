WITH t0 AS (
  SELECT
    t3.c_custkey AS c_custkey,
    t3.c_name AS c_name,
    t3.c_address AS c_address,
    t3.c_nationkey AS c_nationkey,
    t3.c_phone AS c_phone,
    t3.c_acctbal AS c_acctbal,
    t3.c_mktsegment AS c_mktsegment,
    t3.c_comment AS c_comment,
    t4.o_orderkey AS o_orderkey,
    t4.o_custkey AS o_custkey,
    t4.o_orderstatus AS o_orderstatus,
    t4.o_totalprice AS o_totalprice,
    t4.o_orderdate AS o_orderdate,
    t4.o_orderpriority AS o_orderpriority,
    t4.o_clerk AS o_clerk,
    t4.o_shippriority AS o_shippriority,
    t4.o_comment AS o_comment
  FROM main.customer AS t3
  LEFT OUTER JOIN main.orders AS t4
    ON t3.c_custkey = t4.o_custkey AND NOT t4.o_comment LIKE '%special%requests%'
), t1 AS (
  SELECT
    t0.c_custkey AS c_custkey,
    COUNT(t0.o_orderkey) AS c_count
  FROM t0
  GROUP BY
    1
)
SELECT
  t2.c_count,
  t2.custdist
FROM (
  SELECT
    t1.c_count AS c_count,
    COUNT(*) AS custdist
  FROM t1
  GROUP BY
    1
) AS t2
ORDER BY
  t2.custdist DESC,
  t2.c_count DESC