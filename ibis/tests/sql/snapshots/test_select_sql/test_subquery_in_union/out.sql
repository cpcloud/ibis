WITH t0 AS (
  SELECT t3.`a`, t3.`g`, sum(t3.`f`) AS `metric`
  FROM alltypes t3
  GROUP BY 1, 2
),
t1 AS (
  SELECT t0.*
  FROM t1
)
SELECT t2.`a`, t2.`g`, t2.`metric`
FROM (
  WITH t0 AS (
    SELECT t3.`a`, t3.`g`, sum(t3.`f`) AS `metric`
    FROM alltypes t3
    GROUP BY 1, 2
  ),
  t1 AS (
    SELECT t0.*
    FROM t1
  )
  SELECT *
  FROM t1
  UNION ALL
  SELECT t0.*
  FROM t1
) t2