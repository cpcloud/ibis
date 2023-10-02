WITH t0 AS (
  SELECT t3.*, t1.`r_name` AS `region`, t4.`o_totalprice` AS `amount`,
         CAST(t4.`o_orderdate` AS timestamp) AS `odate`
  FROM t0
)
SELECT t0.*
FROM t0
WHERE t0.`amount` > (
  SELECT avg(t5.`amount`) AS `Mean(amount)`
  FROM t0 t5
  WHERE t5.`region` = t0.`region`
)
LIMIT 10