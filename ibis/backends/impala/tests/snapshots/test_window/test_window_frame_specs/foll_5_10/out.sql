SELECT
  SUM(`t0`.`d`) OVER (ORDER BY `t0`.`f` ASC ROWS BETWEEN 5 following AND 10 following) AS `foo`
FROM `alltypes` AS `t0`