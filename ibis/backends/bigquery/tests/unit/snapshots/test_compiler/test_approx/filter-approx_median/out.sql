SELECT (approx_quantiles(CASE WHEN (`t0`.`month` > 0) THEN `t0`.`double_col` ELSE NULL END, 2))[OFFSET(1)] AS `ApproxMedian_double_col_Greater_month_0_` 
FROM `functional_alltypes` AS `t0`