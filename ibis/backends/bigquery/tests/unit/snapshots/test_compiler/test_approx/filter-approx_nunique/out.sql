SELECT approx_count_distinct(CASE WHEN (`t0`.`month` > 0) THEN `t0`.`double_col` ELSE NULL END) AS `ApproxCountDistinct_double_col_Greater_month_0_` 
FROM `functional_alltypes` AS `t0`