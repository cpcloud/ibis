SELECT avg(CASE WHEN (`t0`.`month` > 6) THEN CAST(`t0`.`bool_col` AS INT64) ELSE NULL END) AS `Mean_bool_col_Greater_month_6_` 
FROM `functional_alltypes` AS `t0`