SELECT bit_and(CASE WHEN (`t0`.`bigint_col` > 0) THEN `t0`.`int_col` ELSE NULL END) AS `BitAnd_int_col_Greater_bigint_col_0_` 
FROM `functional_alltypes` AS `t0`