SELECT bit_or(CASE WHEN (`t0`.`bigint_col` > 0) THEN `t0`.`int_col` ELSE NULL END) AS `BitOr_int_col_Greater_bigint_col_0_` 
FROM `functional_alltypes` AS `t0`