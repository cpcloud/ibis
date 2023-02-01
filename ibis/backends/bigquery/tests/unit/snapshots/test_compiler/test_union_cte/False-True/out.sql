WITH `anon_3` AS 
(SELECT `t0`.`string_col` AS `string_col`, sum(`t0`.`double_col`) AS `metric` 
FROM `functional_alltypes` AS `t0` GROUP BY 1), 
`anon_4` AS 
(SELECT `t0`.`string_col` AS `string_col`, sum(`t0`.`double_col`) AS `metric` 
FROM `functional_alltypes` AS `t0` GROUP BY 1), 
`anon_5` AS 
(SELECT `t0`.`string_col` AS `string_col`, sum(`t0`.`double_col`) AS `metric` 
FROM `functional_alltypes` AS `t0` GROUP BY 1)
 SELECT `anon_1`.`string_col`, `anon_1`.`metric` 
FROM (SELECT `anon_2`.`string_col` AS `string_col`, `anon_2`.`metric` AS `metric` 
FROM (SELECT `anon_3`.`string_col` AS `string_col`, `anon_3`.`metric` AS `metric` 
FROM `anon_3`) AS `anon_2` UNION DISTINCT SELECT `anon_4`.`string_col` AS `string_col`, `anon_4`.`metric` AS `metric` 
FROM `anon_4`) AS `anon_1` UNION ALL SELECT `anon_5`.`string_col`, `anon_5`.`metric` 
FROM `anon_5`