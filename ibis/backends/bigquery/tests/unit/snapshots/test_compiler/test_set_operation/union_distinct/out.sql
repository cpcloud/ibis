WITH `anon_1` AS 
(SELECT `t0`.`a` AS `a` 
FROM `t0` AS `t0`), 
`anon_2` AS 
(SELECT `t0`.`a` AS `a` 
FROM `t1` AS `t0`)
 SELECT `anon_1`.`a` 
FROM `anon_1` UNION DISTINCT SELECT `anon_2`.`a` 
FROM `anon_2`