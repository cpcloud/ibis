WITH `t0` AS 
(SELECT `t4`.`file_date` AS `file_date`, `t4`.`PARTITIONTIME` AS `PARTITIONTIME`, `t4`.`val` AS `val` 
FROM `unbound_table` AS `t4` 
WHERE `t4`.`PARTITIONTIME` < DATE '2017-01-01'), 
`t1` AS 
(SELECT CAST(`t0`.`file_date` AS DATE) AS `file_date`, `t0`.`PARTITIONTIME` AS `PARTITIONTIME`, `t0`.`val` AS `val` 
FROM `t0` 
WHERE `t0`.`file_date` < DATE '2017-01-01'), 
`t2` AS 
(SELECT `t1`.`file_date` AS `file_date`, `t1`.`PARTITIONTIME` AS `PARTITIONTIME`, `t1`.`val` AS `val`, `t1`.`val` * 2 AS `XYZ` 
FROM `t1`)
 SELECT `t2`.`file_date`, `t2`.`PARTITIONTIME`, `t2`.`val`, `t2`.`XYZ` 
FROM `t2` JOIN `t2` AS t3 ON true