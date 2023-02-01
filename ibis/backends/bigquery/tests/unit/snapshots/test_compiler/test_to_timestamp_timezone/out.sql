SELECT parse_timestamp('%%F %%Z', concat(`t0`.`date_string_col`, ' America/New_York')) AS `StringToTimestamp_StringConcat_F_Z_` 
FROM `functional_alltypes` AS `t0`