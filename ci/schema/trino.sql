DROP TABLE IF EXISTS map;
CREATE TABLE map (idx BIGINT, kv MAP<VARCHAR, BIGINT>);
INSERT INTO map VALUES
    (1, MAP(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3])),
    (2, MAP(ARRAY['d', 'e', 'f'], ARRAY[4, 5, 6]));

DROP TABLE IF EXISTS struct;
CREATE TABLE struct (abc ROW(a DOUBLE, b VARCHAR, c BIGINT));
INSERT INTO struct
    SELECT ROW(1.0, 'banana', 2) UNION
    SELECT ROW(2.0, 'apple', 3) UNION
    SELECT ROW(3.0, 'orange', 4) UNION
    SELECT ROW(NULL, 'banana', 2) UNION
    SELECT ROW(2.0, NULL, 3) UNION
    SELECT NULL UNION
    SELECT ROW(3.0, 'orange', NULL);
