-- NB: The paths in this file are all relative to /var/lib/clickhouse/user_files
CREATE OR REPLACE TABLE ibis_testing.diamonds ENGINE = Memory AS
SELECT * FROM file('ci/ibis-testing-data/parquet/diamonds.parquet', 'Parquet');

CREATE OR REPLACE TABLE ibis_testing.batting ENGINE = Memory AS
SELECT * FROM file('ci/ibis-testing-data/parquet/batting.parquet', 'Parquet');

CREATE OR REPLACE TABLE ibis_testing.awards_players ENGINE = Memory AS
SELECT * FROM file('ci/ibis-testing-data/parquet/awards_players.parquet', 'Parquet');

CREATE OR REPLACE TABLE ibis_testing.functional_alltypes ENGINE = Memory AS
SELECT * REPLACE(CAST(timestamp_col AS Nullable(DateTime)) AS timestamp_col)
FROM file('ci/ibis-testing-data/parquet/functional_alltypes.parquet', 'Parquet');

CREATE OR REPLACE TABLE ibis_testing.astronauts ENGINE = Memory AS
SELECT * FROM file('ci/ibis-testing-data/parquet/astronauts.parquet', 'Parquet');
