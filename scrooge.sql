--  CREATE OR REPLACE TABLE ticks AS SELECT * FROM read_csv_auto('./docs/tutorial/tutorial_sample_tick.csv');
SELECT
    bucket,
    ARG_MIN(price, time) AS open,
    MAX(price) AS high,
    MIN(price) AS low,
    ARG_MAX(price, time) as close
FROM UNNEST(
    GENERATE_SERIES(
        (SELECT min(time) FROM ticks),
        (SELECT max(time) FROM ticks),
        '1M'::INTERVAL
    )
) AS buckets(bucket)
LEFT JOIN ticks
  ON time >= bucket AND time < bucket + '1M'::INTERVAL
WHERE symbol = 'BTC/USD'
  AND time BETWEEN '2022-04-10' AND '2022-04-17'
GROUP BY bucket
ORDER BY bucket
LIMIT 5
