CREATE OR REPLACE TABLE  `bqtestdataset.job_stats_partitioned`
(
 id INT64,
 status STRING,
 creation_ts TIMESTAMP,
 updation_ts TIMESTAMP
)
PARTITION BY DATE(creation_ts);