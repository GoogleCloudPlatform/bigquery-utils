CREATE OR REPLACE TABLE  `bqtestdataset.job_stats_nonpartitioned`
(
  id INT64,
  status STRING,
  creation_ts TIMESTAMP,
  updation_ts TIMESTAMP
);