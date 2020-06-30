-- Top 10 most expensive queries, last 7 days.
SELECT
  job_id,
  user_email,
  start_time,
  end_time,
  UNIX_MILLIS(end_time) - UNIX_MILLIS(start_time) runtime_millis,
  query,
  CASE
    WHEN error_result IS NOT NULL THEN TRUE
  ELSE
  FALSE
END
  query_failed,
  CASE
    WHEN statement_type = 'CREATE_MODEL' THEN ROUND((total_bytes_processed / POW(2,40)) * CAST(250.00 AS NUMERIC), 2)
    WHEN statement_type IN ('DELETE',
    'SELECT',
    'CREATE_TABLE_AS_SELECT',
    'INSERT',
    'MERGE') THEN ROUND((total_bytes_processed / POW(2,40)) * CAST(5.00 AS NUMERIC), 2)
    WHEN statement_type IS NULL THEN 0
END
  estimated_on_demand_cost,
  total_bytes_processed / POW(2,40) total_tibibytes_processed,
  total_slot_ms
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  job_type="QUERY"
  AND creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND CURRENT_TIMESTAMP()
ORDER BY estimated_on_demand_cost DESC
LIMIT 10
