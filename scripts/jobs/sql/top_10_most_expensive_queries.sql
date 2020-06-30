--   Copyright 2020 Google LLC
--
--   Licensed under the Apache License, Version 2.0 (the "License");
--   you may not use this file except in compliance with the License.
--   You may obtain a copy of the License at
--
--       http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.

-- Top 10 most expensive queries, last 7 days.
SELECT
  job_id,
  user_email,
  start_time,
  end_time,
  UNIX_MILLIS(end_time) - UNIX_MILLIS(start_time) AS runtime_millis,
  query,
  CASE
    WHEN
      error_result IS NOT NULL THEN TRUE
    ELSE
      FALSE
  END AS has_error,
  CASE
    WHEN statement_type = 'CREATE_MODEL'
      THEN ROUND((total_bytes_processed / POW(2,40)) * CAST(250.00 AS NUMERIC), 2)
    WHEN statement_type IN ('DELETE', 'SELECT', 'CREATE_TABLE_AS_SELECT', 'INSERT', 'MERGE')
      THEN ROUND((total_bytes_processed / POW(2,40)) * CAST(5.00 AS NUMERIC), 2)
    WHEN statement_type IS NULL THEN 0
  END AS estimated_on_demand_cost,
  total_bytes_processed / POW(2,40) AS total_tebibytes_processed,
  total_slot_ms
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) AND CURRENT_TIMESTAMP()
  AND job_type = "QUERY"
