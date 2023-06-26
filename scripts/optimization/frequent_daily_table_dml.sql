/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * The following script will return the top 10 tables that 
 * have had the most DML statements run against them in the past 30 days.
 */

SELECT
  EXTRACT(DATE FROM creation_time) AS creation_date,
  COUNT(1) AS dml_per_table,
  destination_table.project_id || '.' || destination_table.dataset_id || '.' || destination_table.table_id AS table_id,
  bqutil.fn.table_url(destination_table.project_id || '.' || destination_table.dataset_id || '.' || destination_table.table_id) AS table_url,
  ARRAY_AGG(job_id) AS job_ids,
  ARRAY_AGG(bqutil.fn.job_url(project_id || ':us.' || job_id) IGNORE NULLS) AS job_urls,
  statement_type,
  SUM(SAFE_DIVIDE(total_bytes_processed, pow(2,30))) AS sum_total_gb_processed,
  AVG(SAFE_DIVIDE(total_bytes_processed, pow(2,30))) AS avg_total_gb_processed,
  SUM(total_slot_ms) AS sum_total_slot_ms,
  AVG(total_slot_ms) AS avg_total_slot_ms,
  SUM(SAFE_DIVIDE(total_slot_ms, (TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)))) AS sum_avg_slots,
  AVG(SAFE_DIVIDE(total_slot_ms, (TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)))) AS avg_avg_slots,
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
WHERE 1=1 -- no op filter to allow easy commenting below
-- Look at the past 30 days of jobs
AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
-- Only look at DML statements
AND statement_type IN ('INSERT', 'UPDATE', 'DELETE', 'MERGE')
GROUP BY creation_date, table_id, table_url, statement_type
ORDER BY dml_per_table DESC
LIMIT 10