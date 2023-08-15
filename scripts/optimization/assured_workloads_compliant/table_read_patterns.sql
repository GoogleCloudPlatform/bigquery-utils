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
 * The following script creates a table named, table_read_patterns,
 * that contains a list of the most frequently read tables within the
 * past 30 days.
 * 30 days is the default timeframe, but you can change this by setting the
 * num_days_to_scan variable to a different value.
 */

DECLARE num_days_to_scan INT64 DEFAULT 30;

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.table_read_patterns
CLUSTER BY project_id, dataset_id, table_id AS
SELECT
  DATE(creation_time) AS date,
  jbo.project_id,
  table.dataset_id,
  table.table_id,
  table.project_id || '.' || table.dataset_id || '.' || table.table_id AS full_table_id,
  job_id,
  bqutil.fn.job_url(jbo.project_id || ':us.' || job_id) AS job_url,
  parent_job_id,
  bqutil.fn.job_url(jbo.project_id || ':us.' || parent_job_id) AS parent_job_url,
  reservation_id,
  total_bytes_billed,
  total_slot_ms,
  creation_time,
  start_time,
  end_time
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION jbo,
UNNEST(referenced_tables) table
WHERE
  DATE(creation_time) >= CURRENT_DATE - num_days_to_scan
  AND (
    table.project_id||table.dataset_id||table.table_id
    <> destination_table.project_id||destination_table.dataset_id||destination_table.table_id
  )
  AND job_type = 'QUERY'
  AND statement_type != 'SCRIPT'
  AND NOT cache_hit
  AND error_result IS NULL;
