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
 * This script creates a table named, table_read_patterns_by_query_hash
 */

DECLARE num_days_to_scan INT64 DEFAULT 30;

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.table_read_patterns_by_query_hash
CLUSTER BY project_id, dataset_id, table_id AS
SELECT
  project_id,
  dataset_id,
  table_id,
  COUNT(query_pattern) AS num_query_patterns,
  ARRAY_AGG(
    STRUCT(
      query_pattern,
      top_10_slot_ms_jobs, 
      avg_slot_hours,
      days_active,
      job_count,
      total_gigabytes_processed,
      avg_gigabytes_processed,
      total_slot_hours,
      avg_total_slot_hours_per_active_day,
      avg_job_duration_seconds,
      total_slots,
      avg_total_slots
      )
    ORDER BY avg_slot_hours * days_active * job_count DESC LIMIT 10
  ) AS top_10_slot_ms_patterns,
  MAX(days_active) AS max_days_active,
  MAX(avg_slot_hours) AS max_avg_slot_hours_across_all_patterns,
  MAX(days_active * avg_slot_hours) AS max_weighted_avg_slot_hours,
FROM(
  SELECT
    referenced_table.project_id,
    referenced_table.dataset_id,
    referenced_table.table_id,
    query_info.query_hashes.normalized_literals                              AS query_pattern,
    ARRAY_AGG(STRUCT(
      bqutil.fn.job_url(jbo.project_id || ':us.' || job_id) AS job_url, 
      bqutil.fn.job_url(jbo.project_id || ':us.' || parent_job_id) AS parent_job_url
      )
      ORDER BY total_slot_ms DESC LIMIT 10
    )                                                                        AS top_10_slot_ms_jobs,
    COUNT(DISTINCT DATE(start_time))                                         AS days_active,
    ARRAY_AGG(DISTINCT jbo.project_id IGNORE NULLS)                          AS project_ids,
    ARRAY_AGG(DISTINCT reservation_id IGNORE NULLS)                          AS reservation_ids,
    COUNT(DISTINCT job_Id)                                                   AS job_count,
    ARRAY_AGG(DISTINCT user_email)                                           AS user_emails,
    SUM(total_bytes_processed) / POW(1024, 3)                                AS total_gigabytes_processed,
    AVG(total_bytes_processed) / POW(1024, 3)                                AS avg_gigabytes_processed,
    SUM(total_slot_ms) / (1000 * 60 * 60)                                    AS total_slot_hours,
    AVG(total_slot_ms) / (1000 * 60 * 60)                                    AS avg_slot_hours,
    SUM(total_slot_ms) / (1000 * 60 * 60) / COUNT(DISTINCT DATE(start_time)) AS avg_total_slot_hours_per_active_day,
    AVG(TIMESTAMP_DIFF(end_time, start_time, SECOND) )                       AS avg_job_duration_seconds,
    SUM(total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND))   AS total_slots,
    AVG(total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND))   AS avg_total_slots,
  FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION jbo, UNNEST(referenced_tables) referenced_table
  WHERE
    DATE(start_time) >= CURRENT_DATE - num_days_to_scan
    AND (
      referenced_table.project_id||referenced_table.dataset_id||referenced_table.table_id
      <> destination_table.project_id||destination_table.dataset_id||destination_table.table_id
    )
    AND state = 'DONE'
    AND error_result IS NULL
    AND job_type = 'QUERY'
    AND statement_type != 'SCRIPT'
    AND referenced_table.table_id NOT LIKE '%INFORMATION_SCHEMA%'
    AND user_email LIKE '%gserviceaccount.com'
  GROUP BY 1,2,3,4
) GROUP BY 1,2,3;
