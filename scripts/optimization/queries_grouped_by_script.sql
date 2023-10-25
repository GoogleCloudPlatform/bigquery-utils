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

DECLARE num_days_to_scan INT64 DEFAULT 30;

CREATE TEMP FUNCTION job_stage_max_slots(job_stages ANY TYPE) AS ((
  SELECT MAX(SAFE_DIVIDE(stage.slot_ms,stage.end_ms - stage.start_ms)) 
  FROM UNNEST(job_stages) stage
));
CREATE TEMP FUNCTION total_bytes_shuffled(job_stages ANY TYPE) AS ((
  SELECT SUM(stage.shuffle_output_bytes) 
  FROM UNNEST(job_stages) stage
));
CREATE TEMP FUNCTION total_shuffle_bytes_spilled(job_stages ANY TYPE) AS ((
  SELECT SUM(stage.shuffle_output_bytes_spilled) 
  FROM UNNEST(job_stages) stage
));

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.queries_grouped_by_script AS
SELECT * REPLACE((
  SELECT
    ARRAY_AGG(DISTINCT table.project_id || "." || table.dataset_id || "." || table.table_id)
  FROM UNNEST(referenced_tables) table
  ) AS referenced_tables)
FROM(
  SELECT
    bqutil.fn.job_url(
        parent.project_id || ':us.' || parent.job_id)              AS job_url,
    parent.user_email,
    parent.start_time,
    ANY_VALUE(parent.labels) AS labels,
    SUM(COALESCE(SAFE_DIVIDE(child.total_slot_ms, TIMESTAMP_DIFF(
      child.end_time, child.start_time, MILLISECOND)), 0))         AS total_slots,
    SUM(COALESCE(child.total_slot_ms, 0))                          AS total_slot_ms,
    SUM(COALESCE(child.total_slot_ms / (1000 * 60 * 60), 0))       AS total_slot_hours,
    SUM(COALESCE(child.total_bytes_processed, 0)) / POW(1024, 3)   AS total_gigabytes_processed,
    SUM(COALESCE(child.total_bytes_processed, 0)) / POW(1024, 4)   AS total_terabytes_processed,
    ARRAY_CONCAT_AGG(child.referenced_tables)                      AS referenced_tables,
    ARRAY_AGG(STRUCT(
      bqutil.fn.job_url(
        child.project_id || ':us.' || child.job_id)                AS job_url,
      child.reservation_id                                         AS reservation_id,
      EXTRACT(DATE FROM child.creation_time)                       AS creation_date,
      TIMESTAMP_DIFF(child.end_time, child.start_time, SECOND)     AS job_duration_seconds,
      child.total_slot_ms                                          AS total_slot_ms,
      SAFE_DIVIDE(child.total_slot_ms,TIMESTAMP_DIFF(
        child.end_time, child.start_time, MILLISECOND))            AS job_avg_slots,
      job_stage_max_slots(child.job_stages)                        AS job_stage_max_slots,
      total_bytes_shuffled(child.job_stages)                       AS total_bytes_shuffled,
      total_shuffle_bytes_spilled(child.job_stages)                AS total_shuffle_bytes_spilled) 
      ORDER BY child.start_time
    )                                                              AS children_jobs_details
  FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION AS parent
  JOIN `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION AS child
  ON parent.job_id = child.parent_job_id
  WHERE 
    DATE(parent.creation_time) >= CURRENT_DATE - num_days_to_scan
    AND parent.state = 'DONE'
    AND parent.error_result IS NULL
  GROUP BY job_url, user_email, start_time
  HAVING 
    ARRAY_LENGTH(children_jobs_details) > 1 
    AND total_slot_ms > 0);
