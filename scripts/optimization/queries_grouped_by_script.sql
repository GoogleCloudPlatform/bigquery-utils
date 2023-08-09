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

SELECT
  parent.job_id,
  parent.start_time,
  ARRAY_AGG(STRUCT(
    bqutil.fn.job_url(
      child.project_id || ':us.' || child.job_id)           AS job_url,
    child.reservation_id                                    AS reservation_id,
    EXTRACT(DATE FROM child.creation_time)                  AS creation_date,
    SAFE_DIVIDE(TIMESTAMP_DIFF(
      child.end_time, child.start_time, MILLISECOND), 1000) AS job_duration_seconds,
    child.total_slot_ms                                     AS total_slot_ms,
    SAFE_DIVIDE(child.total_slot_ms,TIMESTAMP_DIFF(
      child.end_time, child.start_time, MILLISECOND))       AS job_avg_slots,
    job_stage_max_slots(child.job_stages)                   AS job_stage_max_slots,
    total_bytes_shuffled(child.job_stages)                  AS total_bytes_shuffled,
    total_shuffle_bytes_spilled(child.job_stages)           AS total_shuffle_bytes_spilled
  ) ORDER BY child.start_time )
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION AS parent
JOIN `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION AS child
ON parent.job_id = child.parent_job_id
WHERE 
  DATE(parent.creation_time) >= CURRENT_DATE - num_days_to_scan
GROUP BY 1,2;
