/*
 * Copyright 2021 Google LLC
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
 * It is assumed that the following query will be run in the administration
 * project where the reservations were created. If this is not the case,
 * prepend the project id to the table name as follows:
 * `{project_id}`.`region-{region_name}`.INFORMATION_SCHEMA.{table}
 */

/*
 * Job Comparison Report: Returns information about jobs to compare performance
 * and troubleshoot.
 */
 SELECT
  job_id,
  creation_time,
  ROUND(TIMESTAMP_DIFF(start_time, creation_time, MILLISECOND) / 1000, 2) AS creation_duration_s,
  ROUND(TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) / 1000, 2) AS execution_duration_s,
  project_id,
  user_email,
  job_type,
  statement_type,
  total_bytes_processed,
  total_slot_ms,
  total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS avg_slots,
  cache_hit,
  ARRAY(
    SELECT
      STRUCT(
        snap.elapsed_ms,
        snap.total_slot_ms,
        snap.pending_units,
        snap.completed_units,
        snap.active_units,
        snap.elapsed_ms - IFNULL(LAG(snap.elapsed_ms) OVER (ORDER BY snap.elapsed_ms ASC), 0) AS incremental_elapsed_ms,
        snap.total_slot_ms - IFNULL(LAG(snap.total_slot_ms) OVER (ORDER BY snap.elapsed_ms ASC), 0) AS incremental_slot_ms
      )
    FROM
      UNNEST(timeline) snap
  ) AS timeline,
  # Rebuild the job_stages array so we can add additional attributes
  ARRAY(
    SELECT
      STRUCT(
        stage.id,
        stage.name,
        TIMESTAMP_MILLIS(stage.start_ms) AS start_time,
        stage.start_ms,
        TIMESTAMP_MILLIS(stage.end_ms) AS end_time,
        stage.end_ms,
        stage.end_ms - stage.start_ms AS duration_ms,
        stage.slot_ms,
        stage.slot_ms / (stage.end_ms - stage.start_ms) AS avg_slots,
        stage.input_stages,
        stage.status,
        stage.parallel_inputs,
        stage.completed_parallel_inputs,
        stage.records_read,
        stage.records_written,
        stage.shuffle_output_bytes,
        stage.shuffle_output_bytes_spilled,
        stage.wait_ratio_avg,
        stage.wait_ms_avg,
        stage.wait_ratio_max,
        stage.wait_ms_max,
        stage.read_ratio_avg,
        stage.read_ms_avg,
        stage.read_ratio_max,
        stage.read_ms_max,
        stage.compute_ratio_avg,
        stage.compute_ms_avg,
        stage.compute_ratio_max,
        stage.compute_ms_max,
        stage.write_ratio_avg,
        stage.write_ms_avg,
        stage.write_ratio_max,
        stage.write_ms_max,
        ARRAY_TO_STRING(
          ARRAY(
            SELECT
              step.kind
            FROM
              UNNEST(stage.steps) step
            WITH OFFSET AS step_offset
            ORDER BY step_offset ASC
          ),
          ", "
        ) AS steps
      )
    FROM
      UNNEST(job_stages) stage
  ) AS job_stages,
  ARRAY(
    SELECT
      STRUCT(
        ROUND(all_timeline_events.event_time / 1000, 2) AS event_time_seconds,
        job_stage_entry.id as job_stage_id,
        job_stage_entry.name as job_stage_name
      )
    FROM
      UNNEST(job_stages) job_stage_entry
    CROSS JOIN
      (
        (
        SELECT DISTINCT
          jse1.start_ms - UNIX_MILLIS(start_time) event_time
        FROM
          UNNEST(job_stages) jse1
        )
        UNION DISTINCT
        (
        SELECT DISTINCT
          jse2.end_ms - UNIX_MILLIS(start_time) event_time
        FROM
          UNNEST(job_stages) jse2
        )
        UNION DISTINCT
        (
        SELECT DISTINCT
          timeline_entry.elapsed_ms event_time
        FROM
          UNNEST(timeline) as timeline_entry
        )
      ) all_timeline_events
    WHERE
      all_timeline_events.event_time >= job_stage_entry.start_ms - UNIX_MILLIS(start_time) AND
      all_timeline_events.event_time <= job_stage_entry.end_ms - UNIX_MILLIS(start_time) AND
      job_stage_entry.id <= 19
  ) AS stages_gantt
FROM
  -- PUBLIC DASHBOARD USE ONLY
  -- Modify this to use your project's INFORMATION_SCHEMA table as follows:
  -- `region-{region_name}`.INFORMATION_SCHEMA.{table}
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
-- If making a copy of this query, update `@job_param` for the Job Comparison Report.
-- Depending on if this is for the slow or fast job, use `@job_param` or `job_param_2`.
-- When creating the side-by-side comparison view, you will need to duplicate
-- this data source and update parameter to @job_param_2, or similar.
WHERE job_id = @job_param
