/*
 * Copyright 2020 Google LLC
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
 * Job Execution Report: Returns job-level statistics including average
 * slot utilization
 */

SELECT
  project_id,
  job_id,
  reservation_id,
  EXTRACT(DATE FROM creation_time) AS creation_date,
  creation_time,
  end_time,
  TIMESTAMP_DIFF(COALESCE(end_time, CURRENT_TIMESTAMP()), start_time, SECOND) AS job_duration_seconds,
  job_type,
  user_email,
  state,
  error_result,
  total_bytes_processed,
  -- Average slot utilization per job is calculated by dividing
  -- total_slot_ms by the millisecond duration of the job
  SAFE_DIVIDE(total_slot_ms, (TIMESTAMP_DIFF(end_time, start_time, MILLISECOND))) AS avg_slots
FROM
  `region-{region_name}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
ORDER BY
  creation_time DESC
