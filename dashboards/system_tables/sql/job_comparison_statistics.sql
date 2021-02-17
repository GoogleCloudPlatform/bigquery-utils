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
    project_id,
    reservation_id,
    cache_hit,
    job_id,
    start_time,
    job_type,
    priority,
    creation_time,
    end_time,
    error_result.reason AS error_reason,
    SUM(total_bytes_processed) AS sum_bytes_processed,
    SUM(total_slot_ms) AS sum_slot_ms,
	SUM(TIMESTAMP_DIFF(start_time, creation_time, SECOND)) AS creation_sum_sec,
    SUM(TIMESTAMP_DIFF(COALESCE(end_time,CURRENT_TIMESTAMP()), start_time, SECOND)) duration_sum_sec,
    SUM((SELECT SUM(stage.shuffle_output_bytes_spilled) FROM UNNEST(job_stages) stage)) AS shuffle_output_bytes_spilled,
    SUM((SELECT SUM(stage.shuffle_output_bytes) FROM UNNEST(job_stages) stage)) AS shuffle_output_bytes
FROM
	`region-{region_name}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
WHERE
	job_id = @job_param
	OR job_id = @job_param_2
GROUP BY
	1,2,3,4,5,6,7,8,9,10
