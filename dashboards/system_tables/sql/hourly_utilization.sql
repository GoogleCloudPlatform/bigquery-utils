
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
  * Hourly Utilization Report: Returns hourly BigQuery usage
  * by reservation, project, job type, and user
  */

SELECT
  -- usage_time is used for grouping jobs by the hour
  -- usage_date is used to separately store the date this job occurred
  TIMESTAMP_TRUNC(jbo.period_start, HOUR) AS usage_time,
  EXTRACT(DATE from jbo.period_start) AS usage_date,
  jbo.reservation_id,
  jbo.project_id,
  jbo.job_type,
  jbo.user_email,
  -- Aggregate total_slots_ms used for all jobs at this hour and divide
  -- by the number of milliseconds in an hour. Most accurate for hours with
  -- consistent slot usage
  SUM(jbo.period_slot_ms) / (1000 * 60 * 60) AS average_hourly_slot_usage
FROM
  `region-{region_name}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION jbo
WHERE (jbo.statement_type != "SCRIPT" OR jbo.statement_type IS NULL)  -- Avoid duplicate byte counting in parent and children jobs.
GROUP BY
  usage_time,
  usage_date,
  jbo.project_id,
  jbo.reservation_id,
  jbo.job_type,
  jbo.user_email
ORDER BY
  usage_time ASC
