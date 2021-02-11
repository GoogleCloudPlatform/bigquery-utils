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
WITH
  job_info AS (
  SELECT
    creation_time,
    end_time,
    project_id,
    reservation_id
  FROM
    -- PUBLIC DASHBOARD USE ONLY
  	-- Modify this to use your project's INFORMATION_SCHEMA table as follows:
  	-- `region-{region_name}`.INFORMATION_SCHEMA.{table}
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
  WHERE
    job_id = @job_param ),
reservation_capacity AS (
  SELECT
    slot_capacity,
    creation_time,
    end_time,
    job_info.reservation_id
  FROM (
    SELECT
      slot_capacity,
      CONCAT(project_id, ":", "US", ".", reservation_name) AS reservation_id,
      DENSE_RANK() OVER (ORDER BY change_timestamp DESC) AS rownum
    FROM
      -- PUBLIC DASHBOARD USE ONLY
  	  -- Modify this to use your project's INFORMATION_SCHEMA table as follows:
  	  -- `region-{region_name}`.INFORMATION_SCHEMA.{table}
      `region-us`.INFORMATION_SCHEMA.RESERVATION_CHANGES_BY_PROJECT
    WHERE
      change_timestamp <= (
      SELECT
        creation_time
      FROM
        job_info)
      AND CONCAT(project_id, ":", "US", ".", reservation_name) = (
      SELECT
        reservation_id
      FROM
        job_info)) reservation_cap
  JOIN
    job_info
  ON
    reservation_cap.reservation_id = job_info.reservation_id
  WHERE
    reservation_cap.rownum = 1 )
SELECT
  DATETIME_TRUNC(DATETIME(period_start),
    MINUTE) period_start,
  COUNT(DISTINCT
  IF
    (state = 'RUNNING',
      timeline_view.project_id,
      NULL)) AS running_projects,
  timeline_view.reservation_id,
  job_type,
  "minute" AS unit,
  slot_capacity,
  COUNT(DISTINCT
  IF
    (state = 'RUNNING',
      timeline_view.job_id,
      NULL)) AS running_jobs,
  COUNT(DISTINCT
  IF
    (state = 'PENDING',
      timeline_view.project_id,
      NULL)) AS pending_projects,
  COUNT(DISTINCT
  IF
    (state = 'PENDING',
      timeline_view.job_id,
      NULL)) AS pending_jobs,
FROM
  -- PUBLIC DASHBOARD USE ONLY
  -- Modify this to use your project's INFORMATION_SCHEMA table as follows:
  -- `region-{region_name}`.INFORMATION_SCHEMA.{table}
  `region-us`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION timeline_view
JOIN
  reservation_capacity
ON
  timeline_view.reservation_id = reservation_capacity.reservation_id
  AND period_start BETWEEN reservation_capacity.creation_time
  AND reservation_capacity.end_time
GROUP BY
  1,
  3,
  4,
  5,
  6
UNION ALL
SELECT
  DATETIME_TRUNC(DATETIME(period_start),
    HOUR) period_start,
  COUNT(DISTINCT
  IF
    (state = 'RUNNING',
      timeline_view.project_id,
      NULL)) AS running_projects,
  timeline_view.reservation_id,
  job_type,
  "hour" AS unit,
  slot_capacity,
    COUNT(DISTINCT
  IF
    (state = 'RUNNING',
      timeline_view.job_id,
      NULL)) AS running_jobs,
  COUNT(DISTINCT
  IF
    (state = 'PENDING',
      timeline_view.project_id,
      NULL)) AS pending_projects,
  COUNT(DISTINCT
  IF
    (state = 'PENDING',
      timeline_view.job_id,
      NULL)) AS pending_jobs,
FROM
  -- PUBLIC DASHBOARD USE ONLY
  -- Modify this to use your project's INFORMATION_SCHEMA table as follows:
  -- `region-{region_name}`.INFORMATION_SCHEMA.{table}
  `region-us`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION timeline_view
JOIN
  reservation_capacity
ON
  timeline_view.reservation_id = reservation_capacity.reservation_id
  AND period_start BETWEEN reservation_capacity.creation_time
  AND reservation_capacity.end_time
GROUP BY
  1,
  3,
  4,
  5,
  6
UNION ALL
SELECT
  DATETIME_TRUNC(DATETIME(period_start),
    DAY) period_start,
  COUNT(DISTINCT
  IF
    (state = 'RUNNING',
      timeline_view.project_id,
      NULL)) AS running_projects,
  timeline_view.reservation_id,
  job_type,
  "day" AS unit,
  slot_capacity,
    COUNT(DISTINCT
  IF
    (state = 'RUNNING',
      timeline_view.job_id,
      NULL)) AS running_jobs,
  COUNT(DISTINCT
  IF
    (state = 'PENDING',
      timeline_view.project_id,
      NULL)) AS pending_projects,
  COUNT(DISTINCT
  IF
    (state = 'PENDING',
      timeline_view.job_id,
      NULL)) AS pending_jobs,
FROM
  -- PUBLIC DASHBOARD USE ONLY
  -- Modify this to use your project's INFORMATION_SCHEMA table as follows:
  -- `region-{region_name}`.INFORMATION_SCHEMA.{table}
  `region-us`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION timeline_view
JOIN
  reservation_capacity
ON
  timeline_view.reservation_id = reservation_capacity.reservation_id
  AND period_start BETWEEN reservation_capacity.creation_time
  AND reservation_capacity.end_time
GROUP BY
  1,
  3,
  4,
  5,
  6
