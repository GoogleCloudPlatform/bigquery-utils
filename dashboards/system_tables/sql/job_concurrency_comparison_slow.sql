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
    -- Update `@job_param`, depending on if this is for the slow or fast job.
    -- When creating the side-by-side comparison view, you will need to duplicate
    -- this data source and update parameter to @job_param_2, or similar.
    job_id = @job_param),
   organization_capacity AS (
  SELECT
    SUM(slot_capacity) AS org_capacity
  FROM (
    SELECT
      slot_capacity AS slot_capacity,
      reservation_name,
      change_timestamp,
      action,
      -- Find the most recent reservation update grouped by reservation
      DENSE_RANK() OVER (PARTITION BY reservation_name ORDER BY change_timestamp DESC) AS rownum
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
        job_info) ) reservation_cap
  WHERE
    reservation_cap.rownum = 1
    -- Remove deleted reservations, so that only active reservations are displayed
    AND action != "DELETE" ),
  reservation_capacity AS (
  SELECT
    slot_capacity AS reservation_capacity,
    creation_time,
    end_time,
    job_info.project_id,
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
    reservation_cap.rownum = 1 ),
 joined_capacity AS (SELECT
  *
FROM
  reservation_capacity
CROSS JOIN
  organization_capacity    )
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
  reservation_capacity,
  org_capacity,
  COUNT(DISTINCT
  IF
    (state = 'RUNNING'
      AND timeline_view.project_id = joined_capacity.project_id,
      timeline_view.job_id,
      NULL)) AS running_jobs,
  COUNT(DISTINCT
  IF
    (state = 'PENDING',
      timeline_view.project_id,
      NULL)) AS pending_projects,
  COUNT(DISTINCT
  IF
    (state = 'PENDING'
      AND timeline_view.project_id = joined_capacity.project_id,
      timeline_view.job_id,
      NULL)) AS pending_jobs,
  SUM(period_slot_ms) / (1000 * 60) AS reservation_slots_used,
  SUM(
  IF
    (timeline_view.project_id = joined_capacity.project_id,
      (period_slot_ms),
      0)) / (1000 * 60) AS project_slots_used,
  SUM(period_slot_ms) / (1000 * 60) / reservation_capacity AS reservation_utilization,

FROM
  -- PUBLIC DASHBOARD USE ONLY
  -- Modify this to use your project's INFORMATION_SCHEMA table as follows:
  -- `region-{region_name}`.INFORMATION_SCHEMA.{table}
  `region-us`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION timeline_view
JOIN
  joined_capacity
ON
  timeline_view.reservation_id = joined_capacity.reservation_id
  AND period_start BETWEEN joined_capacity.creation_time
  AND joined_capacity.end_time
GROUP BY
  1,
  3,
  4,
  5,
  6,
  7
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
  reservation_capacity,
  org_capacity,
  COUNT(DISTINCT
  IF
    (state = 'RUNNING'
      AND timeline_view.project_id = joined_capacity.project_id,
      timeline_view.job_id,
      NULL)) AS running_jobs,
  COUNT(DISTINCT
  IF
    (state = 'PENDING',
      timeline_view.project_id,
      NULL)) AS pending_projects,
  COUNT(DISTINCT
  IF
    (state = 'PENDING'
      AND timeline_view.project_id = joined_capacity.project_id,
      timeline_view.job_id,
      NULL)) AS pending_jobs,
  SUM(period_slot_ms) / (1000 * 60 * 60) AS reservation_slots_used,
  SUM(
  IF
    (timeline_view.project_id = joined_capacity.project_id,
      (period_slot_ms),
      0)) / (1000 * 60 * 60) AS project_slots_used,
  SUM(period_slot_ms) / (1000 * 60 * 60) / reservation_capacity AS reservation_utilization,
FROM
  -- PUBLIC DASHBOARD USE ONLY
  -- Modify this to use your project's INFORMATION_SCHEMA table as follows:
  -- `region-{region_name}`.INFORMATION_SCHEMA.{table}
  `region-us`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION timeline_view
JOIN
  joined_capacity
ON
  timeline_view.reservation_id = joined_capacity.reservation_id
  AND period_start BETWEEN joined_capacity.creation_time
  AND joined_capacity.end_time
GROUP BY
  1,
  3,
  4,
  5,
  6,
  7
