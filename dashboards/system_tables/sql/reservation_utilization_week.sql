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
 * Reservation Utilization Report: Returns the average usage, average
 * capacity, current capacity, and average utilization of a reservation
 * for the last 7 days
 */

-- This table retrieves the slot capacity history of every reservation
-- including the start and end time of that capacity
WITH
  reservation_slot_capacity AS (
    SELECT
      -- Concatenation is needed as RESERVATION_CHANGES_BY_PROJECT only
      -- includes reservation name but in order to join with
      -- JOBS_BY_ORGANIZATION, reservation id is required
      CONCAT("{project_id}:{location}.", reservation_name) AS reservation_id,
      change_timestamp AS start_time,
      IFNULL(
        LEAD(change_timestamp)
          OVER (
            PARTITION BY reservation_name
            ORDER BY change_timestamp ASC),
        CURRENT_TIMESTAMP()) AS end_time,
      action,
      slot_capacity
    FROM
      `region-{region_name}`.INFORMATION_SCHEMA.RESERVATION_CHANGES_BY_PROJECT
  ),
  -- This table retrieves only the current slot capacity of a reservation
  latest_slot_capacity AS (
    SELECT
      rcp.reservation_name,
      rcp.slot_capacity,
      CONCAT("{project_id}:{location}.", rcp.reservation_name) AS reservation_id,
    FROM
      `region-{region_name}`.INFORMATION_SCHEMA.RESERVATION_CHANGES_BY_PROJECT AS rcp
    WHERE
      -- This subquery returns the latest slot capacity for each reservation
      -- by extracting the reservation with the maximum timestamp
      (rcp.reservation_name, rcp.change_timestamp) IN (
        SELECT AS STRUCT reservation_name, MAX(change_timestamp)
        FROM
          `region-{region_name}`.INFORMATION_SCHEMA.RESERVATION_CHANGES_BY_PROJECT
        GROUP BY reservation_name)
  )
-- Compute the average slot utilization and average reservation utilization
SELECT
  jbo.reservation_id,
  -- Slot usage is calculated by aggregating total_slot_ms for all jobs
  -- in the last week and dividing by the number of milliseconds in a week
  SAFE_DIVIDE(SUM(jbo.total_slot_ms), (1000 * 60 * 60 * 24 * 7)) AS average_weekly_slot_usage,
  AVG(rsc.slot_capacity) AS average_reservation_capacity,
  SAFE_DIVIDE(
      SAFE_DIVIDE(
          SUM(jbo.total_slot_ms),
          1000 * 60 * 60 * 24 * 7
      ),
      AVG(rsc.slot_capacity)
  ) AS reservation_utilization,

  lsc.slot_capacity AS latest_capacity
FROM
  `region-{region_name}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION jbo
-- Join the slot capacity history
LEFT JOIN reservation_slot_capacity rsc
  ON
    jbo.reservation_id = rsc.reservation_id
    AND jbo.creation_time >= rsc.start_time
    AND jbo.creation_time < rsc.end_time
-- Join the latest slot capacity
LEFT JOIN latest_slot_capacity lsc
  ON
    jbo.reservation_id = lsc.reservation_id
WHERE
  -- Includes jobs created 8 days ago but completed 7 days ago
  jbo.creation_time
    BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 8 DAY)
    AND CURRENT_TIMESTAMP()
  AND jbo.end_time
    BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND CURRENT_TIMESTAMP()
GROUP BY
  reservation_id,
  lsc.slot_capacity
ORDER BY
  reservation_id DESC
