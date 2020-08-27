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
 * Reservation Utilization Report: Returns the current reservation
 * assignments and associated slot capacities
 */

-- This table retrieves the latest slot capacity for each reservation
WITH
  latest_slot_capacity as (
    SELECT
      rcp.reservation_name, rcp.slot_capacity
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
-- Extract information about current assignments
SELECT
  acp.assignment_id,
  acp.project_id,
  acp.reservation_name,
  acp.job_type,
  acp.assignee_id,
  acp.assignee_type,
  lsc.slot_capacity
FROM
  `region-{region_name}`.INFORMATION_SCHEMA.ASSIGNMENT_CHANGES_BY_PROJECT AS acp
-- Join to obtain the current slot capacities
LEFT JOIN latest_slot_capacity lsc
  ON lsc.reservation_name = acp.reservation_name
GROUP BY
  acp.assignment_id,
  acp.project_id,
  acp.reservation_name,
  acp.job_type,
  acp.assignee_id,
  acp.assignee_type,
  lsc.slot_capacity
-- In order to return only active assignments (i.e. ones that have not been
-- deleted) we select only assignments that have one entry in this table.
-- Assignments that have been deleted have two entries in this table,
-- one where the action is CREATE and one where the action is DELETE.
HAVING COUNT(assignment_id) = 1
