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
 * Daily Utilization Report: Returns the slot count for active monthly and
 * annual capacity commitments for each day since the first commitment
 * was purchased.
 */

-- 'commitments' extracts the commitment history and computes the cumulative slot count
-- 'results' takes the most recent entry and slot count for a given day
-- 'days' generates an entry for each day between two entries
WITH
  commitments AS (
    SELECT
      change_timestamp,
      commitment_plan,
      action,
      EXTRACT(DATE FROM change_timestamp) AS start_date,
      -- Compute the stop date of a commitment by looking at the following
      -- change_timestamp and subtracting one day. This works because monthly
      -- and yearly commitments cannot be deleted on the same day
      -- they were created.
      IFNULL(
        LEAD(DATE_SUB(EXTRACT(DATE FROM change_timestamp), INTERVAL 1 DAY))
          OVER (PARTITION BY state ORDER BY change_timestamp),
        CURRENT_DATE()) AS stop_date,
      -- In order to calculate the cumulative slots up to this point, add
      -- the slot count of new commitments (indicated by a CREATE or UPDATE action)
      -- and subtract the slot count of deleted commitments
      SUM(CASE WHEN cccp.action IN ('CREATE', 'UPDATE') THEN cccp.slot_count ELSE cccp.slot_count * -1 END)
        -- Cumulative slots are tracked by their state and carried over from
        -- previous rows
        OVER (
          PARTITION BY state
          ORDER BY change_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS slot_cummulative,
      -- In the event that multiple changes occurred in one day, we keep track
      -- of the most recent cumulative value by using row numbers
      ROW_NUMBER()
        OVER (
          PARTITION BY EXTRACT(DATE FROM change_timestamp)
          ORDER BY change_timestamp DESC
        ) AS rn
    FROM
      `region-{region_name}`.INFORMATION_SCHEMA.CAPACITY_COMMITMENT_CHANGES_BY_PROJECT AS cccp
    -- In this case, we only want to look at active commitments that are
    -- monthly or annual, not flex
    WHERE
      state = 'ACTIVE'
      AND commitment_plan != 'FLEX'
    ORDER BY change_timestamp
  ),
  results AS (SELECT * FROM commitments WHERE rn = 1),
  days AS (
    -- This subquery is used to fill in the missing days between a commitment
    -- starting and ending so that it can be graphed properly.
    SELECT day
    FROM (
       SELECT
         start_date,
         stop_date
       FROM results
     ), UNNEST(GENERATE_DATE_ARRAY(start_date, stop_date)) day
  )
SELECT TIMESTAMP(day) as date, LAST_VALUE(slot_cummulative IGNORE NULLS) OVER(ORDER BY day) slots,
FROM days
-- Join these results with the cumulative slot count values for each day
LEFT JOIN results
  ON day = DATE(change_timestamp)
