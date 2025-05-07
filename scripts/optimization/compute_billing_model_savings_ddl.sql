/*
 * Copyright 2024 Google LLC
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

-- US pricing for reference: https://cloud.google.com/bigquery/pricing#storage
-- standard_baseline_rate FLOAT64 DEFAULT .04;
-- enterprise_baseline_1yr_rate FLOAT64 DEFAULT .048;
-- enterprise_baseline_3yr_rate FLOAT64 DEFAULT .036;
-- enterprise_plus_baseline_1yr_rate FLOAT64 DEFAULT .08;
-- enterprise_plus_baseline_3yr_rate FLOAT64 DEFAULT .06;
-- standard_payg_rate FLOAT64 DEFAULT .04;
-- enterprise_payg_rate FLOAT64 DEFAULT .06;
-- enterprise_plus_payg_rate FLOAT64 DEFAULT .1;

DECLARE num_days_to_scan INT64 DEFAULT 30;
DECLARE on_demand_rate FLOAT64 DEFAULT 6.25;
-- REMEMBER: Replace with the values of your current or target baseline slot price.
DECLARE baseline_rate FLOAT64 DEFAULT .04;
-- REMEMBER: Replace with the values of your current or target autoscaling slot price.
DECLARE autoscaling_rate DEFAULT .04;

-- REMEMBER: (optional) Change this to filter based on savings absolute value or percentage
DECLARE threshold_percent FLOAT64 DEFAULT 10.0;  -- 10% difference
DECLARE absolute_threshold FLOAT64 DEFAULT 100.0; -- $100 absolute difference

-- REMEMBER: Set the Admin Project ID, Location, and Reservation Name
DECLARE admin_project_id STRING DEFAULT 'your-admin-project-id'; -- Replace with your Admin Project ID
DECLARE location STRING DEFAULT 'US';              -- Replace with your location
DECLARE reservation_name STRING DEFAULT 'your-reservation-name';  -- Replace with your Reservation Name


CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.compute_billing_model_savings_ddl AS (
WITH job_data AS (
   SELECT
       project_id,
       SUM(total_slot_ms) as total_slot_ms,
       SUM((total_slot_ms/1000/60/60)) as total_slot_hours,
       SUM(total_bytes_billed/ POW(1024, 4)) as tb_billed,
       CASE
           WHEN reservation_id IS NULL THEN 'on_demand'
           ELSE 'reservation'
       END AS usage_type
   FROM
       `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION as jbo
   WHERE
       job_type = 'QUERY'
       AND statement_type != 'SCRIPT'
       AND DATE(jbo.creation_time, "US/Central") >= CURRENT_DATE - num_days_to_scan
   GROUP BY
       project_id, usage_type
),
cost_analysis AS (
   SELECT
       project_id,
       usage_type,
       sum(total_slot_hours) as total_slot_hours,
       sum(tb_billed) as tb_billed,
       sum(tb_billed)*on_demand_rate as cost_on_demand,
       sum(total_slot_hours)*autoscaling_rate as worst_case_cost_reservation, -- Worst case is that only autoscaling slots are used.
       sum(total_slot_hours)*baseline_rate as best_case_cost_reservation -- Best case is that only baseline slots are used.
   FROM job_data
   GROUP BY 1, 2
)
SELECT
   *,
   -- Generate DDL templates with threshold logic and parameters
   CASE
       WHEN usage_type = 'on_demand' AND cost_on_demand > 0 AND (
               (cost_on_demand - worst_case_cost_reservation) / cost_on_demand * 100 >= threshold_percent OR
               cost_on_demand - worst_case_cost_reservation >= absolute_threshold
           ) THEN CONCAT('CREATE ASSIGNMENT `', admin_project_id, '.', 'region-', location, '.', reservation_name, '.ASSIGNMENT_ID` OPTIONS ( assignee="projects/', project_id, '", job_type="QUERY");')   
       WHEN usage_type = 'reservation' AND cost_on_demand > 0 AND (
               (best_case_cost_reservation - cost_on_demand) / cost_on_demand * 100 >= threshold_percent OR
               best_case_cost_reservation - cost_on_demand >= absolute_threshold
           ) THEN CONCAT('CREATE ASSIGNMENT `', admin_project_id, '.', 'region-', location, '.', 'none', '.ASSIGNMENT_ID` OPTIONS ( assignee="projects/', project_id, '", job_type="QUERY");')
       ELSE NULL  -- No DDL if threshold not met or cost_on_demand is zero
   END AS ddl
FROM cost_analysis
);
