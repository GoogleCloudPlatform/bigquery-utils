/*
 * Copyright 2023 Google LLC
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
 * This script retrieves the top 100 queries that have had performance insights
 * generated for them in the past 30 days.
 * 30 days is the default timeframe, but you can change this by setting the
 * num_days_to_scan variable to a different value.
 */

DECLARE num_days_to_scan INT64 DEFAULT 30;

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.query_performance_insights AS
SELECT
  bqutil.fn.job_url(project_id || ':us.' || job_id) AS job_url,
  query_info.performance_insights AS performance_insights
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
WHERE
  DATE(creation_time) >= CURRENT_DATE - num_days_to_scan
  AND job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
  AND statement_type != 'SCRIPT'
  AND EXISTS ( -- Only include queries which had performance insights
    SELECT 1
    FROM UNNEST(
      query_info.performance_insights.stage_performance_standalone_insights
    )
    WHERE
      slot_contention
      OR insufficient_shuffle_quota
      -- The following fields are arrays or complex types, so we check for non-null values
      OR bi_engine_reasons IS NOT NULL
      OR high_cardinality_joins IS NOT NULL
      OR partition_skew IS NOT NULL
    UNION ALL
    SELECT 1
    FROM UNNEST(
      query_info.performance_insights.stage_performance_change_insights
    )
    WHERE input_data_change.records_read_diff_percentage IS NOT NULL
  );
