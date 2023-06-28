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
 * The following script retrieves the top 100 queries that have had performance insights
 * generated for them in the past 30 days.
 */

DECLARE var_n_days INT64 DEFAULT 30;

SELECT
  bqutil.fn.job_url(project_id || ':us.' || job_id) AS job_url,
  (SELECT COUNT(1)
   FROM UNNEST(query_info.performance_insights.stage_performance_standalone_insights) perf_insight 
   WHERE perf_insight.slot_contention
  ) AS num_stages_with_slot_contention,
  (SELECT COUNT(1)
   FROM UNNEST(query_info.performance_insights.stage_performance_standalone_insights) perf_insight
   WHERE perf_insight.insufficient_shuffle_quota
  ) AS num_stages_with_insufficient_shuffle_quota,
  (SELECT ARRAY_AGG(perf_insight.input_data_change.records_read_diff_percentage IGNORE NULLS)
   FROM UNNEST(query_info.performance_insights.stage_performance_change_insights) perf_insight
  ) AS records_read_diff_percentages
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION jbo
WHERE
  DATE(jbo.creation_time) >= CURRENT_DATE - var_n_days
  AND jbo.job_type = 'QUERY'
  AND jbo.end_time > jbo.start_time
  AND jbo.error_result IS NULL
  AND jbo.statement_type != 'SCRIPT'
  AND (
    COALESCE(ARRAY_LENGTH(query_info.performance_insights.stage_performance_standalone_insights),0)
    + COALESCE(ARRAY_LENGTH(query_info.performance_insights.stage_performance_change_insights),0) > 0
  )
  AND EXISTS ( -- Only include queries which had performance insights
    SELECT
      1
    FROM
      UNNEST(query_info.performance_insights.stage_performance_standalone_insights) AS perf_insight
    WHERE
      perf_insight.slot_contention OR perf_insight.insufficient_shuffle_quota
  )
ORDER BY num_stages_with_insufficient_shuffle_quota + num_stages_with_slot_contention DESC
LIMIT 100