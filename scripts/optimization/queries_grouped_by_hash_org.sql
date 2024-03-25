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
 * This script creates a table named, top_bytes_scanning_queries_by_hash, 
 * which contains the top 200 most expensive queries by total bytes scanned
 * within the past 30 days.
 * 30 days is the default timeframe, but you can change this by setting the
 * num_days_to_scan variable to a different value.
 * Queries are grouped by their normalized query pattern, which ignores
 * comments, parameter values, UDFs, and literals in the query text.
 * This allows us to group queries that are logically the same, but
 * have different literals. 
 * 
 * For example, the following queries would be grouped together:
 *   SELECT * FROM `my-project.my_dataset.my_table` WHERE date = '2020-01-01'
 *   SELECT * FROM `my-project.my_dataset.my_table` WHERE date = '2020-01-02'
 *   SELECT * FROM `my-project.my_dataset.my_table` WHERE date = '2020-01-03'
 */

DECLARE num_days_to_scan INT64 DEFAULT 30;

CREATE TEMP FUNCTION num_stages_with_perf_insights(query_info ANY TYPE) AS (
  COALESCE((
    SELECT SUM(IF(i.slot_contention, 1, 0) + IF(i.insufficient_shuffle_quota, 1, 0)) 
    FROM UNNEST(query_info.performance_insights.stage_performance_standalone_insights) i), 0)
  + COALESCE(ARRAY_LENGTH(query_info.performance_insights.stage_performance_change_insights), 0)
);

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.queries_grouped_by_hash_org AS
SELECT
  statement_type,
  query_info.query_hashes.normalized_literals                              AS query_hash,
  COUNT(DISTINCT DATE(start_time))                                         AS days_active,
  ARRAY_AGG(DISTINCT project_id IGNORE NULLS)                              AS project_ids,
  ARRAY_AGG(DISTINCT reservation_id IGNORE NULLS)                          AS reservation_ids,
  SUM(num_stages_with_perf_insights(query_info))                           AS num_stages_with_perf_insights,
  COUNT(DISTINCT (project_id || ':us.' || job_id))                         AS job_count,
  ARRAY_AGG(
    STRUCT(
      bqutil.fn.job_url(project_id || ':us.' || parent_job_id) AS parent_job_url,
      bqutil.fn.job_url(project_id || ':us.' || job_id) AS job_url
    )
    ORDER BY total_slot_ms
    DESC LIMIT 10)                                                         AS top_10_jobs,
  ARRAY_AGG(DISTINCT user_email)                                           AS user_emails,
  SUM(total_bytes_processed) / POW(1024, 3)                                AS total_gigabytes_processed,
  AVG(total_bytes_processed) / POW(1024, 3)                                AS avg_gigabytes_processed,
  SUM(total_slot_ms) / (1000 * 60 * 60)                                    AS total_slot_hours,
  AVG(total_slot_ms) / (1000 * 60 * 60)                                    AS avg_total_slot_hours_per_active_day,
  AVG(TIMESTAMP_DIFF(end_time, start_time, SECOND) )                       AS avg_job_duration_seconds,
  ARRAY_AGG(DISTINCT FORMAT("%T",labels))                                  AS labels,
  SUM(total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND))   AS total_slots,
  AVG(total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND))   AS avg_total_slots,
  -- query hashes will all have the same referenced tables so we can use ANY_VALUE below
  ANY_VALUE(ARRAY(
    SELECT 
      ref_table.project_id || '.' || 
      IF(STARTS_WITH(ref_table.dataset_id, '_'), 'TEMP', ref_table.dataset_id)
      || '.' || ref_table.table_id
    FROM UNNEST(referenced_tables) ref_table
  ))                                                                       AS referenced_tables,
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
WHERE 
  DATE(creation_time) >= CURRENT_DATE - num_days_to_scan
  AND state = 'DONE'
  AND error_result IS NULL
  AND job_type = 'QUERY'
  AND statement_type != 'SCRIPT' 
GROUP BY statement_type, query_hash;
