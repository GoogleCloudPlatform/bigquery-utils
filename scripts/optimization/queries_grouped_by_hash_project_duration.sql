/*
 * Copyright 2025 Google LLC
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
 * This script creates a table named queries_grouped_by_hash_project_duration, 
 * which contains the top 200 most expensive queries by total slot hours
 * within the past 30 days focusing on the duration of each query hash.
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
 ​
DECLARE num_days_to_scan INT64 DEFAULT 30;
CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.queries_grouped_by_hash_project_duration AS
​SELECT
   query_hash,
   SUM(total_slot_ms) / 1000 / 60 / 60 total_slot_hours,
   COUNT(*) AS job_count,
   AVG(time_ms) avg_time_ms,
   MAX(median_time_ms) median_time_ms,
   MAX(p75_time_ms) p75_time_ms,
   MAX(p80_time_ms) p80_time_ms,
   MAX(p90_time_ms) p90_time_ms,
   MAX(p95_time_ms) p95_time_ms,
   MAX(p99_time_ms) p99_time_ms,
   ARRAY_AGG(
    STRUCT(
      bqutil.fn.job_url(project_id || ':us.' || parent_job_id) AS parent_job_url,
      bqutil.fn.job_url(project_id || ':us.' || job_id) AS job_url,
      query as query_text
    )
    ORDER BY total_slot_ms
    DESC LIMIT 10) AS top_10_jobs,,
  -- query hashes will all have the same referenced tables so we can use ANY_VALUE below
   ANY_VALUE(ARRAY(
     SELECT
       ref_table.project_id || '.' ||
       IF(STARTS_WITH(ref_table.dataset_id, '_'), 'TEMP', ref_table.dataset_id)
       || '.' || ref_table.table_id
     FROM UNNEST(referenced_tables) ref_table
   )) AS referenced_tables
FROM (
   SELECT
       query_info.query_hashes.normalized_literals AS query_hash,
       job_id,
       total_slot_ms,
       TIMESTAMP_DIFF(jbo.end_time, jbo.creation_time, MILLISECOND) time_ms,
       query,
       referenced_tables,
       PERCENTILE_CONT(TIMESTAMP_DIFF(jbo.end_time, jbo.creation_time, MILLISECOND), 0.5) OVER (PARTITION BY query_info.query_hashes.normalized_literals) AS median_time_ms,
       PERCENTILE_CONT(TIMESTAMP_DIFF(jbo.end_time, jbo.creation_time, MILLISECOND), 0.75) OVER (PARTITION BY query_info.query_hashes.normalized_literals) AS p75_time_ms,
       PERCENTILE_CONT(TIMESTAMP_DIFF(jbo.end_time, jbo.creation_time, MILLISECOND), 0.8) OVER (PARTITION BY query_info.query_hashes.normalized_literals) AS p80_time_ms,
       PERCENTILE_CONT(TIMESTAMP_DIFF(jbo.end_time, jbo.creation_time, MILLISECOND), 0.90) OVER (PARTITION BY query_info.query_hashes.normalized_literals) AS p90_time_ms,
       PERCENTILE_CONT(TIMESTAMP_DIFF(jbo.end_time, jbo.creation_time, MILLISECOND), 0.95) OVER (PARTITION BY query_info.query_hashes.normalized_literals) AS p95_time_ms,
       PERCENTILE_CONT(TIMESTAMP_DIFF(jbo.end_time, jbo.creation_time, MILLISECOND), 0.99) OVER (PARTITION BY query_info.query_hashes.normalized_literals) AS p99_time_ms
   FROM
       FROM `region-us`.INFORMATION_SCHEMA.JOBS jbo
   WHERE
       DATE(creation_time) >= CURRENT_DATE - num_days_to_scan
       AND jbo.end_time > jbo.start_time
       AND jbo.error_result IS NULL
       AND jbo.statement_type != 'SCRIPT'
) AS jobs_with_hash
GROUP BY query_hash
ORDER BY total_slot_hours DESC 
LIMIT 200;
