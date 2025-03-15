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

DECLARE num_days_to_scan INT64 DEFAULT 30;

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.bi_engine_mode_duration AS
SELECT
 day,
 bi_engine_mode,
 COUNT(*) as job_count,
 AVG(time_ms) avg_time_ms,
 MAX(median_time_ms) median_time_ms,
 MAX(p75_time_ms) p75_time_ms,
 MAX(p80_time_ms) p80_time_ms,
 MAX(p90_time_ms) p90_time_ms,
 MAX(p95_time_ms) p95_time_ms,
 MAX(p99_time_ms) p99_time_ms,
FROM
 (
     SELECT
         day,
         bi_engine_mode,
         time_ms,
         PERCENTILE_CONT(time_ms, 0.5) OVER (PARTITION BY day, bi_engine_mode) as median_time_ms,
         PERCENTILE_CONT(time_ms, 0.75) OVER (PARTITION BY day, bi_engine_mode) as p75_time_ms,
         PERCENTILE_CONT(time_ms, 0.8) OVER (PARTITION BY day, bi_engine_mode) as p80_time_ms,
         PERCENTILE_CONT(time_ms, 0.90) OVER (PARTITION BY day, bi_engine_mode) as p90_time_ms,
         PERCENTILE_CONT(time_ms, 0.95) OVER (PARTITION BY day, bi_engine_mode) as p95_time_ms,
         PERCENTILE_CONT(time_ms, 0.99) OVER (PARTITION BY day, bi_engine_mode) as p99_time_ms,
     FROM
         (
             SELECT
                 DATE(jbo.creation_time) AS day,
                 bi_engine_statistics.bi_engine_mode as bi_engine_mode,
                 job_id,
                 TIMESTAMP_DIFF(jbo.end_time, jbo.creation_time, MILLISECOND) time_ms
             FROM
                 FROM `region-us`.INFORMATION_SCHEMA.JOBS jbo
             WHERE
                 DATE(creation_time) >= CURRENT_DATE - num_days_to_scan
                 AND jbo.end_time > jbo.start_time
                 AND jbo.error_result IS NULL
                 AND jbo.statement_type != 'SCRIPT'                
         )
 )
GROUP BY
 1,
 2
 ORDER BY day, bi_engine_mode ASC;
 