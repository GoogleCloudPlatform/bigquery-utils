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
 * This script creates a table that contains daily slot consumption information
 * about BigQuery jobs for reservation within the past 30 days.
 *
 * 30 days is the default timeframe, but you can change this by setting the
 * num_days_to_scan variable to a different value.
 */
/* ==========================================================================
 Provides the following metrics at the daily level for each reservation_id:-
 job_count - Total Daily Jobs
 ------------ Slot Time Metrics ----------------------------------------------
 avg_total_slot_secs       Average Slot Secs across Jobs
 median_total_slot_secs    Median Slot Secs across Jobs
 p80_total_slot_secs       p80 Slot Secs across Jobs
 total_slot_hours          Totals Slot Hours
 ------------ Job Time Metrics -----------------------------------------------
 avg_time_secs             Average Time across Jobs
 median_time_secs          Median Time across Jobs
 p80_time_secs             p80 Time across Jobs
 ------------- Bytes Scanned Metrics -----------------------------------------
 avg_gb_scanned            Average GB scanned
 p80_gb_scanned            p80 GB scanned
 total_tb_scanned          Total TB scanned per day
 ------------- Bytes Shuffled Metrics ----------------------------------------
 avg_gb_shuffled           Average GB shuffled
 p80_gb_shuffled           p80 GB shuffled
 total_tb_shuffled         Total TB shuffled
 ------------- Bytes Spilled Metrics ----------------------------------------
 avg_gb_spilled            Average GB spilled to disk
 p80_gb_spilled            p80 GB spilled to disk
 total_tb_spilled          Total TB spilled to disk
 
 Parameters:
 num_days_to_scan  - the number of days to look back, range
 ============================================================================= */
DECLARE num_days_to_scan INT64 DEFAULT 30;

CREATE SCHEMA IF NOT EXISTS optimization_workshop;

CREATE
OR REPLACE TABLE optimization_workshop.daily_reservation_analysis AS
SELECT
    day,
    reservation_id,
    COUNT(*) as job_count,
    AVG(total_slot_ms) / 1000 avg_total_slot_secs,
    MAX(median_total_slot_ms) / 1000 median_total_slot_secs,
    MAX(p80_total_slot_ms) / 1000 p80_total_slot_secs,
    SUM(total_slot_ms) / 1000 / 60 / 60 total_slot_hours,
    AVG(time_secs) avg_time_secs,
    MAX(median_time_secs) median_time_secs,
    SUM(time_secs) / 60 / 60 total_time_hours,
    MAX(p80_time_secs) p80_time_secs,
    AVG(bytes_scanned) / POW(1024, 3) avg_gb_scanned,
    MAX(p80_bytes_scanned) / POW(1024, 3) p80_gb_scanned,
    SUM(bytes_scanned) / POW(1024, 4) total_tb_scanned,
    AVG(bytes_shuffled) / POW(1024, 3) avg_gb_shuffled,
    MAX(p80_bytes_shuffled) / POW(1024, 3) p80_gb_shuffled,
    SUM(bytes_shuffled) / POW(1024, 4) total_tb_shuffled,
    AVG(bytes_spilled) / POW(1024, 3) avg_gb_spilled,
    MAX(p80_bytes_spilled) / POW(1024, 3) p80_gb_spilled,
    SUM(bytes_spilled) / POW(1024, 4) total_tb_spilled,
FROM
    (
        SELECT
            day,
            reservation_id,
            total_slot_ms,
            PERCENTILE_CONT(total_slot_ms, 0.5) OVER (PARTITION BY day, reservation_id) as median_total_slot_ms,
            PERCENTILE_CONT(total_slot_ms, 0.8) OVER (PARTITION BY day, reservation_id) as p80_total_slot_ms,
            time_secs,
            PERCENTILE_CONT(time_secs, 0.5) OVER (PARTITION BY day, reservation_id) as median_time_secs,
            PERCENTILE_CONT(time_secs, 0.8) OVER (PARTITION BY day, reservation_id) as p80_time_secs,
            total_bytes_scanned bytes_scanned,
            PERCENTILE_CONT(total_bytes_scanned, 0.8) OVER (PARTITION BY day, reservation_id) as p80_bytes_scanned,
            bytes_shuffled,
            PERCENTILE_CONT(bytes_shuffled, 0.8) OVER (PARTITION BY day, reservation_id) as p80_bytes_shuffled,
            bytes_spilled,
            PERCENTILE_CONT(bytes_spilled, 0.8) OVER (PARTITION BY day, reservation_id) as p80_bytes_spilled
        FROM
            (
                SELECT
                    DATE(jbo.creation_time, "US/Central") as day,
                    reservation_id,
                    job_id,
                    total_slot_ms,
                    TIMESTAMP_DIFF(jbo.end_time, jbo.creation_time, SECOND) time_secs,
                    total_bytes_billed total_bytes_scanned,
                    (
                        SELECT
                            SUM(stage.shuffle_output_bytes)
                        FROM
                            UNNEST(job_stages) stage
                    ) bytes_shuffled,
                    (
                        SELECT
                            SUM(stage.shuffle_output_bytes_spilled)
                        FROM
                            UNNEST(job_stages) stage
                    ) bytes_spilled
                FROM
                    `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION jbo
                WHERE
                    DATE(jbo.creation_time, "US/Central") >= CURRENT_DATE - num_days_to_scan
                    AND jbo.end_time > jbo.start_time
                    AND jbo.error_result IS NULL
                    AND jbo.statement_type != 'SCRIPT'
            )
    )
GROUP BY
    1,
    2;
