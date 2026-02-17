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

DECLARE projects ARRAY<STRING> DEFAULT (
  SELECT 
    ARRAY_AGG(project_id)
  FROM(
    SELECT project_id
    FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
    WHERE DATE(creation_time) >= CURRENT_DATE - 30
    GROUP BY 1
    ORDER BY SUM(total_bytes_billed) DESC
    LIMIT 100
  )
);

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.viewable_queries_grouped_by_hash
(
  Query_Hash STRING,
  Query_Raw_Sample STRING,
  Job_Origin STRING,
  Ref_Tables STRING,
  Days_Active INT64,
  Job_Count INT64,
  Avg_Job_Count_Active_Days INT64,
  Project_Id STRING,
  BQ_Region STRING,
  Reservation_Id STRING,
  Total_Gigabytes_Processed INT64,
  Total_Gigabytes_Processed_Per_Job INT64,
  Avg_Gigabytes_Processed INT64,
  Total_Slot_Hours INT64,
  Avg_Total_Slot_Hours_per_Active_Day INT64,
  Avg_Job_Duration_Seconds INT64,
  Any_Job_Ids ARRAY<STRING>,
  User_Emails STRING,
  Labels STRING
);

FOR p IN (
 SELECT project_id
 FROM
   UNNEST(projects) project_id
)
DO
BEGIN
EXECUTE IMMEDIATE FORMAT("""
INSERT INTO optimization_workshop.viewable_queries_grouped_by_hash
SELECT
    query_hash                                                              AS Query_Hash,
    ANY_VALUE(query_raw)                                                    AS Query_Raw_Sample,
    SPLIT(ANY_VALUE(job_ids)[OFFSET(0)], '_')[OFFSET(0)]                    AS Job_Origin,
    Ref_Tables                                                              AS Ref_Tables,
    COUNT(DISTINCT creation_dt)                                             AS Days_Active,
    SUM(job_count)                                                          AS Job_Count,
    CAST(AVG(job_count) AS INT64)                                           AS Avg_Job_Count_Active_Days,
    Project_Id                                                              AS Project_Id,
    'us'                                                                    AS BQ_Region,
    Reservation_Id                                                          AS Reservation_Id,
    CAST(SUM(total_gigabytes_processed) AS INT64)                           AS Total_Gigabytes_Processed,
    CAST(SUM(total_gigabytes_processed)/sum(job_count) AS INT64)            AS Total_Gigabytes_Processed_Per_Job,
    CAST(AVG(total_gigabytes_processed) AS INT64)                           AS Avg_Gigabytes_Processed,
    CAST(SUM(total_slot_hours_per_day) AS INT64)                            AS Total_Slot_Hours,
    CAST(AVG(total_slot_hours_per_day) AS INT64)                            AS Avg_Total_Slot_Hours_per_Active_Day,
    CAST(AVG(avg_job_duration_seconds) AS INT64)                            AS Avg_Job_Duration_Seconds,
    ANY_VALUE(job_ids)                                                      AS Any_Job_Ids,
    STRING_AGG(DISTINCT user_emails_unnest)                                 AS User_Emails,
    STRING_AGG(DISTINCT labels_concat)                                      AS Labels
FROM (
    SELECT
        query_hash,
        ANY_VALUE(query_raw)                                    AS query_raw,
        ref_tables                                              AS ref_tables,
        creation_dt                                             AS creation_dt,
        project_id                                              AS project_id,
        reservation_id                                          AS reservation_id,
        COUNT(*)                                                AS job_count,
        ARRAY_AGG(job_id ORDER BY total_slot_ms DESC LIMIT 10)  AS job_ids,
        SUM(total_slot_ms) / (1000 * 60 * 60)                   AS total_slot_hours_per_day,
        SUM(total_bytes_processed) / POW(1024, 3)               AS total_gigabytes_processed, 
        AVG(job_duration_seconds)                               AS avg_job_duration_seconds,
        ARRAY_AGG(DISTINCT user_email)                          AS user_emails,
        STRING_AGG(DISTINCT labels_concat)                      AS labels_concat
    FROM (
        SELECT
            query_info.query_hashes.normalized_literals                                     AS query_hash,
            query                                                                           AS query_raw,
            DATE(jbp.creation_time)                                                         AS creation_dt,
            jbp.project_id                                                                  AS project_id,
            jbp.reservation_id                                                              AS reservation_id,
            jbp.job_id                                                                      AS job_id,
            jbp.total_bytes_processed                                                       AS total_bytes_processed,
            jbp.total_slot_ms                                                               AS total_slot_ms,
            jbp.total_slot_ms / TIMESTAMP_DIFF(jbp.end_time, jbp.start_time, MILLISECOND)   AS slots,
            TIMESTAMP_DIFF(jbp.end_time, jbp.start_time, SECOND)                            AS job_duration_seconds,
            user_email,
            STRING_AGG(ref_tables.project_id || '.' ||
                IF
                (STARTS_WITH(ref_tables.dataset_id, '_'),
                'TEMP',
                ref_tables.dataset_id) || '.' || ref_tables.table_id
                ORDER BY
                    ref_tables.project_id || '.' ||
                IF
                (STARTS_WITH(ref_tables.dataset_id, '_'),
                'TEMP',
                ref_tables.dataset_id) || '.' || ref_tables.table_id)                       AS ref_tables,
            FORMAT("%%T", ARRAY_CONCAT_AGG(labels))                                         AS labels_concat 
        FROM  
            `%s.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT as jbp
        JOIN 
            UNNEST(referenced_tables) ref_tables
        WHERE 
            DATE(jbp.creation_time) >= CURRENT_DATE - %i
            AND jbp.end_time > jbp.start_time
            AND jbp.error_result IS NULL
            AND jbp.job_type = 'QUERY'
            AND jbp.statement_type != 'SCRIPT'
            AND ref_tables.table_id not like '%%INFORMATION_SCHEMA%%' 
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    )
    GROUP BY 1, 3, 4, 5, 6)
JOIN
    UNNEST(user_emails) as user_emails_unnest
GROUP BY
    Query_Hash,
    Ref_Tables,
    Project_Id,
    BQ_Region,
    Reservation_Id;
""",
p.project_id, num_days_to_scan);
EXCEPTION WHEN ERROR THEN SELECT @@error.message; --ignore errors
END;
END FOR;
