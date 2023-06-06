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

DECLARE start_date DATE DEFAULT CURRENT_DATE - 30;
DECLARE end_date DATE DEFAULT CURRENT_DATE;
DECLARE projects ARRAY<STRING> DEFAULT (
  SELECT 
    ARRAY_AGG(project_id)
  FROM(
    SELECT project_id
    FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
    WHERE DATE(creation_time) >= CURRENT_DATE - 30
    GROUP BY 1
    ORDER BY SUM(total_bytes_billed) DESC
    LIMIT 10
  )
);

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.top_bytes_scanning_queries_by_hash
(
  Query_Pattern STRING,
  Query_No_Literals STRING,
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
  Users_Email STRING,
  Labels STRING
);

CREATE TEMP FUNCTION sanitize_query(query STRING)
RETURNS STRING AS(
    LOWER(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(query, r"'(.*?)'", "," -- remove literals between single quotes
                                ), r"[0-9]+",","                      -- remove numbers
                            ),r'"(.*?)"', ","                         -- remove literals between double quotes
                        ),
                        '`',''                                        -- remove the '`' character
                    ),
                    ' ',''                                            -- remove empty spaces
                ),
                '\n',''                                               -- remove new line characters
            ),
            ',',''                                                    -- remove commas
        )
    )
);

FOR p IN (
 SELECT project_id
 FROM
   UNNEST(projects) project_id
)
DO
BEGIN
EXECUTE IMMEDIATE FORMAT("""
INSERT INTO optimization_workshop.top_bytes_scanning_queries_by_hash
SELECT
    TO_BASE64(MD5(query_no_literals))                                       AS Query_Pattern,
    query_no_literals                                                       AS Query_No_Literals,
    ANY_VALUE(query_raw)                                                    AS Query_Raw_Sample,
    SPLIT(ANY_VALUE(job_ids)[OFFSET(0)], '_')[OFFSET(0)]                    AS Job_Origin,
    Ref_Tables                                                              AS Ref_Tables,
    COUNT(DISTINCT creation_dt)                                             AS Days_Active,
    SUM(job_count)                                                          AS Job_Count,
    CAST(AVG(job_count) AS INT64)                                           AS Avg_Job_Count_Active_Days,
    Project_Id                                                              AS Project_Id,
    'us'                                                                      AS BQ_Region,
    Reservation_Id                                                          AS Reservation_Id,
    CAST(SUM(total_gigabytes_processed) AS INT64)                           AS Total_Gigabytes_Processed,
    CAST(SUM(total_gigabytes_processed)/sum(job_count) AS INT64)            AS Total_Gigabytes_Processed_Per_Job,
    CAST(AVG(total_gigabytes_processed) AS INT64)                           AS Avg_Gigabytes_Processed,
    CAST(SUM(total_slot_hours_per_day) AS INT64)                            AS Total_Slot_Hours,
    CAST(AVG(total_slot_hours_per_day) AS INT64)                            AS Avg_Total_Slot_Hours_per_Active_Day,
    CAST(AVG(avg_job_duration_seconds) AS INT64)                            AS Avg_Job_Duration_Seconds,
    ANY_VALUE(job_ids)                                                      AS Any_Job_Ids,
    STRING_AGG(DISTINCT users_emails)                                       AS Users_Email,
    STRING_AGG(DISTINCT labels_concat)                                      AS Labels
FROM (
    SELECT
        query_no_literals                                       AS query_no_literals,
        ANY_VALUE(query_raw)                                    AS query_raw,
        ref_tables                                              AS ref_tables,
        creation_dt                                             AS creation_dt,
        CASE
            WHEN FORMAT_DATE('%%A', creation_dt) IN ('Saturday', 'Sunday') THEN 1
            ELSE
                0
            END                                                 AS weekend_flag,
        COUNT(*)                                                AS job_count,
        ARRAY_AGG(job_id ORDER BY total_slot_ms DESC LIMIT 10)  AS job_ids,
        project_id                                              AS project_id,
        reservation_id                                          AS reservation_id,
        SUM(total_slot_ms) / (1000 * 60 * 60)                   AS total_slot_hours_per_day,
        SUM(total_bytes_processed) / (1024 * 1024 * 1024)       AS total_gigabytes_processed, 
        AVG(job_duration_seconds)                               AS avg_job_duration_seconds,
        ARRAY_AGG(DISTINCT user_email)                        AS user_email,
        STRING_AGG(DISTINCT labels_concat)                                           AS labels_concat
    FROM (
        SELECT
            sanitize_query(query)                                                           AS query_no_literals,
            query                                                                           AS query_raw,
            DATE(jbp.creation_time)                                                         AS creation_dt,
            jbp.project_id                                                                  AS project_id,
            jbp.reservation_id                                                              AS reservation_id,
            jbp.job_id                                                                      AS job_id,
            jbp.total_bytes_processed                                                       AS total_bytes_processed,
            jbp.total_slot_ms                                                               AS total_slot_ms,
            jbp.total_slot_ms / TIMESTAMP_DIFF(jbp.end_time, jbp.start_time, MILLISECOND)   AS slots,
            TIMESTAMP_DIFF(jbp.end_time, jbp.start_time, SECOND)                            AS job_duration_seconds,
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
            user_email,
            FORMAT("%%T", ARRAY_CONCAT_AGG(labels))                                          AS labels_concat 
        FROM  
            `%s.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT as jbp
        JOIN 
            UNNEST(referenced_tables) ref_tables
        WHERE 
            DATE(jbp.creation_time) BETWEEN %T AND %T
            AND jbp.end_time > jbp.start_time
            AND jbp.error_result IS NULL
            AND jbp.job_type = 'QUERY'
            AND jbp.statement_type != 'SCRIPT'
            AND ref_tables.table_id not like '%%INFORMATION_SCHEMA%%' 
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9,10, user_email
    )
    GROUP BY 1, 3, 4, 5, project_id, reservation_id)
JOIN
    UNNEST(user_email) as users_emails
GROUP BY
    query_no_literals,
    ref_tables,
    project_id,
    BQ_Region,
    reservation_id
ORDER BY Total_Gigabytes_Processed DESC
LIMIT 200;
""",
p.project_id, start_date, end_date);
EXCEPTION WHEN ERROR THEN SELECT @@error.message; --ignore errors
END;
END FOR;
