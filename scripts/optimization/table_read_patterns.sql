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
 * The following script creates a table named, table_read_patterns,
 * that contains a list of the most frequently read tables within the
 * past 30 days.
 * 30 days is the default timeframe, but you can change this by setting the
 * num_days_to_scan variable to a different value.
 */

DECLARE num_days_to_scan INT64 DEFAULT 30;

DECLARE projects ARRAY<STRING> DEFAULT (
  SELECT ARRAY_AGG(DISTINCT project_id)
  FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
  WHERE DATE(creation_time) >= CURRENT_DATE - num_days_to_scan
);

/*
-- Uncomment this block if you just want to scan the top 1000 projects
-- by total bytes billed in the past 30 days.
DECLARE projects ARRAY<STRING> DEFAULT (
  SELECT ARRAY_AGG(project_id)
  FROM(
    SELECT project_id
    FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
    WHERE DATE(creation_time) >= CURRENT_DATE - num_days_to_scan
    GROUP BY 1
    ORDER BY SUM(total_bytes_billed) DESC
    LIMIT 1000
));
*/

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.table_read_patterns
(
  date DATE,
  project_id STRING,
  dataset_id STRING,
  table_id STRING,
  full_table_id STRING,
  job_id STRING,
  job_url STRING,
  parent_job_id STRING,
  parent_job_url STRING,
  reservation_id STRING,
  total_bytes_billed INT64,
  total_slot_ms INT64,
  creation_time TIMESTAMP,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  stage_name STRING,
  stage_id INT64,
  stage_slot_ms INT64,
  total_job_read_slot_ms INT64,
  records_read INT64,
  records_written INT64,
  shuffle_output_bytes INT64,
  shuffle_output_bytes_spilled INT64,
  parallel_inputs INT64,
  read_ratio_avg FLOAT64,
  read_ms_avg INT64,
  wait_ratio_avg FLOAT64,
  wait_ms_avg INT64,
  compute_ratio_avg FLOAT64,
  compute_ms_avg INT64,
  write_ratio_avg FLOAT64,
  write_ms_avg INT64,
  predicates ARRAY<STRUCT<operator STRING, column STRING, value STRING>>
) CLUSTER BY project_id, dataset_id, table_id;

CREATE TEMP FUNCTION mapColumns(where_clause STRING, column_list ARRAY<STRING>)
RETURNS STRING
LANGUAGE js AS r"""
  if (!where_clause) {
    return null;
  }
  column_list.forEach(function(col) {
    const tokens = col.split(":");
    where_clause = where_clause.replaceAll(tokens[0].trim(), tokens[1]);
  });
  return where_clause;
""";

FOR p IN (
 SELECT project_id
 FROM
   UNNEST(projects) project_id
)
DO
BEGIN
EXECUTE IMMEDIATE FORMAT("""
INSERT INTO optimization_workshop.table_read_patterns
  SELECT
    DATE(creation_time) AS date,
    project_id,
    IF(ARRAY_LENGTH(SPLIT(table_id, '.'))=2, SPLIT(table_id, '.')[OFFSET(0)], SPLIT(table_id, '.')[OFFSET(1)]) AS dataset_id,
    SPLIT(table_id, '.')[ORDINAL(ARRAY_LENGTH(SPLIT(table_id, '.')))] AS table_id,
    IF(ARRAY_LENGTH(SPLIT(table_id, '.'))=2, project_id || '.' || table_id, table_id) AS full_table_id,
    job_id,
    bqutil.fn.job_url(project_id || ':us.' || job_id) AS job_url,
    parent_job_id,
    bqutil.fn.job_url(project_id || ':us.' || parent_job_id) AS parent_job_url,
    reservation_id,
    total_bytes_billed,
    total_slot_ms,
    creation_time,
    start_time,
    end_time,
    stage_name,
    stage_id,
    stage_slot_ms,
    total_job_read_slot_ms,
    records_read,
    records_written,
    shuffle_output_bytes,
    shuffle_output_bytes_spilled,
    parallel_inputs,
    read_ratio_avg,
    read_ms_avg,
    wait_ratio_avg,
    wait_ms_avg,
    compute_ratio_avg,
    compute_ms_avg,
    write_ratio_avg,
    write_ms_avg,
    ARRAY(
      SELECT STRUCT(
        REGEXP_EXTRACT(predicate, '^[[:word:]]+') AS operator,
        REGEXP_EXTRACT(predicate, '[(]([[:word:]]+)') AS column,
        REGEXP_EXTRACT(predicate, '[,](.+)[)]') AS value )
      FROM UNNEST(filters) AS predicate
    ) AS predicates
  FROM (
    SELECT *,
      REGEXP_EXTRACT_ALL(
        mapcolumns(where_clause, projection_list),
        '[[:word:]]+[(][^()]*?[)]') AS filters
    FROM (
      SELECT
        jbp.project_id,
        job_id,
        parent_job_id,
        reservation_id,
        total_bytes_billed,
        total_slot_ms,
        creation_time,
        start_time,
        end_time,
        js.name AS stage_name,
        js.id AS stage_id,
        SUM(js.slot_ms) OVER (PARTITION BY job_id) AS total_job_read_slot_ms,
        js.slot_ms AS stage_slot_ms,
        js.records_read,
        js.records_written,
        js.shuffle_output_bytes,
        js.shuffle_output_bytes_spilled,
        js.parallel_inputs,
        js.read_ratio_avg,
        js.read_ms_avg,
        js.wait_ratio_avg,
        js.wait_ms_avg,
        js.compute_ratio_avg,
        js.compute_ms_avg,
        js.write_ratio_avg,
        js.write_ms_avg,
        SPLIT(js_steps.substeps[safe_OFFSET(0)], ',') AS projection_list,
        REPLACE(js_steps.substeps[safe_OFFSET(1)],'FROM ', '') AS table_id,
        js_steps.substeps[safe_OFFSET(2)] AS where_clause
      FROM `%s.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT jbp
      JOIN UNNEST(job_stages) AS js
      JOIN UNNEST(steps) AS js_steps
      WHERE
        DATE(creation_time) >= CURRENT_DATE - %i
        AND js_steps.kind = 'READ'
        AND jbp.job_type = 'QUERY'
        AND jbp.statement_type != 'SCRIPT'
        AND NOT cache_hit
        AND error_result IS NULL
        AND NOT EXISTS ( -- Exclude queries over INFORMATION_SCHEMA
          SELECT 1
          FROM UNNEST(js_steps.substeps) AS substeps
          WHERE substeps LIKE 'FROM %%.INFORMATION_SCHEMA.%%')
        AND EXISTS ( -- Only include substeps with a FROM clause
          SELECT 1
          FROM UNNEST(js_steps.substeps) AS substeps
          WHERE substeps LIKE 'FROM %%.%%')
))""", p.project_id, num_days_to_scan);
EXCEPTION WHEN ERROR THEN SELECT @@error.message; --ignore errors
END;
END FOR;
