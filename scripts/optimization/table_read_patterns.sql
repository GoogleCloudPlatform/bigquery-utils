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
  SELECT 
    ARRAY_AGG(project_id)
  FROM(
    SELECT project_id
    FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
    WHERE DATE(creation_time) >= CURRENT_DATE - num_days_to_scan
    GROUP BY 1
    ORDER BY SUM(total_bytes_billed) DESC
    LIMIT 100
  )
);

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.table_read_patterns
(
  table_id STRING,
  table_url STRING,
  day DATE,
  column STRING,
  operator STRING,
  value STRING,
  total_slot_ms INT64,
  num_occurrences INT64,
  job_count INT64,
  job_id_array ARRAY<STRING>,
  job_url_array ARRAY<STRING>
);

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
WITH table_read_patterns AS (
  SELECT
  DATE(creation_time) AS date,
  project_id,
  IF(ARRAY_LENGTH(SPLIT(table_id, '.'))=2, project_id || '.' || table_id, table_id) AS table_id,
  job_id,
  reservation_id,
  total_bytes_billed,
  total_slot_ms,
  creation_time,
  start_time,
  end_time,
  stage_name,
  stage_id,
  stage_slot_ms,
  total_stage_slot_ms,
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
  SELECT
    STRUCT( REGEXP_EXTRACT(predicate, '^[[:word:]]+') AS operator,
      REGEXP_EXTRACT(predicate, '[(]([[:word:]]+)') AS column,
      REGEXP_EXTRACT(predicate, '[,](.+)[)]') AS value )
  FROM
    UNNEST(filters) AS predicate ) AS predicates
  FROM (
  SELECT *,
    REGEXP_EXTRACT_ALL( mapcolumns(where_clause,
        projection_list), '[[:word:]]+[(][^()]*?[)]') AS filters
  FROM (
    SELECT
      jbp.project_id,
      job_id,
      reservation_id,
      total_bytes_billed,
      total_slot_ms,
      creation_time,
      start_time,
      end_time,
      js.name AS stage_name,
      js.id AS stage_id,
      SUM(js.slot_ms) OVER (PARTITION BY job_id) AS total_stage_slot_ms,
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
    FROM
      `%s.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT jbp
    JOIN
      UNNEST(job_stages) AS js
    JOIN
      UNNEST(steps) AS js_steps
    WHERE
      DATE(creation_time) >= CURRENT_DATE - %i
      AND js_steps.kind = 'READ'
      AND jbp.job_type = 'QUERY'
      AND jbp.statement_type != 'SCRIPT'
      AND NOT cache_hit
      AND error_result IS NULL
      AND NOT EXISTS ( -- Exclude queries over INFORMATION_SCHEMA
        SELECT
          1
        FROM
          UNNEST(js_steps.substeps) AS substeps
        WHERE
          substeps LIKE 'FROM %%.INFORMATION_SCHEMA.%%')
      AND EXISTS ( -- Only include substeps with a FROM clause
        SELECT
          1
        FROM
          UNNEST(js_steps.substeps) AS substeps
        WHERE
          substeps LIKE 'FROM %%.%%') 
) ) )
SELECT
  table_id,
  bqutil.fn.table_url(table_id) AS table_url,
  DATE(creation_time) as day,
  (SELECT STRING_AGG(COLUMN ORDER BY COLUMN) FROM UNNEST(predicates)) column_list,
  (SELECT STRING_AGG(operator ORDER BY COLUMN) FROM UNNEST(predicates)) operator_list,
  (SELECT STRING_AGG(value ORDER BY COLUMN) FROM UNNEST(predicates)) value_list,
  SUM(stage_slot_ms) AS total_slot_ms,
  COUNT(*) AS num_occurrences,
  COUNT(distinct job_id) as job_count,
  ARRAY_AGG(CONCAT(project_id,':us.',job_id) ORDER BY total_slot_ms LIMIT 10) AS job_id_array,
  ARRAY_AGG(bqutil.fn.job_url(project_id || ':us.' || job_id)) AS job_url_array,
FROM
  table_read_patterns
  GROUP BY 1,2,3,4,5,6;
""",
p.project_id, num_days_to_scan);
EXCEPTION WHEN ERROR THEN SELECT @@error.message; --ignore errors
END;
END FOR;
