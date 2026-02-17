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
 * This script creates a table named, actively_read_tables_with_part_clust_info,
 * that contains a list of the most frequently read tables which are:
 *     - not partitioned
 *     - not clustered
 *     - neither partitioned nor clustered
 */

DECLARE projects ARRAY<STRING> DEFAULT (
  SELECT ARRAY_AGG(DISTINCT project_id)
  FROM optimization_workshop.table_read_patterns
);

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.actively_read_tables_with_part_clust_info
(
  project_id STRING,
  dataset_id STRING,
  table_id STRING,
  total_slot_ms FLOAT64,
  total_jobs INT64,
  max_slot_ms_job_url STRING,
  num_days_queried INT64,
  predicate_columns STRING,
  partitioning_column STRING,
  clustering_columns STRING,
  table_url STRING,
  logical_gigabytes FlOAT64,
  logical_terabytes FLOAT64
);

FOR p IN (
 SELECT project_id
 FROM
   UNNEST(projects) project_id
)
DO 
BEGIN
  EXECUTE IMMEDIATE FORMAT("""
  INSERT INTO optimization_workshop.actively_read_tables_with_part_clust_info
  SELECT
    rp.* EXCEPT(predicates),
    ARRAY_TO_STRING(ARRAY_CONCAT_AGG((
      SELECT
        ARRAY_AGG(predicate_column_counts.column || ':' || predicate_column_counts.cnt)
      FROM (
        SELECT
          STRUCT(predicate.column, COUNT(predicate.column) AS cnt) AS predicate_column_counts
        FROM UNNEST(predicates) predicate
        GROUP BY predicate.column
      ))), ', ') AS predicate_columns,
    partitioning_column,
    clustering_columns,
    bqutil.fn.table_url(rp.project_id || '.' || rp.dataset_id || '.' || rp.table_id) AS table_url,
    SUM(SAFE_DIVIDE(s.total_logical_bytes, POW(2,30))) AS logical_gigabytes,
    SUM(SAFE_DIVIDE(s.total_logical_bytes, POW(2,40))) AS logical_terabytes,
  FROM
    `region-us.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION` s
  JOIN (
    SELECT
      project_id,
      dataset_id,
      table_id,
      ARRAY_CONCAT_AGG(predicates) AS predicates,
      SUM(stage_slot_ms) AS total_slot_ms,
      COUNT(DISTINCT job_id) AS total_jobs,
      ANY_VALUE(job_url HAVING MAX(total_slot_ms)) AS max_slot_ms_job_url,
      COUNT(DISTINCT date) AS num_days_queried,
    FROM optimization_workshop.table_read_patterns
    GROUP BY
      project_id,
      dataset_id,
      table_id
    ) rp
    ON (s.project_id = rp.project_id AND s.table_schema = rp.dataset_id AND s.table_name = rp.table_id)
  JOIN (
    SELECT
      table_catalog,
      table_schema,
      table_name,
      STRING_AGG(IF(IS_PARTITIONING_COLUMN="YES", column_name, CAST(NULL AS STRING))) AS partitioning_column,
      STRING_AGG(
        CASE WHEN clustering_ordinal_position IS NOT NULL THEN column_name END
        ORDER BY CLUSTERING_ORDINAL_POSITION
      ) AS clustering_columns
    FROM `%s.region-us.INFORMATION_SCHEMA.COLUMNS`
    GROUP BY 1,2,3
  ) c ON (s.project_id = c.table_catalog AND s.table_schema = c.table_schema AND s.table_name = c.table_name)
  GROUP BY
    project_id,
    dataset_id,
    table_id,
    table_url,
    total_slot_ms,
    total_jobs,
    max_slot_ms_job_url,
    num_days_queried,
    partitioning_column,
    clustering_columns,
    total_slot_ms;
  """,
  p.project_id);
EXCEPTION WHEN ERROR THEN SELECT @@error.message; --ignore errors
END;
END FOR;
