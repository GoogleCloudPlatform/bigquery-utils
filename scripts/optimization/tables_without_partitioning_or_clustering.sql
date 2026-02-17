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
 * This script creates a table named, tables_without_part_clust,
 * that contains a list of the largest tables which are:
 *     - not partitioned
 *     - not clustered
 *     - neither partitioned nor clustered
 */

DECLARE projects ARRAY<STRING> DEFAULT (
  SELECT 
    ARRAY_AGG(project_id)
  FROM(
    SELECT project_id
    FROM `region-us.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION`
    WHERE NOT deleted 
    GROUP BY 1
    ORDER BY SUM(total_logical_bytes) DESC
    LIMIT 100
  )
);

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.tables_without_part_clust
(
  table_catalog STRING,
  table_schema STRING,
  table_name STRING,
  table_url STRING,
  partitioning_column STRING,
  clustering_columns STRING,
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
  EXECUTE IMMEDIATE FORMAT(
  """  
  INSERT INTO
    optimization_workshop.tables_without_part_clust
  SELECT
    s.table_catalog,
    s.table_schema,
    s.table_name,
    bqutil.fn.table_url(s.table_catalog || '.' || s.table_schema || '.' || s.table_name) AS table_url,
    partitioning_column,
    clustering_columns,
    SUM(SAFE_DIVIDE(s.total_logical_bytes, POW(2,30))) AS logical_gigabytes,
    SUM(SAFE_DIVIDE(s.total_logical_bytes, POW(2,40))) AS logical_terabytes,
  FROM
    `region-us.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION` s
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
  WHERE
    clustering_columns IS NULL OR partitioning_column IS NULL
  GROUP BY
    s.table_catalog,
    s.table_schema,
    s.table_name,
    table_url,
    partitioning_column,
    clustering_columns;
  """,
  p.project_id);
EXCEPTION WHEN ERROR THEN SELECT @@error.message; --ignore errors
END;
END FOR;
