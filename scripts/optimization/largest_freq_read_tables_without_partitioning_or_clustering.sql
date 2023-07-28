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
 * This script creates a table named, largest_freq_read_tables_without_part_clust, 
 * that contains a list of the most frequently read tables which are:
 *     - not partitioned
 *     - not clustered
 *     - neither partitioned nor clustered
 */

DECLARE projects ARRAY<STRING> DEFAULT (
  SELECT 
    ARRAY_AGG(project_id)
  FROM(
    SELECT
      SPlIT(table_id, '.')[OFFSET(0)] AS project_id,
    FROM
      optimization_workshop.table_read_patterns
    GROUP BY 1
    ORDER BY SUM(total_slot_ms) DESC
    LIMIT 10
  )
);

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.largest_freq_read_tables_without_part_clust
(
  table_catalog STRING,
  table_schema STRING,
  table_name STRING,
  table_url STRING,
  is_clustered BOOLEAN,
  is_partitioned BOOLEAN,
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
  INSERT INTO optimization_workshop.largest_freq_read_tables_without_part_clust
  SELECT
    s.table_catalog,
    s.table_schema,
    s.table_name,
    `bigquery-public-data`.persistent_udfs.table_url(s.table_catalog || '.' || s.table_schema || '.' || s.table_name) AS table_url,
    EXISTS(SELECT * FROM UNNEST(c.clustering) AS c WHERE c <> "NULL") is_clustered,
    EXISTS(SELECT * FROM UNNEST(c.partitioning) AS p WHERE p = "YES") is_partitioned,
    SUM(SAFE_DIVIDE(s.total_logical_bytes, POW(2,30))) AS logical_gigabytes,
    SUM(SAFE_DIVIDE(s.total_logical_bytes, POW(2,40))) AS logical_terabytes,
  FROM
    `region-us.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION` s
  JOIN (
    SELECT 
      table_catalog,
      table_schema,
      table_name,
      table_catalog || '.' || table_schema || '.' || table_name AS full_table_id,
      ARRAY_AGG(COALESCE(CAST(CLUSTERING_ORDINAL_POSITION AS STRING), "NULL")) AS clustering,
      ARRAY_AGG(IS_PARTITIONING_COLUMN) AS partitioning 
    FROM `%s.region-us.INFORMATION_SCHEMA.COLUMNS` 
    GROUP BY 
      table_catalog,
      table_schema,
      table_name
    ) c ON (s.project_id = c.table_catalog AND s.table_schema = c.table_schema AND s.table_name = c.table_name)
  WHERE
    NOT EXISTS(SELECT * FROM UNNEST(clustering) AS c WHERE c <> "NULL")
    OR NOT EXISTS(SELECT * FROM UNNEST(partitioning) AS p WHERE p = "YES")
  GROUP BY
    s.table_catalog,
    s.table_schema,
    s.table_name,
    table_url,
    is_clustered,
    is_partitioned
  ORDER BY 5 DESC
  LIMIT 100
  ;
  """,
  p.project_id);
EXCEPTION WHEN ERROR THEN SELECT @@error.message; --ignore errors
END;
END FOR;