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

DECLARE projects ARRAY<STRING> DEFAULT (
  SELECT 
    ARRAY_AGG(project_id)
  FROM(
    SELECT project_id
    FROM `region-us.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION`
    WHERE NOT deleted 
    GROUP BY 1
    ORDER BY SUM(total_logical_bytes) DESC
    LIMIT 10
  )
);

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.largest_tables_without_part_clust
(
  full_table_id STRING,
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
  INSERT INTO optimization_workshop.largest_tables_without_part_clust
  SELECT
    full_table_id,
    `bigquery-public-data`.persistent_udfs.table_url(full_table_id) AS table_url,
    EXISTS(SELECT * FROM UNNEST(clustering) AS c WHERE c <> "NULL") is_clustered,
    EXISTS(SELECT * FROM UNNEST(partitioning) AS p WHERE p = "YES") is_partitioned,
    SUM(SAFE_DIVIDE(total_logical_bytes, pow(2,30))) AS logical_gigabytes,
    SUM(SAFE_DIVIDE(total_logical_bytes, pow(2,40))) AS logical_terabytes,
  FROM
    `region-us.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION`
  JOIN (
    SELECT 
      table_catalog || '.' || table_schema || '.' || table_name AS full_table_id,
      ARRAY_AGG(COALESCE(CAST(CLUSTERING_ORDINAL_POSITION AS STRING), "NULL")) AS clustering,
      ARRAY_AGG(IS_PARTITIONING_COLUMN) AS partitioning 
    FROM `%s.region-us.INFORMATION_SCHEMA.COLUMNS`
    GROUP BY 1
    ) ON (project_id = table_catalog AND table_schema = table_schema AND table_name = table_name)
  WHERE
    NOT EXISTS(SELECT * FROM UNNEST(clustering) AS c WHERE c <> "NULL")
    OR NOT EXISTS(SELECT * FROM UNNEST(partitioning) AS p WHERE p = "YES")
  GROUP BY 1,2,3,4
  ORDER BY 5 DESC
  LIMIT 100
  ;
  """,
  p.project_id);
EXCEPTION WHEN ERROR THEN SELECT @@error.message; --ignore errors
END;
END FOR;
