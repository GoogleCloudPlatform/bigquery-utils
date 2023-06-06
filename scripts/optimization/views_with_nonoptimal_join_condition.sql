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
CREATE OR REPLACE TABLE optimization_workshop.views_with_nonoptimal_join_condition
(
  view_name STRING,
  view_url STRING,
  view_definition STRING,
  join_conditions ARRAY<STRING>
);

FOR p IN (
 SELECT project_id
 FROM
   UNNEST(projects) project_id
)
DO 
BEGIN
  EXECUTE IMMEDIATE FORMAT(r"""
  INSERT INTO optimization_workshop.views_with_nonoptimal_join_condition
  SELECT
  table_name AS view_name,
  `bigquery-public-data`.persistent_udfs.table_url(table_catalog || '.' || table_schema || '.' || table_name) AS view_url,
  view_definition,
  REGEXP_EXTRACT_ALL(REPLACE(UPPER(view_definition), " ON ", "\nON "), r"ON\s+(?:TRIM|UPPER|LOWER)+.*?=.*") AS join_conditions
  FROM
    `%s.region-us.INFORMATION_SCHEMA.VIEWS`
  WHERE 
    ARRAY_LENGTH(REGEXP_EXTRACT_ALL(REPLACE(UPPER(view_definition), " ON ", "\nON "), r"ON\s+(?:TRIM|UPPER|LOWER)+.*?=.*")) >= 1
  """,
  p.project_id);
EXCEPTION WHEN ERROR THEN SELECT @@error.message; --ignore errors
END;
END FOR;
