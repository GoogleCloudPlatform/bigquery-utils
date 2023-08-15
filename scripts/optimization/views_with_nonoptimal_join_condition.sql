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
 * This script creates a table named, views_with_nonoptimal_join_condition,
 * that contains a list of views with non-optimal join conditions.
 */

DECLARE projects ARRAY<STRING> DEFAULT (
  SELECT ARRAY_AGG(DISTINCT project_id)
  FROM `region-us.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION`
  WHERE NOT deleted
);

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.views_with_nonoptimal_join_condition
(
  view_name STRING,
  view_url STRING,
  view_definition STRING,
  join_conditions ARRAY<STRING>
);

CREATE TEMP FUNCTION extract_nonoptimal_join_conditions(view_definition STRING) AS(
  ARRAY_CONCAT(
    REGEXP_EXTRACT_ALL(
      REGEXP_REPLACE(UPPER(view_definition), r"\sON\s", "\nON "),
      r"\nON\s+[A-Z_]+?\([^=]*?=[^=]*"
    ),
    REGEXP_EXTRACT_ALL(
      REGEXP_REPLACE(UPPER(view_definition), r"\sON\s", "\nON "),
      r"\nON\s+[^=]*?=\s*[A-Z_]+?\([^=]*"
)));

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
  bqutil.fn.table_url(table_catalog || '.' || table_schema || '.' || table_name) AS view_url,
  view_definition,
  extract_nonoptimal_join_conditions(view_definition) AS join_conditions
  FROM
    `%s.region-us.INFORMATION_SCHEMA.VIEWS`
  WHERE 
    ARRAY_LENGTH(extract_nonoptimal_join_conditions(view_definition)) >= 1
  """,
  p.project_id);
EXCEPTION WHEN ERROR THEN SELECT @@error.message; --ignore errors
END;
END FOR;
