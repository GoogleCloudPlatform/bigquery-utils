/*
 * Copyright 2020 Google LLC
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

/*Reference - https://cloud.google.com/bigquery/docs/information-schema-tables#advanced_example */

CREATE OR REPLACE FUNCTION fn.MakePartitionByExpression(
  column_name STRING, data_type STRING
) AS (
  IF(
    column_name = '_PARTITIONTIME',
    'DATE(_PARTITIONTIME)',
    IF(
      data_type = 'TIMESTAMP',
      CONCAT('DATE(', column_name, ')'),
      column_name
    )
  )
);

CREATE OR REPLACE FUNCTION fn.MakePartitionByClause(
  columns ARRAY<STRUCT<column_name STRING, data_type STRING, is_nullable STRING,  is_partitioning_column STRING, clustering_ordinal_position INT64>>
) AS (
  IFNULL(
    CONCAT(
      'PARTITION BY ',
      (SELECT fn.MakePartitionByExpression(column_name, data_type)
       FROM UNNEST(columns) WHERE is_partitioning_column = 'YES'),
      '\n'),
    ''
  )
);

CREATE OR REPLACE FUNCTION fn.MakeClusterByClause(
  columns ARRAY<STRUCT<column_name STRING, data_type STRING, is_nullable STRING,  is_partitioning_column STRING, clustering_ordinal_position INT64>>
) AS (
  IFNULL(
    CONCAT(
      'CLUSTER BY ',
      (SELECT STRING_AGG(column_name, ', ' ORDER BY clustering_ordinal_position)
        FROM UNNEST(columns) WHERE clustering_ordinal_position IS NOT NULL),
      '\n'
    ),
    ''
  )
);

CREATE OR REPLACE FUNCTION fn.MakeNullable(data_type STRING, is_nullable STRING)
AS (
  IF(not STARTS_WITH(data_type, 'ARRAY<') and is_nullable = 'NO', ' NOT NULL', '')
);

CREATE OR REPLACE FUNCTION fn.MakeColumnList(
  columns ARRAY<STRUCT<column_name STRING, data_type STRING, is_nullable STRING,  is_partitioning_column STRING, clustering_ordinal_position INT64>>
) AS (
  IFNULL(
    CONCAT(
      '(\n',
      (SELECT STRING_AGG(CONCAT('  ', column_name, ' ', data_type,  fn.MakeNullable(data_type, is_nullable)), ',\n')
       FROM UNNEST(columns)),
      '\n)\n'
    ),
    ''
  )
);

CREATE OR REPLACE FUNCTION fn.MakeOptionList(
  options ARRAY<STRUCT<option_name STRING, option_value STRING>>
) AS (
  IFNULL(
    CONCAT(
      'OPTIONS (\n',
      (SELECT STRING_AGG(CONCAT('  ', option_name, '=', option_value), ',\n') FROM UNNEST(options)),
      '\n)\n'),
    ''
  )
);

/* Prerequisite functions completed. Start of the Procedure */

CREATE OR REPLACE PROCEDURE
  procedure.get_table_ddl (IN fully_qualified_table_name string)

/*
Usage:
    call procedure.get_table_ddl('project.dataset.<pattern>%')
    call procedure.get_table_ddl('project:dataset.<pattern>%')
    call procedure.get_table_ddl('project.dataset.%') to get all ddl's in a dataset
 Project name is mandatory because we can always refer to tables in a Project different from where we are calling the function. At this time this procedure only generates ddl for BigQuery native tables. Does not provide ddl for External tables, views.
*/
  
  /* Authors - 
  BigQuery Information Schema Team
  Jignesh Mehta - jigmehta@google.com
  Praveen Akunuru - pakunuru@google.com
  */


BEGIN
EXECUTE IMMEDIATE
  """
WITH Components AS (
  SELECT
    CONCAT("`", table_catalog, ".", table_schema, ".", table_name,"`") AS table_name,
    ARRAY_AGG(
      STRUCT(column_name, data_type, is_nullable, is_partitioning_column, clustering_ordinal_position)
      ORDER BY ordinal_position
    ) AS columns,
    (SELECT ARRAY_AGG(STRUCT(option_name, option_value))
     FROM `region-us.INFORMATION_SCHEMA.TABLE_OPTIONS` AS t2
     WHERE t.table_name = t2.table_name) AS options
  FROM `region-us.INFORMATION_SCHEMA.TABLES` AS t
  LEFT JOIN `region-us.INFORMATION_SCHEMA.COLUMNS`
  USING (table_catalog, table_schema, table_name)
  WHERE table_type = 'BASE TABLE'
  and UPPER(table_catalog) = UPPER(?)
  and UPPER(table_schema) = UPPER(?)
  and UPPER(table_name) like UPPER(?)
  GROUP BY table_catalog, table_schema, t.table_name
)
SELECT
   CONCAT('CREATE OR REPLACE TABLE ',
    table_name,
    fn.MakeColumnList(columns),
    fn.MakePartitionByClause(columns),
    fn.MakeClusterByClause(columns),
    fn.MakeOptionList(options)) as table_ddl
FROM Components

"""
/* Using named variables or any other functions below is causing BQ to treat this as multiple statements making the user experience sub-optimal. Using REGEXP_EXTRACT_ALL() due to that reason. */
USING
  REGEXP_EXTRACT_ALL(fully_qualified_table_name, "[^(\\.|:)]*")[ORDINAL(1)],
  REGEXP_EXTRACT_ALL(fully_qualified_table_name, "[^(\\.|:)]*")[ORDINAL(3)],
  REGEXP_EXTRACT_ALL(fully_qualified_table_name, "[^(\\.|:)]*")[ORDINAL(5)]; 
  
EXCEPTION
    WHEN ERROR THEN 
	SELECT ''' Usage: \n\t call procedure.get_table_ddl('project.dataset.<pattern>%') or \n\t call procedure.get_table_ddl('project:dataset.<pattern>%') or \n\t call procedure.get_table_ddl('project.dataset.%')  \n\n Project name is mandatory because we can always refer to tables in a Project different from where we are calling the function. At this time this procedure only generates ddl for BigQuery native tables. Does not provide ddl for External tables, views. ''' as ERROR_MSG;
END;
