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
 * This script creates a table that maps a BigQuery Table to the IAM Principals 
 * that can read data from the BigQuery table.
 *
 * The IAM Principal could be assigned a role at the Table level, the Dataset level
 * or any level of Project, Folder or Org which are parents of the Dataset.
 * 
 * This script assumes the following:
 * 1. The Cloud Asset Inventory has been exported to tables in BigQuery under 
 *    project - <CAI_EXPORT_PROJECT> and dataset - <CAI_EXPORT_DATASET>
 * 2. The user has permissions to query <CAI_EXPORT_PROJECT>.<CAI_EXPORT_DATASET>
 * 3. All BigQuery permissions is handled via BigQuery predefined roles
 * 4. The BigQuery roles for reading data from a Table are
 *      a. roles/bigquery.dataEditor
 *      b. roles/bigquery.dataViewer
 *      c. roles/bigquery.dataOwner
 *      d. roles/bigquery.studioAdmin
 *      e. roles/bigquery.admin
 * 5. Replace <RESOURCE_TABLE> with the Resource table name from the CAI Export
 * 6. Replace <IAM_POLICY_TABLE> with the IAM Policy table name from the CAI Export
 *
 * The schema of the table - `bigquery_table_all_readers` is given in ./schema/table_all_readers_schema.json
 */
DECLARE read_date STRING DEFAULT "2023-12-01";

CREATE SCHEMA IF NOT EXISTS cai_analysis;

CREATE
OR REPLACE TABLE cai_analysis.bigquery_table_all_readers AS WITH bq_table_with_parents AS (
    SELECT
        name as bq_table,
        ARRAY_CONCAT(
            [name],
            [resource.parent],
            ARRAY(
                SELECT
                    CONCAT('//cloudresourcemanager.googleapis.com/', a)
                FROM
                    UNNEST(ancestors) as a
            )
        ) as parent_array
    FROM
        `<CAI_EXPORT_PROJECT>.<CAI_EXPORT_DATASET>.<RESOURCE_TABLE>`
    WHERE
        TIMESTAMP_TRUNC(readTime, DAY) = TIMESTAMP(read_date)
        AND asset_type = 'bigquery.googleapis.com/Table'
),
bq_readers AS (
    SELECT
        a.name,
        a.asset_type,
        b.role,
        ARRAY_CONCAT_AGG(b.members) as members,
    FROM
        `<CAI_EXPORT_PROJECT>.<CAI_EXPORT_DATASET>.<IAM_POLICY_TABLE>` a
        INNER JOIN UNNEST(iam_policy.bindings) as b
    WHERE
        TIMESTAMP_TRUNC(readTime, DAY) = TIMESTAMP(read_date)
        AND b.role IN (
            'roles/bigquery.dataEditor',
            'roles/bigquery.dataViewer',
            'roles/bigquery.dataOwner',
            'roles/bigquery.studioAdmin',
            'roles/bigquery.admin'
        )
    GROUP BY
        1,
        2,
        3
)
SELECT
    bqt.bq_table as table_name,
    ARRAY_AGG(
        STRUCT(
            bq_readers.name as parent_name,
            bq_readers.role,
            bq_readers.members
        )
    ) as inherited_readers
FROM
    bq_table_with_parents bqt
    INNER JOIN UNNEST(parent_array) as parents
    INNER JOIN bq_readers ON parents = bq_readers.name
GROUP BY
    1;
