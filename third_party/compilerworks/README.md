# Using Compilerworks UDFs in BigQuery

## Before You Start

In order to create and use the UDFs within your BigQuery workstreams, ensure that the correct IAM permissions are set.


* For creators of the UDFs ensure that they have the following permissions in the dataset of creation:
  * bigquery.routines.create
  * bigquery.jobs.create

* Please ensure that users invoking these UDFs have read access to the dataset which contains the UDFs. You must also **perform one** of the following to ensure the UDFs will have access to read data:
  * Grant users access to the dataset(s), table(s), or  view(s) from which the UDFs read data
  * Implement [AUTHORIZED UDFS](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions#authorized_udf_example) to allow the UDFs to read data from datasets to which the invoking users don't have access.
  * bigquery.routines.get
  * bigquery.jobs.create
  * bigquery.tables.getData (optional if using Authorized UDFs)

## Creating the UDFs

If you want to create and use the UDFs from this repository in your BigQuery dataset,
you must provide your dataset name in the following `bqutil` command:

`bq query --project_id YOUR_PROJECT_ID --dataset_id YOUR_DATASET_ID
--nouse_legacy_sql < cw_stdlib.sql`

Once you create the UDF in your BigQuery project, you can invoke it using the
fully-qualified UDF name (e.g. `<PROJECT>.<DATASET>.<UDF_NAME>()`).

If you are pasting and executing the UDF DDL statement in the BigQuery Console,
you must modify the UDF name to include your desired dataset, and optionally,
the project ID.

`CREATE OR REPLACE FUNCTION your_dataset.function_name() . . .`

or

`CREATE OR REPLACE FUNCTION project_id.your_dataset.function_name() . . .`
