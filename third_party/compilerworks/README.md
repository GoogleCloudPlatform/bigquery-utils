# Using Compilerworks UDFs in BigQuery

## Before You Start

In order to create and use the UDFs within your BigQuery workstreams, ensure that the correct IAM permissions are set.


* For creators of the UDFs ensure that they have the following permissions in the dataset of creation:
  * bigquery.routines.create
  * bigquery.jobs.create

* For query users of the UDFs please ensure that they have read access to the dataset where the udfs are stored along with the dataset with the actual data.
  * bigquery.routines.get
  * bigquery.jobs.create
  * bigquery.tables.getData

## Creating the UDFs

If you want to create and use the UDFs from this repository in your BigQuery dataset,
you must provide your dataset name in the following `bqutil` command:

`bq query --project_id YOUR_PROJECT_ID --dataset_id YOUR_DATASET_ID
--nouse_legacy_sql < cw_stdlib.sql`

Once you create the UDF in your BigQuery project, you can invoke it using the
fully-qualified UDF name (e.g. `<PROJECT>.<DATASET>.<UDF_NAME>()`).

If you are pasting and executing the UDF DDL statement in the BigQuery Console,
you must modify the UDF name to include your desired dataset, and optionally,
the project ID. Note that the default dataset name is cw_udf:

`CREATE OR REPLACE FUNCTION your_dataset.function_name() . . .`

or

`CREATE OR REPLACE FUNCTION project_id.your_dataset.function_name() . . .`