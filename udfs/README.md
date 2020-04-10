# BigQuery UDFs

User-defined functions
([UDFs](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions))
are a feature of SQL supported by BigQuery that enables a user to create a
function using another SQL expression or JavaScript. These functions accept
columns of input and perform actions, returning the result of those actions as a
value.

## Community and Migration Functions

The [community](/udfs/community) folder contains community-contributed functions
that perform some actions in BigQuery. The [migration](/udfs/migration) folder
contains sub-folders such as [teradata](/udfs/migration/teradata),
[redshift](/udfs/migration/redshift), and [oracle](/udfs/migration/oracle) which
contain community-contributed functions that replicate the behavior of
proprietary functions in other data warehouses. These functions can help you
achieve feature parity in a migration from another data warehouse to BigQuery.

## Using the UDFs

All UDFs within this repository will be automatically created under the `bqutil`
project under publicly shared datasets. Queries can then reference the shared
UDFs via `bqutil.<dataset>.<function>()`.

![Alt text](/images/public_udf_architecture.png?raw=true "Public UDFs")

If you want to create and use the UDFs from this repository in your own BigQuery
dataset, you must provide your own dataset name in the following `bqutil`
command:

`bq query --project_id YOUR_PROJECT_ID --dataset_id YOUR_DATASET_ID
--nouse_legacy_sql < SQL_FILE_NAME`

Once the UDF is created in your BigQuery project, you can invoke it using the
fully-qualified UDF name (e.g. `<PROJECT>.<DATASET>.<UDF_NAME>()`).

If you are pasting and executing the UDF DDL statement in the BigQuery Console,
you must modify the UDF name to include your desired dataset, and optionally,
the project ID:

`CREATE OR REPLACE FUNCTION your_dataset.function_name() . . .`

or

`CREATE OR REPLACE FUNCTION project_id.your_dataset.function_name() . . .`

## Contributing UDFs

If you are interested in contributing UDFs to this repository, please see the
[instructions](/udfs/CONTRIBUTING.md) to get started.
