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

All UDFs within this repository are available under the `bqutil` project on publicly
shared datasets. Queries can then reference the shared UDFs via
`bqutil.<dataset>.<function>()`.

![Alt text](/images/public_udf_architecture.png?raw=true "Public UDFs")

If you want to create and use the UDFs from this repository in your BigQuery dataset,
you must provide your dataset name in the following `bqutil` command:

`bq query --project_id YOUR_PROJECT_ID --dataset_id YOUR_DATASET_ID
--nouse_legacy_sql < SQL_FILE_NAME`

Once you create the UDF in your BigQuery project, you can invoke it using the
fully-qualified UDF name (e.g. `<PROJECT>.<DATASET>.<UDF_NAME>()`).

If you are pasting and executing the UDF DDL statement in the BigQuery Console,
you must modify the UDF name to include your desired dataset, and optionally,
the project ID:

`CREATE OR REPLACE FUNCTION your_dataset.function_name() . . .`

or

`CREATE OR REPLACE FUNCTION project_id.your_dataset.function_name() . . .`

### Using JavaScript UDFs

When creating [JavaScript UDFs](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions#javascript-udf-structure) in your dataset, you need both to create the UDF and optionally copy the javascript library to your own Google Storage Bucket.

The base route for all the compiled JS libraries is `gs://bqutil-lib/bq_js_libs/`.

In the following example, we show how to create in your dataset the Levenshtein UDF function, that uses the `js-levenshtein-v1.1.6.js` library.

1. Copy the compiled library to your bucket:
`gsutil cp gs://bqutil-lib/bq_js_libs/js-levenshtein-v1.1.6.js gs://your-bucket`
2. Give permissions to the library. First, if you don't have [uniform bucket-level access](https://cloud.google.com/storage/docs/using-uniform-bucket-level-access) in your bucket, enable it: `gsutil uniformbucketlevelaccess set on gs://your-bucket`. Once done give the [Cloud Storage Object Viewer role](https://cloud.google.com/storage/docs/access-control/iam-roles) at the [bucket](https://cloud.google.com/storage/docs/access-control/using-iam-permissions#bucket-add) or [project](https://cloud.google.com/sdk/gcloud/reference/projects/add-iam-policy-binding) level to a user or group: `gsutil iam ch [user|group]:[user|group]@domain.com:roles/storage.objectViewer gs://your_bucket` 
3. Edit the [levenshtein.sql](community/levenshtein.sql) SQL file and replace the library path `library="${JS_BUCKET}/js-levenshtein-v1.1.6.js"` with your own path `library="gs://your-bucket/js-levenshtein-v1.1.6.js`
4. Create the SQL UDF passing the previously modified SQL file:
`bq query --project_id YOUR_PROJECT_ID --dataset_id YOUR_DATASET_ID --nouse_legacy_sql < levenshtein.sql`

## Contributing UDFs

If you are interested in contributing UDFs to this repository, please see the
[instructions](/udfs/CONTRIBUTING.md) to get started.
