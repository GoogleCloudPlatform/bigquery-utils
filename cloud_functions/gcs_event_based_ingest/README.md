<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [BigQuery Serverless Ingest](#bigquery-serverless-ingest)
  - [Tracking Table](#tracking-table)
  - [Environment Variables](#environment-variables)
  - [GCS Object Naming Convention](#gcs-object-naming-convention)
  - [Triggers](#triggers)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BigQuery Serverless Ingest

Flexible service for performing  BigQuery file loads to existing tables.
This service handles splitting load jobs when the data volume exceeds
the BigQuery 15TB load job limit. The goal of the service is to orchestrate
BigQuery Load Jobs to many bigquery datasets / tables from a single bucket
providing transparent configuration that is overridable at any level.

![architecture](img/bq-ingest.png)

## Environment Variables
- `SUCCESS_FILENAME`: Filename to trigger a load (defaults to `_SUCCESS`).

## GCS Object Naming Convention
### Data Files
Data should be ingested to a prefix containing destination dataset and table
like so:
`gs://${INGESTION_BUCKET}/${BQ_DATASET}/${BQ_TABLE_NAME}/*`
Note, the table prefix can contain multiple sub-prefixes for handling partitions
or for configuring historical / incremental loads differently.

### Configuration Files
The Ingestion has many optional configuration files that should live in
a special `_config/` prefix at the root of the bucket and/or under the dataset
and/or table and/or under the partition prefixes.

For example if you have the following files:
```text
gs://${INGESTION_BUCKET}/_config/load.json`
gs://${INGESTION_BUCKET}/${BQ_DATASET}/_config/load.json`
gs://${INGESTION_BUCKET}/${BQ_DATASET}/${BQ_TABLE_NAME}/_config/load.json
```
The json objects will be merged where key conflicts are resolved by config in
the closest directory to the data.
If the files contents were like this:
`gs://${INGESTION_BUCKET}/_config/load.json`:
```json
{
    "sourceFormat": "CSV",
    "writeDisposition": "WRITE_APPEND",
    "schemaUpdateOptions": ["ALLOW_FILED_RELAXATION"]
}
```
`gs://${INGESTION_BUCKET}/${BQ_DATASET}/_config/load.json`:
```json
{
    "sourceFormat": "AVRO",
}
```
`gs://${INGESTION_BUCKET}/${BQ_DATASET}/_config/load.json`:
```json
{
    "writeDisposition": "WRTITE_TRUNCATE",
}
```

The result of merging these would be:
```json
{
    "sourceFormat": "AVRO",
    "writeDisposition": "WRITE_TRUNCATE",
    "schemaUpdateOptions": ["ALLOW_FILED_RELAXATION"]
}
```
This configuration system gives us the ability to DRY up common defaults but
override them at whatever level is appropriate as new cases come up.

#### Transformation SQL
In some cases we may need to perform transformations on the files in GCS
before they can be loaded to BigQuery. This is handled by query on an
temporary external table over the GCS objects as a proxy for load job.
`gs://${INGESTION_BUCKET}/${BQ_DATASET}/${BQ_TABLE_NAME}/_config/bq_transform.sql`

Note, that the query should select from a `temp_ext` which will be a temporary
external table configured on the fly by the Cloud Function.
The query must handle the logic for inserting into the destination table.
This means it should use BigQuery DML to either `INSERT` or `MERGE` into the
destination table.
For example:
```sql
INSERT {dest_dataset}.{dest_table}
SELECT * FROM temp_ext
```
Note that `{dest_dataset}` and `{dest_table}` can be used to inject the dataset
and table inferred from the GCS path.


The query will be run with the appropriate [external table definitions](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#ExternalDataConfiguration)
defined in:
`gs://${INGESTION_BUCKET}/${BQ_DATASET}/${BQ_TABLE_NAME}/_config/external.json`
If this file is missing the external table will be assumed to be `PARQUET` format.

### Note on Partitions
Partitions and incremental loads should usually use the Transformation SQL to
define the semantics of the `INSERT / MERGE / UPDATE` into the target BQ table
as the DML semantics are much more flexible thant the load job write dispositions.
Furthermore, using external query has the added benefit of circumventing the 
15TB load job limits and commiting large partitions atomically.

## Triggers

### Pub/Sub Storage Notifications `_SUCCESS`
1. Trigger on `_SUCCESS` File to load all other files in that directory.
1. Trigger on non-`_SUCCESS` File will no-op
