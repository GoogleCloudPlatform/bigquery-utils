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
- `DESTINATION_REGEX`:  A [Python Regex with named capturing groups](https://docs.python.org/3/howto/regex.html#non-capturing-and-named-groups)
for (defaults to `(?P<dataset>[\w\-_0-9]+)/(?P<table>[\w\-_0-9]+)/?(?P<partition>\$[\w\-_0-9]+)?/?(?P<batch>[\w\-_0-9]+)?/`): 
  - `dataset`: destintaion BigQuery Dataset
  - `table`: destination BigQuery Table
  - `partition`: (optional) destination BigQuery [partition decorator](https://cloud.google.com/bigquery/docs/creating-partitioned-tables#creating_an_ingestion-time_partitioned_table_when_loading_data)
    (For example $)
  - `batch`: (optional) indicates an incremental load from an upstream system (see [Handling Incremental Loads](#handling-incremental-loads))
- `MAX_BATCH_BYTES`: Max bytes for BigQuery Load job. (default 15 TB)

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
    "sourceFormat": "AVRO"
}
```
`gs://${INGESTION_BUCKET}/${BQ_DATASET}/_config/load.json`:
```json
{
    "writeDisposition": "WRTITE_TRUNCATE"
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

Note, external queries will consume query slots from this project's reservation
or count towards your on-demand billing. They will _not_ use free tie load slots.

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

### Partitions

#### Partition Table Decorators
Note that if the directory immediately before the triggering successfile starts with
a `$` it will be treated as a BigQuery Partition decorator for the destination table.

This means for:
```text
gs://${BUCKET}/foo/bar/$20201026/_SUCCESS
```
will trigger a load job with a destination table of `foo.bar$20201026`
This allows you to specify write disposition at the partition level.
This can be helpful in reprocessing scenarios where you'd want to `WRITE_TRUNCATE`
a partition that had some data quality issue.

#### Hive Partitioning
If your data will be uploaded to GCS from a hadoop system that uses the 
[supported default hive partitioning](https://cloud.google.com/bigquery/docs/hive-partitioned-loads-gcs#supported_data_layouts)
you can specify this in the [`hivePartitioningOptions`](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#hivepartitioningoptions)
key of `load.json` for that table.

Any non-trivial incremental loading to partitions should usually use the
Transformation SQL to define the `INSERT / MERGE / UPDATE / DELETE` logic into
the target BQ table as these DML semantics are much more flexible thant the load
job write dispositions.
Furthermore, using external query has the added benefit of circumventing the 
per load job bytes limits (default 15 TB) and commiting large partitions
atomically.

## Handling Incremental Loads
This solution introduces the concept of `batch_id` which uniquely identifies 
a batch of data committed by an upstream system that needs to be picked up as an
incremental load. You can again set the load job or external query configuration
at any parent folders `_config` prefix. This allows you dictate
"for this table any new batch should `WRITE_TRUNCATE` it's parent partition/table"
or "for that table any new batch should `WRITE_APPEND` to it's parent partition/table".

## Monitoring
Monitoring what data has been loaded by this solution should be done with the
BigQuery [`INFORMATION_SCHEMA` jobs metadata](https://cloud.google.com/bigquery/docs/information-schema-jobs)
If more granular data is needed about a particular job id 

### Job Naming Convention
All load or external query jobs will have a job id witha  prefix following this convention:
```python3
job_id_prefix=f"gcf-ingest-{dest_table_ref.dataset_id}-{dest_table_ref.table_id}-{1}-of-{1}-"
```

### Job Labels
All load or external query jobs are labelled with functional component and cloud function name.
```python3
DEFAULT_JOB_LABELS = {
    "component": "event-based-gcs-ingest",
    "cloud-function-name": getenv("FUNCTION_NAME"),
    "gcs-prefix": gs://bucket/prefix/for/this/ingest,
}
```
If the destination regex matches a batch group, there will be a `batch-id` label.

### Example INFROMATION SCHEMA Query
```sql
SELECT
   job_id,
   job_type,
   start_time,
   end_time,
   query,
   total_bytes_processed,
   total_slot_ms,
   destination_table
   state,
   (SELECT value FROM UNNEST(labels) WHERE key = "component") as component,
   (SELECT value FROM UNNEST(labels) WHERE key = "cloud-function-name") as cloud_function_name,
   (SELECT value FROM UNNEST(labels) WHERE key = "batch-id") as batch_id,
FROM
   `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
   (SELECT value FROM UNNEST(labels) WHERE key = "component") = "event-based-gcs-ingest"
```

## Triggers

### Pub/Sub Storage Notifications `_SUCCESS`
1. Trigger on `_SUCCESS` File to load all other files in that directory.
1. Trigger on non-`_SUCCESS` File will no-op
