# BigQuery Serverless Ingest

Flexible service for performing  BigQuery file loads to existing tables.
This service handles splitting load jobs when the data volume exceeds
the BigQuery 15TB load job limit. The goal of the service is to orchestrate
BigQuery Load Jobs to many bigquery datasets / tables from a single bucket
providing transparent configuration that is overridable at any level.

![architecture](img/bq-ingest.png)

## GCS Object Naming Convention
### Data Files
Data should be ingested to a prefix containing destination dataset and table
like so:
`gs://${INGESTION_BUCKET}/${BQ_DATASET}/${BQ_TABLE_NAME}/*`
Note, the table prefix can contain multiple sub-prefixes for handling partitions
or for configuring historical / incremental loads differently.

### Configurable Naming Convention with Regex
By Default we try to read dataset, table, partition (or yyyy/mm/dd/hh) and
batch id using the following python regex:
```python3
DEFAULT_DESTINATION_REGEX = (
    r"^(?P<dataset>[\w\-\._0-9]+)/"  # dataset (required)
    r"(?P<table>[\w\-_0-9]+)/?"      # table name (required)
    r"(?P<partition>\$[0-9]+)?/?"    # partition decortator (optional)
    r"(?P<yyyy>[0-9]{4})?/?"         # partition year (yyyy) (optional)
    r"(?P<mm>[0-9]{2})?/?"           # partition month (mm) (optional)
    r"(?P<dd>[0-9]{2})?/?"           # partition day (dd)  (optional)
    r"(?P<hh>[0-9]{2})?/?"           # partition hour (hh) (optional)
    r"(?P<batch>[\w\-_0-9]+)?/"      # batch id (optional)
)
```
you can see if this meets your needs in this [regex playground](https://regex101.com/r/5Y9TDh/2)
Otherwise you can override the regex by setting the `DESTINATION_REGEX` to
better fit your naming convention on GCS. Your regex must include
[Python Regex with named capturing groups](https://docs.python.org/3/howto/regex.html#non-capturing-and-named-groups)
for destination `dataset`, and `table`.
Note, that `dataset` can optionally, explicitly specify destination project
(i.e. `gs://${BUCKET}/project_id.dataset_id/table/....`) alternatively,
one can set the `BQ_PROJECT` environment variable to set to override the
default target project for datasets at the function level. The default behavior is to 
infer the project from Application Default Credential (the project in
which the Cloud Function is running, or the ADC configured in Google Cloud SDK
if invoked locally). This is useful in scenarios where a single deployment of
the Cloud Function is responsible for ingesting data into BigQuery tables in
projects other than the one it is deployed in. In these cases it is crucial to
ensure the service account that Cloud Functions is impersonating has the correct
permissions on all destination projects.

Your regex can optionally include  for
- `partition` must be BigQuery Partition decorator with leading `$`
- `yyyy`, `mm`, `dd`, `hr` partition year, month, day, and hour
(depending on your partition granularity)
- `batch` an optional batch id to indicate multiple uploads for this partition.

For example, if your datafiles were laid out like this:
```text
gs://${BUCKET}/${SOURCE_SYSTEM}/${DATASET}/${TABLE}/region=${LOCATION}/yyyy=${YEAR}/mm=${MONTH}/dd=${DAY}/hh=${HOUR}
```
i.e.
```text
gs://my-bucket/on-prem-edw/my_product/transactions/region=US/yyyy=2020/mm=01/dd=02/hh=03/_SUCCESS
```
Then you could use [this regex](https://regex101.com/r/OLpmg4/2):
```text
DESTINATION_REGEX='(?:[\w\-_0-9]+)/(?P<dataset>[\w\-_0-9]+)/(?P<table>[\w\-_0-9]+)/region=(?P<batch>[\w]+)/yyyy=(?P<yyyy>[0-9]{4})/mm=(?P<mm>[0-9]{2})/dd=(?P<dd>[0-9]{2})/hh=(?P<hh>[0-9]{2})/'
```
In this case we can take advantage of a more known rigid structure so our regex
is simpler (no optional capturing groups, optional slashes).
Note, we can use the `region=` string (which may have been partitioned on
in an  upstream system such as Hive) as a batch ID because we might expect that
an hourly partition might have multiple directories that upload to it.
(e.g. US, GB, etc). Because it is all named capturing groups we don't have any
strict ordering restrictions about batch id appearing before / after partition
information.

### Dealing with Different Naming Conventions in the Same Bucket
In most cases, it would be recommended to have separate buckets / deployment
of the Cloud Function for each naming convention as this typically means that
the upstream systems are governed by different teams.

Sometimes many upstream systems might be using different naming conventions
when uploading to the same bucket due to organizational constraints.
In this case you have two options:
1. Try to write a very flexible regex that handles all of your naming conventions with lots of optional groups.
[Regex101 is your friend!](https://regex101.com/)
1. Create separate Pub/Sub notifications and separate deployment of the
Cloud Function with the appropriate `DESTINATION_REGEX` environment variable for
each GCS prefix with a different naming convention.

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
    "schemaUpdateOptions": ["ALLOW_FIELD_RELAXATION"]
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
All load or external query jobs will have a job id with a  prefix following this convention:
```python3
job_id_prefix=f"gcf-ingest-{dest_table_ref.dataset_id}-{dest_table_ref.table_id}"
```
Note, the prefix `gcf-ingest-` is configurable with the `JOB_PREFIX` environment
variable.

### Job Labels
All load or external query jobs are labelled with functional component and
cloud function name.
```python3
DEFAULT_JOB_LABELS = {
    "component": "event-based-gcs-ingest",
    "cloud-function-name": getenv("FUNCTION_NAME"),
    "bucket-id": "<bucket-for-this-notification>"
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
   (SELECT value FROM UNNEST(labels) WHERE key = "component") = "gcf-ingest-"
```

## Triggers
GCS Object Finalize triggers can communicate with Cloud Functions directly or
via Pub/Sub topic. This function supports both reading bucket / object id from
the Pub/Sub attributes or the Cloud Functions event schema. Pub/Sub triggers
offer some additional features as opposed to Cloud Functions direct including
filtering notifications to a prefix within a bucket (rather than bucket wide)
and controlling message retention period for the Pub/Sub topic.
More info can be found here:
[Pub/Sub Storage Notification](https://cloud.google.com/storage/docs/pubsub-notifications)
[Cloud Functions direct trigger](https://cloud.google.com/functions/docs/tutorials/storage)
1. Trigger on `_SUCCESS` File to load all other files in that directory.
1. Trigger on non-`_SUCCESS` File will no-op

## Continuous Integration
We run the following CI checks to ensure code quality and avoid common pitfalls:
- [yapf](https://github.com/google/yapf)
- [flake8](https://flake8.pycqa.org/en/latest/)
- [isort](https://pypi.org/project/isort/)
- [mypy](https://mypy.readthedocs.io/en/stable/)
- [pylint](https://www.pylint.org/) (only on main sources not tests)
- [hadolint](https://github.com/hadolint/hadolint) for Dockerfile.ci

This CI process is defined in [cloudbuild.yaml](cloudbuild.yaml) and can be run
locally with [cloud-build-local](https://cloud.google.com/cloud-build/docs/build-debug-locally)
from the root of the repo with:
```bash
cloud-build-local --config=tools/cloud_functions/gcs_event_based_ingest/cloudbuild.yaml --dryrun=false .
```

### Optimizations / Philosophy
This CI system uses [kaniko cache to speed up builds](https://cloud.google.com/cloud-build/docs/kaniko-cache)
and defaults cache expiration to two weeks.
This notably does not pin python package versions so we know if one of our
dependencies or CI checks has been updated in a way that breaks this tool.
It's better for us to make a conscious decision to adopt new features or adjust
CI configs or pin older version depending on the type for failure.
This CI should be run on all new PRs and nightly.

### Just Running the Tests
#### Running in Docker
```bash
# Build Docker image
PROJECT_ID=$(gcloud config get-value project)
docker build -t gcr.io/$PROJECT_ID/gcs_event_based_ingest_ci -f Dockerfile.ci .
# Run unit tests
docker run --rm -it gcr.io/$PROJECT_ID/gcs_event_based_ingest_ci -k "not IT"
# Run integration tests
docker run --rm -it gcr.io/$PROJECT_ID/gcs_event_based_ingest_ci -k "IT"
# Run all tests
docker run --rm -it gcr.io/$PROJECT_ID/gcs_event_based_ingest_ci
```

#### Running on your local machine
Alternatively to the local cloudbuild or using the docker container to run your
tests, you can `pip3 install -r requirements-dev.txt` and select certain tests
to run with [`pytest`](https://docs.pytest.org/en/stable/usage.html). This is
mostly useful if you'd like to integrate with your IDE debugger.

Note that integration tests will spin up / tear down cloud resources that can
incur a small cost. These resources will be spun up based on your Google Cloud SDK
[Application Default Credentials](https://cloud.google.com/sdk/gcloud/reference/auth/application-default)

#### Pytest Fixtures
All Pytest fixtures are DRY-ed up into `tests/conftest.py`
This is mostly to share fixtures between the main integration test for the cloud function
and the integration tests for the backfill CLI.
See more info on sharing pytest fixtures in the [pytest docs](https://docs.pytest.org/en/stable/fixture.html#conftest-py-sharing-fixture-functions).

#### Running All Tests
```bash
pytest
```
#### Running Unit Tests Only
```bash
pytest -m "not IT"
```

#### Running Integration Tests Only
```bash
pytest -m IT
```

## Deployment
It is suggested to deploy this Cloud Function with the
[accompanying terraform module](terraform_module/gcs_ocn_bq_ingest_function/README.md)

### Google Cloud SDK
Alternatively, you can deploy with Google Cloud SDK:

#### Pub/Sub Notifications
```bash
PROJECT_ID=your-project-id
TOPIC_ID=test-gcs-ocn
PUBSUB_TOPIC=projects/${PROJECT_ID/topics/${TOPIC_ID}

# Create Pub/Sub Object Change Notifications
gsutil notification create -f json -t ${PUBSUB_TOPIC} -e OBJECT_FINALIZE gs://${INGESTION_BUCKET}

# Deploy Cloud Function
gcloud functions deploy test-gcs-bq-ingest \
  --region=us-west4 \
  --source=gcs_ocn_bq_ingest \
  --entrypoint=main \
  --runtime=python38 \
  --trigger-topic=${PUBSUB_TOPIC} \
  --service-account=${SERVICE_ACCOUNT_EMAIL} \
  --timeout=540 \
  --set-env-vars='DESTINATION_REGEX=^(?:[\w\-0-9]+)/(?P<dataset>[\w\-_0-9]+)/(?P<table>[\w\-_0-9]+)/?(?:incremental|history)?/?(?P<yyyy>[0-9]{4})?/?(?P<mm>[0-9]{2})?/?(?P<dd>[0-9]{2})?/?(?P<hh>[0-9]{2})?/?(?P<batch>[0-9]+)?/?'
```

#### Cloud Functions Events
```bash
PROJECT_ID=your-project-id

# Deploy Cloud Function
gcloud functions deploy test-gcs-bq-ingest \
  --region=us-west4 \
  --source=gcs_ocn_bq_ingest \
  --entrypoint=main \
  --runtime=python38 \
  --trigger-resource ${INGESTION_BUCKET} \
  --trigger-event google.storage.object.finalize
  --service-account=${SERVICE_ACCOUNT_EMAIL} \
  --timeout=540 \
  --set-env-vars='DESTINATION_REGEX=^(?:[\w\-0-9]+)/(?P<dataset>[\w\-_0-9]+)/(?P<table>[\w\-_0-9]+)/?(?:incremental|history)?/?(?P<yyyy>[0-9]{4})?/?(?P<mm>[0-9]{2})?/?(?P<dd>[0-9]{2})?/?(?P<hh>[0-9]{2})?/?(?P<batch>[0-9]+)?/?'
```

In theory, one could set up Pub/Sub notifications from multiple GCS Buckets
(owned by different teams but following a common naming convention) to the same
Pub/Sub topic so that data uploaded to any of these buckets could get
automatically loaded to BigQuery by a single deployment of the Cloud Function.

## Backfill
There are some cases where you may have data already copied to GCS according to
the naming convention / with success files before the Object Change
Notifications or Cloud Function have been set up. In these cases, you can use
the `backfill.py` CLI utility to crawl an existing bucket searching for success
files. The utility supports either invoking the Cloud Function main method
locally (in concurrent threads) or publishing notifications for the success
files (for a deployed Cloud Function to pick up).

### Usage
```
python3 -m backfill -h
usage: backfill.py [-h] --gcs-path GCS_PATH [--mode {LOCAL,NOTIFICATIONS}] [--pubsub-topic PUBSUB_TOPIC] [--success-filename SUCCESS_FILENAME] [--destination-regex DESTINATION_REGEX]

utility to backfill success file notifications or run the cloud function locally in concurrent threads.

optional arguments:
  -h, --help            show this help message and exit
  --gcs-path GCS_PATH, -p GCS_PATH
                        GCS path (e.g. gs://bucket/prefix/to/search/)to search for existing _SUCCESS files
  --mode {LOCAL,NOTIFICATIONS}, -m {LOCAL,NOTIFICATIONS}
                        How to perform the backfill: LOCAL run cloud function main method locally (in concurrent threads) or NOTIFICATIONS just push notifications to Pub/Sub for a deployed
                        version of the cloud function to pick up. Default is NOTIFICATIONS.
  --pubsub-topic PUBSUB_TOPIC, --topic PUBSUB_TOPIC, -t PUBSUB_TOPIC
                        Pub/Sub notifications topic to post notifications for. i.e. projects/{PROJECT_ID}/topics/{TOPIC_ID} Required if using NOTIFICATIONS mode.
  --success-filename SUCCESS_FILENAME, -f SUCCESS_FILENAME
                        Override the default success filename '_SUCCESS'
  --destination-regex DESTINATION_REGEX, -r DESTINATION_REGEX
                        Override the default destination regex for determining BigQuery destination based on information encoded in the GCS path of the success file
```

## Alternatives
### BQ Tail
[bqtail](https://github.com/viant/bqtail) is a similar serverless configuration
driven ingest to BigQuery from GCS that achieves batching based on window
in processing time (as driven by Cloud Scheduler). BQ Tail has nice features for
triggering Post actions (BQ queries / GCS file moves or deletes) once the data
is ingested, and slack notifications. bqtail is well suited for use cases where
the atomicity of event partition is not important (e.g. many distributed
publishers uploading logs to GCS). Due to dependency of certain features of
bqtail on Cloud Scheduler it cannot be used inside VPC-SC perimeters.
This tool might be more appropriate when the publisher is authoritative on the
atomicity of batches (e.g. an upstream  hadoop job responsible for commiting an
event time hour's worth of data).

### BigQuery Data Transfer Service
[Cloud Storage Transfer](https://cloud.google.com/bigquery-transfer/docs/cloud-storage-transfer)
can also be used but requires separate configurations per table and supports
only scheduled (rather than event based) loads. This can cause issues if the
upstream publisher of data is behind schedule. This service does not support
external query to perform transformations upon ingest.
