# Event Driven BigQuery Ingest
This directory defines a reusable [Background Cloud Function](https://cloud.google.com/functions/docs/writing/background)
for ingesting any new file at a GCS prefix with a file name containing a
timestamp to be used as the partitioning and clustering column in a partitioned
BigQuery Table.

## Orchestration
1. Files pushed to a Google Cloud Storage bucket.
1. [Pub/Sub Notification](https://cloud.google.com/storage/docs/pubsub-notifications)
object finalize.
1. Cloud Function subscribes to notifications and ingests all the data into
BigQuery a directory once a `_SUCCESS` file arrives.


## Deployment
The source for this Cloud Function can easily be reused to repeat this pattern
for many tables by using the accompanying terraform module (TODO).

This way we can reuse the tested source code for the Cloud Function.

### Environment Variables
To configure each deployement of the Cloud Function we will use
[Environment Variables](https://cloud.google.com/functions/docs/env-var)
All of these environment variables are optional for overriding the
following default behavior.

| Variable              | Description                           | Default                                      |
|-----------------------|---------------------------------------|----------------------------------------------|
| `WAIT_FOR_JOB_SECONDS`| How long to wait before deciding BQ job did not fail quickly| `5` |
| `SUCCESS_FILENAME`    | Filename to trigger a load of a prefix| `_SUCCESS` |
| `DESTINATION_REGEX`   | A [Python Regex with named capturing groups](https://docs.python.org/3/howto/regex.html#non-capturing-and-named-groups) for `dataset`, `table`, (optional: `partition` or `yyyy`, `mm`, `dd`, `hh`, `batch`)
| `MAX_BATCH_BYTES`     | Max bytes for BigQuery Load job      | `15000000000000` ([15 TB](https://cloud.google.com/bigquery/quotas#load_jobs)|
| `JOB_PREFIX`          | Prefix for BigQuery Job IDs          | `gcf-ingest-` |


## Implementation notes
1. To support notifications based on a GCS prefix
(rather than every object in the bucket), we chose to use manually
configure Pub/Sub Notifications manually and use a Pub/Sub triggered
Cloud Function.

