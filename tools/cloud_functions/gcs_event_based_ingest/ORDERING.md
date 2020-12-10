# Ordering Batches
There are use cases where it is important for incremental batches get
applied in order rather than as soon as they are uploaded to GCS (which is the
default behavior of this solution).
1. When using External Query that performs DML other than insert only.
(e.g. an `UPDATE` assumes that prior batches have already been committed) 
1. To ensure that there are not time gaps in the data (e.g. ensure that
2020/01/02 data is not committed to BigQuery before 2020/01/01, or similarly
that 00 hour is ingested before the 01 hour, etc.)

This Cloud Function supports serializing the submission of ingestion jobs to 
BigQuery by using Google Cloud Storage's consistency guarantees to provide a
pessimistic lock on a table to prevent concurrent jobs and
[GCS Object.list](https://cloud.google.com/storage/docs/json_api/v1/objects/list)
lexicographic sorting of results to providing ordering gurantees.
The solution involves a table level `_backlog/` directory to keep track
of success files whose batches have not yet been committed to BigQuery and
a table level `_bqlock` file to keep track of what job is currently ingesting to
that table. This way we can make our Cloud Function idempotent by having all the
state stored in GCS so we can safely retrigger it to skirt the Cloud Functions
timeout.

## Assumptions
This ordering solution assumes that you want to apply batches in lexicographic
order. This is usually the case because path names usually contain some sort of
date / hour information.

## Enabling Ordering
### Environment Variable
Ordering can be enabled at the function level by setting the `ORDER_PER_TABLE`
environment variable to `"True"`.
### Config File
Ordering can be configured at any level of your naming convention (e.g. dataset
table or some sub-path) by placing a `_config/ORDERME` file. This can be helpful
in scenarios where your historical load can be processed safely in parallel but
incrementals must be ordered.
For example:
```text
gs://${BUCKET}/${DATASET}/${TABLE}/historical/_config/load.json
gs://${BUCKET}/${DATASET}/${TABLE}/incremental/_config/external.json
gs://${BUCKET}/${DATASET}/${TABLE}/incremental/_config/bq_transform.sql
gs://${BUCKET}/${DATASET}/${TABLE}/incremental/_config/ORDERME
```

## Dealing With Out of Order Publishing to GCS During Historical Load
In some use cases, there is a period where incrementals that must be applied in
order are uploaded in parallel (meaning their _SUCCESS files are expected to be
out of order). This typically happens during some historical backfill period.
This can be solved by setting the `START_BACKFILL_FILENAME` environment
variable to a file name that indicates that the parallel upload of historical
incrementals is complete (e.g. `_HISTORYDONE`). This will cause all success
files for a table to be added to the backlog until the `_HISTORYDONE` file is
dropped at the table level. At that point the backlog subscriber will begin
processing the batches in order. 

## Batch Failure Behavior
When ordering is enabled, if the BQ job to apply a batch failed, it is not safe
to continue to ingest the next batch. The Cloud Function will leave the
`_bqlock` file and stop trying to process the backlog. The Cloud function 
will report an exception like this which should be alerted on as the ingestion
process for the table will be deadlocked until there is human intervention to
address the failed batch:
```text
    f"previous BigQuery job: {job_id} failed or could not "
    "be found. This will kill the backfill subscriber for "
    f"the table prefix {table_prefix}."
    "Once the issue is dealt with by a human, the lock"
    "file at: "
    f"gs://{lock_blob.bucket.name}/{lock_blob.name} "
    "should be manually removed and a new empty _BACKFILL"
    "file uploaded to:"
    f"gs://{lock_blob.bucket.name}/{table_prefix}/_BACKFILL"
    f"to resume the backfill subscriber so it can "
    "continue with the next item in the backlog.\n"
    "Original Exception:\n"
    f"{traceback.format_exc()}")
```

## Ordering Mechanics Explained
We've treated ordering incremental commits to table  as a variation on the
[Producer-Consumer Problem](https://en.wikipedia.org/wiki/Producer%E2%80%93consumer_problem)
Where we have multiple producers (each call of Backlog Publisher) and a single
Consumer (the Backlog Subscriber which is enforced to be a singleton per table
with a claim file). Our solution is to use GCS `_backlog` directory as our queue
and `_bqlock` as a mutex.

### Backlog Publisher 
The Backlog Publisher has two responsibilities:
1. add incoming success files to a
table's `_backlog` so they are not "forgotten" by the ingestion system.
1. if there is a non-empty backlog start the backfill subscriber (if one is not
already running). This is accomplished by dropping a table level `_BACKFILL` file.

### Backlog Subscriber
The Backlog Subscriber is responsible for keeping track of BigQuery jobs running
on a table and ensure that batches are committed in order. When the backlog is
not empty for a table the backlog subscriber should be running for that table.
It will either be polling a `RUNNING` BigQuery job for completion, or submitting
the next batch in the `_backlog`.

The state of what BigQuery job is currently running on a table is kept in a
`_bqlock` file at the table prefix.

In order to escape the maximum nine-minute (540s) Cloud Function Timeout, the
backfill subscriber will re-trigger itself by posting a new `_BACKFILL` file
until the `_backlog` for the table prefix is empty. When a new success file
arrives it is the reponsibility of the publisher to restart the subscriber.
