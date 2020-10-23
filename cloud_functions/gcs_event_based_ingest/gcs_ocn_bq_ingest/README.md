<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Event Driven BigQuery Ingest with External Table Query](#event-driven-bigquery-ingest-with-external-table-query)
  - [Orchestration](#orchestration)
  - [Ingestion Mechanics](#ingestion-mechanics)
  - [Deployment](#deployment)
  - [Implementation notes](#implementation-notes)
  - [Tests](#tests)
  - [Limitations](#limitations)
  - [Future work](#future-work)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Event Driven BigQuery Ingest 
This directory defines a reusable [Background Cloud Function](https://cloud.google.com/functions/docs/writing/background)
for ingesting any new file at a GCS prefix with a file name containing a
timestamp to be used as the partitioning and clustering column in a partitioned
BigQuery Table.

![architecture](img/arch.png)

## Orchestration
1. Files pulled from on-prem to gcs bucket.
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


#### Optional
| Variable                      | Description                           | Default                                      |
|-------------------------------|---------------------------------------|----------------------------------------------|
| `BQ_LOAD_STATE_TABLE` | BigQuery table to log load state to           | "bigquery_loads.serverless_bq_loads" (in same project as cloud function) |


## Implementation notes
1. To support notifications based on a GCS prefix
(rather than every object in the bucket), we chose to use manually
configure Pub/Sub Notifications manually and use a Pub/Sub triggered
Cloud Function.

## Tests
From the `gcs_ocn_bq_ingest` dir simply run
```bash
pytest
```

## Limitations
1. Cloud Functions have a 10 minute timeout. If the BQ load job takes too long
 the data will still be ingested but the function may be marked in timeout state
 rather than success and the function may not have the opportunity to produce
 all the expected logs.
  - Most of the expected data size for this use case <50 GB per load job should
  load comfortably with in this timeout window.
  - a production implementation should gracefully handle the case when the BQ
  load job takes longer than ~9 mins.


## Future work
1. Add system tests.
