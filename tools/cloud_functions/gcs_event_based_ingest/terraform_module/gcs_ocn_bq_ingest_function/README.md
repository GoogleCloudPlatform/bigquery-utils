# Cloud Function: Event Driven BigQuery Ingest
Terraform module for deploying the Google Cloud Function
for event based ingest of GCS data to BigQuery described [here](../README.md).


Note that by default all environment variables for the cloud function
will be empty deferring to the defaults implemented in the function and
documented [here](../gcs_ocn_bq_ingest_function/README.md)

## Requirements

| Name | Version |
|------|---------|
| terraform | >= 0.12 |
| archive | ~> 2.0.0 |
| google | >= 3.38.0 |
| template | ~> 2.2.0 |

## Providers

| Name | Version |
|------|---------|
| archive | ~> 2.0.0 |
| google | >= 3.38.0 |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| app\_id | Application Name | `any` | n/a | yes |
| cloudfunctions\_source\_bucket | GCS bucket to store Cloud Functions Source | `any` | n/a | yes |
| data\_ingester\_sa | Service Account Email responsible for ingesting data to BigQuery | `any` | n/a | yes |
| destination\_regex | A [Python Regex with named capturing groups](https://docs.python.org/3/howto/regex.html#non-capturing-and-named-groups) for destination `dataset`, `table`, (optional: `partition`, `batch`) | `string` | `""` | no |
| function\_source\_folder | Path to Cloud Function source | `string` | `"../gcs_event_based_ingest/gcs_ocn_bq_ingest/"` | no |
| input\_bucket | GCS bucket to watch for new files | `any` | n/a | yes |
| input\_prefix | GCS prefix to watch for new files in input\_bucket | `any` | `null` | no |
| job\_prefix | Prefix for BigQuery Job IDs | `string` | `""` | no |
| max\_batch\_bytes | Max bytes for BigQuery Load job | `string` | `""` | no |
| project\_id | GCP Project ID | `any` | n/a | yes |
| region | GCP region in which to deploy cloud function | `string` | `"us-central1"` | no |
| success\_filename | Filename to trigger a load of a prefix | `string` | `""` | no |
| use\_pubsub\_notifications | Setting this to true will use Pub/Sub notifications By default we will use Cloud Functions Event direct notifications. See https://cloud.google.com/storage/docs/pubsub-notifications. | `bool` | `false` | no |
| wait\_for\_job\_seconds | How long to wait before deciding BQ job did not fail quickly | `string` | `""` | no |

## Outputs

| Name | Description |
|------|-------------|
| cloud-function | instance of cloud function deployed by this module. |

