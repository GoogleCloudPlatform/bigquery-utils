# Cloud Function: Event Driven BigQuery Ingest
Terraform module for deploying the Google Cloud Function
for event based ingest of GCS data to BigQuery described [here](../README.md).


Note that by default all environment variables for the cloud function
will be empty deferring to the defaults implemented in the function and
documented [here](../gcs_ocn_bq_ingest_function/README.md)
## Requirements

| Name | Version |
|------|---------|
| terraform | >= 0.13 |
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
| bigquery\_project\_ids | Additional project IDs to grant bigquery Admin for the data ingester account | `list(string)` | `[]` | no |
| cloudfunctions\_source\_bucket | GCS bucket to store Cloud Functions Source | `any` | n/a | yes |
| data\_ingester\_sa | Service Account Email responsible for ingesting data to BigQuery | `any` | n/a | yes |
| environment\_variables | Environment variables to set on the cloud function. | `map(string)` | `{}` | no |
| force\_destroy | force destroy resources (e.g. for e2e tests) | `string` | `"false"` | no |
| function\_source\_folder | Path to Cloud Function source | `string` | `"../gcs_event_based_ingest/gcs_ocn_bq_ingest/"` | no |
| input\_bucket | GCS bucket to watch for new files | `any` | n/a | yes |
| input\_prefix | GCS prefix to watch for new files in input\_bucket | `any` | `null` | no |
| project\_id | GCP Project ID containing cloud function, and input bucket | `any` | n/a | yes |
| region | GCP region in which to deploy cloud function | `string` | `"us-central1"` | no |
| timeout | Cloud Functions timeout in seconds | `number` | `540` | no |
| use\_pubsub\_notifications | Setting this to true will use Pub/Sub notifications By default we will use Cloud Functions Event direct notifications. See https://cloud.google.com/storage/docs/pubsub-notifications. | `bool` | `false` | no |

## Outputs

| Name | Description |
|------|-------------|
| cloud-function | instance of cloud function deployed by this module. |
| data-ingester-sa | data ingester service account email created as cloud function identity |
| input-bucket | n/a |

