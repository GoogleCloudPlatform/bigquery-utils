# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
variable "project_id" {
  description = "GCP Project ID containing cloud function, and input bucket"
}

variable "app_id" {
  description = "Application Name"
}

variable "input_bucket" {
  description = "GCS bucket to watch for new files"
}

variable "input_prefix" {
  description = "GCS prefix to watch for new files in input_bucket"
  default     = null
}

variable "cloudfunctions_source_bucket" {
  description = "GCS bucket to store Cloud Functions Source"
}

variable "data_ingester_sa" {
  description = "Service Account Email responsible for ingesting data to BigQuery"
}

variable "environment_variables" {
  description = "Environment variables to set on the cloud function."
  type        = map(string)
  default     = {}
}


variable "region" {
  description = "GCP region in which to deploy cloud function"
  default     = "us-central1"
}

variable "function_source_folder" {
  description = "Path to Cloud Function source"
  default     = "../gcs_event_based_ingest/gcs_ocn_bq_ingest/"
}

variable "use_pubsub_notifications" {
  description = "Setting this to true will use Pub/Sub notifications By default we will use Cloud Functions Event direct notifications. See https://cloud.google.com/storage/docs/pubsub-notifications."
  type        = bool
  default     = false
}

variable "bigquery_project_ids" {
  description = "Additional project IDs to grant bigquery Admin for the data ingester account"
  type        = list(string)
  default     = []
}

variable "force_destroy" {
  description = "force destroy resources (e.g. for e2e tests)"
  default     = "false"
}

variable "timeout" {
  description = "Cloud Functions timeout in seconds"
  default     = 540
}
