# Copyright 2020 Google LLC
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
  description = "GCP Project ID"
}

variable "app_id" {
  description = "Application Name"
}

variable "input_bucket" {
  description = "GCS bucket to watch for new files"
}

variable "cloudfunctions_source_bucket" {
  description = "GCS bucket to store Cloud Functions Source"
}

variable "data_ingester_sa" {
  description = "Service Account Email responsible for ingesting data to BigQuery"
}

variable "wait_for_job_seconds" {
  description = "How long to wait before deciding BQ job did not fail quickly"
  default     = ""
}
variable "success_filename" {
  description = "Filename to trigger a load of a prefix"
  default     = ""
}
variable "destination_regex" {
  description = "A [Python Regex with named capturing groups](https://docs.python.org/3/howto/regex.html#non-capturing-and-named-groups) for destination `dataset`, `table`, (optional: `partition`, `batch`)"
  default     = ""
}
variable "max_batch_bytes" {
  description = "Max bytes for BigQuery Load job"
  default     = ""
}

variable "job_prefix" {
  description = "Prefix for BigQuery Job IDs "
  default     = ""
}

variable "region" {
  description = "GCP region in which to deploy cloud function"
  default     = "us-central1"
}

variable "function_source_folder" {
  description = "Path to Cloud Function source"
  default     = "../gcs_event_based_ingest/gcs_ocn_bq_ingest/"
}

