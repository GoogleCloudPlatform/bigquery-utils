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

data "google_storage_project_service_account" "gcs_account" {
  project = var.project_id
}

resource "google_pubsub_topic" "notification_topic" {
  project = var.project_id
  name    = "${var.app_id}-ocn-notifications"
}

module "bucket" {
  source  = "terraform-google-modules/cloud-storage/google//modules/simple_bucket"
  version = "~> 1.3"

  name       = var.input_bucket
  project_id = var.project_id
  location   = var.region
  iam_members = [{
    role   = "roles/storage.objectAdmin"
    member = module.data_ingester_service_account.iam_email
  }]
}

resource "google_storage_notification" "notification" {
  bucket         = module.bucket.bucket
  payload_format = "JSON_API_V1"
  topic          = google_pubsub_topic.notification_topic.id
  event_types    = ["OBJECT_FINALIZE"]
}

# Zip up source code folder
data "archive_file" "function_source" {
  type        = "zip"
  output_path = "gcs_ocn_bq_ingest.zip"
  source_dir  = var.function_source_folder
}

resource "google_storage_bucket_object" "function_zip_object" {
  name         = "cloudfunctions_source/${data.archive_file.function_source.output_md5}-${basename(data.archive_file.function_source.output_path)}"
  bucket       = var.cloudfunctions_source_bucket
  source       = data.archive_file.function_source.output_path
  content_type = "application/zip"
}

resource "google_cloudfunctions_function" "gcs_to_bq" {
  project               = var.project_id
  name                  = "gcs_to_bq_${var.app_id}"
  region                = var.region
  runtime               = "python38"
  timeout               = 9 * 60 # seconds
  service_account_email = var.data_ingester_sa
  source_archive_bucket = var.cloudfunctions_source_bucket
  source_archive_object = google_storage_bucket_object.function_zip_object.name
  entry_point           = "main"
  environment_variables = {
    WAIT_FOR_JOB_SECONDS = var.wait_for_job_seconds
    SUCCESS_FILENAME     = var.success_filename
    DESTINATION_REGEX    = var.destination_regex
    MAX_BATCH_BYTES      = var.max_batch_bytes
    JOB_PREFIX           = var.job_prefix
  }
  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.notification_topic.id
  }
}

module "data_ingester_service_account" {
  source     = "terraform-google-modules/service-accounts/google"
  version    = "~> 2.0"
  project_id = var.project_id
  names      = [var.data_ingester_sa, ]
  project_roles = [
    "${var.project_id}=>roles/bigquery.jobUser",
    "${var.project_id}=>roles/bigquery.dataEditor",
  ]
}

# Allow the GCS service account to publish notification for new objects to the
# notification topic.
resource "google_pubsub_topic_iam_binding" "gcs_publisher" {
  topic   = google_pubsub_topic.notification_topic.id
  role    = "roles/pubsub.publisher"
  members = ["serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"]
}

# Allow the Cloud Functions SA to subscribe to the notification topic
resource "google_pubsub_topic_iam_binding" "cf_subscriber" {
  topic   = google_pubsub_topic.notification_topic.id
  role    = "roles/pubsub.subscriber"
  members = [module.data_ingester_service_account.iam_email]
}

