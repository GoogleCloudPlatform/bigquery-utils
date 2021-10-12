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

data "google_storage_project_service_account" "gcs_account" {
  project = var.project_id
}

resource "google_pubsub_topic" "notification_topic" {
  count   = var.use_pubsub_notifications ? 1 : 0
  project = var.project_id
  name    = "${var.app_id}-ocn-notifications"
}

module "bucket" {
  depends_on = [module.data_ingester_service_account]
  source     = "terraform-google-modules/cloud-storage/google//modules/simple_bucket"
  version    = "~> 1.3"

  name          = var.input_bucket
  project_id    = var.project_id
  location      = var.region
  force_destroy = var.force_destroy
  iam_members = [{
    role   = "roles/storage.objectAdmin"
    member = "serviceAccount:${var.data_ingester_sa}@${var.project_id}.iam.gserviceaccount.com"
  }]
}

resource "google_storage_notification" "notification" {
  depends_on         = [google_pubsub_topic_iam_binding.gcs_publisher]
  count              = var.use_pubsub_notifications ? 1 : 0
  bucket             = module.bucket.bucket
  object_name_prefix = var.input_prefix
  payload_format     = "JSON_API_V1"
  topic              = google_pubsub_topic.notification_topic[0].id
  event_types        = ["OBJECT_FINALIZE"]
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

locals {
  function_name = "gcs_to_bq_${var.app_id}"
}
resource "google_cloudfunctions_function" "gcs_to_bq" {
  depends_on            = [google_storage_bucket_object.function_zip_object]
  project               = var.project_id
  name                  = local.function_name
  region                = var.region
  runtime               = "python38"
  timeout               = var.timeout
  service_account_email = module.data_ingester_service_account.email
  source_archive_bucket = var.cloudfunctions_source_bucket
  source_archive_object = google_storage_bucket_object.function_zip_object.name
  entry_point           = "main"
  environment_variables = merge(var.environment_variables, {
    GCP_PROJECT          = var.project_id,
    FUNCTION_TIMEOUT_SEC = var.timeout
    FUNCTION_NAME        = local.function_name
  })
  event_trigger {
    event_type = var.use_pubsub_notifications ? "providers/cloud.pubsub/eventTypes/topic.publish" : "google.storage.object.finalize"
    resource   = var.use_pubsub_notifications ? "projects/${var.project_id}/${google_pubsub_topic.notification_topic[0].id}" : module.bucket.bucket.name
  }
}

module "data_ingester_service_account" {
  source     = "terraform-google-modules/service-accounts/google"
  version    = "~> 2.0"
  project_id = var.project_id
  names      = [var.data_ingester_sa, ]
  project_roles = [
    "${var.project_id}=>roles/bigquery.jobUser",
    "${var.project_id}=>roles/storage.admin",
    "${var.project_id}=>roles/errorreporting.writer",
  ]
}

# Grant the ingester service account permissions to mutate data in
# target project(s)
resource "google_project_iam_binding" "ingester_bq_admin" {
  for_each = toset(concat(var.bigquery_project_ids, [var.project_id]))
  project  = each.key
  members  = [module.data_ingester_service_account.iam_email]
  role     = "roles/bigquery.admin"
}

# Allow the GCS service account to publish notification for new objects to the
# notification topic.
resource "google_pubsub_topic_iam_binding" "gcs_publisher" {
  count   = var.use_pubsub_notifications ? 1 : 0
  topic   = google_pubsub_topic.notification_topic[0].id
  role    = "roles/pubsub.publisher"
  members = ["serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"]
}

# Allow the Cloud Functions SA to subscribe to the notification topic
resource "google_pubsub_topic_iam_binding" "cf_subscriber" {
  count   = var.use_pubsub_notifications ? 1 : 0
  topic   = google_pubsub_topic.notification_topic[0].id
  role    = "roles/pubsub.subscriber"
  members = [module.data_ingester_service_account.iam_email]
}

module "project-services" {
  source  = "terraform-google-modules/project-factory/google//modules/project_services"
  version = "4.0.0"

  project_id                  = var.project_id
  disable_services_on_destroy = "false"

  activate_apis = [
    "compute.googleapis.com",
    "iam.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "clouderrorreporting.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "cloudfunctions.googleapis.com",
  ]
}
