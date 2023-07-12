# Copyright 2023 Google LLC
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

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.34.0"
    }
  }
}

resource "random_id" "bucket_prefix" {
  byte_length = 8
}

##########################################
#          BQ Target Dataset             #
##########################################
resource "google_bigquery_dataset" "dataset" {
  project    = var.storage_project_id
  dataset_id = var.target_dataset_name
}

##########################################
#        GCS Bucket for CF code          #
##########################################
resource "google_storage_bucket" "bucket" {
  name                        = "${random_id.bucket_prefix.hex}-gcf-source"
  location                    = "US"
  uniform_bucket_level_access = true
}

##########################################
#          Pub/Sub Topics                #
##########################################
resource "google_pubsub_topic" "snapshot_dataset_topic" {
  name = "snapshot_dataset_topic"
}

resource "google_pubsub_topic" "bq_snapshot_create_snapshot_topic" {
  name = "bq_snapshot_create_snapshot_topic"
}

##########################################
#          Cloud Scheduler               #
##########################################
resource "google_cloud_scheduler_job" "job" {
  name     = "bq-snap-start-process"
  schedule = var.crontab_format

  pubsub_target {
    # topic.id is the topic's full resource name.
    topic_name = google_pubsub_topic.snapshot_dataset_topic.id
    data       = base64encode("{\"source_dataset_name\":\"${var.source_dataset_name}\",\"target_dataset_name\":\"${var.target_dataset_name}\",\"crontab_format\":\"${var.crontab_format}\",\"seconds_before_expiration\":${var.seconds_before_expiration},\"tables_to_include_list\":${var.tables_to_include_list},\"tables_to_exclude_list\":${var.tables_to_exclude_list}}")
  }
}

##########################################
#    bq_backup_fetch_tables_names CF     #
##########################################
data "archive_file" "bq_backup_fetch_tables_names" {
  type        = "zip"
  source_dir  = "../bq_backup_fetch_tables_names"
  output_path = "/tmp/bq_backup_fetch_tables_names.zip"
}

resource "google_storage_bucket_object" "bq_backup_fetch_tables_names" {
  name   = "bq_backup_fetch_tables_names.zip"
  bucket = google_storage_bucket.bucket.name
  source = data.archive_file.bq_backup_fetch_tables_names.output_path
}

resource "google_cloudfunctions_function" "bq_backup_fetch_tables_names" {
  name = "bq_backup_fetch_tables_names"

  runtime               = "python39"
  available_memory_mb   = 128
  entry_point           = "main"
  source_archive_bucket = google_storage_bucket.bucket.name
  source_archive_object = google_storage_bucket_object.bq_backup_fetch_tables_names.name

  environment_variables = {
    DATA_PROJECT_ID            = var.storage_project_id
    PUBSUB_PROJECT_ID          = var.project_id
    TABLE_NAME_PUBSUB_TOPIC_ID = google_pubsub_topic.bq_snapshot_create_snapshot_topic.name
  }

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.snapshot_dataset_topic.id
  }
}

##########################################
#     bq_backup_create_snapshots CF      #
##########################################
data "archive_file" "bq_backup_create_snapshots" {
  type        = "zip"
  source_dir  = "../bq_backup_create_snapshots"
  output_path = "/tmp/bq_backup_create_snapshots.zip"
}

resource "google_storage_bucket_object" "bq_backup_create_snapshots" {
  name   = "bq_backup_create_snapshots.zip"
  bucket = google_storage_bucket.bucket.name
  source = data.archive_file.bq_backup_create_snapshots.output_path
}

resource "google_cloudfunctions_function" "bq_backup_create_snapshots" {
  name = "bq_backup_create_snapshots"

  runtime               = "python39"
  max_instances         = 100 # BQ allows a max of 100 concurrent snapshot jobs per project
  available_memory_mb   = 128
  entry_point           = "main"
  source_archive_bucket = google_storage_bucket.bucket.name
  source_archive_object = google_storage_bucket_object.bq_backup_create_snapshots.name

  environment_variables = {
    BQ_DATA_PROJECT_ID = var.storage_project_id
    BQ_JOBS_PROJECT_ID = var.project_id
  }

  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.bq_snapshot_create_snapshot_topic.id
  }
}

