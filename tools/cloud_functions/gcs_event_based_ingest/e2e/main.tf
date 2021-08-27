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
variable "short_sha" {}
variable "project_id" { default = "bqutil" }
variable "region" { default = "us-central1" }
output "bucket" {
  value = module.gcs_ocn_bq_ingest.input-bucket
}

resource "google_storage_bucket" "cloud_functions_source" {
  name          = "gcf-source-archives${var.short_sha}"
  project       = var.project_id
  storage_class = "REGIONAL"
  location      = var.region
  force_destroy = "true"
}

module "gcs_ocn_bq_ingest" {
  source                       = "../terraform_module/gcs_ocn_bq_ingest_function"
  function_source_folder       = "../gcs_ocn_bq_ingest"
  app_id                       = "gcs-ocn-bq-ingest-e2e-test${var.short_sha}"
  cloudfunctions_source_bucket = google_storage_bucket.cloud_functions_source.name
  data_ingester_sa             = "data-ingester-sa${var.short_sha}"
  input_bucket                 = "gcs-ocn-bq-ingest-e2e-tests${var.short_sha}"
  project_id                   = var.project_id
  environment_variables = {
    START_BACKFILL_FILENAME = "_HISTORYDONE"
  }
  # We'll use a shorter timeout for e2e stress subscriber re-triggering
  timeout       = 60
  force_destroy = "true"
}

terraform {
  backend "local" {
    path = "terraform.tfstate"
  }
}

