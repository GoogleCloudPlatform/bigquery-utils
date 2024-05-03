terraform {
  backend "gcs" {
    prefix  = "terraform/state"
  }
}

provider "google" {
  project = var.project
}

resource "google_storage_bucket" "regional_bucket" {
  for_each                    = toset(var.bq_regions)
  name                        = "${var.project}-lib-${each.value}"
  uniform_bucket_level_access = true
  location                    = each.key
  force_destroy               = false
}

resource "google_cloudbuild_trigger" "regional_trigger" {
  depends_on = [
    google_storage_bucket.regional_bucket
  ]
  for_each = toset(var.bq_regions)
  location = "${each.value}"
  name     = "udf-regional-trigger-${each.value}"
  filename = "cloudbuild.yaml"

  github {
    owner = "GoogleCloudPlatform"
    name  = "bigquery-utils"
    pull_request {
      branch = "^master$"
    }
  }
  included_files = ["udfs/**"]
  ignored_files = ["cloudbuild.yaml", ".*\\.md", "images/*", "tools/**"]

  substitutions = {
    _BQ_LOCATION = "${each.value}"
    _JS_BUCKET   = "gs://${var.project}-lib-${each.value}"
  }

}

