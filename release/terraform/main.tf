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
  public_access_prevention    = var.project == "bqutil" ? "inherited" : "enforced"
  location                    = each.key
  force_destroy               = false
}

resource "google_storage_bucket_iam_member" "member" {
  for_each = var.project == "bqutil" ? toset(var.bq_regions) : []
  bucket = "${var.project}-lib-${each.value}"
  role = "roles/storage.objectViewer"
  member = "allAuthenticatedUsers"
}

resource "google_cloudbuild_trigger" "regional_trigger" {
  depends_on = [
    google_storage_bucket.regional_bucket
  ]
  for_each = toset(var.bq_regions)
  name     = "udf-regional-trigger-${each.value}"
  filename = "cloudbuild.yaml"

  github {
    owner = "GoogleCloudPlatform"
    name  = "bigquery-utils"
    dynamic "pull_request" {
      for_each = var.project == "bqutil-test" ? [1] : []
      content {
        branch = "^master$"
        comment_control = "COMMENTS_ENABLED"
      }
    }
    dynamic "push" {
      for_each = var.project == "bqutil" ? [1] : []
      content {
        branch = "^master$"
      }
    }
  }
  included_files = ["udfs/**", "stored_procedures/**"]
  ignored_files = ["cloudbuild.yaml", ".*\\.md", "images/*", "tools/**"]
  include_build_logs = "INCLUDE_BUILD_LOGS_WITH_STATUS"

  substitutions = {
    _BQ_LOCATION = "${each.value}"
    _JS_BUCKET   = "gs://${var.project}-lib-${each.value}"
  }
}

resource "google_project_iam_member" "project_iam" {
  project  = var.project
  role     = "roles/aiplatform.user"
  for_each = { for k, v in google_bigquery_connection.connection : k => v.cloud_resource[0].service_account_id }
  member   = "serviceAccount:${each.value}"
}

resource "google_bigquery_connection" "connection" {
  for_each      = toset(var.regions)
  connection_id = "procedure"
  location      = each.value
  project       = var.project
  cloud_resource {}
}
