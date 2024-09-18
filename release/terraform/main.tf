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

resource "google_project_iam_member" "bigquery_connection_grant_vertex_ai_user_role" {
  project  = var.project
  role     = "roles/aiplatform.user"
  for_each = { for k, v in google_bigquery_connection.connection : k => v.cloud_resource[0].service_account_id }
  member   = "serviceAccount:${each.value}"
}

resource "google_bigquery_connection" "connection" {
  for_each      = toset(var.bq_regions)
  connection_id = "procedure"
  location      = each.value
  project       = var.project
  cloud_resource {}
}

resource "google_bigquery_dataset_iam_member" "procedure_public_viewers" {
  project    = var.project
  for_each   = var.project == "bqutil" ? toset(var.bq_regions) : []
  dataset_id = "procedure_${replace(each.value, "-", "_")}"
  role       = "roles/bigquery.dataViewer"
  member     = "allAuthenticatedUsers"
}

/* Uncomment below whenever the first contribution to netezza dataset occurs 
resource "google_bigquery_dataset_iam_member" "nz_public_viewers" {
  project    = var.project
  for_each   = var.project == "bqutil" ? toset(var.bq_regions) : []
  dataset_id = "nz_${replace(each.value, "-", "_")}"
  role       = "roles/bigquery.dataViewer"
  member     = "allAuthenticatedUsers"
}
*/

/* Uncomment below whenever the first contribution to oracle dataset occurs 
resource "google_bigquery_dataset_iam_member" "or_public_viewers" {
  project    = var.project
  for_each   = var.project == "bqutil" ? toset(var.bq_regions) : []
  dataset_id = "or_${replace(each.value, "-", "_")}"
  role       = "roles/bigquery.dataViewer"
  member     = "allAuthenticatedUsers"
}
*/

resource "google_bigquery_dataset_iam_member" "rs_public_viewers" {
  project    = var.project
  for_each   = var.project == "bqutil" ? toset(var.bq_regions) : []
  dataset_id = "rs_${replace(each.value, "-", "_")}"
  role       = "roles/bigquery.dataViewer"
  member     = "allAuthenticatedUsers"
}

resource "google_bigquery_dataset_iam_member" "sf_public_viewers" {
  project    = var.project
  for_each   = var.project == "bqutil" ? toset(var.bq_regions) : []
  dataset_id = "sf_${replace(each.value, "-", "_")}"
  role       = "roles/bigquery.dataViewer"
  member     = "allAuthenticatedUsers"
}

resource "google_bigquery_dataset_iam_member" "td_public_viewers" {
  project    = var.project
  for_each   = var.project == "bqutil" ? toset(var.bq_regions) : []
  dataset_id = "td_${replace(each.value, "-", "_")}"
  role       = "roles/bigquery.dataViewer"
  member     = "allAuthenticatedUsers"
}

resource "google_bigquery_dataset_iam_member" "ve_public_viewers" {
  project    = var.project
  for_each   = var.project == "bqutil" ? toset(var.bq_regions) : []
  dataset_id = "ve_${replace(each.value, "-", "_")}"
  role       = "roles/bigquery.dataViewer"
  member     = "allAuthenticatedUsers"
}

resource "google_bigquery_dataset_iam_member" "fn_public_viewers" {
  project    = var.project
  for_each   = var.project == "bqutil" ? toset(var.bq_regions) : []
  dataset_id = "fn_${replace(each.value, "-", "_")}"
  role       = "roles/bigquery.dataViewer"
  member     = "allAuthenticatedUsers"
}
