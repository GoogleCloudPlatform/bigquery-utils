terraform {
  backend "gcs" {
    prefix  = "terraform/state"
  }
}

provider "google" {
  project = var.project
}

data "local_file" "regions_file" {
  filename = "region_to_dataset_suffix_map.yaml"
}

locals {
  regions_map = yamldecode(data.local_file.regions_file.content)
}

resource "google_storage_bucket" "regional_bucket" {
  for_each                    = local.regions_map
  name                        = "${var.project}-lib-${replace(each.value, "_", "-")}"
  uniform_bucket_level_access = true
  location                    = each.key
  force_destroy               = true
}

resource "google_cloudbuild_trigger" "regional_trigger" {
  depends_on = [
    google_storage_bucket.regional_bucket
  ]
  for_each = local.regions_map
  location = "global"
  name     = "udf-regional-trigger-${replace(each.value, "_", "-")}"
  filename = "cloudbuild.yaml"

  github {
    owner = "GoogleCloudPlatform"
    name  = "bigquery-utils"
    push {
      branch = "^master$"
    }
  }
  ignored_files  = ["cloudbuild.yaml", ".*\\.md", "images/*", "udfs/**"]
  included_files = ["cloudbuild.yaml", ".*\\.md", "images/*", "tools/**"]

  substitutions = {
    _BQ_LOCATION = "${each.key}"
    _JS_BUCKET   = "${var.project}-lib-${replace(each.value, "_", "-")}"
  }

}

