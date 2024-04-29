terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = "wpe-tam-sandbox-anthos" #testing
}

data "local_file" "regions_file" {
  filename = "../udfs/region_to_dataset_suffix_map.yaml"
}

locals {
  regions_map = yamldecode(data.local_file.regions_file.content)
}

resource "google_storage_bucket" "regional_bucket" {
  for_each      = local.regions_map
  name          = "bqutil-lib-${each.value}"
  uniform_bucket_level_access = true
  location      = each.key
  force_destroy = true 
}

resource "google_cloudbuild_trigger" "include-build-logs-trigger" {
  depends_on = [
    google_storage_bucket.regional_bucket
  ]
  for_each = local.regions_map
  location = "global"
  name     = "udf-regional-trigger-${each.value}"
  filename = "cloudbuild.yaml"

  github {
    owner = "afleisc"
    name  = "antipattern-cicd"
    push {
      branch = "^master$"
    }
  }
  ignored_files  = ["cloudbuild.yaml", ".*\\.md", "images/*", "udfs/**"]
  included_files = ["cloudbuild.yaml", ".*\\.md", "images/*", "tools/**"]

  substitutions = {
    _BQ_LOCATION = "${each.key}"
    _JS_BUCKET   = "bqutil-lib-${each.value}"
  }

  include_build_logs = "INCLUDE_BUILD_LOGS_WITH_STATUS"
}

