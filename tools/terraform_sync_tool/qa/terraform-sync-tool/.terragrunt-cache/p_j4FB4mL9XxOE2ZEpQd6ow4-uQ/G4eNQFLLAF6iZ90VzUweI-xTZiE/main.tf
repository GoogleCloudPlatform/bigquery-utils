# provider "google" {
#   project = var.project_id
#   region  = "us-central1"
#   zone    = "us-central1-c"
# }

# resource "google_bigquery_dataset" "example_dataset" {
#   dataset_id                  = var.dataset_id
#   friendly_name               = "test"
#   description                 = "This is a description"
#   location                    = "US"
#   default_table_expiration_ms = 3600000
# }

# resource "google_bigquery_dataset_iam_binding" "reader" {
#   dataset_id = google_bigquery_dataset.example_dataset.dataset_id
#   role       = "roles/bigquery.dataViewer"

#   members = [
#     "user:candicehou@google.com",
#   ]
# }

# resource "google_bigquery_table" "foo" {
#   dataset_id = google_bigquery_dataset.example_dataset.dataset_id
#   table_id   = "foo"
#   schema = file("bq_schema.json")
# }

locals {
  datasets        = { for dataset in var.datasets : dataset["dataset_id"] => dataset }
  tables          = { for table in var.tables : table["table_id"] => table }
  views           = { for view in var.views : view["view_id"] => view }

  iam_to_primitive = {
    "roles/bigquery.dataOwner" : "OWNER"
    "roles/bigquery.dataEditor" : "WRITER"
    "roles/bigquery.dataViewer" : "READER"
  }
}

#this is the test for dataset list creation
resource "google_bigquery_dataset" "bq_dataset" {
  for_each        = local.datasets
  friendly_name   = each.value["friendly_name"]
  dataset_id      = each.key
  location        = each.value["location"] 
  project         = var.project_id

  dynamic "default_encryption_configuration" {
    for_each = var.encryption_key == null ? [] : [var.encryption_key]
    content {
      kms_key_name = var.encryption_key
    }
  }

  dynamic "access" {
    for_each = var.access

    content {
      # BigQuery API converts IAM to primitive roles in its backend.
      # This causes Terraform to show a diff on every plan that uses IAM equivalent roles.
      # Thus, do the conversion between IAM to primitive role here to prevent the diff.
      role = lookup(local.iam_to_primitive, access.value.role, access.value.role)

      domain         = lookup(access.value, "domain", null)
      group_by_email = lookup(access.value, "group_by_email", null)
      user_by_email  = lookup(access.value, "user_by_email", null)
      special_group  = lookup(access.value, "special_group", null)
    }
  }
 }

resource "google_bigquery_table" "bq_table" {
  for_each        = local.tables
  dataset_id      = each.value["dataset_id"]
  friendly_name   = each.key
  table_id        = each.key
  labels          = each.value["labels"]
  schema          = file(each.value["schema"])
  clustering      = each.value["clustering"]
  expiration_time = each.value["expiration_time"]
  project         = var.project_id
  deletion_protection = each.value["deletion_protection"]
  depends_on = [google_bigquery_dataset.bq_dataset]
  
  dynamic "time_partitioning" {
    for_each = each.value["time_partitioning"] != null ? [each.value["time_partitioning"]] : []
    content {
      type                     = time_partitioning.value["type"]
      expiration_ms            = time_partitioning.value["expiration_ms"]
      field                    = time_partitioning.value["field"]
      require_partition_filter = time_partitioning.value["require_partition_filter"]
    }
  }
  
  dynamic "range_partitioning" {
    for_each = each.value["range_partitioning"] != null ? [each.value["range_partitioning"]] : []
    content {
      field = range_partitioning.value["field"]
      range {
        start    = range_partitioning.value["range"].start
        end      = range_partitioning.value["range"].end
        interval = range_partitioning.value["range"].interval
      }
    }
  }
  
}

resource "google_bigquery_table" "bq_view" {
  for_each      = local.views
  dataset_id    = each.value["dataset_id"]
  friendly_name = each.key
  table_id      = each.key
  labels        = each.value["labels"]
  project       = var.project_id
  deletion_protection = each.value["deletion_protection"]
  depends_on = [google_bigquery_table.bq_table]

  view {
    query          = each.value["query"]
    use_legacy_sql = each.value["use_legacy_sql"]
  }
}

# resource "google_bigquery_table" "external_table" {
#   for_each        = local.external_tables
#   dataset_id      = each.value["dataset_id"]
#   friendly_name   = each.key
#   table_id        = each.key
#   labels          = each.value["labels"]
#   expiration_time = each.value["expiration_time"]
#   deletion_protection = each.value["deletion_protection"]
#   project         = var.project_id
#   depends_on = [google_bigquery_dataset.bq_dataset]

#   external_data_configuration {
#     autodetect            = each.value["autodetect"]
#     compression           = each.value["compression"]
#     ignore_unknown_values = each.value["ignore_unknown_values"]
#     max_bad_records       = each.value["max_bad_records"]
#     schema                = each.value["schema"]
#     source_format         = each.value["source_format"]
#     source_uris           = each.value["source_uris"]

#     dynamic "csv_options" {
#       for_each = each.value["csv_options"] != null ? [each.value["csv_options"]] : []
#       content {
#         quote                 = csv_options.value["quote"]
#         allow_jagged_rows     = csv_options.value["allow_jagged_rows"]
#         allow_quoted_newlines = csv_options.value["allow_quoted_newlines"]
#         encoding              = csv_options.value["encoding"]
#         field_delimiter       = csv_options.value["field_delimiter"]
#         skip_leading_rows     = csv_options.value["skip_leading_rows"]
#       }
#     }

#     dynamic "google_sheets_options" {
#       for_each = each.value["google_sheets_options"] != null ? [each.value["google_sheets_options"]] : []
#       content {
#         range             = google_sheets_options.value["range"]
#         skip_leading_rows = google_sheets_options.value["skip_leading_rows"]
#       }
#     }

#     dynamic "hive_partitioning_options" {
#       for_each = each.value["hive_partitioning_options"] != null ? [each.value["hive_partitioning_options"]] : []
#       content {
#         mode              = hive_partitioning_options.value["mode"]
#         source_uri_prefix = hive_partitioning_options.value["source_uri_prefix"]
#       }
#     }
#   }
# }

#added - 1220223
# resource "google_bigquery_dataset" "bq_dataset_ignore_access" {
#   for_each        = local.datasets_lyf
#   friendly_name   = each.value["friendly_name"]
#   dataset_id      = each.key
#   location        = each.value["location"] 
#   project         = var.project_id
 
#   dynamic "default_encryption_configuration" {
#     for_each = var.encryption_key == null ? [] : [var.encryption_key]
#     content {
#       kms_key_name = var.encryption_key
#     }
#   }

#   dynamic "access" {
#     for_each = var.access_lyf

#     content {
#       # BigQuery API converts IAM to primitive roles in its backend.
#       # This causes Terraform to show a diff on every plan that uses IAM equivalent roles.
#       # Thus, do the conversion between IAM to primitive role here to prevent the diff.
#       role = lookup(local.iam_to_primitive, access.value.role, access.value.role)

#       domain         = lookup(access.value, "domain", null)
#       group_by_email = lookup(access.value, "group_by_email", null)
#       user_by_email  = lookup(access.value, "user_by_email", null)
#       special_group  = lookup(access.value, "special_group", null)
      
#     }
#   }

#    lifecycle {
#     ignore_changes = [
#        access   #ignoring changes to access
#     ]
#   } 
# }