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