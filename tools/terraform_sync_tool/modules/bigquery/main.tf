locals {
  datasets        = { for dataset in var.datasets : dataset["dataset_id"] => dataset }
  tables          = { for table in var.tables : table["table_id"] => table }

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