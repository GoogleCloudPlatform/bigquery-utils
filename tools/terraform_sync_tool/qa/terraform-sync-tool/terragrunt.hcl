terraform {
  source = "../../modules/bq-setup"
}

include "root" {
  path   = find_in_parent_folders()
  expose = true
}

locals {
  dataset_id = "tf_test_sync_tool"
}

inputs = {
  project_id = include.root.inputs.project_id
  # The ID of the project in which the resource belongs. If it is not provided, the provider project is used.
  datasets = [
    {
      dataset_id    = "${local.dataset_id}"
      friendly_name = "Dataset for Terraform Sync Tool"
      location      = "US"
      labels        = {}
    }
  ]

  tables = [
    {
      table_id            = "TableForTest"
      dataset_id          = "${local.dataset_id}"
      schema              = "json_schemas/TableForTest.json"
      clustering          = []
      expiration_time     = null
      deletion_protection = true
      range_partitioning  = null
      time_partitioning   = null
      labels              = {}
    }
  ]
}