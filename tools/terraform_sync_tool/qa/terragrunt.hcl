# Indicate where to source the terraform module from.
# The URL used here is a shorthand for
# "tfr://registry.terraform.io/terraform-aws-modules/vpc/aws?version=3.5.0".
# Note the extra `/` after the protocol is required for the shorthand
# notation.

locals {
  #TODO: Update your GCP Project ID
  gcp_project_id = "YOUR_GCP_PROJECT_ID" #YOUR_GCP_PROJECT_ID
  bucket_name = "YOUR_GCP_BUCKET_NAME" #YOUR_GCP_BUCKET_NAME
}

inputs = {
  project_id = local.gcp_project_id
  gcp_region = "us-central1"
}

generate "provider" {
  path      = "provider.tf"
  if_exists = "overwrite"
  contents  = <<EOF
provider "google" {
  project = "${local.gcp_project_id}"
}
  EOF
}

remote_state {
  backend = "gcs"
  config = {
    project  = local.gcp_project_id
    location = "us"
    bucket   = local.bucket_name
    prefix   = "${path_relative_to_include()}"
    gcs_bucket_labels = {
      owner = "terragrunt_test"
      name  = "terraform_state_storage"
    }
  }
  generate = {
    path      = "backend.tf"
    if_exists = "overwrite_terragrunt"
  }
}
