# Terraform Sync Tool

This directory contains the setup for the Terraform Sync Tool. Terraform Sync Tool was designed to keep the 
Terraform schemas up-to-date with the BigQuery table schemas in production environment.

## Prerequisite
Before building the terraform sync tool, please ensure billing and Cloud Build are enabled for your Cloud project.

## Understand the Directory

## TODOs
Please make sure to update YOUR_GCP_PROJECT_ID in "./qa/terragrunt.hcl" and YOUR_DATASET_ID in "./qa/terraform-sync-tool/terragrunt.hcl"

## Setup

Use Cloud SDK to set the specified property in your active configuration only
```
gcloud config set project <your-project-id>
```

### Local Test

To test using terragrunt commands
```
env = qa
tool = terraform-sync-tool
echo $env
echo $tool
terragrunt run-all plan -json --terragrunt-non-interactive --terragrunt-working-dir="${env}"/"${tool}" > plan_out.json
```

To test using terragrunt commands without writing the output into a JSON file
```
terragrunt run-all plan -json --terragrunt-non-interactive --terragrunt-working-dir="${env}"/"${tool}"
```

Provide argument to test terraform_sync.py
```
terraform_sync.py plan_out.json
```
