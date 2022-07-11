# Terraform Sync Tool

This directory contains the setup for the Terraform Sync Tool. Terraform Sync Tool was designed to address the schema drifts in BigQuery tables and keep the 
Terraform schemas up-to-date with the BigQuery table schemas in production environment.

Terraform Sync Tool can be integrated into your CI/CD pipeline using Cloud Build. You'll need to add two steps to `cloudbuild.yaml`. 
- Step 0: Use Terragrunt command to detect resource drifts and write output into a JSON file
- Step 1: Use Python scripts to identify and investigate the drifts

Cloud Build fails the build attemps if resource drifts are detected and notifies the latest resource information. Developers should be able to update
the Terraform resources accordingly. 

## Prerequisite
Before building the terraform sync tool, please ensure that billing and Cloud Build are enabled for your Cloud project.

You'll also need to install Terragrunt(https://terragrunt.gruntwork.io/docs/getting-started/install)

## Folder Structure
This directory serves as a starting point for your cloud project with terraform-sync-tool as one of qa tools integrated.

    .
    ├── modules                     # Terraform modules directory
    │   ├── bigquery                # Example Terraform BigQuery Setup
    │   └── ...                     # Other modules setup you have
    ├── qa                          # qa environment directory
    │   ├── terragrunt.hcl      
    │   └── terraform-sync-tool     # Tool terraform-sync-tool
    │           ├── json_schemas    # Terraform schema files 
    │           ├── terragrunt.hcl
    │           └── ...
    ├── cloudbuild.yaml             # Cloud Build configuration file
    ├── deploy.sh                   # Build Step 0 - contains terragrunt commands
    ├── requirements.txt            # Build Step 1 - Specifies python dependencies
    ├── terraform_sync.py           # Build Step 1 - python scripts
    └── ...                         # etc.


## TODO
Please make sure to update 
- **YOUR_GCP_PROJECT_ID** in `./qa/terragrunt.hcl` 
- **YOUR_BUCKET_NAME** in `./qa/terragrunt.hcl` 
- **YOUR_DATASET_ID** in `./qa/terraform-sync-tool/terragrunt.hcl` 

Please **create and configure your trigger in Cloud Build** and make sure to use `cloudbuild.yaml` as **Cloud Build configuration file location**

## Setup

Use Cloud SDK to set the specified property in your active configuration only
```
gcloud config set project <your-project-id>
```

### Local Test

To test using terragrunt commands. Feel free to replace `plan_out.json` with your JSON FILENAME.
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

Provide argument to test `terraform_sync.py`. Feel free to replace `plan_out.json` with your JSON FILENAME.
```
terraform_sync.py plan_out.json
```
