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

You'll need to install Terragrunt(https://terragrunt.gruntwork.io/docs/getting-started/install)

## What is Terragrunt?
Terragrunt is a framework on top of Terraform with some new tools out-of-the-box. 
Using new files *.hcl and new keywords, you can share variables across terraform modules easily.

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

## How to run Terraform Schema Sync Tool(TODO)

#### Use Terraform/Terragrunt commands to test if any resources drifts existed

Terragrunt/Terraform commands:
```
terragrunt run-all plan -json --terragrunt-non-interactive

# If you need to pass variables to specify working directory
env = VALUE_OF_ENV  # Value of env, for example "qa"
tool = VALUE_OF_TOOL  # Value of tool, for example "terraform-sync-tool"
terragrunt run-all plan -json --terragrunt-non-interactive --terragrunt-working-dir="${env}"/"${tool}"

# If you need to write outputs into a json file. Feel free to replace `plan_out.json` with your JSON FILENAME.
terragrunt run-all plan -json --terragrunt-non-interactive > plan_out.json

# If you need to write outputs into a json file with variables specified. Feel free to replace `plan_out.json` with your JSON FILENAME.
env = VALUE_OF_ENV  # Value of env, for example "qa"
tool = VALUE_OF_TOOL  # Value of tool, for example "terraform-sync-tool"
terragrunt run-all plan -json --terragrunt-non-interactive --terragrunt-working-dir="${env}"/"${tool}" > plan_out.json
```

After running the Terrform plan command, **the event type "resource_drift"("type": "resource_drift") indicates a drift has occurred**.
If drifts detected, please update your terraform configurations and address the resource drifts based on the event outputs.


#### Add Could Build Steps to your configuration file

Please check cloud build steps in `cloudbuild.yaml` file, and add these steps to your Cloud Build Configuration File.

Here are two steps in `cloudbuild.yaml` for Terraform Schema Sync Tool integration: 

- step 0: run terraform commands in `deploy.sh` to detects drifts

Add `deploy.sh` to your project directory. `deploy.sh` contains terraform plan command that writes event outputs into `plan_out.json` file. We'll use `plan_out.json` file for further investigation in the future steps. Feel free to repace `plan_out.json` with your JSON filename.


- step 1: run python scripts to investigate terraform output

Add `requirements.txt` and `terraform_sync.py` to your project directory. `requirements.txt` specifies python dependencies, and `terraform_sync.py` contains python scripts to
investigate terraform event outputs stored from step 0 to detect and address schema drifts

#### (Optional if you haven't created Cloud Build Trigger) Create and configure a new Trigger in Cloud Build
Make sure to indicate your cloud configuration file location correctly.

#### That's all you need! Let's commit and test in CLoud Build!


## How to run this sample repo?

#### Fork and Clone this repo

#### Go to the directory you just cloned, and update

- **YOUR_GCP_PROJECT_ID** in `./qa/terragrunt.hcl` 
- **YOUR_BUCKET_NAME** in `./qa/terragrunt.hcl` 
- **YOUR_DATASET_ID** in `./qa/terraform-sync-tool/terragrunt.hcl` 

#### (First time only) Use terraform plan & apply to deploy your resource to you GCP Project

```
env = qa
tool = terraform-sync-tool
echo $env
echo $tool
terragrunt run-all plan -json --terragrunt-non-interactive --terragrunt-working-dir="${env}"/"${tool}"
terragrunt run-all apply -json --terragrunt-non-interactive --terragrunt-working-dir="${env}"/"${tool}"
```
#### Create and configure a new Trigger in Cloud Build
Make sure to indicate your cloud configuration file location correctly. 
In this sample repo, use `tools/terraform_sync_tool/cloudbuild.yaml` as your cloud configuration file location

### How to test each Build Step without triggering Cloud Build?

- To test using terragrunt commands. Feel free to replace `plan_out.json` with your JSON FILENAME and change values of variables.
```
env = qa
tool = terraform-sync-tool
echo $env
echo $tool
terragrunt run-all plan -json --terragrunt-non-interactive --terragrunt-working-dir="${env}"/"${tool}" > plan_out.json
```

- To test using terragrunt commands without writing the output into a JSON file
```
terragrunt run-all plan -json --terragrunt-non-interactive --terragrunt-working-dir="${env}"/"${tool}"
```

- To test python scripts. `terraform_sync.py` requires two arguments: JSON filename and gcp_project_id. Provide arguments to test `terraform_sync.py`. Feel free to replace `plan_out.json` with your JSON FILENAME.
```
terraform_sync.py plan_out.json <YOUR_GCP_PROJECT_ID>
```
