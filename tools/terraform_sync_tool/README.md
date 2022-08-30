# Terraform Sync Tool

This directory contains the Terraform Sync Tool. This tool intentionally fails your CI/CD pipeline when schema drifts occur between 
what your BigQuery Terraform resources declare and what's actually present in your BigQuery environment. 
Theses schema drifts happen when BigQuery tables are updated by processes outside of Terraform (ETL process may dynamically add new columns when loading data into BigQuery). 
When drifts occur, you end up with outdated BigQuery Terraform resource files. This tool detects the schema drifts, 
traces the origins of the drifts, and alerts developers/data engineers (by failing the CI/CD pipeline) 
so they can patch the Terraform in their current commit.


Terraform Sync Tool can be integrated into your CI/CD pipeline. You'll need to add two steps to CI/CD pipeline. 
- Step 0: Run the Terraform plan command (using either Terraform/Terragrunt) with the `-json` option  and write the output into a JSON file using the caret operator `> output.json`
- Step 1: Use Python scripts to identify and investigate the drifts

## How to run Terraform Schema Sync Tool

```bash
###############
# Using Terragrunt
###############
terragrunt run-all plan -json --terragrunt-non-interactive > plan_output.json
python3 terraform_sync.py plan_output.json <YOUR_GCP_PROJECT_ID>
##############
# Using Terraform
##############
terraform plan -json > plan_output.json
python3 terraform_sync.py plan_output.json <YOUR_GCP_PROJECT_ID>
```

## How Terraform Schema Sync Tool Works

![Architecture Diagram](architecture.png)

**Executing the Sync Tool**

The Terraform Sync Tool will be executed as part of the CI/CD pipeline build steps triggered anytime when developers make a change to the linked repository. A build step specifies an action that you want Cloud Build to perform. For each build step, Cloud Build executes a docker container as an instance of docker run. 

**Step 0: Terraform Detects Drifts**

`deploy.sh` contains terraform plan command that writes event outputs into `plan_out.json` file. We'll use `plan_out.json` file for further investigation in the future steps. Feel free to repace `plan_out.json` with your JSON filename. we can pass through the variables ${env} and ${tool} if any. 

**Step 1: Investigate Drifts** 

 `requirements.txt` specifies python dependencies, and `terraform_sync.py` contains python scripts to
investigate terraform event outputs stored from step 0 to detect and address schema drifts

In the python scripts(`terraform_sync.py`), we firstly scan through the output by line to identify all the drifted tables and store their table names. 
After storing the drifted table names and converted them into the table_id format:[gcp_project_id].[dataset_id].[table_id], we make API calls, to fetch the latest table schemas from BigQuery. 

**Step 2: Fail Builds and Notify Expected Schemas** 

Once the schema drifts are detected and identified, we fail the build and notify the developer who makes changes to the repository. The notifications will include the details and the expected schemas in order keep the schema files up-to-date with the latest table schemas in BigQuery. 

To interpret the message, the expected table schema is in the format of [{table1_id:table1_schema}, {table2_id: table2_schema}, ...... ]. table_id falls in the format of [gcp_project_id].[dataset_id].[table_id] 

**What is Terragrunt?**

Terragrunt(https://terragrunt.gruntwork.io/docs/getting-started/install) is a framework on top of Terraform with some new tools out-of-the-box. 
Using new files *.hcl and new keywords, you can share variables across terraform modules easily.

## How to run this sample repo?

#### Fork and Clone this repo

#### Folder Structure 
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

#### Go to the directory you just cloned, and update

- **YOUR_GCP_PROJECT_ID** in `./qa/terragrunt.hcl` 
- **YOUR_BUCKET_NAME** in `./qa/terragrunt.hcl` 
- **YOUR_DATASET_ID** in `./qa/terraform-sync-tool/terragrunt.hcl` 

### Use Terraform/Terragrunt commands to test if any resources drifts existed

Terragrunt/Terraform commands:
```
terragrunt run-all plan -json --terragrunt-non-interactive

# Terraform Command
terraform plan -json
```

After running the Terrform plan command, **the event type "resource_drift"("type": "resource_drift") indicates a drift has occurred**.
If drifts detected, please update your terraform configurations and address the resource drifts based on the event outputs.


#### Add Could Build Steps to your configuration file

Please check cloud build steps in `cloudbuild.yaml` file, and add these steps to your Cloud Build Configuration File.

- step 0: run terraform commands in `deploy.sh` to detects drifts

Add `deploy.sh` to your project directory. 

- step 1: run python scripts to investigate terraform output

Add `requirements.txt` and `terraform_sync.py` to your project directory.

#### (Optional if you haven't created a Cloud Build Trigger) Create and configure a new Trigger in Cloud Build
Make sure to indicate your cloud configuration file location correctly. In this sample repo, use `tools/terraform_sync_tool/cloudbuild.yaml` as your cloud configuration file location

#### That's all you need! Let's commit and test in Cloud Build!
