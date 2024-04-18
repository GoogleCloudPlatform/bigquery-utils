# BigQuery Policy Tag Extractor

## Introduction
This directory contains the [policy_tag_export.sh](policy_tag_export.sh) bash script which extracts BigQuery policy tag information from a given dataset. The script will iterate through at most 10K tables in a dataset and then for every column with a policy tag, it will output the table name, column name, and policy tag ID in CSV format.

## Instructions for use
The simplest way to execute this script is to run it directly in Cloud Shell, but if needed it can be executed as part of a larger CI/CD pipeline or process.

Before using, make sure to update the bash script with the dataset that needs to be reviewed.

To exceute in Cloud Shell:
1. [Launch a Cloud Shell session](https://cloud.google.com/shell/docs/launching-cloud-shell) in the GCP project where your BigQuery data resides. 
  * When Cloud Shell is started, the active project in Cloud Shell is propagated to your gcloud configuration inside Cloud Shell for immediate use. GOOGLE_CLOUD_PROJECT, the environmental variable used by Application Default Credentials library, is also set to point to the active project in Cloud Shell. You can also explicitly set the project using `gcloud config set project [PROJECT_ID]`.
1. [Upload](https://cloud.google.com/shell/docs/uploading-and-downloading-files#upload_and_download_files_and_folders) the policy_tag_export.sh script to the Cloud Shell environment.
1. Execute the script by running `bash policy_tag_export.sh`.
1. List the resources in Cloud Shell (ls) and verify that a file called "policy_tags.csv" was created.
1. [Download](https://cloud.google.com/shell/docs/uploading-and-downloading-files#upload_and_download_files_and_folders) the file.

## Considerations
* Ensure either you or the service account executing the bash script has the bigquery.metadataViewer role to access the required level of information.
* The extractor can identify specific policy tags on columns, but is limited to the information available to the bq command line tool. In it's current state, this is the full policy tag identifier:

projects/<PROJECT_ID>/locations/<LOCATION>/taxonomies/<TAXONOMY_ID>/policyTags/<TAG_ID>