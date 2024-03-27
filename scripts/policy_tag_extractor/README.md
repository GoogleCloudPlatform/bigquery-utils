# BigQuery Policy Tag Extractor

## Introduction
This repo contains a bash script for extracting BigQuery policy tag information from a given dataset. The script will review all the tables in a dataset, and output the table name, column, name, and policy tag ID in a CSV format.

## Instructions for use
The simplest way to execute this script is to run it directly in Cloud Shell, but if needed it can be executed as part of a larger CI/CD pipeline or process.

Before using, make sure to update the bash script with the dataset that needs to be reviewed.

To exceute in Cloud Shell:
1. Start a new session in the GCP project where your BigQuery data resides
2. Open Cloud Shell
3. Upload policy_tag_export.sh to the Cloud Shell environment
4. Execute the script by running "bash policy_tag_export.sh"
5. List the resources in Cloud Shell (ls) and verify that a file called "policy_tags.csv" was created
6. Download the file

## Considerations
* Ensure either you or the service account executing the bash script has the bigquery.metadataViewer role to access the required level of information.
* The extractor can identify specific policy tags on columns, but is limited to the information available to the bq command line tool. In it's current state, this is the full policy tag identifier:

projects/<PROJECT_ID>/locations/<LOCATION>/taxonomies/<TAXONOMY_ID>/policyTags/<TAG_ID>