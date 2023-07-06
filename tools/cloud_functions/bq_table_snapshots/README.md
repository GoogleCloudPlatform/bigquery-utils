# Automate BigQuery Snapshots at Dataset Level

This solution schedules and automates [BigQuery Snapshot](https://cloud.google.com/bigquery/docs/table-snapshots-intro) creation at dataset level, helping create a single point in time picture of our data at scale.  

This solution allows for:

* Any number of datasets - This is especially useful for large organizations undergoing data growth since new datasets can be added just by adding a new cloud scheduler job.
* Any frequency - Users can specify desired snapshot frequency (e.g. daily, weekly, monthly, …) by changing the crontab_format schedule. 
* Customization for tables of choice - Specific tables can be easily included and excluded and snapshot duration easily specified by changing the triggering message body with minimal effort.


## Solution architecture
![alt text](./architecture_diagram.png)

## bq-snap-start-process
The **bq-snap-start-process** Cloud Scheduloer Job will run monthly and trigger the snapshot creation process for dataset_1. The Pub/Sub message body will contain parameters for the snapshot creation, as shown in the example below:
 
```
{
    "source_dataset_name":"DATASET_1",
    "target_dataset_name":"DATASET_1_MONTHLY_NAME",
    "crontab_format":"10 * * * *",
    "seconds_before_expiration":2592000,
    "tables_to_include_list":[],
    "tables_to_exclude_list":[] 
}
```

## bq_backup_fetch_tables_names
The **bq_backup_fetch_tables_names** cloud function will fetch all the table names in source_dataset_name. It will then apply filters based on tables_to_include_list and tables_to_exclude_list to determine the tables in scope. Finally, it will submit one Pub/Sub message per table. 

The following environemnt variables must be set:
* `DATA_PROJECT_ID` id of project used for BQ storage 
* `PUBSUB_PROJECT_ID` id of project with P/S topic
* `TABLE_NAME_PUBSUB_TOPIC_ID` ame of P/S topic where this code will publish to

## bq_backup_create_snapshots
The **bq_backup_create_snapshots** cloud function will submit a BigQuery job to create a snapshot for each table in scope. This cloud function will suffix the snapshot name with the snapshot datetime to guarantee a unique name. It will also calculate and set the expiration time of the snapshot based on seconds_before_expiration. Finally, it will determine the snapshot time based on crontab_format. 
The following environemnt variables must be set:
* `BQ_DATA_PROJECT_ID `id of project used for BQ storage
* `BQ_JOBS_PROJECT_ID` id of project used for BQ compute


### About the crontab_format field
If DATASET_1 has 500 tables, 500 Pub/Sub messages are sent, and 500 Cloud Function invocations are performed. If the Cloud Function used the current time when it creates the snapshots then these 500 snapshots will represent different points in time. To avoid this the Cloud Function will create the snapshots for the table as they were when the Cloud Scheduler job (bq-snap-start-process) was triggered. To achieve this the Cloud Function will calculate the previous interval based on **crontab_format**.


# Deployment

## Declare Variables

```
PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
STORAGE_PROJECT_ID=`gcloud config list --format "value(core.project)" 2>/dev/null`
SOURCE_DATASET_NAME="DATASET_1"
TARGET_DATASET_NAME="SNAPSHOT_DATASET_1"
CRONTAB_FORMAT="10 * * * *"
SECONDS_BEFORE_EXPIRATION=604800
```

* `PROJECT_ID` is the project where processing will happen, where the resurces will be hosted (e.g. Pub / Sub topics, Cloud Functions).
* `STORAGE_PROJECT_ID` is the project where BigQuery tables are stored.

**Note**: in this case `PROJECT_ID` and `STORAGE_PROJECT_ID` are the same but that is not necesarily the case. 


## Terraform Provisioning
```
git clone https://github.com/GoogleCloudPlatform/bigquery-utils.git
cd ./bigquery-utils/tools/cloud_functions/bq_table_snapshots/terraform
terraform init
```

```
terraform apply \
 -var="project_id=${PROJECT_ID}" \
 -var="storage_project_id=${STORAGE_PROJECT_ID}" \
 -var="source_dataset_name=${SOURCE_DATASET_NAME}" \
 -var="target_dataset_name=${TARGET_DATASET_NAME}" \
 -var="crontab_format=${CRONTAB_FORMAT}" \
 -var="seconds_before_expiration=${SECONDS_BEFORE_EXPIRATION}" \
 --auto-approve
```
