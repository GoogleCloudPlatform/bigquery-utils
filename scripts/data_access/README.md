
# Sample Data Access Audit View

## Overview

Google Cloud offers data access logs across many [services](https://cloud.google.com/logging/docs/audit/services).
This sample will walk through [enabling data access logs](https://cloud.google.com/logging/docs/audit/configure-data-access)
in a project, creating a Stackdriver sink, and creating a BigQuery View providing analytical information.

## audit_summary View

The View analyses READ, WRITE, and ADMIN operations across BigQuery datasets and GCS buckets. The logs
contains object and table-level information, but this is dropped.

There is one data access log entry for every service executing. There is also an associated
list in protopayload_auditlog.authorizationInfo that contains the list of permissions
granted (or denied) as part of the service execution.

These permissions are listed for [GCS permissions](https://cloud.google.com/storage/docs/access-control/iam-permissions) and
[BigQuery permissions](https://cloud.google.com/bigquery/docs/access-control#bq-permissions).

The columns of the View are the following:
| Column | Description |
| ------ | ----------- |
| hour | Top-of-the-hour the access occurred |
| service | Service (storage or bigquery) |
| actor | Service account or end-user |
| op | Operation (READ, WRITE, or ADMIN) |
| granted | Whether access was permitted |
| entity | <project>.<GSC bucket> or <project>.<BigQuery dataset> |

## Instructions

Capture the PROJECT_ID of your default project.

    PROJECT_ID=$(gcloud config get-value core/project)

Enable data access audit logs.

    POLICY_FILE=/tmp/policy_file_${PROJECT_ID}.$$
    
    # Get existing project policy
    gcloud projects get-iam-policy ${PROJECT_ID} --format=json > ${POLICY_FILE}
    
    # Merge new_audit_policy.json into the policy
    cat ${POLICY_FILE} | \
      jq --slurpfile audit data_access_policy.json '.auditConfigs=$audit' \
      > ${POLICY_FILE}.new
    
    # Apply the new policy to the project
    gcloud projects set-iam-policy ${PROJECT_ID} ${POLICY_FILE}.new
    if [ $? -ne 0 ]; then
      echo Failed applying policy
    fi
    
    rm $POLICY_FILE

Create your data_access dataset.

    bq mk data_access

Create a data access audit sink. Be sure to grant BigQuery Data Editor role to the appropriate service account.

    gcloud logging sinks create compute_activity \
      bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/data_access \
      --log-filter="logName=\"projects/${PROJECT_ID}/logs/cloudaudit.googleapis.com%2Fdata_access\""

Create your data_access.audit_summary VIEW.

    sed -e "s/\${PROJECT_ID}/${PROJECT_ID}/g" ./create_data_access_view.sql | bq query

