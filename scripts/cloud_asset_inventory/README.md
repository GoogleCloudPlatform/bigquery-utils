# Cloud Asset Inventory Scripts

This folder contains scripts that can be run against Cloud Asset Inventory exports to BigQuery. Refer to the [public documentation](https://cloud.google.com/asset-inventory/docs/exporting-to-bigquery) for directions on setting up export to BigQuery. It is recommended to perform this export as part of a schedule. The tables would be partitioned by either `read-time` or `request-time`. In the attached scripts, the tables are assumed to be partitioned by `read-time`.

The schema for the BigQuery tables generated from this export is given in the section [here](https://cloud.google.com/asset-inventory/docs/exporting-to-bigquery#bigquery-schema).

Here are the scripts that are provided.

### [BigQuery Table Readers](./bq_table_all_readers.sql)

This script will help Data Stewards or Platform Owners determine the which are the IAM Principals (groups, user or service accounts) that can read data from a BigQuery table.

The access to the principal could be applied at any level of the resource hierarchy - Org, Folder, Project or Dataset.



### [BigQuery Table Editors](./bq_table_all_editors.sql)

This script will help Data Stewards or Platform Owners determine the which are the IAM Principals (groups, user or service accounts) that can edit/write data to a BigQuery table.

The access to the principal could be applied at any level of the resource hierarchy - Org, Folder, Project or Dataset.
