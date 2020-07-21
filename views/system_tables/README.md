# BigQuery System Tables Reports
This view illustrates how users can leverage BigQuery's INFORMATION_SCHEMA metadata tables to understand their organization's slot and reservation utilization, job execution, and job errors. Users can use this dashboard and its underlying queries as-is, or use them as a starting point for more complex queries and/or visualizations.

The dashboard is comprised of the following reports:
1. [Daily Utilization Report](./pages/daily_utilization.md)
2. [Hourly Utilization Report](./pages/hourly_utilization.md)
3. [Reservation Utilization Report](./pages/reservation_utilization.md)
4. [Job Execution Report](./pages/job_execution.md)
5. [Job Error Report](./pages/job_error.md)

The above links will direct you to documentation for each individual report which will describe its contents in more detail.

The underlying SQL queries for each report can be found [here](./sql). These queries reference tables in a sample public dataset that was generated from real INFORMATION_SCHEMA usage data but was slightly modified to anonymize values and clean some data.

#### The following steps describe how to make a copy of this dashboard and the underlying data sources for use as-is.

### Prerequisites
In order to create the dashboard and query the INFORMATION_SCHEMA tables a user must have access to the following INFORMATION_SCHEMA tables:
- INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
- INFORMATION_SCHEMA.CAPACITY_COMMITMENT_CHANGES_BY_PROJECT
- INFORMATION_SCHEMA.RESERVATION_CHANGES_BY_PROJECT
- INFORMATION_SCHEMA.ASSIGNMENT_CHANGES_BY_PROJECT
Detailed information about IAM permissions for each table can be found [here](https://cloud.google.com/bigquery/docs/information-schema-jobs#required_permissions) and [here](https://cloud.google.com/bigquery/docs/information-schema-reservations#required_permissions). Note that because this dashboard uses "owner" data credentials, only the owners of the dashboard require access to the underlying tables. More information about data credentials in Data Studio can be found [here](https://support.google.com/datastudio/answer/6371135).

### 1. Copy the data sources
Log in to Data Studio and create a copy of the following data sources. More information on copying data sources can be found [here](https://support.google.com/datastudio/answer/7421646?hl=en&ref_topic=6370331).

1. [Daily Utilization]()
2. [Commitments Timeline]()
3. [Hourly Utilization]()
4. [Current Assignments]()
5. [Reservation Utilization 7 Days]()
6. [Reservation Utilization 30 Days]()
7. [Job Usage]()
8. [Job Errors]()

Once a copy is made, Data Studio will display the details for the data source. For each data source, enter the project_id of the Billing Project. It is recommended to use the administration project where the capacity commitments were purchased, however a different billing project can be used.

Update the data source to reference your project's INFORMATION_SCHEMA tables as follows:
`region-{region_name}`.INFORMATION_SCHEMA.{table}
where {region_name} is the name of the region or multi-region where your commitments and reservations are located.

If you are using a billing project that is **different** from the administration project, update the data source as follows:
`{project_id}`.`region-{region_name}`.INFORMATION_SCHEMA.{table}
where {project_id} is the project id of the billing project and {region_name} is the name of the region or multi-region where your commitments and reservations are located.

When copying the Reservation Utilization data sources, you must also do the following:
1. Replace all instances "admin-project:US." with "{project_id}:{location}."
2. Replace all instances of TIMESTAMP("2020-07-15 23:59:59.000 UTC") with CURRENT_TIMESTAMP()

Once all modifications are complete and a Billing Project is specified, click "Reconnect".

### 2. Copy the Report
Create a copy of the [public dashboard](). You will be asked to choose a new data source for each data source in the report. Select the appropriate data sources from the ones you copied in step 1. Click on create report and rename it as desired.

Once the report is copied and all of the data is rendered, modify any date pickers in the report pages to use the time period you desire (ex: last 14 days, last 28 days, etc).
