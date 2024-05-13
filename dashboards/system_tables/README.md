# BigQuery System Tables Reports
This view illustrates how users can leverage BigQuery's [`INFORMATION_SCHEMA` metadata tables](https://cloud.google.com/bigquery/docs/information-schema-intro) to understand their organization's slot and reservation utilization, job execution, and job errors. Users can use this dashboard and its underlying queries as-is, or use them as a starting point for more complex queries and/or visualizations.

The dashboard is comprised of the following reports:
1. [Daily Utilization Report](./docs/daily_utilization.md)
2. [Hourly Utilization Report](./docs/hourly_utilization.md)
3. [Reservation Utilization Report](./docs/reservation_utilization.md)
4. [Job Execution Report](./docs/job_execution.md)
5. [Job Error Report](./docs/job_error.md)
6. [Job Comparison Report](./docs/job_comparison.md)

The above links will direct you to documentation for each individual report which will describe its contents in more detail.

The underlying SQL queries for each report can be found [here](./sql). These queries reference tables in a sample public dataset that was generated from real `INFORMATION_SCHEMA` usage data but was slightly modified to anonymize values and clean some data.

#### The following steps describe how to make a copy of this dashboard and the underlying data sources for use as-is.

### Prerequisites
In order to create the dashboard and query the INFORMATION_SCHEMA tables a user must have access to the following INFORMATION_SCHEMA tables:
- `INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`
- `INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION`
- `INFORMATION_SCHEMA.CAPACITY_COMMITMENT_CHANGES_BY_PROJECT`
- `INFORMATION_SCHEMA.RESERVATION_CHANGES_BY_PROJECT`
- `INFORMATION_SCHEMA.ASSIGNMENT_CHANGES_BY_PROJECT`

Detailed information about IAM permissions for each table can be found [here](https://cloud.google.com/bigquery/docs/information-schema-jobs#required_permissions) and [here](https://cloud.google.com/bigquery/docs/information-schema-reservations#required_permissions). Note that because this dashboard uses "owner" data credentials, only the owners of the dashboard require access to the underlying tables. More information about data credentials in Data Studio can be found [here](https://support.google.com/datastudio/answer/6371135).

### 1. Data sources
#### 1.1 Copy the data sources
Log in to Data Studio and create a copy of the following data sources. More information on copying data sources can be found [here](https://support.google.com/datastudio/answer/7421646?hl=en&ref_topic=6370331).

1. [Daily Utilization](https://datastudio.google.com/u/0/datasources/ec6e4701-ec72-4d41-a196-1fc3fe4e9922)
2. [Commitments Timeline](https://datastudio.google.com/u/0/datasources/0e21cff7-0682-44cb-b484-a47e6a4d713a)
3. [Hourly Utilization](https://datastudio.google.com/u/0/datasources/41004f9c-d144-431e-879e-c2bf6283b456)
4. [Current Assignments](https://datastudio.google.com/u/0/datasources/1c6536bb-1135-44b2-9fdf-b7d6949dc338)
5. [Reservation Utilization 7 Days](https://datastudio.google.com/datasources/cd566619-dc5e-4d2e-9ddd-c8d6eac61fca)
6. [Reservation Utilization 30 Days](https://datastudio.google.com/u/0/datasources/6547f04e-3278-4576-91da-63a283c444e0)
7. [Job Usage](https://datastudio.google.com/u/0/datasources/041aadcc-d1fc-4ea9-8103-ad21059c94dd)
8. [Job Errors](https://datastudio.google.com/u/0/datasources/a4bedfd8-d496-4798-af03-1998f9c88efd)
9. [Job Comparison](https://datastudio.google.com/datasources/d5a10d1c-89e8-4a0e-a169-c21eab8cd273)
10. [Job Concurrency Slow](https://datastudio.google.com/datasources/b3c48dfa-65a0-4bd8-9b7f-4bab965fe695)
11. [Job Concurrency Fast](https://datastudio.google.com/datasources/c61def4f-99e2-4861-b882-d5d8ae0ab7a6)
12. [Job Analyzer - Slow Job](https://datastudio.google.com/datasources/5fae59f6-ce74-433d-bdec-42795b83cdf1)
13. [Job Analyzer - Fast Job](https://datastudio.google.com/datasources/aabb6aa4-4640-4698-aedb-1a00179e7508)

Please note that for the [Job Comparison Report](docs/job_comparison.md), you will need to make a copy of the [Job Concurrency Slow](sql/job_concurrency_comparison_slow.sql) and [Job Analyzer Slow](sql/job_analyzer_slow.sql) 
queries for the corresponding fast jobs data source. You will need to edit `@job_param` parameter on lines line 44 and line 153 respectively to `@job_param_2` or similar. Examples of how to do this are shown in the sample data sources above.

#### 1.2 Set the billing project
Once a copy is made, Data Studio will display the details for the data source. For each data source, enter the project id of the Billing Project. It is recommended to use the administration project where the capacity commitments were purchased, however a different billing project can be used.

#### 1.3 Modify the data sources
Update the data source to reference your project's `INFORMATION_SCHEMA` tables as follows:

```
`region-{region_name}`.INFORMATION_SCHEMA.{table}
```

where `{region_name}` is the name of the region or multi-region where your commitments and reservations are located.  
&nbsp;  

If you are using a billing project that is **different** from the administration project, update the data source as follows:

```
`{project_id}`.`region-{region_name}`.INFORMATION_SCHEMA.{table}
```

where `{project_id}` is the project id of the billing project and `{region_name}` is the name of the region or multi-region where your commitments and reservations are located.  
&nbsp;  

When copying the Reservation Utilization data sources, you must also do the following:
1. Replace all instances `"admin-project:US."` with `"{project_id}:{location}."`, where `{project_id}` is the project id of your administration project and `{location}` is the GCP region or multi-region where they are located.
2. Replace all instances of `TIMESTAMP("2020-07-15 23:59:59.000 UTC")` with `CURRENT_TIMESTAMP()`.

Once all modifications are complete and a Billing Project is specified, click "Reconnect".

### 2. Dashboard
#### 2.1 Copy the dashboard
Create a copy of the [public dashboard](https://datastudio.google.com/s/kGZzZJWkeyA). You will be asked to choose a new data source for each data source in the report. Select the appropriate data sources from the ones you copied in step 1. Click on create report and rename it as desired.

#### 2.2 Modify the date pickers
Once the report is copied and all of the data is rendered, modify any date pickers in the report pages to use the time period you desire (ex: last week, last 14 days, last 28 days, etc).
