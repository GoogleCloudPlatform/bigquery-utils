There are two views in this folder: **query_audit** (query_audit in this folder) and **query_audit_new** (query_audit_v2.sql in this folder)
query_audit is a view that simplifies querying audit logs to power dashboards, where the logs are old [(Audit Data)](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/AuditData)
query_audit_new is also a view that simplifies querying BigQueryMetaData logs, but operates for new logs [(BigQueryAuditMetadata)](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata)

1. Before **query_audit_v2** can be utilized, there must be at least one table with audit data from Stackdriver Logging. One way to generate this data is to connect Stackdriver Logging with BigQuery:
  1. In Stackdriver Logging, click the downwards facing arrow next to the text box, and switch to advanced mode. Paste the filter below to get the latest version of logs, and click submit:
**protoPayload.metadata.@type="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata"**
3. Click on "Create Sink" at the top of the page, choose BigQuery. 
4. In "Create Sink", name your sink, indicate what dataset you want the table to be created in. It is recommended to have "Use partitioned tables" checked, this will aggregate all logs across multiple days, as opposed to having one table per each day produced. 
5. Once you run a BigQuery job, you will see a table appear in BigQuery with the logs. 
6. To use the view, change your project path to where that table is located. 
7. From here, you can do further analysis in BigQuery by querying the view, or you can connect it to a BI tool such as DataStudio as a data source and build dashboards. 
