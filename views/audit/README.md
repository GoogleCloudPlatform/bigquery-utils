There are two views in this folder: query_audit and query_audit_new
query_audit is a view that simplifies querying audit logs to power dashboards, where the logs are old (https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/AuditData)
query_audit_new is also a view that simplifies querying BigQueryMetaData logs, but operates for new logs (https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata)

query_audit_new queries on logs produced from this Stackdriver filter:

protoPayload.metadata.@type="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata"
