package com.google.bigquery;

import com.google.cloud.bigquery.*;
import com.google.gson.*;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Contains the logic to run query verification.
 */
public class QueryVerifier {

    private final QueryVerificationQuery migratedQuery;
    private final QueryVerificationSchema migratedSchema;

    private final QueryVerificationQuery originalQuery;
    private final QueryVerificationSchema originalSchema;

    public QueryVerifier(QueryVerificationQuery migratedQuery, @Nullable QueryVerificationSchema migratedSchema, @Nullable QueryVerificationQuery originalQuery, @Nullable QueryVerificationSchema originalSchema) {
        this.migratedQuery = migratedQuery;
        this.migratedSchema = migratedSchema;

        this.originalQuery = originalQuery;
        this.originalSchema = originalSchema;
    }

    /**
     * Determines which verification method to use based on provided inputs and runs the verification.
     */
    public void verify() {
        if (originalQuery != null && originalSchema != null) {
            verifyDataAware();
        } else {
            verifyDataFree();
        }
    }

    /**
     * Verifies migrated query by sending a dry-run query job to BQ to check for syntax and semantic errors.
     */
    public void verifyDataFree() {
        boolean verificationResult = false;

        BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

        List<Table> tables = new ArrayList<Table>();

        // Create tables based on schema
        if (migratedSchema != null) {
            if (migratedSchema.isInJsonFormat()) {
                // Schema is JSON
                List<TableInfo> tableInfos = QueryVerifier.getTableInfoFromJsonSchema(migratedSchema);
                if (tableInfos != null) {
                    for (TableInfo tableInfo : tableInfos) {
                        Table table = bigQuery.create(tableInfo);
                        tables.add(table);
                    }
                }
            } else {
                // Schema is DDL
                JobInfo jobInfo = configureJob(migratedSchema.schema(), false);
                Job schemaJob = bigQuery.create(jobInfo);
                try {
                    schemaJob.waitFor();
                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());
                }

                List<TableId> tableIds = QueryVerifier.getTableIdsFromDdlSchema(migratedSchema);
                for (TableId tableId : tableIds) {
                    Table table = bigQuery.getTable(tableId);
                    tables.add(table);
                }
            }

            if (tables.isEmpty()) {
                System.out.println(migratedSchema.path() + " is not correctly formatted.");
            }
        }

        // Create dry-run job
        JobInfo jobInfo = configureJob(migratedQuery.query(), true);

        // TODO Support multiple queries

        try {
            // Run dry-run
            Job queryJob = bigQuery.create(jobInfo);
            verificationResult = queryJob.getStatus().getState() == JobStatus.State.DONE;
        } catch (BigQueryException e) {
            // Print out syntax/semantic errors returned from BQ
            System.out.println(e.getMessage());
        }

        // Clear tables created
        for (Table table : tables) {
            BigQueryOptions.getDefaultInstance().getService().delete(table.getTableId());
        }

        System.out.printf("Data-Free Verification %s\n", verificationResult ? "Succeeded" : "Failed");
    }

    /**
     * Verifies migrated query by sending query jobs to BQ and TD to check for differences in the query results.
     */
    public void verifyDataAware() {
        boolean verificationResult = false;

        // TODO Implement data aware verification

        System.out.printf("Data-Aware Verification %s\n", verificationResult ? "Succeeded" : "Failed");
    }

    /**
     * Reads JSON schema to create table fields based on the schema
     * @param queryVerificationSchema Schema to read from
     * @return List of new table info
     */
    public static List<TableInfo> getTableInfoFromJsonSchema(QueryVerificationSchema queryVerificationSchema) {
        List<TableInfo> tableInfos = new ArrayList<TableInfo>();
        for (JsonElement schemaElement : queryVerificationSchema.getJsonArray()) {
            if (queryVerificationSchema.getJsonArray().size() == 0) {
                return null;
            }
            JsonObject schemaObject = schemaElement.getAsJsonObject();

            if (schemaObject.has("tableReference")) {
                JsonObject tableReference = schemaObject.get("tableReference").getAsJsonObject();

                if (tableReference.has("datasetId") && tableReference.has("tableId")) {
                    TableId tableId = TableId.of(tableReference.get("datasetId").getAsString(), tableReference.get("tableId").getAsString());

                    // Deserialize fields
                    FieldList fieldList = null;
                    try {
                        JsonArray schemaFields = schemaObject.getAsJsonArray("fields");

                        Gson gson = new GsonBuilder()
                                .registerTypeAdapter(FieldList.class, new BigQuerySchemaJsonDeserializer())
                                .create();
                        fieldList = gson.fromJson(schemaFields, FieldList.class);
                    } finally {
                        if (fieldList == null || fieldList.isEmpty()) {
                            // Error in formatting of fields
                            System.out.println(tableId.getTable() + " is not correctly formatted.");

                            // Skip table and try to continue verification even without this table
                            continue;
                        }

                        Schema schema = Schema.of(fieldList);
                        TableDefinition tableDefinition = StandardTableDefinition.of(schema);

                        tableInfos.add(TableInfo.newBuilder(tableId, tableDefinition).build());
                    }
                }
            }
        }
        return tableInfos;
    }

    /**
     * Read DDL schema to identify tables being created
     * @param queryVerificationSchema Schema to read from
     * @return List of new table ids
     */
    public static List<TableId> getTableIdsFromDdlSchema(QueryVerificationSchema queryVerificationSchema) {
        List<TableId> tableIds = new ArrayList<TableId>();

        // Separate DDL schema into statements
        // TODO Account for edge case where semicolon could be inside statement
        String[] statements = queryVerificationSchema.schema().split(";");

        for (String statement : statements) {
            statement = statement.trim().replaceAll("\\s+", " ");

            // Basic validation for DDL
            if (statement.toUpperCase().startsWith("CREATE TABLE")) {
                String[] schema = statement.split(" ");
                if (schema.length >= 3) {
                    String[] tableName = schema[2].split("\\.");

                    // Table name may appear as project.dataset.table or dataset.table
                    if (tableName.length == 2 || tableName.length == 3) {
                        TableId tableId = TableId.of(tableName[tableName.length - 2], tableName[tableName.length - 1]);
                        tableIds.add(tableId);
                    }
                }
            }
        }
        return tableIds;
    }

    /**
     * Create configuration for query jobs
     * @param query to run
     * @param dryRun indicating if the query should be run
     * @return Generated query job info
     */
    public static JobInfo configureJob(String query, boolean dryRun) {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
                .setDryRun(dryRun)
                .build();
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        JobInfo jobInfo = JobInfo.newBuilder(queryConfig)
                .setJobId(jobId)
                .build();
        return jobInfo;
    }

}