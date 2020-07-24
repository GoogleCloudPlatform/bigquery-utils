package com.google.bigquery;

import com.google.cloud.bigquery.*;
import com.google.gson.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Contains the logic to run query verification.
 */
public class QueryVerifier {

    private final QueryVerificationQuery migratedQuery;
    private final QueryVerificationSchema migratedSchema;

    private final QueryVerificationQuery originalQuery;
    private final QueryVerificationSchema originalSchema;

    private final List<QueryVerificationData> data;

    private final BigQuery bigQuery;

    public QueryVerifier(QueryVerificationQuery migratedQuery, @Nullable QueryVerificationSchema migratedSchema, @Nullable QueryVerificationQuery originalQuery, @Nullable QueryVerificationSchema originalSchema, @Nullable List<QueryVerificationData> data) {
        this.migratedQuery = migratedQuery;
        this.migratedSchema = migratedSchema;

        this.originalQuery = originalQuery;
        this.originalSchema = originalSchema;

        this.data = data;

        bigQuery = BigQueryOptions.getDefaultInstance().getService();
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
        List<Table> tables = getBigQueryTablesFromSchema();

        // Create dry-run jobs
        List<JobInfo> jobInfos = getJobInfosFromQuery(migratedQuery, true);

        // Store results for every successful dry-run
        List<QueryJobResults<JobStatistics>> jobResults = new ArrayList<QueryJobResults<JobStatistics>>();

        for (int i = 0; i < jobInfos.size(); i++) {
            JobInfo jobInfo = jobInfos.get(i);

            // Retrieve query
            QueryJobConfiguration queryJobConfiguration = jobInfo.getConfiguration();
            String query = queryJobConfiguration.getQuery();

            try {
                // Run dry-run
                Job queryJob = bigQuery.create(jobInfo);

                // Store results from dry-run
                JobStatistics results = queryJob.getStatistics();
                jobResults.add(QueryJobResults.create(query, results));
            } catch (BigQueryException e) {
                // Print out syntax/semantic errors returned from BQ
                System.err.printf("Error in Query #%d from %s\n%s\n\n", i + 1, migratedQuery.path(), e.getMessage());
            }
        }

        // Clear tables created
        tables.forEach(table -> BigQueryOptions.getDefaultInstance().getService().delete(table.getTableId()));

        System.out.println();
        System.out.printf("%d/%d (%.2f%%) Queries Verified\n", jobResults.size(), jobInfos.size(), jobResults.size() * 100.0f / jobInfos.size());
        System.out.printf("Data-Free Verification %s\n", jobResults.size() == jobInfos.size() ? "Succeeded" : "Failed");
    }

    /**
     * Verifies migrated query by sending query jobs to BQ and TD to check for differences in the query results.
     */
    public void verifyDataAware() {
        List<Table> tables = getBigQueryTablesFromSchema();
        populateBigQueryTablesFromData();

        // Create query jobs
        List<JobInfo> jobInfos = getJobInfosFromQuery(migratedQuery, false);

        // Store results from every job
        List<QueryJobResults<TableResult>> bigQueryJobResults = new ArrayList<QueryJobResults<TableResult>>();

        for (int i = 0; i < jobInfos.size(); i++) {
            JobInfo jobInfo = jobInfos.get(i);

            // Retrieve query
            QueryJobConfiguration queryJobConfiguration = jobInfo.getConfiguration();
            String query = queryJobConfiguration.getQuery();

            QueryJobResults results = null;
            try {
                // Run query job
                Job queryJob = bigQuery.create(jobInfo);
                queryJob.waitFor();

                results = QueryJobResults.create(query, queryJob.getQueryResults());
            } catch (BigQueryException e) {
                // Print out syntax/semantic errors returned from BQ
                System.err.printf("Error in Query #%d from %s\n%s\n\n", i + 1, migratedQuery.path(), e.getMessage());
            } catch (InterruptedException e) {
                System.err.println(e.getMessage());
            } finally {
                // Store results
                bigQueryJobResults.add(results);
            }
        }

        // Clear tables created
        tables.forEach(table -> BigQueryOptions.getDefaultInstance().getService().delete(table.getTableId()));

        // TODO Run queries in TD

        // TODO Compare results

        bigQueryJobResults.removeIf(Objects::isNull);

        System.out.println();
        System.out.printf("%d/%d (%.2f%%) Queries Verified\n", bigQueryJobResults.size(), jobInfos.size(), bigQueryJobResults.size() * 100.0f / jobInfos.size());
        System.out.printf("Data-Aware Verification %s\n", bigQueryJobResults.size() == jobInfos.size() ? "Succeeded" : "Failed");
    }

    /**
     * Creates BQ tables based on the provided schema
     * @return List of newly created tables
     */
    public List<Table> getBigQueryTablesFromSchema() {
        List<Table> tables = new ArrayList<Table>();

        if (migratedSchema != null) {
            if (migratedSchema.isInJsonFormat()) {
                // Schema is JSON
                List<TableInfo> tableInfos = QueryVerifier.getTableInfoFromJsonSchema(migratedSchema);
                if (tableInfos != null) {
                    tableInfos.forEach(tableInfo -> tables.add(bigQuery.create(tableInfo)));
                }
            } else {
                // Schema is DDL
                JobInfo jobInfo = configureJob(migratedSchema.schema(), false);
                Job schemaJob = bigQuery.create(jobInfo);
                try {
                    schemaJob.waitFor();
                } catch (InterruptedException e) {
                    System.err.println(e.getMessage());
                }

                List<TableId> tableIds = QueryVerifier.getTableIdsFromDdlSchema(migratedSchema);
                tableIds.forEach(tableId -> tables.add(bigQuery.getTable(tableId)));
            }

            if (tables.isEmpty()) {
                System.err.println(migratedSchema.path() + " is not correctly formatted.");
            }
        }

        return tables;
    }

    /**
     * Populates BQ tables based on the provided table data
     */
    public void populateBigQueryTablesFromData() {
        for (QueryVerificationData queryVerificationData : data) {
            Table table = bigQuery.getTable(queryVerificationData.datasetName(), queryVerificationData.tableName());

            // Check if no schema was provided for this table
            if (table == null) {
                System.err.println(queryVerificationData.tableName() + " has no provided schema.");

                // Try to continue verification
                continue;
            }

            TableId tableId = table.getTableId();

            // Copy contents of CSV file
            WriteChannelConfiguration writeChannelConfiguration = WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(FormatOptions.csv()).build();
            TableDataWriteChannel writer = bigQuery.writer(writeChannelConfiguration);
            try {
                writer.write(ByteBuffer.wrap(queryVerificationData.contents().getBytes()));
                writer.close();
            } catch (IOException e) {
                System.err.println("I/O Exception: " + e.getMessage());
            }

            // Run table data writing job
            Job writeJob = writer.getJob();
            try {
                writeJob = writeJob.waitFor();

                // Check for errors in writing table data
                if (writeJob.getStatus().getError() != null) {
                    BigQueryError error = writeJob.getStatus().getError();
                    System.err.printf("%s is not correctly formatted.\n%s\n", queryVerificationData.path(), error.getMessage());
                }
            } catch (InterruptedException e) {
                System.err.println(e.getMessage());
            }
        }
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
                            System.err.println(tableId.getTable() + " is not correctly formatted.");

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
     * Creates jobs for each query from a file
     * @param queryVerificationQuery
     * @param dryRun indicating if the query should be run
     * @return
     */
    public static List<JobInfo> getJobInfosFromQuery(QueryVerificationQuery queryVerificationQuery, boolean dryRun) {
        List<JobInfo> jobInfos = new ArrayList<JobInfo>();

        // Separate query into individual statements
        String[] statements = queryVerificationQuery.query().split(";");

        for (String statement : statements) {
            statement = statement.trim();

            JobInfo jobInfo = configureJob(statement, dryRun);
            jobInfos.add(jobInfo);
        }

        return jobInfos;
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