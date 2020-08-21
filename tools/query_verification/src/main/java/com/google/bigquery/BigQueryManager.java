package com.google.bigquery;

import com.google.cloud.bigquery.*;
import com.google.gson.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Class to communicate with BQ to create tables and run queries
 */
public class BigQueryManager implements DataWarehouseManager {

    private final BigQuery bigQuery;

    private final QueryVerificationQuery query;
    private final QueryVerificationSchema schema;
    private final List<QueryVerificationData> data;

    public BigQueryManager(QueryVerificationQuery query, QueryVerificationSchema schema, List<QueryVerificationData> data) {
        bigQuery = BigQueryOptions.getDefaultInstance().getService();

        this.query = query;
        this.schema = schema;
        this.data = data;
    }

    public BigQueryManager(QueryVerificationQuery query, QueryVerificationSchema schema, List<QueryVerificationData> data, BigQuery bigQuery) {
        this.bigQuery = bigQuery;

        this.query = query;
        this.schema = schema;
        this.data = data;
    }

    @Override
    public String getName() {
        return "BigQuery";
    }

    /**
     * Sends query jobs to BQ
     * @return List of query results
     * @throws InterruptedException
     */
    @Override
    public List<QueryJobResults> runQueries() throws IllegalArgumentException, InterruptedException {
        List<Table> tables = getBigQueryTablesFromSchema();
        populateBigQueryTablesFromData();

        // Create query jobs
        List<JobInfo> jobInfos = getJobInfosFromQuery(false);

        // Store results from every job
        List<QueryJobResults> jobResults = new ArrayList<QueryJobResults>();

        for (int i = 0; i < jobInfos.size(); i++) {
            JobInfo jobInfo = jobInfos.get(i);

            // Retrieve query
            QueryJobConfiguration queryJobConfiguration = jobInfo.getConfiguration();
            String statement = queryJobConfiguration.getQuery();

            QueryJobResults jobResult;
            try {
                // Run query job
                Job queryJob = bigQuery.create(jobInfo);
                queryJob.waitFor();

                // Parse and store query results
                List<List<String>> rawResults = new ArrayList<List<String>>();
                Set<List<Object>> results = new HashSet<List<Object>>();
                TableResult queryResults = queryJob.getQueryResults();
                FieldList fields = queryResults.getSchema().getFields();

                queryResults.iterateAll().forEach(values -> {
                    rawResults.add(values.stream().map(value -> value.getStringValue()).collect(Collectors.toList()));
                    results.add(parseResults(values, fields));
                });

                jobResult = QueryJobResults.create(statement, query, null, results, rawResults);
            } catch (BigQueryException e) {
                // Print out syntax/semantic errors returned from BQ
                jobResult = QueryJobResults.create(statement, query, e.getMessage(), null, null);
            }

            // Store results
            jobResults.add(jobResult);
        }

        // Clear tables created
        tables.forEach(table -> bigQuery.delete(table.getTableId()));

        return jobResults;
    }

    /**
     * Sends dry-run query jobs to BQ to check for syntax and semantic errors
     * @return Results from dry-runs
     * @throws InterruptedException if any job get interrupted before returning results
     */
    public List<QueryJobResults> dryRunQueries() throws IllegalArgumentException, InterruptedException {
        List<Table> tables = getBigQueryTablesFromSchema();

        // Create dry-run jobs
        List<JobInfo> jobInfos = getJobInfosFromQuery(true);

        // Store results for every successful dry-run
        List<QueryJobResults> jobResults = new ArrayList<QueryJobResults>();

        for (int i = 0; i < jobInfos.size(); i++) {
            JobInfo jobInfo = jobInfos.get(i);

            // Retrieve query
            QueryJobConfiguration queryJobConfiguration = jobInfo.getConfiguration();
            String statement = queryJobConfiguration.getQuery();

            QueryJobResults jobResult;
            try {
                // Run dry-run
                bigQuery.create(jobInfo);

                // Store results from dry-run
                jobResult = QueryJobResults.create(statement, query, null, null, null);
            } catch (BigQueryException e) {
                // Print out syntax/semantic errors returned from BQ
                jobResult = QueryJobResults.create(statement, query, e.getMessage(), null, null);
            }

            jobResults.add(jobResult);
        }

        // Clear tables created
        tables.forEach(table -> bigQuery.delete(table.getTableId()));

        return jobResults;
    }

    /**
     * Creates jobs for each query from a file
     * @param dryRun indicating if the query should be run
     * @return
     */
    public List<JobInfo> getJobInfosFromQuery(boolean dryRun) {
        List<JobInfo> jobInfos = new ArrayList<JobInfo>();

        // Separate query into individual statements
        // TODO Account for edge case where semicolon could be inside statement
        List<String> statements = Arrays.stream(query.query().split(";")).map(String::trim).filter(statement -> !statement.isEmpty()).collect(Collectors.toList());

        for (String statement : statements) {
            JobInfo jobInfo = configureJob(statement, dryRun);
            jobInfos.add(jobInfo);
        }

        return jobInfos;
    }

    /**
     * Creates BQ tables based on the provided schema
     * @return List of newly created tables
     */
    public List<Table> getBigQueryTablesFromSchema() throws IllegalArgumentException, InterruptedException {
        List<Table> tables = new ArrayList<Table>();

        if (schema != null) {
            if (schema.isInJsonFormat()) {
                // Schema is JSON
                List<TableInfo> tableInfos = getTableInfoFromJsonSchema();
                if (tableInfos != null) {
                    tableInfos.forEach(tableInfo -> tables.add(bigQuery.create(tableInfo)));
                }
            } else {
                // Schema is DDL
                JobInfo jobInfo = configureJob(schema.schema(), false);
                Job schemaJob = bigQuery.create(jobInfo);
                try {
                    schemaJob.waitFor();
                } catch (BigQueryException e) {
                    throw new IllegalArgumentException(schema.path() + " is not correctly formatted. " + e.getMessage());
                }

                List<TableId> tableIds = getTableIdsFromDdlSchema();
                tableIds.forEach(tableId -> tables.add(bigQuery.getTable(tableId)));
            }

            if (tables.isEmpty()) {
                throw new IllegalArgumentException(schema.path() + " is not correctly formatted.");
            }
        }

        return tables;
    }

    /**
     * Reads JSON schema to create table fields based on the schema
     * @return List of new table info
     */
    public List<TableInfo> getTableInfoFromJsonSchema() {
        List<TableInfo> tableInfos = new ArrayList<TableInfo>();
        for (JsonElement schemaElement : schema.getJsonArray()) {
            if (schema.getJsonArray().size() == 0) {
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
                            throw new IllegalArgumentException(tableId.getTable() + " is not correctly formatted.");
                        }

                        Schema tableSchema = Schema.of(fieldList);
                        TableDefinition tableDefinition = StandardTableDefinition.of(tableSchema);

                        tableInfos.add(TableInfo.newBuilder(tableId, tableDefinition).build());
                    }
                }
            }
        }
        return tableInfos;
    }

    /**
     * Read DDL schema to identify tables being created
     * @return List of new table ids
     */
    public List<TableId> getTableIdsFromDdlSchema() {
        List<TableId> tableIds = new ArrayList<TableId>();

        // Separate DDL schema into statements
        // TODO Account for edge case where semicolon could be inside statement
        String[] statements = schema.schema().split(";");

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
     * Converts each value of query results to Java objects based on the field's type
     * @param values from query results
     * @param fields from the schema of the results
     * @return List of objects parsed from query results
     */
    public List<Object> parseResults(FieldValueList values, FieldList fields) {
        List<Object> results = new ArrayList<Object>();
        for (int i = 0; i < values.size(); i++) {
            FieldValue value = values.get(i);

            // Identify type of the value
            StandardSQLTypeName type = fields.get(i).getType().getStandardType();

            Object result;
            try {
                switch (type) {
                    case BOOL:
                        result = value.getBooleanValue();
                        break;
                    case FLOAT64:
                        result = BigDecimal.valueOf(value.getDoubleValue()).setScale(QueryVerifier.DECIMAL_PRECISION, RoundingMode.FLOOR);
                        break;
                    case INT64:
                        result = value.getLongValue();
                        break;
                    case NUMERIC:
                        result = value.getNumericValue().setScale(QueryVerifier.DECIMAL_PRECISION, RoundingMode.FLOOR);
                        break;
                    case STRUCT:
                        FieldList subFields = fields.get(i).getSubFields();
                        FieldValueList subValues = value.getRecordValue();
                        result = parseResults(subValues, subFields);
                        break;
                    case DATE:
                        result = new SimpleDateFormat("yyyy-MM-dd").parse(value.getStringValue());
                        break;
                    case DATETIME:
                        result = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSSSS").parse(value.getStringValue());
                        break;
                    case TIME:
                        result = new SimpleDateFormat("hh:mm:ss.SSSSSS").parse(value.getStringValue());
                        break;
                    case TIMESTAMP:
                        result = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSS zzz").parse(value.getStringValue());
                        break;
                    case STRING:
                        result = value.getStringValue();
                        break;
                    default:
                        // Handle unknown/unsupported types as String
                        System.err.println("Warning: Unsupported type: " + type.name());
                        result = value.getStringValue();
                }
            } catch (ParseException e) {
                result = Optional.empty();
            }

            results.add(result);
        }

        return results;
    }

    /**
     * Create configuration for query jobs
     * @param statement of query to run
     * @param dryRun indicating if the query should be run
     * @return Generated query job info
     */
    private JobInfo configureJob(String statement, boolean dryRun) {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(statement)
                .setDryRun(dryRun)
                .build();
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        JobInfo jobInfo = JobInfo.newBuilder(queryConfig)
                .setJobId(jobId)
                .build();
        return jobInfo;
    }

    /**
     * Populates BQ tables based on the provided table data
     */
    private void populateBigQueryTablesFromData() throws IllegalArgumentException, InterruptedException {
        for (QueryVerificationData queryVerificationData : data) {
            Table table = bigQuery.getTable(queryVerificationData.datasetName(), queryVerificationData.tableName());

            // Check if no schema was provided for this table
            if (table == null) {
                throw new IllegalArgumentException(queryVerificationData.tableName() + " has no provided schema.");
            }

            TableId tableId = table.getTableId();

            // Copy contents of CSV file
            WriteChannelConfiguration writeChannelConfiguration = WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(FormatOptions.csv()).build();
            TableDataWriteChannel writer = bigQuery.writer(writeChannelConfiguration);
            try {
                writer.write(ByteBuffer.wrap(queryVerificationData.contents().getBytes()));
                writer.close();
            } catch (IOException e) {
                // Try to continue verification
                continue;
            }

            // Run table data writing job
            Job writeJob = writer.getJob();
            writeJob = writeJob.waitFor();

            // Check for errors in writing table data
            if (writeJob.getStatus().getError() != null) {
                BigQueryError error = writeJob.getStatus().getError();
                throw new IllegalArgumentException(String.format("%s is not correctly formatted.\n%s\n", queryVerificationData.path(), error.getMessage()));
            }
        }
    }
}
