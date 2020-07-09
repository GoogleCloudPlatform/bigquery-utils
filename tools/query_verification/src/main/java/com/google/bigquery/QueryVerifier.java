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
            try {
                if (migratedSchema.isInJsonFormat()) {
                    TableInfo tableInfo = QueryVerifier.getTableInfoFromJsonSchema(migratedSchema);
                    Table table = bigQuery.create(tableInfo);
                    tables.add(table);
                } else {
                    // TODO Load schema from DDL
                }
            } catch (NullPointerException e) {
                System.out.println(migratedSchema.path() + " is not correctly formatted.");
            }
        }

        // Create dry-run job
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(migratedQuery.query())
                .setDryRun(true)
                .build();
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        JobInfo jobInfo = JobInfo.newBuilder(queryConfig)
                .setJobId(jobId)
                .build();

        // TODO Support multiple queries

        try {
            // Run dry-run
            Job queryJob = bigQuery.create(jobInfo);
            verificationResult = queryJob.getStatistics() != null;
        } catch (BigQueryException e) {
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
     * @return New table info
     */
    @Nullable
    public static TableInfo getTableInfoFromJsonSchema(QueryVerificationSchema queryVerificationSchema) {
        if (queryVerificationSchema.getJsonArray().size() == 0) {
            return null;
        }

        // TODO Support multiple table schema
        JsonObject schemaObject = queryVerificationSchema.getJsonArray().get(0).getAsJsonObject();

        JsonArray schemaFields = schemaObject.getAsJsonArray("fields");
        JsonObject tableReference = schemaObject.get("tableReference").getAsJsonObject();

        // Deserialize fields
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(FieldList.class, new BigQuerySchemaJsonDeserializer())
                .create();
        FieldList fieldList = gson.fromJson(schemaFields, FieldList.class);
        Schema schema = Schema.of(fieldList);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);

        TableId tableId = TableId.of(tableReference.get("datasetId").getAsString(), tableReference.get("tableId").getAsString());
        return TableInfo.newBuilder(tableId, tableDefinition).build();
    }

}