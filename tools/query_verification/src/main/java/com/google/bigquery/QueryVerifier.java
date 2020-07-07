package com.google.bigquery;

import javax.annotation.Nullable;

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

        // TODO Implement data free verification

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

}