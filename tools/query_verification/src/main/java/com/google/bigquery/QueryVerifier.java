package com.google.bigquery;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Contains the logic to run query verification.
 */
public class QueryVerifier {

    private final BigQueryManager migratedInstance;
    private final DataWarehouseManager originalInstance;

    public QueryVerifier(QueryVerificationQuery migratedQuery, @Nullable QueryVerificationSchema migratedSchema, @Nullable QueryVerificationQuery originalQuery, @Nullable QueryVerificationSchema originalSchema, @Nullable List<QueryVerificationData> data) {
        migratedInstance = new BigQueryManager(migratedQuery, migratedSchema, data);
        originalInstance = null;
    }

    /**
     * Determines which verification method to use based on provided inputs and runs the verification.
     */
    public void verify() {
        try {
            if (originalInstance == null) {
                verifyDataFree();
            } else {
                verifyDataAware();
            }
        } catch (IllegalArgumentException | InterruptedException e) {
            System.err.println(e.getMessage());
        }
    }

    /**
     * Verifies migrated query by checking for syntax and semantic errors.
     */
    public void verifyDataFree() throws InterruptedException {
        List<QueryJobResults> results = migratedInstance.dryRunQueries();

        int syntaxErrors = 0;
        int semanticErrors = 0;

        // Look for errors that occurred for each query
        for (QueryJobResults result : results) {
            if (result.error() != null) {
                String query = result.query().query();

                // Locate line number of query in file
                int index = query.indexOf(result.statement());
                query = query.substring(0, index);
                int lineNumber = query.length() - query.replace("\n", "").length() + 1;

                System.err.printf("Error in query (line %d) from %s\n%s\n\n", lineNumber, result.query().path(), result.error());

                // Classify error as syntax or semantic
                if (result.error().startsWith("Syntax error")) {
                    syntaxErrors++;
                } else {
                    semanticErrors++;
                }
            }
        }

        int successfulResults = results.size() - syntaxErrors - semanticErrors;

        // Summary
        System.out.println();
        System.out.printf("Queries Run: %d, No Errors: %d (%.2f%%), Syntax Errors: %d, Semantic Errors: %d\n", results.size(), successfulResults, successfulResults * 100.0f / results.size(), syntaxErrors, semanticErrors);
        System.out.println("Data-Free Verification Completed");
    }

    /**
     * Verifies migrated query by sending query jobs to BQ and TD to check for differences in the query results.
     */
    public void verifyDataAware() throws InterruptedException {
        List<QueryJobResults> migratedResults = migratedInstance.runQueries();

        int migratedSyntaxErrors = 0;
        int migratedSemanticErrors = 0;

        // Look for errors that occurred for each query
        for (QueryJobResults migratedResult : migratedResults) {
            if (migratedResult.error() != null) {
                String query = migratedResult.query().query();

                // Locate line number of query in file
                int index = query.indexOf(migratedResult.statement());
                query = query.substring(0, index);
                int lineNumber = query.length() - query.replace("\n", "").length() + 1;

                System.err.printf("Error in query (line %d) from %s\n%s\n\n", lineNumber, migratedResult.query().path(), migratedResult.error());

                // Classify error as syntax or semantic
                if (migratedResult.error().startsWith("Syntax error")) {
                    migratedSyntaxErrors++;
                } else {
                    migratedSemanticErrors++;
                }
            }
        }

        int successfulMigratedResults = migratedResults.size() - migratedSyntaxErrors - migratedSemanticErrors;

        // TODO Run queries in TD

        // TODO Compare results

        System.out.println();
        System.out.printf("%s Summary\nQueries Run: %d, No Errors: %d (%.2f%%), Syntax Errors: %d, Semantic Errors: %d\n", migratedInstance.getName(), migratedResults.size(), successfulMigratedResults, successfulMigratedResults * 100.0f / migratedResults.size(), migratedSyntaxErrors, migratedSemanticErrors);
        // TODO Print TD statistics
        // TODO Print comparison statistics
        System.out.println("Data-Aware Verification Completed");
    }


}