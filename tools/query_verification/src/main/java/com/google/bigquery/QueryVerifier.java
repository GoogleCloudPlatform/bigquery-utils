package com.google.bigquery;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        } catch (Exception e) {
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
    public void verifyDataAware() throws Exception {
        List<QueryJobResults> migratedResults = migratedInstance.runQueries();
        List<QueryJobResults> originalResults = originalInstance.runQueries();

        ResultDifferences resultDifferences = QueryVerifier.compareResults(migratedResults, originalResults);

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

    /**
     * Finds extra and missing results by locating the differences between the results.
     * @param migratedResults Parsed results returned from BQ
     * @param originalResults Parsed results returned from original data warehouse service
     * @return Differences classified as either extra or missing from migrated results.
     */
    public static ResultDifferences compareResults(List<QueryJobResults> migratedResults, List<QueryJobResults> originalResults) throws IllegalArgumentException {
        // Check if same amount of queries were run
        if (migratedResults.size() == originalResults.size()) {
            // Rows present in migrated query results, but not original query results
            List<List<Object>> extraResults = new ArrayList<List<Object>>();

            // Rows present in original query results, but not migrated query results
            List<List<Object>> missingResults = new ArrayList<List<Object>>();

            for (int i = 0; i < migratedResults.size(); i++) {
                Set<List<Object>> migratedJobResults = migratedResults.get(i).results();
                Set<List<Object>> originalJobResults = originalResults.get(i).results();

                Set<List<Object>> missingResultsSet = new HashSet<List<Object>>(originalJobResults);

                for (List<Object> migratedQueryResults : migratedJobResults) {
                    // Rows that exist in both results are removed from missing results set
                    if (!missingResultsSet.remove(migratedQueryResults)) {
                        // Rows in the migrated results that don't exist in original results are classified as extra in migrated results
                        extraResults.add(migratedQueryResults);
                    }
                }

                // Any leftover rows in the original results without a match are classified as missing in migrated results
                missingResults.addAll(missingResultsSet);
            }

            return ResultDifferences.create(extraResults, missingResults);
        } else {
            throw new IllegalArgumentException("Number of statements in migrated query file should be equal to the number of statements in the original query file.");
        }
    }

}