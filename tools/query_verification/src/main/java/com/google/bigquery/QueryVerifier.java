package com.google.bigquery;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Contains the logic to run query verification.
 */
public class QueryVerifier {

    private final BigQueryManager migratedInstance;
    private final DataWarehouseManager originalInstance;

    public static final int DECIMAL_PRECISION = 10;

    public QueryVerifier(QueryVerificationQuery migratedQuery, @Nullable QueryVerificationSchema migratedSchema, @Nullable QueryVerificationQuery originalQuery, @Nullable QueryVerificationSchema originalSchema, @Nullable List<QueryVerificationData> data) {
        migratedInstance = new BigQueryManager(migratedQuery, migratedSchema, data);
        if (originalQuery == null) {
            originalInstance = null;
        } else {
            originalInstance = new TeradataManager(originalQuery, originalSchema, data);
        }
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
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Verifies migrated query by checking for syntax and semantic errors.
     */
    public void verifyDataFree() throws InterruptedException {
        List<QueryJobResults> results = migratedInstance.dryRunQueries();

        QueryErrors errors = QueryVerifier.classifyErrors(results);

        // Summary
        System.out.println();
        System.out.printf("Queries Run: %d, No Errors: %d (%.2f%%), Syntax Errors: %d, Semantic Errors: %d\n\n", errors.totalQueries(), errors.noErrors(), errors.successRate(), errors.syntaxErrors(), errors.semanticErrors());
        System.out.println("Data-Free Verification Completed");
    }

    /**
     * Verifies migrated query by sending query jobs to BQ and TD to check for differences in the query results.
     */
    public void verifyDataAware() throws Exception {
        List<QueryJobResults> migratedResults = migratedInstance.runQueries();
        List<QueryJobResults> originalResults = originalInstance.runQueries();

        QueryVerifier.exportToCsv(migratedResults, migratedInstance);
        QueryVerifier.exportToCsv(originalResults, originalInstance);

        List<ResultDifferences> resultDifferences = QueryVerifier.compareResults(migratedResults, originalResults);
        QueryVerifier.exportToCsv(resultDifferences);

        QueryErrors migratedErrors = QueryVerifier.classifyErrors(migratedResults);
        QueryErrors originalErrors = QueryVerifier.classifyErrors(originalResults);

        // Summary
        System.out.printf("%s Summary\nQueries Run: %d, No Errors: %d (%.2f%%), Syntax Errors: %d, Semantic Errors: %d\n\n", migratedInstance.getName(), migratedErrors.totalQueries(), migratedErrors.noErrors(), migratedErrors.successRate(), migratedErrors.syntaxErrors(), migratedErrors.semanticErrors());
        System.out.printf("%s Summary\nQueries Run: %d, No Errors: %d (%.2f%%), Syntax Errors: %d, Semantic Errors: %d\n\n", originalInstance.getName(), originalErrors.totalQueries(), originalErrors.noErrors(), originalErrors.successRate(), originalErrors.syntaxErrors(), originalErrors.semanticErrors());
        System.out.println("Verification Summary");
        for (int i = 0; i < resultDifferences.size(); i++) {
            ResultDifferences differences = resultDifferences.get(i);
            if (!differences.missingResults().isEmpty() || !differences.extraResults().isEmpty()) {
                System.out.printf("Differences in query %d results: %d rows missing and %d extra rows in migrated query results.\n", i + 1, differences.missingResults().size(), differences.extraResults().size());
            }
        }
        System.out.println("Raw results and diffs have been exported to the \"query_verification_output\" folder.\n");
        System.out.println("Data-Aware Verification Completed");
    }

    /**
     * Finds extra and missing results by locating the differences between the results.
     * @param migratedResults Parsed results returned from BQ
     * @param originalResults Parsed results returned from original data warehouse service
     * @return Differences classified as either extra or missing from migrated results.
     */
    public static List<ResultDifferences> compareResults(List<QueryJobResults> migratedResults, List<QueryJobResults> originalResults) throws IllegalArgumentException {
        // Check if same amount of queries were run
        if (migratedResults.size() == originalResults.size()) {
            List<ResultDifferences> differences = new ArrayList<ResultDifferences>();

            for (int i = 0; i < migratedResults.size(); i++) {
                Set<List<Object>> migratedJobResults = migratedResults.get(i).results();
                Set<List<Object>> originalJobResults = originalResults.get(i).results();

                if (migratedJobResults == null) {
                    migratedJobResults = new HashSet<List<Object>>();
                }
                if (originalJobResults == null) {
                    originalJobResults = new HashSet<List<Object>>();
                }

                // Rows present in migrated query results, but not original query results
                List<List<String>> extraResults = new ArrayList<List<String>>();

                for (List<Object> migratedQueryResults : migratedJobResults) {
                    // Rows that exist in both results are removed from missing results set
                    if (!originalJobResults.remove(migratedQueryResults)) {
                        // Rows in the migrated results that don't exist in original results are classified as extra in migrated results
                        extraResults.add(toStringTypes(migratedQueryResults));
                    }
                }

                // Any leftover rows in the original results without a match are classified as missing in migrated results
                List<List<String>> missingResults = originalJobResults.stream().map(QueryVerifier::toStringTypes).collect(Collectors.toList());

                differences.add(ResultDifferences.create(extraResults, missingResults));
            }

            return differences;
        } else {
            throw new IllegalArgumentException("Number of statements in migrated query file should be equal to the number of statements in the original query file.");
        }
    }

    /**
     * Converts the object types to strings.
     * @param row List of objects
     * @return List of strings
     */
    private static List<String> toStringTypes(List<Object> row) {
        return row.stream().map(object -> {
            if (object instanceof Date) {
                // Format date objects
                return new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSS zzz").format(object);
            } else {
                // All other types can use toString()
                return object.toString();
            }
        }).collect(Collectors.toList());
    }

    /**
     * Counts and classifies the syntax/semantic errors returned from running queries.
     * @param results List of results
     * @return Number of queries that had syntax errors, semantic errors, and no errors.
     */
    public static QueryErrors classifyErrors(List<QueryJobResults> results) {
        int noErrors = 0;
        int syntaxErrors = 0;
        int semanticErrors = 0;

        // Look for errors that occurred for each query
        for (QueryJobResults result : results) {
            if (result.error() == null) {
                noErrors++;
            } else {
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

        return QueryErrors.create(results.size(), noErrors, syntaxErrors, semanticErrors);
    }

    /**
     * Exports query results to CSV files.
     * @param results List of results
     * @param instance Data warehouse service used
     * @throws IOException
     */
    public static void exportToCsv(List<QueryJobResults> results, DataWarehouseManager instance) throws IOException {
        exportToCsv(results.stream().map(QueryJobResults::rawResults).collect(Collectors.toList()), instance.getName(), "");
    }

    /**
     * Exports diff results to CSV files.
     * @param differences between original and migrated results
     * @throws IOException
     */
    public static void exportToCsv(List<ResultDifferences> differences) throws IOException {
        exportToCsv(differences.stream().map(ResultDifferences::missingResults).collect(Collectors.toList()), "diff", "missing");
        exportToCsv(differences.stream().map(ResultDifferences::extraResults).collect(Collectors.toList()), "diff", "extra");
    }

    private static void exportToCsv(List<List<List<String>>> rawResults, String folderName, String fileNameSuffix) throws IOException {
        Path csvOutputDirectory = Paths.get("query_verification_output", folderName);

        if (csvOutputDirectory.toFile().exists()) {
            // Clear out old output files in the same path
            Files.walk(csvOutputDirectory).forEach(subPath -> {
                // Delete files previously generated by query verification
                if (subPath.getFileName().toString().matches("query\\d+" + fileNameSuffix + "\\.csv")) {
                    subPath.toFile().delete();
                }
            });
            if (csvOutputDirectory.toFile().length() == 0 ) {
                Files.deleteIfExists(csvOutputDirectory);
            }
        }
        Files.createDirectories(csvOutputDirectory);

        for (int i = 0; i < rawResults.size(); i++) {
            List<List<String>> queryResults = rawResults.get(i);

            // Skip if results are empty
            if (queryResults == null || queryResults.isEmpty()) {
                continue;
            }

            // Filepath for each query is "output/[data_warehouse_name]/query[id].csv"
            File csvOutputFile = new File(String.format("%s/query%d%s.csv", csvOutputDirectory, i + 1, fileNameSuffix));
            csvOutputFile.createNewFile();
            FileWriter writer = new FileWriter(csvOutputFile);

            // Convert list to csv row and write in file
            queryResults.stream().map(row -> String.join(",", row)).forEach(row -> {
                try {
                    writer.write(row + System.lineSeparator());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            writer.close();
        }
    }

}