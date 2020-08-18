package com.google.cloud.bigquery.utils.queryfixer.cmd;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;

import java.util.List;

import static com.google.cloud.bigquery.utils.queryfixer.cmd.QueryFixerOptions.JSON_OUTPUT;
import static com.google.cloud.bigquery.utils.queryfixer.cmd.QueryFixerOptions.NATURAL_OUTPUT;

public class AutoModeInteraction extends CommandLineInteraction {

  AutoModeInteraction(String outputFormat, BigQueryOptions bigQueryOptions) {
    super(outputFormat, bigQueryOptions);
  }

  public void interact(String query) {
    List<FixResult> fixResults = queryFixer.autoFix(query);
    printFixResults(query, fixResults);

    // Print the first 10 rows of the query's result if the query is correct.
    // This preview only runs when the output format is natural language.
    if (outputFormat.equals(NATURAL_OUTPUT)) {
      FixResult latestResult = fixResults.get(fixResults.size() - 1);
      if (latestResult.getStatus() == FixResult.Status.NO_ERROR) {
        printQueryResult(latestResult.getQuery());
      }
    }
  }

  private void printQueryResult(String query) {
    BigQuery bigQuery = bigQueryOptions.getService();

    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
    try {
      int maxCount = 10;
      int count = 0;
      System.out.println("=".repeat(20));
      System.out.println("Now query: " + query);
      System.out.printf("Only %d rows are printed\n", maxCount);
      TableResult result = bigQuery.query(queryConfig);

      for (Field field : result.getSchema().getFields()) {
        System.out.printf("%s,", field.getName());
      }
      System.out.print("\n");

      for (FieldValueList row : bigQuery.query(queryConfig).iterateAll()) {
        for (FieldValue val : row) {
          System.out.printf("%s,", val.getValue());
        }
        System.out.print("\n");
        if (++count >= maxCount) {
          break;
        }
      }
    } catch (InterruptedException ignored) {
    }
  }

  private void printFixResults(String query, List<FixResult> fixResults) {
    switch (outputFormat) {
      case NATURAL_OUTPUT:
        System.out.println("Input query: " + query);
        printFixResultsInCommandLine(fixResults);
        break;
      case JSON_OUTPUT:
        printAsJson(fixResults);
        break;
      default:
        System.out.println("Output format (-o) is incorrect. Use --help for usage.");
        System.exit(1);
    }
  }
}
