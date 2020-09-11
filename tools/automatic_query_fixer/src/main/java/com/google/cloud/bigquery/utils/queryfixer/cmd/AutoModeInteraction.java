package com.google.cloud.bigquery.utils.queryfixer.cmd;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.common.flogger.FluentLogger;

import java.util.List;

import static com.google.cloud.bigquery.utils.queryfixer.cmd.QueryFixerOptions.JSON_OUTPUT;
import static com.google.cloud.bigquery.utils.queryfixer.cmd.QueryFixerOptions.NATURAL_OUTPUT;

/**
 * A class responsible for the interaction between users and the query fixer in Auto mode. It
 * continually tries to fix every error in a query until the query is correct or unable to be fixed.
 * If an error can be fixed in multiple ways, then the first method will be selected. In this mode,
 * the query fixer takes the input query and outputs the final fix results without interacting with
 * users during the fix process.
 */
public class AutoModeInteraction extends CommandLineInteraction {

  private static final int PREVIEW_SIZE = 10;
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  AutoModeInteraction(String outputFormat, BigQueryOptions bigQueryOptions) {
    super(outputFormat, bigQueryOptions);
  }

  /**
   * Interact with users in Auto mode.
   *
   * @param query input query.
   */
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
      int maxCount = PREVIEW_SIZE;
      int count = 0;
      System.out.println("=".repeat(20));
      System.out.println("Now query: " + query);
      System.out.printf("Only %d rows are printed\n", maxCount);
      TableResult result = bigQuery.query(queryConfig);

      for (Field field : result.getSchema().getFields()) {
        System.out.printf("%s,", field.getName());
      }
      System.out.println();

      for (FieldValueList row : bigQuery.query(queryConfig).iterateAll()) {
        for (FieldValue val : row) {
          System.out.printf("%s,", val.getValue());
        }
        System.out.println();
        if (++count >= maxCount) {
          break;
        }
      }
    } catch (InterruptedException exception) {
      logger.atWarning().withCause(exception).log("Unable to preview the fixed query.");
    }
  }

  private void printFixResults(String query, List<FixResult> fixResults) {
    switch (outputFormat) {
      case NATURAL_OUTPUT:
        System.out.println("Input query:\n" + query + "\n");
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
