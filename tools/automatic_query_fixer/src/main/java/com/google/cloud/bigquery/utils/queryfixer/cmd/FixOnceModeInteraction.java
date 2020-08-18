package com.google.cloud.bigquery.utils.queryfixer.cmd;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;

import static com.google.cloud.bigquery.utils.queryfixer.cmd.QueryFixerOptions.JSON_OUTPUT;
import static com.google.cloud.bigquery.utils.queryfixer.cmd.QueryFixerOptions.NATURAL_OUTPUT;

public class FixOnceModeInteraction extends CommandLineInteraction {
  FixOnceModeInteraction(String outputFormat, BigQueryOptions bigQueryOptions) {
    super(outputFormat, bigQueryOptions);
  }

  public void interact(String query) {
    FixResult fixResult = queryFixer.fix(query);
    printFixResultInFoMode(fixResult);
  }

  private void printFixResultInFoMode(FixResult fixResult) {
    switch (outputFormat) {
      case NATURAL_OUTPUT:
        System.out.println("Input query: " + fixResult.getQuery());
        printFixResultInCommandLine(fixResult);
        break;
      case JSON_OUTPUT:
        printAsJson(fixResult);
        break;
    }
  }
}
