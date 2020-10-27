package com.google.cloud.bigquery.utils.queryfixer.cmd;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;

import static com.google.cloud.bigquery.utils.queryfixer.cmd.QueryFixerOptions.JSON_OUTPUT;
import static com.google.cloud.bigquery.utils.queryfixer.cmd.QueryFixerOptions.NATURAL_OUTPUT;

/**
 * A class responsible for the interaction between users and the query fixer in Suggestion mode.
 * This mode only fixes one error of an input query if an error exists. Then outputs the Fix Result
 * to users. This mode is useful if a user would like to review individual fixes before the query
 * fixer corrects the next error based on the previous result.
 *
 * <p>This mode can also be integrated with frontend to provide fix suggestion in a UI.
 */
public class SuggestionModeInteraction extends CommandLineInteraction {
  SuggestionModeInteraction(String outputFormat, BigQueryOptions bigQueryOptions) {
    super(outputFormat, bigQueryOptions);
  }

  /**
   * Interact with users in Suggestion mode.
   *
   * @param query input query.
   */
  public void interact(String query) {
    FixResult fixResult = queryFixer.fix(query);
    printFixResultInFoMode(fixResult);
  }

  private void printFixResultInFoMode(FixResult fixResult) {
    switch (outputFormat) {
      case NATURAL_OUTPUT:
        System.out.println("Input query:\n" + fixResult.getQuery() + "\n");
        printFixResultInCommandLine(fixResult);
        break;
      case JSON_OUTPUT:
        printAsJson(fixResult);
        break;
    }
  }
}
