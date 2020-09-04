package com.google.cloud.bigquery.utils.queryfixer.cmd;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;

import java.util.List;
import java.util.Scanner;

/**
 * A class responsible for the interaction between users and the query fixer in User-assisted mode.
 * It continually fixes errors in a query until the query is correct or unable to be fixed. However,
 * If an error can be fixed in multiple ways, then the program will request users to choose one of
 * the fix options by entering their number.
 */
public class UserAssistedModeInteraction extends CommandLineInteraction {

  UserAssistedModeInteraction(String outputFormat, BigQueryOptions bigQueryOptions) {
    super(outputFormat, bigQueryOptions);
  }

  /**
   * Interact with users in User-assisted mode. If multiple options exist to fix an error, the query
   * fixer will ask users to type in a number to choose which options to fix the current error.
   *
   * @param query input query.
   */
  public void interact(String query) {
    while (true) {
      List<FixResult> fixResults = queryFixer.autoFixUntilUncertainOptions(query);

      System.out.println("Input query:\n" + query + "\n");
      printFixResultsInCommandLine(fixResults);

      FixResult latestResult = fixResults.get(fixResults.size() - 1);
      // If the status is not ERROR_FIXED, it should be either NO_ERROR or FAILURE. Thus, the
      // program will be terminated because the fix is completed.
      if (latestResult.getStatus() != FixResult.Status.ERROR_FIXED) {
        return;
      }
      int selectedIndex = readUserInputNumber(latestResult.getOptions().size()) - 1;
      query = latestResult.getOptions().get(selectedIndex).getFixedQuery();
    }
  }

  private static int readUserInputNumber(int maxNumber) {
    // A scanner can read users' input.
    Scanner scanner = new Scanner(System.in);
    while (true) {
      System.out.println("Enter the option you use to fix the query:");
      String userInput = scanner.nextLine(); // Read user input
      try {
        int index = Integer.parseInt(userInput);
        if (index >= 0 && index <= maxNumber) {
          return index;
        }
      } catch (NumberFormatException ignored) {
      }
      System.out.printf("Please enter a number between 1 and %d.\n", maxNumber);
    }
  }
}
