package com.google.cloud.bigquery.utils.queryfixer.cmd;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;

import java.util.List;
import java.util.Scanner;

public class UserAssistModeInteraction extends CommandLineInteraction {

  UserAssistModeInteraction(String outputFormat, BigQueryOptions bigQueryOptions) {
    super(outputFormat, bigQueryOptions);
  }

  public void interact(String query) {
    while (true) {
      List<FixResult> fixResults = queryFixer.autoFixUntilUncertainOptions(query);

      System.out.println("Input query: " + query);
      printFixResultsInCommandLine(fixResults);

      FixResult latestResult = fixResults.get(fixResults.size() - 1);
      if (latestResult.getStatus() != FixResult.Status.ERROR_FIXED) {
        // ...
        printFixResultInCommandLine(latestResult);
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
      System.out.println("Enter the option you use to fix the query.");
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
