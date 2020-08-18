package com.google.cloud.bigquery.utils.queryfixer.cmd;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.utils.queryfixer.AutomaticQueryFixer;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.NonNull;

import java.util.List;

import static com.google.cloud.bigquery.utils.queryfixer.cmd.QueryFixerOptions.*;

public abstract class CommandLineInteraction {
  final String outputFormat;
  final BigQueryOptions bigQueryOptions;
  final AutomaticQueryFixer queryFixer;

  public CommandLineInteraction(@NonNull String outputFormat, BigQueryOptions bigQueryOptions) {
    this.outputFormat = outputFormat;
    this.bigQueryOptions = bigQueryOptions;
    this.queryFixer = new AutomaticQueryFixer(bigQueryOptions);
  }

  public static CommandLineInteraction create(
      String mode, String outputFormat, @NonNull BigQueryOptions bigQueryOptions) {
    if (outputFormat == null) {
      outputFormat = NATURAL_OUTPUT;
    }
    if (mode == null) {
      mode = AUTO_MODE;
    }

    switch (mode) {
      case AUTO_MODE:
        return new AutoModeInteraction(outputFormat, bigQueryOptions);
      case USER_ASSISTANCE_MODE:
      case UA_MODE:
        return new UserAssistModeInteraction(outputFormat, bigQueryOptions);
      case FIX_ONCE_MODE:
      case FO_MODE:
        return new FixOnceModeInteraction(outputFormat, bigQueryOptions);
      default:
        System.out.println("Mode (-m) is incorrect. Use --help for usage.");
        System.exit(1);
        return null;
    }
  }

  public abstract void interact(String query);

  static void printFixResultsInCommandLine(List<FixResult> fixResults) {
    for (int i = 0; i < fixResults.size() - 1; i++) {
      printIntermediateFixResultInCommandLine(fixResults.get(i));
    }
    printFixResultInCommandLine(fixResults.get(fixResults.size() - 1));
  }

  static void printIntermediateFixResultInCommandLine(FixResult fixResult) {
    // There is no need to check the status, because intermediate FixResults can only have
    // ERROR_FIXED as status.
    System.out.println("The query has an error: " + fixResult.getError());
    System.out.println("It is fixed by the approach: " + fixResult.getApproach());
    FixOption option = fixResult.getOptions().get(0);
    System.out.println(String.format("Action: %s", option.getAction()));
    System.out.println();
    System.out.println(String.format("Fixed query: %s", option.getFixedQuery()));
    System.out.println("-----------------------------------\n");
  }

  static void printFixResultInCommandLine(FixResult fixResult) {
    if (fixResult.getStatus() == FixResult.Status.NO_ERROR) {
      System.out.println("The input query is valid. No errors to fix.");
      return;
    }

    System.out.println("The query has an error: " + fixResult.getError());

    if (fixResult.getStatus() == FixResult.Status.FAILURE) {
      System.out.println("Failed to fix the input query.");
      return;
    }

    System.out.println("It can be fixed by the approach: " + fixResult.getApproach());
    printFixOptions(fixResult.getOptions());
  }

  private static void printFixOptions(List<FixOption> options) {
    System.out.println();
    int count = 1;
    for (FixOption option : options) {
      System.out.println(String.format("%d. Action: %s", count++, option.getAction()));
      System.out.println(String.format("   Fixed query: %s", option.getFixedQuery()));
      System.out.println();
    }
  }

  static void printAsJson(Object object) {
    Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
    System.out.println(gson.toJson(object));
  }
}
