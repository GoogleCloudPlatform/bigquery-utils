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

/**
 * A base class to implement the interaction between users and the query fixer. It has three modes:
 * Auto, User-assisting, and Suggestion (fully interactive). Each mode is represented by a subclass.
 */
public abstract class CommandLineInteraction {
  final String outputFormat;
  final BigQueryOptions bigQueryOptions;
  final AutomaticQueryFixer queryFixer;

  CommandLineInteraction(@NonNull String outputFormat, BigQueryOptions bigQueryOptions) {
    this.outputFormat = outputFormat;
    this.bigQueryOptions = bigQueryOptions;
    this.queryFixer = new AutomaticQueryFixer(bigQueryOptions);
  }

  /**
   * Create an instance of {@link CommandLineInteraction} based on the mode and output format.
   *
   * @param mode The mode of the Query Fixer.
   * @param outputFormat the output format: either natural or json.
   * @param bigQueryOptions options to connect BigQuery server.
   * @return an instance of CommandLineInteraction.
   */
  public static CommandLineInteraction create(
      String mode, String outputFormat, @NonNull BigQueryOptions bigQueryOptions) {
    if (outputFormat == null) {
      outputFormat = NATURAL_OUTPUT;
    }
    outputFormat = outputFormat.toLowerCase();

    if (mode == null) {
      mode = AUTO_MODE;
    }
    mode = mode.toLowerCase();

    switch (mode) {
      case AUTO_MODE:
        return new AutoModeInteraction(outputFormat, bigQueryOptions);
      case USER_ASSISTED_MODE:
      case UA_MODE:
        return new UserAssistedModeInteraction(outputFormat, bigQueryOptions);
      case SUGGESTION_MODE:
      case SG_MODE:
        return new SuggestionModeInteraction(outputFormat, bigQueryOptions);
      default:
        System.out.println("Mode (-m) is incorrect. Use --help for usage.");
        System.exit(1);
        return null;
    }
  }

  /**
   * Start the interaction between users and the query fixer.
   *
   * @param query input query.
   */
  public abstract void interact(String query);

  static void printFixResultsInCommandLine(List<FixResult> fixResults) {
    for (int i = 0; i < fixResults.size() - 1; i++) {
      printIntermediateFixResultInCommandLine(fixResults.get(i));
    }
    printFixResultInCommandLine(fixResults.get(fixResults.size() - 1));
  }

  /**
   * Intermediate {@link FixResult} represents the intermediate result of an automatic fixing
   * process. Sometimes a query may have multiple errors and the query fixer will fix them
   * iteratively. If a further fix can be applied to the current state, then the current {@link
   * FixResult} is considered as intermediate.
   */
  static void printIntermediateFixResultInCommandLine(FixResult fixResult) {
    // There is no need to check the status, because intermediate FixResults can only have
    // ERROR_FIXED as status.
    System.out.println("The query has an error: " + fixResult.getError());
    System.out.println("It is fixed by the approach: " + fixResult.getApproach());
    FixOption option = fixResult.getOptions().get(0);
    System.out.println(String.format("Action: %s", option.getAction()));
    System.out.println();
    System.out.println(String.format("Fixed query:\n%s", option.getFixedQuery()));
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
      System.out.println(String.format("   Fixed query:\n%s", option.getFixedQuery()));
      System.out.println();
    }
  }

  static void printAsJson(Object object) {
    Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
    System.out.println(gson.toJson(object));
  }
}
