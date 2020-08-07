package com.google.cloud.bigquery.utils.queryfixer;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.*;

import java.util.List;

public class QueryFixerMain {

  private static CommandLine readFlags(String[] args) {
    Options options = new Options();

    Option option =
        new Option(
            /*opt=*/ "c",
            /*long-opt=*/ "credential",
            /*hasArg=*/ true,
            /*description=*/ "The credential file (in JSON) of service account connecting to BigQuery. Otherwise, the default application-login credential will be used.");
    options.addOption(option);
    option =
        new Option(
            /*opt=*/ "p",
            /*long-opt=*/ "project-id",
            /*hasArg=*/ true,
            /*description=*/ "The ID of project where queries will be performed. This field is required if the project is not specified in credential");
    options.addOption(option);
    option =
        new Option(
            /*opt=*/ "o",
            /*long-opt=*/ "output",
            /*hasArg=*/ true,
            /*description=*/ "The format to output fix results. The available formats are \"natural\" (default) and \"json\"");
    options.addOption(option);
    option =
        new Option(
            /*opt=*/ "i",
            /*long-opt=*/ "interact",
            /*hasArg=*/ true,
            /*description=*/ "Interactive Mode. The available mode are \"none\" (default), \"guide\" and \"all/full\"");
    options.addOption(option);

    if (args.length == 0) {
      System.out.println("Please provide arguments.");
      printHelpAndExit(options);
    }

    CommandLineParser parser = new DefaultParser();
    try {
      return parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      printHelpAndExit(options);
      return null;
    }
  }

  public static void printHelpAndExit(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("-opt <value> --long-opt <value> \"query\"", options);
    System.exit(1);
  }

  public static void main(String[] args) {
    CommandLine cmd = readFlags(args);

    String credentialPath = cmd.getOptionValue("credential");
    String projectId = cmd.getOptionValue("project-id");
    BigQueryOptions bigQueryOptions = buildBigQueryOptions(credentialPath, projectId);

    if (cmd.getArgList().isEmpty()) {
      // In CLI mode, all the instructions are output by print functions, because logger outputs
      // extract info (time, code position) that distracts users.
      System.out.println(
          "Please provide the query as an argument, enclosed by double quote. Use --help for instruction.");
    }

    String query = cmd.getArgList().get(0);
    System.out.println("Input query: " + query);

    AutomaticQueryFixer queryFixer = new AutomaticQueryFixer(bigQueryOptions);

    String interactMode = cmd.getOptionValue("interact");
    if (interactMode == null || interactMode.equalsIgnoreCase("none")) {
      // todo: Implement Non-interactive mode
      FixResult fixResult = queryFixer.fix(query);
      if (fixResult.getOptions().isEmpty()) {
        return;
      }
      String newQuery = fixResult.getOptions().get(0).getFixedQuery();
      printQueryResult(newQuery, bigQueryOptions);

    } else if (interactMode.equalsIgnoreCase("guide")) {
      // todo: Implement guide mode
      return;
    } else if (interactMode.equalsIgnoreCase("all") || interactMode.equalsIgnoreCase("full")) {
      FixResult fixResult = fullInteractMode(queryFixer, query);
      printFixResult(fixResult, cmd.getOptionValue("output"));
    } else {
      System.out.println("Interact Mode (-i) is incorrect. Use --help for usage.");
      System.exit(1);
    }
  }

  private static BigQueryOptions buildBigQueryOptions(String credentialPath, String projectId) {
    if (credentialPath == null) {
      return BigQueryOptions.newBuilder().setProjectId(projectId).build();
    } else {
      // TODO: should support this in near future.
      System.out.println("customized credential is not supported");
      System.exit(1);
      return null;
    }
  }

  private static FixResult fullInteractMode(AutomaticQueryFixer queryFixer, String query) {
    return queryFixer.fix(query);
  }

  private static void printFixResult(FixResult fixResult, String outputFormat) {
    if (outputFormat == null || outputFormat.equalsIgnoreCase("natural")) {
      printFixResultInCommandLine(fixResult);
    } else if (outputFormat.equalsIgnoreCase("json")) {
      printFixResultAsJson(fixResult);
    } else {
      System.out.println("Output Mode (-o) is incorrect. Use --help for usage.");
      System.exit(1);
    }
  }

  private static void printFixResultAsJson(FixResult fixResult) {
    Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
    System.out.println(gson.toJson(fixResult));
  }

  private static void printFixResultInCommandLine(FixResult fixResult) {
    if (fixResult.getStatus() == FixResult.Status.NO_ERROR) {
      System.out.println("The input query is correct");
      return;
    }

    System.out.println("The query has an error: " + fixResult.getError());

    if (fixResult.getStatus() == FixResult.Status.FAILURE) {
      System.out.println("The input query is unable to fix");
      return;
    }

    System.out.println("It can be fixed by the approach: " + fixResult.getApproach());
    printFixOptions(fixResult.getOptions());
  }

  private static void printFixOptions(List<FixOption> options) {
    System.out.println();
    int count = 1;
    for (FixOption option : options) {
      System.out.println(String.format("%d. Option: %s", count++, option.getDescription()));
      System.out.println(String.format("   Fixed query: %s", option.getFixedQuery()));
      System.out.println();
    }
  }

  private static void printQueryResult(String query, BigQueryOptions options) {
    BigQuery bigQuery = options.getService();

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
}
