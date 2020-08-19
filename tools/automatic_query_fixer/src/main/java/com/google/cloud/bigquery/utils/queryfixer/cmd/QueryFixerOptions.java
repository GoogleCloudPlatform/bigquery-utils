package com.google.cloud.bigquery.utils.queryfixer.cmd;

import org.apache.commons.cli.*;

/** A class responsible for reading the program configurations/options from user input. */
public class QueryFixerOptions {

  public static final String CREDENTIALS_SHORTCUT = "c";
  public static final String CREDENTIALS = "credentials";

  public static final String PROJECT_ID_SHORTCUT = "p";
  public static final String PROJECT_ID = "project-id";

  public static final String OUTPUT_SHORTCUT = "o";
  public static final String OUTPUT = "output";
  public static final String JSON_OUTPUT = "json";
  public static final String NATURAL_OUTPUT = "natural";

  public static final String MODE_SHORTCUT = "m";
  public static final String MODE = "mode";
  public static final String AUTO_MODE = "auto";
  public static final String USER_ASSISTANCE_MODE = "user-assistance";
  // The abbreviation of USER_ASSISTANCE_MODE
  public static final String UA_MODE = "ua";
  public static final String FIX_ONCE_MODE = "fix-once";
  // The abbreviation of FIX_ONCE_MODE
  public static final String FO_MODE = "fo";

  private static final Options options = createOptions();
  private final CommandLine commandLine;

  QueryFixerOptions(CommandLine commandLine) {
    this.commandLine = commandLine;
  }

  /**
   * Read the program options from both the flag options and arguments.
   *
   * @param args User input.
   * @return QueryFixerOptions instance.
   */
  public static QueryFixerOptions readUserInput(String[] args) {
    CommandLine commandLine = buildCommandLine(args);
    if (commandLine == null) {
      return null;
    }
    return new QueryFixerOptions(commandLine);
  }

  public static Options createOptions() {
    Options options = new Options();

    Option option =
        new Option(
            /*opt=*/ CREDENTIALS_SHORTCUT,
            /*long-opt=*/ CREDENTIALS,
            /*hasArg=*/ true,
            /*description=*/ "The path to the credential file of the service account connecting to BigQuery. Otherwise, the default application-login credential will be used.");
    options.addOption(option);
    option =
        new Option(
            /*opt=*/ PROJECT_ID_SHORTCUT,
            /*long-opt=*/ PROJECT_ID,
            /*hasArg=*/ true,
            /*description=*/ "The ID of project where queries will be performed. This field is required if the project is not specified in credential");
    options.addOption(option);
    option =
        new Option(
            /*opt=*/ OUTPUT_SHORTCUT,
            /*long-opt=*/ OUTPUT,
            /*hasArg=*/ true,
            /*description=*/ "The format to output fix results. The available formats are \"natural\" (default) and \"json\"");
    options.addOption(option);
    option =
        new Option(
            /*opt=*/ MODE_SHORTCUT,
            /*long-opt=*/ MODE,
            /*hasArg=*/ true,
            /*description=*/ "Interactive Mode. The available mode are \"auto\" (default), \"ua/user-assistance\" and \"fo/fix-once\". Please see the README file for the detailed description.");
    options.addOption(option);
    return options;
  }

  private static CommandLine buildCommandLine(String[] args) {
    if (args.length == 0) {
      System.out.println("Please provide arguments.");
      return null;
    }
    // A parser to resolve the information from the CLI Input.
    CommandLineParser parser = new DefaultParser();
    try {
      return parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      return null;
    }
  }

  /**
   * Read the value of a flag option.
   *
   * @param option the name of a flag.
   * @return Value in string.
   */
  public String getOptionValue(String option) {
    return commandLine.getOptionValue(option);
  }

  /** A static function to print the help menu. */
  public static void printHelpAndExit() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("-opt <value> --long-opt <value> \"query\"", options);
    System.exit(1);
  }

  /**
   * Get the input query from the user input.
   *
   * @return query.
   */
  public String getQuery() {
    if (commandLine.getArgList().isEmpty()) {
      return null;
    }
    return commandLine.getArgList().get(0);
  }
}
