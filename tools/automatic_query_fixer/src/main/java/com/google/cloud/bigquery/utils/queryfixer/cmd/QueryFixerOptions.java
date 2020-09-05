package com.google.cloud.bigquery.utils.queryfixer.cmd;

import com.google.common.flogger.FluentLogger;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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

  public static final String QUERY_FILE_SHORTCUT = "q";
  public static final String QUERY_FILE = "query-file";

  public static final String MODE_SHORTCUT = "m";
  public static final String MODE = "mode";
  public static final String AUTO_MODE = "auto";
  public static final String USER_ASSISTED_MODE = "user-assisted";
  // The abbreviation of USER_ASSISTED MODE
  public static final String UA_MODE = "ua";
  public static final String SUGGESTION_MODE = "suggestion";
  // The abbreviation of SUGGESTION MODE
  public static final String SG_MODE = "sg";

  private static final Options options = createOptions();
  private final CommandLine commandLine;

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

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
            /*description=*/ "Interactive Mode. The available mode are \"auto\" (default), \"ua/user-assistance\" and \"sg/suggestion\". Please see the README file for the detailed description.");
    options.addOption(option);
    option =
        new Option(
            /*opt=*/ QUERY_FILE_SHORTCUT,
            /*long-opt=*/ QUERY_FILE,
            /*hasArg=*/ true,
            /*description=*/ "The directory of the query file. If query has been provided as an argument, this will be ignored.");
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
    formatter.printHelp(
        "AutomaticQueryFixer -opt <value> --long-opt <value> \"query\"\n\n"
            + "  Introduction:\n"
            + "  A command-line tool automatically fixing multiple common errors\n"
            + "  of BigQuery SQL queries.\n\n"
            + "  Sample Usages:\n"
            + "  > AutomaticQueryFixer -m auto -p \"<your gcp project id>\" \"<your query>\"\n"
            + "  > AutomaticQueryFixer -m sg -c \"path/to/credentials.json\" -o json -p \"<your gcp project id>\" \"<your query>\"\n"
            + "  > AutomaticQueryFixer -m ua -o natural -p \"<your gcp project id>\" -q \"path/to/sql_file\"\n\n"
            + "Options:\n",
        options);
    System.exit(1);
  }

  /**
   * Get the input query from the user input.
   *
   * @return query.
   */
  public String getQuery() {
    if (!commandLine.getArgList().isEmpty()) {
      return commandLine.getArgList().get(0);
    }
    String queryFileDirectory = commandLine.getOptionValue(QUERY_FILE);
    try {
      return Files.readString(Path.of(queryFileDirectory));
    } catch (IOException e) {
      logger.atWarning().withCause(e).log(
          "Unable to read the query from the given file directory.");
    }

    return null;
  }
}
