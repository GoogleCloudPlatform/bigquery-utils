package com.google.bigquery;

import static java.lang.System.exit;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.*;

import java.io.IOException;
import org.json.simple.JSONArray;
import java.text.DecimalFormat;
import org.json.simple.JSONObject;

/**
 * This file is the main file for the command line tool.
 * Usage: query_breakdown -i <PATH> [-j] [-l <INTEGER>] [-r <INTEGER>]
 * -i, --inputFile, PATH: this command specifies the path to the file containing queries to be
 *                    inputted into the tool. It is therefore mandatory
 * -j, --json: this command specifies whether the program should output the results in a
 *                   json format. It is therefore optional
 * -l, --limit, INTEGER: this command specifies the path to an integer that the tool takes
 *                       as a limit for the number of milliseconds a tool can spend on a query,
 *                       thereby controlling the overall runtime. It is therefore optional
 * -r, --replacement, INTEGER: this command specifies the number of replacements that can be
 *                             recommended by the ReplacementLogic class, thereby controlling
 *                             the runtime and performance. It is therefore optional
 *
 * Sample Usages: query_breakdown -i input.txt
 *                query_breakdown -i input2.txt -j -l 24 -r 4
 *                query_breakdown -i input3.txt -j
 *                query_breakdown -i input4.txt -l 6 -r 2
 *                query_breakdown -i input5.txt -r 3
 *                query_breakdown -i input6.txt -l 25
 */
public class Main {
  public static void main(String[] args) {
    // starts the timer for runtime measurement
    long start = System.nanoTime();

    String inputFile = null;
    int runtimeLimit = 100000; // default value for runtime limit, measured in seconds
    int replacementLimit = 3; // default value for number of recommended replacements
    boolean jsonOutput = false;
    CommandLine cl = createCommand(args);

    // if there is an error in parsing the commandline
    if (cl == null) {
      exit(1);
    }

    // sets the variables accordingly from the flag arguments
    if (cl.hasOption("i")) {
      inputFile = cl.getOptionValue("i");
    }
    if (cl.hasOption("j")) {
      jsonOutput = true;
    }
    if (cl.hasOption("l")) {
      runtimeLimit = Integer.parseInt(cl.getOptionValue("l"));
    }
    if (cl.hasOption("r")) {
      replacementLimit = Integer.parseInt(cl.getOptionValue("r"));
    }

    InputReader ir = null;
    // this is where we will put the file I/O logic through the input reader.
    try {
      ir = new InputReader(inputFile);
    } catch (IOException e) {
      System.out.println("there was an I/O error while reading the input");
      e.printStackTrace();
      exit(1);
    }

    if (ir.getLocationTrackers().size() != ir.getQueries().size()) {
      System.out.println("there was an error in input parsing: wrong number of queries and "
          + "location trackers");
      exit(1);
    }

    // we initialize a file to output to
    FileWriter writer = null;
    try {
      String absPath = new File("").getAbsolutePath();
      File outputFile = new File(absPath + "/output.txt");
      outputFile.createNewFile();
      writer = new FileWriter(outputFile);
    } catch (IOException e) {
      System.out.println("there was an I/O error while creating an output file");
      e.printStackTrace();
      exit(1);
    }

    /* this is where we feed in each of the original queries to QueryBreakdown, which will find
       all the unparseable components of the query. We then get the results and add them to the
       endResult list. We also output the results in the txt file created before.
     */

    // gets queries and locationTrackers initialized by the InputReader
    List<String> queries = ir.getQueries();
    List<LocationTracker> locationTrackers = ir.getLocationTrackers();

    // contains all the nodes to output as results
    List<Node> endResult = new ArrayList<>();
    for (int i = 0; i < queries.size(); i++) {
      QueryBreakdown qb = new QueryBreakdown(new CalciteParser());
      List<Node> result = qb.run(queries.get(i), runtimeLimit, replacementLimit,
          locationTrackers.get(i));
      endResult.addAll(result);
      try {
        writer.write("Original Query: " + queries.get(i) + "\n\n");
        if (result.isEmpty()) {
          writer.write("Resulting Query: " + "the entire query can be parsed without error"
              + "\n\n");
        }
        else {
          writer.write("Resulting Query: " + qb.getFinalString() + "\n\n");
        }
      } catch (IOException e) {
        System.out.println("there was an I/O error while writing to an output file");
        e.printStackTrace();
        exit(1);
      }
    }

    try {
      writer.close();
    } catch (IOException e) {
      System.out.println("there was an I/O error while closing the writer");
      e.printStackTrace();
      exit(1);
    }

    // keeps track of total characters of unparesable components for performance analysis
    int totalUnparseable = 0;

    // outputs the results accordingly as json or user-readable format
    if (jsonOutput) {
      // all queries paresable
      if (endResult.isEmpty()) {
        System.out.println("[]");
        return;
      }
      JSONArray jsonArray = new JSONArray();
      for (Node node: endResult) {
        jsonArray.add(node.toJSON());
        totalUnparseable += node.getUnparseableCount();
      }

      // add performance metric
      DecimalFormat df = new DecimalFormat("##.#");
      double x = 100 - (double) totalUnparseable / ir.getDocLength() * 100;
      JSONObject performance = new JSONObject();
      performance.put("performance", df.format(x));
      jsonArray.add(performance);

      // add runtime
      long end = System.nanoTime();
      float runtimeSeconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
      JSONObject runtime = new JSONObject();
      runtime.put("runtime", runtimeSeconds);
      jsonArray.add(runtime);
      System.out.println(jsonArray);
    }
    else {
      if (endResult.isEmpty()) {
        System.out.println("The entire query can be parsed without error");
        return;
      }

      // print out results
      for (Node node: endResult) {
        System.out.println(node.toString());
        totalUnparseable += node.getUnparseableCount();
      }

      // print out performance metric
      DecimalFormat df = new DecimalFormat("##.#");
      double x = 100 - (double) totalUnparseable / ir.getDocLength() * 100;
      System.out.println("Percentage of Parseable Components: " + df.format(x) + "%");

      // print out runtime
      long end = System.nanoTime();
      float runtimeSeconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
      System.out.println("Runtime: " + runtimeSeconds + " seconds");
    }

    // to deal with a bug where program does not terminate in CLI without explicit exit call
    System.exit(0);
  }

  /**
   * This is the method that instantiates a CommandLine object for the Apache CLI Interface.
   * It deals with command line parsing as well as help generation once parsing is unavailable
   */
  public static CommandLine createCommand(String[] args) {
    CommandLineParser parser = new DefaultParser();
    Options options = createOptions();
    HelpFormatter help = new HelpFormatter();

    CommandLine cl = null;
    try {
      cl = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println("there was an issue parsing the commandline\n" + e.getMessage());
      help.printHelp("query_breakdown", options, true);
    }

    return cl;
  }

  /**
   * This is the method that instantiates options for the Apache CLI interface
   */
  public static Options createOptions() {
    Options options = new Options();
    options.addOption(Option.builder("i").required(true).longOpt("inputFile").hasArg(true)
        .argName("PATH").desc("this command specifies the path to the file "
            + "containing queries to be inputted into the tool. It is therefore mandatory")
        .build());
    options.addOption(Option.builder("j").longOpt("json")
            .desc("this command specifies whether the program should output the results in a \n"
                    + "json format. It is therefore optional").build());
    options.addOption(Option.builder("l").longOpt("limit").hasArg(true).argName("INTEGER")
        .desc("this command specifies the path to an integer that the tools takes "
            + "as a limit for the number of milliseconds a tool can spend on a query, thereby "
            + "controlling the overall runtime. It is therefore optional").build());
    options.addOption(Option.builder("r").longOpt("replacement").hasArg(true)
        .argName("INTEGER").desc("this command specifies the number of replacements that can be"
            + "recommended by the ReplacementLogic class, thereby controlling"
            + "the runtime and performance. It is therefore optional").build());
    return options;
  }
}