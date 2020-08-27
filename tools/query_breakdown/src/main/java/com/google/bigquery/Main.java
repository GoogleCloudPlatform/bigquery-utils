package com.google.bigquery;

import static java.lang.System.exit;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.*;

import java.io.IOException;
import org.json.simple.JSONArray;

/**
 * This file is the main file for the command line tool.
 * Usage: query_breakdown -i <PATH> [-j] [-l <INTEGER>]
 * -i, --inputFile, PATH: this command specifies the path to the file containing queries to be
 *                    inputted into the tool. It is therefore mandatory
 * -j, --json: this command specifies whether the program should output the results in a
 *                   json format. It is therefore optional
 * -l, --limit, INTEGER: this command specifies the path to an integer that the tool takes as a
 *                    limit for the number of errors to be explored, thereby controlling the
 *                    runtime. It is therefore optional
 *
 * Sample Usage: query_breakdown -i input.txt
 *               query_breakdown -i input2.txt -j -l 3
 *               query_breakdown -i input3.txt -j
 *               query_breakdown -i input4.txt -l 6
 */
public class Main {
  public static void main(String[] args) {
    String inputFile = null;
    int errorLimit = 0;
    boolean jsonOutput = false;
    CommandLine cl = createCommand(args);

    // if there is an error in parsing the commandline
    if (cl == null) {
      exit(1);
    }

    if (cl.hasOption("i")) {
      inputFile = cl.getOptionValue("i");
    }
    if (cl.hasOption("j")) {
      jsonOutput = true;
    }
    if (cl.hasOption("l")) {
      errorLimit = Integer.parseInt( cl.getOptionValue("l"));
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

    /* this is where we feed in the original queries to QueryBreakdown, which will find
       all the unparseable components of the query.
     */
    if (ir.getLocationTrackers().size() != ir.getQueries().size()) {
      System.out.println("there was an error in input parsing: wrong number of queries and "
          + "location trackers");
      exit(1);
    }

    // runs the tool on multiple queries
    List<String> queries = ir.getQueries();
    List<LocationTracker> locationTrackers = ir.getLocationTrackers();
    List<Node> endResult = new ArrayList<>();
    for (int i = 0; i < queries.size(); i++) {
      QueryBreakdown qb = new QueryBreakdown(new CalciteParser());
      List<Node> result = qb.run(queries.get(i), errorLimit, locationTrackers.get(i));
      endResult.addAll(result);
    }

    // outputs the results
    if (jsonOutput) {
      if (endResult.isEmpty()) {
        System.out.println("[]");
        return;
      }
      JSONArray jsonArray = new JSONArray();
      for (Node node: endResult) {
        jsonArray.add(node.toJSON());
      }
      System.out.println(jsonArray);
    }
    else {
      if (endResult.isEmpty()) {
        System.out.println("The entire query can be parsed without error");
        return;
      }
      for (Node node: endResult) {
        System.out.println(node.toString());
      }
    }
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
      System.out.println("there was an issue parsing the commandline" + e.getMessage());
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
            + "as a limit for the number of errors to be explored, thereby controlling"
            + "the runtime. It is therefore optional").build());
    return options;
  }
}