package com.google.bigquery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * This class is where the main logic lives for the algorithm that this tool utilizes. It will
 * also be in charge of outputting the results.
 *
 * Note: functions in this class are left as package private for testing purposes. The visibility
 * (public/private) will properly be set in upcoming PR's.
 */
public class QueryBreakdown {

  // global fields that keeps track of the minimum unparseable component so far
  private int minimumUnparseableComp = Integer.MAX_VALUE;
  private Node solution;

  // the generated tree
  private Node root;
  private static Parser parser;

  /**
   * This is the method that will run QueryBreakdown given an original query and output
   * it to the specified output file, or if that is null, generate a new file to put the output in.
   * The provided timeLimit will stop the tool from running over a certain time.
   */
  public static void run(String originalQuery, String outputFile, int errorLimit) {

    // determines which parser to use
    parser = new CalciteParser();

    // uses the loop function to generate and traverse the tree of possible error recoveries
    loop(originalQuery, errorLimit);

    // write termination logic for output (tracing the node back, reconstructing path, output)
  }

  /**
   * This is where the code for the algorithm will go: essentially, there will be a loop that
   * constantly inputs a new query after adequate error handling
   */
  private static void loop(String inputQuery, int errorLimit) {
    try {
      parser.parseQuery(inputQuery);
    } catch (Exception e) {
      // generates new queries through deletion and replacement
      SqlParserPos pos = ((SqlParseException) e).getPos();
      String deletionQuery = deletion(inputQuery, pos.getLineNum(), pos.getColumnNum(),
          pos.getEndColumnNum());
      List<String> replacementQueries = replacement(inputQuery, pos.getLineNum(),
          pos.getColumnNum(), pos.getEndColumnNum(),
          ((SqlParseException) e).getExpectedTokenNames());

      // recursively loops through the new queries
      loop(deletionQuery, errorLimit);
      for (String s: replacementQueries) {
        loop(s, errorLimit);
      }
    }
    // termination condition: if the parsing doesn't throw exceptions, then the leaf is reached
  }

  /**
   * This method implements the deletion mechanism: given the position of the component, it
   * generates a new query with that component deleted.
   */
  static String deletion(String inputQuery, int startLine, int startColumn,
      int endColumn) {
    StringBuilder sb = new StringBuilder(inputQuery);

    // delete the portion of the string from x (inclusive) to y (exclusive)
    int x;
    int y;

    // when the exception occurs in line 1
    if (startLine == 1) {
      x = startColumn - 1;
      y = endColumn;
      // deals with extra spacing when deleting
      if (inputQuery.charAt(startColumn - 2) == ' ') {
        x = startColumn - 2;
      }
    }
    else {
      int position = findNthIndexOf(inputQuery, '\n', startLine -1);
      x = position + startColumn;
      y = position + endColumn + 1;
      // deals with extra spacing when deleting
      if (inputQuery.charAt(position + startColumn - 1) == ' ') {
        x = position + startColumn - 1;
      }
    }

    sb.delete(x, y);
    return sb.toString();
  }

  /**
   * This method implements the replacement mechanism: given the position of the component, and
   * given the help of the ReplacementLogic class, it determines what to replace the component
   * with and generates a new query with that component replaced.
   */
  private static List<String> replacement(String inputQuery, int startLine, int startColumn,
      int endColumn, Collection<String> expectedTokens) {
    // call ReplacementLogic
    ReplacementLogic.replace(inputQuery);
    return new ArrayList<>();
  }

  /**
   * This helper method returns the index of the nth occurrence of key character in the input
   * string. Returns -1 if there is no such instance.
   */
  static int findNthIndexOf(String string, char key, int n) {
    int position = string.indexOf(key);
    while (n > 1) {
      if (position == -1) {
        return position;
      }
      position = string.indexOf(key, position + 1);
      n -= 1;
    }
    return position;
  }
}
