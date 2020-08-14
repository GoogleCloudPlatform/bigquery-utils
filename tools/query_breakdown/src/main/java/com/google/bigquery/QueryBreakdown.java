package com.google.bigquery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Stack;
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
  private int minimumUnparseableComp;
  private Node solution;

  // the generated tree
  private final Node root;
  private final Parser parser;

  /**
   * Constructor for the QueryBreakdown object. We model this class as an object rather than
   * through static methods because the user should be able to call QueryBreakdown multiple
   * times and create multiple instances of it.
   */
  public QueryBreakdown(Parser parser) {
    this.minimumUnparseableComp = Integer.MAX_VALUE;
    this.root = new Node();
    this.parser = parser;
  }

  /**
   * This is the method that will run QueryBreakdown given an original query and output
   * it to the specified output file or commandline. The provided errorLimit will stop the
   * tool from running over a certain time.
   *
   * TODO: output file feature and runtime limit support
   */
  public void run(String originalQuery, String outputFile, int errorLimit,
      LocationTracker locationTracker) {

    // uses the loop function to generate and traverse the tree of possible error recoveries
    // this will set the variable solution
    loop(originalQuery, errorLimit, root, 0, locationTracker);

    // case where entire query can be parsed
    if (solution.equals(root)) {
      System.out.println("The entire query can be parsed without error");
    }

    // write termination logic for output (tracing the node back, reconstructing path, output)
    Node current = solution;

    // we use a stack to display the results in order
    Stack<Node> stack = new Stack<>();
    while (current.getParent() != null) {
      stack.push(current);
      current = current.getParent();
    }

    // we then pop the stack and output results one by one
    while (!stack.empty()) {
      current = stack.pop();
      if (current.getErrorHandlingType().equals("DELETION")) {
        // print out the result
        System.out.println(String.format("Unparseable portion: Start Line %1$s, End Line %2$s, "
                + "Start Column %3$s, End Column %4$s, %5$s", current.getStartLine(),
            current.getEndLine(), current.getStartColumn(), current.getEndColumn(),
            current.getErrorHandlingType()));
      }
      else {
        System.out.println(String.format("Unparseable portion: Start Line %1$s, End Line %2$s, "
                + "Start Column %3$s, End Column %4$s, %5$s: replaced %6$s with %7$s",
            current.getStartLine(), current.getEndLine(), current.getStartColumn(),
            current.getEndColumn(), current.getErrorHandlingType(), current.getReplaceFrom(),
            current.getReplaceTo()));
      }
    }
  }

  /**
   * This is where the code for the algorithm resides: essentially, there is a loop that
   * constantly inputs a new query after adequate error handling. The loop terminates once
   * the parsing doesn't throw any errors, and in the case that it went through a smaller
   * number of unparseable components than the global minimum, it sets the solution as
   * the global solution and also alters the minimumUnparseableComp variable.
   *
   * TODO: implement errorLimit logic, deal with exception casting
   */
  private void loop(String inputQuery, int errorLimit, Node parent, int depth,
      LocationTracker locationTracker) {
    // termination for branch
    if (depth > minimumUnparseableComp) {
      return;
    }
    try {
      parser.parseQuery(inputQuery);
    } catch (Exception e) {
      // generates new queries through deletion and replacement
      SqlParserPos pos = ((SqlParseException) e).getPos();

      // gets the error location in the original query
      int originalStartColumn =
          locationTracker.getOriginalPosition(pos.getLineNum(), pos.getColumnNum());
      int originalEndColumn =
          locationTracker.getOriginalPosition(pos.getLineNum(), pos.getEndColumnNum());;

      /* deletion: gets the new query, creates a node, and calls the loop again */
      // gets the new query
      String deletionQuery = deletion(inputQuery, pos.getLineNum(), pos.getColumnNum(),
          pos.getEndColumnNum());

      // updates the location tracker to reflect the deletion
      LocationTracker deletedLt = locationTracker.delete
          (pos.getLineNum(), pos.getColumnNum(), pos.getEndColumnNum());

      // creates a node for this deletion
      Node deletionNode = new Node(parent, pos.getLineNum(), originalStartColumn,
          pos.getEndLineNum(), originalEndColumn, depth + 1 );

      // calls the loop again
      loop(deletionQuery, errorLimit, deletionNode, depth + 1, deletedLt);

      /* replacement: gets the new queries, creates nodes, and calls the loop for each of them */
      ArrayList<ReplacedComponent> replacementQueries = replacement(inputQuery, pos.getLineNum(),
          pos.getColumnNum(), pos.getEndColumnNum(),
          ((SqlParseException) e).getExpectedTokenNames());

      // recursively loops through the new queries
      for (ReplacedComponent r: replacementQueries) {
        // updates the location tracker to reflect the replacement
        LocationTracker replacedLt = locationTracker.replace(pos.getLineNum(), pos.getColumnNum(),
            pos.getEndColumnNum(), r.getOriginal(), r.getReplacement());
        Node replacementNode = new Node(parent, pos.getLineNum(), originalStartColumn,
            pos.getEndLineNum(), originalEndColumn, r.getOriginal(), r.getReplacement(),
            depth + 1);
        loop(r.getQuery(), errorLimit, replacementNode, depth + 1, replacedLt);
      }

      /* termination to end the loop if the instance was not a full run through the query.
      In other words, it ensures that the termination condition is not hit on the way back
      up the tree */
      return;
    }
    // termination condition: if the parsing doesn't throw exceptions, then the leaf is reached
    if (depth < minimumUnparseableComp) {
      minimumUnparseableComp = depth;
      solution = parent;
    }
  }

  /**
   * This method implements the deletion mechanism: given the position of the component, it
   * generates a new query with that component deleted.
   */
  static String deletion(String inputQuery, int startLine, int startColumn,
      int endColumn) {
    StringBuilder sb = new StringBuilder(inputQuery);
    int[] index = returnIndex(inputQuery, startLine, startColumn, endColumn);
    sb.delete(index[0], index[1]);
    return sb.toString();
  }

  /**
   * This method implements the replacement mechanism: given the position of the component, and
   * given the help of the ReplacementLogic class, it determines what to replace the component
   * with and generates the new query based on it. It then returns a list of ReplacedComponents
   * containing the new query and the two components that we replace from/to.
   *
   * This is a design decision made due to the fact that we need to expose to the loop the word
   * being replaced and the word we're replacing with.
   *
   * TODO: deal with instances where there are no replacement options
   */
  static ArrayList<ReplacedComponent> replacement(String inputQuery, int startLine, int startColumn,
      int endColumn, Collection<String> expectedTokens) {
    // call ReplacementLogic
    ArrayList<String> finalList = ReplacementLogic.replace(inputQuery,
        expectedTokensFilter(expectedTokens));

    ArrayList<ReplacedComponent> result = new ArrayList<>();

    // get word to replace from
    int[] index = returnIndex(inputQuery, startLine, startColumn, endColumn);
    String replaceFrom = inputQuery.substring(index[0], index[1]);

    // generate the new queries. We need to re-instantiate the StringBuilder each time
    for (String replaceTo: finalList) {
      // replace the token
      StringBuilder sb = new StringBuilder(inputQuery);
      sb.replace(index[0], index[1], replaceTo);
      result.add(new ReplacedComponent(sb.toString(), replaceFrom, replaceTo));
    }
    return result;
  }

  /**
   * This method filters out EOF from the expected tokens as well as the quotations
   */
  static ArrayList<String> expectedTokensFilter(Collection<String> expectedTokens) {
    // remove EOF
    if (expectedTokens.contains("<EOF>")) {
      expectedTokens.remove("<EOF>");
    }

    // filter out the quotations
    ArrayList<String> filtered = new ArrayList<>();
    for (String s : expectedTokens) {
      s = s.replace("\"", "");
      filtered.add(s);
    }

    return filtered;
  }

  /**
   * This helper method returns the beginning and ending index for the component of the given
   * query specified by the startLine, startColumn, and endColumn
   */
  static int[] returnIndex(String inputQuery, int startLine, int startColumn, int endColumn) {
    int[] result = new int[2];
    // when the exception occurs in line 1
    if (startLine == 1) {
      result[0] = startColumn - 1;
      result[1] = endColumn;
    }
    else {
      int position = findNthIndexOf(inputQuery, '\n', startLine -1);
      result[0] = position + startColumn;
      result[1] = position + endColumn + 1;
    }

    return result;
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
