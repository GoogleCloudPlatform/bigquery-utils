package com.google.bigquery;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import com.google.common.util.concurrent.SimpleTimeLimiter;

/**
 * This class is where the main logic lives for the algorithm that this tool utilizes. It will
 * also be in charge of outputting the results  in node format. A new instance of QueryBreakdown is
 * created and utilized per query.
 *
 */
public class QueryBreakdown {

  // global fields that keeps track of the minimum unparseable component so far
  private int minimumUnparseableComp;

  // currently optimal solution leaf node
  private Node solution;

  // the generated tree
  private final Node root;

  // parser for QueryBreakdown
  private final Parser parser;

  // final generated Query for the current optimal solution
  private String finalString;

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
   * This is the method that will run QueryBreakdown given an original query and return the result
   * in the form of a list of nodes. The provided runtimeLimit will stop the
   * tool from running over a certain time on a query, and the replacementLimit will limit the
   * number of replacements recommended. The locationTracker is the one for the original query.
   */
  public List<Node> run(String originalQuery, int runtimeLimit, int replacementLimit,
      LocationTracker locationTracker) {
    /* uses the loop function to generate and traverse the tree of possible error recoveries.
       This will find the optimal solution or abort when timed out */
    try {
      SimpleTimeLimiter limiter = SimpleTimeLimiter.create(Executors.newSingleThreadExecutor());
      limiter.runUninterruptiblyWithTimeout(
          () -> loop(originalQuery, replacementLimit, root, 0, locationTracker),
          runtimeLimit, TimeUnit.MILLISECONDS);
    }
    catch (TimeoutException te) {
      // abort logic: returns current solution or deletes entire query
      if (solution != null) {
        return runTermination();
      }
      Pair first = locationTracker.getOriginalPosition(1, 1);
      Pair last;
      int numLines = locationTracker.getLocation().size();
      if (locationTracker.getLocation().get(numLines - 1).size() == 0) {
        last = locationTracker.getOriginalPosition(numLines - 1,
            locationTracker.getLocation().get(numLines - 2).size());
      }
      else {
        last = locationTracker.getOriginalPosition(numLines,
            locationTracker.getLocation().get(numLines - 1).size());
      }
      Node deletionNode = new Node(null, first.getX(), first.getY(), last.getX(),
          last.getY(), originalQuery.length());
      List<Node> returnNodes = new ArrayList<>();
      returnNodes.add(deletionNode);
      finalString = "";
      return returnNodes;
    }
    // correctly terminated
    return runTermination();
  }

  /**
   * Helper method for termination of the run method above
   */
  private List<Node> runTermination() {
    List<Node> returnNodes = new ArrayList<>();

    // case where entire query can be parsed
    if (solution.equals(root)) {
      return returnNodes;
    }

    // gets the current solution
    Node current = solution;

    // we use a stack to display the results in order
    Stack<Node> stack = new Stack<>();

    // traces back to get the path that led to the current solution
    while (current.getParent() != null) {
      stack.push(current);
      current = current.getParent();
    }

    while(!stack.empty()) {
      current = stack.pop();
      returnNodes.add(current);
    }
    return returnNodes;
  }

  /**
   * This is where the code for the algorithm resides: essentially, there is a loop that
   * constantly inputs a new query after adequate error handling. The loop terminates once
   * the parsing doesn't throw any errors, and in the case that it went through a smaller
   * number of unparseable components than the global minimum, it sets the solution as
   * the global solution and also alters the minimumUnparseableComp variable.
   */
  private void loop(String inputQuery, int replacementLimit, Node parent, int depth,
      LocationTracker locationTracker) {
    // termination for branch if it is deeper than the current solution
    if (depth > minimumUnparseableComp) {
      return;
    }
    try {
      // parses the query
      parser.parseQuery(inputQuery);
    } catch (SqlParseException e) {
      /* generates new queries through deletion and replacement */
      SqlParserPos pos = e.getPos();

      // if statement checks for EOF and validator
      if ((pos.getLineNum() != 0 && pos.getColumnNum() != 0)
          && !(e.getCause().toString().contains("Encountered \"<EOF>\""))
          && !(e.getCause().toString().contains("Encountered: <EOF>"))
          && !e.getCause().toString().contains("SqlValidatorException")) {
        // gets the error location in the original query
        Pair originalStart =
            locationTracker.getOriginalPosition(pos.getLineNum(), pos.getColumnNum());
        Pair originalEnd =
            locationTracker.getOriginalPosition(pos.getEndLineNum(), pos.getEndColumnNum());

        /* deletion: gets the new query, creates a node, and calls the loop again */
        // gets the new query
        String deletionQuery = deletion(inputQuery, pos.getLineNum(), pos.getColumnNum(),
            pos.getEndLineNum(), pos.getEndColumnNum());

        // updates the location tracker to reflect the deletion
        LocationTracker deletedLt = locationTracker.delete
            (pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum());

        // counts number of characters deleted keeping in mind multi-line new line addition
        int deletionNumber = (pos.getLineNum() == pos.getEndLineNum()) ? inputQuery.length() -
            deletionQuery.length() : inputQuery.length() - deletionQuery.length() + 1;

        // creates a node for this deletion
        Node deletionNode = new Node(parent, originalStart.getX(), originalStart.getY(),
            originalEnd.getX(), originalEnd.getY(), deletionNumber);

        // calls the loop again
        loop(deletionQuery, replacementLimit, deletionNode, depth + 1, deletedLt);

        /* replacement: gets the new queries, creates nodes, and calls the loop for each of them */
        ArrayList<ReplacedComponent> replacementQueries = replacement(inputQuery, replacementLimit,
            pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum(),
            e.getExpectedTokenNames());

        // recursively loops through the new queries
        for (ReplacedComponent r: replacementQueries) {
          // updates the location tracker to reflect the replacement
          LocationTracker replacedLt = locationTracker.replace(pos.getLineNum(), pos.getColumnNum(),
              pos.getEndLineNum(), pos.getEndColumnNum(), r.getOriginal(), r.getReplacement());

          // creates the node
          Node replacementNode = new Node(parent, originalStart.getX(), originalStart.getY(),
              originalEnd.getX(), originalEnd.getY(), r.getOriginal(), r.getReplacement(),
              r.getOriginal().length());

          // calls the loop again
          loop(r.getQuery(), replacementLimit, replacementNode, depth + 1, replacedLt);
        }

        /* termination to end the loop if the instance was not a full run through the query.
        In other words, it ensures that the termination condition is not hit on the way back
        up the tree */
        return;
      }
    } catch (Exception e) {
      /* this is boiler plate code when a different exception is thrown from using
         a different parser
       */
      return;
    }

    // termination condition: if the parsing doesn't throw exceptions, then the leaf is reached
    if (depth < minimumUnparseableComp) {
      minimumUnparseableComp = depth;
      solution = parent;
      finalString = inputQuery;
    }
  }

  /**
   * This method implements the deletion mechanism: given the position of the component, it
   * generates a new query with that component deleted.
   */
  public static String deletion(String inputQuery, int startLine, int startColumn,
      int endLine, int endColumn) {
    StringBuilder sb = new StringBuilder(inputQuery);

    // gets the component to delete
    int[] index = returnIndex(inputQuery, startLine, startColumn, endLine, endColumn);
    sb.delete(index[0], index[1]);
    if (startLine != endLine) {
      // we add a new line character whenever we multi-line delete to keep queries in same line
      sb.insert(index[0], '\n');
    }
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
   * replacementLimit is the number of replacements we choose to have
   */
  public static ArrayList<ReplacedComponent> replacement(String inputQuery, int replacementLimit,
      int startLine, int startColumn,
      int endLine, int endColumn, Collection<String> expectedTokens) {
    // get component to replace from
    int[] index = returnIndex(inputQuery, startLine, startColumn, endLine, endColumn);
    String replaceFrom = inputQuery.substring(index[0], index[1]);

    // call ReplacementLogic
    ArrayList<String> finalList = ReplacementLogic.replace(replaceFrom, replacementLimit,
        expectedTokensFilter(expectedTokens));

    ArrayList<ReplacedComponent> result = new ArrayList<>();

    // generate the new queries. We need to re-instantiate the StringBuilder each time
    for (String replaceTo: finalList) {
      // replace the token
      StringBuilder sb = new StringBuilder(inputQuery);
      sb.replace(index[0], index[1], replaceTo);
      if (startLine != endLine) {
        // we add a new line character whenever we multi-line delete to keep queries in same line
        sb.insert(index[0] + replaceTo.length(), '\n');
      }
      result.add(new ReplacedComponent(sb.toString(), replaceFrom, replaceTo));
    }
    return result;
  }

  /**
   * This helper method filters out quotations from the expected tokens for the replacement method
   * above
   */
  private static ArrayList<String> expectedTokensFilter(Collection<String> expectedTokens) {
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
  private static int[] returnIndex(String inputQuery, int startLine, int startColumn, int endLine,
      int endColumn) {
    int[] result = new int[2];
    // when the exception occurs in line 1 and ends in line 1
    if (startLine == 1 && endLine == 1) {
      result[0] = startColumn - 1;
      result[1] = endColumn;
    }
    // when the exception occurs in line 1 but doesn't end in line 1
    else if (startLine == 1) {
      result[0] = startColumn - 1;
      result[1] = findNthIndexOf(inputQuery, '\n', endLine - 1) + endColumn + 1;
    }
    // all other cases
    else {
      int position = findNthIndexOf(inputQuery, '\n', startLine - 1);
      int endPosition = findNthIndexOf(inputQuery, '\n', endLine - 1);
      result[0] = position + startColumn;
      result[1] = endPosition + endColumn + 1;
    }

    return result;
  }

  /**
   * This helper method returns the index of the nth occurrence of key character in the input
   * string. Returns -1 if there is no such instance.
   */
  private static int findNthIndexOf(String string, char key, int n) {
    int position = string.indexOf(key);
    while (n > 1) {
      // if there is no such instance
      if (position == -1) {
        return position;
      }
      // constantly gets the next occurence of the key
      position = string.indexOf(key, position + 1);
      n -= 1;
    }
    return position;
  }

  /**
   * Getter method for finalString variable
   */
  public String getFinalString() {
    return finalString;
  }
}
