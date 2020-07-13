import org.apache.calcite.sql.parser.SqlParseException;

/**
 * This class is where the main logic lives for the algorithm that this tool utilizes. It will
 * also be in charge of outputting the results.
 */
public class UnparseableDetector {

  // global fields that keeps track of the minimum unparseable component so far
  int minimumUnparseableComp;
  Node solution;

  // the generated tree
  Node root;
  /**
   * This is the method that will run the UnparseableDetector given an original query and output
   * it to the specified output file, or if that is null, generate a new file to put the output in.
   * The provided timeLimit will stop the tool from running over a certain time.
   */
  public static void run(String originalQuery, String outputFile, double timeLimit) {
    return;
  }

  /**
   * This is where the code for the algorithm will go: essentially, there will be a loop that
   * constantly inputs a new query after adequate error handling
   */
  public static void loop(String inputQuery) {
    return;
  }

  /**
   * This method implements the deletion mechanism: given the position of the component, it
   * generates a new query with that component deleted.
   */
  public static String deletion(String inputQuery, int startLine, int startColumn, int endLine,
      int endColumn) {
    return "";
  }

  /**
   * This method implements the replacement mechanism: given the position of the component, and
   * given the help of the ReplacementLogic class, it determines what to replace the component
   * with and generates a new query with that component replaced.
   */
  public static String replacement(String inputQuery, int startLine, int startColumn, int endLine,
      int endColumn, SqlParseException exception) {
    // call ReplacementLogic
    ReplacementLogic.replace(inputQuery);
    return "";
  }
}
