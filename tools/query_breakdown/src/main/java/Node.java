import java.util.ArrayList;

/**
 * This class represents a single node in the tree visualization of the algorithm. Below are fields
 * necessary for implementation.
 */
public class Node {

  // ensures tree structure
  ArrayList<Node> children;
  Node parent;

  // ensures that we know which location the error occurs in
  int startLine;
  int startColumn;
  int endLine;
  int endColumn;

  // ensures that we know how the error was handled
  boolean errorHandlingType;
  String replaceFrom;
  String replaceTo;

  // ensures that we know what the performance measure is and how our current path is doing
  int unparseableCount;

  /**
   * constructor for the node class
   */
  public Node (Node parent, int startLine, int startColumn, int endLine, int endColumn,
      boolean errorHandlingType, String replaceFrom, String replaceTo, int unparseableCount) {
  }
}
