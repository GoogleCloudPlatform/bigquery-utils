package com.google.bigquery;

import org.json.simple.JSONObject;

/**
 * This class represents a single node in the tree visualization of the algorithm. Below are fields
 * necessary for implementation.
 */
public class Node {

  // ensures tree structure
  private Node parent;

  // ensures that we know which location the error occurs in
  private int startLine;
  private int startColumn;
  private int endLine;
  private int endColumn;

  // ensures that we know how the error was handled
  private String replaceFrom;
  private String replaceTo;

  // ensures that we know what the performance measure is and how our current path is doing
  private int unparseableCount;

  // indicates how we handled the error
  enum ErrorHandlingType {
    DELETION,
    REPLACEMENT
  }

  ErrorHandlingType type;

  /**
   * constructor for the node class for deletion
   */
  public Node (Node parent, int startLine, int startColumn, int endLine, int endColumn,
      int unparseableCount) {
    this.parent = parent;
    this.startLine = startLine;
    this.startColumn = startColumn;
    this.endLine = endLine;
    this.endColumn = endColumn;
    type = ErrorHandlingType.DELETION;
    this.unparseableCount = unparseableCount;
  }

  /**
   * constructor for the node class for replacement
   */
  public Node (Node parent, int startLine, int startColumn, int endLine, int endColumn,
      String replaceFrom, String replaceTo, int unparseableCount) {
    this.parent = parent;
    this.startLine = startLine;
    this.startColumn = startColumn;
    this.endLine = endLine;
    this.endColumn = endColumn;
    type = ErrorHandlingType.REPLACEMENT;
    this.replaceFrom = replaceFrom;
    this.replaceTo = replaceTo;
    this.unparseableCount = unparseableCount;
  }

  /**
   * Override toString method for outputting results and better debugging
   */
  @Override
  public String toString() {
    if (type.equals(ErrorHandlingType.DELETION)) {
      return String.format("Unparseable portion: Start Line %1$s, End Line %2$s, "
              + "Start Column %3$s, End Column %4$s, %5$s", startLine,
          endLine, startColumn, endColumn, type);
    }
    else {
      return String.format("Unparseable portion: Start Line %1$s, End Line %2$s, "
              + "Start Column %3$s, End Column %4$s, %5$s: replaced %6$s with %7$s",
          startLine, endLine, startColumn, endColumn, type, replaceFrom, replaceTo);
    }
  }

  /**
   * Returns a JSON Object representation of the node for integration
   */
  public JSONObject toJSON() {
    JSONObject errorPosition = new JSONObject();
    errorPosition.put("startLine", startLine);
    errorPosition.put("startColumn", startColumn);
    errorPosition.put("endLine", endLine);
    errorPosition.put("endColumn", endColumn);
    if (type.equals(ErrorHandlingType.DELETION)) {
      JSONObject deletionJson = new JSONObject();
      deletionJson.put("error_position", errorPosition);
      deletionJson.put("error_type", "DELETION");
      return deletionJson;
    }
    else {
      JSONObject replaceJson = new JSONObject();
      replaceJson.put("error_position", errorPosition);
      replaceJson.put("error_type", "REPLACEMENT");
      replaceJson.put("replacedFrom", replaceFrom);
      replaceJson.put("replacedTo", replaceTo);
      return replaceJson;
    }
  }

  /**
   * no args constructor for the root node
   */
  public Node () {}

  /**
   * getters for fields
   */
  public Node getParent() {
    return parent;
  }

  public int getUnparseableCount() {
    return unparseableCount;
  }
}
