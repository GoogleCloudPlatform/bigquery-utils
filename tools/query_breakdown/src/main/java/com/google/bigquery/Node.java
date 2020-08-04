package com.google.bigquery;

import java.util.ArrayList;

/**
 * This class represents a single node in the tree visualization of the algorithm. Below are fields
 * necessary for implementation.
 */
public class Node {

  // ensures tree structure

  private ArrayList<Node> children;
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
   * Override toString method for better debugging
   */
  @Override
  public String toString() {
    return String.format("Parent: %1$s, %2$s\nstartline: %3$s\nstartColumn: %4$s"
        + "\nendLine: %5$s\nendColumn: %6$s\ntype: %7$s\nreplaceFrom: %8$s\nreplaceTo: %9$s\n"
        + "unparseableCount: %10$s\n", parent.getStartColumn(), parent.getEndColumn(), startLine,
        startColumn, endLine, endColumn, type, replaceFrom, replaceTo, unparseableCount);
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

  public int getStartLine() {
    return startLine;
  }

  public int getStartColumn() {
    return startColumn;
  }

  public int getEndLine() {
    return endLine;
  }

  public int getEndColumn() {
    return endColumn;
  }

  public String getReplaceFrom() {
    return replaceFrom;
  }

  public String getReplaceTo() {
    return replaceTo;
  }

  public String getErrorHandlingType() {
    return type.toString();
  }
}
